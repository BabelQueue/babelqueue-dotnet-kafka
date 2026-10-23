using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BabelQueue;
using BabelQueue.Kafka;
using Confluent.Kafka;
using Moq;
using Xunit;

namespace BabelQueue.Kafka.Tests;

/// <summary>
/// ADR-0028: the W3C <c>traceparent</c> / <c>tracestate</c> transport headers are carried out of band, so they
/// must survive every republish the consumer performs (retry tier, DLQ) and the re-inject hop back to the work topic.
/// </summary>
public sealed class KafkaTraceChainTests
{
    private const string Urn = "urn:babel:orders:created";
    private const string Traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    private const string Tracestate = "vendor=opaque,other=1";

    private static readonly byte[] Key = { 0x00, 0x01, 0xFE, 0xFF };

    private static ConsumeResult<byte[], byte[]> Source(Envelope env, bool withTrace = true)
    {
        var headers = KafkaHeaders.Project(env);
        if (withTrace)
        {
            KafkaHeaders.Add(headers, "traceparent", Traceparent);
            KafkaHeaders.Add(headers, "tracestate", Tracestate);
        }
        return new ConsumeResult<byte[], byte[]>
        {
            Topic = "orders",
            Partition = new Partition(0),
            Offset = new Offset(10),
            Message = new Message<byte[], byte[]>
            {
                Key = Key,
                Value = Encoding.UTF8.GetBytes(EnvelopeCodec.Encode(env)),
                Headers = headers,
            },
        };
    }

    private static Mock<IProducer<byte[], byte[]>> Producer(List<(string Topic, Message<byte[], byte[]> Message)> captured)
    {
        var producer = new Mock<IProducer<byte[], byte[]>>();
        producer
            .Setup(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<byte[], byte[]>>(), It.IsAny<CancellationToken>()))
            .Callback<string, Message<byte[], byte[]>, CancellationToken>((t, m, _) => captured.Add((t, m)))
            .ReturnsAsync(new DeliveryResult<byte[], byte[]>());
        return producer;
    }

    private static async Task<(string Topic, Message<byte[], byte[]> Message)> FailOnce(
        ConsumeResult<byte[], byte[]> source, int maxTries, Envelope env)
    {
        var consumer = new Mock<IConsumer<byte[], byte[]>>();
        consumer.Setup(c => c.Consume(It.IsAny<TimeSpan>())).Returns(source);
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var handlers = new Dictionary<string, BabelHandler>
        {
            [Urn] = (_, _, _) => throw new InvalidOperationException("boom"),
        };
        await new KafkaConsumer(consumer.Object, handlers, new KafkaConsumerOptions
        {
            Producer = Producer(captured).Object,
            RetryTopics = RetryTopics.ForTopic("orders").Tier(TimeSpan.FromSeconds(5)).Build(),
            MaxTries = maxTries,
            OnError = (_, _, _) => { },
        }).PollAsync();
        return Assert.Single(captured);
    }

    private static Envelope NewEnvelope(int attempts = 0) =>
        EnvelopeCodec.Make(Urn, new Dictionary<string, object?> { ["order_id"] = 7 }, "orders", "trace-1") with { Attempts = attempts };

    [Fact]
    public async Task RetryRepublishCarriesTraceparentAndTracestate()
    {
        var env = NewEnvelope();
        var (topic, message) = await FailOnce(Source(env), maxTries: 3, env);

        Assert.Equal("orders.retry.1", topic);
        Assert.Equal(Traceparent, KafkaHeaders.GetString(message.Headers, "traceparent"));
        Assert.Equal(Tracestate, KafkaHeaders.GetString(message.Headers, "tracestate"));
        Assert.Equal("1", KafkaHeaders.GetString(message.Headers, KafkaHeaders.Attempts));
    }

    [Fact]
    public async Task DlqRepublishCarriesTraceparentAndTracestate()
    {
        var env = NewEnvelope(attempts: 2);
        var (topic, message) = await FailOnce(Source(env), maxTries: 3, env);

        Assert.Equal("orders.dlq", topic);
        Assert.Equal(Traceparent, KafkaHeaders.GetString(message.Headers, "traceparent"));
        Assert.Equal(Tracestate, KafkaHeaders.GetString(message.Headers, "tracestate"));
    }

    [Fact]
    public async Task RepublishDoesNotInventTraceHeadersWhenTheSourceHasNone()
    {
        var env = NewEnvelope();
        var (_, message) = await FailOnce(Source(env, withTrace: false), maxTries: 3, env);

        Assert.Null(KafkaHeaders.GetString(message.Headers, "traceparent"));
        Assert.Null(KafkaHeaders.GetString(message.Headers, "tracestate"));
    }

    [Fact]
    public async Task PoisonDlqRecordCarriesTheTraceHeaders()
    {
        var headers = new Headers();
        KafkaHeaders.Add(headers, "traceparent", Traceparent);
        var source = new ConsumeResult<byte[], byte[]>
        {
            Topic = "orders",
            Partition = new Partition(0),
            Offset = new Offset(3),
            Message = new Message<byte[], byte[]> { Key = Key, Value = Encoding.UTF8.GetBytes("not json"), Headers = headers },
        };
        var consumer = new Mock<IConsumer<byte[], byte[]>>();
        consumer.Setup(c => c.Consume(It.IsAny<TimeSpan>())).Returns(source);
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();

        await new KafkaConsumer(consumer.Object, new Dictionary<string, BabelHandler>(), new KafkaConsumerOptions
        {
            Producer = Producer(captured).Object,
            RetryTopics = RetryTopics.ForTopic("orders").Tier(TimeSpan.FromSeconds(5)).Build(),
            OnError = (_, _, _) => { },
        }).PollAsync();

        var (topic, message) = Assert.Single(captured);
        Assert.Equal("orders.dlq", topic);
        Assert.Equal(Traceparent, KafkaHeaders.GetString(message.Headers, "traceparent"));
    }

    [Fact]
    public async Task ConsumerFailureThenReinjectKeepsTheTraceAndTheBytes()
    {
        // work topic --(handler throws)--> orders.retry.1 --(re-inject)--> orders
        var env = NewEnvelope();
        var (retryTopic, retryMessage) = await FailOnce(Source(env), maxTries: 3, env);
        Assert.Equal("orders.retry.1", retryTopic);

        // The broker stamps the retry record; an old timestamp makes it due immediately (A6 clamp).
        var now = new DateTimeOffset(2026, 10, 23, 12, 0, 0, TimeSpan.Zero);
        var retryRecord = new ConsumeResult<byte[], byte[]>
        {
            Topic = retryTopic,
            Partition = new Partition(0),
            Offset = new Offset(1),
            Message = new Message<byte[], byte[]>
            {
                Key = retryMessage.Key,
                Value = retryMessage.Value,
                Headers = retryMessage.Headers,
                Timestamp = new Timestamp(now.AddMinutes(-10).UtcDateTime, TimestampType.LogAppendTime),
            },
        };

        var consumer = new Mock<IConsumer<byte[], byte[]>>();
        consumer.SetupSequence(c => c.Consume(It.IsAny<TimeSpan>())).Returns(retryRecord).Returns((ConsumeResult<byte[], byte[]>)null!);
        var produced = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var reinject = new KafkaReinjectConsumer(consumer.Object, new KafkaReinjectOptions
        {
            Producer = Producer(produced).Object,
            Clock = () => now,
            PollTimeout = TimeSpan.FromMilliseconds(10),
        });

        Assert.Equal(1, await reinject.PollAsync());

        var (workTopic, work) = Assert.Single(produced);
        Assert.Equal("orders", workTopic);
        Assert.Equal(Traceparent, KafkaHeaders.GetString(work.Headers, "traceparent"));
        Assert.Equal(Tracestate, KafkaHeaders.GetString(work.Headers, "tracestate"));
        Assert.Equal(Key, work.Key);
        Assert.True(retryMessage.Value.SequenceEqual(work.Value), "re-inject must carry the retry record value byte-for-byte");
        Assert.Equal("1", KafkaHeaders.GetString(work.Headers, KafkaHeaders.Attempts));
        Assert.Null(KafkaHeaders.GetString(work.Headers, KafkaHeaders.Delay));
        Assert.Null(KafkaHeaders.GetString(work.Headers, KafkaHeaders.OriginalTopic));
    }
}
