using System;
using System.Collections.Generic;
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
/// The original record key must survive every consumer-side republish: the retry-tier write, the unknown-URN
/// release, the envelope DLQ, and the raw/poison DLQ. A null key stays null, an empty key stays empty and
/// no key is ever synthesised.
/// </summary>
public sealed class KafkaKeyPreservationTests
{
    private const string Urn = "urn:babel:orders:created";

    public static IEnumerable<object?[]> Keys()
    {
        yield return new object?[] { Encoding.UTF8.GetBytes("order-7") };
        yield return new object?[] { null };
        yield return new object?[] { Array.Empty<byte>() };
    }

    private static Envelope Envelope(int attempts) =>
        EnvelopeCodec.Make(Urn, new Dictionary<string, object?> { ["order_id"] = 7 }, "orders", "trace-1")
            with { Attempts = attempts };

    private static ConsumeResult<byte[], byte[]> Keyed(byte[]? key, byte[] value, Headers headers) => new()
    {
        Topic = "orders",
        Partition = new Partition(0),
        Offset = new Offset(10),
        Message = new Message<byte[], byte[]> { Key = key!, Value = value, Headers = headers },
    };

    private static ConsumeResult<byte[], byte[]> KeyedEnvelope(byte[]? key, int attempts)
    {
        var env = Envelope(attempts);
        return Keyed(key, Encoding.UTF8.GetBytes(EnvelopeCodec.Encode(env)), KafkaHeaders.Project(env));
    }

    private static List<(string Topic, Message<byte[], byte[]> Message)> Captured() => new();

    private static Mock<IConsumer<byte[], byte[]>> ConsumerWith(ConsumeResult<byte[], byte[]> result)
    {
        var consumer = new Mock<IConsumer<byte[], byte[]>>();
        consumer.Setup(c => c.Consume(It.IsAny<TimeSpan>())).Returns(result);
        return consumer;
    }

    private static IProducer<byte[], byte[]> Producer(List<(string Topic, Message<byte[], byte[]> Message)> captured)
    {
        var producer = new Mock<IProducer<byte[], byte[]>>();
        producer
            .Setup(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<byte[], byte[]>>(), It.IsAny<CancellationToken>()))
            .Callback<string, Message<byte[], byte[]>, CancellationToken>((t, m, _) => captured.Add((t, m)))
            .ReturnsAsync(new DeliveryResult<byte[], byte[]>());
        return producer.Object;
    }

    private static RetryTopics Topology() =>
        RetryTopics.ForTopic("orders").Tier(TimeSpan.FromSeconds(5)).Tier(TimeSpan.FromSeconds(60)).Build();

    private static Dictionary<string, BabelHandler> Throwing() => new()
    {
        [Urn] = (_, _, _) => throw new InvalidOperationException("boom"),
    };

    private static void AssertKey(byte[]? expected, Message<byte[], byte[]> sent)
    {
        if (expected is null)
        {
            Assert.Null(sent.Key); // no key synthesised
        }
        else
        {
            Assert.NotNull(sent.Key);
            Assert.Equal(expected, sent.Key);
        }
    }

    private static async Task<Message<byte[], byte[]>> Run(
        ConsumeResult<byte[], byte[]> result, Dictionary<string, BabelHandler> handlers, string expectedTopic, string? unknownUrn = null)
    {
        var captured = Captured();
        var options = new KafkaConsumerOptions { Producer = Producer(captured), RetryTopics = Topology() };
        if (unknownUrn is not null)
        {
            options.UnknownUrn = unknownUrn;
        }
        await new KafkaConsumer(ConsumerWith(result).Object, handlers, options).PollAsync();
        Assert.Single(captured);
        Assert.Equal(expectedTopic, captured[0].Topic);
        return captured[0].Message;
    }

    [Theory]
    [MemberData(nameof(Keys))]
    public async Task RetryTierRepublishKeepsKey(byte[]? key) =>
        AssertKey(key, await Run(KeyedEnvelope(key, 0), Throwing(), "orders.retry.1"));

    [Theory]
    [MemberData(nameof(Keys))]
    public async Task EnvelopeDlqKeepsKey(byte[]? key) =>
        // attempts 2 + 1 == MaxTries (3): terminal, goes to the DLQ.
        AssertKey(key, await Run(KeyedEnvelope(key, 2), Throwing(), "orders.dlq"));

    [Theory]
    [MemberData(nameof(Keys))]
    public async Task UnknownUrnDeadLetterKeepsKey(byte[]? key) =>
        AssertKey(key, await Run(KeyedEnvelope(key, 0), new Dictionary<string, BabelHandler>(), "orders.dlq", UnknownUrnStrategy.DeadLetter));

    [Theory]
    [MemberData(nameof(Keys))]
    public async Task UnknownUrnReleaseKeepsKey(byte[]? key) =>
        AssertKey(key, await Run(KeyedEnvelope(key, 0), new Dictionary<string, BabelHandler>(), "orders.retry.1", UnknownUrnStrategy.Release));

    [Theory]
    [MemberData(nameof(Keys))]
    public async Task NonConformantEnvelopeDlqKeepsKey(byte[]? key) =>
        // "not-json" decodes leniently but fails the conformance check -> annotated "poison" envelope DLQ.
        AssertKey(key, await Run(Keyed(key, Encoding.UTF8.GetBytes("not-json"), new Headers()), new Dictionary<string, BabelHandler>(), "orders.dlq"));

    [Theory]
    [MemberData(nameof(Keys))]
    public async Task RawDlqKeepsKey(byte[]? key)
    {
        var headers = new Headers { { KafkaHeaders.SchemaVersion, Encoding.UTF8.GetBytes("2") } };
        var body = Encoding.UTF8.GetBytes(EnvelopeCodec.Encode(Envelope(0)));
        var sent = await Run(Keyed(key, body, headers), new Dictionary<string, BabelHandler>(), "orders.dlq");
        Assert.Equal(body, sent.Value); // raw forward: bytes untouched
        AssertKey(key, sent);
    }
}
