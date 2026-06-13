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

/// <summary>§6.5 consume: process-then-commit, attempts-from-header, retry-tier republish, DLQ, unknown-URN.</summary>
public sealed class KafkaConsumerTests
{
    private const string Urn = "urn:babel:orders:created";

    private static Envelope Envelope(int attempts) =>
        EnvelopeCodec.Make(Urn, new Dictionary<string, object?> { ["order_id"] = 7 }, "orders", "trace-1")
            with { Attempts = attempts };

    private static ConsumeResult<byte[], byte[]> ResultFor(Envelope env, long offset = 10) => new()
    {
        Topic = "orders",
        Partition = new Partition(0),
        Offset = new Offset(offset),
        Message = new Message<byte[], byte[]>
        {
            Value = Encoding.UTF8.GetBytes(EnvelopeCodec.Encode(env)),
            Headers = KafkaHeaders.Project(env),
        },
    };

    private static Mock<IConsumer<byte[], byte[]>> ConsumerWith(ConsumeResult<byte[], byte[]> result)
    {
        var consumer = new Mock<IConsumer<byte[], byte[]>>();
        consumer.Setup(c => c.Consume(It.IsAny<TimeSpan>())).Returns(result);
        return consumer;
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

    private static RetryTopics Topology() =>
        RetryTopics.ForTopic("orders").Tier(TimeSpan.FromSeconds(5)).Tier(TimeSpan.FromSeconds(60)).Build();

    [Fact]
    public async Task SuccessProcessesThenCommits()
    {
        var consumer = ConsumerWith(ResultFor(Envelope(0), 41));
        var seen = -1;
        var handlers = new Dictionary<string, BabelHandler>
        {
            [Urn] = (env, _, _) => { seen = env.Attempts; return Task.CompletedTask; },
        };

        var count = await new KafkaConsumer(consumer.Object, handlers).PollAsync();

        Assert.Equal(1, count);
        Assert.Equal(0, seen);
        consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Once);
    }

    [Fact]
    public async Task AttemptsHeaderIsAuthoritative()
    {
        var seen = -1;
        var handlers = new Dictionary<string, BabelHandler>
        {
            [Urn] = (env, _, _) => { seen = env.Attempts; return Task.CompletedTask; },
        };
        await new KafkaConsumer(ConsumerWith(ResultFor(Envelope(2))).Object, handlers).PollAsync();
        Assert.Equal(2, seen);
    }

    [Fact]
    public async Task ThrowingHandlerRepublishesToRetryWithAttemptsPlusOne()
    {
        var consumer = ConsumerWith(ResultFor(Envelope(0)));
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        Exception? reported = null;
        var handlers = new Dictionary<string, BabelHandler>
        {
            [Urn] = (_, _, _) => throw new InvalidOperationException("boom"),
        };

        await new KafkaConsumer(consumer.Object, handlers, new KafkaConsumerOptions
        {
            Producer = Producer(captured).Object,
            RetryTopics = Topology(),
            MaxTries = 3,
            OnError = (e, _, _) => reported = e,
        }).PollAsync();

        Assert.IsType<InvalidOperationException>(reported);
        Assert.Equal("orders.retry.1", captured[0].Topic);
        Assert.Equal("1", KafkaHeaders.GetString(captured[0].Message.Headers, KafkaHeaders.Attempts));
        Assert.Equal("5000", KafkaHeaders.GetString(captured[0].Message.Headers, KafkaHeaders.Delay));
        consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Once);
    }

    [Fact]
    public async Task TerminalFailureGoesToDlqWithDeadLetterBlock()
    {
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var handlers = new Dictionary<string, BabelHandler>
        {
            [Urn] = (_, _, _) => throw new InvalidOperationException("boom"),
        };

        await new KafkaConsumer(ConsumerWith(ResultFor(Envelope(2))).Object, handlers, new KafkaConsumerOptions
        {
            Producer = Producer(captured).Object,
            RetryTopics = Topology(),
            MaxTries = 3,
        }).PollAsync();

        Assert.Equal("orders.dlq", captured[0].Topic);
        var dead = EnvelopeCodec.Decode(Encoding.UTF8.GetString(captured[0].Message.Value));
        Assert.Equal("failed", dead.DeadLetter!.Reason);
    }

    [Fact]
    public async Task RetryWithoutTopicsOrDlqRaises()
    {
        var handlers = new Dictionary<string, BabelHandler>
        {
            [Urn] = (_, _, _) => throw new InvalidOperationException("boom"),
        };
        var consumer = new KafkaConsumer(ConsumerWith(ResultFor(Envelope(0))).Object, handlers);
        await Assert.ThrowsAsync<BabelQueueException>(() => consumer.PollAsync());
    }

    [Fact]
    public async Task UnknownUrnFailRaisesAndDoesNotCommit()
    {
        var consumer = ConsumerWith(ResultFor(Envelope(0)));
        var worker = new KafkaConsumer(consumer.Object, new Dictionary<string, BabelHandler>());
        await Assert.ThrowsAsync<UnknownUrnException>(() => worker.PollAsync());
        consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Never);
    }

    [Fact]
    public async Task UnknownUrnDeleteCommits()
    {
        var consumer = ConsumerWith(ResultFor(Envelope(0)));
        await new KafkaConsumer(consumer.Object, new Dictionary<string, BabelHandler>(), new KafkaConsumerOptions
        {
            UnknownUrn = UnknownUrnStrategy.Delete,
        }).PollAsync();
        consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Once);
    }

    [Fact]
    public async Task UnknownUrnDeadLetterGoesToDlq()
    {
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        await new KafkaConsumer(ConsumerWith(ResultFor(Envelope(0))).Object, new Dictionary<string, BabelHandler>(), new KafkaConsumerOptions
        {
            Producer = Producer(captured).Object,
            RetryTopics = Topology(),
            UnknownUrn = UnknownUrnStrategy.DeadLetter,
        }).PollAsync();
        Assert.Equal("orders.dlq", captured[0].Topic);
        var dead = EnvelopeCodec.Decode(Encoding.UTF8.GetString(captured[0].Message.Value));
        Assert.Equal("unknown_urn", dead.DeadLetter!.Reason);
    }

    [Fact]
    public async Task UnknownUrnReleaseRepublishesToRetry()
    {
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        await new KafkaConsumer(ConsumerWith(ResultFor(Envelope(0))).Object, new Dictionary<string, BabelHandler>(), new KafkaConsumerOptions
        {
            Producer = Producer(captured).Object,
            RetryTopics = Topology(),
            UnknownUrn = UnknownUrnStrategy.Release,
        }).PollAsync();
        Assert.Equal("orders.retry.1", captured[0].Topic);
    }

    [Fact]
    public async Task PoisonBodyForwardedRawToDlq()
    {
        var result = new ConsumeResult<byte[], byte[]>
        {
            Topic = "orders",
            Partition = new Partition(0),
            Offset = new Offset(4),
            Message = new Message<byte[], byte[]> { Value = Encoding.UTF8.GetBytes("not-json"), Headers = new Headers() },
        };
        var consumer = ConsumerWith(result);
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        Exception? reported = null;

        await new KafkaConsumer(consumer.Object, new Dictionary<string, BabelHandler>(), new KafkaConsumerOptions
        {
            Producer = Producer(captured).Object,
            RetryTopics = Topology(),
            OnError = (e, _, _) => reported = e,
        }).PollAsync();

        Assert.Equal("orders.dlq", captured[0].Topic);
        Assert.NotNull(reported);
        consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Once);
    }

    [Fact]
    public async Task PollReturnsZeroOnTimeout()
    {
        var consumer = new Mock<IConsumer<byte[], byte[]>>();
        consumer.Setup(c => c.Consume(It.IsAny<TimeSpan>())).Returns((ConsumeResult<byte[], byte[]>?)null);
        var count = await new KafkaConsumer(consumer.Object, new Dictionary<string, BabelHandler>()).PollAsync();
        Assert.Equal(0, count);
    }

    [Fact]
    public async Task RunStopsWhenCancelled()
    {
        var consumer = new Mock<IConsumer<byte[], byte[]>>();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        await new KafkaConsumer(consumer.Object, new Dictionary<string, BabelHandler>()).RunAsync(cts.Token);
        consumer.Verify(c => c.Consume(It.IsAny<TimeSpan>()), Times.Never);
    }
}
