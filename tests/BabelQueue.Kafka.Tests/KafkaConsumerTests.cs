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

    [Theory]
    [InlineData(1, "orders.retry.2")]
    [InlineData(2, "orders.retry.2")]
    [InlineData(9, "orders.retry.2")]
    public async Task AFailureWithAttemptsBeforeAtOrAboveTheTierCountLandsInTheLastTier(int attemptsBefore, string expectedTopic)
    {
        var consumer = ConsumerWith(ResultFor(Envelope(attemptsBefore)));
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var handlers = new Dictionary<string, BabelHandler>
        {
            [Urn] = (_, _, _) => throw new InvalidOperationException("boom"),
        };

        await new KafkaConsumer(consumer.Object, handlers, new KafkaConsumerOptions
        {
            Producer = Producer(captured).Object,
            RetryTopics = Topology(),
            MaxTries = 20,
        }).PollAsync();

        Assert.Equal(expectedTopic, Assert.Single(captured).Topic);
        Assert.Equal("60000", KafkaHeaders.GetString(captured[0].Message.Headers, KafkaHeaders.Delay));
        Assert.Equal((attemptsBefore + 1).ToString(System.Globalization.CultureInfo.InvariantCulture),
            KafkaHeaders.GetString(captured[0].Message.Headers, KafkaHeaders.Attempts));
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
    public async Task PoisonDlqPreservesSourceKey()
    {
        var key = Encoding.UTF8.GetBytes("customer-42");
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var result = new ConsumeResult<byte[], byte[]>
        {
            Topic = "orders",
            Partition = new Partition(0),
            Offset = new Offset(4),
            Message = new Message<byte[], byte[]> { Key = key, Value = Encoding.UTF8.GetBytes("not-json"), Headers = new Headers() },
        };

        await new KafkaConsumer(ConsumerWith(result).Object, new Dictionary<string, BabelHandler>(), new KafkaConsumerOptions
        {
            Producer = Producer(captured).Object,
            RetryTopics = Topology(),
        }).PollAsync();

        Assert.Equal("orders.dlq", captured[0].Topic);
        Assert.Equal(key, captured[0].Message.Key);
        Assert.Equal("poison", ParseValue(captured[0].Message).GetProperty("dead_letter").GetProperty("reason").GetString());
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

    private static async Task<(Message<byte[], byte[]> Dlq, Mock<IConsumer<byte[], byte[]>> Consumer, Exception? Reported)> RunPoison(
        byte[] body, Headers? headers = null, byte[]? key = null)
    {
        var result = new ConsumeResult<byte[], byte[]>
        {
            Topic = "orders",
            Partition = new Partition(0),
            Offset = new Offset(4),
            Message = new Message<byte[], byte[]> { Key = key!, Value = body, Headers = headers ?? new Headers() },
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

        var sent = Assert.Single(captured);
        Assert.Equal("orders.dlq", sent.Topic);
        return (sent.Message, consumer, reported);
    }

    private static System.Text.Json.JsonElement ParseValue(Message<byte[], byte[]> message) =>
        System.Text.Json.JsonDocument.Parse(message.Value).RootElement.Clone();

    [Fact]
    public async Task PoisonBodyIsWrappedWithPoisonDeadLetterAndKeyKept()
    {
        var key = Encoding.UTF8.GetBytes("customer-42");
        var (dlq, consumer, reported) = await RunPoison(Encoding.UTF8.GetBytes("not-json"), key: key);

        var value = ParseValue(dlq);
        Assert.Equal("not-json", value.GetProperty("raw").GetString());
        var deadLetter = value.GetProperty("dead_letter");
        Assert.Equal("poison", deadLetter.GetProperty("reason").GetString());
        Assert.Equal("orders", deadLetter.GetProperty("original_queue").GetString());
        Assert.Equal(0, deadLetter.GetProperty("attempts").GetInt32());
        Assert.Equal("dotnet", deadLetter.GetProperty("lang").GetString());
        Assert.False(string.IsNullOrEmpty(deadLetter.GetProperty("error").GetString()));
        Assert.Equal("BabelQueue.BabelQueueException", deadLetter.GetProperty("exception").GetString());
        Assert.True(deadLetter.GetProperty("failed_at").GetInt64() > 0);
        Assert.Equal(key, dlq.Key);
        Assert.Equal("orders", KafkaHeaders.GetString(dlq.Headers, KafkaHeaders.OriginalTopic));
        Assert.NotNull(reported);
        consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Once);
    }

    [Fact]
    public async Task PoisonJsonObjectKeepsItsKeysAndReplacesAnExistingDeadLetter()
    {
        // data is a list (not an object): the decoder rejects it, so it is poison; its keys are kept verbatim
        // (the number is not reformatted) and the stale dead_letter is overridden.
        var body = "{\"job\":\"urn:babel:orders:created\",\"data\":[1],\"price\":1.50,\"dead_letter\":{\"reason\":\"failed\"}}";
        var (dlq, _, _) = await RunPoison(Encoding.UTF8.GetBytes(body));

        var value = ParseValue(dlq);
        Assert.False(value.TryGetProperty("raw", out _));
        Assert.Equal("urn:babel:orders:created", value.GetProperty("job").GetString());
        Assert.Equal(System.Text.Json.JsonValueKind.Array, value.GetProperty("data").ValueKind);
        Assert.Contains("\"price\":1.50", Encoding.UTF8.GetString(dlq.Value));
        Assert.Equal("poison", value.GetProperty("dead_letter").GetProperty("reason").GetString());
        Assert.Equal("urn:babel:orders:created", KafkaHeaders.GetString(dlq.Headers, KafkaHeaders.Job));
    }

    [Theory]
    [InlineData("[1,2]")]
    [InlineData("{\"a\":1} {\"b\":2}")]
    [InlineData("")]
    public async Task PoisonNonObjectBodiesAreCarriedAsRaw(string body)
    {
        var (dlq, _, _) = await RunPoison(Encoding.UTF8.GetBytes(body));

        var value = ParseValue(dlq);
        Assert.Equal(body, value.GetProperty("raw").GetString());
        Assert.Equal("poison", value.GetProperty("dead_letter").GetProperty("reason").GetString());
    }

    [Fact]
    public async Task PoisonDeadLetterNeverProjectsAZeroSchemaVersionHeader()
    {
        // No meta.schema_version → lenient decode yields 0, which must not become `bq-schema-version: 0`
        // (a later §6.7 gate would reject the DLQ record).
        var (dlq, _, _) = await RunPoison(Encoding.UTF8.GetBytes("not-json"));

        Assert.False(dlq.Headers.TryGetLastBytes(KafkaHeaders.SchemaVersion, out _));
    }

    [Fact]
    public async Task PoisonDeadLetterAttemptsComesFromTheAttemptsHeader()
    {
        var headers = new Headers { { KafkaHeaders.Attempts, Encoding.UTF8.GetBytes("2") } };
        var (dlq, _, _) = await RunPoison(Encoding.UTF8.GetBytes("not-json"), headers);

        Assert.Equal(2, ParseValue(dlq).GetProperty("dead_letter").GetProperty("attempts").GetInt32());
    }

    [Fact]
    public async Task TypedInvalidObjectIsWrappedAsPoisonKeepingItsKeys()
    {
        // A JSON object whose typed member has the wrong type (Go's Decode rejects it) keeps its keys.
        var (dlq, consumer, _) = await RunPoison(Encoding.UTF8.GetBytes("{\"meta\":{\"created_at\":\"2026\"},\"extra\":true}"));

        var value = ParseValue(dlq);
        Assert.True(value.GetProperty("extra").GetBoolean());
        Assert.Equal("poison", value.GetProperty("dead_letter").GetProperty("reason").GetString());
        consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Once);
    }

    [Fact]
    public async Task NonConformantEnvelopeWithThrowingOnErrorStillParksAndCommits()
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

        await new KafkaConsumer(consumer.Object, new Dictionary<string, BabelHandler>(), new KafkaConsumerOptions
        {
            Producer = Producer(captured).Object,
            RetryTopics = Topology(),
            OnError = (_, _, _) => throw new InvalidOperationException("sink down"),
        }).PollAsync();

        Assert.Equal("orders.dlq", Assert.Single(captured).Topic);
        consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Once);
        consumer.Verify(c => c.Seek(It.IsAny<TopicPartitionOffset>()), Times.Never);
    }

    [Fact]
    public async Task NonConformantEnvelopeDlqFailureWithThrowingOnErrorStillSeeksBackAndDoesNotCommit()
    {
        var result = new ConsumeResult<byte[], byte[]>
        {
            Topic = "orders",
            Partition = new Partition(0),
            Offset = new Offset(4),
            Message = new Message<byte[], byte[]> { Value = Encoding.UTF8.GetBytes("not-json"), Headers = new Headers() },
        };
        var consumer = ConsumerWith(result);
        var producer = new Mock<IProducer<byte[], byte[]>>();
        producer
            .Setup(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<byte[], byte[]>>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("broker down"));
        var worker = new KafkaConsumer(consumer.Object, new Dictionary<string, BabelHandler>(), new KafkaConsumerOptions
        {
            Producer = producer.Object,
            RetryTopics = Topology(),
            OnError = (_, _, _) => throw new InvalidOperationException("sink down"),
        });

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() => worker.PollAsync());

        Assert.Equal("broker down", thrown.Message);
        consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Never);
        consumer.Verify(c => c.Seek(It.IsAny<TopicPartitionOffset>()), Times.Once);
    }

    [Fact]
    public async Task ThrowingHandlerWithThrowingOnErrorStillRepublishesToRetryAndCommits()
    {
        var consumer = ConsumerWith(ResultFor(Envelope(0)));
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var handlers = new Dictionary<string, BabelHandler>
        {
            [Urn] = (_, _, _) => throw new InvalidOperationException("boom"),
        };

        await new KafkaConsumer(consumer.Object, handlers, new KafkaConsumerOptions
        {
            Producer = Producer(captured).Object,
            RetryTopics = Topology(),
            MaxTries = 3,
            OnError = (_, _, _) => throw new InvalidOperationException("sink down"),
        }).PollAsync();

        Assert.Equal("orders.retry.1", Assert.Single(captured).Topic);
        consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Once);
        consumer.Verify(c => c.Seek(It.IsAny<TopicPartitionOffset>()), Times.Never);
    }

    [Fact]
    public async Task ExhaustedHandlerWithThrowingOnErrorStillDeadLettersAndCommits()
    {
        var consumer = ConsumerWith(ResultFor(Envelope(2)));
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var handlers = new Dictionary<string, BabelHandler>
        {
            [Urn] = (_, _, _) => throw new InvalidOperationException("boom"),
        };

        await new KafkaConsumer(consumer.Object, handlers, new KafkaConsumerOptions
        {
            Producer = Producer(captured).Object,
            RetryTopics = Topology(),
            MaxTries = 3,
            OnError = (_, _, _) => throw new InvalidOperationException("sink down"),
        }).PollAsync();

        Assert.Equal("orders.dlq", Assert.Single(captured).Topic);
        consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Once);
    }

    [Fact]
    public async Task HandlerFailureWhoseCommitFailsStillReportsTheHandlerErrorAfterSeekingBack()
    {
        var consumer = ConsumerWith(ResultFor(Envelope(0)));
        var order = new List<string>();
        consumer.Setup(c => c.Seek(It.IsAny<TopicPartitionOffset>())).Callback(() => order.Add("seek"));
        consumer.Setup(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()))
            .Throws(new InvalidOperationException("commit down"));
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var handlers = new Dictionary<string, BabelHandler>
        {
            [Urn] = (_, _, _) => throw new InvalidOperationException("boom"),
        };
        var reported = new List<Exception>();

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() => new KafkaConsumer(
            consumer.Object,
            handlers,
            new KafkaConsumerOptions
            {
                Producer = Producer(captured).Object,
                RetryTopics = Topology(),
                MaxTries = 3,
                OnError = (e, _, _) =>
                {
                    order.Add("report");
                    reported.Add(e);
                },
            }).PollAsync());

        Assert.Equal("commit down", thrown.Message);
        Assert.Equal("boom", Assert.Single(reported).Message);
        Assert.Equal(new[] { "seek", "report" }, order);
        consumer.Verify(c => c.Seek(It.IsAny<TopicPartitionOffset>()), Times.Once);
    }

    [Fact]
    public async Task NonConformantEnvelopeWhoseCommitFailsStillReportsTheRejectionAfterSeekingBack()
    {
        var result = new ConsumeResult<byte[], byte[]>
        {
            Topic = "orders",
            Partition = new Partition(0),
            Offset = new Offset(4),
            Message = new Message<byte[], byte[]> { Value = Encoding.UTF8.GetBytes("not-json"), Headers = new Headers() },
        };
        var consumer = ConsumerWith(result);
        var order = new List<string>();
        consumer.Setup(c => c.Seek(It.IsAny<TopicPartitionOffset>())).Callback(() => order.Add("seek"));
        consumer.Setup(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()))
            .Throws(new InvalidOperationException("commit down"));
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var reported = new List<Exception>();

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() => new KafkaConsumer(
            consumer.Object,
            new Dictionary<string, BabelHandler>(),
            new KafkaConsumerOptions
            {
                Producer = Producer(captured).Object,
                RetryTopics = Topology(),
                OnError = (e, _, _) =>
                {
                    order.Add("report");
                    reported.Add(e);
                },
            }).PollAsync());

        Assert.Equal("commit down", thrown.Message);
        Assert.IsType<BabelQueueException>(Assert.Single(reported));
        Assert.Equal(new[] { "seek", "report" }, order);
    }

    [Fact]
    public async Task UnknownUrnFailWithThrowingOnErrorStillRaisesUnknownUrnAndSeeksBack()
    {
        var consumer = ConsumerWith(ResultFor(Envelope(0)));
        var worker = new KafkaConsumer(consumer.Object, new Dictionary<string, BabelHandler>(), new KafkaConsumerOptions
        {
            OnError = (_, _, _) => throw new InvalidOperationException("sink down"),
        });

        await Assert.ThrowsAsync<UnknownUrnException>(() => worker.PollAsync());

        consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Never);
        consumer.Verify(c => c.Seek(It.IsAny<TopicPartitionOffset>()), Times.Once);
    }

    /// <summary>A structurally valid envelope whose <c>data</c> string carries a lone 0xFF byte (invalid UTF-8).</summary>
    private static byte[] EnvelopeBytesWithInvalidUtf8()
    {
        var json = EnvelopeCodec.Encode(Envelope(0));
        var bytes = Encoding.UTF8.GetBytes(json.Replace("\"order_id\":7", "\"order_id\":\"@@\""));
        var at = Array.IndexOf(bytes, (byte)'@');
        Assert.True(at >= 0, "marker not found in encoded envelope");
        var invalid = new byte[bytes.Length - 1];
        Array.Copy(bytes, 0, invalid, 0, at);
        invalid[at] = 0xFF;
        Array.Copy(bytes, at + 2, invalid, at + 1, bytes.Length - at - 2);
        return invalid;
    }

    private static ConsumeResult<byte[], byte[]> ResultWithBody(byte[] body, long offset = 6) => new()
    {
        Topic = "orders",
        Partition = new Partition(0),
        Offset = new Offset(offset),
        Message = new Message<byte[], byte[]> { Key = Encoding.UTF8.GetBytes("k1"), Value = body, Headers = new Headers() },
    };

    [Fact]
    public async Task InvalidUtf8BodyIsWrappedAsPoisonWithReplacementCharsNotDecoded()
    {
        var body = EnvelopeBytesWithInvalidUtf8();
        Assert.Contains((byte)0xFF, body);
        var consumer = ConsumerWith(ResultWithBody(body));
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        Exception? reported = null;
        var calls = 0;
        var handlers = new Dictionary<string, BabelHandler>
        {
            [Urn] = (_, _, _) => { calls++; return Task.CompletedTask; },
        };

        await new KafkaConsumer(consumer.Object, handlers, new KafkaConsumerOptions
        {
            Producer = Producer(captured).Object,
            RetryTopics = Topology(),
            OnError = (e, _, _) => reported = e,
        }).PollAsync();

        Assert.Equal(0, calls);
        Assert.IsType<BabelQueueException>(reported);
        var dlq = Assert.Single(captured);
        Assert.Equal("orders.dlq", dlq.Topic);
        Assert.Equal(Encoding.UTF8.GetBytes("k1"), dlq.Message.Key);
        var value = ParseValue(dlq.Message);
        Assert.Equal("poison", value.GetProperty("dead_letter").GetProperty("reason").GetString());
        // The bad byte is stored as U+FFFD inside the (still valid) object; the handler never saw it.
        Assert.Contains("\uFFFD", value.GetProperty("data").GetProperty("order_id").GetString());
        consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Once);
    }

    [Fact]
    public async Task InvalidUtf8BodyWithThrowingOnErrorStillParksAndCommits()
    {
        var consumer = ConsumerWith(ResultWithBody(EnvelopeBytesWithInvalidUtf8()));
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();

        await new KafkaConsumer(consumer.Object, new Dictionary<string, BabelHandler>(), new KafkaConsumerOptions
        {
            Producer = Producer(captured).Object,
            RetryTopics = Topology(),
            OnError = (_, _, _) => throw new InvalidOperationException("sink down"),
        }).PollAsync();

        Assert.Equal("orders.dlq", Assert.Single(captured).Topic);
        consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Once);
        consumer.Verify(c => c.Seek(It.IsAny<TopicPartitionOffset>()), Times.Never);
    }

    [Fact]
    public async Task InvalidUtf8BodyDlqFailureDoesNotCommitAndSeeksBack()
    {
        var consumer = ConsumerWith(ResultWithBody(EnvelopeBytesWithInvalidUtf8(), 6));
        var calls = 0;
        var handlers = new Dictionary<string, BabelHandler>
        {
            [Urn] = (_, _, _) => { calls++; return Task.CompletedTask; },
        };

        await Assert.ThrowsAsync<ProduceException<byte[], byte[]>>(() =>
            new KafkaConsumer(consumer.Object, handlers,
                new KafkaConsumerOptions { Producer = FailingProducer().Object, RetryTopics = Topology() }).PollAsync());

        Assert.Equal(0, calls);
        consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Never);
        consumer.Verify(
            c => c.Seek(It.Is<TopicPartitionOffset>(t => t.Topic == "orders" && t.Offset.Value == 6)), Times.Once);
    }

    [Fact]
    public async Task InvalidUtf8BodyWithoutDlqTopicIsJustCommitted()
    {
        var consumer = ConsumerWith(ResultWithBody(EnvelopeBytesWithInvalidUtf8()));
        var calls = 0;
        var handlers = new Dictionary<string, BabelHandler>
        {
            [Urn] = (_, _, _) => { calls++; return Task.CompletedTask; },
        };

        await new KafkaConsumer(consumer.Object, handlers).PollAsync();

        Assert.Equal(0, calls);
        consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Once);
    }

    private static ConsumeResult<byte[], byte[]> ResultWithSchemaVersion(string body, string? version)
    {
        var headers = new Headers();
        if (version is not null)
        {
            headers.Add(KafkaHeaders.SchemaVersion, Encoding.UTF8.GetBytes(version));
        }

        return new ConsumeResult<byte[], byte[]>
        {
            Topic = "orders",
            Partition = new Partition(0),
            Offset = new Offset(9),
            Message = new Message<byte[], byte[]> { Value = Encoding.UTF8.GetBytes(body), Headers = headers },
        };
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" \t")]
    [InlineData("1")]
    public async Task AbsentBlankOrCurrentSchemaVersionHeaderDecodesAndRunsHandler(string? version)
    {
        var consumer = ConsumerWith(ResultWithSchemaVersion(EnvelopeCodec.Encode(Envelope(0)), version));
        var calls = 0;
        var handlers = new Dictionary<string, BabelHandler>
        {
            [Urn] = (_, _, _) => { calls++; return Task.CompletedTask; },
        };

        await new KafkaConsumer(consumer.Object, handlers).PollAsync();

        Assert.Equal(1, calls);
        consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Once);
    }

    [Theory]
    [InlineData("2")]
    [InlineData("x")]
    [InlineData(" 1")]
    public async Task UnsupportedSchemaVersionHeaderIsRejectedWithoutDecoding(string version)
    {
        // The body is not valid JSON, but the decoder does not throw on it (it yields an empty envelope that
        // the Accepts() branch would also send to the DLQ with the same settle). The two paths differ only in
        // the OnError message, which is what proves the header gate fired before the decoder ran.
        const string body = "{ this is not json";
        var result = ResultWithSchemaVersion(body, version);
        var consumer = ConsumerWith(result);
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        Exception? reported = null;
        var calls = 0;
        var handlers = new Dictionary<string, BabelHandler>
        {
            [Urn] = (_, _, _) => { calls++; return Task.CompletedTask; },
        };

        await new KafkaConsumer(consumer.Object, handlers, new KafkaConsumerOptions
        {
            Producer = Producer(captured).Object,
            RetryTopics = Topology(),
            OnError = (e, _, _) => reported = e,
        }).PollAsync();

        Assert.Equal(0, calls);
        Assert.NotNull(reported);
        Assert.Contains(KafkaHeaders.SchemaVersion, reported!.Message);
        Assert.Contains($"'{version}'", reported.Message);
        Assert.DoesNotContain("non-conformant", reported.Message);
        var dlq = Assert.Single(captured);
        Assert.Equal("orders.dlq", dlq.Topic);
        Assert.Equal(body, Encoding.UTF8.GetString(dlq.Message.Value));
        consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Once);
    }

    [Theory]
    [InlineData("2")]
    [InlineData("x")]
    public async Task SchemaVersionGateDlqFailureSeeksBackAndDoesNotCommitOrRunHandler(string version)
    {
        var result = ResultWithSchemaVersion(EnvelopeCodec.Encode(Envelope(0)), version);
        var consumer = ConsumerWith(result);
        var producer = new Mock<IProducer<byte[], byte[]>>();
        producer
            .Setup(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<byte[], byte[]>>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new ProduceException<byte[], byte[]>(new Error(ErrorCode.TopicAuthorizationFailed), new DeliveryResult<byte[], byte[]>()));
        var reported = new List<Exception>();
        var calls = 0;
        var handlers = new Dictionary<string, BabelHandler>
        {
            [Urn] = (_, _, _) => { calls++; return Task.CompletedTask; },
        };

        var count = await new KafkaConsumer(consumer.Object, handlers, new KafkaConsumerOptions
        {
            Producer = producer.Object,
            RetryTopics = Topology(),
            PollTimeout = TimeSpan.FromMilliseconds(1),
            OnError = (e, _, _) => reported.Add(e),
        }).PollAsync();

        Assert.Equal(1, count);
        Assert.Equal(0, calls);
        consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Never);
        consumer.Verify(
            c => c.Seek(It.Is<TopicPartitionOffset>(t => t.Topic == "orders" && t.Partition.Value == 0 && t.Offset.Value == 9)),
            Times.Once);
        Assert.Equal(2, reported.Count);
        Assert.IsType<ProduceException<byte[], byte[]>>(reported[1].InnerException);
    }

    [Fact]
    public async Task SchemaVersionGateThrowingOnErrorStillCommitsAfterSuccessfulDeadLetter()
    {
        var result = ResultWithSchemaVersion(EnvelopeCodec.Encode(Envelope(0)), "2");
        var consumer = ConsumerWith(result);
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var calls = 0;
        var handlers = new Dictionary<string, BabelHandler>
        {
            [Urn] = (_, _, _) => { calls++; return Task.CompletedTask; },
        };

        var count = await new KafkaConsumer(consumer.Object, handlers, new KafkaConsumerOptions
        {
            Producer = Producer(captured).Object,
            RetryTopics = Topology(),
            OnError = (_, _, _) => throw new InvalidOperationException("observer exploded"),
        }).PollAsync();

        Assert.Equal(1, count);
        Assert.Equal(0, calls);
        Assert.Single(captured);
        consumer.Verify(c => c.Commit(result), Times.Once);
        consumer.Verify(c => c.Seek(It.IsAny<TopicPartitionOffset>()), Times.Never);
    }

    [Fact]
    public async Task SchemaVersionGateThrowingOnErrorStillSeeksBackAfterDeadLetterFailure()
    {
        var result = ResultWithSchemaVersion(EnvelopeCodec.Encode(Envelope(0)), "2");
        var consumer = ConsumerWith(result);
        var producer = new Mock<IProducer<byte[], byte[]>>();
        producer
            .Setup(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<byte[], byte[]>>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new ProduceException<byte[], byte[]>(new Error(ErrorCode.TopicAuthorizationFailed), new DeliveryResult<byte[], byte[]>()));
        var calls = 0;
        var handlers = new Dictionary<string, BabelHandler>
        {
            [Urn] = (_, _, _) => { calls++; return Task.CompletedTask; },
        };

        var count = await new KafkaConsumer(consumer.Object, handlers, new KafkaConsumerOptions
        {
            Producer = producer.Object,
            RetryTopics = Topology(),
            PollTimeout = TimeSpan.FromMilliseconds(1),
            OnError = (_, _, _) => throw new InvalidOperationException("observer exploded"),
        }).PollAsync();

        Assert.Equal(1, count);
        Assert.Equal(0, calls);
        consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Never);
        consumer.Verify(
            c => c.Seek(It.Is<TopicPartitionOffset>(t => t.Topic == "orders" && t.Partition.Value == 0 && t.Offset.Value == 9)),
            Times.Once);
    }

    [Fact]
    public async Task SchemaVersionGateSeekFailureSurfacesBothErrorsAndDoesNotCommit()
    {
        var result = ResultWithSchemaVersion(EnvelopeCodec.Encode(Envelope(0)), "2");
        var consumer = ConsumerWith(result);
        consumer.Setup(c => c.Seek(It.IsAny<TopicPartitionOffset>())).Throws(new InvalidOperationException("seek failed"));
        var producer = new Mock<IProducer<byte[], byte[]>>();
        producer
            .Setup(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<byte[], byte[]>>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new ProduceException<byte[], byte[]>(new Error(ErrorCode.TopicAuthorizationFailed), new DeliveryResult<byte[], byte[]>()));
        var calls = 0;
        var handlers = new Dictionary<string, BabelHandler>
        {
            [Urn] = (_, _, _) => { calls++; return Task.CompletedTask; },
        };

        var consumerUnderTest = new KafkaConsumer(consumer.Object, handlers, new KafkaConsumerOptions
        {
            Producer = producer.Object,
            RetryTopics = Topology(),
            PollTimeout = TimeSpan.FromMilliseconds(1),
        });

        var error = await Assert.ThrowsAsync<BabelQueueException>(() => consumerUnderTest.PollAsync());

        var inner = Assert.IsType<AggregateException>(error.InnerException);
        Assert.IsType<ProduceException<byte[], byte[]>>(inner.InnerExceptions[0]);
        Assert.IsType<InvalidOperationException>(inner.InnerExceptions[1]);
        Assert.Equal(0, calls);
        consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Never);
    }

    // ---- restart / redelivery: an unsettled record must never be skipped by a later commit ----

    /// <summary>
    /// A two-record partition log behind a mocked <see cref="IConsumer{TKey,TValue}"/> that models Kafka's
    /// cumulative per-partition semantics: <c>Consume</c> advances the in-memory position, <c>Seek</c> rewinds
    /// it and <c>Commit</c> stores offset + 1. A "restart" resets the position to the committed offset.
    /// </summary>
    private sealed class PartitionLog
    {
        private readonly List<ConsumeResult<byte[], byte[]>> _records;

        public PartitionLog(params ConsumeResult<byte[], byte[]>[] records)
        {
            _records = new List<ConsumeResult<byte[], byte[]>>(records);
            Mock.Setup(c => c.Consume(It.IsAny<TimeSpan>())).Returns(() =>
                Position < _records.Count ? _records[Position++] : null!);
            Mock.Setup(c => c.Seek(It.IsAny<TopicPartitionOffset>())).Callback<TopicPartitionOffset>(t =>
                Position = _records.FindIndex(r => r.Offset == t.Offset));
            Mock.Setup(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()))
                .Callback<ConsumeResult<byte[], byte[]>>(r => Committed = (int)r.Offset.Value + 1);
        }

        public Mock<IConsumer<byte[], byte[]>> Mock { get; } = new();

        public int Position { get; private set; }

        /// <summary>Index of the first record a restarted consumer would receive.</summary>
        public int Committed { get; private set; }

        public void Restart() => Position = Committed;
    }

    private static ConsumeResult<byte[], byte[]> Record(long offset, string body) => new()
    {
        Topic = "orders",
        Partition = new Partition(0),
        Offset = new Offset(offset),
        Message = new Message<byte[], byte[]> { Value = Encoding.UTF8.GetBytes(body), Headers = new Headers() },
    };

    private static Mock<IProducer<byte[], byte[]>> FailingProducer()
    {
        var producer = new Mock<IProducer<byte[], byte[]>>();
        producer
            .Setup(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<byte[], byte[]>>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new ProduceException<byte[], byte[]>(new Error(ErrorCode.TopicAuthorizationFailed), new DeliveryResult<byte[], byte[]>()));
        return producer;
    }

    [Fact]
    public async Task PoisonDlqFailureSeeksBackSoARestartedLoopRedeliversTheSameRecord()
    {
        var log = new PartitionLog(Record(0, "not-json"), Record(1, EnvelopeCodec.Encode(Envelope(0))));
        var handled = 0;
        var handlers = new Dictionary<string, BabelHandler> { [Urn] = (_, _, _) => { handled++; return Task.CompletedTask; } };
        var options = new KafkaConsumerOptions { Producer = FailingProducer().Object, RetryTopics = Topology() };

        await Assert.ThrowsAsync<ProduceException<byte[], byte[]>>(
            () => new KafkaConsumer(log.Mock.Object, handlers, options).PollAsync());

        // Restarting the loop on the same consumer must hand out the poison record again, not record 1.
        Assert.Equal(0, log.Position);
        Assert.Equal(0, log.Committed);
        log.Mock.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Never);

        // Once the DLQ recovers the poison record is parked and only then the stream moves on.
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var recovered = new KafkaConsumer(log.Mock.Object, handlers,
            new KafkaConsumerOptions { Producer = Producer(captured).Object, RetryTopics = Topology() });
        await recovered.PollAsync();
        await recovered.PollAsync();
        Assert.Equal("orders.dlq", Assert.Single(captured).Topic);
        Assert.Equal(1, handled);
        Assert.Equal(2, log.Committed);
    }

    [Fact]
    public async Task HandlerRetryRepublishFailureSeeksBackAndDoesNotLetTheNextCommitSkipTheRecord()
    {
        var log = new PartitionLog(Record(0, EnvelopeCodec.Encode(Envelope(0))), Record(1, EnvelopeCodec.Encode(Envelope(0))));
        var attempts = new List<int>();
        var handlers = new Dictionary<string, BabelHandler>
        {
            [Urn] = (_, _, _) => { attempts.Add(attempts.Count); return attempts.Count == 1 ? throw new InvalidOperationException("boom") : Task.CompletedTask; },
        };

        await Assert.ThrowsAsync<ProduceException<byte[], byte[]>>(() =>
            new KafkaConsumer(log.Mock.Object, handlers,
                new KafkaConsumerOptions { Producer = FailingProducer().Object, RetryTopics = Topology() }).PollAsync());

        Assert.Equal(0, log.Position);
        Assert.Equal(0, log.Committed);

        // The crashed loop is restarted: the same record is processed again before anything is committed.
        log.Restart();
        var restarted = new KafkaConsumer(log.Mock.Object, handlers);
        await restarted.PollAsync();
        Assert.Equal(2, attempts.Count);
        Assert.Equal(1, log.Committed);
    }

    [Fact]
    public async Task UnknownUrnFailSeeksBackSoTheRecordRedeliversOnTheNextPoll()
    {
        var log = new PartitionLog(Record(0, EnvelopeCodec.Encode(Envelope(0))));
        var worker = new KafkaConsumer(log.Mock.Object, new Dictionary<string, BabelHandler>());

        await Assert.ThrowsAsync<UnknownUrnException>(() => worker.PollAsync());

        Assert.Equal(0, log.Position);
        log.Mock.Verify(c => c.Seek(It.Is<TopicPartitionOffset>(t => t.Offset.Value == 0)), Times.Once);
        log.Mock.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Never);
    }

    [Fact]
    public async Task UnknownUrnDeadLetterFailureSeeksBackAndDoesNotCommit()
    {
        var log = new PartitionLog(Record(0, EnvelopeCodec.Encode(Envelope(0))));
        var worker = new KafkaConsumer(log.Mock.Object, new Dictionary<string, BabelHandler>(), new KafkaConsumerOptions
        {
            Producer = FailingProducer().Object,
            RetryTopics = Topology(),
            UnknownUrn = UnknownUrnStrategy.DeadLetter,
        });

        await Assert.ThrowsAsync<ProduceException<byte[], byte[]>>(() => worker.PollAsync());

        Assert.Equal(0, log.Position);
        Assert.Equal(0, log.Committed);
        log.Mock.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Never);
    }

    [Fact]
    public async Task SettlementFailureWithFailingSeekSurfacesBothErrorsAndDoesNotCommit()
    {
        var log = new PartitionLog(Record(0, "not-json"));
        log.Mock.Setup(c => c.Seek(It.IsAny<TopicPartitionOffset>())).Throws(new InvalidOperationException("seek failed"));
        var worker = new KafkaConsumer(log.Mock.Object, new Dictionary<string, BabelHandler>(),
            new KafkaConsumerOptions { Producer = FailingProducer().Object, RetryTopics = Topology() });

        var error = await Assert.ThrowsAsync<BabelQueueException>(() => worker.PollAsync());

        var inner = Assert.IsType<AggregateException>(error.InnerException);
        Assert.IsType<ProduceException<byte[], byte[]>>(inner.InnerExceptions[0]);
        Assert.IsType<InvalidOperationException>(inner.InnerExceptions[1]);
        log.Mock.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Never);
    }

    [Fact]
    public async Task CancellationDuringSettlementSeeksBackInsteadOfLeavingThePositionAdvanced()
    {
        var log = new PartitionLog(Record(0, "not-json"));
        var producer = new Mock<IProducer<byte[], byte[]>>();
        producer
            .Setup(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<byte[], byte[]>>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new OperationCanceledException());
        var worker = new KafkaConsumer(log.Mock.Object, new Dictionary<string, BabelHandler>(),
            new KafkaConsumerOptions { Producer = producer.Object, RetryTopics = Topology() });

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => worker.PollAsync());

        Assert.Equal(0, log.Position);
        log.Mock.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Never);
    }

    [Fact]
    public async Task NullValuedSchemaVersionHeaderIsTreatedAsAbsent()
    {
        var result = ResultWithSchemaVersion(EnvelopeCodec.Encode(Envelope(0)), null);
        result.Message.Headers.Add(KafkaHeaders.SchemaVersion, null);
        var consumer = ConsumerWith(result);
        var calls = 0;
        var handlers = new Dictionary<string, BabelHandler>
        {
            [Urn] = (_, _, _) => { calls++; return Task.CompletedTask; },
        };

        await new KafkaConsumer(consumer.Object, handlers).PollAsync();

        Assert.Equal(1, calls);
        consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Once);
    }

    [Fact]
    public async Task PollReturnsZeroOnTimeout()
    {
        var consumer = new Mock<IConsumer<byte[], byte[]>>();
        consumer.Setup(c => c.Consume(It.IsAny<TimeSpan>())).Returns((ConsumeResult<byte[], byte[]>)null!);
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
