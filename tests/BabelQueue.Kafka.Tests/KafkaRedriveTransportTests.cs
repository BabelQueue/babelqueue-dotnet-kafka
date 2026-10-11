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
/// A DLQ redrive over Kafka (ADR-0026) carries the DLQ record's key byte-for-byte on the redrive write and on
/// the restore write back to the DLQ: a null key stays null, an empty key stays empty, none is synthesised.
/// </summary>
public sealed class KafkaRedriveTransportTests
{
    private const string Urn = "urn:babel:orders:created";

    public static IEnumerable<object?[]> Keys()
    {
        yield return new object?[] { Encoding.UTF8.GetBytes("order-7") };
        yield return new object?[] { null };
        yield return new object?[] { Array.Empty<byte>() };
    }

    private static string DeadBody()
    {
        var env = EnvelopeCodec.Make(Urn, new Dictionary<string, object?> { ["order_id"] = 7 }, "orders", "trace-1");
        return EnvelopeCodec.Encode(DeadLetters.Annotate(env with { Attempts = 3 }, "failed", "orders"));
    }

    private static ConsumeResult<byte[], byte[]> DlqRecord(byte[]? key, string body, long offset = 5) => new()
    {
        Topic = "orders.dlq",
        Partition = new Partition(0),
        Offset = new Offset(offset),
        Message = new Message<byte[], byte[]> { Key = key!, Value = Encoding.UTF8.GetBytes(body), Headers = new Headers() },
    };

    private static Mock<IConsumer<byte[], byte[]>> ConsumerWith(params ConsumeResult<byte[], byte[]>[] records)
    {
        var queue = new Queue<ConsumeResult<byte[], byte[]>>(records);
        var consumer = new Mock<IConsumer<byte[], byte[]>>();
        consumer.Setup(c => c.Consume(It.IsAny<TimeSpan>()))
            .Returns(() => queue.Count > 0 ? queue.Dequeue() : null!);
        return consumer;
    }

    private static IProducer<byte[], byte[]> Producer(List<(string Topic, Message<byte[], byte[]> Message)> captured, string? failTopic = null)
    {
        var producer = new Mock<IProducer<byte[], byte[]>>();
        producer
            .Setup(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<byte[], byte[]>>(), It.IsAny<CancellationToken>()))
            .Returns<string, Message<byte[], byte[]>, CancellationToken>((t, m, _) =>
            {
                if (t == failTopic)
                {
                    throw new InvalidOperationException("publish refused");
                }

                captured.Add((t, m));
                return Task.FromResult(new DeliveryResult<byte[], byte[]>());
            });
        return producer.Object;
    }

    private static void AssertKey(byte[]? expected, Message<byte[], byte[]> sent)
    {
        if (expected is null)
        {
            Assert.Null(sent.Key);
        }
        else
        {
            Assert.NotNull(sent.Key);
            Assert.Equal(expected, sent.Key);
        }
    }

    [Theory]
    [MemberData(nameof(Keys))]
    public async Task RedrivenRecordKeepsTheKeyByteForByte(byte[]? key)
    {
        var record = DlqRecord(key, DeadBody());
        var consumer = ConsumerWith(record);
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var transport = new KafkaRedriveTransport(consumer.Object, Producer(captured));

        var result = await Redrive.RedriveAsync(transport, "orders.dlq", new Redrive.Options());

        Assert.Equal(1, result.Redriven);
        var (topic, sent) = Assert.Single(captured);
        Assert.Equal("orders", topic);
        AssertKey(key, sent);
        var env = EnvelopeCodec.Decode(Encoding.UTF8.GetString(sent.Value));
        Assert.Null(env.DeadLetter);
        Assert.Equal(0, env.Attempts);
        Assert.Equal(Urn, KafkaHeaders.GetString(sent.Headers, KafkaHeaders.Job));
        Assert.Equal(0, KafkaHeaders.GetInt(sent.Headers, KafkaHeaders.Attempts, -1));
        consumer.Verify(c => c.Commit(record), Times.Once);
    }

    [Theory]
    [MemberData(nameof(Keys))]
    public async Task RestoreWriteBackToTheDlqKeepsTheKey(byte[]? key)
    {
        var consumer = ConsumerWith(DlqRecord(key, DeadBody()));
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var transport = new KafkaRedriveTransport(consumer.Object, Producer(captured));

        var result = await Redrive.RedriveAsync(transport, "orders.dlq", new Redrive.Options(DryRun: true));

        Assert.Equal(1, result.Skipped);
        var (topic, sent) = Assert.Single(captured);
        Assert.Equal("orders.dlq", topic);
        AssertKey(key, sent);
    }

    [Theory]
    [MemberData(nameof(Keys))]
    public async Task FailedRedriveWriteRestoresToTheDlqWithTheKey(byte[]? key)
    {
        var consumer = ConsumerWith(DlqRecord(key, DeadBody()));
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var transport = new KafkaRedriveTransport(consumer.Object, Producer(captured, failTopic: "orders"));

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => Redrive.RedriveAsync(transport, "orders.dlq", new Redrive.Options()));

        var (topic, sent) = Assert.Single(captured);
        Assert.Equal("orders.dlq", topic);
        AssertKey(key, sent);
    }

    [Theory]
    [MemberData(nameof(Keys))]
    public async Task UndecodableBodyIsRestoredVerbatimWithItsKey(byte[]? key)
    {
        var consumer = ConsumerWith(DlqRecord(key, "not json at all"));
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var transport = new KafkaRedriveTransport(consumer.Object, Producer(captured));

        await Redrive.RedriveAsync(transport, "orders.dlq", new Redrive.Options());

        var (topic, sent) = Assert.Single(captured);
        Assert.Equal("orders.dlq", topic);
        AssertKey(key, sent);
        Assert.Equal("not json at all", Encoding.UTF8.GetString(sent.Value));
        Assert.Empty(sent.Headers);
    }

    [Theory]
    [MemberData(nameof(Keys))]
    public async Task InvalidUtf8RecordIsRestoredByteForByteWithItsKeyAndHeaders(byte[]? key)
    {
        var poison = new byte[] { 0xFF, 0xFE, 0x01 };
        var headers = new Headers
        {
            { "traceparent", Encoding.UTF8.GetBytes("00-abc-def-01") },
            { "bq-schema-version", Encoding.UTF8.GetBytes("2") },
        };
        var record = new ConsumeResult<byte[], byte[]>
        {
            Topic = "orders.dlq",
            Partition = new Partition(0),
            Offset = new Offset(5),
            Message = new Message<byte[], byte[]> { Key = key!, Value = poison, Headers = headers },
        };
        var consumer = ConsumerWith(record);
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var transport = new KafkaRedriveTransport(consumer.Object, Producer(captured));
        var before = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        var result = await Redrive.RedriveAsync(transport, "orders.dlq", new Redrive.Options());

        Assert.Equal(0, result.Redriven);
        var (topic, sent) = Assert.Single(captured);
        Assert.Equal("orders.dlq", topic);
        Assert.Equal(poison, sent.Value);
        AssertKey(key, sent);
        Assert.Equal("00-abc-def-01", KafkaHeaders.GetString(sent.Headers, "traceparent"));
        Assert.Equal("2", KafkaHeaders.GetString(sent.Headers, "bq-schema-version"));
        Assert.True(sent.Timestamp.UnixTimestampMs >= before);
        // Committed only AFTER the verbatim restore was acknowledged.
        consumer.Verify(c => c.Commit(record), Times.Once);
    }

    [Fact]
    public async Task NullValueRecordIsRestoredAsNullValue()
    {
        var record = DlqRecord(Encoding.UTF8.GetBytes("k"), "x");
        record.Message.Value = null!;
        var consumer = ConsumerWith(record);
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var transport = new KafkaRedriveTransport(consumer.Object, Producer(captured));

        await Redrive.RedriveAsync(transport, "orders.dlq", new Redrive.Options());

        var (topic, sent) = Assert.Single(captured);
        Assert.Equal("orders.dlq", topic);
        Assert.Null(sent.Value);
    }

    [Fact]
    public async Task FailedRestoreOfInvalidUtf8RecordDoesNotCommit()
    {
        var record = DlqRecord(null, "x");
        record.Message.Value = new byte[] { 0xFF, 0xFE, 0x01 };
        var consumer = ConsumerWith(record);
        var transport = new KafkaRedriveTransport(consumer.Object, Producer(new(), failTopic: "orders.dlq"));

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => Redrive.RedriveAsync(transport, "orders.dlq", new Redrive.Options()));

        consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Never);
    }

    [Fact]
    public async Task RestoreIsStampedNowNotWithTheEnvelopeCreatedAt()
    {
        var body = DeadBody();
        var consumer = ConsumerWith(DlqRecord(null, body));
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var transport = new KafkaRedriveTransport(consumer.Object, Producer(captured));
        var before = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        await Redrive.RedriveAsync(transport, "orders.dlq", new Redrive.Options(DryRun: true));

        var (_, sent) = Assert.Single(captured);
        Assert.Equal(body, Encoding.UTF8.GetString(sent.Value));
        Assert.True(sent.Timestamp.UnixTimestampMs >= before);
    }

    [Fact]
    public async Task PartitionEofMarkerDoesNotEndTheDrain()
    {
        var eof = new ConsumeResult<byte[], byte[]> { Topic = "orders.dlq", Partition = new Partition(0), IsPartitionEOF = true };
        var consumer = ConsumerWith(eof, DlqRecord(null, DeadBody(), offset: 9));
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var transport = new KafkaRedriveTransport(consumer.Object, Producer(captured), TimeSpan.FromSeconds(5));

        var result = await Redrive.RedriveAsync(transport, "orders.dlq", new Redrive.Options());

        Assert.Equal(1, result.Redriven);
    }

    [Fact]
    public async Task PlainPublishCarriesNoKey()
    {
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var transport = new KafkaRedriveTransport(ConsumerWith().Object, Producer(captured));

        await transport.PublishAsync("orders", DeadBody());

        var (_, sent) = Assert.Single(captured);
        Assert.Null(sent.Key);
    }

    [Fact]
    public async Task EmptyDlqPopsNull()
    {
        var transport = new KafkaRedriveTransport(ConsumerWith().Object, Producer(new()));

        Assert.Null(await transport.PopAsync("orders.dlq"));
        var result = await Redrive.RedriveAsync(transport, "orders.dlq", new Redrive.Options());
        Assert.Equal(0, result.Redriven);
        Assert.Equal(0, result.Skipped);
    }
}
