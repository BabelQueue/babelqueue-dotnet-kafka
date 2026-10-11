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

    private static ConsumeResult<byte[], byte[]> DlqRecord(byte[]? key, string body, long offset = 5, int partition = 0) => new()
    {
        Topic = "orders.dlq",
        Partition = new Partition(partition),
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

    [Fact]
    public async Task FailedRestoreSeeksBackNeverCommitsAndTheSameRecordIsSeenAgain()
    {
        var record = DlqRecord(Encoding.UTF8.GetBytes("order-7"), DeadBody(), offset: 11);
        var queue = new Queue<ConsumeResult<byte[], byte[]>>(new[] { record });
        var consumer = new Mock<IConsumer<byte[], byte[]>>();
        consumer.Setup(c => c.Consume(It.IsAny<TimeSpan>()))
            .Returns(() => queue.Count > 0 ? queue.Dequeue() : null!);
        // Seek(tpo) repositions the consumer: the same record is delivered again on the next poll.
        consumer.Setup(c => c.Seek(It.IsAny<TopicPartitionOffset>()))
            .Callback<TopicPartitionOffset>(_ => queue.Enqueue(record));

        // Round 1: the restore write back to the DLQ fails (DryRun: no work-topic write is attempted).
        var failing = new KafkaRedriveTransport(consumer.Object, Producer(new(), failTopic: "orders.dlq"));
        var ex = await Assert.ThrowsAnyAsync<Exception>(
            () => Redrive.RedriveAsync(failing, "orders.dlq", new Redrive.Options(DryRun: true)));
        Assert.NotNull(ex);

        consumer.Verify(c => c.Seek(record.TopicPartitionOffset), Times.Once);
        consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Never);

        // Round 2: the DLQ is writable again; the very same record is seen once more and only then committed.
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var healthy = new KafkaRedriveTransport(consumer.Object, Producer(captured));
        var result = await Redrive.RedriveAsync(healthy, "orders.dlq", new Redrive.Options(DryRun: true));

        Assert.Equal(1, result.Skipped);
        Assert.Single(captured);
        consumer.Verify(c => c.Commit(record), Times.Once);
    }

    [Fact]
    public async Task FailedSeekIsReportedTogetherWithTheRestoreErrorAndNeverCommits()
    {
        var record = DlqRecord(null, DeadBody());
        var consumer = ConsumerWith(record);
        consumer.Setup(c => c.Seek(It.IsAny<TopicPartitionOffset>())).Throws(new InvalidOperationException("seek refused"));
        var transport = new KafkaRedriveTransport(consumer.Object, Producer(new(), failTopic: "orders.dlq"));

        var ex = await Assert.ThrowsAsync<AggregateException>(
            () => Redrive.RedriveAsync(transport, "orders.dlq", new Redrive.Options(DryRun: true)));

        Assert.Contains(ex.InnerExceptions, e => e.Message == "publish refused");
        Assert.Contains(ex.InnerExceptions, e => e.Message == "seek refused");
        consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Never);
    }

    // A consumer over a small multi-partition log: it serves each partition from its own position in
    // partition order, and Seek moves that partition's position, like a real consumer would.
    private static (Mock<IConsumer<byte[], byte[]>> Consumer, Dictionary<int, long> Positions) PartitionedConsumer(
        params ConsumeResult<byte[], byte[]>[] records)
    {
        var positions = records.Select(r => r.Partition.Value).Distinct()
            .ToDictionary(p => p, p => records.Where(r => r.Partition.Value == p).Min(r => r.Offset.Value));
        var consumer = new Mock<IConsumer<byte[], byte[]>>();
        consumer.Setup(c => c.Consume(It.IsAny<TimeSpan>())).Returns(() =>
        {
            foreach (var partition in positions.Keys.OrderBy(p => p))
            {
                var next = records.FirstOrDefault(r => r.Partition.Value == partition && r.Offset.Value >= positions[partition]);
                if (next is not null)
                {
                    positions[partition] = next.Offset.Value + 1;
                    return next;
                }
            }

            return null!;
        });
        consumer.Setup(c => c.Seek(It.IsAny<TopicPartitionOffset>()))
            .Callback<TopicPartitionOffset>(tpo => positions[tpo.Partition.Value] = tpo.Offset.Value);
        return (consumer, positions);
    }

    [Fact]
    public async Task FailedRestoreRewindsEveryPartitionWithUnsettledRecordsAndCommitsNothing()
    {
        var p0 = DlqRecord(null, DeadBody(), offset: 5, partition: 0);
        var p1a = DlqRecord(null, DeadBody(), offset: 10, partition: 1);
        var p1b = DlqRecord(null, DeadBody(), offset: 11, partition: 1);
        var (consumer, _) = PartitionedConsumer(p0, p1a, p1b);

        // Round 1: the batch is [P0@5, P1@10, P1@11]; P0's restore write fails.
        var failing = new KafkaRedriveTransport(consumer.Object, Producer(new(), failTopic: "orders.dlq"));
        await Assert.ThrowsAnyAsync<Exception>(
            () => Redrive.RedriveAsync(failing, "orders.dlq", new Redrive.Options(DryRun: true)));

        // Both partitions go back to their first unsettled record; P1 is not committed past.
        consumer.Verify(c => c.Seek(p0.TopicPartitionOffset), Times.Once);
        consumer.Verify(c => c.Seek(p1a.TopicPartitionOffset), Times.Once);
        consumer.Verify(c => c.Seek(It.IsAny<TopicPartitionOffset>()), Times.Exactly(2));
        consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Never);

        // Round 2: all three records are delivered again and committed exactly once each.
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var healthy = new KafkaRedriveTransport(consumer.Object, Producer(captured));
        var result = await Redrive.RedriveAsync(healthy, "orders.dlq", new Redrive.Options(DryRun: true));

        Assert.Equal(3, result.Skipped);
        Assert.Equal(3, captured.Count);
        consumer.Verify(c => c.Commit(p0), Times.Once);
        consumer.Verify(c => c.Commit(p1a), Times.Once);
        consumer.Verify(c => c.Commit(p1b), Times.Once);
    }

    [Fact]
    public async Task PublishFailureWithSuccessfulRestoreRewindsOnlyTheRemainingBatch()
    {
        var first = DlqRecord(null, DeadBody(), offset: 5);
        var second = DlqRecord(null, DeadBody(), offset: 6);
        var (consumer, positions) = PartitionedConsumer(first, second);

        // Round 1: the work-topic write fails, the restore to the DLQ succeeds and is committed.
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var failing = new KafkaRedriveTransport(consumer.Object, Producer(captured, failTopic: "orders"));
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => Redrive.RedriveAsync(failing, "orders.dlq", new Redrive.Options()));
        Assert.Equal("publish refused", ex.Message);

        // The committed record is not sought back to (no duplicate); the unprocessed one is.
        consumer.Verify(c => c.Commit(first), Times.Once);
        consumer.Verify(c => c.Seek(second.TopicPartitionOffset), Times.Once);
        consumer.Verify(c => c.Seek(first.TopicPartitionOffset), Times.Never);
        consumer.Verify(c => c.Seek(It.IsAny<TopicPartitionOffset>()), Times.Once);
        Assert.Equal(6, positions[0]);
        Assert.Single(captured, c => c.Topic == "orders.dlq");

        // Round 2: only the second record comes back, and it is redriven.
        var healthy = new KafkaRedriveTransport(consumer.Object, Producer(captured));
        var result = await Redrive.RedriveAsync(healthy, "orders.dlq", new Redrive.Options());

        Assert.Equal(1, result.Redriven);
        Assert.Single(captured, c => c.Topic == "orders.dlq"); // still just the one restore write
        consumer.Verify(c => c.Commit(second), Times.Once);
        consumer.Verify(c => c.Commit(first), Times.Once);
    }

    [Fact]
    public async Task PublishFailureWithSuccessfulRestoreOnManyPartitionsRewindsEachToItsFirstUnsettledOffset()
    {
        var p0a = DlqRecord(null, DeadBody(), offset: 5, partition: 0);
        var p0b = DlqRecord(null, DeadBody(), offset: 6, partition: 0);
        var p1 = DlqRecord(null, DeadBody(), offset: 10, partition: 1);
        var (consumer, positions) = PartitionedConsumer(p0a, p0b, p1);

        // Round 1: batch [P0@5, P0@6, P1@10]; the work-topic write for P0@5 fails, its restore to
        // the DLQ succeeds and is committed.
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var failing = new KafkaRedriveTransport(consumer.Object, Producer(captured, failTopic: "orders"));
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => Redrive.RedriveAsync(failing, "orders.dlq", new Redrive.Options()));
        Assert.Equal("publish refused", ex.Message);

        // The committed partition goes to offset + 1, the other to its first unsettled offset;
        // nothing is sought back onto the record that was already restored and committed.
        consumer.Verify(c => c.Commit(p0a), Times.Once);
        consumer.Verify(c => c.Seek(p0b.TopicPartitionOffset), Times.Once);
        consumer.Verify(c => c.Seek(p1.TopicPartitionOffset), Times.Once);
        consumer.Verify(c => c.Seek(p0a.TopicPartitionOffset), Times.Never);
        consumer.Verify(c => c.Seek(It.IsAny<TopicPartitionOffset>()), Times.Exactly(2));
        Assert.Equal(6, positions[0]);
        Assert.Equal(10, positions[1]);
        Assert.Single(captured, c => c.Topic == "orders.dlq");

        // Round 2: exactly the two unprocessed records come back and are redriven; no duplicate
        // of the restored record appears.
        var healthy = new KafkaRedriveTransport(consumer.Object, Producer(captured));
        var result = await Redrive.RedriveAsync(healthy, "orders.dlq", new Redrive.Options());

        Assert.Equal(2, result.Redriven);
        Assert.Single(captured, c => c.Topic == "orders.dlq"); // still just the one restore write
        consumer.Verify(c => c.Commit(p0b), Times.Once);
        consumer.Verify(c => c.Commit(p1), Times.Once);
        consumer.Verify(c => c.Commit(p0a), Times.Once);
    }

    [Fact]
    public async Task CommitFailureAfterSuccessfulRedriveRewindsTheUnsettledRecords()
    {
        var first = DlqRecord(null, DeadBody(), offset: 5, partition: 0);
        var second = DlqRecord(null, DeadBody(), offset: 6, partition: 0);
        var (consumer, positions) = PartitionedConsumer(first, second);
        consumer.Setup(c => c.Commit(first)).Throws(new InvalidOperationException("commit refused"));

        var transport = new KafkaRedriveTransport(consumer.Object, Producer(new()));
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => Redrive.RedriveAsync(transport, "orders.dlq", new Redrive.Options()));
        Assert.Equal("commit refused", ex.Message);

        // The uncommitted first record and the unprocessed second one are both handed back, so a
        // later cumulative commit cannot skip them.
        consumer.Verify(c => c.Seek(first.TopicPartitionOffset), Times.Once);
        Assert.Equal(5, positions[0]);
    }
}
