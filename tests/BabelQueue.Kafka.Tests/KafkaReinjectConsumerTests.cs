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

/// <summary>ADR-0034 §5–§7 re-inject consumer: A6 wait target, A7 header policy, byte fidelity, ack-then-commit.</summary>
public sealed class KafkaReinjectConsumerTests
{
    private static readonly DateTimeOffset Now = new(2026, 10, 23, 12, 0, 0, TimeSpan.Zero);

    private static byte[] Utf8(string value) => Encoding.UTF8.GetBytes(value);

    private static ConsumeResult<byte[], byte[]> RetryRecord(
        DateTimeOffset recordTimestamp,
        string topic = "orders.retry.1",
        long offset = 7,
        byte[]? key = null,
        byte[]? value = null,
        Headers? headers = null,
        TimestampType timestampType = TimestampType.LogAppendTime) => new()
    {
        Topic = topic,
        Partition = new Partition(2),
        Offset = new Offset(offset),
        Message = new Message<byte[], byte[]>
        {
            Key = key!,
            Value = value ?? Utf8("{\"job\":\"urn:babel:orders:created\",\"data\":{\"n\":1},\"meta\":{}}"),
            Timestamp = new Timestamp(recordTimestamp.UtcDateTime, timestampType),
            Headers = headers ?? new Headers
            {
                { KafkaHeaders.Delay, Utf8("5000") },
                { KafkaHeaders.OriginalTopic, Utf8("orders") },
                { KafkaHeaders.Attempts, Utf8("1") },
                { KafkaHeaders.Job, Utf8("urn:babel:orders:created") },
                { "traceparent", Utf8("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01") },
            },
        },
    };

    private sealed class Harness
    {
        private readonly List<ConsumeResult<byte[], byte[]>> _queue;
        private int _next;

        public Harness(Func<DateTimeOffset> clock, params ConsumeResult<byte[], byte[]>[] records)
        {
            _queue = records.ToList();
            Consumer.Setup(c => c.Consume(It.IsAny<TimeSpan>()))
                .Returns(() => _next < _queue.Count ? _queue[_next++] : null!);
            Consumer.Setup(c => c.Pause(It.IsAny<IEnumerable<TopicPartition>>()))
                .Callback<IEnumerable<TopicPartition>>(p => Paused.AddRange(p));
            Consumer.Setup(c => c.Resume(It.IsAny<IEnumerable<TopicPartition>>()))
                .Callback<IEnumerable<TopicPartition>>(p => Resumed.AddRange(p));
            Consumer.Setup(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()))
                .Callback<ConsumeResult<byte[], byte[]>>(r => Events.Add($"commit:{r.Offset.Value}"));
            Producer.Setup(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<byte[], byte[]>>(), It.IsAny<CancellationToken>()))
                .Returns<string, Message<byte[], byte[]>, CancellationToken>((t, m, _) =>
                {
                    Events.Add("produce:" + t);
                    if (FailProduce > 0)
                    {
                        FailProduce--;
                        throw new ProduceException<byte[], byte[]>(
                            new Error(ErrorCode.Local_MsgTimedOut), new DeliveryResult<byte[], byte[]>());
                    }
                    Produced.Add((t, m));
                    return Task.FromResult(new DeliveryResult<byte[], byte[]>());
                });
            Reinject = new KafkaReinjectConsumer(Consumer.Object, new KafkaReinjectOptions
            {
                Producer = Producer.Object,
                Clock = clock,
                PollTimeout = TimeSpan.FromMilliseconds(10),
                RetryDelay = TimeSpan.FromSeconds(2),
                OnError = (e, _) => Errors.Add(e),
            });
        }

        public Mock<IConsumer<byte[], byte[]>> Consumer { get; } = new();

        public Mock<IProducer<byte[], byte[]>> Producer { get; } = new();

        public KafkaReinjectConsumer Reinject { get; }

        public List<TopicPartition> Paused { get; } = new();

        public List<TopicPartition> Resumed { get; } = new();

        public List<(string Topic, Message<byte[], byte[]> Message)> Produced { get; } = new();

        public List<string> Events { get; } = new();

        public List<Exception> Errors { get; } = new();

        public int FailProduce { get; set; }
    }

    [Fact]
    public async Task ARecordOlderThanItsTierIsCarriedWithoutWaiting()
    {
        // A6 clamp: timestamp + 5s is long past, so the target is "now" — no pause outlives the poll.
        var h = new Harness(() => Now, RetryRecord(Now.AddMinutes(-10)));

        var done = await h.Reinject.PollAsync();

        Assert.Equal(1, done);
        Assert.Equal(0, h.Reinject.HeldCount);
        Assert.Equal("orders", Assert.Single(h.Produced).Topic);
        Assert.Equal(new[] { "produce:orders", "commit:7" }, h.Events);
        Assert.Single(h.Resumed);
    }

    [Fact]
    public async Task AFreshRecordWaitsItsTierByPausingThePartitionThenIsCarried()
    {
        var clock = Now;
        var h = new Harness(() => clock, RetryRecord(Now.AddSeconds(-1))); // due in 4s

        Assert.Equal(0, await h.Reinject.PollAsync());
        Assert.Equal(1, h.Reinject.HeldCount);
        Assert.Single(h.Paused);
        Assert.Empty(h.Produced);
        h.Consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Never);

        clock = Now.AddSeconds(3);
        Assert.Equal(0, await h.Reinject.PollAsync());
        Assert.Empty(h.Produced);

        clock = Now.AddSeconds(4);
        Assert.Equal(1, await h.Reinject.PollAsync());
        Assert.Equal(0, h.Reinject.HeldCount);
        Assert.Equal(new[] { "produce:orders", "commit:7" }, h.Events);
        Assert.Equal(h.Paused, h.Resumed);
    }

    [Fact]
    public async Task ATimestampFromTheFutureIsClampedToNowPlusTier()
    {
        // A producer-clock timestamp far ahead of the broker must not park the record beyond one tier.
        var clock = Now;
        var h = new Harness(() => clock, RetryRecord(Now.AddHours(5)));

        await h.Reinject.PollAsync();
        Assert.Equal(1, h.Reinject.HeldCount);

        clock = Now.AddSeconds(5);
        Assert.Equal(1, await h.Reinject.PollAsync());
    }

    [Fact]
    public async Task ARecordWithoutATimestampWaitsTheFullTier()
    {
        var clock = Now;
        var h = new Harness(() => clock, RetryRecord(Now.AddHours(-1), timestampType: TimestampType.NotAvailable));

        await h.Reinject.PollAsync();
        Assert.Equal(1, h.Reinject.HeldCount);

        clock = Now.AddSeconds(5);
        Assert.Equal(1, await h.Reinject.PollAsync());
    }

    [Fact]
    public async Task ValueAndKeyAreCarriedByteForByteAndOnlyTheTwoRoutingHeadersAreDropped()
    {
        // Non-canonical JSON on purpose (spacing, key order, escapes, a lone-surrogate escape): never re-encoded.
        var body = Utf8("{ \"meta\" : {\"attempts\":1} ,\"data\":{\"s\":\"\\ud800 \\u00e9\"},\"job\":\"urn:babel:orders:created\" }");
        var key = new byte[] { 0x00, 0xFF, 0x10, 0x7F };
        var h = new Harness(() => Now, RetryRecord(Now.AddMinutes(-1), key: key, value: body));

        await h.Reinject.PollAsync();

        var (topic, message) = Assert.Single(h.Produced);
        Assert.Equal("orders", topic);
        Assert.Equal(body, message.Value);
        Assert.Equal(key, message.Key);

        var names = message.Headers.Select(x => x.Key).ToList();
        Assert.DoesNotContain(KafkaHeaders.Delay, names);
        Assert.DoesNotContain(KafkaHeaders.OriginalTopic, names);
        Assert.Equal("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", KafkaHeaders.GetString(message.Headers, "traceparent"));
        Assert.Equal("1", KafkaHeaders.GetString(message.Headers, KafkaHeaders.Attempts));
        Assert.Equal("urn:babel:orders:created", KafkaHeaders.GetString(message.Headers, KafkaHeaders.Job));
        Assert.Equal(3, names.Count);
    }

    [Fact]
    public async Task AFailedWriteIsNotCommittedAndIsWrittenAgainAfterTheRetryDelay()
    {
        var clock = Now;
        var h = new Harness(() => clock, RetryRecord(Now.AddMinutes(-1))) { FailProduce = 1 };

        Assert.Equal(0, await h.Reinject.PollAsync());
        Assert.Equal(1, h.Reinject.HeldCount);
        Assert.Single(h.Errors);
        Assert.Equal(new[] { "produce:orders" }, h.Events);
        h.Consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Never);
        Assert.Empty(h.Resumed);

        clock = Now.AddSeconds(1); // RetryDelay (2s) has not passed
        await h.Reinject.PollAsync();
        Assert.Equal(new[] { "produce:orders" }, h.Events);

        clock = Now.AddSeconds(2);
        Assert.Equal(1, await h.Reinject.PollAsync());
        Assert.Equal(new[] { "produce:orders", "produce:orders", "commit:7" }, h.Events);
        Assert.Equal(0, h.Reinject.HeldCount);
    }

    [Fact]
    public async Task WithoutTheOriginalTopicHeaderTheTierTopicNameGivesTheWorkTopic()
    {
        var headers = new Headers { { KafkaHeaders.Delay, Utf8("5000") } };
        var h = new Harness(() => Now, RetryRecord(Now.AddMinutes(-1), topic: "billing.eu.retry.3", headers: headers));

        await h.Reinject.PollAsync();

        Assert.Equal("billing.eu", Assert.Single(h.Produced).Topic);
    }

    [Fact]
    public async Task ARecordThatNamesNoTargetIsReportedAndNeverCommitted()
    {
        var headers = new Headers { { KafkaHeaders.Delay, Utf8("5000") } };
        var h = new Harness(() => Now, RetryRecord(Now.AddMinutes(-1), topic: "not-a-tier", headers: headers));

        Assert.Equal(0, await h.Reinject.PollAsync());

        Assert.IsType<BabelQueueException>(Assert.Single(h.Errors));
        Assert.Empty(h.Produced);
        h.Consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Never);
    }

    [Fact]
    public async Task AnUndecodableBodyIsCarriedAsIs()
    {
        var body = Utf8("not json at all \u00e9");
        var h = new Harness(() => Now, RetryRecord(Now.AddMinutes(-1), value: body));

        Assert.Equal(1, await h.Reinject.PollAsync());

        Assert.Equal(body, Assert.Single(h.Produced).Message.Value);
        Assert.Empty(h.Errors);
    }

    [Fact]
    public void TheProducerIsRequired()
    {
        Assert.Throws<BabelQueueException>(
            () => new KafkaReinjectConsumer(new Mock<IConsumer<byte[], byte[]>>().Object, new KafkaReinjectOptions()));
    }
}
