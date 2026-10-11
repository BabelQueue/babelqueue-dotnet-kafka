using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BabelQueue;
using BabelQueue.Kafka;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Xunit;

namespace BabelQueue.Kafka.Tests;

/// <summary>
/// A <see cref="FactAttribute"/> that is skipped unless <c>BQ_INTEGRATION=1</c>, so the live-broker tests stay out of
/// the normal suite (xUnit 2 has no dynamic skip).
/// </summary>
[AttributeUsage(AttributeTargets.Method)]
internal sealed class KafkaIntegrationFactAttribute : FactAttribute
{
    public KafkaIntegrationFactAttribute()
    {
        if (Environment.GetEnvironmentVariable("BQ_INTEGRATION") != "1")
        {
            Skip = "Live-broker test: set BQ_INTEGRATION=1 (and BQ_KAFKA_BROKERS) to run.";
        }
    }
}

/// <summary>
/// Live-broker integration tests for the Kafka transport (§6) — opt-in. Enable with <c>BQ_INTEGRATION=1</c> and
/// (optionally) <c>BQ_KAFKA_BROKERS</c> (default <c>localhost:9092</c>). The CI <c>integration</c> job runs an
/// <c>apache/kafka</c> KRaft service container. Every test uses unique topic names.
/// </summary>
public sealed class KafkaIntegrationTests
{
    private const string Urn = "urn:babel:orders:created";
    private static readonly TimeSpan Deadline = TimeSpan.FromSeconds(60);

    private static string Brokers
    {
        get
        {
            var value = Environment.GetEnvironmentVariable("BQ_KAFKA_BROKERS");
            return string.IsNullOrWhiteSpace(value) ? "localhost:9092" : value;
        }
    }

    private static async Task CreateTopicsAsync(params string[] topics)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Brokers }).Build();
        var specs = new List<TopicSpecification>();
        foreach (var topic in topics)
        {
            specs.Add(new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 });
        }
        await admin.CreateTopicsAsync(specs).ConfigureAwait(false);
    }

    private static IProducer<byte[], byte[]> NewProducer() =>
        new ProducerBuilder<byte[], byte[]>(new ProducerConfig { BootstrapServers = Brokers, Acks = Acks.All }).Build();

    private static IConsumer<byte[], byte[]> NewConsumer(string group, params string[] topics)
    {
        var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
        {
            BootstrapServers = Brokers,
            GroupId = group,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
        }).Build();
        consumer.Subscribe(topics);
        return consumer;
    }

    private static ConsumeResult<byte[], byte[]> AwaitDlqRecord(string dlq)
    {
        using var reader = NewConsumer("bq-it-dlq-" + Guid.NewGuid(), dlq);
        var stop = DateTime.UtcNow + Deadline;
        while (DateTime.UtcNow < stop)
        {
            var result = reader.Consume(TimeSpan.FromMilliseconds(500));
            if (result?.Message is not null)
            {
                return result;
            }
        }
        throw new Xunit.Sdk.XunitException($"no record reached {dlq} within {Deadline}");
    }

    [KafkaIntegrationFact]
    public async Task HandlerErrorRetriesThenDeadLettersAfterMaxTries()
    {
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var work = "bq-it-orders-" + suffix;
        var topology = RetryTopics.ForTopic(work).Tier(TimeSpan.FromSeconds(1)).Build();
        var retry = topology.Tiers[0].Topic;
        var dlq = topology.DlqTopic!;
        await CreateTopicsAsync(work, retry, dlq);

        var calls = 0;
        using (var producer = NewProducer())
        using (var raw = NewConsumer("bq-it-group-" + suffix, work, retry))
        {
            var handlers = new Dictionary<string, BabelHandler>
            {
                [Urn] = (_, _, _) =>
                {
                    Interlocked.Increment(ref calls);
                    throw new InvalidOperationException("boom");
                },
            };
            var worker = new KafkaConsumer(raw, handlers, new KafkaConsumerOptions
            {
                Producer = producer,
                RetryTopics = topology,
                MaxTries = 3,
                PollTimeout = TimeSpan.FromMilliseconds(500),
            });

            await KafkaPublisher.Create(producer, topology)
                .PublishAsync(Urn, new Dictionary<string, object?> { ["order_id"] = 7 }, "trace-it-1");

            var stop = DateTime.UtcNow + Deadline;
            while (Volatile.Read(ref calls) < 3 && DateTime.UtcNow < stop)
            {
                await worker.PollAsync();
            }
            Assert.Equal(3, calls);
            // One more poll settles the terminal record (the DLQ write is confirmed before it returns).
            await worker.PollAsync();
        }

        var dead = AwaitDlqRecord(dlq);
        var envelope = EnvelopeCodec.Decode(Encoding.UTF8.GetString(dead.Message.Value));
        Assert.NotNull(envelope.DeadLetter);
        Assert.Equal("failed", envelope.DeadLetter!.Reason);
        Assert.Equal(work, envelope.DeadLetter.OriginalQueue);
        Assert.Equal("trace-it-1", envelope.TraceId);
    }

    [KafkaIntegrationFact]
    public async Task SchemaVersionGateRejectionForwardsRawBodyToDlq()
    {
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var work = "bq-it-gate-" + suffix;
        var topology = RetryTopics.ForTopic(work).Tier(TimeSpan.FromSeconds(1)).Build();
        var dlq = topology.DlqTopic!;
        await CreateTopicsAsync(work, topology.Tiers[0].Topic, dlq);

        // Deliberately not a valid v1 envelope: the gate must reject on the header alone (§6.7).
        var body = Encoding.UTF8.GetBytes("{\"job\":\"urn:babel:orders:created\",\"data\":{},\"future\":true}");
        var handled = 0;
        var rejected = 0;
        using (var producer = NewProducer())
        using (var raw = NewConsumer("bq-it-group-" + suffix, work))
        {
            var handlers = new Dictionary<string, BabelHandler>
            {
                [Urn] = (_, _, _) =>
                {
                    Interlocked.Increment(ref handled);
                    return Task.CompletedTask;
                },
            };
            var worker = new KafkaConsumer(raw, handlers, new KafkaConsumerOptions
            {
                Producer = producer,
                RetryTopics = topology,
                PollTimeout = TimeSpan.FromMilliseconds(500),
                OnError = (_, _, _) => Interlocked.Increment(ref rejected),
            });

            var headers = new Headers();
            KafkaHeaders.Add(headers, KafkaHeaders.Job, Urn);
            KafkaHeaders.Add(headers, KafkaHeaders.SchemaVersion, "2");
            await producer.ProduceAsync(work, new Message<byte[], byte[]> { Value = body, Headers = headers });

            var stop = DateTime.UtcNow + Deadline;
            while (Volatile.Read(ref rejected) == 0 && DateTime.UtcNow < stop)
            {
                await worker.PollAsync();
            }
            Assert.True(rejected >= 1, "the gate must report the rejection");
        }

        var dead = AwaitDlqRecord(dlq);
        Assert.Equal(body, dead.Message.Value);
        Assert.Equal("2", KafkaHeaders.GetString(dead.Message.Headers, KafkaHeaders.SchemaVersion));
        Assert.Equal(0, handled);
    }

    /// <summary>Read the first <paramref name="count"/> records of a single-partition topic from the beginning.</summary>
    private static List<ConsumeResult<byte[], byte[]>> ReadRecords(string topic, int count)
    {
        var records = new List<ConsumeResult<byte[], byte[]>>();
        using var reader = NewConsumer("bq-it-read-" + Guid.NewGuid(), topic);
        var stop = DateTime.UtcNow + Deadline;
        while (records.Count < count && DateTime.UtcNow < stop)
        {
            var result = reader.Consume(TimeSpan.FromMilliseconds(500));
            if (result?.Message is not null)
            {
                records.Add(result);
            }
        }
        Assert.True(records.Count >= count, $"expected {count} record(s) on {topic}, read {records.Count} within {Deadline}");
        return records;
    }

    // A null key (no key at all) and an empty key (zero-length) are different on the wire, so the
    // null-ness is asserted separately from the byte comparison.
    private static void AssertSameKey(string hop, byte[]? expected, byte[]? actual)
    {
        Assert.True(expected is null == actual is null, $"{hop}: null-ness changed (expected {(expected is null ? "null" : "set")}, got {(actual is null ? "null" : "set")})");
        Assert.Equal(expected, actual);
    }

    /// <summary>
    /// Seed one record with <paramref name="key"/>, fail its handler to the DLQ, then dry-run and redrive it, reading the
    /// records back from the broker and asserting the key at every hop (§6.5).
    /// </summary>
    private static async Task AssertKeySurvivesRetryDlqAndRedriveAsync(byte[]? key)
    {
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var work = "bq-it-key-" + suffix;
        var sandbox = "bq-it-key-sandbox-" + suffix;
        var topology = RetryTopics.ForTopic(work).Tier(TimeSpan.FromSeconds(1)).Build();
        var retry = topology.Tiers[0].Topic;
        var dlq = topology.DlqTopic!;
        await CreateTopicsAsync(work, retry, dlq, sandbox);

        var calls = 0;
        using var producer = NewProducer();
        using (var raw = NewConsumer("bq-it-group-" + suffix, work, retry))
        {
            var handlers = new Dictionary<string, BabelHandler>
            {
                [Urn] = (_, _, _) =>
                {
                    Interlocked.Increment(ref calls);
                    throw new InvalidOperationException("boom");
                },
            };
            var worker = new KafkaConsumer(raw, handlers, new KafkaConsumerOptions
            {
                Producer = producer,
                RetryTopics = topology,
                MaxTries = 2,
                PollTimeout = TimeSpan.FromMilliseconds(500),
            });

            var envelope = EnvelopeCodec.Make(Urn, new Dictionary<string, object?> { ["order_id"] = 7 }, work, "trace-it-key");
            await producer.ProduceAsync(work, new Message<byte[], byte[]>
            {
                Key = key!,
                Value = Encoding.UTF8.GetBytes(EnvelopeCodec.Encode(envelope)),
                Headers = KafkaHeaders.Project(envelope),
            });

            var stop = DateTime.UtcNow + Deadline;
            while (Volatile.Read(ref calls) < 2 && DateTime.UtcNow < stop)
            {
                await worker.PollAsync();
            }
            Assert.Equal(2, calls);
            // One more poll settles the terminal record (the DLQ write is confirmed before it returns).
            await worker.PollAsync();
        }

        AssertSameKey("seed", key, ReadRecords(work, 1)[0].Message.Key);
        AssertSameKey("retry republish", key, ReadRecords(retry, 1)[0].Message.Key);
        AssertSameKey("DLQ write", key, ReadRecords(dlq, 1)[0].Message.Key);

        // Dry run restores the record verbatim onto the DLQ (original + restore); the real redrive then drains the
        // restore (the group's committed offset is past the original) onto a sandbox topic. A pop can return nothing
        // while the consumer group is still joining, so each run repeats until it saw a record (an empty run has no
        // side effects).
        using var ops = NewConsumer("bq-it-ops-" + suffix, dlq);
        var transport = new KafkaRedriveTransport(ops, producer, TimeSpan.FromSeconds(2));
        var until = DateTime.UtcNow + Deadline;
        var dry = await Redrive.RedriveAsync(transport, dlq, new Redrive.Options(DryRun: true, Max: 1));
        while (dry.Items.Count == 0 && DateTime.UtcNow < until)
        {
            dry = await Redrive.RedriveAsync(transport, dlq, new Redrive.Options(DryRun: true, Max: 1));
        }
        Assert.Single(dry.Items);
        AssertSameKey("dry-run restore", key, ReadRecords(dlq, 2)[1].Message.Key);

        var moved = await Redrive.RedriveAsync(transport, dlq, new Redrive.Options(ToQueue: sandbox, Max: 1));
        while (moved.Items.Count == 0 && DateTime.UtcNow < until)
        {
            moved = await Redrive.RedriveAsync(transport, dlq, new Redrive.Options(ToQueue: sandbox, Max: 1));
        }
        Assert.Equal(1, moved.Redriven);
        AssertSameKey("redrive", key, ReadRecords(sandbox, 1)[0].Message.Key);
    }

    [KafkaIntegrationFact]
    public Task NullRecordKeySurvivesRetryDlqAndRedrive() => AssertKeySurvivesRetryDlqAndRedriveAsync(null);

    [KafkaIntegrationFact]
    public Task EmptyRecordKeySurvivesRetryDlqAndRedrive() => AssertKeySurvivesRetryDlqAndRedriveAsync(Array.Empty<byte>());

    [KafkaIntegrationFact]
    public Task NonEmptyRecordKeySurvivesRetryDlqAndRedrive() => AssertKeySurvivesRetryDlqAndRedriveAsync(Encoding.UTF8.GetBytes("customer-42"));
}
