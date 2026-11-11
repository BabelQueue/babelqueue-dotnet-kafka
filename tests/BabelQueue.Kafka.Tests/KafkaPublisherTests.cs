using System;
using System.Collections.Generic;
using System.Text;
using System.Diagnostics;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using BabelQueue;
using BabelQueue.Tracing;
using BabelQueue.Kafka;
using Confluent.Kafka;
using Moq;
using Xunit;

namespace BabelQueue.Kafka.Tests;

/// <summary>§6 produce: value = envelope, record ts = created_at, bq- headers; delay → tier or raise.</summary>
public sealed class KafkaPublisherTests
{
    private const string Urn = "urn:babel:orders:created";

    private static Mock<IProducer<byte[], byte[]>> Producer(List<(string Topic, Message<byte[], byte[]> Message)> captured)
    {
        var producer = new Mock<IProducer<byte[], byte[]>>();
        producer
            .Setup(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<byte[], byte[]>>(), It.IsAny<CancellationToken>()))
            .Callback<string, Message<byte[], byte[]>, CancellationToken>((t, m, _) => captured.Add((t, m)))
            .ReturnsAsync(new DeliveryResult<byte[], byte[]>());
        return producer;
    }

    [Fact]
    public async Task PublishProjectsValueHeadersAndTimestamp()
    {
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var producer = Producer(captured);

        var id = await KafkaPublisher.Create(producer.Object, "orders")
            .PublishAsync(Urn, new Dictionary<string, object?> { ["order_id"] = 7 }, "trace-1");

        Assert.Single(captured);
        var (topic, message) = captured[0];
        Assert.Equal("orders", topic);
        Assert.Equal(Urn, KafkaHeaders.GetString(message.Headers, KafkaHeaders.Job));
        Assert.Equal("trace-1", KafkaHeaders.GetString(message.Headers, KafkaHeaders.TraceId));
        Assert.Equal(id, KafkaHeaders.GetString(message.Headers, KafkaHeaders.MessageId));
        Assert.Equal("0", KafkaHeaders.GetString(message.Headers, KafkaHeaders.Attempts));

        var decoded = EnvelopeCodec.Decode(Encoding.UTF8.GetString(message.Value));
        Assert.Equal(Urn, EnvelopeCodec.Urn(decoded));
        Assert.Equal(decoded.Meta!.CreatedAt, message.Timestamp.UnixTimestampMs);
    }

    [Fact]
    public async Task DelayWithoutRetryTopicsRaises()
    {
        var captured = new List<(string, Message<byte[], byte[]>)>();
        var publisher = KafkaPublisher.Create(Producer(captured).Object, "orders");
        await Assert.ThrowsAsync<BabelQueueException>(
            () => publisher.PublishAsync(Urn, null, null, TimeSpan.FromSeconds(30)));
    }

    [Fact]
    public async Task DelayRoutesToTheSmallestSufficientRetryTier()
    {
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var producer = Producer(captured);
        var rt = RetryTopics.ForTopic("orders").Tier(TimeSpan.FromSeconds(5)).Tier(TimeSpan.FromSeconds(60)).Build();

        await KafkaPublisher.Create(producer.Object, rt).PublishAsync(Urn, null, null, TimeSpan.FromSeconds(30));

        var (topic, message) = captured[0];
        Assert.Equal("orders.retry.2", topic);
        Assert.Equal("30000", KafkaHeaders.GetString(message.Headers, KafkaHeaders.Delay));
        Assert.Equal("orders", KafkaHeaders.GetString(message.Headers, KafkaHeaders.OriginalTopic));
    }

    // ADR-0028: a W3C traceparent rides as a record header beside the envelope, never inside it.
    private const string TraceparentValue = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    private const string TracestateValue = "vendor=opaque,other=1";

    private static Dictionary<string, string> TraceHeaders() => new()
    {
        ["traceparent"] = TraceparentValue,
        ["tracestate"] = TracestateValue,
    };

    private static ConsumeResult<byte[], byte[]> Received(string topic, Message<byte[], byte[]> sent) =>
        new()
        {
            Topic = topic,
            Partition = new Partition(0),
            Offset = new Offset(0),
            Message = sent,
        };

    private static HashSet<string> TopLevelKeys(byte[] body)
    {
        using var doc = JsonDocument.Parse(body);
        var keys = new HashSet<string>(StringComparer.Ordinal);
        foreach (var property in doc.RootElement.EnumerateObject())
        {
            keys.Add(property.Name);
            if (property.Value.ValueKind == JsonValueKind.Object && property.Name == "meta")
            {
                foreach (var inner in property.Value.EnumerateObject())
                {
                    keys.Add("meta." + inner.Name);
                }
            }
        }
        return keys;
    }

    [Fact]
    public async Task PublishWithHeadersCarriesTraceparentAndLeavesTheBodyAnEnvelope()
    {
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var publisher = KafkaPublisher.Create(Producer(captured).Object, "orders");

        await publisher.PublishAsync(Urn, new Dictionary<string, object?> { ["order_id"] = 7 }, "trace-1");
        await publisher.PublishWithHeadersAsync(Urn, new Dictionary<string, object?> { ["order_id"] = 7 }, TraceHeaders(), "trace-1");

        var plain = captured[0].Message;
        var traced = captured[1].Message;

        // The received record carries the header verbatim (UTF-8).
        var consumed = Received("orders", traced);
        Assert.Equal(TraceparentValue, KafkaHeaders.GetString(consumed.Message.Headers, "traceparent"));
        Assert.Equal(TracestateValue, KafkaHeaders.GetString(consumed.Message.Headers, "tracestate"));
        Assert.Equal(TraceparentValue, KafkaTraceHeaders.Extract(consumed.Message.Headers)["traceparent"]);

        // The body is the same envelope shape and never mentions the trace header.
        var body = Encoding.UTF8.GetString(traced.Value);
        Assert.DoesNotContain("traceparent", body, StringComparison.Ordinal);
        Assert.DoesNotContain(TraceparentValue, body, StringComparison.Ordinal);
        Assert.Equal(TopLevelKeys(plain.Value), TopLevelKeys(traced.Value));
        Assert.Equal(Urn, EnvelopeCodec.Urn(EnvelopeCodec.Decode(body)));

        // The contract projection is untouched.
        Assert.Equal(Urn, KafkaHeaders.GetString(traced.Headers, KafkaHeaders.Job));
        Assert.Equal("trace-1", KafkaHeaders.GetString(traced.Headers, KafkaHeaders.TraceId));
    }

    [Fact]
    public async Task PublishWithoutHeadersAddsNoTraceHeaders()
    {
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var publisher = KafkaPublisher.Create(Producer(captured).Object, "orders");

        await publisher.PublishAsync(Urn, null, "trace-1");
        await publisher.PublishWithHeadersAsync(Urn, null, new Dictionary<string, string>(), "trace-1");
        await publisher.PublishWithHeadersAsync(Urn, null, null, "trace-1");

        foreach (var (_, message) in captured)
        {
            Assert.Null(KafkaHeaders.GetString(message.Headers, "traceparent"));
            Assert.Null(KafkaHeaders.GetString(message.Headers, "tracestate"));
            Assert.Equal(captured[0].Message.Headers.Count, message.Headers.Count);
        }
    }

    [Fact]
    public async Task PublishWithHeadersNeverClobbersAContractHeader()
    {
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var publisher = KafkaPublisher.Create(Producer(captured).Object, "orders");

        await publisher.PublishWithHeadersAsync(
            Urn, null, new Dictionary<string, string> { ["bq-job"] = "urn:evil", ["bq-attempts"] = "9", [""] = "x", ["empty"] = "" });

        var headers = captured[0].Message.Headers;
        Assert.Equal(Urn, KafkaHeaders.GetString(headers, KafkaHeaders.Job));
        Assert.Equal("0", KafkaHeaders.GetString(headers, KafkaHeaders.Attempts));
        Assert.Single(headers, h => h.Key == KafkaHeaders.Job);
        Assert.Null(KafkaHeaders.GetString(headers, "empty"));
    }

    [Fact]
    public async Task DelayedPublishWithHeadersCarriesTraceparentOnTheRetryTierRecord()
    {
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var rt = RetryTopics.ForTopic("orders").Tier(TimeSpan.FromSeconds(60)).Build();

        await KafkaPublisher.Create(Producer(captured).Object, rt)
            .PublishWithHeadersAsync(Urn, null, TraceHeaders(), null, TimeSpan.FromSeconds(30));

        var (topic, message) = Assert.Single(captured);
        Assert.Equal("orders.retry.1", topic);
        Assert.Equal(TraceparentValue, KafkaHeaders.GetString(message.Headers, "traceparent"));
        Assert.Equal("30000", KafkaHeaders.GetString(message.Headers, KafkaHeaders.Delay));
        Assert.Equal("orders", KafkaHeaders.GetString(message.Headers, KafkaHeaders.OriginalTopic));
    }

    [Fact]
    public async Task ConsumerSpanIsATrueChildOfTheProducerSpanViaTheReceivedHeaders()
    {
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var publisher = KafkaPublisher.Create(Producer(captured).Object, "orders");
        var producerTrace = ActivityTraceId.CreateFromString(TraceparentValue.AsSpan(3, 32));

        await publisher.PublishWithHeadersAsync(Urn, null, TraceHeaders(), "trace-1");
        var consumed = Received("orders", captured[0].Message);
        var envelope = EnvelopeCodec.Decode(Encoding.UTF8.GetString(consumed.Message.Value));

        Activity? consumerSpan = null;
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == Telemetry.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStarted = activity =>
            {
                if (activity.Kind == ActivityKind.Consumer && activity.TraceId == producerTrace)
                {
                    consumerSpan = activity;
                }
            },
        };
        ActivitySource.AddActivityListener(listener);

        await Telemetry.Wrap(_ => Task.CompletedTask, KafkaTraceHeaders.Extract(consumed.Message.Headers))(envelope);

        Assert.NotNull(consumerSpan);
        Assert.Equal("b7ad6b7169203331", consumerSpan!.ParentSpanId.ToHexString());
    }

    [Fact]
    public void ExtractOfNoHeadersIsEmpty()
    {
        Assert.Empty(KafkaTraceHeaders.Extract(null));
        Assert.Empty(KafkaTraceHeaders.Extract(new Headers()));
    }

    [Fact]
    public async Task PublishWithHeadersDropsEveryContractHeaderIncludingTheRetryPair()
    {
        // Parity with the Go reference: bq-delay / bq-original-topic are owned by the transport and never
        // accepted from the carrier, even though a plain first publish does not project them (§6.3).
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var carrier = new Dictionary<string, string> { ["traceparent"] = TraceparentValue };
        foreach (var key in new[]
        {
            KafkaHeaders.Job, KafkaHeaders.TraceId, KafkaHeaders.MessageId, KafkaHeaders.SchemaVersion,
            KafkaHeaders.SourceLang, KafkaHeaders.Attempts, KafkaHeaders.Delay, KafkaHeaders.OriginalTopic,
        })
        {
            carrier[key] = "evil";
        }

        await KafkaPublisher.Create(Producer(captured).Object, "orders").PublishWithHeadersAsync(Urn, null, carrier, "trace-1");

        var (topic, message) = Assert.Single(captured);
        Assert.Equal("orders", topic);
        Assert.Null(KafkaHeaders.GetString(message.Headers, KafkaHeaders.Delay));
        Assert.Null(KafkaHeaders.GetString(message.Headers, KafkaHeaders.OriginalTopic));
        Assert.Equal(Urn, KafkaHeaders.GetString(message.Headers, KafkaHeaders.Job));
        Assert.Equal("trace-1", KafkaHeaders.GetString(message.Headers, KafkaHeaders.TraceId));
        Assert.Equal("0", KafkaHeaders.GetString(message.Headers, KafkaHeaders.Attempts));
        Assert.DoesNotContain(message.Headers, h => System.Text.Encoding.UTF8.GetString(h.GetValueBytes()) == "evil");
        Assert.Equal(TraceparentValue, KafkaHeaders.GetString(message.Headers, "traceparent"));
    }

    [Fact]
    public async Task ExtractLeavesOutTheContractHeaders()
    {
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        await KafkaPublisher.Create(Producer(captured).Object, "orders").PublishWithHeadersAsync(Urn, null, TraceHeaders(), "trace-1");

        var map = KafkaTraceHeaders.Extract(captured[0].Message.Headers);

        Assert.Equal(new[] { "traceparent", "tracestate" }, new SortedSet<string>(map.Keys, StringComparer.Ordinal));
    }

    // O6: the envelope Telemetry.PublishAsync builds is the one that is sent, so the producer span's
    // messaging.message.id is the meta.id on the wire (and the traceparent rides beside the envelope).
    [Fact]
    public async Task TelemetryPublishEnvelopeRecordsTheWireMessageId()
    {
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var publisher = KafkaPublisher.Create(Producer(captured).Object, "orders");
        const string envelopeUrn = "urn:babel:orders:envelope-id";
        var activities = new List<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == Telemetry.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStopped = activity =>
            {
                if (activity.Kind == ActivityKind.Producer && Equals(activity.GetTagItem("babelqueue.urn"), envelopeUrn))
                {
                    lock (activities)
                    {
                        activities.Add(activity);
                    }
                }
            },
        };
        ActivitySource.AddActivityListener(listener);

        var headers = new Dictionary<string, string>();
        var id = await Telemetry.PublishAsync(
            envelopeUrn,
            new Dictionary<string, object?> { ["order_id"] = 7 },
            headers,
            env => publisher.PublishEnvelopeAsync(env, headers),
            "orders");

        var producerSpan = Assert.Single(activities);
        var sentRecord = Assert.Single(captured).Message;
        var body = Encoding.UTF8.GetString(sentRecord.Value);
        var traceparent = KafkaHeaders.GetString(sentRecord.Headers, "traceparent");
        var wire = EnvelopeCodec.Decode(body);

        Assert.Equal(id, wire.Meta!.Id);
        Assert.Equal(wire.Meta.Id, producerSpan.GetTagItem("messaging.message.id"));
        Assert.Equal(Telemetry.UuidOf(producerSpan.TraceId), wire.TraceId);
        Assert.Equal(wire.TraceId, producerSpan.GetTagItem("messaging.message.conversation_id"));
        Assert.Equal(EnvelopeCodec.SchemaVersion, wire.Meta.SchemaVersion);
        Assert.Equal(headers["traceparent"], traceparent);
        Assert.Contains(producerSpan.TraceId.ToHexString(), traceparent, StringComparison.Ordinal);
        Assert.DoesNotContain("traceparent", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PublishEnvelopeSendsTheGivenEnvelopeUnchanged()
    {
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var publisher = KafkaPublisher.Create(Producer(captured).Object, "orders");
        var made = EnvelopeCodec.Make(
            "urn:babel:orders:created", new Dictionary<string, object?> { ["order_id"] = 7 }, "orders", "trace-fixed");
        var envelope = made with
        {
            Extras = new Dictionary<string, JsonElement> { ["x_tenant"] = JsonSerializer.SerializeToElement("t1") },
            Meta = made.Meta! with
            {
                Extras = new Dictionary<string, JsonElement> { ["x_region"] = JsonSerializer.SerializeToElement("eu") },
            },
        };

        var id = await publisher.PublishEnvelopeAsync(envelope);

        var sentRecord = Assert.Single(captured).Message;
        var body = Encoding.UTF8.GetString(sentRecord.Value);
        var traceparent = KafkaHeaders.GetString(sentRecord.Headers, "traceparent");
        Assert.Equal(envelope.Meta!.Id, id);
        Assert.Equal(EnvelopeCodec.Encode(envelope), body);
        var wire = EnvelopeCodec.Decode(body);
        Assert.Equal("trace-fixed", wire.TraceId);
        Assert.Equal(EnvelopeCodec.SchemaVersion, wire.Meta!.SchemaVersion);
        Assert.Equal(envelope.Meta.Id, wire.Meta.Id);
        Assert.Equal("t1", wire.Extras!["x_tenant"].GetString());
        Assert.Equal("eu", wire.Meta.Extras!["x_region"].GetString());
        Assert.Null(traceparent);
    }

    // Documents the PublishEnvelopeAsync contract: meta.queue is whatever the envelope carries. Telemetry stamps
    // its queue argument (default "default"), not this publisher's target, so callers must pass the queue.
    [Fact]
    public async Task TelemetryPublishWithoutAQueueArgumentStampsDefaultMetaQueue()
    {
        var captured = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var publisher = KafkaPublisher.Create(Producer(captured).Object, "orders");
        await Telemetry.PublishAsync(
            "urn:babel:orders:created", null, env => publisher.PublishEnvelopeAsync(env));

        var sentRecord = Assert.Single(captured).Message;
        var body = Encoding.UTF8.GetString(sentRecord.Value);
        var traceparent = KafkaHeaders.GetString(sentRecord.Headers, "traceparent");
        Assert.Equal("default", EnvelopeCodec.Decode(body).Meta!.Queue);
        Assert.Null(traceparent);
    }
}
