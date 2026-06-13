using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using BabelQueue;
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
}
