using System.Globalization;
using System.Text;
using Confluent.Kafka;

namespace BabelQueue.Kafka;

/// <summary>
/// Sends canonical-envelope messages to one Kafka work topic with the §6 projection: the record
/// value is the envelope JSON, the record timestamp mirrors <c>meta.created_at</c>, and the
/// contract fields are mirrored onto <c>bq-</c> headers so a consumer routes on <c>bq-job</c>
/// without decoding the body. The envelope is unchanged (<c>schema_version</c> stays 1).
///
/// <para>Kafka has no native delayed delivery: a positive delay requires a <see cref="RetryTopics"/>
/// topology and is routed to the matching tier; on a plain publisher a delay raises
/// <see cref="BabelQueueException"/> rather than being silently dropped (§6.4).</para>
/// </summary>
public sealed class KafkaPublisher
{
    private readonly IProducer<byte[], byte[]> _producer;
    private readonly string _workTopic;
    private readonly RetryTopics? _retryTopics;

    private KafkaPublisher(IProducer<byte[], byte[]> producer, string workTopic, RetryTopics? retryTopics)
    {
        ArgumentNullException.ThrowIfNull(producer);
        _producer = producer;
        _workTopic = workTopic;
        _retryTopics = retryTopics;
    }

    /// <summary>A publisher onto <paramref name="topic"/> with no retry topics (a delay raises).</summary>
    public static KafkaPublisher Create(IProducer<byte[], byte[]> producer, string topic) => new(producer, topic, null);

    /// <summary>A publisher onto the topology's work topic, with delay routed via its retry tiers.</summary>
    public static KafkaPublisher Create(IProducer<byte[], byte[]> producer, RetryTopics retryTopics)
    {
        ArgumentNullException.ThrowIfNull(retryTopics);
        return new KafkaPublisher(producer, retryTopics.WorkTopic, retryTopics);
    }

    /// <summary>
    /// Build the canonical envelope for <c>(urn, data)</c>, send it with the §6 projection, and
    /// return the message id (<c>meta.id</c>). A positive <paramref name="delay"/> routes to the
    /// matching retry tier (requires a <see cref="RetryTopics"/>); otherwise a delay raises.
    /// </summary>
    public async Task<string> PublishAsync(
        string urn,
        IReadOnlyDictionary<string, object?>? data = null,
        string? traceId = null,
        TimeSpan? delay = null,
        CancellationToken cancellationToken = default)
    {
        var envelope = EnvelopeCodec.Make(urn, data, _workTopic, traceId);
        if (delay is { } window && window > TimeSpan.Zero)
        {
            if (_retryTopics is null)
            {
                throw new BabelQueueException("Kafka has no native delayed delivery; a delay requires retry topics (none configured).");
            }
            var tier = _retryTopics.TierForDelay(window);
            await SendAsync(tier.Topic, envelope, window, _workTopic, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await SendAsync(_workTopic, envelope, null, null, cancellationToken).ConfigureAwait(false);
        }
        return envelope.Meta?.Id ?? string.Empty;
    }

    private async Task SendAsync(string topic, Envelope envelope, TimeSpan? delay, string? originalTopic, CancellationToken cancellationToken)
    {
        var headers = KafkaHeaders.Project(envelope);
        if (delay is { } window)
        {
            KafkaHeaders.Add(headers, KafkaHeaders.Delay, ((long)window.TotalMilliseconds).ToString(CultureInfo.InvariantCulture));
        }
        if (originalTopic is not null)
        {
            KafkaHeaders.Add(headers, KafkaHeaders.OriginalTopic, originalTopic);
        }
        var timestamp = envelope.Meta?.CreatedAt ?? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var message = new Message<byte[], byte[]>
        {
            Value = Encoding.UTF8.GetBytes(EnvelopeCodec.Encode(envelope)),
            Headers = headers,
            Timestamp = new Timestamp(timestamp, TimestampType.CreateTime),
        };
        await _producer.ProduceAsync(topic, message, cancellationToken).ConfigureAwait(false);
    }
}
