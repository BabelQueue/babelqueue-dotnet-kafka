using Confluent.Kafka;

namespace BabelQueue.Kafka;

/// <summary>
/// Processes one decoded, validated envelope and the raw Kafka record it arrived on. Returning
/// normally acknowledges it (the consumer commits the offset past it); throwing routes it to a
/// retry topic with <c>bq-attempts + 1</c> (or to the DLQ once max-tries is reached) — Kafka's
/// analogue of "republish then ack".
/// </summary>
public delegate Task BabelHandler(Envelope envelope, ConsumeResult<byte[], byte[]> result, CancellationToken cancellationToken);
