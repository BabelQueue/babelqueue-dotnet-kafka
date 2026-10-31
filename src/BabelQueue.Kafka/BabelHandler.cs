using Confluent.Kafka;

namespace BabelQueue.Kafka;

/// <summary>
/// Processes one decoded, validated envelope and the raw Kafka record it arrived on. Returning
/// normally acknowledges it (the consumer commits the offset past it); throwing routes it to a
/// retry topic with <c>bq-attempts + 1</c> (or to the DLQ once max-tries is reached) — Kafka's
/// analogue of "republish then ack".
/// </summary>
/// <remarks>
/// To continue a W3C <c>traceparent</c> that rode beside the envelope (ADR-0028), hand
/// <c>KafkaTraceHeaders.Extract(result.Message.Headers)</c> to <c>Telemetry.Wrap(handler, headers)</c>; the consumer span then becomes a true child of the
/// producer span. With no <c>traceparent</c> in the record headers the core falls back to the v0.1 <c>trace_id</c> mapping.
/// </remarks>
public delegate Task BabelHandler(Envelope envelope, ConsumeResult<byte[], byte[]> result, CancellationToken cancellationToken);
