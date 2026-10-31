using Confluent.Kafka;

namespace BabelQueue.Kafka;

/// <summary>Tuning and hooks for <see cref="KafkaConsumer"/>.</summary>
public sealed class KafkaConsumerOptions
{
    /// <summary>The producer used to republish retry/DLQ records (required for retry/DLQ).</summary>
    public IProducer<byte[], byte[]>? Producer { get; set; }

    /// <summary>The retry/DLQ topology; enables per-record retry, delay, and dead-lettering.</summary>
    public RetryTopics? RetryTopics { get; set; }

    /// <summary>Attempts before terminal dead-lettering (default 3).</summary>
    public int MaxTries { get; set; } = 3;

    /// <summary>Strategy for a URN with no handler: <see cref="UnknownUrnStrategy"/> values (default <c>fail</c>).</summary>
    public string UnknownUrn { get; set; } = UnknownUrnStrategy.Fail;

    /// <summary>Called for a poison record, an unmapped URN, or a throwing handler. The loop never stops.</summary>
    public Action<Exception, Envelope?, ConsumeResult<byte[], byte[]>>? OnError { get; set; }

    /// <summary>
    /// Optional dead-letter observer (ADR-0039 section 5.1), called once per record the consumer wrote to the DLQ
    /// topic, after the broker confirmed the write: an exhausted retry, an unmapped URN under <c>dead_letter</c>, a
    /// poison body, or a raw record rejected by the schema-version gate (empty reason). Not called when the write
    /// fails, for a retry-tier republish, or when no DLQ is configured. An observer exception is caught and logged;
    /// it never changes the settle outcome. Needs a <c>BabelQueue.Core</c> that ships <see cref="IDeadLetterObserver"/>.
    /// </summary>
    public IDeadLetterObserver? DeadLetterObserver { get; set; }

    /// <summary>Per-poll consume timeout (default 1s).</summary>
    public TimeSpan PollTimeout { get; set; } = TimeSpan.FromSeconds(1);
}
