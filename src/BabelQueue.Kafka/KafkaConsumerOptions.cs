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

    /// <summary>Per-poll consume timeout (default 1s).</summary>
    public TimeSpan PollTimeout { get; set; } = TimeSpan.FromSeconds(1);
}
