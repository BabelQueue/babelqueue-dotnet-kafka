using Confluent.Kafka;

namespace BabelQueue.Kafka;

/// <summary>Tuning and hooks for <see cref="KafkaReinjectConsumer"/>.</summary>
public sealed class KafkaReinjectOptions
{
    /// <summary>
    /// The producer that writes records back to their original topic (required). A write counts as acknowledged
    /// when <c>ProduceAsync</c> completes without throwing, so use <c>Acks = All</c> (or at least <c>Leader</c>);
    /// <c>Acks = None</c> gives no acknowledgement to wait for.
    /// </summary>
    public IProducer<byte[], byte[]>? Producer { get; set; }

    /// <summary>
    /// Optional topology, used only as a fallback to learn a record's tier when it carries no usable
    /// <c>bq-delay</c> header (the tier is looked up by the retry topic's name).
    /// </summary>
    public RetryTopics? RetryTopics { get; set; }

    /// <summary>The time source for the A6 wait target (default <see cref="DateTimeOffset.UtcNow"/>); injectable for tests.</summary>
    public Func<DateTimeOffset> Clock { get; set; } = static () => DateTimeOffset.UtcNow;

    /// <summary>Upper bound of one blocking poll (default 1s); the poll is shortened when a held record is due sooner.</summary>
    public TimeSpan PollTimeout { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>How long a record whose write failed waits before the write is tried again (default 1s).</summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Called when a write-back or a commit fails. The loop never stops.</summary>
    public Action<Exception, ConsumeResult<byte[], byte[]>>? OnError { get; set; }
}
