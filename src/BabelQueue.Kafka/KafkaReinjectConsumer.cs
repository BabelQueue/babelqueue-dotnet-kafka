using System.Globalization;
using System.Text.RegularExpressions;
using Confluent.Kafka;

namespace BabelQueue.Kafka;

/// <summary>
/// The re-inject consumer for the retry tiers (ADR-0034 §5–§7). For each record of a
/// <c>&lt;topic&gt;.retry.&lt;n&gt;</c> topic it:
/// <list type="number">
///   <item>waits until the target time — <c>record.timestamp + tier_ms</c>, clamped to <c>[now, now + tier_ms]</c> —
///   by <b>pausing the partition</b> and resuming it once due, so the poll loop is never blocked and other
///   partitions keep flowing;</item>
///   <item>writes the <b>value and key byte for byte</b> to the topic named in <c>bq-original-topic</c>. The body is
///   never decoded or re-encoded and <c>attempts</c> is never touched, so a record that cannot be decoded is carried
///   as it is;</item>
///   <item>drops <c>bq-delay</c> and <c>bq-original-topic</c> and keeps every other header unchanged
///   (<c>traceparent</c> included);</item>
///   <item>commits the retry-topic offset <b>only after the write is acknowledged</b>. A failed write is not
///   committed: the record stays held (partition paused) and is written again after
///   <see cref="KafkaReinjectOptions.RetryDelay"/> — at-least-once.</item>
/// </list>
/// <para>
/// The wrapped <see cref="IConsumer{TKey,TValue}"/> must be subscribed to the retry topics
/// (<see cref="RetryTopics.Tiers"/>) and run with <c>EnableAutoCommit = false</c>. Retry topics SHOULD use
/// <c>message.timestamp.type = LogAppendTime</c> (so the broker clock sets <c>record.timestamp</c>) and a retention
/// longer than the largest tier. A held record lives in memory only: it is redelivered after a restart, and a
/// rebalance that moves its partition away can make it be written once more by the new owner (at-least-once).
/// A record whose timestamp is not available waits the full tier.
/// </para>
/// </summary>
public sealed partial class KafkaReinjectConsumer
{
    private readonly IConsumer<byte[], byte[]> _consumer;
    private readonly KafkaReinjectOptions _options;

    // At most one held record per partition: the partition is paused while it is held, so order is preserved.
    private readonly Dictionary<TopicPartition, Held> _held = new();

    public KafkaReinjectConsumer(IConsumer<byte[], byte[]> consumer, KafkaReinjectOptions options)
    {
        ArgumentNullException.ThrowIfNull(consumer);
        ArgumentNullException.ThrowIfNull(options);
        if (options.Producer is null)
        {
            throw new BabelQueueException("The Kafka re-inject consumer needs a producer to write records back.");
        }
        _consumer = consumer;
        _options = options;
    }

    /// <summary>Number of records currently held (waiting for their target time, or for a write retry).</summary>
    public int HeldCount => _held.Count;

    /// <summary>
    /// One loop turn: re-inject every held record that is due, then consume at most one new record (blocking up to the
    /// poll timeout, shortened when a held record is due sooner). Returns the number of records re-injected.
    /// </summary>
    public async Task<int> PollAsync(CancellationToken cancellationToken = default)
    {
        var done = await FlushDueAsync(cancellationToken).ConfigureAwait(false);

        var result = _consumer.Consume(NextTimeout());
        if (result?.Message is null)
        {
            return done;
        }

        var partition = result.TopicPartition;
        if (_held.ContainsKey(partition))
        {
            // Defensive: a paused partition must not deliver. Rewind so the record is delivered again after resume.
            _consumer.Seek(result.TopicPartitionOffset);
            return done;
        }

        var held = new Held(result, WaitTarget(result));
        _held[partition] = held;
        _consumer.Pause(new[] { partition });
        done += await FlushDueAsync(cancellationToken).ConfigureAwait(false);
        return done;
    }

    /// <summary>Poll until <paramref name="cancellationToken"/> is cancelled.</summary>
    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            await PollAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private TimeSpan NextTimeout()
    {
        if (_held.Count == 0)
        {
            return _options.PollTimeout;
        }
        var now = _options.Clock();
        var soonest = _held.Values.Min(h => h.Due);
        var wait = soonest - now;
        if (wait < TimeSpan.Zero)
        {
            wait = TimeSpan.Zero;
        }
        return wait < _options.PollTimeout ? wait : _options.PollTimeout;
    }

    // A6: target = record.timestamp + tier_ms, clamped to [now, now + tier_ms]. A record without an available
    // timestamp (or without a known tier) cannot be placed on the broker's clock: it waits the full tier / not at all.
    private DateTimeOffset WaitTarget(ConsumeResult<byte[], byte[]> result)
    {
        var now = _options.Clock();
        var tier = TierMillis(result);
        if (tier <= 0)
        {
            return now;
        }
        var upper = now.AddMilliseconds(tier);
        var timestamp = result.Message.Timestamp;
        if (timestamp.Type == TimestampType.NotAvailable)
        {
            return upper;
        }
        var target = timestamp.UtcDateTime.AddMilliseconds(tier);
        var targetOffset = new DateTimeOffset(target, TimeSpan.Zero);
        if (targetOffset < now)
        {
            return now;
        }
        return targetOffset > upper ? upper : targetOffset;
    }

    private long TierMillis(ConsumeResult<byte[], byte[]> result)
    {
        var header = KafkaHeaders.GetString(result.Message.Headers, KafkaHeaders.Delay);
        if (header is not null
            && long.TryParse(header.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var millis)
            && millis >= 0)
        {
            return millis;
        }
        var tier = _options.RetryTopics?.Tiers.FirstOrDefault(t => string.Equals(t.Topic, result.Topic, StringComparison.Ordinal));
        return tier is null ? 0 : (long)tier.Delay.TotalMilliseconds;
    }

    private async Task<int> FlushDueAsync(CancellationToken cancellationToken)
    {
        if (_held.Count == 0)
        {
            return 0;
        }
        var now = _options.Clock();
        var due = _held.Where(kv => kv.Value.Due <= now).Select(kv => kv.Key).ToList();
        var done = 0;
        foreach (var partition in due)
        {
            if (await TryReinjectAsync(_held[partition], cancellationToken).ConfigureAwait(false))
            {
                _held.Remove(partition);
                Resume(partition);
                done++;
            }
        }
        return done;
    }

    private async Task<bool> TryReinjectAsync(Held held, CancellationToken cancellationToken)
    {
        var result = held.Result;
        try
        {
            var message = BuildMessage(result);
            var target = OriginalTopic(result);
            await _options.Producer!.ProduceAsync(target, message, cancellationToken).ConfigureAwait(false);
        }
#pragma warning disable CA1031 // Any failed write must leave the record uncommitted and held for another attempt.
        catch (Exception error) when (error is not OperationCanceledException)
#pragma warning restore CA1031
        {
            held.Due = _options.Clock() + _options.RetryDelay;
            SafeReport(error, result);
            return false;
        }

        // Acknowledged: only now is the retry-topic offset committed.
        try
        {
            _consumer.Commit(result);
        }
#pragma warning disable CA1031 // The record is already written; a failed commit must not write it again from here.
        catch (Exception error)
#pragma warning restore CA1031
        {
            // The write is durable. Re-writing from this instance would duplicate it for nothing; the offset is
            // covered by the next successful commit, or the record is redelivered (at-least-once).
            SafeReport(error, result);
        }
        return true;
    }

    private static Message<byte[], byte[]> BuildMessage(ConsumeResult<byte[], byte[]> result)
    {
        var headers = new Headers();
        if (result.Message.Headers is { } source)
        {
            foreach (var header in source)
            {
                if (header.Key == KafkaHeaders.Delay || header.Key == KafkaHeaders.OriginalTopic)
                {
                    continue; // A7: re-inject drops exactly these two, keeps the rest (traceparent included).
                }
                headers.Add(header.Key, header.GetValueBytes());
            }
        }
        return new Message<byte[], byte[]>
        {
            Key = result.Message.Key,
            Value = result.Message.Value,
            Headers = headers,
        };
    }

    private static string OriginalTopic(ConsumeResult<byte[], byte[]> result)
    {
        var header = KafkaHeaders.GetString(result.Message.Headers, KafkaHeaders.OriginalTopic);
        if (!string.IsNullOrEmpty(header))
        {
            return header;
        }
        // No header: the SDK-owned topology names the tier topic <topic>.retry.<n>, so the work topic is recoverable.
        var match = RetrySuffix().Match(result.Topic);
        if (match.Success)
        {
            return result.Topic[..match.Index];
        }
        throw new BabelQueueException(
            $"Cannot re-inject a record of '{result.Topic}': no bq-original-topic header and the topic is not a '<topic>.retry.<n>' tier.");
    }

    private void Resume(TopicPartition partition)
    {
        try
        {
            _consumer.Resume(new[] { partition });
        }
        catch (KafkaException)
        {
            // A partition lost in a rebalance cannot be resumed; its new owner reads it.
        }
    }

    private void SafeReport(Exception error, ConsumeResult<byte[], byte[]> result)
    {
        try
        {
            _options.OnError?.Invoke(error, result);
        }
#pragma warning disable CA1031 // A throwing hook must never stop the loop.
        catch (Exception)
#pragma warning restore CA1031
        {
        }
    }

    [GeneratedRegex(@"\.retry\.[0-9]+$", RegexOptions.CultureInvariant)]
    private static partial Regex RetrySuffix();

    private sealed class Held
    {
        public Held(ConsumeResult<byte[], byte[]> result, DateTimeOffset due)
        {
            Result = result;
            Due = due;
        }

        public ConsumeResult<byte[], byte[]> Result { get; }

        public DateTimeOffset Due { get; set; }
    }
}
