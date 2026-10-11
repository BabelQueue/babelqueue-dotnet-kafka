using System.Diagnostics;
using System.Text;
using Confluent.Kafka;

namespace BabelQueue.Kafka;

/// <summary>
/// A <see cref="Redrive.ITransport"/> over Apache Kafka, so <see cref="Redrive.RedriveAsync"/> (ADR-0026)
/// can replay a <c>&lt;topic&gt;.dlq</c> topic back to its work topic.
///
/// <para>The supplied consumer must already be subscribed to (or assigned) the DLQ topic with auto-commit
/// disabled; <see cref="PopAsync"/> consumes one record at a time (a poll that returns nothing means the DLQ
/// is drained) and <see cref="AckAsync"/> commits the record's offset only after <see cref="Redrive"/> has
/// re-published it (at-least-once). Every write is awaited, so a failure surfaces and the offset is never
/// committed past it.</para>
///
/// <para>It implements <see cref="Redrive.IKeyedRepublisher"/>: the redrive write to the work topic and the
/// restore write back to the DLQ carry the DLQ record's <b>key byte-for-byte</b> (a null key stays null, an
/// empty key stays empty, none is synthesised — contract §6.5). Partition equality is not guaranteed.</para>
///
/// <para>A <b>restore</b> write (the same body written back to the DLQ it was popped from: dry run, unselected,
/// undecodable, failed redrive write) re-writes the DLQ record <b>verbatim</b> — its original value bytes (a null
/// value stays null), original headers and key — with a fresh timestamp, exactly like
/// <see cref="KafkaConsumer"/>'s raw DLQ write. A record whose value is not valid UTF-8 is popped with an empty
/// body (so <see cref="Redrive"/> treats it as undecodable and restores it) and is therefore never altered or
/// committed away. A <b>redrive</b> write to the work topic re-projects the §6 <c>bq-</c> headers from the body
/// being published (a decodable envelope only), so the redriven record carries the reset <c>attempts</c> and no
/// stale <c>dead_letter</c> metadata. The <c>Bypass</c> option's <c>bq-replay-bypass</c> header is not propagated
/// by this transport.</para>
/// </summary>
public sealed class KafkaRedriveTransport : Redrive.ITransport, Redrive.IKeyedRepublisher
{
    private readonly IConsumer<byte[], byte[]> _consumer;
    private readonly IProducer<byte[], byte[]> _producer;
    private readonly TimeSpan _pollTimeout;
    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    /// <summary>A redrive transport that polls the DLQ with a 1 second timeout.</summary>
    public KafkaRedriveTransport(IConsumer<byte[], byte[]> consumer, IProducer<byte[], byte[]> producer)
        : this(consumer, producer, TimeSpan.FromSeconds(1))
    {
    }

    /// <summary>A redrive transport with an explicit poll timeout (an empty poll means the DLQ is drained).</summary>
    public KafkaRedriveTransport(IConsumer<byte[], byte[]> consumer, IProducer<byte[], byte[]> producer, TimeSpan pollTimeout)
    {
        ArgumentNullException.ThrowIfNull(consumer);
        ArgumentNullException.ThrowIfNull(producer);
        _consumer = consumer;
        _producer = producer;
        _pollTimeout = pollTimeout;
    }

    /// <inheritdoc />
    public Task<Redrive.Reserved?> PopAsync(string queue)
    {
        // A partition-EOF marker (EnablePartitionEof) is not a record and not "the DLQ is drained": keep polling
        // until the timeout elapses, so records waiting on another partition are still reached.
        var clock = Stopwatch.StartNew();
        while (true)
        {
            var remaining = _pollTimeout - clock.Elapsed;
            var result = _consumer.Consume(remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero);
            if (result is null)
            {
                return Task.FromResult<Redrive.Reserved?>(null);
            }

            if (result.IsPartitionEOF || result.Message is null)
            {
                if (clock.Elapsed >= _pollTimeout)
                {
                    return Task.FromResult<Redrive.Reserved?>(null);
                }

                continue;
            }

            // Strict decode: a lenient one would swap invalid bytes for U+FFFD, and a restore must never alter the
            // record. An unreadable (invalid UTF-8) or null value is handed out as an empty body, which Redrive
            // treats as undecodable and restores - and PublishFromAsync then re-writes the ORIGINAL bytes.
            return Task.FromResult<Redrive.Reserved?>(new Redrive.Reserved(DecodeStrict(result.Message.Value), result));
        }
    }

    /// <inheritdoc />
    public Task PublishAsync(string queue, string body) => SendAsync(queue, body, null);

    /// <inheritdoc />
    public Task PublishFromAsync(string queue, string body, Redrive.Reserved from)
    {
        var record = Record(from);
        return queue == record.Topic && body == from.Body
            ? RestoreAsync(record)
            : SendAsync(queue, body, record.Message.Key);
    }

    /// <inheritdoc />
    public Task AckAsync(Redrive.Reserved message)
    {
        _consumer.Commit(Record(message));
        return Task.CompletedTask;
    }

    /// <summary>Write the DLQ record back unchanged (value bytes, headers, key) with a fresh timestamp.</summary>
    private async Task RestoreAsync(ConsumeResult<byte[], byte[]> record)
    {
        var message = new Message<byte[], byte[]>
        {
            Key = record.Message.Key,
            Value = record.Message.Value,
            Headers = record.Message.Headers,
            Timestamp = new Timestamp(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), TimestampType.CreateTime),
        };
        await _producer.ProduceAsync(record.Topic, message).ConfigureAwait(false);
    }

    private static string DecodeStrict(byte[]? value)
    {
        if (value is null)
        {
            return string.Empty;
        }

        try
        {
            return StrictUtf8.GetString(value);
        }
        catch (DecoderFallbackException)
        {
            return string.Empty;
        }
    }

    private async Task SendAsync(string topic, string body, byte[]? key)
    {
        var message = new Message<byte[], byte[]>
        {
            Key = key!,
            Value = Encoding.UTF8.GetBytes(body),
            Timestamp = new Timestamp(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), TimestampType.CreateTime),
        };

        // EnvelopeCodec.Decode never throws: a body that is not an envelope yields one with no job, which is
        // published without header projection and with a fresh timestamp.
        var envelope = EnvelopeCodec.Decode(body);
        if (!string.IsNullOrWhiteSpace(envelope.Job))
        {
            message.Headers = KafkaHeaders.Project(envelope);
            if (envelope.Meta is { } meta)
            {
                message.Timestamp = new Timestamp(meta.CreatedAt, TimestampType.CreateTime);
            }
        }

        await _producer.ProduceAsync(topic, message).ConfigureAwait(false);
    }

    private static ConsumeResult<byte[], byte[]> Record(Redrive.Reserved message) =>
        (ConsumeResult<byte[], byte[]>)message.Handle!;
}
