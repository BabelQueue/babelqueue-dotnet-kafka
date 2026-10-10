using System.Globalization;
using System.Text;
using Confluent.Kafka;

namespace BabelQueue.Kafka;

/// <summary>
/// Consumes a Kafka work topic in <strong>process-then-commit</strong> mode (manual commit,
/// at-least-once): each record is decoded, validated, routed to the handler for its URN (read
/// from the <c>bq-job</c> header), and its offset committed only after the handler returns. A
/// throwing handler routes the envelope to a <c>&lt;topic&gt;.retry.&lt;n&gt;</c> tier with
/// <c>bq-attempts + 1</c> (the SDK-owned retry, §6.5), then commits; once max-tries is reached the
/// envelope goes to <c>&lt;topic&gt;.dlq</c> with a <c>dead_letter</c> block. Kafka exposes no
/// native delivery count, so the <c>bq-attempts</c> header is the authoritative counter (the
/// body's <c>attempts</c> is the fallback for non-BabelQueue records). Before decoding, the
/// <c>bq-schema-version</c> header is checked (§6.7): an absent or blank header decodes normally, any
/// value other than <c>"1"</c> sends the raw record to the DLQ without decoding it, then commits. If that DLQ
/// write fails the record is <b>not</b> committed: the consumer seeks back to its offset (so a later record's
/// cumulative commit cannot skip it), reports via <see cref="KafkaConsumerOptions.OnError"/> and the loop continues.
/// <para>
/// Any <b>other</b> failure that escapes <see cref="PollAsync"/> (a failed poison / retry / DLQ write, an unmapped
/// URN under <c>fail</c>, a cancellation mid-settlement) also leaves the record uncommitted <b>and seeks the consumer
/// back to its offset</b> before the exception is rethrown. <c>Consume</c> has already advanced the in-memory position,
/// and Kafka's commit is a cumulative per-partition position, so without the seek a caller that catches the
/// exception and polls again (or restarts <see cref="RunAsync"/>) would receive the next record, and committing
/// it would skip the unsettled one for good. With the seek, the next poll redelivers the same record. This relies
/// on <c>EnableAutoCommit = false</c> (see the README): an auto-committing consumer stores the offset itself.
/// </para>
/// </summary>
public sealed class KafkaConsumer
{
    private readonly IConsumer<byte[], byte[]> _consumer;
    private readonly IReadOnlyDictionary<string, BabelHandler> _handlers;
    private readonly KafkaConsumerOptions _options;

    // Set when a seek-back has been attempted for the record being handled, so the PollAsync safety net does
    // not seek (or wrap a seek failure) a second time. The consumer is single-threaded, like IConsumer itself.
    private bool _seekBackAttempted;

    public KafkaConsumer(
        IConsumer<byte[], byte[]> consumer,
        IReadOnlyDictionary<string, BabelHandler> handlers,
        KafkaConsumerOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(consumer);
        ArgumentNullException.ThrowIfNull(handlers);
        _consumer = consumer;
        _handlers = handlers;
        _options = options ?? new KafkaConsumerOptions();
    }

    /// <summary>Consume one record (up to the poll timeout), route + settle it. Returns 1, or 0 on timeout.</summary>
    public async Task<int> PollAsync(CancellationToken cancellationToken = default)
    {
        var result = _consumer.Consume(_options.PollTimeout);
        if (result?.Message is null)
        {
            return 0;
        }
        _seekBackAttempted = false;
        try
        {
            await HandleAsync(result, cancellationToken).ConfigureAwait(false);
        }
#pragma warning disable CA1031 // Any settlement failure must rewind the unsettled record, whatever the cause.
        catch (Exception error) when (!_seekBackAttempted)
#pragma warning restore CA1031
        {
            // The record was neither committed nor parked: rewind so a restarted loop resumes at THIS offset
            // instead of at the next record (whose cumulative commit would silently skip it).
            SeekBack(result, error);
            throw;
        }
        return 1;
    }

    /// <summary>Poll until <paramref name="cancellationToken"/> is cancelled.</summary>
    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            await PollAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// The <c>bq-schema-version</c> header as UTF-8, or null when it is absent <b>or carries a null value</b>
    /// (Kafka allows null header values; a null value counts as "absent" and falls through to the decode path).
    /// </summary>
    private static string? ReadSchemaVersionHeader(Headers? headers)
        => headers is not null && headers.TryGetLastBytes(KafkaHeaders.SchemaVersion, out var bytes) && bytes is not null
            ? Encoding.UTF8.GetString(bytes)
            : null;

    private async Task HandleAsync(ConsumeResult<byte[], byte[]> result, CancellationToken cancellationToken)
    {
        var headers = result.Message.Headers;

        // §6.7: version-gate on the bq-schema-version header before decoding the body. The value is
        // compared exactly as sent (no trimming), like the Pulsar/Go/Python/Node consumers (GR-5).
        var declaredVersion = ReadSchemaVersionHeader(headers);
        if (declaredVersion is not null && !IsBlankSchemaVersion(declaredVersion)
            && !string.Equals(
                declaredVersion,
                EnvelopeCodec.SchemaVersion.ToString(CultureInfo.InvariantCulture),
                StringComparison.Ordinal))
        {
            // The body is deliberately not decoded, so OnError gets an empty envelope. The raw record goes
            // to the DLQ (same path as a poison body) and the offset is committed. Settle FIRST, report
            // afterwards: a throwing OnError must never bypass the commit / seek-back (it is isolated below).
            var rejected = new BabelQueueException(
                $"Rejected a Kafka record: unsupported {KafkaHeaders.SchemaVersion} header '{declaredVersion}' "
                + $"(supported: {EnvelopeCodec.SchemaVersion}); body not decoded.");
            try
            {
                await DeadLetterRawAsync(result, cancellationToken).ConfigureAwait(false);
            }
#pragma warning disable CA1031 // A failed DLQ write must rewind the record, whatever the cause.
            catch (Exception dlqError) when (dlqError is not OperationCanceledException)
#pragma warning restore CA1031
            {
                // The gated record could not be parked. Do NOT commit: Consume() already advanced the
                // in-memory position, and a later record's cumulative commit would skip this one. Seek back
                // to its offset so the next poll redelivers it (it never reaches a handler: the gate rejects
                // it again), report, and back off for one poll interval so a persistent failure doesn't spin.
                await RewindAfterDeadLetterFailureAsync(result, rejected, dlqError, cancellationToken).ConfigureAwait(false);
                return;
            }
            Commit(result);
            SafeReport(rejected, EmptyEnvelope(), result);
            return;
        }

        var envelope = Reconcile(EnvelopeCodec.Decode(Value(result)), headers);

        if (!EnvelopeCodec.Accepts(envelope))
        {
            // A non-conformant / poison record: forward the raw bytes to the DLQ for triage.
            _options.OnError?.Invoke(
                new BabelQueueException("Rejected a non-conformant BabelQueue envelope from Kafka."), envelope, result);
            await DeadLetterRawAsync(result, cancellationToken).ConfigureAwait(false);
            Commit(result);
            return;
        }

        var urn = KafkaHeaders.GetString(headers, KafkaHeaders.Job) ?? EnvelopeCodec.Urn(envelope);
        if (!_handlers.TryGetValue(urn, out var handler))
        {
            await OnUnknownUrnAsync(result, envelope, urn, cancellationToken).ConfigureAwait(false);
            return;
        }

        try
        {
            await handler(envelope, result, cancellationToken).ConfigureAwait(false);
            Commit(result);
        }
#pragma warning disable CA1031 // The consume loop must survive any handler exception.
        catch (Exception error)
#pragma warning restore CA1031
        {
            _options.OnError?.Invoke(error, envelope, result);
            await RetryOrDeadLetterAsync(result, envelope, error, cancellationToken).ConfigureAwait(false);
            Commit(result);
        }
    }

    private async Task RewindAfterDeadLetterFailureAsync(
        ConsumeResult<byte[], byte[]> result, Exception rejected, Exception dlqError, CancellationToken cancellationToken)
    {
        SeekBack(result, dlqError);

        // The rewind is done; only now run the (isolated) callbacks.
        SafeReport(rejected, EmptyEnvelope(), result);
        SafeReport(
            new BabelQueueException(
                $"Failed to dead-letter a schema-version-gated Kafka record {result.Topic}[{result.Partition.Value}]"
                + $"@{result.Offset.Value}; rewound for redelivery, not committed.",
                dlqError),
            EmptyEnvelope(),
            result);
        try
        {
            await Task.Delay(_options.PollTimeout, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Shutting down: the rewind already happened, nothing to commit.
        }
    }

    /// <summary>
    /// Seeks the consumer back to <paramref name="result"/>'s own offset (nothing was committed for it).
    /// If the seek itself fails, throws a <see cref="BabelQueueException"/> carrying both the original
    /// <paramref name="cause"/> and the seek error, naming the record so it can be recovered by hand.
    /// </summary>
    private void SeekBack(ConsumeResult<byte[], byte[]> result, Exception cause)
    {
        _seekBackAttempted = true;
        try
        {
            _consumer.Seek(result.TopicPartitionOffset);
        }
#pragma warning disable CA1031 // Surface the seek failure together with the original failure.
        catch (Exception seekError)
#pragma warning restore CA1031
        {
            throw new BabelQueueException(
                $"Kafka seek back to {result.Topic}[{result.Partition.Value}]@{result.Offset.Value} failed after a "
                + "settlement failure; the unsettled record may be skipped by a later commit on this partition.",
                new AggregateException(cause, seekError));
        }
    }

    private static Envelope EmptyEnvelope() => new(null, null, null, null, 0, null);

    /// <summary>
    /// Invokes <see cref="KafkaConsumerOptions.OnError"/> on the schema-version-gate path, where it must not be
    /// able to disturb settlement: a throwing callback is swallowed (there is no safer sink than the consumer
    /// itself) so the already-performed commit / seek-back stands and the loop keeps running.
    /// </summary>
    private void SafeReport(Exception error, Envelope envelope, ConsumeResult<byte[], byte[]> result)
    {
        try
        {
            _options.OnError?.Invoke(error, envelope, result);
        }
#pragma warning disable CA1031 // A misbehaving observer must not bypass the gate's commit / seek-back.
        catch (Exception)
#pragma warning restore CA1031
        {
        }
    }

    /// <summary>
    /// True when the declared schema version is empty or consists <b>only</b> of ASCII whitespace
    /// (space, <c>\t</c>, <c>\n</c>, U+000B, <c>\f</c>, <c>\r</c>). Anything else (NBSP, U+0085, U+FEFF, …)
    /// is <b>not</b> blank, unlike <see cref="string.IsNullOrWhiteSpace"/>.
    /// </summary>
    private static bool IsBlankSchemaVersion(string value)
    {
        foreach (var c in value)
        {
            if (c != ' ' && (c < '\t' || c > '\r'))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary><c>bq-attempts</c> header is authoritative; the body <c>Attempts</c> is the fallback.</summary>
    private static Envelope Reconcile(Envelope envelope, Headers headers)
    {
        var attempts = KafkaHeaders.GetInt(headers, KafkaHeaders.Attempts, envelope.Attempts);
        return attempts == envelope.Attempts ? envelope : envelope with { Attempts = attempts };
    }

    private async Task OnUnknownUrnAsync(ConsumeResult<byte[], byte[]> result, Envelope envelope, string urn, CancellationToken cancellationToken)
    {
        switch (_options.UnknownUrn)
        {
            case UnknownUrnStrategy.Delete:
                Commit(result);
                return;
            case UnknownUrnStrategy.DeadLetter:
                await DeadLetterAsync(envelope, result, "unknown_urn", null, cancellationToken).ConfigureAwait(false);
                Commit(result);
                return;
            case UnknownUrnStrategy.Release:
                await RepublishRetryAsync(result, envelope, cancellationToken).ConfigureAwait(false);
                Commit(result);
                return;
            default:
                // Fail: surface and do NOT commit — PollAsync seeks back, so the record redelivers on the next poll.
                _options.OnError?.Invoke(new UnknownUrnException(urn), envelope, result);
                throw new UnknownUrnException(urn);
        }
    }

    private async Task RetryOrDeadLetterAsync(ConsumeResult<byte[], byte[]> result, Envelope envelope, Exception error, CancellationToken cancellationToken)
    {
        var hasTiers = _options.RetryTopics?.HasTiers ?? false;
        var hasDlq = (_options.RetryTopics?.DlqTopic) is not null;
        if (!hasTiers && !hasDlq)
        {
            throw new BabelQueueException("Kafka per-record retry requires retry topics and/or a DLQ; neither is configured.", error);
        }
        if (hasTiers && envelope.Attempts + 1 < _options.MaxTries)
        {
            await RepublishRetryAsync(result, envelope, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await DeadLetterAsync(envelope, result, "failed", error, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task RepublishRetryAsync(ConsumeResult<byte[], byte[]> result, Envelope envelope, CancellationToken cancellationToken)
    {
        var topics = _options.RetryTopics;
        if (topics is null || !topics.HasTiers)
        {
            throw new BabelQueueException("Kafka retry/release requires retry topics; none are configured.");
        }
        var tier = topics.TierForAttempt(envelope.Attempts);
        var bumped = envelope with { Attempts = envelope.Attempts + 1 };
        var headers = KafkaHeaders.Project(bumped);
        KafkaHeaders.Add(headers, KafkaHeaders.Delay, ((long)tier.Delay.TotalMilliseconds).ToString(System.Globalization.CultureInfo.InvariantCulture));
        KafkaHeaders.Add(headers, KafkaHeaders.OriginalTopic, OriginalTopic(result));
        await ProduceAsync(tier.Topic, bumped, headers, cancellationToken).ConfigureAwait(false);
    }

    private async Task DeadLetterAsync(Envelope envelope, ConsumeResult<byte[], byte[]> result, string reason, Exception? error, CancellationToken cancellationToken)
    {
        var dlq = _options.RetryTopics?.DlqTopic;
        if (dlq is null)
        {
            return; // dead-lettering disabled → degrade to commit-and-drop
        }
        var original = OriginalTopic(result);
        var annotated = DeadLetters.Annotate(envelope, reason, original, envelope.Attempts, error?.Message, error?.GetType().FullName);
        var headers = KafkaHeaders.Project(annotated);
        KafkaHeaders.Add(headers, KafkaHeaders.OriginalTopic, original);
        await ProduceAsync(dlq, annotated, headers, cancellationToken).ConfigureAwait(false);
    }

    private async Task DeadLetterRawAsync(ConsumeResult<byte[], byte[]> result, CancellationToken cancellationToken)
    {
        var dlq = _options.RetryTopics?.DlqTopic;
        if (dlq is null)
        {
            return;
        }
        RequireProducer();
        var message = new Message<byte[], byte[]>
        {
            Key = result.Message.Key,
            Value = result.Message.Value,
            Headers = result.Message.Headers,
            Timestamp = new Timestamp(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), TimestampType.CreateTime),
        };
        await _options.Producer!.ProduceAsync(dlq, message, cancellationToken).ConfigureAwait(false);
    }

    private async Task ProduceAsync(string topic, Envelope envelope, Headers headers, CancellationToken cancellationToken)
    {
        RequireProducer();
        var message = new Message<byte[], byte[]>
        {
            Value = Encoding.UTF8.GetBytes(EnvelopeCodec.Encode(envelope)),
            Headers = headers,
            Timestamp = new Timestamp(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), TimestampType.CreateTime),
        };
        await _options.Producer!.ProduceAsync(topic, message, cancellationToken).ConfigureAwait(false);
    }

    private void Commit(ConsumeResult<byte[], byte[]> result) => _consumer.Commit(result);

    private static string Value(ConsumeResult<byte[], byte[]> result) =>
        result.Message.Value is { } v ? Encoding.UTF8.GetString(v) : string.Empty;

    private static string OriginalTopic(ConsumeResult<byte[], byte[]> result) =>
        KafkaHeaders.GetString(result.Message.Headers, KafkaHeaders.OriginalTopic) ?? result.Topic;

    private void RequireProducer()
    {
        if (_options.Producer is null)
        {
            throw new BabelQueueException("This Kafka consumer needs a producer to republish (retry/DLQ).");
        }
    }
}
