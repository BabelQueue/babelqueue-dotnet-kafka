using System.Globalization;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
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

        // Undecodable / poison body (§6.5): dead-letter it WRAPPED (reason "poison"), as the Go reference does, with
        // the record key unchanged. Invalid UTF-8 is poison too (a lenient decoder would hand a corrupted payload
        // to a handler); the wrapper then carries the exact bytes as base64 with "raw_encoding": "base64" (ADR-0035).
        // Settle FIRST, report afterwards (as on the schema-version gate): a throwing OnError must never bypass
        // the DLQ write / commit, or the record would be redelivered and fail forever. A failed write seeks back
        // without committing, like every other poison record.
        var body = StrictBody(result);
        var undecodable = body is null
            ? new BabelQueueException("Rejected a Kafka record: the body is not valid UTF-8.")
            : DecodeFailure(body);
        if (undecodable is not null)
        {
            await SettlePoisonAsync(result, undecodable, EmptyEnvelope(), cancellationToken)
                .ConfigureAwait(false);
            return;
        }

        // The shape scan accepts everything Go's typed Decode accepts, but the core codec is stricter in places
        // (e.g. a lone surrogate escape it cannot carry). Whatever it throws, the record is poison: it must be parked,
        // never allowed to escape PollAsync (a seek-back + redelivery would wedge the partition on it forever).
        Envelope envelope;
        try
        {
            envelope = Reconcile(EnvelopeCodec.Decode(body!), headers);
        }
#pragma warning disable CA1031 // Any decode failure is poison (Go parity), whatever its type.
        catch (Exception decodeError) when (decodeError is not OperationCanceledException)
#pragma warning restore CA1031
        {
            await SettlePoisonAsync(
                result,
                decodeError as BabelQueueException ?? new BabelQueueException("Undecodable BabelQueue body: " + decodeError.Message, decodeError),
                EmptyEnvelope(),
                cancellationToken).ConfigureAwait(false);
            return;
        }

        var urn = KafkaHeaders.GetString(headers, KafkaHeaders.Job) ?? EnvelopeCodec.Urn(envelope);
        if (urn.Length == 0)
        {
            // ADR-0034 §10 / ADR-0032 §6: a body with no valid job/urn identity (and no bq-job header) has the same fate
            // as an undecodable one on Kafka: parked in <topic>.dlq with reason "poison", the handler is never called.
            await SettlePoisonAsync(
                result,
                new BabelQueueException("Undecodable BabelQueue body: neither 'job' nor 'urn' is a valid identity."),
                EmptyEnvelope(),
                cancellationToken).ConfigureAwait(false);
            return;
        }
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
            // Settle FIRST (retry republish / DLQ), report afterwards: a throwing OnError must never bypass
            // the settlement, or the record would be redelivered to the failing handler forever.
            try
            {
                await RetryOrDeadLetterAsync(result, envelope, error, cancellationToken).ConfigureAwait(false);
                Commit(result);
            }
#pragma warning disable CA1031 // Seek back, then report the handler failure, whatever the settlement failure was.
            catch (Exception settleError)
#pragma warning restore CA1031
            {
                SeekBackThenReport(result, settleError, error, envelope);
                throw;
            }
            SafeReport(error, envelope, result);
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

    /// <summary>
    /// Settlement failed (the DLQ / retry write or the commit): rewind the record first, then hand the original
    /// <paramref name="reported"/> error to <see cref="KafkaConsumerOptions.OnError"/>. The report runs even when the
    /// seek itself fails, and the caller rethrows the settlement failure (or this method throws the seek failure).
    /// </summary>
    private void SeekBackThenReport(
        ConsumeResult<byte[], byte[]> result, Exception settleError, Exception reported, Envelope envelope)
    {
        try
        {
            SeekBack(result, settleError);
        }
        finally
        {
            SafeReport(reported, envelope, result);
        }
    }

    private static Envelope EmptyEnvelope() => new(null, null, null, null, 0, null);

    /// <summary>
    /// Invokes <see cref="KafkaConsumerOptions.OnError"/> on every settlement path (schema-version gate, invalid
    /// UTF-8 body, undecodable body, handler failure, unknown URN), where it must not be able to disturb
    /// settlement: a throwing callback is swallowed (there is no safer sink than the consumer itself) so the
    /// already-performed commit / seek-back stands and the loop keeps running.
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
                SafeReport(new UnknownUrnException(urn), envelope, result);
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
        KafkaHeaders.CarryTrace(headers, result.Message.Headers);
        await ProduceAsync(tier.Topic, result.Message.Key, bumped, headers, cancellationToken).ConfigureAwait(false);
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
        KafkaHeaders.CarryTrace(headers, result.Message.Headers);
        await ProduceAsync(dlq, result.Message.Key, annotated, headers, cancellationToken).ConfigureAwait(false);
    }

    private async Task SettlePoisonAsync(
        ConsumeResult<byte[], byte[]> result, Exception cause, Envelope envelope, CancellationToken cancellationToken)
    {
        try
        {
            await DeadLetterPoisonAsync(result, cause, cancellationToken).ConfigureAwait(false);
            Commit(result);
        }
#pragma warning disable CA1031 // Seek back, then report the rejection, whatever the settlement failure was.
        catch (Exception settleError)
#pragma warning restore CA1031
        {
            SeekBackThenReport(result, settleError, cause, envelope);
            throw;
        }
        SafeReport(cause, envelope, result);
    }

    /// <summary>
    /// Why <paramref name="body"/> is not a decodable BabelQueue envelope, or null when it is: exactly the bodies the
    /// Go reference's typed <c>Decode</c> rejects (see <see cref="EnvelopeShape"/>). A well-typed object that merely
    /// fails <see cref="EnvelopeCodec.Accepts"/> is not undecodable.
    /// </summary>
    internal static BabelQueueException? DecodeFailure(string body) => EnvelopeShape.Violation(body);

    private static readonly JsonWriterOptions PoisonWriterOptions = new()
    {
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        Indented = false,
    };

    /// <summary>
    /// The poison DLQ record (ADR-0035): a body that is a strict JSON object without a top-level <c>dead_letter</c>
    /// key is spliced byte for byte (no value is re-encoded); anything else is carried as <c>raw</c> (plain text when
    /// valid UTF-8, otherwise base64 plus <c>raw_encoding: "base64"</c>). The <c>dead_letter</c> block has reason
    /// <c>poison</c>.
    /// </summary>
    internal static byte[] PoisonBody(byte[] body, string? error, string? exception, long failedAt, string originalQueue, int attempts)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream, PoisonWriterOptions))
        {
            writer.WriteStartObject();
            writer.WriteString("reason", "poison");
            if (error is null) { writer.WriteNull("error"); } else { writer.WriteString("error", error); }
            if (exception is null) { writer.WriteNull("exception"); } else { writer.WriteString("exception", exception); }
            writer.WriteNumber("failed_at", failedAt);
            writer.WriteString("original_queue", originalQueue);
            writer.WriteNumber("attempts", attempts);
            writer.WriteString("lang", EnvelopeCodec.SourceLang);
            writer.WriteEndObject();
        }
        return PoisonRecord.Build(body, Encoding.UTF8.GetString(stream.ToArray()));
    }

    private async Task DeadLetterPoisonAsync(ConsumeResult<byte[], byte[]> result, Exception cause, CancellationToken cancellationToken)
    {
        var dlq = _options.RetryTopics?.DlqTopic;
        if (dlq is null)
        {
            return; // dead-lettering disabled → degrade to commit-and-drop
        }
        RequireProducer();
        var original = OriginalTopic(result);
        var attempts = Math.Max(KafkaHeaders.GetInt(result.Message.Headers, KafkaHeaders.Attempts, 0), 0);
        var record = PoisonBody(result.Message.Value ?? Array.Empty<byte>(), cause.Message, cause.GetType().FullName, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), original, attempts);
        Headers headers;
        try
        {
            headers = KafkaHeaders.Project(EnvelopeCodec.Decode(Encoding.UTF8.GetString(record), static _ => { }));
        }
#pragma warning disable CA1031 // Header projection is best-effort: the poison record must be parked regardless.
        catch (Exception)
#pragma warning restore CA1031
        {
            headers = new Headers();
        }
        KafkaHeaders.Add(headers, KafkaHeaders.OriginalTopic, original);
        KafkaHeaders.CarryTrace(headers, result.Message.Headers);
        var message = new Message<byte[], byte[]>
        {
            Key = result.Message.Key,
            Value = record,
            Headers = headers,
            Timestamp = new Timestamp(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), TimestampType.CreateTime),
        };
        await _options.Producer!.ProduceAsync(dlq, message, cancellationToken).ConfigureAwait(false);
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

    // The source record's key is carried unchanged (null stays null, empty stays empty, nothing is synthesised) so a
    // keyed stream keeps its key across retry tiers and the DLQ.
    private async Task ProduceAsync(string topic, byte[]? key, Envelope envelope, Headers headers, CancellationToken cancellationToken)
    {
        RequireProducer();
        var message = new Message<byte[], byte[]>
        {
            Key = key!,
            Value = Encoding.UTF8.GetBytes(EnvelopeCodec.Encode(envelope)),
            Headers = headers,
            Timestamp = new Timestamp(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), TimestampType.CreateTime),
        };
        await _options.Producer!.ProduceAsync(topic, message, cancellationToken).ConfigureAwait(false);
    }

    private void Commit(ConsumeResult<byte[], byte[]> result) => _consumer.Commit(result);

    // Strict UTF-8 (no BOM emission, throws on invalid bytes) instead of Encoding.UTF8's silent U+FFFD replacement.
    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    /// <summary>The record value as a string, "" for a null value, or null when it is not valid UTF-8.</summary>
    private static string? StrictBody(ConsumeResult<byte[], byte[]> result)
    {
        if (result.Message.Value is not { } v)
        {
            return string.Empty;
        }
        try
        {
            return StrictUtf8.GetString(v);
        }
        catch (DecoderFallbackException)
        {
            return null;
        }
    }

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
