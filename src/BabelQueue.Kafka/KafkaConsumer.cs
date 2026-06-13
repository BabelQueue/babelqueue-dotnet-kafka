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
/// body's <c>attempts</c> is the fallback for non-BabelQueue records).
/// </summary>
public sealed class KafkaConsumer
{
    private readonly IConsumer<byte[], byte[]> _consumer;
    private readonly IReadOnlyDictionary<string, BabelHandler> _handlers;
    private readonly KafkaConsumerOptions _options;

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
        await HandleAsync(result, cancellationToken).ConfigureAwait(false);
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

    private async Task HandleAsync(ConsumeResult<byte[], byte[]> result, CancellationToken cancellationToken)
    {
        var headers = result.Message.Headers;
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
                // Fail: surface and do NOT commit — the record redelivers on the next poll.
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
