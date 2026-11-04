using System.Globalization;
using System.Text;
using Confluent.Kafka;

namespace BabelQueue.Kafka;

/// <summary>
/// Projects the envelope's contract fields onto native Kafka record headers and reads them
/// back. All values are UTF-8 byte strings (Kafka headers are bytes); the conceptually-integer
/// headers (<c>bq-attempts</c>, <c>bq-schema-version</c>) are written as their decimal string
/// (e.g. <c>"1"</c>) and parsed back to ints. The body stays authoritative (Contract §6.3).
/// </summary>
internal static class KafkaHeaders
{
    public const string Job = "bq-job";
    public const string TraceId = "bq-trace-id";
    public const string MessageId = "bq-message-id";
    public const string SchemaVersion = "bq-schema-version";
    public const string SourceLang = "bq-source-lang";
    public const string Attempts = "bq-attempts";
    public const string Delay = "bq-delay";
    public const string OriginalTopic = "bq-original-topic";

    /// <summary>W3C trace-context headers, carried out of band (ADR-0028); never part of the envelope.</summary>
    public const string Traceparent = "traceparent";

    public const string Tracestate = "tracestate";

    /// <summary>
    /// The record headers the transport owns: the §6.3 projection plus the retry-topic pair of §6.4. They are never
    /// accepted from, nor surfaced as, out-of-band transport headers (parity with the Go reference).
    /// </summary>
    private static readonly HashSet<string> Contract = new(StringComparer.Ordinal)
    {
        Job, TraceId, MessageId, SchemaVersion, SourceLang, Attempts, Delay, OriginalTopic,
    };

    /// <summary>True when <paramref name="key"/> is a contract header the transport owns.</summary>
    public static bool IsContract(string key) => Contract.Contains(key);

    /// <summary>The mandatory <c>bq-</c> header set projected from the envelope (Contract §6.3).</summary>
    public static Headers Project(Envelope envelope)
    {
        var headers = new Headers();
        Put(headers, Job, envelope.Job);
        Put(headers, TraceId, envelope.TraceId);
        if (envelope.Meta is { } meta)
        {
            Put(headers, MessageId, meta.Id);
            if (meta.SchemaVersion > 0)
            {
                // 0 means "absent" (a lenient decode of a body without meta.schema_version); never project it.
                headers.Add(SchemaVersion, Bytes(meta.SchemaVersion.ToString(CultureInfo.InvariantCulture)));
            }
            Put(headers, SourceLang, meta.Lang);
        }
        headers.Add(Attempts, Bytes(envelope.Attempts.ToString(CultureInfo.InvariantCulture)));
        return headers;
    }

    /// <summary>
    /// Copies the W3C <c>traceparent</c> / <c>tracestate</c> headers of a consumed record onto the record that is
    /// republished for it (retry tier, DLQ), so the trace survives the hop (ADR-0028). Headers that are absent
    /// are not invented; the <c>bq-</c> projection is unaffected.
    /// </summary>
    public static void CarryTrace(Headers target, Headers? source)
    {
        if (source is null)
        {
            return;
        }
        foreach (var name in new[] { Traceparent, Tracestate })
        {
            if (source.TryGetLastBytes(name, out var bytes))
            {
                target.Add(name, bytes);
            }
        }
    }

    public static void Add(Headers headers, string key, string value) => headers.Add(key, Bytes(value));

    /// <summary>
    /// Overlays the out-of-band string <paramref name="extra"/> headers (e.g. a W3C <c>traceparent</c>, ADR-0028) onto
    /// an outgoing record's <paramref name="headers"/>, beside the <c>bq-</c> projection. A contract header name
    /// (<see cref="IsContract"/>, including <c>bq-delay</c> / <c>bq-original-topic</c>) is always dropped, a header
    /// already present is never overwritten, and empty keys/values are skipped. Keys are applied in
    /// ordinal order so the result is deterministic. A <c>null</c>/empty map is a no-op.
    /// </summary>
    public static void Merge(Headers headers, IReadOnlyDictionary<string, string>? extra)
    {
        if (extra is null || extra.Count == 0)
        {
            return;
        }

        foreach (var key in extra.Keys.OrderBy(static k => k, StringComparer.Ordinal))
        {
            var value = extra[key];
            if (string.IsNullOrEmpty(key) || string.IsNullOrEmpty(value) || IsContract(key) || headers.TryGetLastBytes(key, out _))
            {
                continue;
            }

            headers.Add(key, Bytes(value));
        }
    }

    /// <summary>The last header value for <paramref name="key"/> as a UTF-8 string, or null if absent.</summary>
    public static string? GetString(Headers? headers, string key)
    {
        if (headers is not null && headers.TryGetLastBytes(key, out var bytes))
        {
            return Encoding.UTF8.GetString(bytes);
        }
        return null;
    }

    /// <summary>The header value for <paramref name="key"/> as an int, or <paramref name="fallback"/>.</summary>
    public static int GetInt(Headers? headers, string key, int fallback)
    {
        var s = GetString(headers, key);
        return s is not null && int.TryParse(s.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var n)
            ? n
            : fallback;
    }

    private static void Put(Headers headers, string key, string? value)
    {
        if (!string.IsNullOrEmpty(value))
        {
            headers.Add(key, Bytes(value));
        }
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);
}

/// <summary>
/// Out-of-band transport-header carrier for Kafka (ADR-0028). A W3C <c>traceparent</c> (and <c>tracestate</c>) rides
/// as a UTF-8 record header <b>beside</b> the frozen envelope — never inside it — next to the §6 <c>bq-</c>
/// projection. This is the consume-side half: it surfaces a consumed record's headers as a flat map a handler hands to
/// <c>Telemetry.Wrap(handler, headers)</c>, so the consumer span becomes a true child of the producer span. With no
/// <c>traceparent</c> the core falls back to the v0.1 <c>trace_id</c> mapping.
/// </summary>
public static class KafkaTraceHeaders
{
    /// <summary>
    /// The headers of a consumed record as a flat map (the last value wins for a repeated key; empty when the record
    /// has none). Only out-of-band headers (e.g. <c>traceparent</c>, <c>tracestate</c>) surface: the <c>bq-</c>
    /// contract headers the transport owns are left out, so handing the map back to
    /// <c>PublishWithHeadersAsync</c> cannot re-inject them. Empty values are skipped.
    /// </summary>
    public static Dictionary<string, string> Extract(Headers? headers)
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        if (headers is null)
        {
            return map;
        }

        foreach (var header in headers)
        {
            var bytes = header.GetValueBytes();
            if (bytes is { Length: > 0 } && !KafkaHeaders.IsContract(header.Key))
            {
                map[header.Key] = Encoding.UTF8.GetString(bytes);
            }
        }

        return map;
    }
}
