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
