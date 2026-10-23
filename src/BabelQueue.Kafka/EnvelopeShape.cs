using System.Globalization;
using System.Text.Json;

namespace BabelQueue.Kafka;

/// <summary>
/// The poison trigger set: a body is undecodable when a strict, typed decode into <c>Envelope</c> / <c>Meta</c> /
/// <c>DeadLetter</c> (the Go reference's <c>babelqueue.Decode</c>, except that member names are case-sensitive here)
/// rejects it.
/// <list type="bullet">
/// <item>the body must be exactly one JSON value (no BOM, trailing tokens or malformed numbers); a bare <c>null</c>
/// decodes to an empty envelope, any other non-object is rejected;</item>
/// <item><c>trace_id</c>, <c>meta.id|queue|lang</c> and <c>dead_letter.reason|error|exception|original_queue|lang</c>
/// are string-or-null;</item>
/// <item><c>meta.schema_version|created_at</c>, <c>attempts</c> and <c>dead_letter.failed_at|attempts</c> are
/// integer-or-null: an integer literal (no fraction or exponent) that fits int64;</item>
/// <item><c>data</c>, <c>meta</c> and <c>dead_letter</c> are object-or-null; numbers inside <c>data</c> must be finite
/// doubles;</item>
/// <item>member names are matched exactly as spelled (case-sensitive, as the contract schema and the core decoder read
/// them) and every occurrence of a duplicated key is checked; any other member (a differently-cased name or a
/// non-string <c>urn</c> alias included) is only checked for syntax;</item>
/// <item><c>job</c> is deliberately not type-checked (ADR-0032 section 6): a non-string <c>job</c> is ignored, the last
/// duplicate wins and the identity rule then applies; a body without a valid <c>job</c>/<c>urn</c> identity is not an
/// undecodable one here, the consumer parks it as poison on its own identity check.</item>
/// </list>
/// A well-typed but non-conformant envelope is <b>not</b> poison (Go's consume path never calls <c>Accepts</c>).
/// </summary>
internal static class EnvelopeShape
{
    private enum Kind { String, Integer, Object, Data }

    private static readonly Dictionary<string, Kind> Meta = new(StringComparer.Ordinal)
    {
        ["id"] = Kind.String, ["queue"] = Kind.String, ["lang"] = Kind.String,
        ["schema_version"] = Kind.Integer, ["created_at"] = Kind.Integer,
    };

    private static readonly Dictionary<string, Kind> DeadLetter = new(StringComparer.Ordinal)
    {
        ["reason"] = Kind.String, ["error"] = Kind.String, ["exception"] = Kind.String,
        ["original_queue"] = Kind.String, ["lang"] = Kind.String,
        ["failed_at"] = Kind.Integer, ["attempts"] = Kind.Integer,
    };

    private static readonly Dictionary<string, Kind> Top = new(StringComparer.Ordinal)
    {
        ["trace_id"] = Kind.String, ["attempts"] = Kind.Integer,
        ["data"] = Kind.Data, ["meta"] = Kind.Object, ["dead_letter"] = Kind.Object,
    };

    /// <summary>Why <paramref name="body"/> is not a decodable BabelQueue envelope, or null when it is.</summary>
    internal static BabelQueueException? Violation(string body)
    {
        JsonDocument doc;
        try
        {
            doc = JsonDocument.Parse(body, new JsonDocumentOptions { MaxDepth = 10_000 });
        }
        catch (JsonException e)
        {
            return Undecodable(e.Message, e);
        }
        using (doc)
        {
            var root = doc.RootElement;
            if (root.ValueKind == JsonValueKind.Null)
            {
                return null;
            }
            if (root.ValueKind != JsonValueKind.Object)
            {
                return Undecodable("expected a JSON object.", null);
            }
            string? problem;
            try
            {
                problem = CheckObject(root, "", Top, null);
            }
            catch (InvalidOperationException e)
            {
                // System.Text.Json cannot materialise a member name holding a lone UTF-16 surrogate escape
                // (e.g. "\ud800"); the core codec cannot carry it either, so it is poison here, never a throw.
                return Undecodable("a member name is not representable (lone UTF-16 surrogate escape).", e);
            }
            if (problem is not null)
            {
                return Undecodable(problem, null);
            }
            return ExceedsCoreDepth(root)
                ? Undecodable("nesting is deeper than the core codec can decode (" + CoreMaxDepth + " levels); the body is parked verbatim.", null)
                : null;
        }
    }

    /// <summary>
    /// Whether <paramref name="root"/> nests deeper than the core codec can decode. The shape scan accepts Go's 10000
    /// levels, but <c>EnvelopeCodec.Decode</c> (default <c>System.Text.Json</c> depth of 64) turns anything deeper into a
    /// silent <i>empty</i> envelope instead of an exception, which would reach a handler with its content dropped (or wedge
    /// the partition on the unknown-URN path). Such a body is poison here instead: parked verbatim in the DLQ, never lost,
    /// never stuck. Measured on the document the shape scan already parsed (no second parse); iterative because a recursive
    /// walk cannot survive 10000 levels on a consumer thread's stack.
    /// </summary>
    private static bool ExceedsCoreDepth(JsonElement root)
    {
        var pending = new Stack<(JsonElement Element, int Depth)>();
        pending.Push((root, 1));
        while (pending.Count > 0)
        {
            var (element, depth) = pending.Pop();
            switch (element.ValueKind)
            {
                case JsonValueKind.Object:
                    if (depth > CoreMaxDepth)
                    {
                        return true;
                    }
                    foreach (var property in element.EnumerateObject())
                    {
                        pending.Push((property.Value, depth + 1));
                    }
                    break;
                case JsonValueKind.Array:
                    if (depth > CoreMaxDepth)
                    {
                        return true;
                    }
                    foreach (var item in element.EnumerateArray())
                    {
                        pending.Push((item, depth + 1));
                    }
                    break;
            }
        }
        return false;
    }

    /// <summary>The core codec's JSON nesting limit (System.Text.Json default).</summary>
    internal const int CoreMaxDepth = 64;

    private static BabelQueueException Undecodable(string message, Exception? inner) =>
        inner is null
            ? new BabelQueueException("Undecodable BabelQueue body: " + message)
            : new BabelQueueException("Undecodable BabelQueue body: " + message, inner);

    private static string? CheckObject(JsonElement obj, string path, Dictionary<string, Kind> members, Dictionary<string, Dictionary<string, Kind>>? nested)
    {
        foreach (var property in obj.EnumerateObject())
        {
            if (!members.TryGetValue(property.Name, out var kind))
            {
                continue; // unknown member: syntax only
            }
            var value = property.Value;
            var name = path + property.Name;
            switch (kind)
            {
                case Kind.String:
                    if (value.ValueKind is not (JsonValueKind.String or JsonValueKind.Null))
                    {
                        return $"'{name}' must be a string.";
                    }
                    break;
                case Kind.Integer:
                    if (value.ValueKind == JsonValueKind.Null)
                    {
                        break;
                    }
                    if (value.ValueKind != JsonValueKind.Number || !IsInt64Literal(value.GetRawText()))
                    {
                        return $"'{name}' must be an integer.";
                    }
                    break;
                case Kind.Data:
                    if (value.ValueKind == JsonValueKind.Null)
                    {
                        break;
                    }
                    if (value.ValueKind != JsonValueKind.Object)
                    {
                        return $"'{name}' must be a JSON object.";
                    }
                    if (!AllNumbersFinite(value))
                    {
                        return $"'{name}' holds a number that is not a finite double.";
                    }
                    break;
                case Kind.Object:
                    if (value.ValueKind == JsonValueKind.Null)
                    {
                        break;
                    }
                    if (value.ValueKind != JsonValueKind.Object)
                    {
                        return $"'{name}' must be a JSON object.";
                    }
                    var inner = string.Equals(property.Name, "meta", StringComparison.Ordinal) ? Meta : DeadLetter;
                    var problem = CheckObject(value, name + ".", inner, null);
                    if (problem is not null)
                    {
                        return problem;
                    }
                    break;
            }
        }
        return null;
    }

    /// <summary>An integer literal (optional minus, no fraction or exponent) that fits int64, as Go's typed decode requires.</summary>
    private static bool IsInt64Literal(string raw) =>
        long.TryParse(raw, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out _)
        && raw.AsSpan().IndexOfAny('.', 'e', 'E') < 0;

    // Iterative on purpose: the scan accepts Go's 10000 levels of nesting, which a recursive walk cannot survive on a
    // consumer thread's stack (a StackOverflow cannot be caught and would kill the process).
    private static bool AllNumbersFinite(JsonElement root)
    {
        var pending = new Stack<JsonElement>();
        pending.Push(root);
        while (pending.Count > 0)
        {
            var element = pending.Pop();
            switch (element.ValueKind)
            {
                case JsonValueKind.Number:
                    if (!(double.TryParse(element.GetRawText(), NumberStyles.Float, CultureInfo.InvariantCulture, out var d)
                        && double.IsFinite(d)))
                    {
                        return false;
                    }
                    break;
                case JsonValueKind.Object:
                    foreach (var property in element.EnumerateObject())
                    {
                        pending.Push(property.Value);
                    }
                    break;
                case JsonValueKind.Array:
                    foreach (var item in element.EnumerateArray())
                    {
                        pending.Push(item);
                    }
                    break;
            }
        }
        return true;
    }
}
