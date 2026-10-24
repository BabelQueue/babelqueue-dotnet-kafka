using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using BabelQueue;
using BabelQueue.Kafka;
using Confluent.Kafka;
using Moq;

namespace BabelQueue.Kafka.Tests;

/// <summary>One manifest case of a section this package carries.</summary>
internal sealed record SuiteCase(string Section, string Id, Func<Task> Run);

internal sealed class ConformanceFailure : Exception
{
    public ConformanceFailure(string message) : base(message)
    {
    }
}

/// <summary>
/// Every case of the Kafka-scoped sections of the vendored conformance manifest (<c>kafka</c>, <c>backoff</c>,
/// <c>kafka_poison</c>, plus the core-scoped <c>poison_dlq</c> whose writer lives here), each runnable on its own, plus the <c>results.json</c> writer of ADR-0032. Case ids follow
/// <c>conformance/schema/results.schema.json</c>: a top-level <c>cases</c> array gives <c>&lt;name&gt;</c> (or
/// <c>&lt;section&gt;#&lt;idx&gt;</c>), a sub-key with its own <c>cases</c> gives <c>&lt;sub-key&gt;/&lt;name&gt;</c> (or
/// <c>&lt;sub-key&gt;/&lt;idx&gt;</c>), any other object sub-key is one case named after it. A case with no runner here is
/// reported as a failure, never silently skipped, so a manifest that grows cannot pass unnoticed.
/// </summary>
internal static class KafkaConformanceSuite
{
    private const string Urn = "urn:babel:orders:created";
    // Scope "core" in the manifest, but the writer of the poison DLQ record (ADR-0035) is this package's consumer.
    private const string PoisonDlqSection = "poison_dlq";
    private static readonly string Dir = Path.Combine(AppContext.BaseDirectory, "conformance");

    private static readonly Lazy<JsonElement> Manifest = new(() =>
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(Path.Combine(Dir, "manifest.json")));
        return doc.RootElement.Clone();
    });

    internal static string ConformanceVersion => File.ReadAllText(Path.Combine(Dir, "CONFORMANCE_VERSION")).Trim();

    /// <summary>The manifest sections whose declared scope is <c>kafka</c> (the in-scope sections of this package).</summary>
    internal static IReadOnlyList<string> Sections() =>
        Manifest.Value.GetProperty("sections").EnumerateObject()
            .Where(s => s.Value.GetProperty("scope").GetString() == "kafka" || s.Name == PoisonDlqSection)
            .Select(s => s.Name)
            .ToList();

    internal static IReadOnlyList<SuiteCase> All()
    {
        var cases = new List<SuiteCase>();
        foreach (var section in Sections())
        {
            var body = Manifest.Value.GetProperty(section);
            if (body.TryGetProperty("cases", out var top) && top.ValueKind == JsonValueKind.Array)
            {
                var index = 0;
                foreach (var c in top.EnumerateArray())
                {
                    var id = Name(c) ?? section + "#" + index;
                    cases.Add(Case(section, id, null, c));
                    index++;
                }
            }

            foreach (var sub in body.EnumerateObject())
            {
                if (sub.Name is "cases" or "description" or "schema" || sub.Value.ValueKind != JsonValueKind.Object)
                {
                    continue;
                }

                if (sub.Value.TryGetProperty("cases", out var nested) && nested.ValueKind == JsonValueKind.Array)
                {
                    var index = 0;
                    foreach (var c in nested.EnumerateArray())
                    {
                        var id = sub.Name + "/" + (Name(c) ?? index.ToString(CultureInfo.InvariantCulture));
                        cases.Add(Case(section, id, sub, c));
                        index++;
                    }
                }
                else
                {
                    cases.Add(Case(section, sub.Name, sub, sub.Value));
                }
            }
        }

        return cases;
    }

    private static string? Name(JsonElement c) =>
        c.ValueKind == JsonValueKind.Object && c.TryGetProperty("name", out var n) && n.ValueKind == JsonValueKind.String ? n.GetString() : null;

    private static SuiteCase Case(string section, string id, JsonProperty? sub, JsonElement c)
    {
        Func<Task>? run = (section, sub?.Name) switch
        {
            ("kafka", "property_projection") => () => PropertyProjection(c),
            ("kafka", "attempts_reconciliation") => () => AttemptsReconciliation(c),
            ("kafka", "schema_version_gate") => () => SchemaVersionGate(sub!.Value.Value, c),
            ("backoff", null) => () => RequestedDelay(c),
            ("backoff", "retry_republish") => () => RetryRepublish(c, "attempts"),
            ("backoff", "retry_attempt_tier") => () => RetryRepublish(c, "attempts_before"),
            ("backoff", "topic_names") => () => TopicNames(c),
            ("backoff", "default_tiers") => () => DefaultTiers(c),
            ("kafka_poison", null) => () => Poison(c),
            ("poison_dlq", null) => () => PoisonDlq(c),
            _ => null,
        };
        return new SuiteCase(section, id, run ?? (() => throw new ConformanceFailure($"no runner for {section}/{id}")));
    }

    // -- kafka ------------------------------------------------------------------------------------------------------

    private static Task PropertyProjection(JsonElement projection)
    {
        var body = File.ReadAllText(Path.Combine(Dir, projection.GetProperty("envelope_file").GetString()!));
        var headers = KafkaHeaders.Project(EnvelopeCodec.Decode(body));
        foreach (var golden in projection.GetProperty("headers").EnumerateObject())
        {
            Require(golden.Value.GetString() == KafkaHeaders.GetString(headers, golden.Name), $"header {golden.Name} differs from the golden value");
        }

        return Task.CompletedTask;
    }

    private static async Task AttemptsReconciliation(JsonElement testCase)
    {
        var env = EnvelopeCodec.Make(Urn, new Dictionary<string, object?> { ["x"] = 1 }, "orders")
            with { Attempts = testCase.GetProperty("body_attempts").GetInt32() };
        var headers = new Headers();
        var headerAttempts = testCase.GetProperty("header_attempts");
        if (headerAttempts.ValueKind != JsonValueKind.Null)
        {
            KafkaHeaders.Add(headers, KafkaHeaders.Attempts, headerAttempts.GetInt32().ToString(CultureInfo.InvariantCulture));
        }

        var consumer = ConsumerOf(Record("orders", EnvelopeCodec.Encode(env), headers));
        var seen = -1;
        var handlers = new Dictionary<string, BabelHandler> { [Urn] = (e, _, _) => { seen = e.Attempts; return Task.CompletedTask; } };
        await new KafkaConsumer(consumer.Object, handlers).PollAsync();
        Require(seen == testCase.GetProperty("expected_attempts").GetInt32(), $"handler saw attempts {seen}");
    }

    private static async Task SchemaVersionGate(JsonElement gate, JsonElement testCase)
    {
        var property = gate.GetProperty("property").GetString()!;
        var body = File.ReadAllText(Path.Combine(Dir, gate.GetProperty("fixture").GetString()!));
        var job = EnvelopeCodec.Decode(body).Job!;
        var absent = testCase.TryGetProperty("absent", out var a) && a.GetBoolean();
        var value = absent ? null : testCase.GetProperty("value").GetString();
        var decode = testCase.GetProperty("expect").GetString() == "decode";

        var headers = new Headers();
        if (!absent)
        {
            headers.Add(property, value is null ? null : Encoding.UTF8.GetBytes(value));
        }

        var consumer = ConsumerOf(Record("orders", body, headers));
        var sent = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var seen = 0;
        var handlers = new Dictionary<string, BabelHandler> { [job] = (_, _, _) => { seen++; return Task.CompletedTask; } };
        await new KafkaConsumer(consumer.Object, handlers, new KafkaConsumerOptions
        {
            Producer = ProducerInto(sent).Object,
            RetryTopics = RetryTopics.ForTopic("orders").Tier(TimeSpan.FromSeconds(5)).Build(),
        }).PollAsync();

        Require((decode ? 1 : 0) == seen, $"expected {(decode ? "decode" : "reject")} but the handler ran {seen} time(s)");
        Require(decode ? sent.Count == 0 : sent.Select(s => s.Topic).SequenceEqual(new[] { "orders.dlq" }), "unexpected republish");
        consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Once);
    }

    // -- backoff ----------------------------------------------------------------------------------------------------

    private static RetryTopics Topology(JsonElement testCase)
    {
        var builder = RetryTopics.ForTopic(testCase.GetProperty("work_topic").GetString()!);
        foreach (var tier in testCase.GetProperty("retry_tiers_ms").EnumerateArray())
        {
            builder.Tier(TimeSpan.FromMilliseconds(tier.GetInt64()));
        }

        return builder.Build();
    }

    private static async Task RequestedDelay(JsonElement testCase)
    {
        var topics = Topology(testCase);
        var delay = testCase.GetProperty("delay_ms").GetInt64();
        var sent = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var publisher = KafkaPublisher.Create(ProducerInto(sent).Object, topics);
        var expect = testCase.GetProperty("expect");

        if (expect.ValueKind == JsonValueKind.String)
        {
            Require(expect.GetString() == "reject", "unknown expect");
            await Rejects(() => publisher.PublishAsync(Urn, null, null, TimeSpan.FromMilliseconds(delay)));
            Require(sent.Count == 0, "a rejected delay must not publish");
            return;
        }

        await publisher.PublishAsync(Urn, null, null, TimeSpan.FromMilliseconds(delay));
        Require(sent.Count == 1, $"expected one record, got {sent.Count}");
        var tierMs = expect.GetProperty("tier_ms").GetInt64();
        var tier = topics.Tiers.Single(t => (long)t.Delay.TotalMilliseconds == tierMs);
        Require(sent[0].Topic == tier.Topic, $"topic {sent[0].Topic}, expected {tier.Topic}");
        RequireHeaders(sent[0].Message.Headers, expect.GetProperty("headers"));
    }

    private static async Task RetryRepublish(JsonElement testCase, string attemptsField)
    {
        var workTopic = testCase.GetProperty("work_topic").GetString()!;
        var topics = Topology(testCase);
        var attempts = testCase.GetProperty(attemptsField).GetInt32();
        var expect = testCase.GetProperty("expect");
        byte[]? key = testCase.TryGetProperty("key", out var k) ? Encoding.UTF8.GetBytes(k.GetString()!) : null;

        // The release/retry entry point: a record of an unrouted URN under the Release strategy is republished to its tier.
        var envelope = EnvelopeCodec.Make(Urn, new Dictionary<string, object?> { ["x"] = 1 }, workTopic) with { Attempts = attempts };
        var record = Record(workTopic, EnvelopeCodec.Encode(envelope), new Headers());
        record.Message.Key = key!;
        var consumer = ConsumerOf(record);
        var sent = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var kafka = new KafkaConsumer(consumer.Object, new Dictionary<string, BabelHandler>(), new KafkaConsumerOptions
        {
            Producer = ProducerInto(sent).Object,
            RetryTopics = topics,
            UnknownUrn = UnknownUrnStrategy.Release,
        });

        if (expect.ValueKind == JsonValueKind.String)
        {
            Require(expect.GetString() == "reject", "unknown expect");
            await Rejects(() => kafka.PollAsync());
            Require(sent.Count == 0, "a rejected retry must not publish");
            return;
        }

        await kafka.PollAsync();
        Require(sent.Count == 1, $"expected one record, got {sent.Count}");
        var expectedTopic = expect.GetProperty("topic").GetString()!;
        Require(sent[0].Topic == expectedTopic, $"topic {sent[0].Topic}, expected {expectedTopic}");
        var tierMs = expect.GetProperty("tier_ms").GetInt64();
        Require(topics.TierForAttempt(attempts).Topic == expectedTopic && (long)topics.TierForAttempt(attempts).Delay.TotalMilliseconds == tierMs,
            "RetryTopics.TierForAttempt disagrees with the manifest");
        RequireHeaders(sent[0].Message.Headers, expect.GetProperty("headers"));
        var bodyAttempts = EnvelopeCodec.Decode(Encoding.UTF8.GetString(sent[0].Message.Value)).Attempts;
        Require(bodyAttempts == expect.GetProperty("body_attempts").GetInt32(), $"body attempts {bodyAttempts}");
        if (expect.TryGetProperty("key", out var expectedKey))
        {
            Require(sent[0].Message.Key is not null && sent[0].Message.Key.SequenceEqual(Encoding.UTF8.GetBytes(expectedKey.GetString()!)), "the record key was not forwarded byte for byte");
        }
    }

    private static Task TopicNames(JsonElement testCase)
    {
        var topics = RetryTopics.ForTopic(testCase.GetProperty("work_topic").GetString()!);
        var n = testCase.GetProperty("n").GetInt32();
        for (var i = 1; i <= n; i++)
        {
            topics.Tier(TimeSpan.FromSeconds(i));
        }

        var actual = topics.Build().Tiers[n - 1].Topic;
        Require(actual == testCase.GetProperty("expect_topic").GetString(), $"retry topic {actual}");
        return Task.CompletedTask;
    }

    private static Task DefaultTiers(JsonElement testCase)
    {
        var expected = testCase.GetProperty("expect").EnumerateArray().Select(e => e.GetInt64()).ToArray();
        Require(RetryTopics.RecommendedTierMillis.Select(m => (long)m).SequenceEqual(expected), "RecommendedTierMillis differs from the manifest");
        return Task.CompletedTask;
    }

    // -- kafka_poison -----------------------------------------------------------------------------------------------

    private static async Task Poison(JsonElement testCase)
    {
        var body = testCase.GetProperty("body").GetString()!;
        var expect = testCase.GetProperty("expect").GetString()!;
        var (verdict, identity) = Classify(body);
        Require(verdict == expect, $"classified {verdict}, expected {expect}");

        var consumer = ConsumerOf(Record("orders", body, new Headers()));
        var sent = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var handled = 0;
        var handlers = new Dictionary<string, BabelHandler> { [identity.Length == 0 ? Urn : identity] = (_, _, _) => { handled++; return Task.CompletedTask; } };
        await new KafkaConsumer(consumer.Object, handlers, new KafkaConsumerOptions
        {
            Producer = ProducerInto(sent).Object,
            RetryTopics = RetryTopics.ForTopic("orders").Tier(TimeSpan.FromSeconds(5)).Build(),
        }).PollAsync();

        consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Once);
        if (expect == "ok")
        {
            Require(handled == 1 && sent.Count == 0, "an ok case must reach the handler with no DLQ record");
            return;
        }

        var reason = sent.Count == 1 && sent[0].Topic == "orders.dlq"
            ? JsonDocument.Parse(sent[0].Message.Value).RootElement.GetProperty("dead_letter").GetProperty("reason").GetString()
            : null;
        Require(handled == 0 && reason == "poison", "a no_identity/poison case must go to orders.dlq with reason poison");
    }

    private static (string Verdict, string Identity) Classify(string body)
    {
        if (KafkaConsumer.DecodeFailure(body) is not null)
        {
            return ("poison", "");
        }

        try
        {
            var identity = EnvelopeCodec.Urn(EnvelopeCodec.Decode(body));
            return (identity.Length == 0 ? "no_identity" : "ok", identity);
        }
        catch (Exception)
        {
            return ("poison", "");
        }
    }

    // -- poison_dlq (ADR-0035) --------------------------------------------------------------------------------------

    private static readonly JsonDocumentOptions DeepJson = new() { MaxDepth = 10_100 };

    private static Task PoisonDlq(JsonElement testCase)
    {
        var bytes = testCase.TryGetProperty("body_base64", out var b64)
            ? Convert.FromBase64String(b64.GetString()!)
            : Encoding.UTF8.GetBytes(testCase.GetProperty("body").GetString()!);
        var originalQueue = Manifest.Value.GetProperty(PoisonDlqSection).GetProperty("original_queue").GetString()!;
        var expect = testCase.GetProperty("expect");
        var shape = expect.GetProperty("shape").GetString()!;

        var record = KafkaConsumer.PoisonBody(bytes, "poison", "System.Exception", 1700000000000L, originalQueue, 0);
        var text = Encoding.UTF8.GetString(record);
        Require(!text.Contains('\uFFFD', StringComparison.Ordinal), "the record holds U+FFFD");

        using var doc = JsonDocument.Parse(record, DeepJson);
        Require(doc.RootElement.ValueKind == JsonValueKind.Object, "the record is not a JSON object");
        var deadLetter = doc.RootElement.EnumerateObject().Where(p => p.Name == "dead_letter").ToList();
        Require(deadLetter.Count == 1, "the record must carry exactly one top-level dead_letter");
        Require(
            deadLetter[0].Value.GetProperty("reason").GetString() == "poison"
            && deadLetter[0].Value.GetProperty("original_queue").GetString() == originalQueue,
            "dead_letter must have reason poison and the original queue");
        var withoutDeadLetter = doc.RootElement.EnumerateObject().Where(p => p.Name != "dead_letter").ToList();

        var want = expect.GetProperty("record");
        if (shape == "splice_or_wrapper" && !SameMembers(withoutDeadLetter, want))
        {
            foreach (var literal in expect.GetProperty("splice_literals").EnumerateArray())
            {
                Require(text.Contains(literal.GetString()!, StringComparison.Ordinal), $"the record does not contain {literal.GetString()} verbatim");
            }

            Require(!withoutDeadLetter.Any(p => p.Name is "raw" or "raw_encoding"), "a spliced record has a top-level raw / raw_encoding member");
            var keys = withoutDeadLetter.Select(p => p.Name).Distinct(StringComparer.Ordinal).OrderBy(n => n, StringComparer.Ordinal).ToList();
            var wantKeys = expect.GetProperty("splice_keys").EnumerateArray().Select(k => k.GetString()!).OrderBy(n => n, StringComparer.Ordinal).ToList();
            Require(keys.SequenceEqual(wantKeys), $"top-level members {string.Join(",", keys)}, expected {string.Join(",", wantKeys)}");
            return Task.CompletedTask;
        }

        Require(SameMembers(withoutDeadLetter, want), $"the record without dead_letter differs from the expected record: {text}");
        return Task.CompletedTask;
    }

    private static bool SameMembers(List<JsonProperty> members, JsonElement want)
    {
        var wantMembers = want.EnumerateObject().ToList();
        return members.Count == wantMembers.Count
            && members.All(m => wantMembers.Count(w => w.Name == m.Name && SameJson(m.Value, w.Value)) == 1);
    }

    private static bool SameJson(JsonElement a, JsonElement b)
    {
        if (a.ValueKind != b.ValueKind)
        {
            return false;
        }

        return a.ValueKind switch
        {
            JsonValueKind.Object => SameMembers(a.EnumerateObject().ToList(), b),
            JsonValueKind.Array => a.GetArrayLength() == b.GetArrayLength()
                && a.EnumerateArray().Zip(b.EnumerateArray(), SameJson).All(x => x),
            JsonValueKind.String => a.GetString() == b.GetString(),
            JsonValueKind.Number => a.GetRawText() == b.GetRawText(),
            _ => true,
        };
    }

    // -- helpers ----------------------------------------------------------------------------------------------------

    private static void Require(bool condition, string message)
    {
        if (!condition)
        {
            throw new ConformanceFailure(message);
        }
    }

    private static void RequireHeaders(Headers headers, JsonElement expected)
    {
        foreach (var header in expected.EnumerateObject())
        {
            var actual = KafkaHeaders.GetString(headers, header.Name);
            Require(actual == header.Value.GetString(), $"header {header.Name} is '{actual}', expected '{header.Value.GetString()}'");
        }
    }

    private static async Task Rejects(Func<Task> action)
    {
        try
        {
            await action();
        }
        catch (BabelQueueException)
        {
            return;
        }

        throw new ConformanceFailure("expected a BabelQueueException");
    }

    private static ConsumeResult<byte[], byte[]> Record(string topic, string body, Headers headers) => new()
    {
        Topic = topic,
        Partition = new Partition(0),
        Offset = new Offset(0),
        Message = new Message<byte[], byte[]> { Value = Encoding.UTF8.GetBytes(body), Headers = headers },
    };

    private static Mock<IConsumer<byte[], byte[]>> ConsumerOf(ConsumeResult<byte[], byte[]> result)
    {
        var consumer = new Mock<IConsumer<byte[], byte[]>>();
        consumer.Setup(c => c.Consume(It.IsAny<TimeSpan>())).Returns(result);
        return consumer;
    }

    private static Mock<IProducer<byte[], byte[]>> ProducerInto(List<(string Topic, Message<byte[], byte[]> Message)> sent)
    {
        var producer = new Mock<IProducer<byte[], byte[]>>();
        producer
            .Setup(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<byte[], byte[]>>(), It.IsAny<CancellationToken>()))
            .Callback<string, Message<byte[], byte[]>, CancellationToken>((t, m, _) => sent.Add((t, m)))
            .ReturnsAsync(new DeliveryResult<byte[], byte[]>());
        return producer;
    }

    // -- results.json (ADR-0032) --------------------------------------------------------------------------------------

    /// <summary>Run every case, build the <c>results.json</c> document, and write it to <paramref name="path"/> when given.</summary>
    internal static async Task<string> WriteResultsAsync(string? path)
    {
        var sections = new SortedDictionary<string, SortedDictionary<string, (string Status, string? Reason)>>(StringComparer.Ordinal);
        foreach (var c in All())
        {
            (string, string?) outcome;
            try
            {
                await c.Run();
                outcome = ("pass", null);
            }
#pragma warning disable CA1031 // Any failure of a case is that case's recorded result, not a crash of the writer.
            catch (Exception e)
#pragma warning restore CA1031
            {
                outcome = ("fail", e.Message);
            }

            if (!sections.TryGetValue(c.Section, out var bucket))
            {
                bucket = sections[c.Section] = new SortedDictionary<string, (string, string?)>(StringComparer.Ordinal);
            }

            bucket[c.Id] = outcome;
        }

        using var stream = new MemoryStream();
        using (var w = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = true }))
        {
            w.WriteStartObject();
            w.WriteString("sdk", "babelqueue-dotnet-kafka");
            w.WriteString("version", PackageVersion());
            w.WriteString("conformance_version", ConformanceVersion);
            w.WriteString("generated_at", DateTimeOffset.UtcNow.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture));
            w.WriteStartArray("bindings");
            w.WriteStringValue("kafka");
            w.WriteEndArray();
            w.WriteStartObject("sections");
            foreach (var (section, cases) in sections)
            {
                w.WriteStartObject(section);
                foreach (var (id, (status, reason)) in cases)
                {
                    w.WriteStartObject(id);
                    w.WriteString("status", status);
                    if (reason is not null)
                    {
                        w.WriteString("reason", reason);
                    }

                    w.WriteEndObject();
                }

                w.WriteEndObject();
            }

            w.WriteEndObject();
            w.WriteEndObject();
        }

        var json = Encoding.UTF8.GetString(stream.ToArray()) + "\n";
        if (!string.IsNullOrWhiteSpace(path))
        {
            await File.WriteAllTextAsync(path, json);
        }

        return json;
    }

    private static string PackageVersion()
    {
        var info = typeof(KafkaPublisher).Assembly
            .GetCustomAttributes(typeof(System.Reflection.AssemblyInformationalVersionAttribute), false)
            .Cast<System.Reflection.AssemblyInformationalVersionAttribute>()
            .Single().InformationalVersion;
        var plus = info.IndexOf('+', StringComparison.Ordinal);
        return plus < 0 ? info : info[..plus];
    }
}
