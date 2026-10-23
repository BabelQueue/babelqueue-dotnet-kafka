using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Xunit;

namespace BabelQueue.Kafka.Tests;

/// <summary>
/// Runs every case of the Kafka-scoped manifest sections through <see cref="KafkaConformanceSuite"/> and, when
/// <c>BQ_CONFORMANCE_RESULTS</c> names a path, writes the <c>results.json</c> release asset (ADR-0032).
/// </summary>
public sealed class KafkaConformanceResultsTests
{
    [Fact]
    public async Task ResultsHaveNoFailuresAndAreWrittenOnRequest()
    {
        var json = await KafkaConformanceSuite.WriteResultsAsync(Environment.GetEnvironmentVariable("BQ_CONFORMANCE_RESULTS"));
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.Equal("babelqueue-dotnet-kafka", root.GetProperty("sdk").GetString());
        Assert.Equal(new[] { "kafka" }, root.GetProperty("bindings").EnumerateArray().Select(b => b.GetString()).ToArray());
        Assert.Equal(KafkaConformanceSuite.ConformanceVersion, root.GetProperty("conformance_version").GetString());

        var sections = root.GetProperty("sections");
        Assert.Equal(new[] { "backoff", "kafka", "kafka_poison" }, sections.EnumerateObject().Select(s => s.Name).OrderBy(n => n, StringComparer.Ordinal).ToArray());

        var failures = new List<string>();
        foreach (var section in sections.EnumerateObject())
        {
            foreach (var c in section.Value.EnumerateObject())
            {
                if (c.Value.GetProperty("status").GetString() != "pass")
                {
                    failures.Add($"{section.Name}/{c.Name}: {(c.Value.TryGetProperty("reason", out var r) ? r.GetString() : "")}");
                }
            }
        }

        Assert.True(failures.Count == 0, string.Join("; ", failures));
    }

    [Fact]
    public void EveryInScopeSectionIsEnumeratedWithUniqueIds()
    {
        var cases = KafkaConformanceSuite.All();
        Assert.Equal(new[] { "backoff", "kafka", "kafka_poison" }, KafkaConformanceSuite.Sections().OrderBy(s => s, StringComparer.Ordinal).ToArray());
        Assert.Equal(cases.Count, cases.Select(c => (c.Section, c.Id)).Distinct().Count());
        Assert.Contains(cases, c => c is { Section: "kafka", Id: "property_projection" });
        Assert.Contains(cases, c => c is { Section: "kafka", Id: "schema_version_gate/0" });
        Assert.Contains(cases, c => c is { Section: "backoff", Id: "below-first-tier" });
        Assert.Contains(cases, c => c is { Section: "backoff", Id: "retry_attempt_tier/key-forwarded-byte-for-byte" });
        Assert.Contains(cases, c => c is { Section: "backoff", Id: "default_tiers/recommended-tier-list" });
        Assert.True(cases.Count(c => c.Section == "kafka_poison") >= 92);
    }
}
