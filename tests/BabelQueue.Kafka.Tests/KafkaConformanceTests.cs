using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using BabelQueue;
using BabelQueue.Kafka;
using Confluent.Kafka;
using Moq;
using Xunit;

namespace BabelQueue.Kafka.Tests;

/// <summary>
/// Apache Kafka binding conformance against the vendored canonical suite's <c>kafka</c> block:
/// the §6 header projection and the <c>bq-attempts</c>-header-authoritative-else-body
/// reconciliation. No Kafka, no network.
/// </summary>
public sealed class KafkaConformanceTests
{
    private static readonly string Dir = Path.Combine(AppContext.BaseDirectory, "conformance");

    private static JsonElement Kafka()
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(Path.Combine(Dir, "manifest.json")));
        return doc.RootElement.GetProperty("kafka").Clone();
    }

    [Fact]
    public void PropertyProjectionMatchesGolden()
    {
        var projection = Kafka().GetProperty("property_projection");
        var body = File.ReadAllText(Path.Combine(Dir, projection.GetProperty("envelope_file").GetString()!));
        var headers = KafkaHeaders.Project(EnvelopeCodec.Decode(body));

        foreach (var golden in projection.GetProperty("headers").EnumerateObject())
        {
            Assert.Equal(golden.Value.GetString(), KafkaHeaders.GetString(headers, golden.Name));
        }
    }

    [Fact]
    public async Task AttemptsReconciliationMatchesGolden()
    {
        foreach (var testCase in Kafka().GetProperty("attempts_reconciliation").GetProperty("cases").EnumerateArray())
        {
            var bodyAttempts = testCase.GetProperty("body_attempts").GetInt32();
            var expected = testCase.GetProperty("expected_attempts").GetInt32();
            var env = EnvelopeCodec.Make("urn:babel:orders:created", new Dictionary<string, object?> { ["x"] = 1 }, "orders")
                with { Attempts = bodyAttempts };

            var headers = new Headers();
            var headerAttempts = testCase.GetProperty("header_attempts");
            if (headerAttempts.ValueKind != JsonValueKind.Null)
            {
                KafkaHeaders.Add(headers, KafkaHeaders.Attempts, headerAttempts.GetInt32().ToString(CultureInfo.InvariantCulture));
            }

            var result = new ConsumeResult<byte[], byte[]>
            {
                Topic = "orders",
                Partition = new Partition(0),
                Offset = new Offset(0),
                Message = new Message<byte[], byte[]> { Value = Encoding.UTF8.GetBytes(EnvelopeCodec.Encode(env)), Headers = headers },
            };
            var consumer = new Mock<IConsumer<byte[], byte[]>>();
            consumer.Setup(c => c.Consume(It.IsAny<TimeSpan>())).Returns(result);

            var seen = -1;
            var handlers = new Dictionary<string, BabelHandler>
            {
                ["urn:babel:orders:created"] = (e, _, _) => { seen = e.Attempts; return Task.CompletedTask; },
            };
            await new KafkaConsumer(consumer.Object, handlers).PollAsync();

            Assert.Equal(expected, seen);
        }
    }
}
