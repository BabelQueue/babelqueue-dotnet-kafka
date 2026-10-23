using System;
using System.Collections.Generic;
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
using Xunit;

namespace BabelQueue.Kafka.Tests;

/// <summary>
/// The vendored conformance <c>kafka_poison</c> section (ADR-0034 section 10, ADR-0032 section 6) replaces the old
/// <c>poison-trigger-set.json</c> table. Every case is classified with the core decoder (<c>ok</c>: parses and carries a
/// valid identity; <c>no_identity</c>: parses but has none; <c>poison</c>: cannot be decoded) and its fate is then checked
/// through the transport: <c>ok</c> reaches the handler, <c>no_identity</c> and <c>poison</c> both go to
/// <c>&lt;topic&gt;.dlq</c> with <c>reason: poison</c> without calling it. The section also pins that
/// <c>System.Text.Json</c>-based decoding takes the LAST duplicate top-level key.
/// </summary>
public sealed class PoisonTriggerSetTests
{
    private const string Urn = "urn:babel:orders:created";

    public static IEnumerable<object[]> Rows()
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "conformance", "manifest.json")));
        foreach (var row in doc.RootElement.GetProperty("kafka_poison").GetProperty("cases").EnumerateArray())
        {
            yield return new object[] { row.GetProperty("name").GetString()!, row.GetProperty("body").GetString()!, row.GetProperty("expect").GetString()! };
        }
    }

    /// <summary>The verdict of the core decoder (the runner's classification of a case).</summary>
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

    [Fact]
    public void TheSectionIsVendoredAndCoversAllThreeVerdicts()
    {
        var rows = Rows().ToList();
        Assert.True(rows.Count >= 92, $"expected at least 92 cases, got {rows.Count}");
        Assert.Equal(new[] { "no_identity", "ok", "poison" }, rows.Select(r => (string)r[2]).Distinct().OrderBy(x => x, StringComparer.Ordinal));
    }

    [Theory]
    [MemberData(nameof(Rows))]
    public void TheCoreDecoderClassifiesEveryCase(string name, string body, string expect)
    {
        Assert.True(expect == Classify(body).Verdict, $"{name}: expected {expect}, got {Classify(body).Verdict}");
    }

    [Fact]
    public void ADuplicateTopLevelKeyTakesTheLastValue()
    {
        // System.Text.Json (the core decoder) keeps the last duplicate, so the identity rule runs on the last value.
        Assert.Equal(("ok", "urn:x"), Classify("{\"job\":5,\"job\":\"urn:x\"}"));
        Assert.Equal(("no_identity", ""), Classify("{\"job\":\"urn:x\",\"job\":5}"));
        Assert.Equal(("ok", "urn:y"), Classify("{\"job\":\"urn:x\",\"job\":\"urn:y\"}"));
    }

    private static async Task<(List<(string Topic, Message<byte[], byte[]> Message)> Sent, Mock<IConsumer<byte[], byte[]>> Consumer, int Reported, int Handled)> Run(
        string body, string unknownUrn = UnknownUrnStrategy.Delete, bool withHandler = false, bool jobHeader = false, List<Envelope>? received = null, string handlerUrn = Urn)
    {
        var result = new ConsumeResult<byte[], byte[]>
        {
            Topic = "orders",
            Partition = new Partition(0),
            Offset = new Offset(4),
            Message = new Message<byte[], byte[]> { Value = Encoding.UTF8.GetBytes(body), Headers = new Headers() },
        };
        if (jobHeader)
        {
            result.Message.Headers.Add("bq-job", Encoding.UTF8.GetBytes(Urn));
        }
        var consumer = new Mock<IConsumer<byte[], byte[]>>();
        consumer.Setup(c => c.Consume(It.IsAny<TimeSpan>())).Returns(result);
        var sent = new List<(string Topic, Message<byte[], byte[]> Message)>();
        var producer = new Mock<IProducer<byte[], byte[]>>();
        producer
            .Setup(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<byte[], byte[]>>(), It.IsAny<CancellationToken>()))
            .Callback<string, Message<byte[], byte[]>, CancellationToken>((t, m, _) => sent.Add((t, m)))
            .ReturnsAsync(new DeliveryResult<byte[], byte[]>());
        var reported = 0;
        var handled = 0;
        var handlers = new Dictionary<string, BabelHandler>();
        if (withHandler)
        {
            handlers[handlerUrn] = (envelope, _, _) => { handled++; received?.Add(envelope); return Task.CompletedTask; };
        }

        await new KafkaConsumer(consumer.Object, handlers, new KafkaConsumerOptions
        {
            Producer = producer.Object,
            RetryTopics = RetryTopics.ForTopic("orders").Tier(TimeSpan.FromSeconds(5)).Build(),
            UnknownUrn = unknownUrn,
            OnError = (_, _, _) => reported++,
        }).PollAsync();
        return (sent, consumer, reported, handled);
    }

    [Fact]
    public async Task TheConsumerSettlesEveryCaseWithTheFateItsVerdictDemands()
    {
        var failures = new List<string>();
        foreach (var row in Rows())
        {
            var (name, body, expect) = ((string)row[0], (string)row[1], (string)row[2]);
            var identity = Classify(body).Identity;
            foreach (var strategy in new[] { UnknownUrnStrategy.Delete, UnknownUrnStrategy.DeadLetter, UnknownUrnStrategy.Fail })
            {
                var (sent, consumer, reported, handled) = await Run(body, strategy, withHandler: true, handlerUrn: identity.Length == 0 ? Urn : identity);
                consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Once);
                consumer.Verify(c => c.Seek(It.IsAny<TopicPartitionOffset>()), Times.Never);
                if (expect == "ok")
                {
                    if (handled != 1 || sent.Count != 0 || reported != 0) { failures.Add($"{name} [{strategy}] (expected the handler, no DLQ record)"); }
                    continue;
                }
                var parked = sent.Count == 1 && sent[0].Topic == "orders.dlq" && reported == 1 && handled == 0
                    && JsonDocument.Parse(sent[0].Message.Value).RootElement.GetProperty("dead_letter").GetProperty("reason").GetString() == "poison";
                if (!parked) { failures.Add($"{name} [{strategy}] (expected a poison DLQ record, no handler)"); }
            }
        }
        Assert.True(failures.Count == 0, string.Join("; ", failures));
    }

    [Theory]
    [InlineData("{\"job\":\"" + Urn + "\"}")]
    [InlineData("{\"job\":\"" + Urn + "\",\"trace_id\":\"\",\"data\":{}}")]
    [InlineData("{\"job\":\"" + Urn + "\",\"trace_id\":\"t\",\"data\":{},\"meta\":{\"schema_version\":2}}")]
    public async Task ATypedValidButNonConformantEnvelopeIsRoutedToItsHandlerLikeGo(string body)
    {
        // Go's consume path never calls Accepts: a missing data, blank trace_id or body schema_version != 1 is routed by URN.
        var (sent, consumer, reported, handled) = await Run(body, withHandler: true);
        Assert.Equal(1, handled);
        Assert.Empty(sent);
        Assert.Equal(0, reported);
        consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Once);
    }

    [Theory]
    [InlineData("{}")]
    [InlineData("null")]
    [InlineData("{\"job\":null}")]
    [InlineData("{\"job\":5}")]
    public async Task ABodyWithNoIdentityIsPoisonOnEveryUnknownUrnStrategy(string body)
    {
        // ADR-0034 section 10: no job/urn identity has the fate of an undecodable body, whatever the unknown-URN strategy.
        foreach (var strategy in new[] { UnknownUrnStrategy.Delete, UnknownUrnStrategy.DeadLetter, UnknownUrnStrategy.Release, UnknownUrnStrategy.Fail })
        {
            var (sent, consumer, _, handled) = await Run(body, strategy, withHandler: true);
            var record = Assert.Single(sent);
            Assert.Equal("orders.dlq", record.Topic);
            Assert.Equal("poison", JsonDocument.Parse(record.Message.Value).RootElement.GetProperty("dead_letter").GetProperty("reason").GetString());
            Assert.Equal(0, handled);
            consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Once);
        }
    }

    [Fact]
    public async Task TheBqJobHeaderStillSuppliesTheIdentityOfABodyWithoutOne()
    {
        var (sent, _, _, handled) = await Run("{}", withHandler: true, jobHeader: true);
        Assert.Equal(1, handled);
        Assert.Empty(sent);
    }

    [Theory]
    [InlineData("{\"job\":\"" + Urn + "\",\"trace_id\":\"t\",\"data\":{\"k\":\"\\ud800\"},\"meta\":{\"schema_version\":1}}")]
    [InlineData("{\"job\":\"" + Urn + "\",\"trace_id\":\"t\",\"data\":{\"\\ud800\":1},\"meta\":{\"schema_version\":1}}")]
    [InlineData("{\"\\ud800\":1,\"job\":\"" + Urn + "\"}")]
    [InlineData("{\"job\":\"\\ud800\"}")]
    [InlineData("{\"job\":\"" + Urn + "\",\"data\":{\"k\":\"\\udc00\"}}")]
    public async Task ABodyTheCoreDecoderThrowsOnIsDeadLetteredAsPoisonNeverLeftToWedgeThePartition(string body)
    {
        // A lone surrogate escape passes the shape scan (Go accepts it as U+FFFD) but the core decoder cannot carry it:
        // that must be a poison DLQ record + commit, never an exception out of PollAsync (which would seek back and
        // redeliver the same record forever).
        var (sent, consumer, reported, handled) = await Run(body, withHandler: true);
        var record = Assert.Single(sent);
        Assert.Equal("orders.dlq", record.Topic);
        Assert.Equal("poison", JsonDocument.Parse(record.Message.Value).RootElement.GetProperty("dead_letter").GetProperty("reason").GetString());
        Assert.Equal(0, handled);
        Assert.Equal(1, reported);
        consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Once);
        consumer.Verify(c => c.Seek(It.IsAny<TopicPartitionOffset>()), Times.Never);
    }

    private static string NestedBody(int levels) =>
        "{\"job\":\"" + Urn + "\",\"trace_id\":\"t\",\"data\":{\"x\":" + new string('[', levels) + new string(']', levels)
        + "},\"meta\":{\"schema_version\":1}}";

    [Theory]
    [InlineData(62, true)]
    [InlineData(63, false)]
    [InlineData(64, false)]
    [InlineData(65, false)]
    [InlineData(1500, false)]
    [InlineData(9990, false)]
    public async Task ABodyNestedDeeperThanTheCoreCodecCarriesIsPoisonNeverAnEmptyEnvelopeNorAWedge(int levels, bool decodedInFull)
    {
        // Go decodes nesting up to 10000; the core codec turns anything over 64 levels into a silent EMPTY envelope. That must
        // never reach a handler (content dropped) nor take the unknown-URN path: with or without the bq-job header and under
        // every strategy it is either handled in full or parked as poison (keys kept) -- and always committed.
        foreach (var jobHeader in new[] { true, false })
        {
            foreach (var strategy in new[] { UnknownUrnStrategy.Fail, UnknownUrnStrategy.Delete, UnknownUrnStrategy.DeadLetter })
            {
                var received = new List<Envelope>();
                var (sent, consumer, reported, handled) = await Run(NestedBody(levels), strategy, withHandler: true, jobHeader: jobHeader, received: received);
                var label = $"levels={levels} header={jobHeader} strategy={strategy}";
                consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Once);
                consumer.Verify(c => c.Seek(It.IsAny<TopicPartitionOffset>()), Times.Never);
                if (decodedInFull)
                {
                    Assert.True(handled == 1 && received[0].Data!.Count == 1 && received[0].Meta is not null && sent.Count == 0, label);
                }
                else
                {
                    Assert.True(handled == 0 && reported == 1 && sent.Count == 1, label);
                    var value = JsonDocument.Parse(sent[0].Message.Value, new JsonDocumentOptions { MaxDepth = 10_200 }).RootElement;
                    Assert.Equal("poison", value.GetProperty("dead_letter").GetProperty("reason").GetString());
                    Assert.False(value.TryGetProperty("raw", out _), label); // the keys survive: job/data/meta are not wrapped
                    Assert.Equal(Urn, value.GetProperty("job").GetString());
                    Assert.True(value.TryGetProperty("data", out _) && value.TryGetProperty("meta", out _), label);
                }
            }
        }
    }

    [Fact]
    public void KeyNamesAreCaseSensitiveLikeTheCoreDecoder()
    {
        // A folded or re-cased name is an unknown member (syntax-checked only), never the typed one.
        Assert.Null(KafkaConsumer.DecodeFailure("{\"attempt\u017f\":\"x\"}"));
        Assert.Null(KafkaConsumer.DecodeFailure("{\"ATTEMPTS\":\"x\"}"));
        Assert.Null(KafkaConsumer.DecodeFailure("{\"JOB\":5}"));
        Assert.Null(KafkaConsumer.DecodeFailure("{\"META\":{\"ID\":5}}"));
        Assert.Null(KafkaConsumer.DecodeFailure("{\"trace_\u0131d\":5}"));
        Assert.Null(KafkaConsumer.DecodeFailure("{\"trace_\u0130d\":5}"));
        // The exact spelling is still type-checked.
        Assert.NotNull(KafkaConsumer.DecodeFailure("{\"attempts\":\"x\"}"));
        Assert.NotNull(KafkaConsumer.DecodeFailure("{\"meta\":{\"id\":5}}"));
        Assert.NotNull(KafkaConsumer.DecodeFailure("{\"trace_id\":5}"));
    }

    [Fact]
    public void ANonStringJobOrUrnIsIgnoredNotATypeError()
    {
        Assert.Null(KafkaConsumer.DecodeFailure("{\"urn\":5}"));
        Assert.Null(KafkaConsumer.DecodeFailure("{\"job\":5}"));
        Assert.Equal(("no_identity", ""), Classify("{\"job\":5}"));
    }
}
