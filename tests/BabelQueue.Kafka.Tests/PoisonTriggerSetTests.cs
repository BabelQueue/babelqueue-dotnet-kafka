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
/// The poison trigger set is Go's: a body is poison exactly when <c>babelqueue.Decode</c> (typed
/// <c>Envelope</c>/<c>Meta</c>/<c>DeadLetter</c>) rejects it. <c>poison-trigger-set.json</c> holds the bodies and the
/// verdicts produced by running <c>babelqueue.Decode</c> itself; the same table is checked in every SDK.
/// </summary>
public sealed class PoisonTriggerSetTests
{
    private const string Urn = "urn:babel:orders:created";

    public static IEnumerable<object[]> Rows()
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "poison-trigger-set.json")));
        foreach (var row in doc.RootElement.EnumerateArray())
        {
            yield return new object[] { row.GetProperty("name").GetString()!, row.GetProperty("body").GetString()!, row.GetProperty("poison").GetBoolean() };
        }
    }

    [Fact]
    public void TheTableIsNotEmpty() => Assert.True(Rows().Count() > 50);

    [Theory]
    [MemberData(nameof(Rows))]
    public void TheShapeCheckMatchesGo(string name, string body, bool poison)
    {
        Assert.True(poison == (KafkaConsumer.DecodeFailure(body) is not null), $"{name}: Go poison={poison}");
    }

    private static async Task<(List<(string Topic, Message<byte[], byte[]> Message)> Sent, Mock<IConsumer<byte[], byte[]>> Consumer, int Reported, int Handled)> Run(
        string body, string unknownUrn = UnknownUrnStrategy.Delete, bool withHandler = false, bool jobHeader = false, List<Envelope>? received = null)
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
            handlers[Urn] = (envelope, _, _) => { handled++; received?.Add(envelope); return Task.CompletedTask; };
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
    public async Task TheConsumerDeadLettersExactlyThePoisonRowsAndNothingElse()
    {
        var failures = new List<string>();
        foreach (var row in Rows())
        {
            var (name, body, poison) = ((string)row[0], (string)row[1], (bool)row[2]);
            var (sent, consumer, reported, _) = await Run(body);
            consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<byte[], byte[]>>()), Times.Once);
            consumer.Verify(c => c.Seek(It.IsAny<TopicPartitionOffset>()), Times.Never);
            if (poison)
            {
                var ok = sent.Count == 1 && sent[0].Topic == "orders.dlq" && reported == 1
                    && JsonDocument.Parse(sent[0].Message.Value).RootElement.GetProperty("dead_letter").GetProperty("reason").GetString() == "poison";
                if (!ok) { failures.Add(name + " (expected a poison DLQ record)"); }
            }
            else if (sent.Count != 0 || reported != 0)
            {
                failures.Add(name + " (expected no DLQ record)");
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
    public async Task AnEmptyJobOrABareNullTakesTheUnknownUrnPathNotThePoisonPath(string body)
    {
        var (sent, _, _, _) = await Run(body, UnknownUrnStrategy.DeadLetter);
        var record = Assert.Single(sent);
        var value = JsonDocument.Parse(record.Message.Value).RootElement;
        Assert.Equal("unknown_urn", value.GetProperty("dead_letter").GetProperty("reason").GetString());
        Assert.False(value.TryGetProperty("raw", out _));
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
    public void ANonStringUrnAliasIsIgnoredNotPoison()
    {
        Assert.Null(KafkaConsumer.DecodeFailure("{\"urn\":5}"));
        Assert.NotNull(KafkaConsumer.DecodeFailure("{\"job\":5}"));
    }
}
