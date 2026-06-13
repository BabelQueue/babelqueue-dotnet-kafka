using System.Collections.Generic;
using BabelQueue;
using BabelQueue.Kafka;
using Confluent.Kafka;
using Xunit;

namespace BabelQueue.Kafka.Tests;

/// <summary>§6.3 header projection + parsing (no broker): all values are UTF-8 byte strings.</summary>
public sealed class KafkaHeadersTests
{
    [Fact]
    public void ProjectsContractHeaders()
    {
        var env = EnvelopeCodec.Make(
            "urn:babel:orders:created", new Dictionary<string, object?> { ["order_id"] = 7 }, "orders", "trace-1");
        var headers = KafkaHeaders.Project(env);

        Assert.Equal("urn:babel:orders:created", KafkaHeaders.GetString(headers, KafkaHeaders.Job));
        Assert.Equal("trace-1", KafkaHeaders.GetString(headers, KafkaHeaders.TraceId));
        Assert.Equal(env.Meta!.Id, KafkaHeaders.GetString(headers, KafkaHeaders.MessageId));
        Assert.Equal("1", KafkaHeaders.GetString(headers, KafkaHeaders.SchemaVersion));
        Assert.Equal(env.Meta.Lang, KafkaHeaders.GetString(headers, KafkaHeaders.SourceLang));
        Assert.Equal("0", KafkaHeaders.GetString(headers, KafkaHeaders.Attempts));
    }

    [Fact]
    public void GetIntParsesAndFallsBack()
    {
        var headers = new Headers();
        KafkaHeaders.Add(headers, KafkaHeaders.Attempts, "3");
        KafkaHeaders.Add(headers, "bq-bad", "not-a-number");

        Assert.Equal(3, KafkaHeaders.GetInt(headers, KafkaHeaders.Attempts, -1));
        Assert.Equal(9, KafkaHeaders.GetInt(headers, "bq-missing", 9));
        Assert.Equal(0, KafkaHeaders.GetInt(headers, "bq-bad", 0));
        Assert.Null(KafkaHeaders.GetString(headers, "bq-missing"));
    }
}
