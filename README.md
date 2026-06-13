# BabelQueue — Apache Kafka (.NET)

`BabelQueue.Kafka` — an Apache Kafka transport for [BabelQueue](https://babelqueue.com),
built on [Confluent.Kafka](https://github.com/confluentinc/confluent-kafka-dotnet) and the
framework-agnostic [`BabelQueue.Core`](https://github.com/BabelQueue/babelqueue-dotnet).

A canonical-envelope **publisher** and a URN-routed, **process-then-commit** consumer, so a
Kafka-based .NET service speaks the same wire contract (envelope shape, URN identity, trace
propagation) as the Java, Python, Go and Node SDKs. Implements
[§6 of the broker-bindings contract](https://babelqueue.com/docs/spec/1.x/broker-bindings#apache-kafka).

Kafka has **no native** per-message ack, delayed delivery, dead-letter queue, or delivery
counter — this transport absorbs all four in the binding layer (the envelope stays
`schema_version: 1`): the record **value** is the envelope JSON, the contract fields are
mirrored onto `bq-` headers (route on `bq-job` without decoding the body), the record
timestamp mirrors `meta.created_at`, **`bq-attempts` is the authoritative retry counter**,
consume is process-then-commit (manual commit), retry/delay use SDK-owned tiered retry topics
`<topic>.retry.<n>`, and terminal failures go to an opt-in `<topic>.dlq`.

## Install

```bash
dotnet add package BabelQueue.Kafka
```

Requirements: **.NET 8**. It pulls `BabelQueue.Core` and `Confluent.Kafka` transitively.

## Produce

```csharp
using Confluent.Kafka;
using BabelQueue.Kafka;

using var producer = new ProducerBuilder<byte[], byte[]>(
    new ProducerConfig { BootstrapServers = "localhost:9092" }).Build();

var id = await KafkaPublisher.Create(producer, "orders")
    .PublishAsync("urn:babel:orders:created", new Dictionary<string, object?> { ["order_id"] = 1042 });
```

`PublishAsync` returns the message `meta.id`; pass a `traceId` to continue a trace, or a
`delay` (`TimeSpan`) — delays require a retry topology (`KafkaPublisher.Create(producer, retryTopics)`)
and route to the matching tier; on a plain publisher a delay raises `BabelQueueException`.

## Consume

```csharp
using var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "orders-workers",
    EnableAutoCommit = false,            // manual commit is required
    AutoOffsetReset = AutoOffsetReset.Earliest,
}).Build();
consumer.Subscribe("orders");

var retry = RetryTopics.ForTopic("orders")
    .Tier(TimeSpan.FromSeconds(5)).Tier(TimeSpan.FromMinutes(1)).Build(); // .retry.1/.2 + orders.dlq

var worker = new KafkaConsumer(consumer, new Dictionary<string, BabelHandler>
{
    ["urn:babel:orders:created"] = (env, result, ct) =>
    {
        // env.Data, env.TraceId, env.Attempts ...
        return Task.CompletedTask;
    },
}, new KafkaConsumerOptions { Producer = producer, RetryTopics = retry, MaxTries = 3 });

await worker.RunAsync(cancellationToken); // consume → process → commit
```

A throwing handler republishes the envelope to the next `<topic>.retry.<n>` tier with
`bq-attempts + 1`, then commits; once `MaxTries` is reached it goes to `<topic>.dlq` with a
`dead_letter` block. The consumer routes on the `bq-job` header. Unknown-URN strategy is one
of `fail` / `delete` / `release` / `dead_letter`.

## Contract mapping (§6)

| Envelope | Apache Kafka |
| :--- | :--- |
| body | record `value` (byte-identical across SDKs) |
| `job` (URN) | header `bq-job` (consumer routes on this) |
| `trace_id` | header `bq-trace-id` |
| `meta.id` | header `bq-message-id` |
| `meta.schema_version` | header `bq-schema-version` (`"1"`) |
| `meta.lang` | header `bq-source-lang` |
| `meta.created_at` | record `Timestamp` (Unix ms) |
| `attempts` | header `bq-attempts` (**authoritative**; body is the fallback) |
| reserve / ack | consume → process → **commit offset** (manual) |
| retry / delay | republish to `<topic>.retry.<n>` (`bq-attempts + 1`) |
| dead-letter | `<topic>.dlq` + `dead_letter` block |

The `IProducer` / `IConsumer` interfaces are mockable, so the unit tests use Moq — no Kafka,
no network. The envelope is unchanged (`schema_version` stays `1`); Apache Kafka is purely
additive.

## Build & test

```bash
dotnet test
```

xUnit; analyzers run as errors; ≥90% line coverage enforced.

## License

MIT
