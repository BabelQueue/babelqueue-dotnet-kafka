# Changelog

All notable changes to `BabelQueue.Kafka` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

### Fixed
- `KafkaConsumer` now preserves the original record key when it republishes a record: the retry-tier write, the unknown-URN release and the annotated envelope `<topic>.dlq` write previously produced a `null` key. A null key stays null, an empty key stays empty and no key is synthesised; the raw/poison DLQ already kept it. Partition equality is not guaranteed. No public API change; wire envelope, `dead_letter` shape and header names are unchanged (`schema_version` stays 1). Patch-level behaviour fix.
- `KafkaConsumer` now decodes the record body as **strict** UTF-8. Previously `Encoding.UTF8.GetString` silently replaced invalid bytes with U+FFFD, so a structurally valid envelope with a corrupted string inside `data` was accepted and handed to a handler with altered content. A body that is not valid UTF-8 is now treated as poison, matching the Java/Go/Node transports: the original bytes (and key/headers) are forwarded raw to `<topic>.dlq`, the offset is committed, and only then does `OnError` receive a `BabelQueueException` (isolated: a throwing callback cannot bypass the DLQ write or commit). If the DLQ write fails the record is not committed and the consumer seeks back (unchanged settlement behaviour); with no DLQ topic configured the record is just committed. The `bq-schema-version` header keeps its lenient decode: invalid bytes yield U+FFFD, which never equals `"1"`, so such a record is already rejected by the schema-version gate. No public API change; wire envelope, `dead_letter` shape and header names are unchanged.

## [1.0.2] - 2026-10-11

### Fixed
- `KafkaConsumer` now preserves the source record's key on the raw forward to `<topic>.dlq` (poison body / schema-version gate), matching the Java and Node transports. Previously the key was dropped, so DLQ partition assignment and key-based triage diverged from the source topic. The annotated `dead_letter` envelope and retry-tier republish are unchanged (parity with Java/Node). No public API change; wire envelope and header names are unchanged.
- `KafkaConsumer` no longer loses a record when its settlement throws. Previously a failing poison-body DLQ write, retry-tier / DLQ republish after a handler failure, or unknown-URN DeadLetter / Release / Fail settlement threw out of `PollAsync` after `Consume` had already advanced the in-memory position; a restarted loop then received the *next* record and committing it (Kafka offsets are cumulative) skipped the unsettled one. `PollAsync` now `Seek`s back to the record's offset on any settlement failure (including cancellation) before rethrowing, without committing it, so the record is redelivered. If the seek itself fails a `BabelQueueException` wrapping both errors is thrown. Supersedes the 1.0.1 note that the other DLQ paths are unchanged. Behaviour note: with neither retry tiers nor a DLQ topic configured, a handler failure raises `BabelQueueException`; that record is now redelivered on every poll (the partition stalls on it) instead of being skipped after a restart, so configure retry topics or a DLQ. Requires `EnableAutoCommit = false` as documented. No public API change.

## [1.0.1] - 2026-10-10

### Fixed
- `KafkaConsumer` now checks the `bq-schema-version` header (UTF-8) **before** decoding the record (§6.7). A value other than exactly `"1"` (ordinal, untrimmed) is forwarded raw to `<topic>.dlq` and committed without decoding, the same path as an undecodable record (a no-op forward when no DLQ topic is configured, so the record is just committed). A header with a null value counts as absent. Absent or blank (empty / ASCII whitespace) falls through to the normal decode. No public API change.
- The schema-version gate no longer loses a record when the DLQ write fails: `KafkaConsumer` now `Seek`s back to the failed record's offset (so a later record's cumulative commit cannot skip it), does not commit it, reports via `OnError` and backs off for one `PollTimeout`; the record never reaches a handler. `OnError` is invoked **after** the commit / seek-back and isolated, so a throwing callback cannot bypass the protection. If the seek itself fails a `BabelQueueException` wrapping both errors is thrown. Other DLQ paths (poison body, exhausted retries, unknown URN) are unchanged. No public API change.

## [1.0.0] - 2026-06-13

### Added
- Initial release. An Apache Kafka transport on `BabelQueue.Core` + `Confluent.Kafka`,
  implementing §6 of the broker-bindings contract. Kafka has no native ack/delay/DLQ/delivery-
  counter, so the binding absorbs all four: `KafkaPublisher` (value = canonical envelope,
  record timestamp = `meta.created_at`, the `bq-` header projection — `bq-job` routes; a delay
  routes to a `<topic>.retry.<n>` tier or raises without one) and `KafkaConsumer`
  (**process-then-commit** manual-commit consume; a throwing handler republishes to the next
  retry tier with **`bq-attempts + 1`** then commits; terminal failures go to `<topic>.dlq` with
  the additive `dead_letter` block; the `bq-attempts` header is the authoritative counter with
  the body as fallback; `fail`/`delete`/`release`/`dead_letter` unknown-URN strategies; poison
  records forwarded raw to the DLQ). `RetryTopics` configures the tiered retry/delay topology.
  .NET 8, xUnit, analyzers-as-errors, ≥90% line coverage; the `IProducer`/`IConsumer` interfaces
  are mocked with Moq (no Kafka, no network). The envelope is unchanged (`schema_version: 1`);
  Apache Kafka is purely additive.
