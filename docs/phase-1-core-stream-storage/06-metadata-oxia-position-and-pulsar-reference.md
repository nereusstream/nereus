# 06 Metadata Oxia Position and Pulsar Reference

本文回答两个实现前容易混淆的问题：

1. `nereus-metadata-oxia` 是否应该有单独 Future；
2. 是否需要参考 Apache Pulsar 已集成的 Oxia metadata store 代码。

## 1. Position

`nereus-metadata-oxia` 不应该成为独立的产品 Future。它是横切基础设施模块，服务多个
Future：

| Future | Uses `nereus-metadata-oxia` for |
| --- | --- |
| Future 1 | stream metadata、append session、offset index、trim、object manifest |
| Future 3 | cursor state、ack range small state、cursor snapshot reference |
| Future 4 | generation replacement、compaction task/checkpoint、GC references |
| Future 6 | SBT/SDT state、repair state、catalog snapshot references |
| Future 7 | broker session、routing ring、brown-out state、watch invalidation |
| Future 8 | advanced Pulsar state such as delayed index, transaction/pending-ack references, schema/system bootstrap |

Each Future owns its own metadata schema and state machine. `nereus-metadata-oxia` owns the shared Oxia
client binding, Oxia keyspace assembly, partition routing, CAS/condition helpers, watch translation,
exception mapping, fake/contract test harness, and schema record serialization conventions.

Durable component encoders and identity hashes that are also used by object WAL keys live in `nereus-api`
(`KeyComponentCodec` and `DeterministicIds`). `nereus-metadata-oxia` must consume those helpers instead of
owning a separate copy of the algorithms.

## 2. Module Boundary

Phase 1 should keep these dependencies:

```text
nereus-core -> nereus-metadata-oxia
nereus-metadata-oxia -> nereus-api
```

Do not add a generic `nereus-metadata-api` module in Phase 1 unless a second production metadata backend
appears. Nereus is intentionally Oxia-backed; a generic metadata abstraction too early would hide Oxia
features that are correctness-critical, especially partition routing and sequence/conditional commit
semantics.

Allowed test shape:

```text
nereus-metadata-oxia test fixtures -> FakeOxiaMetadataStore
nereus-core tests -> fake through the same OxiaMetadataStore interface
```

The fake must implement the same operation-level semantics as the real adapter. Production code must not
branch on fake-only capabilities.

## 3. Pulsar Oxia Code Checked

On 2026-07-05, Apache Pulsar `master` contains Oxia support under `pulsar-metadata`:

- [`OxiaMetadataStore.java`](https://github.com/apache/pulsar/blob/master/pulsar-metadata/src/main/java/org/apache/pulsar/metadata/impl/oxia/OxiaMetadataStore.java)
- [`OxiaMetadataStoreProvider.java`](https://github.com/apache/pulsar/blob/master/pulsar-metadata/src/main/java/org/apache/pulsar/metadata/impl/oxia/OxiaMetadataStoreProvider.java)
- [`MetadataStoreFactoryImpl.java`](https://github.com/apache/pulsar/blob/master/pulsar-metadata/src/main/java/org/apache/pulsar/metadata/impl/MetadataStoreFactoryImpl.java)
- [`MetadataStoreTest.java`](https://github.com/apache/pulsar/blob/master/pulsar-metadata/src/test/java/org/apache/pulsar/metadata/MetadataStoreTest.java)
- [`OxiaPartitionKeyTest.java`](https://github.com/apache/pulsar/blob/master/pulsar-metadata/src/test/java/org/apache/pulsar/metadata/OxiaPartitionKeyTest.java)
- [`OxiaSequenceKeysTest.java`](https://github.com/apache/pulsar/blob/master/pulsar-metadata/src/test/java/org/apache/pulsar/metadata/OxiaSequenceKeysTest.java)
- [`OxiaSmokeTest.java`](https://github.com/apache/pulsar/blob/master/tests/integration/src/test/java/org/apache/pulsar/tests/integration/oxia/OxiaSmokeTest.java)

Observed Pulsar integration pattern:

- `oxia://host:port/[namespace]` is a metadata-store provider scheme；
- provider parsing defaults namespace to `default`；
- `MetadataStoreFactoryImpl` registers the Oxia provider next to ZooKeeper, memory, and RocksDB；
- the implementation wraps `AsyncOxiaClient` behind Pulsar's generic `MetadataStore` API；
- `Option.PartitionKey` is propagated to get/list/put/delete/range scan；
- native sequence keys require a partition key in the multi-shard tests；
- notifications are translated into Pulsar metadata notifications；
- Oxia client exceptions are mapped into generic metadata-store exceptions；
- integration tests run an Oxia container and use smoke tests for publish/consume with Oxia metadata.

## 4. What To Reuse

We should reference Pulsar's Oxia code for:

- URL and namespace parsing conventions；
- client lifecycle and close behavior；
- config file loading；
- batching options and session timeout wiring；
- partition-key propagation to every operation；
- sequence key and sequence update test patterns；
- notification-to-watch translation；
- exception mapping；
- testcontainer-based Oxia integration tests；
- provider registration shape if Future 2 later plugs into Pulsar-owned code.

## 5. What Not To Copy

Do not use Pulsar's generic `MetadataStore` API as the Phase 1 L0 commit API.

Reasons:

- Future 1 needs stream-specific `commitStreamSlice` semantics, not generic key-value operations；
- producer ack requires a carefully defined linearization point；
- Nereus currently uses one stream-head CAS as the linearization point, then materializes offset-index and
  committed-slice records from the committed log；
- object manifest/reference updates are repairable and must not force cross-shard atomicity；
- Phase 1 modules must not depend on Pulsar classes。

Pulsar's Oxia implementation proves that Oxia can be integrated as a Pulsar metadata backend. It does not
prove that Pulsar's generic metadata API is sufficient for Nereus stream visibility commits.

## 6. Oxia Java Client Surface Checked

On 2026-07-05, M0.5 added a real-client capability spike using:

- Oxia client: `io.github.oxia-db:oxia-client:0.9.0`；
- Oxia Testcontainers helper: `io.github.oxia-db:oxia-testcontainers:0.7.4`；
- Testcontainers JUnit: `org.testcontainers:junit-jupiter:1.20.4`；
- Oxia image: `oxia/oxia:0.16.3`；
- Gradle task: `./gradlew :nereus-metadata-oxia:oxiaCapabilitySpike`。

The spike is implemented under the dedicated `oxiaCapabilitySpike` source set in `nereus-metadata-oxia`.
It writes `build/reports/oxia-capability-spike/summary.md` and `summary.json` when the Docker-backed task
runs successfully. Root `phase1Check` only compiles the spike source; it does not start Docker.

The public Oxia Java client repository surface checked for this decision is:

- [`AsyncOxiaClient.java`](https://github.com/oxia-db/oxia-client-java/blob/main/client-api/src/main/java/io/oxia/client/api/AsyncOxiaClient.java)；
- [`SyncOxiaClient.java`](https://github.com/oxia-db/oxia-client-java/blob/main/client-api/src/main/java/io/oxia/client/api/SyncOxiaClient.java)；
- [`PutOption.java`](https://github.com/oxia-db/oxia-client-java/blob/main/client-api/src/main/java/io/oxia/client/api/options/PutOption.java)；
- [`client.proto`](https://github.com/oxia-db/oxia-client-java/blob/main/client/src/main/proto/io/streamnative/oxia/client.proto)。

Observed surface:

- public sync/async APIs expose single-key `put`, `delete`, `get`, `list`, `rangeScan`, notifications, and
  sequence-update calls；
- `PutOption`/`DeleteOption` support conditional version checks and partition key options；
- partition-key routing is explicit on get/put/list/rangeScan/sequence APIs；
- the proto contains batched write messages, but the selected public Java API does not expose a supportable
  multi-key conditional write/transaction primitive for Nereus。

M0.5 result: `NOT_SUPPORTED_BY_PUBLIC_JAVA_API` for the original single-key-group conditional multi-write
assumption. This is a valid spike result, not a test failure. Phase 1 now treats it as a design input and
uses a public-API-compatible protocol: immutable commit-log record first, then one conditional put of the
stream head, then repairable materialization of offset-index and committed-slice records.

## 7. Capability Spike Contract

Before implementing the real `OxiaMetadataStore`, keep the M0.5 spike runnable against the exact Oxia Java
client dependency planned for Phase 1. This is a stop-the-line gate, not a nice-to-have experiment.

Already covered by M0.5:

1. stream-scoped keys can be routed with the same partition key；
2. single-key `IfVersionIdEquals` CAS succeeds and stale versions fail with an Oxia version-conflict
   exception；
3. sequence keys require partition-key-aware usage and generate fixed-width suffixes；
4. range/list ordering works for fixed-width numeric offset-index keys；
5. the selected public Java API does not expose the multi-key conditional write primitive required by the
   original design。

Still required before the real adapter:

1. keep the documented stream-head CAS protocol in `02-oxia-metadata-and-commit.md` as the real/fake
   adapter contract；
2. add real/fake contract tests for that protocol；
3. verify watch/notification ordering and whether notifications can collapse intermediate updates；
4. record exception mapping from the redesigned adapter operations into Nereus errors。

Required spike shape:

- run against the real client, not only `FakeOxiaMetadataStore`；
- record API calls, observed exception types, dependency versions, container image, and whether partition
  key is explicit on each operation；
- keep `NOT_SUPPORTED_BY_PUBLIC_JAVA_API` as a passing report status for M0.5, because the spike's job is
  to discover capability, not to make the unavailable primitive appear。

Because conditional multi-write is not available through the selected public Java API, Phase 1 must not
emulate it with unsafe multi-step writes. The supported design is one authoritative stream-head record CAS
plus immutable commit-log records and derived indexes. If a future Oxia client exposes a supportable
multi-key transaction API, it needs a new spike and migration note before replacing this protocol.
The archived pre-M0.5 design is kept in `09-legacy-oxia-multi-key-commit-design.md` as reference only.

## 8. Contract Tests

`nereus-metadata-oxia` needs contract tests that run against both the fake and real Oxia adapter:

- stream-head CAS advances committed end and commitVersion exactly once；
- commit-log records are invisible until reachable from `StreamHeadRecord.lastCommitId`；
- compatible head CAS conflicts caused by same-writer renew or trim retry without fencing the append；
- derived offset-index and committed-slice materialization is idempotent；
- failure after head CAS but before materialization is recoverable by same-slice retry and read repair；
- derived-index repair respects `maxRecordsToRepair` and reports budget exhaustion separately from
  corruption；
- mixed stream/object key group commit is not expressible on the ack path；
- missing or wrong partition key is rejected by the fake and detected in real-adapter tests；
- stale session token maps to `FENCED_APPEND`；
- committed-end conflict maps to `OFFSET_CONFLICT`；
- committed-slice marker or head-chain replay prevents duplicate same physical slice commit；
- partition key is passed for get/put/scan/watch；
- metadata versions are monotonic and separated from durable `commitVersion`；
- fixed-width offset scan returns `[9, 10)` for target offset `9`；
- object reference repair can rebuild references from stream-head commit chain and materialized offset
  index；
- watch notifications are hints only and cache correctness survives missed, duplicate, collapsed, and
  out-of-order notifications；
- metadata codec rejects wrong record type, unknown required schema version, checksum mismatch, and
  truncated payload using the same codec in fake and real adapters。
