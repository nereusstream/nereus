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
- offset index, committed end offset, append session, and committed-slice marker must be committed in one
  stream key group；
- object manifest/reference updates are repairable and must not force cross-shard atomicity；
- Phase 1 modules must not depend on Pulsar classes。

Pulsar's Oxia implementation proves that Oxia can be integrated as a Pulsar metadata backend. It does not
prove that Pulsar's generic metadata API is sufficient for Nereus stream visibility commits.

## 6. Oxia Java Client Surface Checked

On 2026-07-05, the public Oxia Java client repository exposes:

- [`AsyncOxiaClient.java`](https://github.com/oxia-db/oxia-client-java/blob/main/client-api/src/main/java/io/oxia/client/api/AsyncOxiaClient.java)
- [`SyncOxiaClient.java`](https://github.com/oxia-db/oxia-client-java/blob/main/client-api/src/main/java/io/oxia/client/api/SyncOxiaClient.java)
- [`client.proto`](https://github.com/oxia-db/oxia-client-java/blob/main/client/src/main/proto/io/streamnative/oxia/client.proto)

Observed surface:

- public sync/async APIs expose single-key `put`, `delete`, `get`, `list`, `rangeScan`, notifications, and
  sequence-update calls；
- `PutOption`/`DeleteOption` support conditional version checks and partition key options；
- the proto contains batched write messages, but the public Java API surface above does not by itself prove
  a supportable multi-key conditional commit API for Nereus。

Therefore the Phase 1 design must keep treating single-key-group conditional multi-write as an unproven
capability until a real-client spike proves the exact API and failure semantics.

## 7. Required Capability Spike

Before implementing the real `OxiaMetadataStore`, run a small capability spike against the exact Oxia
Java client dependency planned for Phase 1. This is a stop-the-line gate, not a nice-to-have experiment.

1. Verify how to route every stream-scoped key with the same partition key.
2. Verify whether the Java client supports conditional multi-write within one key group through a public
   or supportable API.
3. Verify version/CAS failure mapping for stale append sessions and offset conflicts.
4. Verify sequence key behavior if we use server-assigned stream ids, commit versions, or virtual ledger ids.
5. Verify watch/notification ordering and whether notifications can collapse intermediate updates.
6. Verify range scan ordering with fixed-width numeric key encoding.

Required spike shape:

- create two or more keys routed by the same stream partition key；
- issue one commit attempt that conditionally updates committed-end, writes one offset-index key, and
  writes one committed-slice marker；
- force a condition failure on one participating key and prove no participating write is applied；
- force concurrent writers racing on the same expected committed end and prove exactly one visible commit；
- run against the real client, not only `FakeOxiaMetadataStore`；
- record API calls, observed exception types, and whether partition key is explicit on each operation。

If conditional multi-write is not available, Phase 1 must not emulate it with unsafe multi-step writes.
Instead, redesign the append linearization around one authoritative stream-head record CAS plus derived
index records, or another Oxia-supported atomic primitive.

## 8. Contract Tests

`nereus-metadata-oxia` needs contract tests that run against both the fake and real Oxia adapter:

- same stream key group commit succeeds atomically；
- mixed stream/object key group commit is rejected on the ack path；
- missing or wrong partition key is rejected by the fake and detected in real-adapter tests；
- stale session token maps to `FENCED_APPEND`；
- committed-end conflict maps to `OFFSET_CONFLICT`；
- committed-slice marker prevents duplicate slice commit；
- partition key is passed for get/put/scan/watch；
- metadata versions are monotonic and separated from durable `commitVersion`；
- fixed-width offset scan returns `[9, 10)` for target offset `9`；
- object reference repair can rebuild references from offset index；
- watch notifications are hints only and cache correctness survives missed, duplicate, collapsed, and
  out-of-order notifications；
- metadata codec rejects wrong record type, unknown required schema version, checksum mismatch, and
  truncated payload using the same codec in fake and real adapters。
