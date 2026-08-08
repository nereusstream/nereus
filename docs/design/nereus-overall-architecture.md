---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
evidenceStatus: DocumentationOnly
authority: ProductArchitecture
sourceTuple: v2-m0
---

# Nereus V2 overall architecture

## 1. Product objective

Nereus V2 is a multi-protocol shared-storage engine with topic-level storage choices:

- Kafka Object mode targets AutoMQ-class cost and throughput with a protocol-native Kafka runtime;
- Kafka BookKeeper modes offer a lower-latency, explicitly higher-cost option;
- Pulsar BookKeeper mode must not weaken native ManagedLedger behavior or performance;
- Pulsar Object mode is cost-first and accepts a higher acknowledgement latency;
- KoP design remains available but is deferred from the 0.2 runtime.

The product does not force all objectives through one WAL. It shares correctness, immutable object, lifecycle, and
observability contracts while preserving different Object and BookKeeper data paths.

## 2. Architecture

```text
Kafka clients                         Pulsar clients
     |                                     |
Kafka protocol-native runtime        Pulsar protocol-native runtime
KRaft / UnifiedLog semantics         ManagedLedger / cursor semantics
     |                                     |
     +---------- TopicStorageBinding ------+
                        |
            exclusive writer epoch
                        |
        +---------------+----------------+
        |                                |
  Object WAL engine                BookKeeper WAL engine
  cost-first ACK                   quorum/performance ACK
        |                                |
        +--------- sealed sources -------+
                        |
          materialization / compaction
                        |
        immutable segments + indexes
                        |
        manifest-selected logical view
                        |
       retention proof + physical GC
```

## 3. Stable component boundaries

### Protocol runtime

Owns request semantics, native positions, producer/transaction/cursor state, visibility, leader/owner lifecycle, and the
client response. It selects the already-bound storage profile but does not redefine that profile at runtime.

### Topic binding and ownership

Creates an immutable semantic binding per topic incarnation and grants an exclusive writer epoch. Kafka uses KRaft;
Pulsar uses MetadataStore/Oxia plus native broker/ManagedLedger authority.

### WAL

Owns durable bytes and the per-stream contiguous durable prefix:

- `OBJECT_WAL` batches multiple frames into immutable bounded Object groups;
- `BOOKKEEPER_WAL_ONLY` acknowledges at BookKeeper quorum and has no Object dependency;
- `BOOKKEEPER_WAL_ASYNC_OBJECT` acknowledges at BookKeeper quorum and offloads sealed ranges later.

Normal admitted append performs no remote control-metadata read or mutation.

### Segment and manifest

Immutable descriptors identify source and materialized ranges. One fenced manifest root selects the logical read view.
Physical generations may overlap during publication and grace; the logical result may not be ambiguous.

### Materialization, compaction, and GC

Workers consume frozen source roots, write deterministic outputs, and publish only after revalidating durable authority.
Logical trim is separate from physical deletion. GC requires every protocol retention floor, read pin, task/source
protection, generation root, response-loss state, and grace condition to be safe.

### Metadata backends

Shared capabilities are split into binding, ownership, manifest, trim, and work coordination. KRaft stores durable
low-churn Kafka roots; it is not used for per-append metadata or high-churn worker heartbeats. Pulsar retains native
ledger/cursor/offload authority alongside Nereus lifecycle roots.

## 4. Append correctness

The append sequence is:

1. open the immutable binding and acquire ownership outside normal append;
2. serialize one stream's append admission;
3. allocate a dense local range;
4. write frames to the selected primary WAL;
5. validate provider completion and the current writer epoch;
6. advance the stream's contiguous durable/readable prefix;
7. return the protocol-native success;
8. publish sealed/read-optimized generations asynchronously.

Control metadata is not the append linearization point. A timeout is an uncertain outcome resolved from deterministic
identity, range, writer epoch, length, checksum, and durable predecessor coverage.

Detailed contract: [Correctness and append](../v2/01-correctness-and-append.md).

## 5. Object path

Object WAL uses bounded group commit to control request cost. A group-level node/shard epoch identifies the physical
object; every frame carries its own stream incarnation and writer epoch. ACK prefixes advance independently per stream,
so one unrelated stream does not impose a shard-wide correctness barrier.

The WAL object is already the durable Object copy. Background work rewrites it into read-optimized segments and indexes.
PUT-response-loss recovery verifies length and trustworthy checksum/version identity; ETag alone is not sufficient.

Detailed contract: [Object WAL](../v2/03-object-wal.md).

## 6. BookKeeper and Pulsar path

BookKeeper ACK never waits for Object storage. Pulsar BookKeeper profiles preserve native ManagedLedger positions and
lifecycle. The exact mapping between common logical ranges and `(ledgerId, entryId)`, plus async offload authority,
remains an M2 design gate.

Nereus cannot delete a ledger solely because a separate object manifest was published while stock ManagedLedger still
references the ledger. The preferred design integrates with native offload metadata/lifecycle.

Detailed contract: [BookKeeper and Pulsar](../v2/04-bookkeeper-and-pulsar.md).

## 7. Read and lifecycle

The reader resolves active tail, then the manifest-selected preferred sealed generation, then an exact protected source
fallback. Cache never selects authority. A corrupt preferred generation is quarantined; fallback is allowed only while
the source remains protected.

Timestamp and offset indexes are published with the same source cut as payload bytes. Materialization and compaction
cannot plan from a stale local metadata snapshot without final durable revalidation.

Detailed contract: [Manifest, read, retention, and GC](../v2/05-manifest-read-retention-gc.md).

## 8. Ownership and handoff

Ownership tokens bind protocol topic identity, incarnation, owner epoch, and backend version. Stale owners cannot admit
new offsets or publish an in-flight completion.

Planned handoff may provide a short-lived hint containing durable end, active source, and manifest root. The target
validates the hint and otherwise performs bounded durable recovery. The hint is never the only correctness path.

Detailed contract: [Metadata backends and handoff](../v2/06-metadata-backends-and-handoff.md).

## 9. Product acceptance

Kafka and Pulsar goals are exact-source gates, not architectural claims:

- Kafka Object comparison pins one clean AutoMQ commit, equal durability/resources/request budget, thresholds, and a
  reconstruction receipt;
- Kafka BookKeeper results disclose their higher cost;
- Pulsar BookKeeper runs native feature and performance parity against a pinned Pulsar source;
- old V1 receipts do not qualify V2.

Detailed contract: [Protocol integrations](../v2/07-protocol-integrations.md).

## 10. Explicit tradeoffs

The normative [tradeoff register](../v2/tradeoffs.md) records the benefit, cost, mitigation, and evidence gate for every
accepted or provisional compromise. Important examples are group-commit latency, two metadata backends, protocol-boundary
duplication, immutable topic profile, physical generation overlap, no synchronous BK/Object double write, and the clean
V1 break.

## 11. Status and historical boundary

M0 is documentation-only. V2 runtime implementation and evidence are NotStarted. Existing V1 implementation contracts,
including [BookKeeper primary WAL](../phase-bk-bookkeeper-primary-wal/README.md), remain historical evidence until their
code slices are replaced. Their exact archive is
`v0.1@a14d925da5763f36208f8ddca7bef31f3eb90b0b`.

See the [V2 implementation plan](../v2/08-implementation-plan-and-gates.md) and
[scenario matrix](../v2/09-scenario-evidence-matrix.md).
