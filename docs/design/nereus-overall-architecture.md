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

Nereus V2 is a multi-protocol Storage Fabric with topic-level storage choices:

- Kafka Object mode targets AutoMQ-class cost and throughput with a protocol-native Kafka runtime;
- Kafka BookKeeper modes offer a lower-latency, explicitly higher-cost option;
- Pulsar BookKeeper mode must not weaken native ManagedLedger behavior or performance;
- Pulsar Object mode is cost-first and accepts a higher acknowledgement latency;
- KoP design remains available but is deferred from the 0.2 runtime.

The product does not force all objectives through one WAL or one universal position. Kafka and Pulsar retain independent
Protocol Cells, Position Domains, and Native Write Authorities while sharing correctness, immutable physical
descriptors, lifecycle, and observability contracts.

## 2. Architecture

```text
Nereus Storage Fabric
├── Kafka Protocol Cell
│   ├── Kafka protocol-native runtime / KRaft / UnifiedLog semantics
│   └── Kafka Topic Protocol Bindings -> Kafka Position Domain
├── Pulsar Protocol Cell
│   ├── Pulsar protocol-native runtime / MetadataStore / ManagedLedger semantics
│   └── Pulsar Topic Protocol Bindings -> Pulsar Position Domain
└── Shared Data Plane
    ├── append-only Storage Epoch chains
    ├── Protocol Coverage
    │   ├── Kafka Offset Range
    │   └── ledger-keyed Pulsar Coverage
    ├── Physical Extent
    │   ├── Object WAL / Object segments
    │   └── BookKeeper ledgers
    └── manifests / materialization / cache / retention / GC / observability
```

The two position domains are not numerically comparable. Shared components join typed Protocol Coverage to Physical
Extents; they never turn an object key or BookKeeper coordinate into a second protocol position truth.

## 3. Stable component boundaries

### Protocol runtime

Owns request semantics, native positions, producer/transaction/cursor state, visibility, leader/owner lifecycle, and the
client response. One Topic Incarnation has exactly one Position Domain and one Native Write Authority at a time.

### Topic Protocol Binding, Storage Epoch, and ownership

Creates an immutable Topic Protocol Binding per Topic Incarnation. The binding fixes Protocol Cell, protocol kind,
Position Domain, payload mapping, and Native Write Authority. An append-only Storage Epoch chain selects the immutable
profile/format/checksum/encryption contract for each protocol-native frontier interval. The exact online transition
matrix and transition state machine remain open; at most one epoch may admit new positions at a time.

Ownership grants an exclusive Owner Epoch inside that binding. Kafka uses KRaft; Pulsar uses MetadataStore/Oxia plus
native broker/ManagedLedger authority. Owner Epoch and Storage Epoch are distinct.

### WAL

Owns durable bytes and the binding-scoped typed durable frontier:

- `OBJECT_WAL` batches multiple frames into immutable bounded Object groups;
- `BOOKKEEPER_WAL_ONLY` acknowledges at BookKeeper quorum and has no Object dependency;
- `BOOKKEEPER_WAL_ASYNC_OBJECT` acknowledges at BookKeeper quorum and offloads sealed Protocol Coverage later.

Normal admitted append performs no remote control-metadata read or mutation. An ACK proves the complete returned Kafka
Offset Range or Pulsar Coverage, not an untyped global range.

### Segment and manifest

Immutable descriptors keep Protocol Coverage separate from Physical Extent and identify their Topic Protocol Binding,
Topic Incarnation, and Storage Epoch. One fenced manifest root selects the logical read view. Physical generations may
overlap during publication and grace; the binding-scoped logical result may not be ambiguous.

### Materialization, compaction, and GC

Workers consume frozen source roots, write deterministic outputs, and publish only after revalidating durable authority.
Logical trim is separate from physical deletion. GC requires every protocol retention floor, read pin, task/source
protection, generation root, response-loss state, and grace condition to be safe.

### Metadata backends

Shared capabilities are split into protocol binding, storage epoch, ownership, manifest, typed trim, and work
coordination. KRaft stores durable low-churn Kafka roots; it is not used for per-append metadata or high-churn worker
heartbeats. Pulsar retains native ledger/cursor/offload authority alongside Nereus lifecycle roots.

## 4. Append correctness

The append sequence is:

1. open the immutable Topic Protocol Binding and active Storage Epoch, then acquire ownership outside normal append;
2. serialize one binding's append admission;
3. allocate positions through its Kafka or Pulsar Position Domain;
4. write frames carrying typed Protocol Coverage to the selected primary WAL;
5. validate provider completion, Storage Epoch, and current Owner Epoch;
6. advance the binding's contiguous typed durable/readable frontier;
7. return the protocol-native success only for fully covered positions;
8. publish sealed/read-optimized generations asynchronously.

Control metadata is not the append linearization point. A timeout is an uncertain outcome resolved from deterministic
identity, binding/incarnation, Storage Epoch, Owner Epoch, typed coverage, length, checksum, and durable predecessor
coverage.

Detailed contract: [Correctness and append](../v2/01-correctness-and-append.md).

## 5. Object path

Object WAL uses bounded group commit to control request cost. A group-level node/shard epoch identifies the Physical
Extent; every frame carries its own binding, incarnation, Storage Epoch, Owner Epoch, and typed Protocol Coverage. ACK
frontiers advance independently per binding, so one unrelated binding does not impose a shard-wide correctness barrier.

The WAL object is already the durable Object copy. Background work rewrites it into read-optimized segments and indexes.
PUT-response-loss recovery verifies length and trustworthy checksum/version identity; ETag alone is not sufficient.

Detailed contract: [Object WAL](../v2/03-object-wal.md).

## 6. BookKeeper and Pulsar path

BookKeeper ACK never waits for Object storage. Kafka on BookKeeper retains Kafka Offset Range as protocol truth and uses
BookKeeper only as a Physical Extent. Pulsar BookKeeper profiles preserve native ManagedLedger positions, ledger-chain
ordering, and lifecycle. Cross-ledger Pulsar Coverage is a ledger-keyed range collection; V2 does not persist
`ledgerBase + entryId` as a universal offset.

The exact Kafka ledger layout and Pulsar async-offload authority remain M2 gates. Pulsar Object WAL retains Pulsar
Position/MessageId truth over Object Extents; BookKeeper/Object profile-transition mechanics remain an open question.

Nereus cannot delete a ledger solely because a separate object manifest was published while stock ManagedLedger still
references the ledger. The preferred design integrates with native offload metadata/lifecycle.

Detailed contract: [BookKeeper and Pulsar](../v2/04-bookkeeper-and-pulsar.md).

## 7. Read and lifecycle

Within one Topic Protocol Binding and Storage Epoch chain, the reader resolves active tail, then the manifest-selected
preferred sealed generation, then an exact protected source fallback. Cache never selects authority. A corrupt preferred
generation is quarantined; fallback is allowed only while the source remains protected.

Timestamp and protocol-position indexes are published with the same source cut as payload bytes. Materialization and
compaction cannot plan from a stale local metadata snapshot without final durable revalidation.

Detailed contract: [Manifest, read, retention, and GC](../v2/05-manifest-read-retention-gc.md).

## 8. Ownership and handoff

Ownership tokens bind Protocol Cell, Topic Protocol Binding, Topic Incarnation, Storage Epoch, Owner Epoch, and backend
version. Stale owners cannot admit new positions or publish an in-flight completion.

Planned handoff may provide a short-lived hint containing typed durable/readable frontiers, active Physical Extent, and
manifest root. The target validates the hint and otherwise performs bounded durable recovery. The hint is never the only
correctness path.

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
accepted or provisional compromise. Important examples are group-commit latency, protocol-native position domains,
multi-protocol shared-infrastructure isolation, two metadata backends, protocol-boundary duplication, epoch-scoped
profiles, physical generation overlap, no synchronous BK/Object double write, and the clean V1 break.

## 11. Status and historical boundary

M0 is documentation-only. V2 runtime implementation and evidence are NotStarted. Existing V1 implementation contracts,
including [BookKeeper primary WAL](../phase-bk-bookkeeper-primary-wal/README.md), remain historical evidence until their
code slices are replaced. Their exact archive is
`v0.1@a14d925da5763f36208f8ddca7bef31f3eb90b0b`.

See the [V2 implementation plan](../v2/08-implementation-plan-and-gates.md) and
[scenario matrix](../v2/09-scenario-evidence-matrix.md).
