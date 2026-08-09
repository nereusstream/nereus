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
Protocol Cells, Position Domains, Native Write Authorities, and Cell Provider Sessions while sharing correctness,
immutable physical descriptors, lifecycle contracts, and optional external provider infrastructure.

## 2. Architecture

```text
Nereus Storage Fabric
├── Kafka Protocol Cell
│   ├── Kafka protocol-native runtime / KRaft / UnifiedLog semantics
│   ├── Kafka Topic Protocol Bindings -> Kafka Position Domain
│   └── Cell Provider Scopes / Sessions
├── Pulsar Protocol Cell
│   ├── Pulsar protocol-native runtime / MetadataStore / ManagedLedger semantics
│   ├── Pulsar Topic Protocol Bindings -> Pulsar Position Domain
│   └── Cell Provider Scopes / Sessions
└── Shared Data Plane
    ├── optional shared Object Storage / BookKeeper / transport capacity
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
profile/format/checksum/encryption contract for each protocol-native frontier interval. In 0.2, each Topic Incarnation
has exactly one initial epoch and no online profile-transition API/state machine. The chain model remains explicit for
future evolution, and at most one epoch may ever admit new positions at a time.

Binding plus initial epoch are one immutable `TopicBindingAggregateRecord`. Kafka adds it to the atomic `CreateTopics`
result; MetadataStore/Oxia creates one key and resolves a lost response by exact reread equality. Binding and epoch APIs
are typed projections, not separately writable authorities. Kafka native topic UUID or Pulsar persistence-name plus
generation is the typed incarnation/ABA fence; aggregate and binding/initial-epoch IDs are retry-stable deterministic
derivations. Partial or conflicting state never admits I/O.

Ownership grants an exclusive Owner Epoch inside that binding. Kafka uses KRaft; Pulsar uses MetadataStore/Oxia plus
native broker/ManagedLedger authority. Owner Epoch and Storage Epoch are distinct.

### Provider boundary

Multiple Protocol Cells may use the same external Object Storage or BookKeeper infrastructure. Each cell nevertheless
owns a distinct Cell Provider Scope and independently drainable/closeable sessions. Namespace, credential/KMS and
operator scope, admission/quota, retry/circuit-breaker state, cache/task roots, and physical-GC authorization are
cell-scoped. Compatible lower-level transport may be pooled, but it owns no protocol, manifest, task, cache, or deletion
authority.

Object groups never cross Protocol Cells in 0.2. Shared worker processes and executors use cell-scoped queues, budgets,
fencing, and authorities. Dedicated provider infrastructure remains an optional deployment topology; intentionally
shared physical infrastructure remains a common physical failure domain.

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

One pre-open WalRun Root fixes scope, prefix, run/session identity, epoch-validation rules, format families, and bounded
recovery budgets. Each conditional group key includes fixed-width sequence/body length plus the complete SHA-256;
restart discovers the ACKed open tail through bounded strong same-prefix LIST, without adding a per-group metadata
commit. Async checkpoint pages only accelerate it.

The WAL object is already the durable Object copy. SHA-256/v1 protects the exact canonical provider request body;
CRC32C/v1 independently protects exact assigned Kafka batch bytes or exact Pulsar ManagedLedger entry bytes after the
outer Object envelope is decoded. Native protocol checksums remain separate. Background work rewrites the WAL into
read-optimized segments and indexes.

One assigned Kafka RecordBatch is one frame, while every frame from one partition storage append is one all-or-none
commit set. One Pulsar ManagedLedger entry is one frame/commit set. Object grouping does not change either boundary.

After PUT-response loss, HEAD is sufficient only when a typed Provider Object Proof matches immutable version, exact
length, SHA-256, and `FULL_OBJECT` scope; otherwise recovery performs a bounded full GET. ETag, application user
metadata, and composite checksum scope are insufficient, and an unverifiable provider is rejected for Object WAL.

Detailed contract: [Object WAL](../v2/03-object-wal.md).

## 6. BookKeeper and Pulsar path

BookKeeper ACK never waits for Object storage. Kafka on BookKeeper retains Kafka Offset Range as protocol truth and uses
BookKeeper only as a Physical Extent. Pulsar BookKeeper profiles preserve native ManagedLedger positions, ledger-chain
ordering, and lifecycle. Cross-ledger Pulsar Coverage is a ledger-keyed range collection; V2 does not persist
`ledgerBase + entryId` as a universal offset.

The exact Kafka ledger layout remains an M2 evidence gate. Pulsar async Object offload processes sealed non-current
ledgers only; one native attempt publishes one bounded data Object followed by one deterministic sparse-index/root
Object. It does not stream the current append ledger in 0.2. Pulsar Object WAL allocates increasing virtual ledger IDs
from a native-excluded cell slice assigned by one bounded deployment-wide CAS registry. Explicit MetadataStore/Oxia
links remain Ledger Chain authority, while Object groups remain Physical Extents. BookKeeper/Object profile-transition
mechanics are deferred beyond 0.2.

For Pulsar `BOOKKEEPER_WAL_ASYNC_OBJECT`, ManagedLedger ledger/offload metadata is the sole attempt, completion,
read/fallback, and deletion-eligibility authority. Nereus provides a `LedgerOffloader`; its manifest is derived and cannot
delete a ledger or overrule native lifecycle state. The offloader exposes the deterministic data/root pair as one
ledger-equivalent `ReadHandle`; persisted attempt location/key derivation survives config drift, the bounded root binds
sealed metadata and contiguous data coverage, and cleanup proves root absent before data deletion.

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
cell-scoped Provider sessions over optional shared infrastructure, two metadata backends, protocol-boundary duplication,
epoch-scoped profiles, physical generation overlap, no synchronous BK/Object double write, and the clean V1 break.

## 11. Status and historical boundary

M0 is documentation-only. V2 runtime implementation and evidence are NotStarted. Existing V1 implementation contracts,
including [BookKeeper primary WAL](../phase-bk-bookkeeper-primary-wal/README.md), remain historical evidence until their
code slices are replaced. Their exact archive is
`v0.1@a14d925da5763f36208f8ddca7bef31f3eb90b0b`.

See the [V2 implementation plan](../v2/08-implementation-plan-and-gates.md) and
[scenario matrix](../v2/09-scenario-evidence-matrix.md).
