---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
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

M1 readiness freezes the pure V2 module/promotion boundary plus NTB1/NSE1 outer identities, NPC1/NTI1 layouts and flat
NTA1 direction, four production metadata capabilities, Kafka's one profile input and linear topic-create admission,
the Oxia-backed ELM ownership-fence candidate boundary, and INSTANCEID-derived fresh-only Registry governance with
inline writers. Round 4 adds exact protocol/Pulsar leaves, Kafka last-wins and remote-log admission, a minimal local
store-wide continuity contract, canonical UUID/NLI1 namespace identity, and one receipt hierarchy. Round 5 permits the
M1.1a foundation slice after removing derived NTA1 authorities, reusing the Oxia dummy barrier client-side, fixing a
120-byte writer row, and removing receipt run/result duplication. The additive domain/SPI foundation and its local
gate now exist. The client-only O1 continuity API/state machine, final client fork, exact server runtime, and focused
evidence now exist. O2's single-key/continuity scaffold is locally verified with a non-promotable receipt; V2 remains
`InProgress` because the activatable descendants are still absent. Complete NTA1
FrameEncodingPolicy/name/total caps, writer count, complete metadata-oxia production codecs, Kafka/Pulsar runtime
adapters, P1/R1, receipt caps, final provider-integration artifacts, and promotion evidence remain open or pending.
The O2 scaffold deliberately cannot activate runtime.

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

Policy also remains typed. Topic/Tenant-or-Namespace classes express latency/cost intent, Protocol Cell/shard policy
owns shared checkpoint/allocator/recovery budgets, and host/process policy supplies resource ceilings only. Correctness,
parser caps, and durable compatibility cannot be switched off. Effective budgets use the minimum across scopes; host
pressure may backpressure or early-seal. Product/Deployment owns the base semantic default; Cell/host cannot replace
it. Values affecting bytes/recovery persist at their Storage Epoch, hard-recovery WalRun Root, Object-group, or
offload-attempt boundary, and one policy identity never crosses those lifecycles.

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
result; MetadataStore/Oxia creates one key and resolves a lost response through a closed exact result. One
`VersionedAggregateSnapshot` supplies Binding/initial-Epoch projections, not separately writable authorities. Bootstrap
IDs are create-only 16-byte identities; `bindingId`/`storageEpochId` are the exact 32-byte NTB1/NSE1 derivations.
Canonical NTA1 is flat/no-tail with only absent initial sealed end and persists only independent semantics; derived
protocol/profile facts and the binding back-reference are not repeated. Its FrameEncodingPolicy/legality/cap table
remains OPEN.
Oxia wraps that payload while Kafka maps generated fields directly into the domain validator. Kafka
activates it only at fresh-bootstrap `nereus.storage.version=2`; V1 and runtime transitions are rejected. Partial or
conflicting state never admits I/O.

Kafka's generated non-flexible wire-v0 record uses API key 32000 and is owned by `TopicImage`; snapshots place it
between the topic and partitions, and `RemoveTopicRecord` removes it with the topic. The sole input-only
`nereus.storage.profile` setting never becomes ConfigRecord authority; request-order, residue-free, one-pass cumulative
batch sizing precedes Raft append. MetadataLoader validates
touched topics at ordinary image publication and scans all topics only for snapshot/bootstrap. Pulsar instead uses
exact `RESERVED -> ACTIVE -> DELETING -> DELETED` selector CAS state plus authoritative ownership-witness A/B and a
gap-safe sequence CAS into one atomic ACTIVE fence word. Unsupported ownership backends fail V2 admission. These native
lifecycle fences prevent partial Kafka images, per-access Oxia I/O, stale fence reinstall, and same-name Pulsar ABA.

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

Kafka further separates Allocated, profile-Durable, Readable/LEO, HW, and LSO. The ordered publication cut includes
locator, producer, transaction/aborted, and Kafka leader-epoch state; `acks=1` waits for LEO and `acks=all` waits for
native ISR-derived HW. BookKeeper/Object durability never silently replaces ISR or transaction visibility.

Control metadata is not the append linearization point. A timeout is an uncertain outcome resolved from deterministic
identity, binding/incarnation, Storage Epoch, Owner Epoch, typed coverage, length, checksum, and durable predecessor
coverage.

Detailed contract: [Correctness and append](01-correctness-and-append.md).

## 5. Object path

Object WAL uses bounded group commit to control request cost. A group-level node/shard epoch identifies the Physical
Extent; every frame carries its own binding, incarnation, Storage Epoch, Owner Epoch, and typed Protocol Coverage. ACK
frontiers advance independently per binding, so one unrelated binding does not impose a shard-wide correctness barrier.

One pre-open WalRun Root fixes physical scope, prefix, run/session identity, binding-context epoch-validation rules,
format families, aggregate hard run bounds, and cumulative recovery envelope. A low-frequency hashed CAS pointer
anchors the current root and bounded predecessor lineage. Up to three builders instantiate lazily under that one Root;
each owns lane-local physical outcome order while shared recovery/memory budgets remain aggregate. Product class/lane
IDs are permanently `0=OBJECT_LATENCY`, `1=OBJECT_BALANCED`, and `2=OBJECT_COST`; target/linger values remain versioned
evidence outputs. Each leaf carries that one-digit lane, sequence, exclusive directory-prefix end, body length, and
complete SHA-256. Sequence allocates only after immutable group-plan admission and before HKDF/encryption/final-body
seal. Restart discovers the provider-resolved open tail through bounded strong LIST without a per-group metadata
commit.

One publisher-epoch-fenced async combiner inventories `ProviderResolvedExtentDescriptor` values through one run-wide
checkpoint predecessor chain and a per-lane `LaneExtentResolvedThrough` vector. It does not wait for every member's
protocol ACK. Root is encoded once per page rather than repeated in 256 rows; rows and Seal contain no binding/read
frontier, ACK, gap, or per-binding coverage. The Seal binds one provider-resolved terminal vector/final head plus
minimum aggregate count/byte facts; three lane-specific chains, Roots, or pointers are not used.

The Root also fixes checkpoint provider-proof mode, adapter/canonicalizer version, and token hard cap. `NONE` is the
default. Only an evidenced Provider may admit a version-bound FULL_OBJECT SHA-256 token; a row then stores only tag,
length, and bounded canonical binary token bytes. It never repeats Root scope or row length/SHA and never makes a
whole-Object GET part of routine recovery.

NWG1 stores an authoritative in-body Binding Context Table and Append Unit Directory. Exact binding/Storage/Owner Epoch
authority is object-local rather than singular in a multi-binding root. One Kafka commit set stays inside one extent and
each frame block is independently compressed, authenticated, and decoded. AES-256-GCM/HKDF-SHA-256 v1 wraps one
run-scoped data key, derives one key per ObjectExtent, and separates authenticated directory and frame nonce domains;
KMS rotation happens only at run rollover.

Physical `LaneExtentResolvedThrough` and logical `BindingDurableFrontier` are separate. Before position allocation, the
owner reserves tracker slot and active-tail locator capacity together. One checked 64-bit owner-local ticket represents
one complete Kafka commit set or Pulsar entry; coverage remains authority and tickets never enter wire/API/config.
Normal completion uses a ring/window and recovery uses bounded collect/sort with fresh tickets.

One shared `VerifiedExtent` feeds compact, range-aggregated Kafka/Pulsar locator spans in a shard-owned segmented
active-tail index. A binding installs the next contiguous locators hidden, publishes Readable/Durable frontiers, then
ACKs; gap-behind locators remain invisible. A generic Protocol Coverage TreeMap is forbidden on the append/ACK hot
path, while no heavy index object per Binding/unit is frozen. A's typed gap cannot block B or physical checkpoint
progress, and the ACK path adds no provider/KMS/metadata I/O or repeated directory decryption.

`BindingReadViewSnapshot` is a logical capture, not one object per ACK/read. Append only installs hidden locators,
release-publishes Readable/Durable frontiers, and ACKs. Low-frequency manifest handoff publishes source-selection
generations that readers pin for one Binding-scoped protocol read batch through allocation-free local RCU/epoch/hazard
or reader-slot state. Each unfinished batch owns one slot from a bounded cross-Binding sharded pool. Standard hazard
ordering publishes `{Binding,G}`, establishes StoreLoad, revalidates G, then captures a coherent generation-tagged
`{sourceGenerationId, ReadableFrontier, activeTailViewVersion}` before dereference. A request may read disjoint manifest
and active-tail ranges, but one append unit and any declared whole-range fallback remain source-pure. The slot has one
atomic `FREE/PINNED(generation)` lease word; callbacks equality-check it, cancellation stops new source use, and only
complete terminal source drain clears it. Nonresponsive calls consume bounded quarantine rather than being force-cleared.

Locator and source-protection retirement are two stages: publish preferred+protected-fallback, drain older-view pins,
then use one Binding selector CAS to atomically select `PREFERRED_ONLY`, close E, grant same-owner no-fallback E+1, and
persist E's closure anchor. Takeover competes on the same exact predecessor. The selector exposes only
`ADMITTING/STOPPED`; a response-unknown cut admits no further E read until exact reread, and STOPPED can recover only to
a fresh epoch. Cross-key reread or backend history cannot supply the anchor. The selector logically owns one small
bounded inline canonical unresolved-anchor set and preserves a dedicated emergency STOPPED envelope. A membership-
neutral transition copies validated canonical bytes; 0.2 adds no anchor page/index/chain.

Every source/protection row inherits its own `first_i`; the selector transition shares `last=E`, and source i releases
only after `[first_i,E]` has continuous source-independent planned-drain/qualified-expiry proof. Batch minimum is
summary only. Inline activation costs one selector CAS; atomically validated reference mode costs one immutable create
plus the CAS; N sources still require up to N exact release CAS operations and bounded O(N) reconciliation. A
quarantined source does not block sibling release but blocks full-batch retirement and consumes capacity.

Each needed proof is deterministic create-only after an asynchronous irreversible terminal cut binding the selector-
carried closure anchor and immutable historical capability evidence. One closed verifier accepts planned-drain or
qualified-expiry variants; on non-transactional backends the immutable candidate supplies safety while owner/reconciler
fences supply ACL/rate/audit. Eligible anchors prune asynchronously in batches or piggyback a selector transition.
Full batch metadata compacts only after every source is released/retired and selector/lineage/recovery/response-loss
references disappear, using the irreversible same-key `FULL_V1 -> RETIRED_V1` exact-version CAS. The compact tombstone
remains permanent in 0.2 and is never source-GC authority. Retired-view/pin, anchor/proof/batch/source/tombstone
count/bytes/age/deadline are hard-bounded; pressure may STOP admission but never delete early. Normal reads still
perform no metadata I/O.

Routine random reads authenticate the Root-bound in-body header/directory and selected frame without first requiring a
new full-Object provider proof. The leaf's exclusive bounded prefix end normally gives prefix plus frame GET; without
it, recovery uses header, directory, and frame GETs. Short/long hints reuse already read bytes, and the hint cannot
authorize an offset. ADR 0058 makes prefix bytes the primary cap and prioritizes 4,096/16,384-frame evidence without a
paginated or second-authority directory. The exposed approximate directory size is an accepted key-metadata tradeoff.

0.2 has no partial-run recovery-skip vector. Except for an already authoritative whole-WalRun retirement frontier,
takeover uses bounded parallel directory-prefix GETs under one cumulative GET/bytes/time envelope. A partial
Root/Seal-bound certificate is reconsidered only if M3/M7 proves this baseline misses its recovery SLO and would have
useful skip hit rate.

WalRun Root and Seal are separate immutable Cell control-metadata records. A successor binds both predecessor
identities, then one exact CAS advances the current-run pointer. A pointer left on a sealed run is recovered forward and
never authorizes reopening it.

The WAL object is already the durable Object copy. SHA-256/v1 protects the exact canonical provider request body;
CRC32C/v1 independently protects exact assigned Kafka batch bytes or exact Pulsar ManagedLedger entry bytes after the
outer Object envelope is decoded. Native protocol checksums remain separate. Background work rewrites the WAL into
read-optimized segments and indexes.

One assigned Kafka RecordBatch is one frame, while every frame from one partition storage append is one all-or-none
commit set. One Pulsar ManagedLedger entry is one frame/commit set. Object grouping does not change either boundary.

After PUT-response loss, HEAD is sufficient only when a typed Provider Object Proof matches immutable version, exact
length, SHA-256, and `FULL_OBJECT` scope; otherwise recovery performs a bounded full GET. ETag, application user
metadata, and composite checksum scope are insufficient, and an unverifiable provider is rejected for Object WAL.
After process loss, a present group is verified; a proven-absent never-ACKed gap fences the old run while preserving
other lanes' provider-resolved extents and advanced binding frontiers, then retries only in a fresh run. Unknown
presence remains fail-closed; 0.2 has no local ciphertext journal claim.

Detailed contract: [Object WAL](03-object-wal.md).

## 6. BookKeeper and Pulsar path

BookKeeper ACK never waits for Object storage. Kafka on BookKeeper retains Kafka Offset Range as protocol truth and uses
BookKeeper only as a Physical Extent. Pulsar BookKeeper profiles preserve native ManagedLedger positions, ledger-chain
ordering, and lifecycle. Cross-ledger Pulsar Coverage is a ledger-keyed range collection; V2 does not persist
`ledgerBase + entryId` as a universal offset.

Kafka BookKeeper profiles use one logical ledger chain per partition, low-frequency run/generation roots, packed
in-ledger RecordBatch range-index checkpoints, and an owner-local active-tail locator. One partition append remains the
atomic commit set; one RecordBatch is the read-index unit; one sealed run is the lifecycle unit. Bounded BookKeeper I/O
may complete out of order, but publication/ACK advances only through the contiguous Kafka Offset prefix. Fetch uses
run/block floor lookup and targeted entry reads rather than remote metadata or whole-extent checksum reads. M2 still
owns exact `NBKE2` bytes, numeric bounds, and 10k/100k dedicated-ledger evidence; pooled ledgers require a future ADR.

Pulsar async Object offload processes sealed non-current ledgers only; one native attempt publishes one bounded data
Object followed by one deterministic sparse-index/root Object. It does not stream the current append ledger in 0.2.
Pulsar Object WAL allocates increasing virtual ledger IDs
from one fixed aligned `2^40`, never-reused Cell slice assigned by the 64-KiB/256-lifetime Registry selected for its
immutable ledger-ID compatibility namespace. The Registry binds a complete writer commitment and exclusion interlock.
Slice ownership survives broker/provider change and retirement leaves a permanent tombstone. 0.2 never resizes,
relocates, extends, or adds a second slice; exhaustion fails closed and new capacity uses a new Cell. After Registry
exhaustion a new logical domain is insufficient: capacity requires a new compatibility namespace backed by a disjoint
ledger-ID space, or an independent cluster. Explicit MetadataStore/Oxia links remain Ledger Chain authority, while
Object groups remain Physical Extents. BookKeeper/Object profile-transition mechanics are deferred beyond 0.2.

STRICT_SERIALIZED and RANGE_LEASED remain open allocator protocols. ADR 0055 requires maximum sustainable rollover RPS
under every predeclared SLO, actual rollover distribution/jitter/storm/failure cuts, and native Pulsar
rollover/append-stall comparison, including mass broker takeover. Any RANGE candidate keeps its grant with the
ManagedLedger incarnation across owner-only head fencing, lets a new owner finish the same RESERVED grant, and burns at
most one stale candidate. Installed-range use does not wait for allocator clear, but a high-priority reconciler must
unblock the next Cell grant. These constraints do not select RANGE_LEASED.

ADR 0086 is authoritative for the Kafka BookKeeper run/range-index and ordered-pipeline direction. ADR 0087 owns the
Kafka frontier, idempotency, transaction, shared-storage ISR/HW, delayed-Fetch, compaction-gap, and bounded protocol-
recovery semantics layered over it.

For Pulsar `BOOKKEEPER_WAL_ASYNC_OBJECT`, ManagedLedger ledger/offload metadata is the sole attempt, completion,
read/fallback, and deletion-eligibility authority. Nereus provides a `LedgerOffloader`; its manifest is derived and cannot
delete a ledger or overrule native lifecycle state. The offloader exposes the deterministic data/root pair as one
ledger-equivalent `ReadHandle`; persisted attempt location/key derivation survives config drift, the bounded root binds
sealed metadata and contiguous data coverage using NPO1 hard parser limits. While both sources are eligible, native
reads allow one whole-range fallback over independently verifiable NPD1 blocks. One ManagedLedger-owned composite
handle owns both children and source pins. Topic/Namespace policy persists `RETAIN_BK` or `DELETE_AFTER_VERIFIED`; the
latter drains BK pins before exact Object revalidation and irreversible BK_DELETE_INTENT/DONE. The compatibility
`bookkeeperDeleted=true` fence may mean INTENT or DONE, but only DONE proves physical absence. Cleanup proves root
absent before data deletion.

NPD1 uses checked 32-byte data/64-byte block envelopes and a 16-byte derived-entry-ID row. Upload, digest, and full
verification stream or use bounded segments; the Object hard cap is not an allocation size. Block-policy evidence uses
1/4/8/16-MiB candidates, while Product/Deployment owns the base default, Namespace/Topic may override, Cell admits /
caps, and the resolved class persists in the offload attempt.

Detailed contract: [BookKeeper and Pulsar](04-bookkeeper-and-pulsar.md).

## 7. Read and lifecycle

Within one Topic Protocol Binding and Storage Epoch chain, the reader resolves active tail, then the manifest-selected
preferred sealed generation, then an exact protected source fallback. Cache never selects authority. A corrupt preferred
generation is quarantined; fallback is allowed only while the source remains protected.

Active-tail correctness cannot be disabled. Binding/tenant policy may choose only a conservative soft share;
shard/Cell/host ceilings remain hard. Takeover rebuilds physical Root/checkpoint/LIST inventory first, then publishes
Binding views independently. Locator retirement waits for manifest coverage of the same typed range and safe
source-protection/read-pin state.

Timestamp and protocol-position indexes are published with the same source cut as payload bytes. Materialization and
compaction cannot plan from a stale local metadata snapshot without final durable revalidation.

Detailed contract: [Manifest, read, retention, and GC](05-manifest-read-retention-gc.md).

## 8. Ownership and handoff

Ownership tokens bind Protocol Cell, Topic Protocol Binding, Topic Incarnation, Storage Epoch, Owner Epoch, and backend
version. Stale owners cannot admit new positions or publish an in-flight completion.

Planned handoff may provide a short-lived hint containing typed durable/readable frontiers, active Physical Extent, and
manifest root. The target validates the hint and otherwise starts from the authoritative current-WalRun pointer and
performs bounded durable recovery. The hint is never the only correctness path.

Detailed contract: [Metadata backends and handoff](06-metadata-backends-and-handoff.md).

## 9. Product acceptance

Kafka and Pulsar goals are exact-source gates, not architectural claims:

- Kafka Object comparison pins one clean AutoMQ commit, equal durability/resources/request budget, thresholds, and a
  reconstruction receipt;
- Kafka BookKeeper results disclose their higher cost;
- Pulsar BookKeeper runs native feature and performance parity against a pinned Pulsar source;
- old V1 receipts do not qualify V2.

Detailed contract: [Protocol integrations](07-protocol-integrations.md).

## 10. Explicit tradeoffs

The normative [tradeoff register](tradeoffs.md) records the benefit, cost, mitigation, and evidence gate for every
accepted or provisional compromise. Important examples are group-commit latency, protocol-native position domains,
cell-scoped Provider sessions over optional shared infrastructure, two metadata backends, protocol-boundary duplication,
epoch-scoped profiles, physical generation overlap, no synchronous BK/Object double write, and the clean V1 break.

## 11. Status and historical boundary

M0 remains the documentation/source-lock baseline. The M1.1a-A domain/SPI foundation is implemented and locally gated,
but V2 runtime activation and current-source promotion evidence are not started. Existing V1 implementation contracts,
including [BookKeeper primary WAL](../v1/phase-bk-bookkeeper-primary-wal/README.md), remain historical evidence until
their
code slices are replaced. Their exact archive is
`v0.1@a14d925da5763f36208f8ddca7bef31f3eb90b0b`.

See the [V2 implementation plan](08-implementation-plan-and-gates.md) and
[scenario matrix](09-scenario-evidence-matrix.md).
