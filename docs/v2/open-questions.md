---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeQuestionLog
sourceTuple: v2-m0
---

# V2 open questions

This file records proposals that have not been accepted as runtime contracts and retains resolved gate IDs for
traceability. An answer moves into a normative document or ADR only after explicit confirmation; editing this file
alone cannot close a gate.

## Restarted Grill 2: current frontier

The user explicitly confirmed all five round-4 recommendations. ADRs 0028 through 0032 now resolve
`V2-OPEN-META-03`, `V2-OPEN-BK-05`, `V2-OPEN-OBJ-07`, `V2-OPEN-OBJ-08`, and `V2-OPEN-PUL-OBJ-03`. The next
independent frontier is:

| Gate | Decision needed now | Current recommendation, not a decision |
| --- | --- | --- |
| `V2-OPEN-META-04` | which logical v1 fields and compatibility axis freeze the aggregate schema | use one whole-record schema version, a closed immutable payload, and one shared logical validator across backends |
| `V2-OPEN-KAF-META-01` | how Kafka activates the clean-break V2 metadata format | reuse `nereus.storage.version` at fresh-format-only level 2 and reject V1/runtime upgrade or downgrade |
| `V2-OPEN-BK-06` | which canonical wire and hard parser limits freeze sealed-ledger root v1 | use a bounded self-digesting big-endian `NPO1` format with four ordered sections |
| `V2-OPEN-BK-07` | which final proof is required before native BookKeeper source deletion | revalidate the exact root/data/read path, then recheck native attempt state under CAS |
| `V2-OPEN-BK-08` | which one-shot Object/BookKeeper fallback states and errors are legal | use whole-range, single-source fallback only while native metadata says both sources remain eligible |
| `V2-OPEN-OBJ-09` | where exact per-binding epochs live in a multi-binding WalRun | keep the root physical/run-scoped and put binding incarnation plus exact epochs in object-local binding/frame context |
| `V2-OPEN-OBJ-10` | whether crash recovery of a provider-absent unACKed group requires a local ciphertext journal | avoid a local durability prerequisite; burn the old run/sequence and require an idempotent fresh retry |
| `V2-OPEN-OBJ-11` | which bounded WalRun lifecycle prevents an eventually unrecoverable prefix | cap count, bytes, age, and recoverable predecessors, then drain/seal/roll over before any bound is crossed |
| `V2-OPEN-OBJ-12` | whether the worst-case recovery envelope constrains normal ACK/admission | make the full cumulative recovery envelope an append-admission invariant |
| `V2-OPEN-OBJ-13` | which authority discovers the current WalRun Root and bounded lineage | use one low-frequency per-shard CAS pointer bound to the exact root hash and run epoch |
| `V2-OPEN-OBJ-14` | where the authoritative append-unit directory lives and whether a commit set may span extents | put a bounded directory in a new `NWG1` body and prohibit one commit set from crossing an ObjectExtent |
| `V2-OPEN-PUL-OBJ-04` | which durable identity owns a virtual-ledger slice | bind it to deployment/reservation domain and immutable Pulsar Protocol Cell identity |
| `V2-OPEN-PUL-OBJ-05` | which irreversible slice lifecycle preserves never-reuse | use `ACTIVE -> RETIRING -> RETIRED`; keep exhaustion derived and retired rows forever |
| `V2-OPEN-PUL-OBJ-06` | which slice geometry and lifetime caps keep the registry bounded | use one fixed aligned `2^k` slice per Cell plus independent encoded-registry limits |

The complete questions and recommendations are in
[round 5](grill-notes/07-restarted-grill-2-wire-recovery-and-slice-contracts.md). None of these fourteen new
recommendations is accepted yet.

## Initial binding and epoch publication

### `V2-OPEN-META-01`: resolved atomic visible create

Resolved by [ADR 0019](../decisions/0019-v2-initial-binding-epoch-atomic-visibility.md). Topic Protocol Binding and its
initial Storage Epoch form one visible `TopicBindingAggregate`. Incomplete or uncertain create is recovered or rejected;
it never admits open, append, or read and never causes a default epoch to be invented.

### `V2-OPEN-META-02`: resolved aggregate physical representation

Resolved by [ADR 0023](../decisions/0023-v2-topic-binding-aggregate-record.md). One immutable
`TopicBindingAggregateRecord` physically contains the complete binding and initial epoch. Kafka adds it to atomic
`CreateTopics`; MetadataStore/Oxia creates one key and resolves response loss by exact reread equality. Logical stores
are typed projections, and 0.2 does not use a cross-key `CREATING` saga.

### `V2-OPEN-META-03`: resolved aggregate incarnation, key, and deterministic IDs

Resolved by [ADR 0028](../decisions/0028-v2-topic-incarnation-keys-and-deterministic-ids.md). Kafka native topic UUID or
Pulsar canonical persistence/name facts plus binding generation form a protocol-discriminated incarnation. Aggregate
keys are incarnation-scoped, values repeat the complete identity, and binding/initial-epoch IDs are separate
domain-separated deterministic SHA-256 derivations with no retry-dependent input.

### `V2-OPEN-META-04`: aggregate logical schema v1

Open. The current recommendation is one `aggregateSchemaVersion=1` compatibility axis over an immutable, closed
`TopicBindingAggregateV1` payload containing the complete binding plus ordinal-zero epoch. It would exclude lifecycle,
attempt, timestamp, controller-offset, backend-version, and extension-map state; Kafka and Oxia would share validation
and golden vectors without requiring byte-identical physical records. Confirmation must precede the M1 schema freeze.

### `V2-OPEN-KAF-META-01`: Kafka V2 feature activation

Open. The current recommendation reuses `nereus.storage.version`, makes level 2 the only feature level accepted by V2
nodes, and permits it only at a fresh KRaft storage format/bootstrap. Level 1 remains the V1 format and runtime
0/1-to-2 upgrades or 2-to-0/1 downgrades would be rejected. This clean-break choice and its controller validation must
be confirmed before the Kafka metadata schema is frozen.

## Object WAL durability verification

### `V2-OPEN-OBJ-02`: resolved PUT-response-loss proof

Resolved by [ADR 0018](../decisions/0018-v2-object-wal-uncertain-put-proof.md). When an immutable Object WAL PUT may have
succeeded but the response was lost:

1. use HEAD only when it returns exact length plus a trustworthy content checksum bound to the immutable object
   identity/version;
2. otherwise perform a bounded GET and recompute the expected checksum;
3. never treat ETag alone as content identity;
4. do not admit a provider to `OBJECT_WAL` when deterministic immutable create, the required read-after-write
   behavior, or bounded verification cannot be established.

This closes the design choice only. M3 still needs real-provider response-loss and checksum-drift evidence.

### `V2-OPEN-OBJ-04`: resolved checksum byte domains

Resolved by [ADR 0021](../decisions/0021-v2-object-wal-checksum-domains.md). `ObjectExtentDigest` protects the exact
canonical provider request body, while `FramePayloadChecksum` protects the binding-defined protocol payload bytes after
Object decode. The fields and proof domains are distinct and cannot substitute for each other.

### `V2-OPEN-OBJ-05`: resolved initial algorithms and provider-bound proof

Resolved by [ADR 0025](../decisions/0025-v2-initial-checksum-algorithms-and-provider-proof.md).
`ObjectExtentDigest` is SHA-256/v1; `FramePayloadChecksum` is CRC32C/v1. Expected extent identity remains outside the
body, and typed `ProviderObjectProof` must match version, length, SHA-256, and `FULL_OBJECT` scope or recovery performs a
bounded full GET. ETag, user metadata, and composite checksums do not qualify.

### `V2-OPEN-OBJ-06`: resolved canonical protocol frame bytes

Resolved by [ADR 0026](../decisions/0026-v2-protocol-native-frame-payload-bytes.md). Frame CRC32C covers exact assigned
Kafka `MemoryRecords`/batch bytes or exact Pulsar ManagedLedger entry bytes after only the outer Object envelope is
decoded. Application records/messages are not reserialized, and native protocol checksums remain independent.

### `V2-OPEN-OBJ-07`: resolved Object WAL group identity and crash discovery

Resolved by [ADR 0030](../decisions/0030-v2-object-wal-run-root-and-content-addressed-discovery.md). One immutable root
is persisted before opening a run; every group key carries fixed-width sequence, exact body length, and full SHA-256.
Bounded strong same-prefix LIST discovers an ACKed open tail without per-group metadata publication. Async checkpoint
pages are accelerators only, and non-qualifying providers are rejected for `OBJECT_WAL`.

### `V2-OPEN-OBJ-08`: resolved protocol frame and append commit-set granularity

Resolved by [ADR 0031](../decisions/0031-v2-protocol-frame-and-append-commit-set.md). One assigned Kafka RecordBatch is a
frame and every frame from one partition storage append is one all-or-none commit set. One Pulsar ManagedLedger entry is
one frame/commit set. Object groups, requests, transactions, and individual batched messages do not redefine append
atomicity.

### `V2-OPEN-OBJ-09`: multi-binding WalRun epoch placement

Open. The current recommendation preserves cross-binding batching: the root would carry only Cell/provider/shard/run
authority and format/recovery contracts, while each object-local binding context and frame would carry the exact typed
incarnation, Storage Epoch, and Owner Epoch. A singular topic epoch in the root would instead require one run per
binding/epoch and change the cost objective.

### `V2-OPEN-OBJ-10`: provider-absent in-flight group after process loss

Open. A root plus LIST can recover provider-present extents but cannot recreate exact final ciphertext for a proven
absent, never-ACKed group. The current recommendation avoids a broker-local fsync journal: permanently burn the old
run/sequence, return to the proven Durable Frontier, and let protocol idempotency rebuild a fresh attempt. Requiring
same-key retry across process loss would instead require journaling exact post-encryption bytes and completion state.

### `V2-OPEN-OBJ-11`: bounded WalRun lifecycle

Open. The current recommendation caps every run by extent count, canonical bytes, age, and recoverable predecessor
count. Admission would stop before a cap, then drain/reconcile and seal before publishing a successor. Exact numeric
limits remain evidence-derived, while seal/checkpoint/retirement authority and handoff order remain descendants.

### `V2-OPEN-OBJ-12`: recovery envelope as an admission invariant

Open. The current recommendation makes worst-case recovery cost constrain normal ACK/admission across roots/runs,
LIST pages/keys/bytes, HEAD/GET/full-GET work, decoded units, memory, concurrency, retries, and wall time. Fallback may
not reset counters; predicted exhaustion would trigger rollover or backpressure, and actual exhaustion would fail
closed without skipping coverage or advancing the frontier.

### `V2-OPEN-OBJ-13`: current WalRun Root discovery authority

Open. The current recommendation is one low-frequency per-shard CAS `CurrentWalRunPointer` containing the exact root
key/hash and shard run epoch. A successor would link to its predecessor so recovery can walk a bounded lineage to the
retirement frontier. Exact pointer/root wire and lineage bounds depend on the epoch-placement and lifecycle decisions.

### `V2-OPEN-OBJ-14`: in-object append-unit directory and co-location

Open. The current recommendation introduces a new `NWG1` major format with one bounded authoritative
`BindingContextTable + AppendUnitDirectory` near the fixed header. One Kafka append commit set must remain complete in
one ObjectExtent; every frame block would be independently decodable and integrity checked. Exact fields and limits
remain a descendant after this authority/co-location decision.

## Storage Epoch transitions

### `V2-OPEN-MIGRATION-01`: resolved 0.2 transition scope

Which profile transitions are implemented in 0.2, and which remain domain-model capability only?

Earlier transition ordering proposal, retained as input rather than a decision:

1. Pulsar `BOOKKEEPER_WAL_ONLY` ↔ `BOOKKEEPER_WAL_ASYNC_OBJECT` is easiest because BookKeeper remains primary.
2. Kafka `OBJECT_WAL` ↔ a BookKeeper profile can cut at a Kafka Offset frontier.
3. Pulsar BookKeeper ↔ Object WAL is substantially harder because native ManagedLedger ledger-chain semantics change.

Resolved by [ADR 0015](../decisions/0015-v2-0.2-storage-epoch-runtime-scope.md): 0.2 persists the Storage Epoch chain model
and enforces typed-cut and single-admitting-epoch invariants, but exposes no online transition API/state machine. The
runtime creates one initial epoch per Topic Incarnation; later releases may activate transitions only after accepting
their own contracts.

### `V2-OPEN-MIGRATION-02`: transition state machine

Deferred beyond the 0.2 runtime. The historical proposed states are retained as future design input:

```text
ACTIVE_OLD
PREPARING_NEW_EPOCH
DRAINING_OLD_WRITER
OLD_EPOCH_SEALED
NEW_EPOCH_ACTIVE
MATERIALIZING_HISTORY
RETIRING_OLD_PHYSICAL
COMPLETED
```

A future transition feature still needs exact authority, retry, cancellation, response-loss, crash-cut, rollback, and
operator-visible semantics. This question does not block 0.2.

### `V2-OPEN-MIGRATION-03`: historical data movement

Deferred beyond the 0.2 runtime. Must a future profile transition backfill old Protocol Coverage into the new physical
profile, or may the reader retain a permanent multi-epoch history? If backfill is optional, which cost/latency policies
trigger it and when may the old Physical Extent be retired?

## Pulsar BookKeeper/Object evolution

### `V2-OPEN-BK-01`: resolved Pulsar async Object authority

Resolved by [ADR 0017](../decisions/0017-v2-pulsar-managed-ledger-offload-authority.md): native ManagedLedger
ledger/offload metadata is the sole authority for attempts/completion, read/fallback, and BookKeeper deletion
eligibility. Nereus implements its Object format through a `LedgerOffloader`; a Nereus manifest is derived and cannot
independently authorize native ledger deletion.

The local Pulsar checkout already records an attempt UUID before calling the offloader, marks completion afterward,
opens offloaded reads from ledger metadata, and consults the offload context before BookKeeper deletion. Reusing that
state machine best preserves the “not weaker than native Pulsar” requirement.

### `V2-OPEN-BK-03`: resolved sealed-ledger execution

Resolved by [ADR 0020](../decisions/0020-v2-pulsar-sealed-ledger-async-offload.md). 0.2 offloads sealed, non-current
ManagedLedger ledgers through the ledger-based offloader and excludes active-ledger streaming. Rollover and lag
admission bound cold-copy delay without adding Object latency to BookKeeper ACK.

### `V2-OPEN-BK-04`: resolved sealed-ledger Object layout

Resolved by [ADR 0024](../decisions/0024-v2-pulsar-sealed-ledger-object-layout.md). One native attempt uses exactly one
bounded immutable data Object plus one deterministic sparse-index/root Object. Data publishes before root; offload
success proves both objects, `0..LAC` coverage, integrity, and a ledger-equivalent `ReadHandle`. Both cleanup keys remain
derivable when the root is absent.

### `V2-OPEN-BK-05`: resolved sealed-ledger keys, root v1, and lifecycle order

Resolved by [ADR 0029](../decisions/0029-v2-pulsar-sealed-ledger-root-and-lifecycle.md). Persisted attempt scope and key
version derive both conditional-create keys. A bounded root binds attempt/sealed metadata, data SHA/format, contiguous
index, and self-digest. Publication verifies data, root, and the real read path; cleanup proves root then data absent and
covers attempt-scoped multipart residue.

### `V2-OPEN-BK-06`: sealed-ledger root v1 wire and parser limits

Open. The current recommendation is an independent, big-endian `NPO1` canonical binary with a fixed header, exactly
four ordered typed sections, and a trailing SHA-256 over all preceding canonical bytes. Strict count/string/object
limits would be checked before allocation or index trust; HEAD would first enforce the 8 MiB root limit. The full
proposed limit table remains preserved in the round-5 record pending confirmation.

### `V2-OPEN-BK-07`: Object revalidation before BookKeeper source deletion

Open. The current recommendation adds a narrow final revalidation of the exact attempt/root, data identity and digest,
sealed-ledger facts, and production reader boundaries before setting native `bookkeeperDeleted=true`; the native
metadata CAS would then recheck the same eligible attempt. Failure retains BookKeeper and backs off, while permanent
Object mismatch quarantines the attempt.

### `V2-OPEN-BK-08`: native Object/BookKeeper read fallback

Open. The current recommendation allows at most one whole-range fallback while native metadata says both sources are
eligible: Object-first may fall back for availability/integrity failure, while BookKeeper-first falls back only after
native missing-ledger resolution. One range must come wholly from one source; source deletion makes Object-only final.
The BK read-pin/drain mechanism needed to make this safe remains a descendant.

## Pulsar Object WAL

### `V2-OPEN-PUL-OBJ-01`: resolved virtual ledger identity and chain authority

Resolved by [ADR 0022](../decisions/0022-v2-pulsar-object-wal-virtual-ledger-authority.md). A Pulsar-cell
`PulsarVirtualLedgerStore` owns virtual ledger allocation and an explicit append-only Ledger Chain. Object identity,
byte offsets, and Object-run sequence never become Pulsar positions or chain authority.

### `V2-OPEN-PUL-OBJ-02`: resolved numeric compatibility and namespace enforcement

Resolved by [ADR 0027](../decisions/0027-v2-pulsar-virtual-ledger-numeric-compatibility.md). The deployment reserves
`[2^62, 2^63 - 2]`, excludes native allocation, and assigns non-overlapping never-reused cell slices. Cell allocators are
increasing with gaps and no reuse. Numeric order preserves stock comparison only; explicit predecessor/head metadata
remains Ledger Chain authority.

### `V2-OPEN-PUL-OBJ-03`: resolved deployment reservation registry authority

Resolved by [ADR 0032](../decisions/0032-v2-pulsar-virtual-ledger-reservation-registry.md). One bounded deployment-wide
registry is slice-allocation authority; its canonical assignment table advances through single-key CAS. Per-cell lookup
and watches are derived. Exact capacity, slice lifecycle, allocator, and Ledger Chain protocols remain descendants.

### `V2-OPEN-PUL-OBJ-04`: durable slice owner identity

Open. The current recommendation binds each immutable assignment to deployment ID, reservation-domain ID, protocol,
and immutable Pulsar Protocol Cell ID. Broker/session, display alias, and provider scope remain runtime/admission
attributes so ordinary restart, scale, or provider rotation does not consume another finite slice.

### `V2-OPEN-PUL-OBJ-05`: slice lifecycle and retirement

Open. The current recommendation is the irreversible lifecycle `ACTIVE -> RETIRING -> RETIRED`: retirement stops new
allocation first, then retains the final assignment forever as a never-reuse tombstone. Exhaustion would be derived
from counter/bounds rather than stored as a lifecycle state. The exact proof permitting RETIRED remains a descendant.

### `V2-OPEN-PUL-OBJ-06`: slice geometry and registry lifetime capacity

Open. The current recommendation gives every Cell exactly one immutable, equal-size, aligned `2^k` contiguous slice
inside the reserved numeric domain, with separate hard `maxRegistryBytes` and lifetime `maxAssignmentsEver` caps that
include retired Cells. Exact `k`, resize, and second-slice policy remain downstream choices requiring workload and
support-lifetime evidence.

### `V2-OPEN-PUL-MIGRATION-01`: new incarnation or HybridManagedLedger

The initial proposal is to migrate between Pulsar BookKeeper and Object WAL through a new Topic Incarnation, backfill,
catch-up, and alias/routing cutover. A later alternative is a `HybridManagedLedger` whose Ledger Chain contains both
Object virtual ledgers and BookKeeper ledgers.

The choice is not accepted. It must account for cursor and MessageId stability, partial batch ACK, retention, offload,
recovery, compaction, replication, transactions, and rollback.

## Cross-protocol access and migration

### `V2-OPEN-PROJECTION-SCOPE-01`: resolved 0.2 runtime scope

Resolved by [ADR 0016](../decisions/0016-v2-0.2-cross-protocol-runtime-scope.md): 0.2 retains the domain identities,
invariants, and rejection of a second Native Write Authority, but does not implement Projection Map storage,
secondary-protocol serving, semantic state translation, or authority-transfer runtime.

### `V2-OPEN-PROJECTION-01`: Projection Map granularity

Deferred beyond the 0.2 runtime. Should future Projection Map entries be segment-level coverage mappings, ledger-level
mappings, batch mappings, or a hybrid? The proposal avoids one control-metadata mutation per message, but random seek,
partial batch ACK, and corruption repair must remain bounded.

### `V2-OPEN-PROJECTION-02`: Migration Link state machine

Deferred beyond the 0.2 runtime. The historical proposed Kafka/Pulsar authority-transfer saga is:

```text
SOURCE_ACTIVE
TARGET_PREPARED
BACKFILLING
TAIL_CATCHING_UP
TARGET_CAUGHT_UP
SOURCE_FENCED
TARGET_ACTIVATED
SOURCE_RETIRED
```

Failure and rollback semantics at every cut remain undecided. In particular, no state may permit both source and target
to allocate native positions.

### `V2-OPEN-PROJECTION-03`: semantic transfer contract

A future runtime must decide how to translate:

- Kafka consumer groups and Pulsar subscription cursors;
- Pulsar batch indexes and Kafka record offsets;
- partial batch ACK;
- transactions and visibility;
- compaction tombstones;
- delayed delivery;
- Key_Shared routing;
- schemas, Pulsar properties, and Kafka headers;
- producer deduplication state.

For example, one Pulsar entry with batch indexes `0..2` might map to one Kafka Offset Range of length three. This is an
input example, not an accepted canonical payload mapping.

## Resolved questions

### Restarted Grill 2 round 4 decisions: resolved by ADRs 0028 through 0032

Resolved on 2026-08-09 after explicit confirmation:

- `V2-OPEN-META-03` → [ADR 0028](../decisions/0028-v2-topic-incarnation-keys-and-deterministic-ids.md);
- `V2-OPEN-BK-05` → [ADR 0029](../decisions/0029-v2-pulsar-sealed-ledger-root-and-lifecycle.md);
- `V2-OPEN-OBJ-07` → [ADR 0030](../decisions/0030-v2-object-wal-run-root-and-content-addressed-discovery.md);
- `V2-OPEN-OBJ-08` → [ADR 0031](../decisions/0031-v2-protocol-frame-and-append-commit-set.md);
- `V2-OPEN-PUL-OBJ-03` → [ADR 0032](../decisions/0032-v2-pulsar-virtual-ledger-reservation-registry.md).

Their original recommendations and source rationale remain in
[the round 4 record](grill-notes/06-restarted-grill-2-schema-discovery-and-registry.md).

### Restarted Grill 2 round 3 decisions: resolved by ADRs 0023 through 0027

Resolved on 2026-08-09 after explicit confirmation:

- `V2-OPEN-META-02` → [ADR 0023](../decisions/0023-v2-topic-binding-aggregate-record.md);
- `V2-OPEN-BK-04` → [ADR 0024](../decisions/0024-v2-pulsar-sealed-ledger-object-layout.md);
- `V2-OPEN-OBJ-05` → [ADR 0025](../decisions/0025-v2-initial-checksum-algorithms-and-provider-proof.md);
- `V2-OPEN-OBJ-06` → [ADR 0026](../decisions/0026-v2-protocol-native-frame-payload-bytes.md);
- `V2-OPEN-PUL-OBJ-02` → [ADR 0027](../decisions/0027-v2-pulsar-virtual-ledger-numeric-compatibility.md).

Their original recommendations and source rationale remain in
[the round 3 record](grill-notes/05-restarted-grill-2-physical-proof-and-native-ordering.md).

### Restarted Grill 2 round 2 decisions: resolved by ADRs 0019 through 0022

Resolved on 2026-08-09 after explicit confirmation:

- `V2-OPEN-META-01` → [ADR 0019](../decisions/0019-v2-initial-binding-epoch-atomic-visibility.md);
- `V2-OPEN-BK-03` → [ADR 0020](../decisions/0020-v2-pulsar-sealed-ledger-async-offload.md);
- `V2-OPEN-OBJ-04` → [ADR 0021](../decisions/0021-v2-object-wal-checksum-domains.md);
- `V2-OPEN-PUL-OBJ-01` → [ADR 0022](../decisions/0022-v2-pulsar-object-wal-virtual-ledger-authority.md).

Their original recommendations and source rationale remain in
[the round 2 record](grill-notes/04-restarted-grill-2-initial-authority-and-object-identity.md).

### Restarted Grill 2 decisions: resolved by ADRs 0015 through 0018

Resolved on 2026-08-09 after explicit confirmation:

- `V2-OPEN-MIGRATION-01` → [ADR 0015](../decisions/0015-v2-0.2-storage-epoch-runtime-scope.md);
- `V2-OPEN-PROJECTION-SCOPE-01` → [ADR 0016](../decisions/0016-v2-0.2-cross-protocol-runtime-scope.md);
- `V2-OPEN-BK-01` → [ADR 0017](../decisions/0017-v2-pulsar-managed-ledger-offload-authority.md);
- `V2-OPEN-OBJ-02` → [ADR 0018](../decisions/0018-v2-object-wal-uncertain-put-proof.md).

Their original recommendations and source rationale remain in
[the restarted Grill 2 record](grill-notes/03-restarted-grill-2-scope-and-offload-frontier.md).

### `V2-OPEN-FABRIC-01`: resolved by ADR 0014

Resolved on 2026-08-09. Multiple Protocol Cells may share physical provider infrastructure, compatible transport
capacity, worker processes, and observability. Each cell owns a distinct Cell Provider Scope/session, namespace,
credential/KMS and operator scope, admission/retry/circuit-breaker state, queue/cache accounting, task root, GC
capability, drain, and close lifecycle. Object groups do not cross cells in 0.2. Dedicated provider infrastructure is an
optional stronger deployment topology; an outage of shared physical infrastructure may affect all attached cells.

The normative contract is [ADR 0014](../decisions/0014-v2-provider-sharing-and-protocol-cell-isolation.md). This ID is no
longer an active design gate.
