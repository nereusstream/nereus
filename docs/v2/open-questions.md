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

The user explicitly confirmed all five round-3 recommendations. ADRs 0023 through 0027 now resolve
`V2-OPEN-META-02`, `V2-OPEN-BK-04`, `V2-OPEN-OBJ-05`, `V2-OPEN-OBJ-06`, and `V2-OPEN-PUL-OBJ-02`. The next
independent frontier is:

| Gate | Decision needed now | Current recommendation, not a decision |
| --- | --- | --- |
| `V2-OPEN-META-03` | which protocol-native incarnation, authority key, and deterministic IDs identify one aggregate | use Kafka topic UUID or Pulsar persistence name + generation, then domain-separated deterministic IDs |
| `V2-OPEN-BK-05` | which exact keys, root-v1 facts, publication verification, and delete order make the two-object attempt ledger-equivalent | use attempt-derived conditional keys and a bounded self-checking root; verify data then root/read, delete root then data |
| `V2-OPEN-OBJ-07` | where each group's expected length/SHA lives and how a crashed owner discovers every ACKed extent without per-group metadata | use one pre-open WalRun root plus scoped seq/length/SHA leaf keys and bounded strong prefix LIST |
| `V2-OPEN-OBJ-08` | what one Kafka/Pulsar frame is and which multi-frame append unit is atomic | one assigned Kafka RecordBatch per frame plus an all-or-none partition commit set; one Pulsar entry per frame |
| `V2-OPEN-PUL-OBJ-03` | which physical registry record atomically proves deployment-wide virtual-ledger slice non-overlap | use one bounded canonical deployment registry updated by single-key CAS; derive per-cell lookup indexes |

The complete questions and recommendations are in
[round 4](grill-notes/06-restarted-grill-2-schema-discovery-and-registry.md). None of these five new recommendations is
accepted yet.

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

### `V2-OPEN-META-03`: aggregate incarnation, key, and deterministic IDs

Open. The current recommendation is to use Kafka's native topic UUID as the Kafka incarnation/ABA fence and Pulsar's
canonical persistence name plus binding generation as the Pulsar incarnation. Aggregate keys would be scoped by that
typed incarnation, while binding and initial-epoch IDs would be domain-separated deterministic hashes rather than
random or time/backend-dependent values. Exact schema/version, delete/recreate retirement, and replay rules depend on
this choice and are intentionally deferred.

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

### `V2-OPEN-OBJ-07`: Object WAL group identity and crash discovery

Open. The current recommendation is to persist one immutable WalRun root before opening append, encode fixed-width
sequence plus exact body length and SHA-256 in every scoped conditional-create leaf key, and discover ACKed extents by
a strongly consistent, globally bounded prefix LIST. Periodic descriptor pages may accelerate recovery but would not be
the ACKed tail authority. This deliberately trades recovery LIST work and a narrower provider admission set for no
per-group metadata-service commit in the cost-first ACK path.

### `V2-OPEN-OBJ-08`: protocol frame and append commit-set granularity

Open. The current recommendation is one complete broker-assigned Kafka RecordBatch per frame, with all frames decoded
from one partition `MemoryRecords` storage append forming an all-or-none `KafkaAppendCommitSet`. One exact Pulsar
ManagedLedger entry would be one frame and one commit set. ObjectExtent groups, network requests, transactions, and
individual Pulsar batched messages would not redefine protocol append atomicity.

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

### `V2-OPEN-BK-05`: sealed-ledger keys, root v1, and lifecycle order

Open. The current recommendation is to derive conditional-create data/root keys only from persisted provider scope,
ledger ID, attempt UUID, and key-derivation version. A bounded canonical root would bind the attempt, sanitized sealed
ledger metadata, data length/SHA-256, outer format, contiguous sparse index, and an independent root self-digest.
Publication would verify data, then root, then the real offloaded read path before success; deletion would prove root
absent before deleting data and would cover attempt-scoped multipart residue.

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

### `V2-OPEN-PUL-OBJ-03`: deployment reservation registry authority

Open. The current recommendation is one bounded deployment-wide
`PulsarVirtualLedgerNamespaceRegistryRecord`, whose canonically sorted assignment table is updated by single-key CAS.
Per-cell lookup records would be repairable derived indexes, not allocation authority. This avoids relying on a
multi-key transaction that the pinned MetadataStore/Oxia surface does not expose; exact capacity, slice lifecycle,
allocator, and Ledger Chain protocols depend on this root choice.

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
