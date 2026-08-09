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

The user explicitly confirmed all fourteen round-5 recommendations. ADRs 0033 through 0041 now resolve that complete
frontier. The next independent frontier is:

| Gate | Decision needed now | Current recommendation, not a decision |
| --- | --- | --- |
| `V2-OPEN-KAF-META-02` | where the Kafka aggregate record lives and how replay/snapshot/delete own it | use one explicit generated record owned by `TopicImage`, with topic-cascaded deletion and final image validation |
| `V2-OPEN-PUL-META-01` | when a deleted Pulsar incarnation's full aggregate may be retired | keep a permanent generation selector and replace the full aggregate with a compact same-key tombstone only after exact proof |
| `V2-OPEN-BK-09` | which independently verifiable unit NPO1 indexes inside its data Object | use ordered `NPD1` multi-entry blocks with bounded entry directories and per-block integrity/codec boundaries |
| `V2-OPEN-BK-10` | which layer owns dual-source handles and source-specific read pins | use one ManagedLedger-owned composite handle whose BK pin drain precedes native deletion CAS |
| `V2-OPEN-OBJ-15` | which NWG1 key hierarchy, AEAD, nonce, and directory-authentication contract applies | wrap one run key, derive unique object keys, and independently authenticate the directory and every frame |
| `V2-OPEN-OBJ-16` | where immutable WalRun roots live and how seal/successor publication works | store immutable root/seal records in control metadata and advance one exact CAS pointer only after sealing |
| `V2-OPEN-PUL-OBJ-07` | whether a fixed virtual-ledger slice may be resized or extended | forbid resize, relocation, extension, and second slices in 0.2; exhaustion fails closed |

The complete questions and recommendations are in
[round 6](grill-notes/08-restarted-grill-2-runtime-ownership-and-crypto.md). None of these seven recommendations is
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

### `V2-OPEN-META-03`: resolved aggregate incarnation, key, and deterministic IDs

Resolved by [ADR 0028](../decisions/0028-v2-topic-incarnation-keys-and-deterministic-ids.md). Kafka native topic UUID or
Pulsar canonical persistence/name facts plus binding generation form a protocol-discriminated incarnation. Aggregate
keys are incarnation-scoped, values repeat the complete identity, and binding/initial-epoch IDs are separate
domain-separated deterministic SHA-256 derivations with no retry-dependent input.

### `V2-OPEN-META-04`: resolved aggregate logical schema v1

Resolved by [ADR 0033](../decisions/0033-v2-topic-binding-aggregate-logical-schema-v1.md). One closed whole-record
logical schema v1 owns the complete immutable binding plus ordinal-zero epoch, excludes mutable/retry-dependent fields,
and maps both Kafka and Oxia physical records through one validator and shared semantic vectors.

### `V2-OPEN-KAF-META-01`: resolved Kafka V2 feature activation

Resolved by [ADR 0034](../decisions/0034-v2-kafka-feature-level-2-bootstrap-activation.md). V2 uses
`nereus.storage.version=2` only at fresh KRaft format/bootstrap, advertises `[2,2]`, permanently rejects level-1 V1
state, and forbids every runtime upgrade/downgrade.

### `V2-OPEN-KAF-META-02`: Kafka aggregate record and image ownership

Should level-2 Kafka store the aggregate as an opaque attachment, a parallel image, or one generated metadata record
owned by `TopicImage`? The current recommendation is an explicit typed wire-v0 record ordered between `TopicRecord`
and `PartitionRecord`s, topic-cascaded removal with no second delete record, and fatal complete-image validation at
atomic batch or completed-snapshot publication. This is not yet accepted.

### `V2-OPEN-PUL-META-01`: Pulsar aggregate retirement and recreation ABA

Must every full immutable Pulsar aggregate remain forever, or may reference-free deleted incarnations release most of
that metadata? The current recommendation keeps a permanent compact name/generation selector and, only after the exact
incarnation is deleted, unreferenced, drained, and past audit grace, CAS-replaces the full aggregate with a compact
permanent tombstone at the same incarnation key. The key is never absent or reusable, so a late `putIfAbsent` cannot
resurrect it. This is not yet accepted.

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

### `V2-OPEN-OBJ-09`: resolved multi-binding WalRun epoch placement

Resolved by [ADR 0037](../decisions/0037-v2-object-wal-binding-context-epoch-authority.md). The root remains
physical/run authority, while bounded object-local binding contexts carry each frame's exact incarnation, Storage
Epoch, and Owner Epoch, preserving cross-binding PUT amortization.

### `V2-OPEN-OBJ-10`: resolved provider-absent in-flight group after process loss

Resolved by [ADR 0038](../decisions/0038-v2-object-wal-provider-absent-crash-contract.md). 0.2 has no broker-local
ciphertext journal claim. A present object is verified; a proven-absent never-ACKed gap fences the old run and may retry
only through protocol idempotency in a fresh run; unknown presence remains fail-closed.

### `V2-OPEN-OBJ-11`: resolved bounded WalRun lifecycle

Resolved by [ADR 0039](../decisions/0039-v2-bounded-walrun-lifecycle-recovery-and-root-pointer.md). Every run has hard
extent/byte/age/predecessor limits and stops admission, drains/reconciles, seals, and publishes a successor before a
limit can be crossed.

### `V2-OPEN-OBJ-12`: resolved recovery envelope as an admission invariant

Resolved by [ADR 0039](../decisions/0039-v2-bounded-walrun-lifecycle-recovery-and-root-pointer.md). One cumulative
worst-case envelope constrains normal ACK/admission across provider, decode, memory, retry, and time work. Fallback never
resets it; predicted exhaustion backpressures and actual exhaustion fails closed.

### `V2-OPEN-OBJ-13`: resolved current WalRun Root discovery authority

Resolved by [ADR 0039](../decisions/0039-v2-bounded-walrun-lifecycle-recovery-and-root-pointer.md). One low-frequency
per-shard CAS pointer binds exact root key/SHA/run epoch and anchors a bounded predecessor lineage; normal admitted
group append remains free of metadata I/O.

### `V2-OPEN-OBJ-14`: resolved in-object append-unit directory and co-location

Resolved by [ADR 0040](../decisions/0040-v2-nwg1-append-unit-directory-and-colocation.md). NWG1 carries one bounded
authoritative in-body binding-context/append-unit directory, co-locates every Kafka commit set in one ObjectExtent, and
independently compresses/authenticates/checks each frame block.

### `V2-OPEN-OBJ-15`: NWG1 key hierarchy, AEAD, and authenticated directory

Should NWG1 pay a KMS wrap per ObjectExtent, share one raw key, or use a bounded key hierarchy? The current
recommendation wraps one random WalRun key under an immutable Cell KMS version, derives a unique AES-256 object key per
run sequence with HKDF-SHA-256, assigns disjoint fixed nonces to directory and frame ordinals, and authenticates the
range-readable directory plus every independently compressed frame. This is not yet accepted.

### `V2-OPEN-OBJ-16`: WalRun Root home and immutable seal publication

Should a WalRun Root be a provider Object or control-metadata record, and does sealing mutate it? The current
recommendation uses immutable root and seal records in the Cell's control-metadata backend, then creates a successor
bound to both and advances `CurrentWalRunPointer` with one exact CAS. A sealed run is never reopened. This is not yet
accepted.

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

### `V2-OPEN-BK-06`: resolved sealed-ledger root v1 wire and parser limits

Resolved by [ADR 0035](../decisions/0035-v2-pulsar-npo1-sealed-ledger-root-format.md). NPO1 is an independent bounded
big-endian four-section canonical root with strict ordering/UTF-8/duplicate/overflow rules, hard parser limits, and a
root SHA validated before index trust.

### `V2-OPEN-BK-07`: resolved Object revalidation before BookKeeper source deletion

Resolved by [ADR 0036](../decisions/0036-v2-pulsar-native-dual-source-read-and-deletion-safety.md). ManagedLedger
revalidates the exact root/data/read path without holding its metadata mutex, then CAS-rechecks the same eligible attempt
before `bookkeeperDeleted=true`; failure retains BookKeeper and permanent mismatch quarantines Object.

### `V2-OPEN-BK-08`: resolved native Object/BookKeeper read fallback

Resolved by [ADR 0036](../decisions/0036-v2-pulsar-native-dual-source-read-and-deletion-safety.md). Native metadata
permits at most one whole-range, single-source fallback while both sources remain eligible; Object corruption remains a
deletion veto, and `bookkeeperDeleted=true` is permanently Object-only.

### `V2-OPEN-BK-09`: sealed-ledger NPD1 data-block contract

Which physical unit does NPO1's sparse index authorize inside the data Object? The current recommendation is an
ordered, gap-free `NPD1` sequence of independently verifiable multi-entry blocks. Each root row binds an exact block
range and digest; each block has a bounded entry directory, never splits an entry, resets compression/AEAD/integrity,
and gives an oversize entry one dedicated bounded block. This is not yet accepted.

### `V2-OPEN-BK-10`: ManagedLedger dual-source handle and read pins

Which layer owns fallback and prevents BookKeeper deletion from racing admitted reads? The current recommendation is a
ManagedLedger-owned composite handle with lazy Object/BK children and source-specific range pins. Deletion fences new
BK pins, drains existing pins, revalidates Object, and only then CASes native deletion state. This is not yet accepted.

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

### `V2-OPEN-PUL-OBJ-04`: resolved durable slice owner identity

Resolved by [ADR 0041](../decisions/0041-v2-pulsar-virtual-ledger-slice-contract.md). The durable owner is the
deployment/reservation-domain/Pulsar Protocol Cell tuple; broker/session/alias/provider change cannot consume or mutate
the assignment.

### `V2-OPEN-PUL-OBJ-05`: resolved slice lifecycle and retirement

Resolved by [ADR 0041](../decisions/0041-v2-pulsar-virtual-ledger-slice-contract.md). Lifecycle is irreversible
`ACTIVE -> RETIRING -> RETIRED`; only ACTIVE allocates, RETIRED remains a permanent tombstone, and exhaustion is derived
rather than a lifecycle state.

### `V2-OPEN-PUL-OBJ-06`: resolved slice geometry and registry lifetime capacity

Resolved by [ADR 0041](../decisions/0041-v2-pulsar-virtual-ledger-slice-contract.md). Every Cell gets one immutable
equal-size aligned `2^k` slice, while numeric and encoded/lifetime registry caps both include retired Cells. Exact `k`
and expansion policy remain downstream gates.

### `V2-OPEN-PUL-OBJ-07`: virtual-ledger slice expansion policy

May a Cell resize, relocate, extend, or attach another interval after exhausting its fixed slice? The current
recommendation forbids all four in 0.2: exhaustion fails closed, and added capacity requires a new Protocol Cell ID and
new slice plus a future explicit topic-migration contract. Exact `k` and admission must cover the supported lifetime.
This is not yet accepted.

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

### Restarted Grill 2 round 5 decisions: resolved by ADRs 0033 through 0041

Resolved on 2026-08-09 after explicit confirmation:

- `V2-OPEN-META-04` → [ADR 0033](../decisions/0033-v2-topic-binding-aggregate-logical-schema-v1.md);
- `V2-OPEN-KAF-META-01` → [ADR 0034](../decisions/0034-v2-kafka-feature-level-2-bootstrap-activation.md);
- `V2-OPEN-BK-06` → [ADR 0035](../decisions/0035-v2-pulsar-npo1-sealed-ledger-root-format.md);
- `V2-OPEN-BK-07..08` →
  [ADR 0036](../decisions/0036-v2-pulsar-native-dual-source-read-and-deletion-safety.md);
- `V2-OPEN-OBJ-09` → [ADR 0037](../decisions/0037-v2-object-wal-binding-context-epoch-authority.md);
- `V2-OPEN-OBJ-10` → [ADR 0038](../decisions/0038-v2-object-wal-provider-absent-crash-contract.md);
- `V2-OPEN-OBJ-11..13` →
  [ADR 0039](../decisions/0039-v2-bounded-walrun-lifecycle-recovery-and-root-pointer.md);
- `V2-OPEN-OBJ-14` → [ADR 0040](../decisions/0040-v2-nwg1-append-unit-directory-and-colocation.md);
- `V2-OPEN-PUL-OBJ-04..06` →
  [ADR 0041](../decisions/0041-v2-pulsar-virtual-ledger-slice-contract.md).

Their original recommendations and source rationale remain in
[the round 5 record](grill-notes/07-restarted-grill-2-wire-recovery-and-slice-contracts.md).

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
