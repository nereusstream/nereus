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

The user explicitly confirmed the previous four recommendations. ADRs 0019 through 0022 now resolve
`V2-OPEN-META-01`, `V2-OPEN-BK-03`, `V2-OPEN-OBJ-04`, and `V2-OPEN-PUL-OBJ-01`. The next independent frontier is:

| Gate | Decision needed now | Current recommendation, not a decision |
| --- | --- | --- |
| `V2-OPEN-META-02` | which physical record shape implements the accepted atomic visible Topic Binding Aggregate | store one immutable composite aggregate record; project binding/epoch views from it |
| `V2-OPEN-BK-04` | whether one sealed Pulsar ledger attempt uses one or multiple independently addressable data extents | start with one bounded data extent plus one deterministic sparse-index/root object |
| `V2-OPEN-OBJ-05` | which initial checksum algorithms and provider-bound proof fields are frozen | use SHA-256/v1 for extents, CRC32C/v1 for frames, and a separate full-object/version proof |
| `V2-OPEN-OBJ-06` | which exact Kafka/Pulsar bytes are canonical frame payloads | checksum exact protocol-native serialized batches/entries after Object decode, not reserialized application records |
| `V2-OPEN-PUL-OBJ-02` | how virtual ledger IDs remain compatible with stock numeric MessageId ordering without making it authority | allocate monotonic IDs from an enforced native-excluded band; keep explicit chain metadata authoritative |

The complete questions and recommendations are in
[the next Grill 2 round](grill-notes/05-restarted-grill-2-physical-proof-and-native-ordering.md). None of these five
new recommendations is accepted yet.

## Initial binding and epoch publication

### `V2-OPEN-META-01`: resolved atomic visible create

Resolved by [ADR 0019](../decisions/0019-v2-initial-binding-epoch-atomic-visibility.md). Topic Protocol Binding and its
initial Storage Epoch form one visible `TopicBindingAggregate`. Incomplete or uncertain create is recovered or rejected;
it never admits open, append, or read and never causes a default epoch to be invented.

### `V2-OPEN-META-02`: aggregate physical representation

KRaft can append a bounded atomic controller record batch, while the currently pinned MetadataStore/Oxia public APIs
offer single-key conditional puts rather than an all-conditions-or-no-writes multi-key transaction. The accepted
visible-aggregate contract still permits one composite record, separate replay-atomic records, or an intent/root.

Current recommendation, not a decision: 0.2 stores one immutable `TopicBindingAggregateRecord` that structurally
contains the complete Topic Protocol Binding and initial Storage Epoch. Kafka adds that one record to the existing
atomic `CreateTopics` controller result; MetadataStore/Oxia creates one key with `putIfAbsent` and resolves response
loss by rereading and exact-content comparison. Logical binding and epoch stores project typed views from that record.
Use ADR 0019's `CREATING` flow only if a future schema is forced across separately published keys. This pays for
whole-record schema evolution and CAS, but removes cross-key partial state from the normal 0.2 design.

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

### `V2-OPEN-OBJ-05`: initial algorithms and provider-bound proof

The current S3 adapter computes CRC32C over uploaded bytes but stores it as Nereus user metadata. It does not request a
provider checksum, enable HEAD checksum mode, or retain provider version/checksum-type fields. That metadata echo cannot
prove ADRs 0018/0021. The pinned SDK can request SHA-256 or CRC32C and expose `FULL_OBJECT` versus `COMPOSITE`, version ID,
and provider checksum values.

Current recommendation, not a decision: freeze `ObjectExtentDigest = SHA-256/v1` and
`FramePayloadChecksum = CRC32C/v1`. Persist the expected extent digest in the immutable Object Extent descriptor outside
the request body, and model provider evidence separately as `ProviderObjectProof` with provider version ID, canonical
body length, checksum algorithm, checksum type, and value. A PUT/HEAD fast proof qualifies only for the same immutable
version, exact length, same digest, and `FULL_OBJECT` scope. Providers without that proof use bounded full GET and
SHA-256 recomputation; inability to complete either proof rejects `OBJECT_WAL`. This adds SHA-256 CPU and may add a rare
GET, but keeps content identity collision-resistant and the hot per-frame checksum cheap.

### `V2-OPEN-OBJ-06`: canonical protocol frame bytes

Kafka's native CRC32C covers only the magic-v2 batch region from attributes to batch end; Pulsar's optional native CRC
covers metadata-size, metadata, and a possibly compressed/encrypted payload. Neither value has the exact V2 frame
domain automatically, and decoding to application records/messages would add reserialization and can be impossible for
opaque client-encrypted Pulsar payloads.

Current recommendation, not a decision: in V2, “decoded frame payload” means bytes after the outer Object envelope is
decrypted/decompressed, not decoded application messages. A Kafka frame checksums the exact assigned protocol-native
`MemoryRecords`/complete record-batch byte sequence, including its batch boundaries; a Pulsar frame checksums the exact
ManagedLedger entry byte sequence, preserving native compression, encryption, and batch representation. Native
Kafka/Pulsar checksums are still validated inside their original domains. This avoids canonical re-encoding and covers
all opaque protocol bytes, at the cost of not defining an additional per-application-record checksum.

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

### `V2-OPEN-BK-04`: sealed-ledger Object layout

Native Pulsar treats `(ledgerId, UUID)` as one atomic attempt and requires `offload` success, `readOffloaded`, and
`deleteOffloaded` to behave as one complete ledger. It does not require one provider object; the stock offloader already
uses deterministic data and index objects. Multiple independent data extents are possible, but a failure before the
root exists or a root-first delete can make partial extents undiscoverable unless 0.2 adds another durable attempt
inventory and cleanup state machine.

Current recommendation, not a decision: map one sealed ledger attempt to exactly one bounded immutable data
`ObjectExtent` plus one deterministic immutable sparse-index/root object. Multipart transfer may build the data object,
but it remains one provider object; ledger byte/entry/age rollover bounds its size. Both keys are deterministic and
attempt-scoped, data publishes before the root, and the offload future succeeds only after both objects, contiguous
`0..LAC` coverage, digests, and a ledger-equivalent `ReadHandle` are verified. `deleteOffloaded` can idempotently delete
both known keys even when the root was never published. This gives up independent extent parallelism in 0.2, but avoids
a new partial-attempt inventory and stays close to native Pulsar's proven lifecycle shape.

## Pulsar Object WAL

### `V2-OPEN-PUL-OBJ-01`: resolved virtual ledger identity and chain authority

Resolved by [ADR 0022](../decisions/0022-v2-pulsar-object-wal-virtual-ledger-authority.md). A Pulsar-cell
`PulsarVirtualLedgerStore` owns virtual ledger allocation and an explicit append-only Ledger Chain. Object identity,
byte offsets, and Object-run sequence never become Pulsar positions or chain authority.

### `V2-OPEN-PUL-OBJ-02`: numeric compatibility and namespace enforcement

The pinned Pulsar source stores ManagedLedger ledgers in a numerically sorted map and public `Position`/`MessageIdAdv`
comparison orders ledger ID before entry ID. Native long-ledger allocation can also enter the V1 high-ID range. Thus an
explicit chain alone does not make arbitrary or merely high-bit virtual IDs compatible with stock broker/client
ordering, and a static high-range convention does not prove non-collision.

Current recommendation, not a decision: reserve `[2^62, 2^63 - 2]` for virtual ledgers at the Pulsar deployment
boundary and let one reservation registry assign non-overlapping, never-reused slices to Protocol Cells. Modify and
verify the native ledger-ID generator so it cannot allocate anywhere in that band; fail profile admission when the
deployment or cell reservation is absent, overlapping, drifted, or revoked. A cell-scoped single-key CAS allocator
issues strictly increasing IDs inside its slice, permits gaps, and never reuses IDs. Explicit predecessor/head metadata
remains the Ledger Chain authority; numeric monotonicity is only a compatibility projection for stock broker/client
comparisons. This adds a Pulsar-fork and deployment-reservation obligation, but avoids replacing public MessageId
ordering or accepting native/cross-cell collisions.

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
