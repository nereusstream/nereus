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

The user explicitly confirmed the previous four recommendations. ADRs 0015 through 0018 now resolve
`V2-OPEN-MIGRATION-01`, `V2-OPEN-PROJECTION-SCOPE-01`, `V2-OPEN-BK-01`, and `V2-OPEN-OBJ-02`. The next independent
frontier is:

| Gate | Decision needed now | Current recommendation, not a decision |
| --- | --- | --- |
| `V2-OPEN-META-01` | how Topic Protocol Binding and its one initial Storage Epoch become atomically visible | publish one atomic aggregate when the backend supports it; otherwise use an idempotent `CREATING` intent and reject incomplete opens |
| `V2-OPEN-BK-03` | whether Pulsar async offload starts with sealed-ledger or streaming/current-ledger execution | offload sealed non-current ledgers only in 0.2 |
| `V2-OPEN-OBJ-04` | which exact byte domains Object WAL checksums protect | use a canonical provider-request-body digest plus independent decoded-frame payload checksums |
| `V2-OPEN-PUL-OBJ-01` | who allocates Pulsar Object-WAL virtual ledger IDs and orders the ledger chain | use a Pulsar-cell MetadataStore/Oxia virtual-ledger authority; never derive positions from Object keys |

The complete questions and recommendations are in
[the next Grill 2 round](grill-notes/04-restarted-grill-2-initial-authority-and-object-identity.md). None of these four
new recommendations is accepted yet.

## Initial binding and epoch publication

### `V2-OPEN-META-01`: atomic visible create

0.2 creates one immutable Topic Protocol Binding and exactly one initial Storage Epoch. Can a reader/writer ever observe
only one half after create timeout, retry, controller/store failover, or concurrent topic open?

Current recommendation, not a decision: expose binding plus initial epoch as one visible aggregate. KRaft should publish
the records in one replay-atomic controller batch. A MetadataStore/Oxia backend should use one transaction when its
exact API satisfies the conformance contract; otherwise it persists a deterministic `CREATING` intent and idempotently
completes both records before changing the aggregate to `ACTIVE`. Open, append, and read reject or recover an incomplete
aggregate; they never invent a default epoch.

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

### `V2-OPEN-OBJ-04`: checksum byte domains

The uncertain-PUT proof now requires an expected checksum, but encryption, compression, payload decoding, and provider
checksum headers can refer to different byte streams.

Current recommendation, not a decision: use two explicit layers. The Object Extent carries a digest of the canonical
request body presented to the provider after Nereus compression and client-side encryption; a provider checksum proves
durability only when its documented scope matches that byte stream and the immutable version. Each frame separately
carries a checksum over the decoded protocol payload/record bytes. Recovery validates the stored-object layer before
decode, then frame checksums after decode. The two fields use distinct names and cannot substitute for one another.

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

### `V2-OPEN-BK-03`: sealed-ledger or streaming offload

The pinned Pulsar development source exposes both ledger-based `offload(ReadHandle, UUID, ...)` and an evolving
`streamingOffload(...)` API shape. Its ordinary prefix-offload path selects non-current ledgers and completes native
metadata around a ledger offload attempt.

Current recommendation, not a decision: 0.2 offloads only sealed, non-current ManagedLedger ledgers through the
ledger-based offloader. It does not stream the active ledger into Object storage. This keeps offload completion aligned
with an immutable ledger coverage and native fallback/deletion semantics. The cost is cold-copy lag of up to one ledger
rollover; ledger size/age limits and lag admission policy bound that delay.

## Pulsar Object WAL

### `V2-OPEN-PUL-OBJ-01`: virtual ledger identity and chain authority

`OBJECT_WAL` still has to expose stable `PulsarPosition(ledgerId, entryId)` and MessageId without turning Object group
keys, byte offsets, or shared group sequence into protocol positions.

Current recommendation, not a decision: a Pulsar-cell `PulsarVirtualLedgerStore` in MetadataStore/Oxia allocates unique
virtual ledger IDs from an explicitly reserved identity domain and publishes their append-only Ledger Chain order.
Entry IDs are allocated serially inside that virtual ledger; chain order comes from metadata, never numeric ledger-ID
sorting. Object WAL groups remain Physical Extents and may contain frames from multiple bindings in the same cell, so an
object key can never be a ledger ID. Ledger creation/rollover is low-frequency control metadata, not a per-append commit.

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
