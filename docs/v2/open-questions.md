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

## Restarted Grill 2 frontier

ADR 0014 resolved the earlier Provider-sharing question. After that document update, the current independent decision
frontier is:

| Gate | Decision needed now | Current recommendation, not a decision |
| --- | --- | --- |
| `V2-OPEN-MIGRATION-01` | how much online Storage Epoch transition runtime belongs in 0.2 | keep the chain/invariants, but ship no online transition API/state machine in 0.2 |
| `V2-OPEN-PROJECTION-SCOPE-01` | whether Kafka/Pulsar projection or authority migration runtime belongs in 0.2 | retain only the model and dual-authority rejection in 0.2 |
| `V2-OPEN-BK-01` | who authorizes Pulsar BookKeeper-to-Object offload, fallback, and deletion | native ManagedLedger metadata plus a Nereus LedgerOffloader |
| `V2-OPEN-OBJ-02` | how Object WAL resolves a lost PUT response | capability-tiered HEAD proof or bounded GET; never ETag alone |

The complete rationale and source observations are in
[the restarted Grill 2 record](grill-notes/03-restarted-grill-2-scope-and-offload-frontier.md). All four answers still
require explicit confirmation.

## Object WAL durability verification

### `V2-OPEN-OBJ-02`: PUT-response-loss proof

Which provider proof is sufficient when the immutable Object WAL PUT may have succeeded but the caller lost the
response?

Current recommendation, not a decision:

1. use HEAD only when it returns exact length plus a trustworthy content checksum bound to the immutable object
   identity/version;
2. otherwise perform a bounded GET and recompute the expected checksum;
3. never treat ETag alone as content identity;
4. do not admit a provider to `OBJECT_WAL` when deterministic immutable create, the required read-after-write
   behavior, or bounded verification cannot be established.

This closes a design contract only. M3 still needs real-provider response-loss and checksum-drift evidence.

## Storage Epoch transitions

### `V2-OPEN-MIGRATION-01`: initial transition matrix

Which profile transitions are implemented in 0.2, and which remain domain-model capability only?

Earlier transition ordering proposal, retained as input rather than a decision:

1. Pulsar `BOOKKEEPER_WAL_ONLY` ↔ `BOOKKEEPER_WAL_ASYNC_OBJECT` is easiest because BookKeeper remains primary.
2. Kafka `OBJECT_WAL` ↔ a BookKeeper profile can cut at a Kafka Offset frontier.
3. Pulsar BookKeeper ↔ Object WAL is substantially harder because native ManagedLedger ledger-chain semantics change.

Restarted Grill 2 recommendation, not a decision: 0.2 persists the Storage Epoch chain model and enforces typed-cut and
single-admitting-epoch invariants, but exposes no online transition API/state machine. The runtime creates one initial
epoch per Topic Incarnation; later releases may activate transitions without changing the durable model.

### `V2-OPEN-MIGRATION-02`: transition state machine

The proposed states are:

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

The design still needs exact authority, retry, cancellation, response-loss, crash-cut, rollback, and operator-visible
semantics for every transition. In particular, it must define what happens after the old epoch is sealed but activation
of the new epoch is uncertain.

### `V2-OPEN-MIGRATION-03`: historical data movement

Must a profile transition backfill old Protocol Coverage into the new physical profile, or may the reader retain a
permanent multi-epoch history? If backfill is optional, which cost/latency policies trigger it and when may the old
Physical Extent be retired?

## Pulsar BookKeeper/Object evolution

### `V2-OPEN-BK-01`: Pulsar async Object authority

For Pulsar `BOOKKEEPER_WAL_ASYNC_OBJECT`, which authority records offload attempts/completion, serves fallback, and
permits BookKeeper deletion?

Current recommendation, not a decision: retain native ManagedLedger ledger/offload metadata as authority and implement
the Nereus Object format through a `LedgerOffloader`. A Nereus manifest may remain a derived read/materialization index
but cannot independently authorize native ledger deletion.

The local Pulsar checkout already records an attempt UUID before calling the offloader, marks completion afterward,
opens offloaded reads from ledger metadata, and consults the offload context before BookKeeper deletion. Reusing that
state machine best preserves the “not weaker than native Pulsar” requirement. Initial sealed-ledger versus streaming
offload mechanics depend on this authority decision and belong to a later frontier.

### `V2-OPEN-PUL-MIGRATION-01`: new incarnation or HybridManagedLedger

The initial proposal is to migrate between Pulsar BookKeeper and Object WAL through a new Topic Incarnation, backfill,
catch-up, and alias/routing cutover. A later alternative is a `HybridManagedLedger` whose Ledger Chain contains both
Object virtual ledgers and BookKeeper ledgers.

The choice is not accepted. It must account for cursor and MessageId stability, partial batch ACK, retention, offload,
recovery, compaction, replication, transactions, and rollback.

## Cross-protocol access and migration

### `V2-OPEN-PROJECTION-SCOPE-01`: 0.2 runtime scope

Should 0.2 deliver Kafka/Pulsar Access Projection or Migration Link runtime, or only retain their domain boundary and
reject dual Native Write Authorities?

Current recommendation, not a decision: 0.2 retains the types, invariants, and scenario-level rejection of a second
Native Write Authority, but does not implement Projection Map storage, secondary-protocol serving, semantic state
translation, or authority-transfer runtime. This keeps 0.2 focused on beating the Kafka and native Pulsar baselines.

If runtime delivery is deferred, `V2-OPEN-PROJECTION-01..03` stay documented but do not block the 0.2 release. If it is
selected, all three become release-blocking design gates.

### `V2-OPEN-PROJECTION-01`: Projection Map granularity

Should Projection Map entries be segment-level coverage mappings, ledger-level mappings, batch mappings, or a hybrid?
The proposal avoids one control-metadata mutation per message, but random seek, partial batch ACK, and corruption repair
must remain bounded.

### `V2-OPEN-PROJECTION-02`: Migration Link state machine

The proposed Kafka/Pulsar authority-transfer saga is:

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

The mapping must decide how to translate:

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

### `V2-OPEN-FABRIC-01`: resolved by ADR 0014

Resolved on 2026-08-09. Multiple Protocol Cells may share physical provider infrastructure, compatible transport
capacity, worker processes, and observability. Each cell owns a distinct Cell Provider Scope/session, namespace,
credential/KMS and operator scope, admission/retry/circuit-breaker state, queue/cache accounting, task root, GC
capability, drain, and close lifecycle. Object groups do not cross cells in 0.2. Dedicated provider infrastructure is an
optional stronger deployment topology; an outage of shared physical infrastructure may affect all attached cells.

The normative contract is [ADR 0014](../decisions/0014-v2-provider-sharing-and-protocol-cell-isolation.md). This ID is no
longer an active design gate.
