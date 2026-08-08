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

## Storage Epoch transitions

### `V2-OPEN-MIGRATION-01`: initial transition matrix

Which profile transitions are implemented in 0.2, and which remain domain-model capability only?

Current proposal, not a decision:

1. Pulsar `BOOKKEEPER_WAL_ONLY` ↔ `BOOKKEEPER_WAL_ASYNC_OBJECT` is easiest because BookKeeper remains primary.
2. Kafka `OBJECT_WAL` ↔ a BookKeeper profile can cut at a Kafka Offset frontier.
3. Pulsar BookKeeper ↔ Object WAL is substantially harder because native ManagedLedger ledger-chain semantics change.

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

### `V2-OPEN-PUL-MIGRATION-01`: new incarnation or HybridManagedLedger

The initial proposal is to migrate between Pulsar BookKeeper and Object WAL through a new Topic Incarnation, backfill,
catch-up, and alias/routing cutover. A later alternative is a `HybridManagedLedger` whose Ledger Chain contains both
Object virtual ledgers and BookKeeper ledgers.

The choice is not accepted. It must account for cursor and MessageId stability, partial batch ACK, retention, offload,
recovery, compaction, replication, transactions, and rollback.

## Cross-protocol access and migration

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
