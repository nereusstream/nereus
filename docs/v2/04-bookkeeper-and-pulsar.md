---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NormativeWithOpenGates
sourceTuple: v2-m0
---

# BookKeeper and Pulsar

The profile and ACK boundaries are accepted. Exact ledger layout, Pulsar offload authority, and native-position mapping
remain proposed until the M2 spikes close `V2-OPEN-BK-01`, `V2-OPEN-BK-02`, and `V2-OPEN-PUL-01`.

## Shared BookKeeper contract

Both BookKeeper profiles:

- acknowledge only after the configured quorum accepts the complete append range;
- fence old writers through ownership epoch plus BookKeeper fencing/recovery semantics;
- keep a sealed ledger readable until its replacement generation and all reader/retention protections are safe;
- treat create/add/seal/delete response loss as uncertain provider outcomes;
- never add Object latency to the ACK boundary.

## Kafka BookKeeper layout

The starting design uses one active ledger per Kafka partition because it makes offset continuity, ownership, seal, and
retention easy to reason about. It is provisional under `T-LEDGER-01`. Before M2 freezes it, scale evidence must cover
10k and 100k partitions, open-handle memory, metadata operations, recovery time, bookie pressure, and rollover rate.
The result may select pooled/striped ledgers only if partition fencing and range retirement remain unambiguous.

## Pulsar native BookKeeper path

For Pulsar BookKeeper profiles, ManagedLedger remains the native append/read/cursor lifecycle. Nereus must not insert a
generic remote-metadata commit between BookKeeper completion and the ManagedLedger result. `(ledgerId, entryId)` stays
the protocol-visible position.

M2 must freeze one of these logical-coordinate contracts:

1. a durable base offset for each ledger, with `logicalOffset = ledgerBase + entryId`; or
2. a lifecycle-only common range index that explicitly does not replace Pulsar Position authority.

The design may not rely on a process-local rollover base.

## Async Object offload authority

For `BOOKKEEPER_WAL_ASYNC_OBJECT`, the preferred direction is a native ManagedLedger offload integration or custom
offloader. Pulsar ledger metadata must continue to authorize cursor, retention, offload fallback, and source deletion.
A Nereus manifest may be a derived read index or an explicitly integrated extension; it cannot independently delete a
ledger that stock ManagedLedger still references.

A BookKeeper source becomes physically deletable only after all of these are durable and revalidated:

- the exact sealed ledger/range and checksum were materialized;
- the preferred Object generation was published and is readable;
- native Pulsar offload/ledger metadata recognizes the replacement where applicable;
- logical retention passed the whole source range;
- no cursor, reader pin, recovery root, task, or source protection references it;
- grace and response-loss reconciliation completed.

## Lag policy

Async offload exposes pending ledgers/bytes/age and oldest unmaterialized offset. Policy may alert, throttle, or stop new
admission before BookKeeper capacity is exhausted. It never changes an already admitted append into a synchronous
Object write.

Relevant tradeoffs: `T-BK-01`, `T-LEDGER-01`, and `T-PROTOCOL-01`. Required scenarios: `V2-BK-001`,
`V2-BK-002`, and `V2-BK-003`.
