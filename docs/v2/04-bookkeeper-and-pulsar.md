---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NormativeWithOpenGates
sourceTuple: v2-m0
---

# BookKeeper and Pulsar

The profile and ACK boundaries and protocol-native position model are accepted. Exact Kafka ledger layout and Pulsar
offload authority remain proposed until the M2 spikes close `V2-OPEN-BK-01` and `V2-OPEN-BK-02`.

## Shared BookKeeper contract

Both BookKeeper profiles:

- acknowledge only after the configured quorum accepts the complete typed Protocol Coverage;
- fence old writers through Owner Epoch plus BookKeeper fencing/recovery semantics;
- keep a sealed ledger readable until its replacement generation and all reader/retention protections are safe;
- treat create/add/seal/delete response loss as uncertain provider outcomes;
- never add Object latency to the ACK boundary.

BookKeeper ledger/entry coordinates always identify a `BookKeeperExtent`. They become protocol positions only on the
native Pulsar BookKeeper path; Kafka retains Kafka Offsets regardless of its physical ledger placement.

## Kafka BookKeeper layout

The starting design uses one active ledger per Kafka partition because it makes Kafka Offset continuity, ownership, seal,
and retention easy to reason about. Each append joins a binding-scoped `KafkaOffsetRange` to a
`BookKeeperExtent`; entry IDs are not exposed as Kafka Offsets. The layout is provisional under `T-LEDGER-01`.
Before M2 freezes it, scale evidence must cover 10k and 100k partitions, open-handle memory, metadata operations,
recovery time, bookie pressure, and rollover rate. The result may select pooled/striped ledgers only if partition
fencing, typed coverage, and extent retirement remain unambiguous.

## Pulsar native BookKeeper path

For Pulsar BookKeeper profiles, ManagedLedger remains the native append/read/cursor lifecycle. Nereus must not insert a
generic remote-metadata commit between BookKeeper completion and the ManagedLedger result. `(ledgerId, entryId)` stays
the protocol-visible position and MessageId truth.

A `PulsarCoverage` is a ledger-keyed collection of half-open entry ranges. When it crosses ledgers, the authoritative
ManagedLedger Ledger Chain proves ordering and adjacency. V2 does not persist a durable ledger base or compute
`logicalOffset = ledgerBase + entryId`; it also does not order ledgers by numeric ledger ID alone.

For Pulsar `OBJECT_WAL`, the same Pulsar Position Domain and ledger-chain rules describe `PulsarCoverage`, while
durable bytes use `ObjectExtent`. Exact ObjectManagedLedger encoding belongs to its implementation milestone; it may not
replace MessageId truth with object keys or byte offsets.

Online Pulsar BookKeeper/Object evolution is not implied by this model. New-incarnation migration versus a future hybrid
ledger-chain design remains `V2-OPEN-PUL-MIGRATION-01`.

## Async Object offload authority

For Kafka `BOOKKEEPER_WAL_ASYNC_OBJECT`, the Nereus manifest joins sealed Kafka Offset Range coverage to the preferred
Object Extent while retaining the BookKeeper Extent as protected fallback. For Pulsar, the preferred direction is a
native ManagedLedger offload integration or custom offloader. Pulsar ledger metadata must continue to authorize cursor,
retention, offload fallback, and source deletion. A Nereus manifest may be a derived read index or an explicitly
integrated extension; it cannot independently delete a ledger that stock ManagedLedger still references.

A BookKeeper source becomes physically deletable only after all of these are durable and revalidated:

- the exact sealed Kafka Offset Range or ledger-keyed Pulsar Coverage and checksum were materialized;
- the preferred Object generation was published and is readable;
- native Pulsar offload/ledger metadata recognizes the replacement where applicable;
- typed logical retention passed the whole source coverage;
- no cursor, reader pin, recovery root, task, or source protection references it;
- grace and response-loss reconciliation completed.

## Lag policy

Async offload exposes pending ledgers/bytes/age and the oldest unmaterialized typed Protocol Frontier. Policy may alert,
throttle, or stop new admission before BookKeeper capacity is exhausted. It never changes an already admitted append
into a synchronous Object write.

Relevant tradeoffs: `T-BK-01`, `T-LEDGER-01`, `T-PROTOCOL-01`, and `T-POSITION-01`. Required scenarios:
`V2-BK-001`, `V2-BK-002`, `V2-BK-003`, and `V2-POSITION-001`.
