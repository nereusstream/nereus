---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NormativeWithOpenGates
sourceTuple: v2-m0
---

# BookKeeper and Pulsar

The profile and ACK boundaries, protocol-native position model, and Pulsar ManagedLedger offload authority are accepted.
Exact Kafka ledger layout remains proposed until the M2 scale spike closes `V2-OPEN-BK-02`. Pulsar 0.2 offload execution
and its one-data-plus-root Object pair are accepted.

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
durable bytes use `ObjectExtent`. A Pulsar-cell `PulsarVirtualLedgerStore` in MetadataStore/Oxia allocates reserved-domain
virtual ledger IDs and publishes explicit append-only Ledger Chain order. Entry IDs are allocated serially in the active
virtual ledger. The deployment excludes `[2^62, 2^63 - 2]` from native allocation and assigns each cell one
non-overlapping, never-reused slice; the cell allocator issues increasing IDs with permitted gaps. Numeric monotonicity
keeps stock MessageId comparison compatible, but explicit predecessor/head metadata remains chain authority. Object
identity, bytes, and group/run sequence never become MessageId truth. Exact rollover and recovery mechanics remain open.

Online Pulsar BookKeeper/Object evolution is not implied by this model. New-incarnation migration versus a future hybrid
ledger-chain design remains `V2-OPEN-PUL-MIGRATION-01`.

## Async Object offload authority

For Kafka `BOOKKEEPER_WAL_ASYNC_OBJECT`, the Nereus manifest joins sealed Kafka Offset Range coverage to the preferred
Object Extent while retaining the BookKeeper Extent as protected fallback.

For Pulsar, native ManagedLedger ledger/offload metadata is the sole authority for attempt identity, completion,
offloaded read selection and fallback, and BookKeeper deletion eligibility. Nereus implements a custom
`LedgerOffloader` that produces the accepted immutable Object format and completes only after its bytes and authoritative
root are durable and readable. A Nereus manifest is a rebuildable derived read/materialization index; it cannot complete
a native offload, overrule fallback, or independently authorize ledger deletion. Disagreement fails closed in favor of
ManagedLedger. ADR 0017 is authoritative.

0.2 calls the ledger-based `offload(ReadHandle, UUID, ...)` only for sealed, non-current ledgers. It does not stream the
current append-admitting ledger to Object storage. Ledger rollover bounds immutable coverage; size/entry/age policy and
lag admission bound cold-copy delay without moving Object into the ACK path. ADR 0020 is authoritative.

One attempt publishes exactly one bounded immutable data Object followed by one deterministic immutable sparse-index/
root Object. Both keys are attempt-scoped and derivable from `(ledgerId, UUID)` inside the Cell Provider Scope. Multipart
transfer may construct the single data Object. Offload completes only after both objects, contiguous `0..LAC` coverage,
digests, and one ledger-equivalent `ReadHandle` are verified. Reads present one sealed ledger; idempotent cleanup derives
both keys even when the root is absent. ADR 0024 is authoritative.

A BookKeeper source becomes physically deletable only after all of these are durable and revalidated:

- the exact sealed Kafka Offset Range or ledger-keyed Pulsar Coverage and checksum were materialized;
- the preferred Object generation was published and is readable;
- native Pulsar offload/ledger metadata recognizes the replacement where applicable;
- typed logical retention passed the whole source coverage;
- no cursor, reader pin, recovery root, task, or source protection references it;
- grace and response-loss reconciliation completed.

Offloader completion creates deletion eligibility; it does not itself bypass the remaining native retention, cursor,
read-pin, deletion-lag, or Nereus source-protection checks.

## Lag policy

Async offload exposes pending ledgers/bytes/age and the oldest unmaterialized typed Protocol Frontier. Policy may alert,
throttle, or stop new admission before BookKeeper capacity is exhausted. It never changes an already admitted append
into a synchronous Object write.

Relevant tradeoffs: `T-BK-01`, `T-LEDGER-01`, `T-PROTOCOL-01`, and `T-POSITION-01`. Required scenarios:
`V2-BK-001..004` and `V2-POSITION-001..003`. See
[ADR 0017](../decisions/0017-v2-pulsar-managed-ledger-offload-authority.md),
[ADR 0020](../decisions/0020-v2-pulsar-sealed-ledger-async-offload.md), and
[ADR 0022](../decisions/0022-v2-pulsar-object-wal-virtual-ledger-authority.md),
[ADR 0024](../decisions/0024-v2-pulsar-sealed-ledger-object-layout.md), and
[ADR 0027](../decisions/0027-v2-pulsar-virtual-ledger-numeric-compatibility.md).
