# ADR 0022: V2 Pulsar Object WAL virtual-ledger authority

## Status

Accepted for Pulsar `OBJECT_WAL`. Implementation and runtime evidence are not started at M0.

## Context

Pulsar clients, cursors, replication, transactions, and broker behavior require stable
`PulsarPosition(ledgerId, entryId)`/MessageId semantics even when primary bytes never enter BookKeeper. Object WAL group
keys, byte offsets, and group sequence are physical placement facts; one group may contain frames from multiple bindings
inside one Protocol Cell. Deriving protocol positions from those facts would violate ADR 0011 and couple MessageId truth
to Object batching.

## Decision

Each Pulsar Protocol Cell owns a `PulsarVirtualLedgerStore` in its MetadataStore/Oxia authority:

- It allocates unique 64-bit virtual ledger IDs from an explicitly reserved identity domain that cannot collide with
  BookKeeper/native or another cell's ledger IDs under the configured deployment contract.
- It publishes the append-only Ledger Chain order for each Topic Protocol Binding/Incarnation. Chain order comes from
  authoritative metadata links/version, never from numeric ledger-ID sorting.
- The protocol-native serialized writer lane allocates monotonically increasing entry IDs inside the current virtual
  ledger. Normal admitted append performs no remote metadata read or mutation.
- Virtual-ledger create, seal, and rollover are low-frequency fenced control-plane operations. A new ledger becomes
  append-admitting only after its identity and predecessor link are durably published; at most one virtual ledger admits
  entries for a binding at a time.
- Object WAL frames carry the resulting ledger-keyed `PulsarCoverage`. Object keys, Object Extents, WalRun/shard
  sequence, and byte offsets never become ledger IDs, entry IDs, or Ledger Chain order.
- Recovery obtains chain/ledger identity from `PulsarVirtualLedgerStore` and proves acknowledged entry coverage from the
  primary Object WAL. It cannot repair a missing protocol ledger by inventing an ID from physical data.

The exact rollover/seal state machine, uncertain publication recovery, and interactions with cursor trim, compaction,
replication, and transaction recovery remain downstream gates.

ADRs 0027, 0032, and 0041 refine allocation to one deployment-wide bounded reservation registry. Each immutable Pulsar
Protocol Cell identity owns one aligned equal `2^k` slice and follows `ACTIVE -> RETIRING -> RETIRED`; retired bounds
remain permanent never-reuse evidence. Exact expansion policy, allocator/chain epochs, and retirement proof remain
downstream gates.

## Consequences

- `V2-OPEN-PUL-OBJ-01` is resolved.
- Pulsar Object mode preserves native position shape without paying BookKeeper merely to allocate ledger IDs.
- V2 owns a protocol-specific virtual-ledger allocator and chain store, including namespace reservation and recovery.
- M1/M3 must prove identity uniqueness, explicit chain ordering, current-ledger fencing, zero normal-append metadata I/O,
  rollover/restart recovery, and rejection of Object-derived or numerically sorted positions.

Numeric compatibility and reservation enforcement are refined by
[ADR 0027](0027-v2-pulsar-virtual-ledger-numeric-compatibility.md), and registry authority by
[ADR 0032](0032-v2-pulsar-virtual-ledger-reservation-registry.md) plus
[ADR 0041](0041-v2-pulsar-virtual-ledger-slice-contract.md). This decision refines ADR 0011 and is tracked by
`T-POSITION-01`, `V2-POSITION-001..007`.
