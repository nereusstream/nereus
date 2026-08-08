# Pulsar Context

The Pulsar Context owns Pulsar-native ManagedLedger behavior, positions, MessageIds, and cursor semantics while
delegating physical durability and lifecycle to the Shared Storage Context.

## Language

**Pulsar Managed Ledger**:
The Pulsar-native ordered ledger-chain aggregate for one bound Topic Incarnation.
_Avoid_: Stream, Kafka partition

**Pulsar Position**:
The only protocol position truth for a Pulsar Managed Ledger, identified by ledger ID and entry ID within its ledger
chain.
_Avoid_: Kafka offset, ledger-base logical offset, object byte offset

**Pulsar Coverage**:
A ledger-keyed collection of half-open entry ranges within one Topic Protocol Binding.
_Avoid_: Subtractable cross-ledger range, global logical range

**Ledger Chain**:
The protocol-authoritative ordering of real or virtual ledgers belonging to one Pulsar Managed Ledger.
_Avoid_: Numerically sorted ledger IDs, global offset map

**Pulsar Position Domain**:
The Position Domain whose ordering and adjacency rules are proven by the Ledger Chain and Pulsar Position semantics.
_Avoid_: Universal position domain

**Pulsar Native Write Authority**:
The Pulsar broker and ManagedLedger ownership permitted to allocate new Pulsar Positions for a bound Topic Incarnation.
_Avoid_: Object writer authority, Kafka leader authority

**Pulsar Offload Authority**:
For `BOOKKEEPER_WAL_ASYNC_OBJECT`, native ManagedLedger ledger/offload metadata is the sole authority for attempt,
completion, offloaded read/fallback, and BookKeeper deletion eligibility. A Nereus `LedgerOffloader` writes Object
bytes; a Nereus manifest is derived.
_Avoid_: Parallel manifest authority, generic cross-protocol offload state
