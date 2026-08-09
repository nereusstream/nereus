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

**Pulsar Virtual Ledger Store**:
The Pulsar-cell MetadataStore/Oxia authority that allocates reserved-domain virtual ledger IDs and publishes explicit
append-only Ledger Chain order for Object WAL. Object keys and WalRun/group sequence never allocate or order positions.
_Avoid_: Object-derived ledger ID, numeric ledger-ID ordering

**Virtual Ledger Reservation**:
The deployment authority that excludes one high signed-long domain from native allocation and assigns non-overlapping,
never-reused slices to Pulsar Protocol Cells.
_Avoid_: High-bit convention, reusable cell range, allocator-local assumption

**Virtual Ledger Namespace Registry**:
The one bounded deployment-wide CAS record that canonically owns every virtual-ledger slice assignment and proves
global non-overlap/non-reuse. Per-cell lookup and watches are derived only.
_Avoid_: Independent authoritative slice key, watch authority, locally merged assignment table

**Virtual Ledger Slice Assignment**:
One immutable aligned `2^k` interval owned by a durable Pulsar Protocol Cell identity. Lifecycle is
ACTIVE→RETIRING→RETIRED; retired assignments and bounds remain permanent never-reuse evidence.
_Avoid_: Broker-owned slice, provider-owned slice, deleted tombstone, resized bounds

**Pulsar Position Domain**:
The Position Domain whose ordering and adjacency rules are proven by the Ledger Chain and Pulsar Position semantics.
_Avoid_: Universal position domain

**Pulsar Native Write Authority**:
The Pulsar broker and ManagedLedger ownership permitted to allocate new Pulsar Positions for a bound Topic Incarnation.
_Avoid_: Object writer authority, Kafka leader authority

**Pulsar Offload Authority**:
For `BOOKKEEPER_WAL_ASYNC_OBJECT`, native ManagedLedger ledger/offload metadata is the sole authority for attempt,
completion, offloaded read/fallback, and BookKeeper deletion eligibility. A Nereus `LedgerOffloader` writes Object
bytes from sealed non-current ledgers only in 0.2; a Nereus manifest is derived.
_Avoid_: Parallel manifest authority, generic cross-protocol offload state

**Sealed-Ledger Object Pair**:
The one bounded data Object and one sparse-index/root Object that together represent a native sealed-ledger offload
attempt. Both identities are deterministic and attempt-scoped.
_Avoid_: Independent child extents, manifest-authorized offload completion

**Sealed-Ledger Root**:
The bounded canonical NPO1 root that binds one offload attempt to sanitized closed-ledger metadata, data length/SHA-256,
outer format, contiguous sparse index, and its own integrity domain under fixed parser limits.
_Avoid_: Stock index assumed sufficient, current-config key derivation, unbounded offsets

**Native Dual-Source Read**:
The ManagedLedger-owned whole-range selection/fallback between an eligible Object attempt and BookKeeper source. One
range uses one source, fallback occurs at most once, and `bookkeeperDeleted=true` is Object-only.
_Avoid_: Mixed-source range, fallback loop, reading physical BookKeeper residue after native deletion

**Pulsar Frame**:
The exact bytes of one ManagedLedger entry and one `(ledgerId, entryId)`. Client batching, compression, encryption, and
transaction markers remain within their native entry boundary.
_Avoid_: Individual batched message, publish request, transaction
