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
The compatibility-namespace authority that excludes one high signed-long domain from every admitted ledger-ID writer
and assigns non-overlapping, never-reused slices to Pulsar Protocol Cells.
_Avoid_: High-bit convention, reusable cell range, allocator-local assumption

**Virtual Ledger Namespace Registry**:
The exactly one bounded CAS authority selected while V2 allocation is admitted in an immutable
`ledgerIdCompatibilityNamespaceId` derived as
`SHA-256(NLI1 || u32be(36) || canonicalInstanceIdAscii[36])`. M1 admits only a root proven
absent immediately before init; format or an ID change is not freshness proof. The Registry binds one bounded inline
writer commitment and admission interlock, canonically owns every slice assignment, and proves global non-overlap/non-
reuse. It uses `k=40`, at most 65,536 canonical bytes, 256 lifetime assignments, and 192 bytes per assignment row. Per-
cell allocator state is a namespace-bound versioned derived view. Two closed writer kinds use one canonical 120-byte
row per independently revocable cohort and immutable proof-only admission evidence; there is no random row ID or
generic writer kind. Writer count remains an evidence-derived OPEN cap. Ownership-watch continuity is one local store-
wide epoch; the client reuses Oxia v0.9's no-offset dummy notification as its ready barrier and persists no provider
connection/session/shard identity.
_Avoid_: Deployment-name namespace, omitted-writer digest, second Registry, independent authoritative slice key,
external writer-set snapshot, source-SHA writer identity, format-as-cleanup, watch authority, locally merged assignment
table, per-rollover Registry read

**Virtual Ledger Slice Assignment**:
One immutable aligned `2^k` interval owned by a durable Pulsar Protocol Cell identity. Lifecycle is
ACTIVE→RETIRING→RETIRED; retired assignments and bounds remain permanent never-reuse evidence. 0.2 never changes its
bounds or attaches another slice and fails closed at exhaustion.
_Avoid_: Broker-owned slice, provider-owned slice, deleted tombstone, resized bounds, second slice

**Virtual Ledger Allocator Evidence**:
The ADR-0055 source-qualified comparison that measures maximum sustainable rollover RPS while every predeclared SLO
holds across real distribution/jitter/storm/crash cuts, mass broker takeover, and a native Pulsar
rollover/append-stall baseline. It selects neither STRICT_SERIALIZED nor RANGE_LEASED by itself.
_Avoid_: Active-ledger-count-only benchmark, serialized-p99-capacity metric, owner-change range reacquisition storm,
host-selected allocator mode

**ManagedLedger Range Grant**:
A RANGE-candidate reservation permanently bound to one ManagedLedger incarnation and grant ID rather than a broker
owner. Takeover changes only head owner epoch, may finish the same RESERVED grant when allocation state is unchanged,
and burns at most one stale-owner candidate ID; it does not select RANGE_LEASED as the 0.2 allocator.
_Avoid_: Owner-scoped range, takeover tail burn, unknown-response fence, installed-range use blocked by allocator clear

**Pulsar Topic Generation Selector**:
The permanent name-scoped monotonic generation authority using exact
RESERVED→ACTIVE→DELETING→DELETED CAS transitions to fence create/delete/recreate. ACTIVE plus aggregate identity is
cached only after authoritative ownership witness A/B and stale-install-safe sequence CAS. The first M1 witness
candidate is limited to Oxia 0.9.0-backed MetadataStore ELM, but pinned source supplies only direct GET/Stat/CAS
primitives; M1 still adds and proves acquisition transitions, an all-writer closed kernel, and the provider lifecycle/
gap hook. Normal data access captures and rechecks one atomic local fence word.
_Avoid_: Name-only aggregate key, selector deletion, generation rollback, endpoint/bool/TableView witness, best-effort
watch as authority, current-primitives-as-complete-adapter, tearable generation plus valid bits

**Retired Topic Incarnation Tombstone**:
The compact permanent same-key replacement for an exactly reference-free full aggregate, binding its incarnation,
original aggregate digest, and retirement-proof digest.
_Avoid_: Deleting the incarnation key, reusable tombstone, compaction before reference-free proof

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

**Sealed-Ledger Data Block**:
One independently decodable NPD1 multi-entry block whose NPO1 row binds its contiguous entry range, location,
codec/encryption facts, and encoded SHA-256. Its checked 16-byte directory rows derive entry IDs from authenticated
first-entry plus ordinal; upload/digest/full verification are streaming or bounded-segment operations.
_Avoid_: Padded scan-forward block, repeated entry ID, data-Object-sized ByteBuffer, entry split, cross-block state

**Native Dual-Source Read**:
The ManagedLedger-owned whole-range selection/fallback between an eligible Object attempt and BookKeeper source. One
cached composite handle owns both lazy children and source pins; one range uses one source, fallback occurs at most
once, and `bookkeeperDeleted=true` is the Object-only compatibility fence for BK_DELETE_INTENT/DONE after BK-pin
drain. Only BK_DELETE_DONE proves physical deletion or absence.
_Avoid_: Mixed-source range, fallback loop, reading physical BookKeeper residue after native deletion

**BookKeeper Retention Class**:
The persisted offload-attempt policy `RETAIN_BK` or `DELETE_AFTER_VERIFIED`. The latter mandates pin drain, Object
revalidation, BK_DELETE_INTENT, physical absence proof, and BK_DELETE_DONE; after INTENT it cannot revert.
_Avoid_: Boolean deletion policy, host-local delete mode, skipping verification

**Pulsar Frame**:
The exact bytes of one ManagedLedger entry and one `(ledgerId, entryId)`. Client batching, compression, encryption, and
transaction markers remain within their native entry boundary.
_Avoid_: Individual batched message, publish request, transaction
