---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: Normative
sourceTuple: v2-m0
---

# Manifest, read, retention, and GC

## Immutable physical descriptors

Every physical source or materialized output has an immutable descriptor containing:

- Protocol Cell, Cell Provider Scope, Topic Protocol Binding, Topic Incarnation, and Storage Epoch identity;
- typed Protocol Coverage and Position Domain version;
- source kind and epoch-scoped profile;
- an `ObjectExtent` or `BookKeeperExtent`;
- generation, format, payload mapping, and policy version;
- canonical Object-request-body length plus SHA-256/v1 Object Extent Digest, typed Provider Object Proof where
  available, protocol entry/record count, min/max timestamp, and CRC32C/v1 frame descriptors where applicable;
- index descriptors required for protocol-position and timestamp lookup;
- creation Owner Epoch/task identity.

A mutable record never changes the meaning of an immutable object or ledger range.

## Manifest authority

One Topic Protocol Binding manifest root selects the typed logical read view across its Storage Epoch chain. It
references immutable descriptors and advances by fenced compare-and-set. A publication may add a preferred generation
only when:

- the output covers exactly durable, binding-scoped Protocol Coverage;
- all bytes and indexes are validated;
- the source set, task identity, policy, and output format still match;
- ownership/worker fencing is current;
- an equal or higher generation has not already won.

Publication is idempotent. Duplicate workers converge on the same deterministic task/output identity or cancel as stale.
Worker processes or executors may be shared, but queue budgets, task roots, fencing, admission, and publication
authority are cell-scoped. A stalled or stale task in one cell cannot consume another cell's reserved share or publish
against its manifest.

## Logical view and physical overlap

The correctness invariant is one unambiguous binding-scoped logical read view, not physical non-overlap. During
publication and grace, primary WAL, source segments, and a materialized generation may cover the same Protocol Coverage.

The resolver first selects the Storage Epoch interval through the binding's Position Domain, then uses this order:

1. current active tail for unsealed acknowledged Protocol Coverage in the active epoch;
2. manifest-selected preferred generation for sealed Protocol Coverage;
3. exact source generation/Physical Extent as fallback while source protection remains valid;
4. fail closed when neither the selected generation nor a permitted source can prove the requested bytes.

For Pulsar Object WAL, ledger/entry lookup first resolves the explicit virtual Ledger Chain from Pulsar authority, then
maps ledger-keyed Pulsar Coverage to Object Extents. It never derives Ledger Chain order from a manifest, Object key, or
numeric ledger-ID order.

For Pulsar sealed-ledger async offload, native ManagedLedger metadata alone selects eligible sources. While Object and
BookKeeper are both authorized, one inclusive range may perform at most one whole-range fallback and must return every
entry from one source. Object integrity failure remains degraded/quarantined even if BookKeeper succeeds. Once native
metadata says `bookkeeperDeleted=true`, Object is the only legal source; physical BookKeeper residue is not a fallback.
The flag may mean BK_DELETE_INTENT or BK_DELETE_DONE and therefore is not a physical-delete receipt.

Cache is never authority. Cache keys and accounting include Protocol Cell and Cell Provider Scope; each cell has an
independent capacity share. A cache hit is validated against the selected descriptor generation and both declared
checksum domains/families.

The Object Extent descriptor remains outside the canonical body it digests. A Provider Object Proof may accelerate
verification but cannot replace the expected descriptor, and protocol-native Kafka/Pulsar bytes remain exact across
cache and materialization boundaries unless a new format explicitly defines a rewrite.

Whole-Object proof establishes PUT durability and resolves uncertain recovery; it is not repeated before each routine
NWG1 frame range. Normal random read uses the leaf's bounded exclusive `directoryPrefixEnd19`, then authenticates the
Root-bound header/directory and selected frame locally. Short/long hints reuse already read bytes and never authorize
offsets. A checkpoint/manifest stores structured leaf fields and reconstructs the key from the Root prefix rather than
repeating the complete Object key.

For an open Object-WAL run, the canonical lane/sequence/prefix-end/length/SHA leaf plus verified group header
reconstructs that descriptor; the pre-open WalRun Root supplies its exact scope/prefix and aggregate recovery budgets.
Asynchronous checkpoint pages inventory provider-resolved extents even when one member still waits for its binding
frontier. They do not prove member ACK/readability and cannot hide a provider-resolved lane tail that remains
discoverable by bounded provider LIST. Runtime binding trackers and gaps are reconstructed rather than persisted as a
second manifest authority.

Checkpoint rows and Seal are physical-only: Root identity appears once per page, and no row stores a binding/read
frontier, ACK bitmap, gap state, or per-binding coverage. 0.2 admits no partial-run skip vector or manifest-derived
extent omission. Apart from an authoritative whole-WalRun retirement frontier, recovery performs a bounded prefix GET
through every discovered/checkpointed extent in the current non-retired run. Every request/byte/time unit charges the
cumulative recovery envelope, and this directory path never performs a whole-Object GET.

Provider-proof mode defaults to `NONE` and is fixed by the WalRun Root, not Topic policy. An evidenced version-bound
mode stores only a closed proof tag, checked token length, and bounded canonical binary version bytes. The token may pin
a range read but cannot replace Root/key identity or directory/frame AEAD. `NONE` does not add a whole-Object recovery
read because checkpoint rows already describe provider-resolved extents.

The owner publishes active-tail readability before ACK. One shared `VerifiedExtent` feeds compact locator ranges in a
shard-owned segmented, protocol-specific index; it does not repeat Object/KMS/directory validation per Binding. For one
Binding, locators for the next contiguous range are installed hidden, Readable/Durable frontiers are published, and
only then is ACK completed. Locators beyond a gap remain invisible. This is logical per-Binding isolation, not a
requirement for one heavy index object per Binding or append unit.

Tracker and locator capacity are reserved together before protocol position allocation. A locator retires only after
the manifest-selected generation covers its exact typed range and source protection/read pins make replacement safe.
The replacement view is installed before removal. Active-tail readability and hard bounds cannot be disabled; only
binding/tenant soft shares, Cell/shard recovery concurrency, host ceilings, and materialization pressure are tunable.

`BindingReadViewSnapshot` is logical. Append does not create a snapshot generation: hidden locators precede a release-
published frontier and ACK. Low-frequency source-selection generations are pinned allocation-free for one
Binding-scoped protocol read batch. One snapshot may select disjoint manifest and active-tail ranges, while one atomic
append unit and every declared whole-range fallback remain source-pure.

Reclamation publishes preferred+protected-fallback first and drains older pins before retiring obsolete index state.
Protection remains until a later view removes fallback and every fallback-bearing pin drains. Retired-view/pin count,
bytes, age, and deadline are hard-bounded; capacity pressure may block handoff/retirement or new read admission but
never removes a locator/protection early. Exact local coherent capture and the durable no-fallback cut remain open.

## Timestamp and protocol-position indexes

Kafka Offset, Pulsar Position/entry, batch, and timestamp indexes are first-class descriptor members where their
protocol path requires them. They are built from the same typed source cut as the payload and published atomically
through the manifest root. Timestamp lookup uses bounded candidate scans and protocol-native sentinel semantics; it must
not linearly scan a full partition or ManagedLedger under normal operation.

## Materialization and compaction

Materialization converts readable primary-WAL/sealed sources into read-optimized Object segments. Compaction may change
record visibility but preserves typed Protocol Coverage and protocol transaction/control-marker rules.

Planner input is a frozen manifest/source root. A local metadata snapshot may schedule work but final publication
revalidates durable authority. A newer generation or policy invalidates stale work before activation.

## Logical trim and physical GC

Logical trim advances a binding-scoped typed Trim Frontier independently from physical deletion. Physical GC requires:

- manifest no longer selects the source as the only readable generation;
- all protocol cursor/group/transaction retention floors pass the complete source;
- no reader pin, recovery root, task protection, source-protection record, Access Projection, Projection Map, or
  Migration Link still requires the source;
- Protocol Cell, Cell Provider Scope, physical-delete capability, Owner Epoch, worker epoch, and Storage Epoch are
  revalidated;
- response-loss state has converged and grace has elapsed;
- deletion identity matches the immutable provider object or ledger.

Deletion is metadata-first, retry-safe, and fail-closed. A provider success with lost response must converge without
deleting a recreated foreign object or repeating an unsafe operation.

Pulsar sealed-ledger offload cleanup is root-first: deterministic persisted attempt facts derive both keys, root absence
is proven before data deletion, and completion requires both objects plus covered multipart residue absent. This pair
rule does not grant a Nereus manifest native ManagedLedger deletion authority.

For `DELETE_AFTER_VERIFIED`, before ManagedLedger commits BK_DELETE_INTENT plus `bookkeeperDeleted=true`, it performs
bounded final NPO1/data/read-path revalidation, then rechecks the same attempt and eligible state in the native metadata
CAS. Object I/O does not hold the metadata mutex. Failure retains BookKeeper; permanent mismatch quarantines the Object
attempt. The cached `DualSourceReadHandle` fences new BookKeeper range pins and drains every admitted BK range before
revalidation/CAS. Physical deletion starts only after INTENT and BK-child invalidation/close; success or authoritative
absence publishes BK_DELETE_DONE. Retirement, audit, and capacity accounting require the three-state fact. RETAIN_BK
never enters INTENT, and INTENT can never revert to RETAIN_BK.

A GC executor may be shared only as a capacity pool. Every request enters through a cell-scoped task root and delete
capability, and foreign provider keys, ledgers, scopes, or credentials fail closed before provider I/O.

## Corruption

A corrupt preferred generation is quarantined. The reader may fall back only to a still-protected verified source. If
the source was safely retired and the preferred generation is corrupt, the result is an unrecoverable data error; the
system does not synthesize records or silently skip the requested Protocol Coverage.

Relevant tradeoffs: `T-MANIFEST-01`, `T-POSITION-01`, `T-PROJECTION-01`, `T-POLICY-01`, and `T-FABRIC-01`. Required
scenarios: `V2-READ-001..004`, `V2-OBJ-002/020..024`, `V2-BK-007..008`, `V2-BK-011`,
`V2-PROJECTION-001`, `V2-POLICY-001`, and `V2-FABRIC-003`.
