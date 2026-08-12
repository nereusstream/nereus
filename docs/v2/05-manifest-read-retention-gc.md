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
published frontier and ACK. One unfinished Binding-scoped protocol read batch reserves one exclusive slot from a
bounded sharded cross-Binding pool. It publishes `{Binding,G}`, establishes StoreLoad, revalidates G, then captures one
stable generation-tagged frontier/view cell before dereferencing G. Its one atomic `SlotLeaseWord` remains pinned
through every source access and buffer use; cancellation forbids new use but only complete terminal drain clears it.
Late mismatched callbacks are no-ops and nonresponsive work consumes bounded quarantine. One snapshot may select
disjoint manifest and active-tail ranges, while one atomic append unit and every declared whole-range fallback remain
source-pure.

For Kafka, ADR 0087 extends the coherent capture with Kafka leader epoch, Log Start, LEO/Readable, HW, LSO,
committed-producer-state generation, transaction/aborted-index generation, and leader-epoch-index generation. Replica,
read-uncommitted, and read-committed
Fetch select LEO, HW, and LSO respectively. A source generation change after capture never replans the request, but one
snapshot may intentionally read disjoint non-overlapping Object and BookKeeper ranges. This is not a requirement that
an entire Fetch use one source; source purity remains per atomic append unit and declared whole-range fallback.
`read_committed` returns protocol-native batches through LSO plus Kafka's native aborted-transactions response metadata;
the storage layer does not silently delete aborted batches/control markers as a replacement for Kafka Fetch semantics.

`ObjectMaterializedFrontier` is only a contiguous routing hint derived from exact Source Map coverage. It does not
advance LEO/HW/LSO. A sequential Fetch cursor is disposable and may not retain this read pin across requests; the next
Fetch captures a new view and discards the cursor unless every run/source/index/state identity still matches.

Reclamation durably publishes `PREFERRED_WITH_FALLBACK` first and drains older pins before retiring obsolete index
state. One later Binding/incarnation selector CAS competes atomically with takeover/read grant and performs the complete
`PWF(O,E,ADMITTING) -> PO(O,E+1,ADMITTING,batch[last=E],anchor[E])` cut. It closes E, grants no-fallback E+1, and
persists the closure anchor. Cross-key reread/backend history cannot substitute. `STOPPED` recovery uses only a fresh
epoch; an unknown response forbids further E admission until exact reread. One small bounded inline canonical set owns
unresolved anchors. Every `ADMITTING` cut reserves a complete emergency STOPPED envelope under the backend hard cap;
normal work cannot borrow it.

Each source/protection row inherits its own `first_i`; source i remains protected until current pins drain and every
epoch in `[first_i,sharedLast]` has contiguous planned-drain/qualified-expiry proof. Batch minimum is summary only.
Each proof is on demand after an asynchronous irreversible terminal cut, fenced, closed-verifier-checked, deterministic
and create-only. Planned-drain and qualified-expiry variants use the same verifier; for a non-transactional backend the
immutable candidate is the safety proof, while owner/reconciler fencing is ACL/rate/audit. Valid anchors prune
asynchronously in batches. N source rows retain up to N exact release CAS operations plus bounded O(N) recovery scan.
One quarantined member blocks full-batch retirement/capacity but not sibling release. After every member/reference
retires, full metadata changes only by exact-version same-key `FULL_V1 -> RETIRED_V1`; the permanent 0.2 tombstone
cannot release protection or authorize GC. Missing/revoked evidence retains protection and consumes Cell
count/bytes/age admission.

## Timestamp and protocol-position indexes

Kafka Offset, Pulsar Position/entry, batch, and timestamp indexes are first-class descriptor members where their
protocol path requires them. They are built from the same typed source cut as the payload and published atomically
through the manifest root. Timestamp lookup uses bounded candidate scans and protocol-native sentinel semantics; it must
not linearly scan a full partition or ManagedLedger under normal operation.

## Materialization and compaction

Non-compacting materialization converts readable primary-WAL/sealed sources into read-optimized Object segments while
preserving exact Kafka RecordBatch bytes. Kafka compaction is a distinct protocol-semantic rewrite: it may remove part
of a batch, create sparse/empty batches or new batch boundaries, and rewrite CRC/timestamp fields while preserving
logical offsets, producer recovery, control-marker/coordinator-epoch semantics, transaction/aborted metadata,
tombstone retention, and native timestamp/ListOffsets behavior. It rebuilds every affected range, producer,
transaction/aborted, leader-epoch, and timestamp index before generation publication. Typed coverage remains exact;
byte identity with the pre-compaction generation is neither required nor claimed.

Planner input is a frozen manifest/source root. A local metadata snapshot may schedule work but final publication
revalidates durable authority. A newer generation or policy invalidates stale work before activation.

## Logical trim and physical GC

Logical trim advances a binding-scoped typed Trim Frontier independently from physical deletion. Physical GC requires:

- manifest no longer selects the source as the only readable generation;
- all protocol cursor/group/transaction retention floors pass the complete source;
- no reader pin, recovery root, task protection, source-protection record, Access Projection, Projection Map, or
  Migration Link still requires the source;
- Object-WAL retirement has durable `PREFERRED_ONLY`, current-slot terminal drain, contiguous required Read Admission
  Epoch proof coverage per source interval, selector-carried closure anchors, irreversible terminal cuts, and exact
  historical capability-evidence binding;
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

Relevant tradeoffs: `T-MANIFEST-01`, `T-KAFKA-01`, `T-POSITION-01`, `T-PROJECTION-01`, `T-POLICY-01`, and
`T-FABRIC-01`. Required scenarios: `V2-READ-001..015`, `V2-OBJ-002/020..024`, `V2-BK-007..008`, `V2-BK-011`,
`V2-KAF-DATA-006..012/015..016`, `V2-PROJECTION-001`, `V2-POLICY-001..002`, and `V2-FABRIC-003`.
