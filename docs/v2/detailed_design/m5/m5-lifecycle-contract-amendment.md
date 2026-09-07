---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: NotRun
authority: NormativeDetailedDesignAmendment
sourceTuple: v2-m1
---

# M5 lifecycle contract amendment

`M5-AMENDMENT-3-LIFECYCLE-V2`, accepted by ADR 0148 on 2026-09-07. This is implementation design authority;
no physical-delete authority, source-bound child, scenario promotion, Final or production authority is implied.
Use [current contracts](m5-current-contracts.md) to distinguish historical inputs from current implementation.

## Exact supersession

Only the following clauses are replaced. Unlisted integrity, source-lock, authority and evidence requirements remain.
The original documents, manifests and receipts are immutable; the amendment manifest binds their predecessor chain.

| Historical source and clause | Replacement in this amendment |
| --- | --- |
| M5-I0, Closed lifecycle / Persistent identity domains / Module ownership | Three reclamation branches; physical identity separated from eligibility; BK compaction carrier owned by kafka-bookkeeper and storage-bookkeeper |
| M5-A, Three closed representation modes / Materialized payload family / Immutable generation and manifest view | These Object modes remain; BK_ONLY compaction selects the new sealed BK carrier and typed manifest descriptor |
| M5-B, Determinism and publication, Object-only NMS1 output; M5-B wire projection publication rule | Shared semantic output and eight indexes, followed by profile-selected Object or sealed BK publication |
| M5-C, Authoritative retention-floor snapshot / Reference-free proof / Eligibility | Logical floors constrain logical expiry; replacement requires semantic coverage for every retained floor plus zero old physical references |
| Amendment 1 / ADR 0146, One physical authority cell / Exact single-key retirement protocol / Caps, recovery, and evidence delta | Terminal slots may fold into authenticated history rooted in the same selector; all ticket, migration and M4 projection rules remain |
| M5-D, Exact delete target / Final revalidation predicate / BookKeeper deletion composition | Stable typed resource and revisioned eligibility; explicit reclamation branch; native BK/Pulsar prerequisite preserved |
| Amendment 2 / ADR 0147, One permanent authority cell per target / Canonical authority / CAS-1 / CAS-2 / Recovery / Hard caps | Stable physical namespace key, typed value snapshot, READ_FENCED takeover and refresh, current-owner dispatch, durable-versus-resident capacity |
| M5-E, Five exclusive evidence children / Required real boundaries / Aggregate Final contract | Add the obligations below to their existing exclusive owners; retain exact-source, zero-skip, archive and aggregate requirements |
| docs/v2/04-bookkeeper-and-pulsar.md, Async Object offload authority logical-retention prerequisite; ADR 0052 Decision policy interpretation | Complete replacement may release that BK representation while logical messages remain; native ManagedLedger source authority and M4 protection remain prerequisites |
| docs/v2/05-manifest-read-retention-gc.md, retirement/deletion eligibility and permanent inline batch slots | Apply reason-specific eligibility and authenticated history; logical visibility, metadata retirement and physical absence remain distinct |

The BK_ONLY guarantee in docs/v2/02-storage-profiles-and-topic-binding.md is preserved and fulfilled, not weakened.
No M1-M4 wire identity or source lock is changed. Historical M4 dependency validation is still exact.

## Stable physical identity

`PhysicalResourceIdV2` is a closed tagged union, encoded in big-endian fixed-width numbers and length-prefixed exact
bytes. Text uses strict UTF-8 with round-trip validation; no Unicode normalization, case folding, URL decoding,
slash folding or percent decoding is allowed. Empty components, unpaired surrogates, unsupported kinds, trailing
bytes, overflow and oversize fields fail closed. SHA-256 uses the domain `NEREUS_V2_M5_PHYSICAL_RESOURCE_V2`.

The namespace consists of a provider-kind tag, an immutable physical service/cluster identity and an immutable
container/ledger-namespace identity. It is provisioned by the admitted Provider/BK adapter and checked against the
actual resource. It does not contain Binding, Protocol Cell, current storage epoch, worker, owner, credentials,
connection endpoint or capability generation. Aliases of one backend must resolve to the same namespace; an adapter
unable to prove alias equivalence cannot admit deletion. Two Cells observing the same physical namespace therefore
cannot create distinct authorities. Cell membership and access rights are validated in the value admission.

| Tag | Exact immutable fields in addition to namespace | Excluded changing facts |
| --- | --- | --- |
| OBJECT_VERSION | exact object key bytes; Provider immutable version ID or an admitted immutable create identity with equivalent conditional-delete semantics | length/body/root hashes, ETag alone, format role (NWG1/NMS1/NPO1/NPD1), current owner, manifest and KMS version |
| BOOKKEEPER_LEDGER | non-negative ledger ID; namespace incarnation must prevent ledger ID reuse | ensemble, LAC, metadata version/fingerprint, writer lease and owner |
| MULTIPART_UPLOAD | exact object key bytes and exact upload ID; Provider must guarantee non-reuse within the namespace | part inventory, scan state, owner and grace |

Object format roles are eligibility/validation fields, never extra physical identity tags. If a provider can reuse
a version/upload ID, or a BK namespace can reset IDs without a new namespace incarnation, admission is unsupported.
The immutable create identity option requires a provider conditional operation matching that identity; local UUIDs
without a matching provider operation are insufficient. Unversioned overwriteable objects are not admitted.

The authority key is `v2/physical-delete-m5-v2/<PhysicalResourceIdSha256>/authority-v2`. It is created once and never
recreated after done. The full canonical resource is stored in the value and checked on every read, ticket and CAS;
a digest collision or namespace mismatch quarantines the operation. Namespace admission also binds exactly one
metadata backend cluster/namespace for authority routing; identical key strings in different stores do not establish
uniqueness. Every alias/discovery path must use that route. Cross-Cell resource sharing remains unsupported: a foreign
Cell may discover a resource but cannot acquire ownership or create a second authority. Old opaque V1 authorities are non-dispatchable
under this amendment. An offline compatibility check must prove no old dispatch authority exists before new runtime
activation; automatic dual-key migration is forbidden. The current V1 foundation has no certified runtime deployment.

`DeleteEligibilitySnapshotV2` contains the exact resource digest, reclamation reason, all member Bindings and covered
ranges, native authority versions, manifest/selector observations, typed floors, reference roots, exact applicable
M4 RELEASED identities, semantic replacement or orphan proof, policy/grace, current Cell admission, owner/storage/
Provider/KMS capability identities and external-version observations. It has a canonical digest in the authority
value, refreshed only by revision-incrementing CAS. Resource equality survives all such refreshes.

## Three reclamation branches

| Reason | Required positive proof | Logical retention |
| --- | --- | --- |
| REPLACED_REPRESENTATION | Published durable replacement covers every applicable logical read and protocol recovery obligation; all old physical references gone | Messages may remain indefinitely; each logical floor is mapped to valid replacement coverage |
| LOGICAL_EXPIRY | Typed trim crosses the entire target and every applicable retention/producer/transaction/subscription floor permits expiry | Mandatory full-range expiry |
| UNPUBLISHED_ARTIFACT | Exact task/attempt fenced terminal; no publication/adoption/recovery/response-loss path can select output; authority-time grace and complete rescan | No invented logical trim; apply every actual reference and task veto |

Replacement coverage is a typed per-protocol certificate: exact old physical resource and range; replacement
resource(s), immutable index roots and selection revision; byte equivalence for non-compaction; accepted disposition
and suppression roots for compaction; retained latest values/tombstones; timestamp/ListOffsets; producer sequence,
transaction/control/aborted state and leader-epoch/recovery roots; replica/fallback/checkpoint and shared-member
coverage; and fresh protocol-owner acceptance. Missing adapters or partial coverage retain the source.

The complete existing floor/reference inventory is preserved. Each row is classified as LOGICAL_OBLIGATION,
OLD_PHYSICAL_REFERENCE or BOTH with explicit target/range relevance. Replacement discharges a logical obligation
only by exact coverage; it never ignores the row. OLD_PHYSICAL_REFERENCE must be absent. BOTH requires coverage
and absence. Unknown or unclassified rows veto. Expiry checks both the logical floor and old physical references.

All published-source paths still require exact M4 `RELEASED` for each applicable protection key/generation/batch/proof
head, zero new admission and drained local pins, fallback/recovery and every shared member. An unpublished artifact
without an M4 protection needs an authoritative proof that no protection was ever admitted, not fabricated RELEASED.
Object conditional deletion, BK sealed identity validation, multipart exact abort and response-loss proofs remain.

Pulsar keeps native ManagedLedger completion, offload-attempt identity, source eligibility, fenced BK pin drain and
`BK_DELETE_INTENT/DONE`. Logical subscription backlog may be served by the verified offload representation.
The native adapter must affirm that it no longer needs the BK source for reads or recovery; a Nereus manifest alone
is insufficient. ADR 0052 RETAIN_BK remains a policy veto even when replacement is complete; DELETE_AFTER_VERIFIED admits the replacement branch only after its safety predicates. ADRs 0020/0024/0029/0035/0036 layout and native authority clauses are preserved.
Object pair cleanup still requires root absence before data absence before multipart cleanup.
Replacement of Kafka BK runs retains all native Kafka offset and recovery semantics without waiting for expiry.

## BK_ONLY Kafka compaction carrier

The semantic compiler produces carrier-independent retained RecordBatches, dispositions, suppression/coverage roots
and all eight indexes: OFFSET_OR_POSITION, PAYLOAD_LOCATOR, TIMESTAMP, PRODUCER_RECOVERY, TRANSACTION,
ABORTED_TRANSACTION, LEADER_EPOCH, CHECKSUM_COVERAGE. Physical locator encoding is carrier-specific and validated
against exact stored bytes. Existing Object output remains supported.

For BOOKKEEPER_WAL_ONLY, persist a deterministic compaction task in the control authority, allocate owned output
ledgers in the admitted BK namespace, write records and index segments, seal all ledgers, and verify their immutable
metadata fingerprints plus full checksums and index coverage. Publish one typed SEALED_BK_COMPACTED_RUN_V2 descriptor
by the same exact selector CAS semantics. Descriptor contains task/cut/profile/revision, ordered ledger IDs and
sealed fingerprints, range/gap/suppression roots, eight index locators/hashes, semantic proof and predecessor.
No Object store, Object configuration, PUT/GET/LIST or object-materialization constructor is allowed in this path.

Empty output publishes explicit complete empty coverage and required recovery/control indexes; it does not invent a
data ledger or fall back to deleted records. Multi-ledger output is selected as one descriptor only after all parts
validate; a failed part prevents publication. Response loss rereads the selector and exact task identity. Recovery
loads that descriptor and only its sealed BK ledgers/indexes, rebuilds validated caches and protocol state, and
preserves gaps and suppression. An index checksum/missing ledger failure cannot silently use obsolete input.

Unknown ledger-create results remain in a durable attempt inventory reconciled through the BK task/creation
metadata, never by assuming that an absent client response means no ledger. Unselected sealed/open output is an
UNPUBLISHED_ARTIFACT after task fencing, complete owned inventory and no adoption path. Old input deletion uses
REPLACED_REPRESENTATION after selection, all recovery state transfer, exact M4 release and no physical references.
__consumer_offsets and __transaction_state remain BK_ONLY and must both participate in semantic/read/restart/delete
tests with Object disabled. Native broker/controller activation and compaction promotion remain M6.

## Bounded selector state and protected history

The successor envelope retains M4's byte-exact FULL batch projection and all selector/admission meanings. It adds
`retiredHistoryRoot`, `retiredHistoryCount`, a monotonic envelope revision and bounded active FULL/terminal slots.
The 1 MiB and 1024-slot limits constrain resident active work, not lifetime completed batches. History never projects
as a read source. Existing BatchIds and canonical M4 batches are unchanged.

History is an authenticated immutable binary sparse Merkle dictionary keyed by the 256-bit BatchId. A leaf binds
the exact retired tombstone bytes/digest and Binding incarnation. Empty-leaf and branch domains are distinct;
branches bind depth and both child hashes. Empty subtrees are canonical. Membership/nonmembership proofs have
exactly the bounded depth and reject wrong key, depth, hash, Binding, malformed or missing nodes. Implementations
may compress paths only with an equivalent canonical proof. A root is authoritative only inside the selector.

Folding proceeds from the exact selector revision: verify each chosen slot is RETIRED, no ticket/fence/reference
can still mutate it, and no live projection uses it; construct immutable content-addressed history nodes; create or
verify their exact bytes; then one selector-key CAS changes root/count and removes only those terminal slots.
Prewritten nodes have no authority before CAS. Conflicting node bytes quarantine. No all-or-nothing multi-key
transaction is inferred. CAS failure retains the old selector; unknown CAS requires exact authoritative reread.

An active hole does not prevent folding unrelated terminal leaves. Every new slot/ticket/control admission checks
both active slots and a proof against the exact current history root. A historical BatchId is permanently rejected,
even when a caller presents a different payload. Absence of a slot alone is never permission. Concurrent root change
invalidates the proof and requires retry. Legacy wrapping closes legacy writes and validates all original retired
slots before folding; it never erases an M4 batch or accepts a legacy selector after wrapping.

If a lost retirement/fold response is followed by further folds, exact membership of the expected tombstone proves
the target terminal without claiming that unrelated selector fields stayed unchanged. A stale operation may return
EXISTING_TERMINAL, but may never recreate a slot. Restart loads the current selector root; a stale cache/root cannot
authorize mutation. Missing/corrupt history retains data and quarantines control admission. Root/count/revision may
never roll back; any import/restore must establish an equally strong current authority fence before writes.

Inductive safety: initial legacy tombstones or empty root reject existing IDs; a fold only adds exact terminal leaves;
an admission proves nonmembership at its CAS predecessor; therefore every successful successor preserves rejection
of every retired ID. Active holes remain in the selector and cannot disappear through folding. History payload storage
is proportional to history, while selector bytes and proof working memory stay bounded independently of age.

Metadata admission reserves active slots, history-write bytes, unresolved prewrites, pending CAS and per-Cell I/O.
CAS contention cannot create unbounded unaccounted nodes. Immutable historical nodes are retained in this amendment;
their physical GC would require proof against all live/root/snapshot/receipt references and is not implicitly allowed.
Expose history bytes/count, active bytes/count, unresolved prewrites, oldest active work, fold backlog/latency and
durable namespace headroom. Operators can provision durable capacity without rebuilding topics or reusing BatchIds.

M5-D done records are also permanent durable history. Compact done values must retain resource, attempt, revision,
owner/capability and absence-proof identity at the same key. A retained-done cap applies to resident cache/work queues,
not lifetime successful deletes. Cache eviction must reload authoritative done before any new admission. Durable
namespace quota is separate, measured and expandable; exhaustion safely stops new GC intent while allowing existing
intent reconciliation. No timeout deletes a done record, and no finite metadata device is claimed to grow forever.

## Writer relevance and fencing

The [writer matrix](m5-lifecycle-writer-matrix.md) is required input to activation, with exact code-entry and executable
evidence links. Enum coverage or a generic guard does not satisfy a row. For each resource, proof adapters return a
target-relevant membership/version token; unrelated append or manifest advancement does not invalidate that token.
Removing a reference must still reconcile its ticket; adding a reference after fencing is forbidden. A new current
Binding state can advance independently only when it neither names nor can reopen the fenced old resource.

Normal append never obtains a target ticket or adds a remote control call. Ordinary reads use M4 local admission
epochs/pins. Fencing is a low-frequency control action that closes admission and drains already admitted pins before
exact RELEASED. A reference-admitting control writer uses durable tickets. Native lease/owner/capability revocation
may not be blocked by GC: it monotonically invalidates observations; the coordinator must refresh under the durable
read fence, and an invalid dispatch epoch cannot become valid again merely by restoring old fields.

Multi-target writers canonicalize and deduplicate full resource IDs, sort by unsigned canonical bytes, acquire in that
order and dispatch only after all tickets are authoritatively acquired. Conflict releases only acquired tickets for
which no external dispatch occurred and that exact non-dispatch is proven. Partial or unknown dispatch retains all
affected tickets until each external fact is reconciled. This is deadlock avoidance, not atomic multi-target mutation.
An operation requiring atomic multi-target effects remains unsupported. Pulsar root/data/multipart ordering further
restricts dispatch; sorted acquisition does not override root-before-data cleanup.

## READ_FENCED recovery and dispatch

CAS-1 persists resource, eligibility digest, fence epoch, readAttempt, observation epoch, current coordinator owner
and capability plus revision. It grants no external deletion. A replacement coordinator proves the previous owner
fenced through native durable authority, then exact-CASes the same READ_FENCED value to an incremented observation
epoch/revision and its own owner/capability. The resource and closed-admission fence never reopen. The prior external
observation is discarded. A stale/unsupported new capability retains the resource until a qualified refresh.

The new owner reconstructs the full target-relevant eligibility snapshot and performs full external identity reads
under that epoch. Refresh may change proofs/owner/capability, never the resource. If replacement coverage or native
eligibility was lost, preserve the fence and record a visible recovery veto; repair/adopt a valid replacement through
the explicit recovery path before trying again. Revocation is not a reason to erase the fence or force deletion.

CAS-2 matches exact revision, fence, observation epoch, owner, capability and snapshot, binds the external full
identity and fixes the delete attempt. A late old observation/result cannot satisfy it. Dispatch requires an exact
current intent reread, owner fencing and provider/BK/multipart identity match. INTENT takeover preserves target and
attempt while refreshing dispatch epoch and admitted capability via exact CAS and fresh external validation.
Revoked capabilities or unknown old in-flight calls veto conflicting external actions; old requests may affect only
the already-authorized immutable resource. Their late callbacks cannot advance successor done state.

Delete-response loss uses full authoritative exact-identity/absence reconciliation. Changed object version or sealed
ledger fingerprint is not absence of the requested target. Completion binds the current dispatch token and absence
proof. No cancellation, timeout, local cache, generic receipt or successful CAS-1 creates dispatch or done authority.

## Acceptance ownership and unchanged later boundaries

The [acceptance matrix](m5-lifecycle-acceptance.json) is normative coverage, with all new executions initially OPEN.
Its source-bound evidence must be produced on one exact tested source with admitted real Oxia, BK and Object
dependencies wherever applicable; mock/model results are supplementary. The five M5-E children retain exclusive
ownership and must all pass before the source/attachment/archive and aggregate checks permit Final.

Review 7.1: CURRENT_SOURCE_CELL_ISOLATION owns bounded same-lane slow/unknown PUT tests across multiple Bindings,
other lanes/Cells and shard counts. Capture per-Binding latency/throughput, pending bytes, convergence time, admission
and recovery work; fixed lane classes are not arbitrary PUT parallelism. Preserve ADR 0062 sequence/nonce safety.
Production-scale sizing and noisy-neighbor certification remain M8, with raw per-Binding data as an M5 handoff.

Review 7.2: RETENTION_METADATA_RETIREMENT owns whole-run recovery retirement independently of physical GC. Require
complete durable replacement of replay/checkpoint/protocol roots and every member, then retire the whole run from
recovery admission while keeping any selected long-lived Object read representation. No partial-run skip is added.
Measure prefix GET count/bytes and restoration time against non-retired extent count under blocked GC; a pending
physical deletion must not by itself retain an otherwise proven-unneeded recovery run. M7 owns planned operational
handoff; M8 owns scale qualification using this interface.

Review 8.1: CURRENT_SOURCE_CELL_ISOLATION reuses the exact M4 drain contract. Test never-callback I/O, unconfirmed
cancel, retained source buffer, actual completion/confirmed close and restart; timeout cannot clear a pin. Enforce
per-Binding shares within Cell quarantine and prove a healthy Binding retains admitted capacity. An unplanned death
without accepted durable-drain proof remains fenced; qualified expiry is not silently admitted. M6 owns native
process drain wiring, M7 planned handoff and M8 deployment qualification. M5 must record the retained case and test
actual verifiable exit when the underlying operation completes or the accepted durable drain proof is available.

Review 8.2: CURRENT_SOURCE_CELL_ISOLATION covers storage payload reads and descriptor replay, BK/Object read load,
Applied lag and journal fault injection in the admitted M2 test boundary. Native Observed-in-HW followed by journal
loss plus leader failure, election correctness and ISR shrink are M6 process scenarios V2-KAF-DATA-012/013/022;
they remain PLANNED. Handoff requires exact descriptor durability, leader eligibility and payload replay interfaces,
fault ordering, aggregate read metrics and source-locked native execution. No model PASS promotes these rows.

Review 8.3: PHYSICAL_DELETE_ORPHAN_RECONCILIATION owns real native offload-attempt/completion/source eligibility,
M4 protection and BK intent/done composition for both replacement and expiry, including active subscription backlog.
M6 still owns production broker process activation, and M8 native offload parity/scale. No Nereus cursor replaces the
native authority. M5 owns the composition implementation and real dependency test; it cannot defer that to M6.

## External context, not source-locked evidence

[Pulsar tiered-storage overview](https://pulsar.apache.org/docs/next/tiered-storage-overview/) describes accessible
backlog after offload. [Kafka distribution](https://kafka.apache.org/43/implementation/distribution/) describes
offsets-topic compaction. These support the review motivation; runtime validation still uses repository source locks.
