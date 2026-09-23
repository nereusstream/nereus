---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: FocusedOnly
authority: NormativeDesignIndex
sourceTuple: v2-m1
---

# Current M5 contracts and implementation

Read [ADR 0148](../../../decisions/0148-v2-m5-lifecycle-contract-amendment.md) and the
[lifecycle amendment](m5-lifecycle-contract-amendment.md) first. The exact supersession table selects current clauses;
unlisted original I0/A-E and amendments 1/2 remain applicable. Their frozen bytes and receipts are historical inputs.
The [amendment 3 manifest](m5-design-amendment-3.json) binds this decision and its immutable predecessor chain.

| Flow | Current contract | Current implementation and evidence boundary |
| --- | --- | --- |
| Resource authority | Typed stable namespace/resource ID; eligibility in revisioned value | Typed M5RI V2 identity now feeds authority M5DA wire 4 and generic same-key CAS; typed M5ES eligibility and full fact reread now guard CAS-1/CAS-2; guarded BK creation now observes actual INSTANCEID and permanent reservations, and a permanent BK binding now selects one actual Oxia namespace; Object namespace, complete writer/Cell admission and real dispatch remain OPEN |
| Replacement / expiry / unpublished cleanup | Three explicit branches with complete semantic and physical-reference proofs | Reason-specific typed predicates, snapshot invalidation and fact freshness passed the focused gate; guarded BK task cancellation and native drain now produce a durable physical cut, while real protocol proof producers, grace/rescans and full deletion composition remain OPEN |
| Kafka compaction | Shared semantics; Object or sealed BK carrier; internal topics remain BK_ONLY | Shared semantics, inventoried BK parts and sealed descriptor publication/recovery now compose real Oxia control with real BK under the existing M4 planner and hazard kernel; a native SPI profile fences late creates; same-selector decisions and native writer drain now produce restart-stable cancelled task terminals, and sorted input/output tickets wrap the guarded native publisher; native raw-run, selected-generation and one-selected-plus-raw input membership use exact BK/Oxia sources and scoped read budgets, including one ticketed mixed-input republication and recovery; complete catalog and namespace/task/protocol-owner admission, ordinary reads, internal-topic lifecycle and cleanup remain OPEN |
| Binding retirement | Bounded active selector plus authenticated immutable history | M5R1 wire 2 binds history root/count and monotonic activation ordinals; additive wire 3 preserves that history while carrying one bounded task decision until exact permanent archival; a configured native M4/history route now passes source-locked Oxia continuation/restart checks on the existing M4 selector key; unique native namespace/Binding assignment, quota/restart accounting, Cell I/O/metrics and physical GC worker scheduling remain OPEN |
| Permanent delete history | Compact done at the same resource key; resident cap independent of lifetime history | M5DC V2 retains exact resource/attempt/revision/owner/capability/absence identities; native Oxia compaction/cache recovery and a configured durable quota route pass; pre-reserved intent writes continue at exhaustion and pending grants/refunds recover after restart; unique namespace/all-writer admission, backend disk provisioning and GC scheduling remain OPEN |
| Writers and recovery | Target-relevant tickets, local pins, READ_FENCED takeover, current-owner intent/done | READ_FENCED refresh/takeover now passes real Oxia CAS, fact-version invalidation, competing-client and server-restart checks with synthetic owner/eligibility facts; sorted multi-resource tickets now wrap the guarded native BK publication path; automatic same-key recovery veto now survives restart and requires qualified refresh before intent; complete mixed native catalog admission, owner adapters, physical dispatch and [concrete writer matrix](m5-lifecycle-writer-matrix.md) remain OPEN |
| Evidence | Five M5-E children plus amended [acceptance matrix](m5-lifecycle-acceptance.json) | No revised source-bound M5 children or aggregate Final; scenario promotion remains unauthorized |

M5-C logical trim treats a retry as exact only when the stored frontier binds the same identity, predecessor,
floor snapshot root, policy and owner/storage fences. An authoritative new snapshot at the same numeric floor
persists a successor generation; unchanged position alone cannot stand in for that snapshot. Focused unit and real
Oxia reconnection tests cover this distinction. Physical writer tickets and complete floor producers remain OPEN.

The [physical identity projection](m5-physical-resource-identity-projection.json) and
[eligibility projection](m5-delete-eligibility-projection.json) record the current focused results:
`v2M5DeleteEligibilityCheck` passed 42 tasks at the typed-eligibility slice, including 7 identity, 13 authority,
12 coordinator, 10 eligibility and 184 existing retention tests. The later
[observation recovery projection](m5-read-fenced-recovery-projection.json) adds 7 coordinator cases and requires a
native verifier that is unsupported by default. These results remain non-promotable and use synthetic eligibility facts.
`v2M5ReadFencedRecoveryCheck` passed 44 tasks with the expanded 19-test coordinator suite and 3 recovery contract tests.
The later `v2M5ReadFencedOxiaCheck` runs the actual configured namespace route and coordinator with all eligibility
and observation facts stored under scoped native keys and bound to server versions/hashes. Six integration cases
cover post-CAS delivery loss, competing-client refresh, the default unsupported owner verifier, and changed native
fact versions (including identical bytes), plus delayed rejection losing to a successful native refresh, and typed INTENT CAS competition/fact invalidation. Two separate
JVM phases verify the same server container's persisted fence, veto and typed INTENT before and after restart; each checkpoint contains only route, key, hash and version. The six archived suites contain
57 cases/phases. Owner/fencing statements, semantic transfer, M4 RELEASED and external identity remain synthetic;
no native protocol owner adapter, external identity read, delete call or M5-E receipt is supplied by these tests.
The recovery projection's five contract tests preserve this distinction.

`DeleteRecoveryVetoV2` now stores one 73-byte rejection extension at the existing authority key. Ordinary authority
values retain their exact wire-4 encoding; only READ_FENCED values carrying a veto use wire 5. Refresh and CAS-2
owner/capability/fact validation failures automatically attempt the exact veto CAS and still complete exceptionally.
`RecoveryRejectedException.vetoResult()` distinguishes confirmed persistence, conflict, unchanged predecessor and
unknown response; a failed or cancelled caller cannot turn rejection into successful recovery. The field retains a
bounded reason, rejected observation epoch/context hash and exact predecessor authority hash, with no appended history.
Repeated failure with the same reason and observation context rereads the same veto without incrementing revision. A veto prevents intent binding until
a new observation epoch and full freshly qualified snapshot clear it. Resource, original read attempt and closed fence
remain fixed; delayed old failures cannot overwrite a successful successor. The expanded coordinator suite has 35
cases, including seven veto/cancellation/reconciliation/compatibility cases; the four ordinary V4 phase hashes were
captured from the previously published runtime and remain byte-for-byte identical. The public coordinator veto entry also accepts evidence-collection failure before a complete successor snapshot
exists, without inventing owner facts or qualifying any new authority. Native ownership, complete proof production,
and physical dispatch remain OPEN.

`DeleteDispatchRefreshV2` now keeps only the latest pair of observation contexts, previous dispatch epoch and exact
predecessor revision/hash. Typed INTENT refresh uses additive M5DA wire 6 and preserves the original CAS-1 fence,
CAS-2 revision, delete attempt and external identity. It requires the exact next dispatch/observation epochs, full
fresh same-resource eligibility, predecessor native fencing when the owner changes, and current native owner/capability
plus complete fact reads before and after the actual external identity read. Its new token also binds capability,
eligibility and refresh evidence. Missing adapters, changed facts/identity and stale CAS retain the closed intent;
cancelling the observer cannot abort accepted native work or its CAS reconciliation. Repeated refreshes do not append
history; once a refresh establishes authoritative absence, later refreshes reject reappearance. Legacy hash-only
takeovers cannot be upgraded by inventing typed context.

`KafkaBookKeeperDeleteIdentityReaderV2` captures/re-reads through the guarded instance-bound BK client, binding the
physical namespace/ledger, run/configuration, sealed LAC/length, quorums/digest/credential and complete native metadata
fingerprint. Native NoSuchLedger is required for absence. The source-locked BK/Oxia integration uses actual sealed
metadata, exact authority CAS and actual fixture-ledger removal before absence completion; its owner and semantic/M4
proofs remain synthetic. Native BK intent binding also rejects a persisted eligibility snapshot whose M4 RELEASED
fact is outside the canonical protection key grammar, even if that fact remains version/hash-fresh. `completeAbsent`
revalidates native owner/capability and all facts around an actual absence
read, then persists ALREADY_ABSENT and supports permanent compact done. The legacy hash-supplied `completeDelete`
entry only rereads an existing exact terminal, including compact historical done; it cannot mutate metadata.
The production physical-delete dispatch composition, Object/provider-wide readers and native protocol owner/proof
producers remain OPEN. No child receipt or aggregate obligation closes from these bounded integrations.

`M5BookKeeperNativeDeleteAuthorityV2` now stores one permanent native M5DE epoch per guarded physical BK ledger.
The record binds stable resource, owner UUID, epoch and the fingerprint of the complete admitted BK capability;
its epoch equals native znode version plus one, with overflow rejected. Claim advances the exact native version
under namespace, closed task-create and permanent reservation checks; an exact stored candidate reconciles a
lost reply. Every current-owner check rereads the reservation body to prove its exact task binding; another task
with the same run configuration cannot reuse the observed epoch to delete that ledger. Deletion re-reads the full sealed identity and performs all four fence checks plus deletion of the exact
native ledger version in one ZooKeeper transaction. A paused predecessor cannot delete after a successor claims
the epoch, even if the predecessor had already read identical sealed metadata. A same-byte metadata rewrite also
invalidates the captured version. Native NoSuchLedger alone proves absence; unknown replies retain uncertainty
until actual native rereads resolve it, and current epoch is revalidated before reporting. Observer cancellation
does not cancel accepted work. The epoch remains after ledger deletion and retains constant-size latest state.

Four real native cases plus two independent-JVM phases verify these boundaries, including full BK append/seal,
actual server BADVERSION, applied-but-lost epoch/intent/delete replies and retained epoch/intent after the same
ZooKeeper/bookie services restart. Each JVM verifies the locked BK client artifact. The restart checkpoint carries
input spec, ledger ID and expected hashes/versions/old-owner identifier; the new JVM reads the surviving native epoch
and intent itself before deletion. These remain low-level native operations on guarded test ledgers.

`KafkaBookKeeperDeleteObservationAuthorityV2` now verifies the configured GC owner's UUID and complete admitted
capability against the actual permanent epoch. Its read-only fact route returns that exact native M5DE body and a
resource-scoped native version, never an invented or separately stored proof body. Both owner and capability roles
refer to the same exact fact. A same-named Oxia record cannot shadow native facts, foreign-resource routes fail, and
native facts cannot be written through the metadata port. Predecessor fencing requires a strictly newer native epoch;
multiple native claims may precede one successful Oxia observation refresh. These native GC facts do not establish
Kafka/Pulsar protocol semantics, replacement coverage or M4 RELEASED.

`M5BookKeeperNativeDeleteIntentV2` stores one permanent M5DI binding per physical resource. The body fixes native
epoch, M5 dispatch token, exact M5 authority SHA and full ledger metadata SHA. An identical retry reconciles the same
record; changing any bound identity under the same native epoch fails closed. A successor epoch replaces only this
bounded record by exact native CAS. `bindIntent` on the Kafka adapter rereads the exact current M5 intent, validates
native owner/capability and every bound eligibility fact, reads full native identity, binds the native record, then
revalidates owner, every eligibility fact and exact M5 metadata. If M5 changes during binding, the old binding remains for explicit epoch/observation recovery. Native
delete using this binding atomically checks both epoch and intent versions with the ledger metadata version, and
revalidates current native binding before reporting. Epoch and intent survive ledger absence; neither is recreated
from a checkpoint or appended as per-attempt history.

Native intent binding rereads the complete deduplicated eligibility vector on both sides of native binding, including
namespace/member inventory, logical floors, selector/manifest/trim, all M4 RELEASED records, every physical reference
(including audit/grace), protocol semantic transfers, and task/adoption facts where applicable. Reads must match the
exact key, native metadata version and canonical SHA; equal bytes at a newer version are stale. A missing/changed
pre-binding fact prevents native intent creation. A post-binding change fails the caller while retaining the permanent
native binding, so a successful earlier binding cannot be presented as current eligibility. The bound route uses this
same validation. Qualified snapshot refresh repairs the stale fact vector; a changed native token still requires a
new native epoch if an earlier native binding exists.

The native regression changes an audit/grace fact before binding and an M4 fact after binding while preserving their
bytes and the exact M5 intent. Public bound admission also rejects a stale audit fact and resumes only after a full
snapshot refresh. These are freshness tests using synthetic protocol/M4/audit statements in real Oxia. They do not
establish an admitted authority-time service, clock bounds, a complete reference scan or dispatch capacity. Grace's
numeric deadline/observed-time fields alone are not proof of native time authority; that producer remains OPEN.

Real BK/Oxia cases verify actual GC-owner takeover and same-owner epoch refresh, forged/shadowed native fact
rejection, exact intent binding and changed-M5-intent recovery. One fixture performs the bound low-level native delete
and current-owner absence completion/compaction; protocol/M4 eligibility remains synthetic and M4 selection remains
unchanged. Intent binding alone does not prove grace or reserve Cell dispatch/unknown capacity. Native epoch/intent
canonical capacity has the separate reservation described below. Complete protocol proof production, unguarded legacy
ledgers, all-provider dispatch and full writer coverage remain OPEN.

`KafkaBookKeeperDeleteObservationAuthorityV2.dispatchBoundDelete` now joins the exact current Oxia M5 INTENT to
its permanent BK intent and configured native Cell budget. It checks the active bound route, exact M5 value,
native owner, complete eligibility fact vector and sealed ledger identity; after the external identity read it
rereads the route, M5 value, owner, fact vector and native intent before reserving Cell capacity and issuing the
server-fenced native delete. It rejects a stale native token or changed eligibility fact without dispatch or Cell
mutation. Its result is only a native operation outcome: the coordinator must independently reconcile actual
absence, write/compact M5 DONE and settle quota. The real BK/Oxia restart phase exercises that sequence; the
protocol, M4 and grace facts in this fixture are synthetic, so writer closure, real source membership and full M5-D
dispatch admission remain OPEN.

`BoundPhysicalDeleteAuthorityRouteV2` now supplies explicit unique-route and active-resource admission to public GC
claim/bind. `OxiaPhysicalMetadataNamespaceV2.openAuthorityRoute` installs actual native marker/binding revalidation;
raw constructors have no such admission and fail closed. The route checks the deterministic root, physical namespace,
existing permanent quota grant and current nonterminal M5 authority. A grant without an authority is insufficient;
full/compact done and settled grants reject new GC work. `M5BookKeeperNativeCreateClientV2.requireNamespaceBinding`
rereads INSTANCEID and permanent binding through the same owned native connection. The GC adapter compares both
native assignments before and after claim/bind. Endpoint aliases and caller roots cannot select a second authority.
The old generic-store binding and route-free claim helpers are package-private; the public bound native delete method now
requires a durable native intent and the Cell budget below. Failure after accepted native work retains the record for reconciliation.

The bound runner adds one active-route case and two independent-JVM restart phases. They reject manually constructed
routes, unreserved resources and grants lacking active authorities, then verify persisted native epoch/intent and
exact M5 value/hash/version before takeover. The new owner advances native epoch, refreshes M5 intent, binds the new
token, removes the fixture ledger through the budgeted BK adapter path and completes/compacts done. Durable quota settles,
then rejects another claim without changing the epoch. This route uses the existing Oxia authority-byte reservation;
the dispatch adapter separately uses the native Cell slots, while grace and protocol/M4 eligibility remain synthetic.
Complete cross-Cell/all-writer admission and the full M5 dispatcher still require implementation and source-bound
evidence.

`M5BookKeeperNativeDeleteQuotaV2` adds one permanent 104-byte M5NQ head at the actual BK namespace's
`nereus-m5-native-v2/delete-capacity` path. It accounts the canonical head path/value plus both permanent epoch/intent
paths and their fixed encoded sizes for each resource. Namespace identity, charge, capacity, count, exact native
version and checksum are read from that head. First claim atomically increments it and creates the epoch in the same
ZooKeeper multi as namespace/task/reservation checks. A stale capacity CAS or duplicate epoch cannot partially charge
or create authority; a lost reply is reconciled through actual epoch and quota reads. Bound clients require this head.
The historical unbound fixture profile remains available without a head; it cannot qualify public bound GC admission.

Bootstrap is explicit through `M5BookKeeperNamespaceAuthorityV2.nativeDeleteQuota()`, before any guarded native ledger
reservation. Initialization creates the permanent ledger-reservation parent and quota head in one transaction, so a
concurrent or pre-existing reservation prevents zero-state import. Missing heads cannot be recreated after that
parent exists; operational connection and restart only reread authority. An already initialized namespace is never
implicitly expanded or reset. Existing unaccounted native namespaces require a separately qualified migration; this
slice neither imports them nor claims their writer coverage.

Every first epoch reserves its complete future intent storage. Epoch takeover and intent replacement retain constant
size and consume no new capacity; DONE and Oxia quota settlement never refund native epoch/intent records. Explicit
capacity expansion preserves the reservation count and advances the exact head version. Exhaustion blocks a new
resource's first claim while existing native intent recovery can continue. Native version/counter overflow fails
before mutation. This canonical-record budget excludes creation/task records, directory/ACL/stat overhead, replicas,
ZooKeeper transaction logs and filesystem space; it is independent of backend provisioning and per-Cell I/O budgets.

Two native quota cases exercise actual ZooKeeper transactions, stale concurrent reservations, lost applied replies,
duplicate create, exhaustion, explicit expansion and missing/changed-head rejection in isolated fixture namespaces.
The bound BK/Oxia restart checkpoint adds the original quota-head hash: the fresh JVM reads it before GC mutation,
recovers the existing intent at full native capacity, and retains the charge after native deletion and permanent DONE.
A new bound resource is rejected without an epoch at exhaustion, then admitted exactly once after explicit expansion.
These are required validations for the native capacity slice; whole-M5 evidence remains separate.

`M5BookKeeperDeleteCellBudgetV2` owns one explicitly provisioned permanent M5CB head per physical namespace and
configured `CellProviderScopeId`, using the actual native namespace connection. Every invocation reserves one
dispatch slot and one possible-unknown slot, with effective capacity equal to the smaller immutable limit (each
1..64). The canonical head is 120 bytes plus 84 bytes per retained invocation, in addition to its exact path bytes.
It records exact native version, namespace/Cell digests, limits and bounded unique resource/operation/intent identities.
Checksum, canonical ordering, exact length, permanent-node status and actual stat version are checked on every read.
A permanent Cell parent prevents automatic reconstruction of a missing head. Reconnect and service restart read
existing state; they do not bootstrap, enlarge limits, expire reservations or redispatch work.

Reservation CAS includes the current native namespace, task, resource reservation, GC epoch and intent checks.
Duplicate resources, exhausted slots or a different Cell/namespace binding reject before invoking native deletion.
The actual deletion still atomically checks native intent and exact ledger version. Observer cancellation only
cancels its detached observation. A definite failure before invocation can release its reservation; an issued call
keeps it until completion. Terminal unknowns remain charged and may only run read-only identity/absence checks;
actual native absence permits exact operation-identified release. Exact presence or changed identity during unknown
reconciliation retains the record. ACTIVE records never become terminal based on age or restart. Head conflicts
have at most four release/terminal-state attempts; unresolved mutations retain capacity and never trigger deletion.
An old operation callback cannot remove a newer resource reservation with a different operation identity.

The bound fixture's restart checkpoint includes its empty Cell head hash and rereads it before any GC mutation.
Two additional actual native tests verify cancelled held invocation, occupied-head equality through client reconnect,
rejection of duplicate/full/foreign-Cell dispatch, independent configured Cells, release after known pre-dispatch
failure and native completion, and terminal-unknown retention followed by read-only absence release. Intent token and
M5 authority digests in those low-level fixtures are synthetic. The additional separate-JVM Cell restart phases
below establish occupied-head persistence. A newer permanent native GC epoch now permits exact ACTIVE or
callback-terminal UNKNOWN hold release:
the old delete transaction checks its old epoch at the ZooKeeper server, so it either linearized before the successor
claim or is rejected afterward. The budget checks the held resource and complete old native intent, then rereads and
verifies the successor before removing that operation's hold. This frees only Cell capacity; it does not infer ledger
absence, dispatch a new delete, refresh M5 eligibility or settle quota. Same-epoch transport drain and
terminal-UNKNOWN presence recovery remain OPEN. These limits account logical native operations,
not transport buffers, per-Binding fairness/rates or total backend storage. Cell heads are not charged to the separate
per-resource native quota; explicit per-Cell provisioning does not establish a global Cell-count budget. Protocol
Cell ownership, grace and complete M5 dispatch composition remain required before whole-M5 acceptance.

`M5BookKeeperNativeDeleteAuthorityV2.reconcileCellDeleteAbsence` additionally rereads actual native metadata using
only the permanent native intent and the instance-bound handle. It releases an exact callback-terminal UNKNOWN Cell
hold only on authoritative ledger absence; exact presence, changed metadata or an uncertain read retains capacity.
This does not require reconstructing the vanished full delete target from an old JVM's checkpoint, does not dispatch
another delete and does not write M5 DONE. An unchanged native epoch/intent is verified before and after observation.

`KafkaBookKeeperDeleteObservationAuthorityV2.reconcileBoundCellDeleteAbsence` exposes that native recovery only
through the configured BK/Oxia route and an exact, still-current M5 INTENT whose resource, dispatch token and full
stored-value digest match the permanent native intent. It does not require the old JVM's GC owner UUID or a
reconstructed BK target. The native operation accepts only a matching callback-terminal UNKNOWN Cell hold; a missing
or ACTIVE hold cannot be released by this path. Changed eligibility facts are not promoted by Cell capacity recovery:
M5 DONE still requires a separate current-owner, fresh-eligibility and actual-absence reconciliation.

The bound BK/Oxia test now drives a real routed delete to callback-terminal UNKNOWN by reporting one controlled
CONNECTIONLOSS before forwarding its original exact ZooKeeper delete transaction. Exact ledger presence retains the
Cell hold; the fixture then delivers those same native operations and verifies actual ledger absence. The hold
survives the existing BookKeeper/Oxia service restart while another resource in the same Cell deletes independently.
A new JVM checks the durable Oxia INTENT, native epoch/intent, and the original hold's operation/resource/intent
identity before `reconcileBoundCellDeleteAbsence` releases only that hold. This recovery instance has a different GC
owner UUID; the separate M5 DONE/compaction and Oxia quota settlement use the owner read from the permanent native
epoch. The delayed delivery is a controlled test transport, not evidence of production callback timing. Eligibility,
M4 RELEASED and grace/reference facts in this bound fixture remain synthetic.

`M5BookKeeperDeleteCellBudgetV2RestartTest` begins after the original bound restart checks and lifecycle cases. It
creates three actual three-byte sealed BK ledgers with native intent bindings. For ACTIVE, the real native delete is
applied but its successful callback is withheld; cancelling its observer leaves an occupied ACTIVE head even though
actual ledger metadata is absent. For two UNKNOWN cases, a delete-only CONNECTIONLOSS is injected without forwarding
the transaction. One exact ledger remains; for the other, the fixture separately applies the old native deletion
after the callback-terminal hold is retained, leaving actual metadata absent. A second restart preserves the
same ZooKeeper, three bookie and Oxia containers, images and data, with changed start times.

The fresh JVM reads the original native head/epoch/intent hashes before any mutation. ACTIVE remains ineligible for
unknown reconciliation despite native absence; present UNKNOWN observes exact presence and remains charged. All three
reject new deletion attempts without changing their heads. A new healthy configured Cell then creates, seals and
deletes another real ledger; the three occupied heads remain byte-identical afterward. Checkpoints contain fixture
inputs and identity hashes; actual native records supply recovery observations. The runner archives all three restart
phase XMLs, hashes the checkpoint files and records both sets of service-restart identities/times. An additional
fresh-JVM phase releases the absent UNKNOWN hold without an epoch change or checkpoint target. It separately claims
strictly newer native epochs for the retained ACTIVE and present UNKNOWN operations and releases their exact Cell
holds. Each old native intent and actual ledger absence or exact presence remains separately observable. A real
paused-callback test
shows that a late old delete fails at the server after the successor claim, while a new intent can reserve the Cell
and delete the still-present ledger. These checks establish conservative retention followed by qualified fencing;
they do not free capacity merely because the original JVM ended, establish same-epoch transport-buffer drain,
or supply per-Binding fairness or a complete dispatcher.

The [shared Kafka semantic projection](m5-kafka-semantic-core-projection.json) tracks the compiler extraction and
complete row validation. Its outputs are in memory and do not authorize publication, read adoption or input deletion.
`v2M5KafkaSemanticCoreCheck` passed 42 tasks, including 14 Kafka and 7 Object materialization tests. The added
interleaved-transaction case fixed producer-independent aborted marking and uses a V2 plan/output task identity.
M5-A publication now checks the supplied protected fallback set against the validated generation before writing,
and treats a selected selector as an exact retry only when its fallback set, owner/read epochs, capability and
admission state still match. A same-manifest competing fallback selection leaves the task uncommitted.
The [BK carrier projection](m5-bookkeeper-compaction-carrier-projection.json) adds immutable task/part inventory,
native ID reservation, bounded retained-batch/index chunks, native fencing and complete entry/metadata verification.
That writer slice requires expected output bodies for part reconciliation. The subsequent
[descriptor projection](m5-bookkeeper-descriptor-projection.json) adds KBSD2, exact M4 selection and independent
read-only BK recovery without expected output or old-source bodies. That descriptor slice did not supply native
namespace/task authority, M4 read-source-plan admission, real Oxia control, internal-topic lifecycle or physical deletion;
the later integration results below identify which of these paths now have focused execution.
BK descriptor publication now accepts an exact retry only with the durable task selection decision for the exact
predecessor, successor and validated fallback set. A matching descriptor and source generation in an M4 selector
without this task decision does not establish that this task selected it.
The [M4 recovery bridge projection](m5-bookkeeper-m4-recovery-projection.json) adds low-frequency descriptor recovery
under the existing M4 planner and generation lease. Observer cancellation retains the lease through native termination;
selector closure can install a successor while the old generation remains pinned. Recovered caches retain their exact
captured authority. This bridge requires the owner's admitted current reference and Cell budget, and does not add
ordinary-read remote control I/O or independently supply native protocol-owner admission.
The [retired-history projection](m5-retired-history-projection.json) records authenticated 256-level M5H2 proofs,
exact immutable prewrites followed by one selector CAS, version-1 byte preservation and permanent BatchId rejection
after folds. M4 control and Binding ticket admission use the current history root. Local prewrite reservations retain
unknown attempts and bound pending bytes; native namespace headroom/restoration and Cell scheduling remain separate.
The [real Oxia history projection](m5-retired-history-oxia-projection.json) now adds native one-key retirement and
1,026 continuous folds with one active hole, 982-byte post-fold selectors, exact retry under response loss and
rejection after client reconnect. A separate two-tombstone fixture retains its exact selector/version/history across
a service restart. That published verification used synthetic source/reference facts and a test-only synchronous
M4 bridge. The subsequent [Binding route projection](m5-binding-lifecycle-route-projection.json) replaces the M4/history
bridge with `OxiaBindingLifecycleMetadataStoreV2` and the production M5 control facade. It shares the native M3 selector
key, validates typed full-Binding values and immutable history nodes, and scopes exact version tokens to the configured
Cell root/shard/Binding. The new route repeats the continuous and restart checks. Synthetic external proof facts,
unique native namespace/Binding assignment, quota restoration, Cell I/O/metrics, complete protocol writers and
source-bound M5 children remain OPEN. The subsequent [BK/Oxia control projection](m5-bookkeeper-oxia-control-projection.json)
adds typed immutable task/part/descriptor/candidate routing and real Oxia/BK publication and protected recovery.
Its coordinator composition uses `rawControlMetadata()` because M4 owns the M5 selector projection; direct projected
callers retain `controlMetadata()`. Bounded owner-executor continuations keep synchronous control calls off BK completion
threads. This joint route passes response-loss, missing-record, fresh-client and same-Oxia-server restart cases;
source/policy/namespace-admission facts remain synthetic and native task/writer/cleanup obligations remain OPEN.
The subsequent [native BK create projection](m5-bookkeeper-native-create-projection.json) adds an explicit owned
`m5zk` client profile without changing the locked BookKeeper source or wire. Its permanent task fence and ledger
reservation are checked in the same ZooKeeper transaction that creates native ledger metadata. The task scope binds
actual native INSTANCEID and exact run configurations. It verifies allocation reuse rejection, native response loss,
a held create transaction after fencing, and late recreation attempts after test-owned deletion. Guarded BK/Oxia
compaction now uses the observed native physical namespace; fresh JVMs retain the fence and recover selected output
after the same ZooKeeper, three bookie and Oxia containers restart. Closing creates alone deliberately does not close
publication or settle outstanding appends. Permanent records cannot be garbage-collected or reopened; administrative
namespace reformat and external record deletion are outside this admitted profile. Global writer/namespace admission,
complete TaskTerminal, quota and cleanup remain OPEN, with all 17 acceptance obligations still OPEN/null.
The subsequent [BK task-terminal projection](m5-bookkeeper-task-terminal-projection.json) composes native create
fencing with same-selector publication/cancellation and recovery of existing writers. M5R1 wire version 3 adds at most
one inline task decision; versions 1 and 2 retain their exact encodings. An immutable per-task archive must be read back
before the inline decision is cleared. A publication attempt captures the raw authority before checking archive absence,
so cancellation followed by archive/clear cannot be hidden by an identical projected M4 selector. The native route
checks archive ownership and permanence. A cancelled task's terminal records its bounded physical cut only after native
creation is closed and each present ledger has an exact recovered seal. Already selected tasks retain a reference veto;
stale tasks without a prior decision and native metadata mismatch retain unknown. Real BK/Oxia tests cover delayed
native publication, old-writer append rejection, lost responses and independent JVM recovery after all five services
restart. This guarded profile does not fence delayed unused inventory metadata, admit all stock/protocol writers, or
supply grace, complete reference rescans and cleanup authority. Complete TaskTerminal and all 17 obligations remain OPEN.
The [permanent-done projection](m5-permanent-done-projection.json) adds M5DC/version-2 compaction of an exact full
DELETE_DONE at the same stable resource key. It retains the final attempt/revision/owner/capability and absence-proof
identities, increments the revision, and binds the full predecessor hash. The native namespace route rejects imports,
phase rollback, changed proofs and any compact-done successor. A count/encoded-byte bounded cache stores only permanent
compact terminals; absent and active values always require authoritative reads. `inspect()` distinguishes compact done
from absent authority, and rediscovery cannot recreate OPEN after eviction. A fixed full-done retry matches its exact
compact successor as EXISTING_TERMINAL without claiming that the old full candidate is still stored. Native Oxia tests retain 258 terminal
samples with at most eight resident entries, reject a held ticket CAS after compaction, reconcile lost responses, and
recover done plus an existing intent in a fresh JVM after server restart. Source/owner/absence proofs in these storage
samples remain synthetic. That slice did not supply real Provider/BK deletion, durable quota or GC scheduling;
the full DONE_CAPACITY acceptance still requires the later implementation and source-bound evidence.
The subsequent [durable GC quota projection](m5-gc-quota-projection.json) adds fixed-size M5GH/M5GQ records and
`OxiaQuotaTargetDeleteStoreV2`. A 272-byte head carries at most one pending operation; each resource keeps a permanent
156-byte grant/settlement entry. New authority admission reserves a full 1 MiB value plus native keys and permanent
accounting overhead. Existing reserved intent mutations continue without acquiring new quota or clearing another
resource's pending grant. Only authoritative same-key compact done releases unused reservation; exact done bytes and
permanent entry/key costs remain charged. The head CAS precedes the entry mutation and clearing CAS, and retries
capture the exact head before reading entry absence to exclude duplicate charge/refund after ABA. Unknown results
retain the reservation. Explicit capacity expansion preserves all counters and pending work.
Initialization refuses existing authority or orphan accounting entries. The locked server's actual hierarchical
encoder prefixes total slash count, so the route scans each fixed-depth record family separately and cancels on the
first result; a generic parent-prefix scan is not an emptiness proof. Tests cover both digest extremes and malformed
legacy values. The configured route verifies 130 permanent settlements against native key/value byte totals, all six
response-loss boundaries, held native grant/refund CAS races and recovery in an independent JVM after service restart.
The recovered intent completes while capacity is full and another grant remains pending; later settlement and explicit
expansion restore admission. Canonical byte quota is distinct from backend WAL/replica/disk space, Binding history
quota, resident cache and work queues. The composition owner must exclude old/raw writers and assign a unique native
route before initializing this profile. All-writer/native namespace admission, real deletion proofs, actual backend
capacity provisioning, GC worker scheduling and all 17 source-bound acceptance obligations remain OPEN.
The [physical namespace projection](m5-physical-namespace-projection.json) adds an actual-backend binding for BK.
Oxia provisioning conditionally creates one permanent M5NM marker at a fixed namespace-global key; operational
reconnect must reread the expected instance, native version and marker hash and never recreates a missing record.
The actual BookKeeper INSTANCEID selects the physical namespace. Its permanent M5BG gate binds one M5NA metadata
identity and advances exactly once from UNBOUND/native version 0 to BOUND/version 1. Both ledger reservation and
native ledger creation check that version in their ZooKeeper transactions, so binding cuts off delayed unbound creates.
A correctly bound client still obeys the existing permanent task fence and per-ledger reservation.
The guarded factory derives the authority root solely from canonical physical namespace bytes. Each quota/authority
operation rereads the actual Oxia marker and physical-backend binding; a second metadata backend cannot use the same
physical namespace through this factory, even if it constructs identical key strings. Endpoint aliases do not enter
the identity or route. Explicit namespace provisioning is not activation or offline compatibility proof. This profile
excludes administrative marker cloning/rewrite, native reformat and external deletion of permanent records. It closes
neither outstanding append handles nor stock/protocol writers. Object namespace assignment, cross-Cell ownership,
Binding routing, all-writer admission, old-authority absence, external deletion and all 17 obligations remain OPEN.
The native namespace runner passes 71 executed main tasks and a separate 16-task post-restart JVM (one executed).
Its three identity unit cases and two native phases pass without skips; 702 captured inputs remain unchanged. The
prior 71-case legacy regression and service restart are reused only after exact source/XML archive revalidation.
The carrier gate passed 60 executed tasks with 8 inventory/layout, 7 real carrier and 8 real Cell-session tests;
its exact-native-run check now precedes fencing, including the foreign-run zero-fence negative case.

The [publication ticket projection](m5-publication-tickets-projection.json) adds bounded, sorted acquisition on every
resolved input and sealed output physical resource before the guarded BK publisher can write immutable publication
records or attempt the native selector CAS. Each invocation has a private nonce; the ticket context binds the exact
descriptor, complete canonical target set, capability and source-owner fence. A later acquisition failure prevents
dispatch and rolls back only that invocation's acquired prefix. Unknown external completion retains its tickets.
Only an actual irreversible native selected/cancelled Task decision permits terminal reconciliation, including older
concurrent invocations; observer cancellation leaves native completion and cleanup running. Recovery removes at most
256 tickets per pass and reports remaining targets. Missing authority is never initialized implicitly.
The explicit publisher constructor enables this path; earlier constructors remain foundation profiles and are not
complete writer admission. The native fixture verifies actual input/output bytes, namespace binding and quota, but its
logical source membership and OPEN eligibility are synthetic. An admitted source owner must derive complete immutable
physical membership under existing protection; this slice does not supply the native input catalog, all writer rows,
READ_FENCED owner verification, physical deletion or any of the 17 aggregate obligations.
The final native runner passes 75 executed main tasks and a 20-task post-restart JVM (one executed). Its 28 archived
cases/phases pass without skips; both the 706-input new manifest and 685-input legacy manifest remain exact. The
71-case legacy regression is reused only after source/XML revalidation.

The [Kafka run-root projection](m5-kafka-run-roots-projection.json) adds `OxiaKafkaRunRootAuthorityV2` through the
actual bound namespace factory. M5KR records bind the full protocol scope and stable native ledger identity. A permanent
genesis choice or a single SEALED-parent CAS selects an immutable child identity; unselected prewrites remain invisible.
The selected child's admission bit is a positive cache of that permanent choice. A fresh reader can recover a selected
pending child with one parent/genesis read, without walking lifetime history or rewriting the root. SEALED roots remain
readable after selecting a successor. An additive wire-3 retired form retains the original link and successor while
closing this catalog's read admission; retries cannot reopen it, and a retired parent without a choice fences new
successors. No proof-bound native retirement mutation writes that form yet. Existing selected roots therefore remain
readable, and this representation is not a reference-free proof or physical-delete authority. These permanent records
are not a bounded lifetime history or quota solution.
A real BK/Oxia restart fixture writes that form through a test-only native CAS. After restart, it verifies the old
root's read veto, the selected successor's continued readability, and the old ledger's still-readable physical bytes.
This verifies persistence of the representation, not production retirement admission or deletion.
The K3 lifecycle now confirms the exact stored retired marker through the native run-root authority before changing
its local state from SEALED to RETIRED. A caller's retirement permit alone cannot make this transition; a missing or
mismatched marker, or a failed authority read, leaves the local run SEALED. The restart fixture checks both sides of
this transition around its test-only CAS. This confirmation does not write the marker or prove reference freedom.
Every root mutation acquires its ledger ticket; successor selection acquires both parent and child tickets before
native verification or prewrites. The exact root and optional parent enter the context. Only irreversible native root
or selection facts reconcile prior invocations; unknown results retain tickets and observer cancellation does not stop
completion. The factory rereads both actual namespace assignments before each root operation.
`KafkaBookKeeperNativeRootVerifierV2` observes NBKE2 entry zero through an independent native handle without recovery
fencing. The header-only read bypasses reader LAC because a newly quorum-written header can still have reader LAC -1.
It validates exact native metadata, entry identity and bytes, then closes the handle; this observation is not ACK/quorum
or current-owner admission proof. Publication requires the complete header binding and configuration; SEALED publication additionally requires closed
native metadata and the exact terminal footer/end. This does not verify all DATA/index contents or current protocol
ownership. The native fixture explicitly admits synthetic OPEN resource authority after actual ledger creation; the
production root adapter never creates missing authority or expands quota implicitly. Root metadata quota, resource-birth
admission, native source-cut/membership production, ordinary reads, all writer rows and physical deletion remain OPEN.
The internal-topic cases exercise native run/header/DATA/footer/root lifecycle for `__consumer_offsets` and
`__transaction_state`; complete internal message semantics and compaction/delete acceptance remain separate.

The final root runner passes 81 executed main tasks and a separate 21-task post-restart JVM (two executed). Its nine
archived suites contain 41 cases/phases, including seven root unit cases, four native cases and two root restart phases,
with zero failures/errors/skips. Both the 713-input root manifest and 691-input legacy manifest remain unchanged. The
legacy regression was rerun on an independent cluster: 93 executed main tasks, three executed post-restart reads and
71 archived cases/phases. These focused results do not close any aggregate obligation.

The [native run-source projection](m5-kafka-run-source-projection.json) adds actual NBKE2 input capture and physical
membership. `OxiaKafkaRunRootAuthorityV2.readSelectedRoot` accepts only a canonical key on its assigned route and returns
only a selected root. `KafkaBookKeeperRunSourceV2` requires a nonempty SEALED run, acquires its physical read ticket,
opens an owned read-only session and verifies every native entry. It checks full header/binding/configuration, Kafka
body CRC/offsets/leader epoch, append-group continuity and aggregate digests, index locators against actual DATA,
checkpoint sections/bounds, terminal footer/directory and total native length. It rereads the selected root afterward.
The source identity excludes the mutable successor link and replica layout; it binds stable root bytes, closed physical
bounds and actual frame/body digests. The returned SourceExtent, record/timestamp summaries and InputBatch bodies are
derived from native bytes. Membership resolution compares the complete frozen extent and exact Binding/provider route;
it does not accept a caller's physical member map. One budget spans all sources and bounds entries, batches, records,
native bytes, retained payload and decoded record bytes. A fixed-scratch decompressed-stream preflight checks count,
lengths, total decoded bytes and exact EOF before Kafka allocates record bodies, including malformed oversized length
declarations. This does not account for every codec scratch allocation or replace complete Cell memory/I/O admission.
A failed scan returns no partial input. The owned session must drain and close before this
invocation's read ticket is released, including on parse failure; cancellation does not abandon admitted work. Unknown
close retains the ticket; unresolved ticket release also prevents a successful capture result. Old-JVM read-ticket
recovery and complete Cell admission remain required.

Raw `capture` and complete-plan `resolve` now require the caller's fixed process-local
`KafkaBookKeeperReadCellBudgetV2`. One raw scope charges one owner/read slot, the sum of its configured native-frame
and retained-payload allowances plus two maximum-size canonical run-root records, and its decoded allowance. The two
root records cover the initial selected-root read and the reread after the native scan. A single scope spans all
sequential native sessions and accumulated inputs in one `resolve`; it does not return the charge between runs.
`resolve` checks the frozen Binding and Cell before catalog reads. Standalone `capture` now requires the caller's
expected Binding and reserves its share
before the first selected-root catalog read. A wrong or unadmitted Binding cannot borrow another source's identity;
the actual selected root must still match the reserved Binding before physical admission. Unknown native
creation/close retains the charge. Confirmed session
close returns it only after the whole operation finishes, including partial input rejection or observer cancellation.
The same caller-supplied budget can admit selected-generation capture, so a held raw resolution prevents another
same-Binding capture at capacity.

When raw native termination is confirmed but its physical ticket cleanup is unresolved, a privately constructed
`KafkaBookKeeperRunSourceV2.TicketCleanupException` retains the exact operation ID, Context and local terminal proof.
Its repeatable metadata-only retry never reopens the ledger or session. A failed native close grants no retry handle.
Other metadata discovery and transport buffers run under the owner/read-slot reservation, but their bytes and queue
footprint are not charged.
Neither result-cache ownership nor backend-internal buffers are included in this configured allowance; full
provider memory, native process drain and crash-ticket reconstruction remain required.
The real BK/Oxia raw-run test withholds delivery of a completed native entry read and then of the confirmed native
close. The cancelled observer cannot discharge the ticket or Binding share at either point; a second Binding in the
same configured Cell can still read, and actual callback delivery permits exact local cleanup. No durable ACTIVE
process-drain proof, retained-result ownership or restart reconstruction is established by this fault injection.

The raw-run adapter feeds the existing ticketed BK compactor and selected-output recovery. It reports range-index
coverage separately and never declares all protocol indexes complete; the existing semantic compiler rebuilds output
indexes. Tests use user and internal-topic routes with actual generic Kafka batches. Complete native internal message
schemas, protocol key/transaction/frontier proofs and resource-birth admission remain separate. Compacted-generation,
Object and Pulsar input catalogs, ordinary read-owner integration, source lifetime/read-ticket recovery, root quota,
READ_FENCED and physical deletion remain OPEN. No aggregate obligation closes from this raw-run source path alone.

The final raw-run source runner passed 84 executed main tasks and a separate 22-task restart JVM (three executed).
Its 13 archived suites contain 51 cases/phases with no failures/errors/skips. The independent legacy regression passed
71 archived cases/phases; both the 718-input current map and 695-input legacy map were individually unchanged. The
allocation preflight also passed the 3 budget and 14 existing semantic compaction tests. Exact local source hashes,
restart identities and archive locations are recorded in the implementation log.

The next [input-plan verification slice](m5-kafka-input-plan-projection.json) closes a narrower admission gap found
while preparing compacted-generation inputs. Publication now passes the complete CompactionPlan to the input port.
The raw-run adapter compares the entire native input list, including source identities, ordinals, order and canonical
bytes, before returning physical members. Matching the SourceExtent alone cannot justify an omitted or substituted
batch. The source-cut-only overload remains a physical-membership query and is not the publication entry point.
The follow-up native run passed 51 archived cases/phases and 71 independent legacy cases/phases, including same-service
retained-data restarts. The omitted-input output created no candidate pointer or selector CAS. Both independently
captured source maps remained unchanged; exact hashes and archive identities are in the implementation log. These
checks do not supply native protocol-owner/semantic admission or complete compacted-generation source capture.

The [selected-generation input slice](m5-kafka-selected-source-projection.json) adds native capture of the current
selected BK descriptor and every data/index ledger, including an index-only generation. The new source kind appends
ordinal 4 without changing older kind encodings; older implementations reject it and no mixed-version migration is
claimed. Its canonical SourceExtent body/length/hash describe the exact immutable descriptor, while InputBatch bodies
come from independently verified native parts. A generation carries no fabricated single-ledger identity. All physical
members receive read tickets before opening; the owned session drains/closes before releasing this invocation's tickets.
The current admitted M4 selector and descriptor must be unchanged after capture. Record count/decoded bytes share one
generation budget; decoding runs on the supplied owner executor. `capture()` and complete-plan `resolve()` now use
`KafkaBookKeeperReadOwnerV2`, rather than a separate direct-session path. The constructor requires the admitted
owner's shared `KafkaBookKeeperReadCellBudgetV2`; one capture reserves one owner/read slot plus its configured
encoded/decoded allowances. Initial bounded selector/descriptor discovery identifies the Binding before reservation;
physical ticket/session/native-read work follows reservation. The same M4 planner/hazard and actual session-drain
path protects direct scoped reads and compaction input capture. Exact selector/descriptor revalidation still occurs
before returning a snapshot. Confirmed-drain ticket cleanup failures retain the exact-operation retry handle.

A held ordinary scoped reader can exhaust the same Binding share and prevent a selected-source capture from acquiring
a second physical ticket; after known native completion the capture succeeds and returns its share. Failed selected
capture cleanup can be retried without discharging a live same-Context reader. The native generation and index-only
restart cases use this same mandatory path. Raw NBKE2 run capture, complete read-population admission, retained result
caches, provider buffers and metadata-I/O budgeting remain separate required work.

Only typed zero-record generations can supply an empty compaction input list; all eight indexes and the gap root still
must verify. M5-A Object materialization, the Object-facing compactor and unguarded BK publication reject this source
kind. The selected-source resolver compares the full native extent, exact predecessor selector and every input batch.
The native fixture first preserves M4's rejection of the previous raw fallback set as
protection for the new generation, then executes exact closeFallback, protects the new generation at its current read
epoch, and completes a second physical-ticketed native publication and read. The source extent and physical members
must remain stable across the M4 closure. Old raw protection records and their active retirement batch must remain
intact: this closure is not RELEASED or deletion. Complete M4 release, full mixed raw/generation catalog admission and native
protocol-owner semantics remain required. No aggregate obligation closes from this path alone.

The shared M5 publication validator requires a new fallback protection's first epoch to equal E+1 when introducing
fallback from a preferred-only selector at E. Epoch E has no fallback closure/proof liability and must not be included.
Existing fallback identities retain their original first epochs and exact set digest. Native tests reread those
identities at both publications and after both generations' restart; the unit proof/release case uses synthetic drain
facts only. Actual M4 RELEASED and admitted native read-owner drain remain OPEN.

The current native run passed 93 executed main tasks, a separate 26-task restart JVM (six executed) and a second
16-task occupied-Cell restart JVM (one executed), with 104 archived cases/phases and no failures/errors/skips.
The independent legacy regression passed 82 cases/phases. Both the 745-input map and 730-input legacy map were
independently unchanged, including the amended descriptor checker and its negative tests. The original first-generation checkpoints remain; four separate second-generation checkpoints
now reverify the user topic, both internal topics and index-only output in a fresh JVM. The admitted Binding route
loads the current native descriptor and immutable task; the checkpoint supplies only client configuration and expected
hashes. Exact source extent, selector, all data/index parts and old PROTECTED records survive restart. Both control
clients report zero record creates and selector CAS calls, while temporary physical read tickets still use their
separate native authority route. This focused recovery does not complete protocol-owner admission or M4 RELEASED.

The [scoped BK read owner](m5-kafka-read-owner-projection.json) now composes complete physical read tickets, an owned
native session, exact durable selection and the existing M4 planner/hazard kernel. Its structured lifetime closes
local admission, waits for accepted reads and native session termination, then releases only its own tickets. The
exact M4 closure path closes local admission before CAS; unknown/conflicting closure cannot return local drain
material, and exact retry retains the old read until actual completion. Cancellation of reads, work or an outer
observer cannot bypass cleanup; an ended owner cannot issue another read or late selector transition. Native delayed
read/close verification passes. Its privately constructed local evidence is not an M4 terminal/proof. After the
native session terminates, the same owner can release its old protection through the hazard pool that admitted its
reads; a real BK/Oxia case confirms the canonical `RELEASED` version is then reread by M5 eligibility. That case
reserves the old ledger's GC quota before creating its OPEN M5 authority, then qualifies the exact next authority
revision through the bound route after native read tickets drain. It supplies the planned terminal/proof and all
other eligibility facts as fixtures. The selected raw run root is still present and no native delete intent is
created; this does not establish complete protocol-owner quiescence or physical-delete eligibility. Public `run`
requires the fixed process-local Cell reservation below. Full protocol-owner population admission, retained
result-cache/backend transport accounting and crash-ticket reconciliation remain OPEN. Existing service-restart
cases do not prove reconstruction of this owner's local drain.

`KafkaBookKeeperReadCellBudgetV2` fixes a Cell's admitted Binding shares before use. Full Binding identity includes
BindingId, incarnation and storage epoch; a changed epoch or foreign Cell cannot consume another share. At most
1,024 configured Bindings share at most 65,536 owners/read slots. The sum of reserved shares must fit the Cell hard
limit. A scope reserves one owner, its configured recovery capacity and capacity multiplied by each encoded/decoded
byte allowance before any metadata/ticket/session admission. Shares are not borrowed and admission has no waiting
queue, so an occupied Binding cannot consume its healthy sibling's reserved share. Snapshot methods observe only;
there is no public reset/release or expiry path.

Only the read-owner lifecycle can release a reservation. A failure before session creation returns it; after session
creation begins, unknown creation/close or pending IO retains it. Actual read completion plus confirmed session
termination releases the exact reservation once, including when the lifetime or outer observer was cancelled.
Physical ticket cleanup still follows its existing authority protocol. The old budget-free `run` entry was removed.
Six owner tests include exhaustion before session creation, identity/profile rejection, unknown-close retention and
pre-session failure release. Actual BK/Oxia cases verify held native read and held session-close callbacks, fixed
Binding shares, healthy same-Cell progress, and final share/ticket release after known termination.

The raw NBKE2 source now distinguishes failure to construct any session from accepted native work. A throwing or
null session supplier yields an exact no-dispatch terminal for its already acquired physical ticket, and its Cell
reservation returns without a read. Ticket cleanup uncertainty still requires exact-operation reconciliation.
The real BK/Oxia regression verifies both constructor failures leave zero tickets and share usage, then reuses the
same Binding budget for a successful native capture. This does not resolve a session whose creation or close is
actually uncertain.

The selected-generation `KafkaBookKeeperReadOwnerV2` follows the same pre-session boundary. A synchronous throw or
null from its session supplier produces an exact no-dispatch ticket terminal before marking the Cell share as
session-started. Confirmed ticket cleanup returns the original failure and lets a subsequent owner use the same
Binding share; uncertain cleanup retains the exact-operation retry handle. A focused guard/M4 unit regression
reproduces the former unresolved-ticket result and verifies the correction. The source-locked BK/Oxia run rechecks
native selected reads and restarts, but does not inject this construction failure into BookKeeper. Once a session
is returned, unknown close still retains the share and tickets.

When IO and the owned session have definitely terminated but physical ticket cleanup remains unresolved,
`KafkaBookKeeperReadOwnerV2` returns a privately constructed `TicketCleanupException`. It retains this invocation's
operation ID, complete physical membership, Context and local terminal proof, while preserving an original work
failure as its cause. `reconcileTickets()` performs metadata-only cleanup through `reconcileOperation`; it never
creates another session or reruns work. Every ticket must match both the operation ID and all Context fields.
A same-operation ticket with different authority facts remains unresolved. A live sibling sharing the Context keeps
its own distinct ticket. Each pass removes at most 256 tickets across at most 2,048 targets / 1 MiB of canonical
input; it returns unresolved targets and can be repeated. Cancelling its observer does not stop internal cleanup.
Known native termination already releases the local Cell share, independently of pending metadata cleanup.

Unknown native creation/close, incomplete IO and pre-dispatch admission failure do not construct this handle. This
is a local post-drain repair path, not recovery of a crashed owner's drain or proof that a failed close is harmless.
The existing Context-wide `reconcileTerminal` requires a native terminal covering every matching attempt; a single
reader's drain cannot authorize it. The real BK/Oxia test retains a live same-Context reader, drops the terminated
reader's ticket-release CAS, then retries only that exact operation and observes the live reader's tickets and share.

This object belongs to one admitted process-local Cell owner and must live for that owner's entire lifetime. It does
not establish uniqueness across independently created Cell-owner instances, durable process-restart budgeting,
provider-internal transport memory, retained result caches, bytes/IO rates or all other native read entry points.
Review 8.1 still requires the complete M4 quarantine/retained-buffer evidence and Binding metrics; M6 owns native
process-drain wiring. An unplanned death cannot reconstruct a local drain or clear durable tickets from this object.

`v2M5LifecycleDesignCheck` checks the amendment chain, exact bytes, coverage and authority boundaries. It proves no
runtime behavior. `v2M5HistoricalDesignCheck` replays the unmodified freeze validator at c86fde3e and compares
current frozen bytes; the original current-checkout pre-implementation gate still rejects implementation descendants.
Each implementation slice must update this table, its projection and the
[implementation log](m5-implementation-log.md) together. Earlier focused gate completion does not satisfy amended
requirements by inheritance.

M3's historical Final selected RANGE_SELECTED(RANGE_64) and implemented NWG1; M4 has an immutable Final with exact
RELEASED authority. Older design-stage prose is not a current NotStarted/undecided status. Neither historical result
is a current M5 recertification. Keep their source locks, receipts, payload bytes and exclusion boundaries intact.

V2-KAF-DATA-012/013/022 native broker/controller activation and Fetch/compaction promotion remain M6 PLANNED.
M7 owns planned operational handoff and M8 owns native parity, deployment scale and noisy-neighbor qualification.
M5 composition and bounded failure/capacity tests remain required in this task. Production deployment and the
previously paused benchmark campaign have no authorization from this amendment.
