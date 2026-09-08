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
| Kafka compaction | Shared semantics; Object or sealed BK carrier; internal topics remain BK_ONLY | Shared semantics, inventoried BK parts and sealed descriptor publication/recovery now compose real Oxia control with real BK under the existing M4 planner and hazard kernel; a native SPI profile fences late creates; same-selector decisions and native writer drain now produce restart-stable cancelled task terminals, and sorted input/output tickets wrap the guarded native publisher; native run roots now have an exact-header/seal adapter; complete namespace/task/protocol-owner admission, ordinary reads, internal-topic lifecycle and cleanup remain OPEN |
| Binding retirement | Bounded active selector plus authenticated immutable history | M5R1 wire 2 binds history root/count and monotonic activation ordinals; additive wire 3 preserves that history while carrying one bounded task decision until exact permanent archival; a configured native M4/history route now passes source-locked Oxia continuation/restart checks on the existing M4 selector key; unique native namespace/Binding assignment, quota/restart accounting, Cell I/O/metrics and physical GC worker scheduling remain OPEN |
| Permanent delete history | Compact done at the same resource key; resident cap independent of lifetime history | M5DC V2 retains exact resource/attempt/revision/owner/capability/absence identities; native Oxia compaction/cache recovery and a configured durable quota route pass; pre-reserved intent writes continue at exhaustion and pending grants/refunds recover after restart; unique namespace/all-writer admission, backend disk provisioning and GC scheduling remain OPEN |
| Writers and recovery | Target-relevant tickets, local pins, READ_FENCED takeover, current-owner intent/done | In-memory READ_FENCED refresh/takeover now preserves admission fencing and rejects stale observation epochs; sorted multi-resource tickets now wrap the guarded native BK publication path; complete native input membership, owner adapters, intent capability refresh, visible durable veto and [concrete writer matrix](m5-lifecycle-writer-matrix.md) remain OPEN |
| Evidence | Five M5-E children plus amended [acceptance matrix](m5-lifecycle-acceptance.json) | No revised source-bound M5 children or aggregate Final; scenario promotion remains unauthorized |

The [physical identity projection](m5-physical-resource-identity-projection.json) and
[eligibility projection](m5-delete-eligibility-projection.json) record the current focused results:
`v2M5DeleteEligibilityCheck` passed 42 tasks at the typed-eligibility slice, including 7 identity, 13 authority,
12 coordinator, 10 eligibility and 184 existing retention tests. The later
[observation recovery projection](m5-read-fenced-recovery-projection.json) adds 7 coordinator cases and requires a
native verifier that is unsupported by default. These results remain non-promotable and use synthetic eligibility facts.
`v2M5ReadFencedRecoveryCheck` passed 44 tasks with the expanded 19-test coordinator suite and 3 recovery contract tests.
The [shared Kafka semantic projection](m5-kafka-semantic-core-projection.json) tracks the compiler extraction and
complete row validation. Its outputs are in memory and do not authorize publication, read adoption or input deletion.
`v2M5KafkaSemanticCoreCheck` passed 42 tasks, including 14 Kafka and 7 Object materialization tests. The added
interleaved-transaction case fixed producer-independent aborted marking and uses a V2 plan/output task identity.
The [BK carrier projection](m5-bookkeeper-compaction-carrier-projection.json) adds immutable task/part inventory,
native ID reservation, bounded retained-batch/index chunks, native fencing and complete entry/metadata verification.
That writer slice requires expected output bodies for part reconciliation. The subsequent
[descriptor projection](m5-bookkeeper-descriptor-projection.json) adds KBSD2, exact M4 selection and independent
read-only BK recovery without expected output or old-source bodies. That descriptor slice did not supply native
namespace/task authority, M4 read-source-plan admission, real Oxia control, internal-topic lifecycle or physical deletion;
the later integration results below identify which of these paths now have focused execution.
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
readable after selecting a successor. These permanent records are not a bounded lifetime history or quota solution.
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
