---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: CurrentSourceReceipt
authority: NormativePlan
sourceTuple: v2-m1
---

# Implementation plan and gates

## Milestones

| Milestone | Scope | Current status | Required aggregate |
| --- | --- | --- | --- |
| M0 | V1 archive references, Context Map/glossaries, V2 ADRs/contracts, open-question/session logs, source/scenario manifests, tradeoff register, documentation gate | DocumentationGated | `v2M0Check` |
| M1 | pure V2 active graph; Java-17/JDK-only domain and exact four-capability metadata SPI; NTB1/NSE1 identities and strict NTA1 aggregate; complete Kafka API-key-32000 KRaft record/image/CreateTopics pseudo-config/resolution/sizing/projection/publication authority; Pulsar selector CAS plus authoritative ABA-safe ownership witness, gap-safe stale-install exclusion, and local atomic ACTIVE fence; compatibility-namespace Registry, complete writer-set/interlock, versioned derived slice view, and `REGISTRY_CONFORMANCE`; allocator `HARNESS_CONFORMANCE_ONLY` with no mode selection; remove every active V1 runtime/gate | **Promotion-derived: implementation and pure-V2 prune are complete; current completion is exactly the trusted N3 receipts/scenario state accepted by `v2M1FinalCheck`** | `v2M1FinalCheck` |
| M2 | Owner Epoch lane and typed frontier; Kafka BookKeeper per-partition leader-epoch-bound run chain, NBKE2 DATA/control frames, packed RecordBatch range indexes, pre-position reservation, bounded overlapping writes and fenced ordered locator/producer/transaction/leader-epoch publication; distinct Allocated/Durable/LEO/HW/LSO state, native duplicate semantics plus storage-retry digests, speculative producer deltas, profile-neutral checkpoint kernel with BK implementation, compact follower-descriptor/Observed/Applied/election-adoption primitives with hard journal/source/apply-lag eligibility, targeted floor+coverage+successor Fetch, delayed-wakeup seam, sequential cursor, bounded suffix recovery, async-Object source switch, and 10k/100k evidence; Pulsar deterministic NPD1-data/NPO1-root pair with checked 16-byte-row/streaming envelope and provider admission plus native-relative block-policy evidence; ManagedLedger-owned dual-source handle/read pins/final-delete revalidation and persisted BK_DELETE state/retention policy | **Promotion-derived: the global current-source receipt binds exact Kafka/Pulsar Final child roots and the disjoint 21-scenario union accepted by `v2M2Check`; M3 Object WAL, M6 process activation, M8 parity, and mixed/downstream rows remain excluded** | `v2M2Check` |
| M3 | one-cell NWG1 Object WAL groups; binding-context epoch authority, exact per-commit Kafka leader epoch, and commit-set co-location; run-key/per-Object AEAD; final class/lane leaf grammar and post-plan sequence allocation; up to three lazy lanes under one Root/pointer; provider-resolved physical frontier plus owner-local per-binding typed frontier; physical-only de-duplicated checkpoint rows/Seal; separate bounded Root-bound NWKCP1 Kafka protocol-checkpoint family selected by an independent publisher-fenced OPEN/TERMINAL Head; one publisher-epoch-fenced physical vector chain; pre-position tracker/locator reservation and local tickets; shared-verified range-aggregated fenced active-tail publication before ACK; Root-fixed NONE/optional bounded provider-proof mode; provider-absent cuts; conservative bounded prefix/LIST recovery with no partial skip vector; provider/session evidence; fixed-slice Pulsar virtual-ledger path with RANGE evidence | **In progress; exact `848dd2db...` ordinary source closure passes 937 module tests, 100 governance contracts, eleven-module artifact/API publication and independent consumer compile, 3 NWG1 mutation tests, 17 NWG1 wire tests, and 7 Object-WAL trace tests with zero failure/error/skip; the V2 allocator child/Final checker independently replays planner/disposition/selector semantics and retains a separate strict V1 compatibility profile; exact `9355e64a...` adds checkpoint-per-action adaptive execution and the freshly rerun allocator pre-campaign passes 254 offline tests plus 5 plan/configuration contracts with zero failure/error/skip; ADR-0107 wires one unique default-off bounded-adaptive entry and freezes the 288 interval/360 fault/32 scale/680 total action projection plus a 48,000-second process cap, but Phase A runs no campaign; the four-test real-Oxia prerequisite remains diagnostic-only and seals NADV2 `4bd6d4fe...c0cc`; the Final aggregate rejects the absent receipt; preselection C1 Provider/KMS, U/P native-reference, C2 evidence-only, and the other ordinary children are published, while allocator campaign execution, selection/child, post-selection current-source M2 and child freshness, scenario promotion, and Final remain open** | `v2M3Check` |
| M4 | manifest, protocol-position/timestamp indexes, Storage Epoch resolver, logical Binding read snapshot, bounded sharded generation-tagged hazard slots, ABA-safe lease word and terminal source drain, `ADMITTING/STOPPED` Binding selector with fused fallback-removal/E+1 cut, small inline closure-anchor set plus emergency STOPPED envelope, closed-verifier terminal publication and async batched prune, per-source first/shared-last intervals, deterministic on-demand proofs/window, exact inline/reference activation, explicit bounded O(N) reconciliation, and two-stage retirement | Planned | `v2M4Check` |
| M5 | materialization, compaction, retention, immutable capability-evidence verification, exact per-source protection release, irreversible same-key `FULL_V1 -> RETIRED_V1` batch compaction with permanent compact tombstone, retained-source/batch/tombstone admission and alerting, per-cell cache/task isolation, physical GC | Planned | `v2M5Check` |
| M6 | Kafka/Pulsar broker/controller process integration; native Kafka Produce/Fetch/Admin, replica-Fetch compact descriptor transport, durable observation journal, hard-bounded Observed/Applied ISR eligibility, native election adoption, ISR/minISR/HW/LSO, delayed-Fetch purgatory, native duplicate/error semantics, transactions/control markers, leader-epoch truncation, restart/catch-up/snapshot, and protocol compatibility evidence over the M1/M2/M3 authorities; Pulsar native process integration | Planned | `v2M6Check` |
| M7 | fencing, planned handoff, bounded recovery, cell-local drain/close isolation, mixed-profile operations | Planned | `v2M7Check` |
| M8 | scale, shared-infrastructure/noisy-neighbor chaos, exact-source AutoMQ comparison, Pulsar native parity, release evidence | Planned | `v2M8Check` and `v2FinalCheck` |

M0, M1 and M2 gates are registered, and the current M1/M2 Final evidence is complete. M3 source, governance, evidence,
and aggregate task names are now registered, but only their exact published receipts may prove completion. The
intermediate focused gates remain explicitly non-promotable; M4-M8 task names remain future contracts until
implemented.

The accepted
[M2-K0 Kafka implementation-input closure](detailed_design/m2/kafka-m2-k0-implementation-input-closure.md) fixed
the execution boundary for exact `NBKE2` wire/caps, the checked numeric model, the minimum module graph, the
Cell-scoped BookKeeper provider contract, and Kafka evidence hierarchy before implementation. Those inputs and the
Pulsar-owned slices are now executable: `v2M2KafkaInputsCheck` remains a non-promotable prerequisite,
`v2M2KafkaFinalCheck` is the Kafka child, and global `v2M2Check` binds the Kafka/Pulsar children and exact disjoint
21-scenario union. No M2 task is an empty or zero-test success. Rows shared with M3/M4/M5/M6 remain `PLANNED` until
every named milestone supplies its evidence.

The accepted [M3 detailed-design index](detailed_design/m3/README.md) and
[M3-I0 NWG1 input closure](detailed_design/m3/m3-i0-nwg1-implementation-input-closure.md) remain documentation-input
authority only. Production projection/codec, mutation runner, Object-WAL state kernel, runtime/control/provider
integration, Kafka/Pulsar paths, allocator/Oxia path, and ordinary/governance/module-API gates now exist at the exact
implementation checkpoints recorded by the M3 index. W1 also has a complete non-promotable current-source M2
regression checkpoint. D1 now has an exact-source six-test/six-record local-cap runner whose CREATE_NEW payload binds
the harness, test, and six component sources and explicitly claims no Provider transfer; it remains separate from C1
real Provider/KMS evidence. Exact preselection D1 evidence passes 12/0/0/0 at `2b9636dd...` and publishes child
`6769769b...2a78`; C1 passes 4/0/0/0 at `e4d207e3...` and publishes child `184fa0e2...a180`. Both are
non-promotable. Exact `f0a3310d...` A/B evidence passes 20/0/0/0 and publishes non-promotable child
`26c50098...e14b`, binding the canonical six-vector/two-fixture/84-record/240-path manifest, all 25 rejection codes
and 16 validation stages, the fixed ZSTD interoperability frames, and the exact 114-row TSV. Formal post-selection
A/B/D1/C1/allocator receipts, final-source W1 freshness,
scenario promotion, and Final remain open. ADR 0101 now preserves exact Cell proofs under concurrent RANGE renewal after the
failed `d819500f...` matrix selected no mode. ADR 0102 keeps the immutable 16-MiB JUnit cap after the
`1ef4f108...` full matrix passed its one testcase but failed sealing because 970,241 identical expected native-harness
cleanup WARNs expanded the exact XML to 113,519,059 bytes; that run also selected no mode and must be rerun from raw
execution at the later exact source. ADR 0103 keeps the 600-second construction cap after the `bd254d24...` rerun
failed RANGE-1024 10k-to-100k population expansion before any RANGE-1024 100k measurement row. Its diagnostic files
select nothing. ADR 0104 replaces exhaustive formal execution with a validator-proof adaptive V2 campaign while
retaining all 288 logical cells, and removes the one-JVM Cell-proof lock as formal performance authority. Its offline
`--plan-only` freezes 13/17/288 execution bounds and separate phase budgets; the interrupted
`full-matrix-16254510-r1` and all V1 products remain immutable diagnostic-only. No full formal V2 campaign may run
until its bounded runner, four-actor harness, compatibility, and short real-Oxia gates are clean. The pure domain V2
campaign schema/planner/validator/selector is now implemented: it enumerates all 288 logical cells, recomputes both
terminal-conservation equations and every disposition, drives descending native/candidate rates, and returns valid
non-promotable `NONE_QUALIFIED`/`BOTH_QUALIFIED` evaluations. Its 13 focused tests cover the 13/17/288 execution
bounds without accessing Oxia. The production-neutral SPI workflow now runs the exact STRICT or RANGE transition
chain with a stable request/descriptor identity, exact same-key rereads, typed CAS/create reconciliation, and one
global retry bound; it contains no Java lock or worker pool. Eight focused tests cover all response-loss points,
independent coordinator conflict, retry exhaustion, and owner/slice/descriptor fail-closed cuts. These are
implementation conformance, not campaign evidence. The V2 evidence source now also contains the physically bounded
four-actor Runner and candidate harness: queue capacity is exactly twice the offered rate, every actor lane admits at
most one request, queued work receives only the pre-admission drop at the frozen cutoff, and admitted work has one
fixed five-second cleanup deadline and one real terminal. The Runner exposes backlog/in-flight/waiter/terminal facts,
and the harness revalidates both conservation equations while adapting four identity-distinct instances of the same
production-neutral workflow. Seven focused contract tests cover Cell concurrency four with per-lane concurrency one,
queue overflow/cutoff, completion/failure/timeout partitioning, warm-up separation, independent actor routing, and the
absence of a Java correctness lock or worker pool. It is not real-Oxia or promotion evidence. The next offline slice
implements canonical `NACP2` checkpoint/resume, fixed
`NAEV2` evaluation, fixed diagnostic-only `NADV2`, and the exact source/attachment/JUnit promotion decision. Resume
revalidates the complete ordered prefix, predecessor digest, source/executor tuple, and non-increasing independent
budgets. Interrupted/infrastructure-failed checkpoints cannot seal; NONE/BOTH seal and reach the gate as valid
non-promotable decisions. The former default V1/full script path now refuses execution, while plan, pre-campaign,
checkpoint, evaluation, promotion, and separate short-diagnostic entrypoints are explicit. Formal actor workflows are
also capped at 64 retries, four seconds total elapsed, and 25 milliseconds maximum backoff, inside the five-second
Runner cleanup grace; a request-local lock-free store guard rejects post-timeout continuation dispatch. Promotion
independently rederives NAEV2 and rehashes both diagnostic and formal JUnit bytes;
it cannot trust a caller-constructed canonical seal. The exact `5d86b572...` short real-Oxia gate now passes its four
STRICT/installed-RANGE/range-renewal/conflict-storm tests with zero failure/error/skip and seals diagnostic-only NADV2
`9694673...d6e3`; its JUnit is `a8b0f884...55f3e`. No formal V2 campaign has run. No empty task,
documentation-only receipt, synthetic Root fixture, diagnostic
real-service run, or focused local PASS may promote an M3 scenario.
ADR 0106 now wires mutually exclusive V1-compatibility and V2 campaign profiles into the closed allocator child and
Final hierarchy. At exact `cccf16c4...`, the governed Python checker independently replays the raw adaptive campaign,
recomputes every disposition dependency and selector result, rejects fully rehashed caller forgeries, and covers exact
20-cell STRICT and minimum 17-cell RANGE_16 positive paths. The aggregate governance inventory is 100/0/0/0. This is
offline/synthetic governance only and does not create campaign or selection evidence.
At exact `9355e64a...`, a bounded adaptive executor closes the offline orchestration gap: it consumes only the
validator's next action, accounts seven independent budgets before admission, persists CREATE_NEW NACP2 after every
action/stop/failure, resumes only the exact source and ordered prefix, and never seals evaluation. Eight executor
tests raise the freshly rerun pre-campaign inventory to 254/0/0/0; the plan/configuration checker remains 5/5. No
formal task or script mode was enabled and no allocator evidence or selection was produced.
ADR 0107 closes only the next formal-entry wiring boundary. The frozen zero-decision plan now expands the unchanged
domain inventory into at most 288 interval actions, 360 single-cut fault actions, and 32 RANGE-row scale actions,
with 680 total actions and a separate 48,000-second process cap; its stable SHA-256 is
`4fbeb2d43bd5865cb6139277a5021ed1b0762223f4983fc8fa50f8edc975ff08`. Pure `--plan-only` reports these values and
the exact source tuple without touching external services or evidence. The sole default-off task is
`:nereus-metadata-oxia:realAllocatorV2BoundedAdaptiveFormalCampaign`; it requires separate exact-SHA authorization
and a clean, fully locked source/runtime tuple and new empty output before it can start. Phase A registers and tests
this path but does not invoke it. It therefore creates no NACP2/NAEV2/NARS2, allocator child, source-lock update,
scenario promotion, or Final, and allocator state remains `UNSELECTED`.
The authorized V2 Stage-B campaign later completed at exact source `6c92d937...` with final NACP2
`2a500526...e33c` and valid non-promotable NAEV2 `10fa2033...a7e5d` status `NONE_QUALIFIED`; no selection was
created. ADR 0108 preserves that evidence and corrects the next protocol's feasibility model rather than lowering any
threshold. Stage B.1 owns distinct V3 codecs/planner/proof, four-by-64 bounded async admission, native-baseline
unavailability, and diagnostic-only investigation. Its stop point is a new exact clean implementation SHA before any
V3 formal campaign; source locks, children, M2 freshness, scenarios, and Final remain unchanged.
The accepted implementation freezes V3 plan digest `019fcac7...35e9`, 328/360/32/720 action maxima, a 34,260-second
budget sum inside the unchanged 48,000-second envelope, and `4/64/256/1` admission. The initial exact-source
`baae2625...326` diagnostic-only run passed 13/0/0/0. Its Native and direct Oxia rows were healthy under the short
load, while bounded allocator rows identified CAS/reconcile contention and the installed RANGE-64 path as materially
cheaper. Stage B.1 therefore closes protocol feasibility and diagnostic observability only; it does not claim a formal
candidate qualification or authorize a workflow change that would weaken proof or SLOs.

Stage B.2 is governed by ADR 0109. It preserves the first exact-source V3 terminal
`NATIVE_BASELINE_UNAVAILABLE` and its immutable external archive, removes the four-worker admission-after Native
queue, and makes formal plus diagnostic call one true async ManagedLedger interval runtime. Publication requires the
old profile to fail `NATIVE_EXECUTOR_INFEASIBLE`, all eight 200-request/second Native baselines and two representative
rows to pass one exact-schedule diagnostic canary with actual operation concurrency above four, and current-source
NADV3 to bind the complete diagnostic JUnit XML inventory. Stage B.2 stops before any new formal campaign.
The shared schedule/profile digests are `b0e923a0...e798` and `4b11530b...d751`; their source-bound V3 plan digest is
`5f94079e...b283`, with the existing 328/360/32/720 action limits and 48,000-second hard cap unchanged. Publication
also requires canonical revalidation of the complete five-suite, 16-test NADV3 JUnit inventory.
For ADR 0114 and later current sources, ADR 0115 extends that closed inventory to exactly six suites and 17 tests by
including the formal-equivalent STRICT fixed-1000/derived-800 diagnostic; historical five-suite receipts remain
unchanged and parse-compatible.

ADR 0110 governs the next V3 formal harness correction. The `e60327ae...-r1` attempt remains immutable
`INFRASTRUCTURE_FAILED` evidence with no evaluation or selection. Candidate warm-up typed load rejection must remain
fully counted and attached but may not prevent the complete measured failed row from driving deterministic rate
descent. Unexpected warm-up failure, timeout, incomplete drain, and every measured drop/failure/timeout remain
fail-closed. A rerun requires a new exact clean pushed source, fresh executor/NADV3/preflight identities, and a new
`<source>-r1` directory; the failed attempt is never resumed or copied.

ADR 0111 governs the next formal projection correction. Exact source `c0e28f8e...` proved the ADR-0110 classifier and
then stopped before dispatching the legal derived-800 action because first-population budget detection queried that
DERIVED slot as a baseline-independent fixed rate. The 21-file failed attempt is immutable and externally archived.
Physical budget projection must now identify first setup only by FIXED ordinal zero plus the exact highest fixed
offered rate; a derived action retains its resolved planner rate and unchanged interval/cleanup charge. Publication
requires the derived-800 regression, stable `5f94079e...b283` plan digest, full current-source gates, fresh NADV3, and
fresh preflight before any new formal directory.

ADR 0112 governs the following V3 workload-entry correction. Exact source `1771b000...` completed the Native rows and
the first failed fixed STRICT row, then stopped before dispatching the planner-required derived-800 action because the
formal candidate path still called the ADR-0094 V1/V2 fixed-rate-only schedule entry. Its 21-file failed attempt is
immutable and externally archived. V1/V2 entries continue rejecting derived rates; the V3 Native and candidate paths
must both construct the exact closed fixed-plus-derived schedule through an explicit V3 entry. Publication requires
derived 800/600/400/267 schedule regressions through both paths and an unchanged `5f94079e...b283` plan digest.

ADR 0113 governs the next formal handoff correction. Exact source `ba7e313f...` proved the derived entry and completed
the first RANGE-16 10k interval, but its following fault action consumed the exact post-interval Head beside a stale
harness-local Cell snapshot because the async completion discarded `Result.exactCell`. The 26-file failed attempt is
immutable and externally archived. V3 completion must monotonically merge the exact terminal Cell before replacing
the exact Head, reject reservation/order/identity drift, and leave production CAS/reread correctness and V2 behavior
unchanged. Publication requires the completion-order regression, full current-source gates, fresh NADV3, and fresh
preflight before a new exact-source formal directory.

ADR 0114 governs the following fail-closed diagnostic correction. Exact source `ee335a8c...` completed every Native
baseline and the first validator-consumable failed STRICT action, but the derived-800 warm-up contained one unexpected
failure beside 936 exact typed load rejections. The old detail retained only the first failure overall and therefore
could not name the unexpected path. The 22-file attempt is immutable and externally archived. The runner must retain
the first unexpected summary separately, and the complete current-source diagnostic inventory must execute exact
fixed-1000 then derived-800 formal schedules on one real-Oxia population with zero unexpected warm-up failure, zero
warm-up timeout, full drain, and real concurrency above four. Classification, interval bytes, frozen plan, workload,
budgets, SLOs, dispositions, and selection rules remain unchanged.

ADR 0115 governs the NADV3 inventory closure exposed at exact source `372ca975...`. The full real-Oxia diagnostic
completed six suites at 17/0/0/0, but sealing correctly rejected the stale five-suite allowlist. The failed diagnostic,
JUnit, Gradle log, and exit status are immutable in an external read-only archive. Current-source sealing must bind
the exact six-suite/17-test inventory and still reject missing, extra, aliased, failed, errored, or skipped testcases.

ADR 0116 closes the remaining exact-name drift exposed at `bc867579...`: six suite files and 17 zero-failure tests
ran, but the allowlist named a description that differed from the emitted JUnit method. The second failed diagnostic
is independently archived. Current-source contracts bind the literal emitted identity and reject a substituted name.

ADR 0117 governs the RANGE completion handoff exposed at exact source `b9659232...`. Its first RANGE-16 interval
completed all measured requests but 26 warm-up callbacks returned another in-flight request's exact reserved Cell
snapshot and were incorrectly rejected as if their own workflow had failed to clear. The 25-file attempt is immutable
and externally archived. The population proof must remain reservation-free, ignore only a same-identity transient
reserved completion, advance only on cleared monotonic snapshots, and retain every production CAS/reread and
fail-closed interval rule. Publication requires the exact RANGE-16 formal schedule inside the six-suite/18-test
NADV3 inventory, full source gates, fresh preflight, and a new exact-source formal directory.

The exact `9f88fbfb...` 10k RANGE Cell-proof diagnostic passes one testcase with zero failure/error/skip, but remains
non-promotable. The exact `e739799f...` RANGE-1024 10k-to-100k construction-only guard then passes one testcase in
459.537 seconds with zero failure/error/skip against the locked real Oxia image and unchanged 120/600-second caps.
Its attachment set rehashes under `SHA256SUMS` SHA-256
`1161419f12ad18b6402a31c36f42f2f7571a97ecc540f217d562a075d8e85229`; it emits no selection input or receipt. It
cannot be reused by the ADR-0104 V2 campaign.

Exact `1d181dc6...` common Object-WAL state evidence passes 7/0/0/0 and publishes non-promotable child
`ed5b131e...2f4a`, binding all 50 deterministic traces and the closed 21-outcome/call-profile inventory. This child
is common-kernel evidence only; it neither substitutes for Kafka/Pulsar native evidence nor promotes a scenario.

R/K ordinary evidence inputs are now closed artifacts instead of opaque typed files. The eight-row WalRun recovery
manifest and its self-test bind the control, lazy-lane, checkpoint, Seal/successor, lineage, bounded-tail, and session
inventory. The production-codec NWKCP1 emitter binds a 324-byte immutable Object and 434-byte OPEN/TERMINAL Heads.
The child validator recomputes their rows, keys, byte lengths, SHA-256 values, Root context and state transition, and
rejects fully rehashed substitutions. Module checks pass 148/0/0/0 and 261/0/0/0; exact-source R/K receipts remain
open at that implementation boundary and neither artifact promotes a scenario or substitutes for Kafka native
evidence. Exact `35e6784d...` R evidence subsequently passes 9/0/0/0 and publishes non-promotable child
`1863f293...8f90`. Exact `fc7aa790...` K evidence then passes 5/0/0/0 and publishes non-promotable child
`864fdedc...0c7f`, binding the production-codec fixture. Post-selection R/K freshness and the separate Kafka native
child remain open at that source boundary. Exact `53361fe2...` U evidence subsequently seals the five closed Kafka
Object-WAL suites at 41/0/0/0, all 52 source artifacts, and dedicated Kafka commit `323e0351...`; its independent
JUnit/native wrappers publish non-promotable child `3dcb5e9e...52dd` with derived totals 82/0/0/0. Post-selection U
freshness remains required. Exact `4c546639...` P evidence then passes the full Pulsar offload module at 140/0/0/0,
seals its two closed suites at 46/0/0/0 with six source artifacts against dedicated Pulsar `7ff90833...`, and
publishes non-promotable child `94084eac...071c` with derived totals 92/0/0/0. Post-selection P freshness, allocator
selection, and M6 native activation remain open.

ADR 0105 versions M3 typed-evidence source locks so the current preselection state is explicitly `UNSELECTED`.
Non-allocator children may bind the exact M3 Kafka/Pulsar branches and fixed Provider/KMS artifacts in that state,
but allocator sealing and M3 Final require a uniquely qualified `STRICT` or `RANGE` mode. The mode transition changes
production source and therefore forces fresh Final-source child receipts; it cannot reuse preselection receipts.

Exact `c27c2f3f...` C2 evidence-only execution passes 1/0/0/0 and publishes non-promotable child
`b55114e4...485e`. It carries no independent benefit evidence, remains outside the production allowlist, cannot
substitute for C1, and promotes no scenario; its presence closes only the required Final child inventory shape.

ADR 0015 limits 0.2 to one initial Storage Epoch per Topic Incarnation and no online profile-transition runtime. ADR 0016
excludes Kafka/Pulsar Access Projection and Migration Link runtime. Their future state machines, Pulsar
BookKeeper/Object profile transition, and KoP therefore remain outside the 0.2 implementation plan; their deferred
questions do not block this release.

## Milestone delivery contract

Every implementation milestone must land one coherent set:

1. production and test implementation;
2. the affected normative V2 contract, ADR/context language, and open-question update;
3. scenario Markdown and JSON statuses;
4. tradeoff status or mitigation changes;
5. source-lock changes, when an external or fork source changed;
6. ordinary deterministic gate;
7. exact-source receipt for any `Verified` or performance/parity claim.

M1 finishes with no compilable, publishable, or runnable V1 graph. The transition first lands domain/SPI and dependency
checks, then cuts old settings/BOM/publication/CI edges, then removes the V1 runtime and Phase/F9 tasks/scripts in
separate mechanical commits. Architecture, gate rewiring, and the large deletion are not one review. Nothing is marked
deprecated or retained as a compatibility shim; protected `v0.1`/`v0.1.0` history preserves the old product line. KoP
runtime leaves the active graph while its design documents remain.

M1 began with the explicitly partial M1.1a-A foundation: Java-17/JDK-only modules, bootstrap identities,
ProtocolKind/NPC1/NTI1/NTB1/NSE1 and authority-leaf codecs, minimal independent aggregate domain types, four closed
metadata capabilities, dependency/API boundaries, deterministic tests, and reproducible JAR/source-JAR/POM hashing.
The separate Oxia client-continuity target is complete at final fork `091a42c`, and metadata-oxia O2 is locally
verified at Nereus `050f908a` with its immutable client/API bundle, four single-key adapters, continuity scaffold,
69 focused tests, 299 whole-module tests, and `promotionEligible=false` receipt. Those slices by themselves did not
implement or activate NTA1, P1, or R1. M1.1b-Q1 readiness evidence is complete at `94881e67`: 14 test-scope tests measure
the 4-KiB and 16-KiB per-name candidates, 8,397/32,973-byte checked parser caps, all six legality rows, and strict
allocation boundaries. Its result is `READINESS_EVIDENCE_ONLY`, `promotionEligible=false`; M1.1b still owns its strict
production codec/goldens under the now accepted table: `NONE={0,0,empty}` and
`ZSTD_FAST_IF_SMALLER_V1={1,1,empty}`, six legal rows, classic `persistent://`, 4,096 UTF-8 bytes per canonical Pulsar
name, and exact checked caps `54/8,214/8,397`. Q1 remains historical/non-promotable. The production codec, exact
goldens, pure-input inventory boundary, and O2 aggregate adapter are exact-locally complete at `01a70f17`. The
[implementation receipt](evidence/v2-m0/m1.1b/README.md) binds 55 domain, 73 focused O2, and 303 whole metadata-oxia
tests with zero failure/error/skip and `promotionEligible=false`. Selector/Registry codecs, K1/P1/R1, real
Oxia/Registry conformance, exact-source aggregation, scenario promotion, and M1 Final were completed by their later
M1 slices and canonical N2/N3 evidence; the historical receipts in this paragraph remain non-promotable. Real
existing-cluster inventory is deferred migration evidence; fresh-only 0.2 requires the pure-input tool and boundary
tests, not a customer-cluster execution.

## M1 implementation and promotion contract

The M1 module dependency is `nereus-domain <- nereus-metadata-spi <- nereus-metadata-oxia`. Domain is Java-17/JDK-only
and contains canonical values, deterministic IDs, and validators. SPI contains exactly aggregate Publisher/Reader,
Pulsar generation Selector Store, and Pulsar virtual-ledger Registry Store, with ADR 0082's closed create/CAS outcomes.
One `VersionedAggregateSnapshot` supplies Binding/initial-Epoch projections; child authorities and generic metadata
facades are forbidden. Kafka `:metadata` consumes the exact immutable domain JAR/POM without transitively importing
SPI/Oxia and implements none of these SPIs; its generated physical record remains Kafka-owned. Published domain
artifacts are source-qualified by JAR/POM SHA and may not be overwriteable SNAPSHOT/changing/composite inputs.

Kafka feature 2, API-32000, atomic CreateTopics aggregate, input-only pseudo-config resolution, read-only config
projection, TopicImage/Delta/replay/snapshot/remove, and publication validation form one activatable M1 source tuple. A
generated record may land dormant but cannot be advertised, formatted, or emitted alone. M6 owns complete
broker/controller process and Produce/Fetch/Admin/restart evidence only. Ordinary delta validation is touched-topic-
only without canonical SHA recomputation; full scans are bootstrap/snapshot/full-catch-up only; CreateTopics and
validateOnly apply the stock request-wide partition guard and then request-order greedy pre-admission of the exact final
cumulative record count and serialized Raft-batch bytes while preserving native per-topic partial-success behavior.
Duplicate pseudo-configs are last-wins, the production native validator runs after pseudo removal, and
`CreateTopicPolicy` sees only native configs. The candidate list includes actual native configuration-derived records, including applicable `ClearElrRecord`; the
linear sizer shares effective Raft limits, and a current-batch miss is re-estimated with fresh-batch offset deltas before
the final append may reject it. V2 admission requires `remote.log.storage.system.enable=false`; M1 tests the interlock
and M6 proves RLMM remains inactive.

Pulsar M1 builds the first witness candidate only for Oxia 0.9.0-backed MetadataStore ELM: pinned source proves direct
GET/Stat/versioned-CAS primitives, not a complete adapter. M1 adds acquisition fields/transitions, a qualified provider
lifecycle/gap hook, and one closed writer kernel, with syncer disabled and all ownership writers upgraded. It arms gap-
safe invalidation, captures authoritative ABA-safe native ownership witness A/B around exact selector/aggregate
validation, and installs only by CAS from the same invalidation sequence. Legacy/TableView/mixed/third-party backends
missing acquisition identity, authoritative read, ordered loss hook, or reconnect-gap invalidation fail V2 admission
closed. Ordinary access captures/rechecks one atomic fence word with zero remote metadata I/O. Full
aggregate-to-retired-tombstone replacement remains M5; complete process integration remains M6.
The provider hook exports only a local store-wide `WatchContinuityEpoch` plus ready barrier; provider internal
connection/session/shard identities are not persisted. A gap invalidates all store fences and triggers bounded,
coalesced A/read/B recovery. The accepted O1 client fork uses the confirmed server base's existing no-start-offset dummy
notification batch as the ready barrier and adds no server wire/RPC; a gap discards the old offset and obtains a new
barrier. Source-lock schema v2 keeps the confirmed implementation/conformance bases and final fork/artifact/server-
image identities distinct; canonical N2/N3 evidence now binds and verifies that exact tuple.

M1 implements the mode-independent virtual-ledger Registry bound to the immutable 32-byte ledger-ID compatibility
namespace `SHA-256(NLI1 || u32be(36) || canonicalInstanceIdAscii[36])`. Only an exact lowercase canonical non-zero
36-byte UUID from a root authoritatively absent immediately before init is
admitted; format/changed identity is not cleanup proof. V2 admission requires exactly one selected Registry, one bounded
inline canonical writer commitment, and an ACL/credential/deployment interlock with independently revocable writers;
there is no external membership reference. Allocators use a namespace-bound versioned derived slice view. Its real-
Oxia receipt is `REGISTRY_CONFORMANCE`. STRICT/RANGE candidate SPI and cut injection remain evidence-only and emit a distinct
`HARNESS_CONFORMANCE_ONLY` receipt with `selectionEligible=false`; M1 runs deterministic/small smoke only and neither
persists nor activates a mode. M3 owns 10k/100k multi-broker capacity evidence and selection.

The Registry closes writer kinds to native BookKeeper and Nereus virtual-ledger ID allocation. Each cohort uses a
120-byte canonical row; `RegistryAdmissionEvidenceV1` is bounded immutable activation proof, not allocation authority.
The accepted [M1.1c-R0 capacity-spike design](detailed_design/m1/m1.1c-registry-capacity-spike.md) now fixes the
test/evidence method and `v2M1RegistryCapacityCheck` now reproduces 18 clean focused tests plus deterministic
JSON/Markdown at Nereus `03d27256`. Two writer kinds, full binary-by-credential overlap, rollback, fenced residue, and
optional allocation-capable bootstrap/admin cohorts derive the accepted `maxWriterCount=14`. A 184-byte fixed
accounting header, the existing 120-byte writer row, and unchanged 192/256/65,536 assignment/envelope limits derive a
51,016-byte largest legal value and 14,520-byte reserved margin. This closes the R1 capacity input only. R0 leaves the
O2 Registry codec unavailable and cannot emit `REGISTRY_CONFORMANCE`.

The accepted [M1-2 receipt/parser-cap design](detailed_design/m1/m1-2-receipt-parser-caps.md) is verified at Nereus
`75593faf` by 36 clean focused tests and deterministic
[readiness evidence](evidence/v2-m0/m1-2-receipt-caps/README.md). Eleven named sample families bind the measured
pre-M1-2 Foundation/O1/O2/NTA1 and Registry inputs, representative all-pass, multi-scenario/multi-suite,
maximum-failure, fault-cut, exact Registry attachment, and sanitized-log shapes. ADR 0084 is the sole normative cap
table; the JSON is its machine projection and binds SHA-256
`2197c814dc887d742cdda119f4e68c4f5f2276df0f44b15de3d524a2445c692d`. This closes only G1's persisted-v1 numeric
input. It adds no production parser/constant authority, N1/K1/P1/R1, real Oxia/Registry conformance, N2/N3, scenario
promotion, or M1 Final.

N1 follows the accepted [immutable artifact design](detailed_design/m1/n1-immutable-domain-artifact.md). It uses a
clean pushed Nereus source commit, the exact `0.2.0-n1.<40-hex-source-SHA>` coordinate, two byte-identical clean
builds, and an absent source-SHA repository directory. A later evidence-only binding records the binary JAR,
source JAR, POM, Gradle metadata, byte lengths, and SHA-256 values. N1 is now verified from source `330aaec3` with
manifest `9058ff01` by `v2M1N1ArtifactCheck`; it remains an input milestone rather than M1 PASS.

K1 follows the accepted [Kafka KRaft metadata-authority design](detailed_design/m1/k1-kafka-kraft-metadata-authority.md)
and consumes only that exact N1 artifact. It is focused-exact complete at the clean pushed Kafka commit `8afbc42566`:
feature 2, API 32000, direct domain mapping, CreateTopics/image/publication authority, and Admin projection pass 39
tests in 16 suites under `v2M1K1FocusedCheck`. The receipt remains `K1_FOCUSED_ONLY` and
`promotionEligible=false`; K1 does not own Produce/Fetch, M6 process activation, scenario promotion, or M1 PASS.

`docs/v2/source-locks.json` is the sole expected-SHA authority for external Kafka/Pulsar/Oxia sources. Checkout paths
may be overridden; expected SHAs may not. The manifest cannot self-lock the current Nereus commit; a promotion receipt
binds it. M1 gates are:

N2 sets `sourceTupleId=v2-m1`. `focusedEvidenceSourceTupleId=v2-m0` is a provenance label only for immutable N1/K1/P1/
R1/G1 and readiness inputs created before N2; it cannot substitute for the final receipt source tuple. N2/N3 receipts
bind the exact N2 Nereus commit and SHA-256 of the `v2-m1` source-lock bytes instead of relabelling those inputs.

- `v2M1FoundationCheck`: current partial domain/SPI unit, golden, API/dependency, documentation, and artifact-hash gate;
  it cannot prove backend/runtime conformance, the pure final graph, or M1 PASS;
- `v2M1Nta1CodecCheck`: exact-local production NTA1/validator/goldens/inventory/O2-aggregate gate; it uses no Docker
  and cannot prove K1/P1/R1, real conformance, runtime activation, scenario promotion, or M1 PASS;
- `v2M1RegistryCapacityCheck`: current deterministic R0 writer-count/canonical-byte readiness gate with 18 clean tests,
  exact generated evidence equality, source/digest binding, and production/scenario absence checks; it cannot prove R1,
  real Oxia, allocator selection, `REGISTRY_CONFORMANCE`, or M1 PASS;
- `v2M1ReceiptCapsCheck`: current deterministic M1-2 readiness gate with 36 clean focused tests, generated/committed
  byte equality, JSON/source/source-lock/digest binding, formula recomputation, production-absence and non-promotion
  checks; it cannot prove the G1 production validator, N1/K1/P1/R1, N2/N3, scenario promotion, or M1 PASS/Final;
- `v2M1N1ArtifactCheck`: exact source-qualified domain/SPI bundle, two-build reproducibility, POM/Gradle-metadata
  dependency boundary, artifact digests, and non-promotable receipt; it cannot prove K1/P1/R1 or M1 PASS;
- `v2M1K1FocusedCheck`: exact clean Kafka fork, immutable N1 input, generated API inventory, 39 exact zero-skip tests,
  dependency/runtime-scope checks, and a non-promotable K1 receipt; it cannot prove P1/R1/G1, V1 prune, scenario
  promotion, broker data-path activation, or M1 PASS;
- `v2M1P1FocusedCheck`: exact clean Pulsar fork, immutable N1/P1/O1 inputs, 100 Nereus metadata tests, two real-Oxia
  tests, 36 Pulsar tests, native capability/runtime-scope checks, and a non-promotable P1 receipt; it cannot prove R1/G1,
  full BrokerService/PersistentTopic data-path activation, V1 prune, scenario promotion, or M1 PASS;
- `v2M1R1FocusedCheck`: exact N1/O1 inputs, 35 domain tests, eight metadata tests, two source-locked real-Oxia tests,
  held writer-interlock and immutable-evidence cuts, closed response-loss outcomes, derived-view binding, and allocator-mode
  absence. Its focused wrapper names `REGISTRY_CONFORMANCE` but is non-promotable; it cannot select an allocator, prune
  V1, promote a scenario, or claim M1 PASS;
- `v2M1G1ValidatorCheck`: 49 production receipt/Final tests and 14 evidence-only allocator-cut tests, strict reference
  verification, exact virtual-ledger scenario/kind policy, no gate rerun, and no allocator candidate in production; it
  cannot prove the pure-V2 graph, exact final source tuple, scenario promotion, N2/N3, or M1 PASS;
- `v2M1Check`: no Docker/fork/composite; local domain/schema/SPI/codec/harness, active-graph, and V1-absence checks;
- `v2M1ExactSourceCheck`: clean exact forks before/after, isolated immutable artifacts, real Oxia, and focused fork tests;
- `v2M1EvidenceFreshnessCheck`: clean current checkout, exact source-lock digest, strict tested-source ancestry, and
  only linear `docs/v2/evidence/v2-m1/n3/` descendant commits;
- `v2M1FinalCheck`: require freshness, then aggregate previous outcomes and receipt schema without rerunning suites.

Zero tests, skipped mandatory tests, failure, dirtied/changed source, stale evidence, or digest mismatch fails. PR CI
runs the fast gate; trusted promotion requires the protected `v2-m1-promotion` environment and `nereus-v2-m1` runner
to regenerate and byte-compare the complete seven-file gate/report/receipt/Final set before Final. Promotion uses N1
foundation, P1/K1 fork commits, N2 source-tuple/final execution,
then evidence-only N3. Virtual-ledger conformance payloads use one strict RFC-8785/JCS envelope with the closed kinds
`REGISTRY_CONFORMANCE | HARNESS_CONFORMANCE_ONLY`; this is not the universe of all M1 evidence kinds. Each receipt binds
N2/P1/K1, source-lock digest, domain JAR/POM SHAs, Oxia server image plus client/test identities, closed receipt kind,
one `scenarios[] -> suites[]` result hierarchy and normalized `discovered/executed/passed/failed/skipped/aborted`
counts. Derived summaries cannot become another authority. Internal retries/dynamic tests are forbidden and mandatory
PASS has non-zero execution with no failure/skip/abort. Attachments are allowlisted regular files referenced by kind,
safe sorted POSIX-relative path, length, and SHA-256 rather than embedded. The canonical root contains exactly
`schema/kind/sourceTuple/scenarios/attachments`; canonical bytes supply content identity, with no `runIdentity`, leaf
IDs, or separately authoritative aggregate result. Attachment kinds are `TEST_REPORT`, `REGISTRY_BYTES`,
`REGISTRY_ADMISSION_EVIDENCE`, `WRITER_INTERLOCK_SNAPSHOT`, and `SANITIZED_LOG_EXCERPT`. A Registry scenario requires
`REGISTRY_CONFORMANCE`; allocator cut scenarios require `HARNESS_CONFORMANCE_ONLY` and `selectionEligible=false`.
Cross-M1 scenario rows are split before promotion so future evidence cannot be borrowed. N3 may change only receipts,
attachments, and their exactly covered scenario status/index; it may not modify code, gates, workflows, ADRs, or source
locks. The Final index is a typed path/length/SHA promotion manifest, not another result authority. Evidence-derived
root/count/path/file/total/log caps are now accepted in ADR 0084 from M1-2 evidence. The production G1 validator has
focused current-source evidence and the pure-V2 graph prune is complete. Trusted N2/N3 state is represented only by
the canonical gate results, receipts, Final index, and scenario manifest; neither M1-2 nor the G1 focused wrapper can
stand in for Final.

## Status model

Normative documents carry:

- `designStatus: Accepted | Proposed`;
- `implementationStatus: NotStarted | InProgress | Verified`;
- `evidenceStatus: NotRun | DocumentationOnly | CurrentSourceReceipt`;
- `sourceTuple` pointing to the structured lock.

`Verified` requires `CurrentSourceReceipt` and a non-empty receipt path. Historical or focused evidence must use a
lower status.

Scenario statuses are `PLANNED`, `IMPLEMENTED_NOT_RUN`, `PASSED_CURRENT_SOURCE`, `FAILED`, or
`BLOCKED_ENVIRONMENT`. Moving a row to `PASSED_CURRENT_SOURCE` requires the exact product/fork/baseline tuple and
receipt to be present in the same change.

## M0 gate

`v2DocumentationCheck` validates required files and front matter, Context Map/glossaries, accepted/superseded ADR
state, the three-profile vocabulary, typed-position, Storage Epoch, and cell-scoped Provider contracts, JSON structure,
source/archive identity, tradeoff IDs, scenario synchronization, local Markdown links, and receipt/status consistency.
`v2M0Check` is the M0 aggregate and is wired into CI.

The legacy Phase 3, Phase 4, and BookKeeper documentation checks remain available for V1 implementation evidence, but
their literal requirements no longer govern the V2 overview/index.

## Final evidence

M8 evidence is append-only under `docs/v2/evidence/<source-tuple-id>/`. The receipt records:

- exact clean product, Kafka, Pulsar, AutoMQ, BookKeeper/provider, and client identities;
- environment, object request accounting, durability/replication, dataset, warmup, duration, and thresholds;
- scenario results with no missing, duplicate, or silently skipped mandatory rows;
- artifacts and checksums required to reconstruct the run.

The acceptance baseline fields in [source locks](source-locks.json) intentionally remain `NOT_PINNED` at M0.
