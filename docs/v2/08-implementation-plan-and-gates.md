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
| M3 | one-cell NWG1 Object WAL groups; binding-context epoch authority, exact per-commit Kafka leader epoch, and commit-set co-location; run-key/per-Object AEAD; final class/lane leaf grammar and post-plan sequence allocation; up to three lazy lanes under one Root/pointer; provider-resolved physical frontier plus owner-local per-binding typed frontier; physical-only de-duplicated checkpoint rows/Seal; separate bounded Root-bound NWKCP1 Kafka protocol-checkpoint family selected by an independent publisher-fenced OPEN/TERMINAL Head; one publisher-epoch-fenced physical vector chain; pre-position tracker/locator reservation and local tickets; shared-verified range-aggregated fenced active-tail publication before ACK; Root-fixed NONE/optional bounded provider-proof mode; provider-absent cuts; conservative bounded prefix/LIST recovery with no partial skip vector; provider/session evidence; fixed-slice Pulsar virtual-ledger path with RANGE evidence | **CLOSED / hard-frozen. Exact common tested source `e5e53e62865c21845621037bea5f18c092bd4259` binds `RANGE_SELECTED(RANGE_64)`, eleven child receipts, 26 promoted scenarios, and immutable Final SHA-256 `81c7004a923e5b96cab0a3c8b4f1fa26d71606a2208bbabe779f0d872f84f84a`. M6 process activation and M8 native parity remain excluded.** | `v2M3Check` |
| M4 | allocation-free Binding-scoped logical `BindingReadViewSnapshot`; deterministic typed protocol/profile source plan and one-shot pre-observability fallback; bounded generation-tagged hazard slots with stable conservative scanning and ABA-safe terminal drain; fused selector/terminal/proof-window/fold/capability control; exact per-source interval verification and protection-generation release CAS; four-child evidence hierarchy; no physical deletion | **CLOSED. Exact tested source `595c8b34779d1e88187eb0084bf18e65ab2dd742` binds four children, the evidence-selected physical/capability choices, five M4-only scenario promotions, and current immutable Final SHA-256 `31235c738400c71252e1c1c923aabda6f66545767b01c20962c0a881303e1b07`. M5 physical deletion, M6 process activation, M8 native parity, and production deployment authority remain excluded.** | `v2M4Check` |
| M5 | deterministic materialization with Object-WAL reuse/index-only/rewrite selection; immutable generation and selector publication; Kafka-semantic compaction plus complete index rebuild; typed logical retention and exact reference-free proof; consume exact `RELEASED`; irreversible same-key `FULL_V1 -> RETIRED_V1` batch compaction and final Pulsar aggregate tombstone; final provider/source revalidation; per-Cell admission/isolation; conditional Object, root/data/multipart, BookKeeper, orphan, and GC execution | **IMPLEMENTATION IN PROGRESS. Design commit `c86fde3e` is hard-frozen. M5-A materialization/publication passes `v2M5MaterializationCheck`; M5-B real Kafka magic-v2 semantic rewriting and complete index rebuild passes `v2M5KafkaCompactionCheck`; M5-C Binding/Pulsar permanent metadata retirement passes source-locked real Oxia execution in `v2M5RetentionRetirementCheck`. Focused M5-D version-matched Object, exact sealed-ledger BookKeeper, pure orphan/per-Cell admission, Pulsar root-before-data ordering, and exact multipart cores pass non-promotable gates. ADR 0147 accepts a target-scoped physical-delete authority with closed writers and exact same-key CAS, but its implementation is NotStarted and it grants no intent/done authority. The complete M5-D gate and all five source-bound evidence children remain NotRun; all 17 M5 rows remain `PLANNED`, and physical deletion is not authorized.** | future `v2M5Check` |
| M6 | Kafka/Pulsar broker/controller process integration; native Kafka Produce/Fetch/Admin, replica-Fetch compact descriptor transport, durable observation journal, hard-bounded Observed/Applied ISR eligibility, native election adoption, ISR/minISR/HW/LSO, delayed-Fetch purgatory, native duplicate/error semantics, transactions/control markers, leader-epoch truncation, restart/catch-up/snapshot, and protocol compatibility evidence over the M1/M2/M3 authorities; Pulsar native process integration | Planned | `v2M6Check` |
| M7 | fencing, planned handoff, bounded recovery, cell-local drain/close isolation, mixed-profile operations | Planned | `v2M7Check` |
| M8 | scale, shared-infrastructure/noisy-neighbor chaos, exact-source AutoMQ comparison, Pulsar native parity, release evidence | Planned | `v2M8Check` and `v2FinalCheck` |

M0, M1, M2, M3, and M4 are closed by their respective aggregate evidence. M5 detailed design is hard-frozen and its
M5-A, M5-B, and M5-C implementation gates are complete and non-promotable; M5-D and current-source evidence have not
run. M3's current closure is the immutable e5 Final
identified below; its historical diagnostics and earlier Finals remain history rather than alternate current
authority. The [M4 index](detailed_design/m4/README.md) and
[M4-A read-view authority](detailed_design/m4/m4-a-read-view-authority.md) and
[M4-B typed source plan](detailed_design/m4/m4-b-source-plan-and-fallback.md), followed by
[M4-C hazard/reclamation races](detailed_design/m4/m4-c-hazard-slot-reclamation.md) and
[M4-D evidence ownership/freeze](detailed_design/m4/m4-d-evidence-ownership-and-freeze.md), hard-freeze the design
boundary without claiming implementation, scenario promotion, receipt, or Final. The later M4 implementation and
evidence tooling do not amend those frozen inputs. `v2M4DesignCheck` remains explicitly non-promotable;
`v2M4Check` is authoritative after its four exact-source children and current immutable Final are published and
synchronized.

The [M5 index](detailed_design/m5/README.md),
[M5-I0 implementation-input closure](detailed_design/m5/m5-i0-implementation-input-closure.md),
[M5-A materialization/publication](detailed_design/m5/m5-a-materialization-and-manifest-publication.md),
[M5-B Kafka compaction](detailed_design/m5/m5-b-kafka-compaction-and-index-rebuild.md),
[M5-C retention/metadata retirement](detailed_design/m5/m5-c-retention-reference-free-and-metadata-retirement.md),
[M5-D physical delete/orphan/GC](detailed_design/m5/m5-d-physical-delete-orphan-and-gc.md), and
[M5-E evidence/freeze](detailed_design/m5/m5-e-evidence-ownership-and-freeze.md) close the design boundary only.
`v2M5DesignCheck` validates their exact bytes and unchanged predecessor/scenario/open-gate state. Its result is
`DESIGN_FROZEN_IMPLEMENTATION_NOT_STARTED`; it is not a child receipt, scenario PASS, Final, or deletion authority.
Because a later M5 design commit cannot truthfully be recertified as the older M4 tested source, the design aggregate
uses `v2M5HistoricalM4DependencyCheck` to reparse the immutable M4 Final/children/scenarios and prove closure ancestry;
it does not weaken or replace current-source `v2M4Check`.

## Design-frozen milestone: M5

| Item | Frozen value |
| --- | --- |
| Frozen predecessor | M4 Final at tested source `595c8b34779d1e88187eb0084bf18e65ab2dd742`, SHA-256 `31235c738400c71252e1c1c923aabda6f66545767b01c20962c0a881303e1b07` |
| Design phases | I0; A materialization/publication; B Kafka compaction/indexes; C retention/reference-free/metadata retirement; D physical delete/orphan/GC; E evidence/freeze |
| Mutable read authority | existing selector key upgraded to the single-Binding retirement authority; deterministic M4 selector projection; no second pointer or batch key |
| Materialization modes | `REFERENCE_REUSE`, `INDEX_ONLY_GENERATION`, `REWRITE_GENERATION` |
| Release boundary | exact M4 protection key/generation/value in `RELEASED`; no inferred substitute |
| Metadata retirement families | Object-WAL batch `FULL_V1 -> RETIRED_V1`; Pulsar full aggregate to permanent incarnation tombstone after physical cleanup |
| M5-promotable rows | exact 14-row set frozen in M5-E, still `PLANNED` here |
| M6-deferred shared rows | `V2-KAF-DATA-012/013/022`, still `PLANNED` |
| Open gate | `V2-OPEN-READ-15` remains active for optional tombstone deletion; 0.2 permanent tombstones are frozen |
| Design result | `DESIGN_FROZEN_IMPLEMENTATION_NOT_STARTED` |
| Accepted M5-C amendment | ADR 0146 plus `m5-design-amendment-1.json`: durable ticket/fence serialization and exact single-key CAS; M5-C implementation complete, source-bound child not run |
| Accepted M5-D amendment | ADR 0147 plus `m5-design-amendment-2.json`: one permanent target-scoped physical-delete authority, complete writer closure, and two exact same-key CAS transitions; implementation `NotStarted`, no dispatch or delete authority |
| Exclusions | full M5-D state machine/cleanup, M5 evidence/Final, physical deletion authority, M6/M7/M8, tombstone deletion, allocator-orphan GC, production authority |

Implementation must proceed A, B, C, D, then current-source evidence. The detailed design freezes semantic state,
identity, ordering, veto, module, and ownership rules; exact codec offsets/caps, finite budgets, real Provider delete
capability, and performance thresholds remain later implementation/evidence selections inside that envelope.

The current [M5 implementation log](detailed_design/m5/m5-implementation-log.md) records the M5-A, M5-B, M5-C, and focused M5-D descendants
without changing the immutable freeze. The additive
[single-Binding retirement authority amendment](detailed_design/m5/m5-c-single-binding-retirement-authority-amendment.md)
is accepted and source-bound separately. `v2M5RetentionRetirementCheck` now proves its exact one-key path against the
source-locked real Oxia server, but remains an implementation-only result rather than a source-bound M5 child.
The additive [target-scoped physical-delete authority amendment](detailed_design/m5/m5-d-target-scoped-physical-delete-authority-amendment.md)
and ADR 0147 are chained by `m5-design-amendment-2.json`. They accept only the M5-D one-target linearization
replacement; the authority record, closed-writer wiring, real Oxia execution, external dispatch/recovery, and evidence
remain unimplemented at this boundary.
`v2M5MaterializationCheck`, `v2M5KafkaCompactionCheck`, and `v2M5RetentionRetirementCheck` are deliberately
non-promotable: they prove production source shape and deterministic implementation execution, but none is a future
real-boundary child receipt or the M5 Final.

M5-D has started with the focused `v2M5VersionMatchDeleteCheck`: the shared Provider transport defaults to unsupported,
and only an enabled versioned bucket admits exact-version delete. Fixed-digest MinIO proves exact deletion,
same-key-recreation protection, and lost-response LIST/full-GET reconciliation. This is not the full M5-D gate and
cannot create intent, claim `DELETE_DONE`, or authorize physical deletion.

The next focused `v2M5BookKeeperDeleteCheck` binds BookKeeper 4.18.0 client/source and the fixed-digest server image,
captures only closed ledgers with an exact metadata fingerprint, rejects stale or rebound targets before dispatch,
and requires an authoritative metadata no-such-ledger result after every delete response. It is an execution adapter,
not dispatch authority: the complete M5-D persisted intent/fence and Pulsar external execution remain outstanding.

`v2M5OrphanAdmissionCheck` then closes the pure six-class orphan taxonomy and authority-time mark/grace/rescan rules.
Only physical output, multipart residue, and exactly released sources may reach a future-intent candidate; permanent
metadata fences and allocator no-reuse evidence remain permanent, while foreign/unknown identity quarantines. Hard
per-Cell inventory, queues, concurrency, rates, memory, scanner, and quarantine envelopes fail closed without exposing
an intent mutation or external-delete API.

`v2M5PulsarCleanupOrderCheck` fixes the pure NPO1-root-before-NPD1-data rule without reusing M2's unconditional delete
path. The target binds exact attempt, root/data bodies and immutable versions, persisted-intent/M4-release/reference/
multipart/provider roots. Unknown or exact-old presence cannot advance, changed identity quarantines, and owned
multipart absence is required last. The gate explicitly exposes no external mutation or intent mutation API.

## Closed milestone: M4

| Item | Frozen value |
| --- | --- |
| Exact tested source | `595c8b34779d1e88187eb0084bf18e65ab2dd742` |
| Tested source-lock SHA-256 | `02601b3de76857d5f0c8657b285bc91584486d08077e201a4d9fd34f377b07a2` |
| Current immutable Final | `docs/v2/evidence/v2-m4/final/final-source-595c8b34779d1e88187eb0084bf18e65ab2dd742/m4-final.json` |
| Final SHA-256 | `31235c738400c71252e1c1c923aabda6f66545767b01c20962c0a881303e1b07` |
| Children | `READ_VIEW_HAZARD` 13 tests; `SOURCE_PLAN_EXECUTION` 6; `QUIESCENCE_PROTECTION_RELEASE` 19; `CURRENT_SOURCE_INTEGRATION_PERFORMANCE` 5 |
| Promoted scenarios | `V2-READ-001/003/004/005/007` |
| Preserved shared rows | `V2-READ-006/008..015` remain `PLANNED` with null receipts |
| Exclusions | `M5_PHYSICAL_DELETION`, `M6_PROCESS_ACTIVATION`, `M8_NATIVE_PARITY`, `PRODUCTION_DEPLOYMENT_AUTHORITY` |

`v2M4Check` is the authoritative aggregate. The historical `v2M4DesignCheck` remains the pre-implementation freeze
gate and is not a post-publication Final gate because its source check intentionally requires absent evidence paths.

## Closed milestone: M3

The M3-to-M4 transition fixes the following immutable closure identity:

| Item | Frozen value |
| --- | --- |
| Production and exact test source | `e5e53e62865c21845621037bea5f18c092bd4259` |
| Production allocator lock | `m3EvidenceBindings.allocatorMode=RANGE` in `docs/v2/source-locks.json` |
| Selected allocator | `RANGE_64` |
| Final | `docs/v2/evidence/v2-m3/final/e5e53e62865c21845621037bea5f18c092bd4259/m3-final.json` |
| Final SHA-256 | `81c7004a923e5b96cab0a3c8b4f1fa26d71606a2208bbabe779f0d872f84f84a` |
| Child/scenario closure | eleven child receipts; 26 promoted scenarios |
| Formal archive manifest SHA-256 | `a6c12ad7e642cedaf806b495d88f9e6685f665dc4d07c94357f9aa05638e84ba` |
| Formal selection SHA-256 | `92c13584eb2ca08e38b299d4facc6e65cab5cd6e4e6b7bec12aaf83356f8e2fe` |
| Exclusions | `M6_PROCESS_ACTIVATION`, `M8_NATIVE_PARITY` |

M3 reopens only if its production source lock, selected allocator, workload/qualification/RANGE-size/harness,
canonical protocol or persistent bytes, Final/archive identity, or certified-source equality is invalidated. M4 reader,
validator, pin, scenario, or ordinary refactoring work does not reopen M3. Ordinary M4 CI verifies M3 identities and
affected contracts; it does not rerun the formal allocator campaign.

The remaining M3 implementation chronology below is retained as an audit trail. Any statement there that described a
then-pending rerun is historical and is superseded by the closure identity above.

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
and 16 validation stages, the fixed ZSTD interoperability frames, and the exact 114-row TSV. Those preselection
receipts remain historical and non-promotable. Exact common-source reruns at `ae8e3f7f...` closed their owned scope;
ADR 0142's later source-governed documentation checker correction makes those results immutable prior-source evidence
and requires one fresh chain. ADR 0101 now preserves exact Cell proofs under concurrent RANGE renewal after the
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
also required canonical revalidation of the complete five-suite, 16-test NADV3 JUnit inventory at that source.
For ADR 0114 through ADR 0116 sources, ADR 0115 extends that closed inventory to exactly six suites and 17 tests by
including the formal-equivalent STRICT fixed-1000/derived-800 diagnostic. ADR 0117 adds the exact RANGE-16 sequence,
so current-source NADV3 is six suites/18 tests; historical receipts remain unchanged and parse-compatible.
The [Stage B.2 current-source recertification record](detailed_design/m3/stage-b2-native-executor-current-source-recertification.md)
requires a fresh diagnostic-only replay on the exact pushed source containing that record. It preserves the original
four-commit correction ancestry and the immutable `4bf51a38...-r1` archive, and stops with no formal directory for the
new source. It is not Stage B r2 authorization.

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

ADR 0118 governs post-terminal V3 verification. Exact source `d22a693a...` completed normally and produced valid
non-promotable `NONE_QUALIFIED` NAEV3 `0b0f3953...5726`, with no NARS3. The immutable 44-file physical attachment
inventory maps to 22 logical NACP3 execution records; the promotion CLI must reconstruct the exact interval/scale and
ordered fault aggregates, validate filename/content hashes, and consume every file once. Current-source NADV3 must
bind plan digest `5f94079e...b283`, not only schedule digest `b0e923a0...e798`. A diagnostic-only exact RANGE-16
cutoff replay may attribute the first dropped ordinal and scheduler lag, but may not change physical cutoff, workload,
qualification, or selection. This Stage B.2 recertification still stops before another formal campaign.

ADR 0119 corrects one non-normative canary assertion exposed by the exact `8d9023d2...` full diagnostic. A 500-rps
representative row completed all 15,000 measured offers with zero measured failure/timeout and complete drain, but one
warm-up pre-admission drop failed JUnit. The eight 200-rps baseline rows retain zero warm-up and measured drop gates;
representative warm-up drops remain explicit telemetry, consistent with their observational-only role. No formal
qualification or selection rule changes.

ADR 0120 closes the residual global offer-coordinator dependency exposed by final ordinal 7,999 in the exact
`94fa710a...` 10k/25ms/200 Native baseline. Four bounded-lifetime per-actor producers replay the unchanged schedule and
join a single warm-up/measurement barrier before using the existing admission queues. The physical cutoff and
pre-admission drop partition are unchanged; publication still requires all eight baseline rows to reach zero measured
drop/failure/timeout on the new exact source.

ADR 0121 removes no measured Native gate. It preserves the exact `7dcab4be...` attempt where the corrected 25ms/200
row passed 6,000/6,000, but a later 100k/10ms/200 row failed only the separate `warmupDropped` assertion despite
measured 6,000/6,000 and full drain. Warm-up pre-admission remains raw telemetry for all rows; measured
drop/failure/timeout=0 remains mandatory for all eight baselines.

ADR 0122 records that per-actor producers alone did not close the final timer/wake handoff: exact `100e5358...`
retained ordinals 7,998/7,999 as measured drops after about 5.4ms scheduler lag. A bounded final-50ms precision wait
and immediate non-blocking dispatch fast path remove that runner artifact without changing any target, cutoff, queue,
permit, outcome, or baseline zero-drop requirement.

ADR 0123 preserves the exact `c1ba429b...` Native-green/full-diagnostic-failed source. RANGE-16 fixed-1000 reproduced
15 pre-admission drops with first-drop scheduler lag zero, so the next iteration must diagnose binding wait and
workflow completion rather than change offer timing. RANGE sequence attachments are now written before assertions and
include first dropped binding, queue wait, and rollover p99. The 30,000-completion gate and six-suite/18-test NADV3
inventory remain unchanged.

ADR 0124 preserves the exact `704056b7...` 18/1/0/0 full diagnostic. Its eight Native baseline rows passed, but five
final offers in the 1ms/500 representative row fired late only after unrelated diagnostic classes had occupied the
same Gradle worker; the standalone canary at that source passed all ten rows. The full task now remains
`maxParallelForks=1` while using `forkEvery=1`, retaining one serial 18-test XML inventory and one shared-runtime
ten-row Native class without making cross-suite JVM state a formal-equivalence prerequisite.

ADR 0125 records the next protocol correction after exact `0cc962e9...` completed a fully verified V3
`NONE_QUALIFIED`. V3's immutable schedule contains on-time same-binding tail collisions closer to the physical cutoff
than the observed candidate rollover p99, so one instant cannot both close offered load and classify all waiting work
as overload. V4 retains every request, rate, zero-drop/SLO rule, `4/64/256/1`, per-binding single-flight, and bounded
queue. It adds only a two-second final admission drain equal to the existing starvation maximum, then applies the same
pre-admission drop and five-second admitted cleanup partitions. Implementation must publish independent V4 wire,
profile, feasibility, diagnostics, formal entry, promotion, and selection gates while V1/V2/V3 remain byte-stable.
The source-bound V4 core now fixes zero-decision plan digest
`1121c56cb6cd59c319c7d2eacedc8de9978bcbc2edc0008f08ef87393e0eb975` and execution-profile digest
`38a3bbda5b63365bc535a5669469728cfcd0c0189684a30c1d53f75b13b7fb35`. NACP4 carries independent V4 lineage,
campaign identity, plan/profile identity, and 42-second interval accounting around the already proved V3 logical
planner payload; NAEV4/NADV4/NARS4 consume only that outer authority. Cross-version parsers reject every direct wire
substitution, and the wrapper rejects any nested logical checkpoint not bound to the exact V4 plan digest.
Before the ADR-0125 V4 formal entry, `realAllocatorV4DiagnosticTest` had to finish the exact 21-test/eight-suite inventory with zero
failure/error/skip, including both V4 cutoff/drain runner contracts and the exact real-Oxia RANGE-16
fixed-1000/derived-800 terminal-drain rows. `sealRealAllocatorV4Diagnostic` and
`validateRealAllocatorV4Diagnostic` must then round-trip canonical NADV4 from that task's own XML directory; NADV3
remains fixed at its prior 18-test/six-suite inventory and cannot satisfy the V4 gate.

ADR 0126 preserves the completed `c44a56c2...-r1` V4 `NONE_QUALIFIED` result and raises only the current-source
diagnostic inventory to 22 tests/nine suites. The added suite replays RANGE-1024 10k/10ms fixed-1000 then derived-800
with the formal workload and V4 drain while measuring real RTT, controlled-delay scheduler/callback lag, operation
outstanding, workflow latency, and runner queue/outstanding. It must remain diagnostic-only. Any implementation
correction derived from those measurements requires a new pushed exact source, fresh NADV4, and new formal directory;
the frozen V4 plan, rates, SLOs, zero-drop rule, and selection semantics cannot change.
ADR 0127 accepts the measured correction: initial Cell/Head, pre-create Cell/Head, and pre-publish Cell/node proofs
remain exact store operations but dispatch in three independent pairs. Create/CAS same-key rereads and all bounded
reconcile/deadline guards remain unchanged. The current-source diagnostic gate remains 22 tests/nine suites and must
prove both RANGE-1024 10ms rows have zero drop/failure/timeout before a new formal entry.
ADR 0128 records that exact source `83193069...` passed that gate and moved the formal eliminating boundary to 25ms,
but still sealed `NONE_QUALIFIED`. Before any scheduler change, the current-source NADV4 inventory becomes 23 tests
in the same nine suites and must split the exact 25ms fixed/derived sequence into real RTT, injected-delay scheduler
lag, callback lag, operation outstanding, workflow latency, and runner queue telemetry. No new formal entry is allowed
until the measured cause is corrected on a new pushed exact source and the complete current-source diagnostic passes.
ADR 0129 records the exact `d434f910...` measurement and accepts proof reuse only for the store-observed installed
RANGE steady-state branch. Initial authorities and both mutation same-key rereads remain, while two duplicate proof
pairs are removed; the public API and every renewal/conflict/fault path stay proofful. A fresh pushed exact source must
pass the complete 23-test/nine-suite NADV4 and prove the 25ms rows before formal execution.
ADR 0130 records the exact `ad9dce4f...` result: the six-operation path has no retry/failure/timeout but still drops
2,882 derived-800 offers. Only the same store-observed installed-RANGE branch may retain Oxia's exact successful
key/version acknowledgement as its applied snapshot; every missing/failed/conflicting response still rereads. A new
exact pushed source must prove four common-path operations, both 25ms rows at zero drop/failure/timeout, complete
23-test/nine-suite canonical NADV4, and all source gates before formal execution.
ADR 0131 records that exact `3bc11088...` still measured six operations because the formal/diagnostic instrumented
client used the interface-default empty acknowledgement. Both wrapper levels must forward acknowledgement values
through the same latency/loss/crash/telemetry chain. A deterministic no-legacy-fallback contract plus a fresh exact
25ms receipt and complete canonical NADV4 are required before formal execution.
ADR 0132 records that exact `e53b3af8...` reached zero derived reconcile retries but still measured six operations
because the outer evidence-store decorator inherited the specialized SPI defaults and routed both mutations back to
the ordinary proofful methods. The decorator must forward both installed-RANGE specialized operations through its
unchanged exact-key/fault telemetry helper. A deterministic zero-legacy/zero-read decorator contract, fresh exact
25ms receipt, and complete canonical NADV4 remain required before formal execution.
ADR 0133 records that exact `792c77de...` proves the four-operation derived-800 path with zero drop, while fixed-1000
still drops 1,999 offers and reports 25,780 retries. The diagnostic now counts the exact closed retry-reason enum at
the unchanged source-governed backoff boundary and requires its total to match completed Result accounting. Both rows
must still reach zero drop before canonical NADV4 or formal execution.
ADR 0134 records that exact `026dfddf...` attributes 25,814 of 25,890 fixed-row retries to `RESERVATION_BUSY`, with
only 26 real Cell CAS conflicts. The workflow must consume a target Head's exact installed unconsumed RANGE grant
before interpreting another Head's Cell reservation, while Heads needing a new grant retain the bounded reservation
path. Deterministic dual-path contracts, both zero-drop 25ms rows, canonical NADV4, and all source gates remain required
before formal execution.
ADR 0135 records that exact `9fcbc7f2...` reduces fixed-1000 drop to 156 but still performs redundant proof reads
after acknowledged reserve/install/clear results. The successful bounded renewal path may reuse those exact snapshots
through install, clear, candidate create, and publish; every uncertain/conflicting result retains the public proofful
fallback. Both zero-drop 25ms rows, canonical NADV4, and all source gates remain mandatory before formal execution.
ADR 0136 records that exact `e50c455e...` reduces fixed-1000 drop to three, with real RTT and callback lag below the
remaining single-thread controlled-delay scheduler lag. The shared formal/diagnostic latency injector now has four
source-governed timer workers per actor while every delayed item remains inside the existing Runner outstanding
inventory. Both zero-drop 25ms rows, canonical NADV4, and all source gates remain mandatory before formal execution.
ADR 0137 records the complete `bb928a0...` diagnostic: the 10ms row and both derived rows are lossless, but the 25ms
fixed storm reaches the 256-global cap and drops 124 requests. The distinct V5 `4/128/512/1`
admission/profile/codec/feasibility boundary is now implemented with plan digest `3e0aea42527e...`, execution-profile
digest `76d9bc38ce6f...`, strict NACP5/NAEV5/NADV5/NARS5 bytes, a 19-file canonical raw manifest, and independent
V5 launcher/Gradle entrypoints. ADR 0138 preserves the subsequent exact `a1664de9...` FAILED V5 diagnostic: Native,
RANGE-16, RANGE-1024, and terminal drain are lossless, while STRICT accurately records bounded candidate loss. The
compatibility rows now use the formal-equivalent V5 runner/drain. ADR 0144 adds the two frozen-target boundary
contracts to new-source authority. A fresh 26-test/ten-suite diagnostic must pass with
zero failure/error/skip, complete raw accounting, lossless RANGE/Native authority rows, and canonical NADV5 before
formal execution. STRICT still needs zero loss and every unchanged SLO to qualify in formal evaluation; diagnostic
accounting does not preselect it. The preserved NADV4 and FAILED V5 attempts are non-authoritative.
ADR 0145 then records exact `3b96a298...` as another immutable infrastructure-invalid attempt: its RANGE-64 100k
construction finished all 90,000 Head creates and 84,118/89,424 initial grants before the legacy 600-second harness
cutoff. New exact-source V5 execution binds that runtime cutoff to the already-frozen 900-second construction budget.
The real-Oxia RANGE-1024 100k guard, canonical 26-test diagnostic, full source closure, and a fresh create-new formal
directory remain mandatory; no old action or checkpoint is reusable.
Exact `8a60d931...` subsequently passed that V5 diagnostic and completed formal with 123 physical actions, 33
checkpoints, and canonical `RANGE_SELECTED(RANGE_64)` NAEV5. Promotion failed closed before decision/NARS5 because
the shared verifier applied V3's 16 MiB cap to a valid 27.20 MiB V5 100k mass-takeover attachment. ADR 0139 preserves
the complete attempt as promotion-invalid and versions only the physical-file cap: V3/V4 remain 16 MiB and V5 is
32 MiB. Exact `d5b3569b...` then passed the complete V5 diagnostic and formal campaign, independently replayed 32
records/306 dispositions/123 physical files, sealed `RANGE_SELECTED(RANGE_64)`, passed promotion, and produced
canonical NARS5. ADR 0140 authorizes `allocatorMode=RANGE` and adds the mutually exclusive governed V5 child profile.
Because that lock transition is itself production source, a fresh diagnostic/formal run at the published selected
source remains mandatory before the allocator child and all downstream Final freshness work.
The V4 formal source entry is now explicit and default-off: the pure plan script reports the accepted plan/profile,
`run-v2-m3-real-allocator-evidence-v4.sh` validates the exact clean pushed source, all locked external inputs and a
canonical current-source NADV4 before creating either the service or output directory, and
`realAllocatorV4BoundedAdaptiveFormalCampaign` runs only the V4 wrapper/runtime. Checkpoint, evaluation, promotion,
and optional selection use independent V4 Gradle/CLI commands and reject V3 bytes. The first V4 run may begin only
after the final source SHA has rerun the complete real-Oxia diagnostic and all source/documentation gates; an earlier
diagnostic with V3-labeled Native row raw identity is retained as diagnostic history but is not reusable authority.

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
