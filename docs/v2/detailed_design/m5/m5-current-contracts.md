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
| Resource authority | Typed stable namespace/resource ID; eligibility in revisioned value | Typed M5RI V2 identity now feeds authority M5DA wire 4 and generic same-key CAS; typed M5ES eligibility and full fact reread now guard CAS-1/CAS-2; native namespace/route admission and real dispatch remain OPEN |
| Replacement / expiry / unpublished cleanup | Three explicit branches with complete semantic and physical-reference proofs | Reason-specific typed predicates, snapshot invalidation and fact freshness passed the focused gate; real protocol proof producers and full deletion composition remain OPEN |
| Kafka compaction | Shared semantics; Object or sealed BK carrier; internal topics remain BK_ONLY | Shared semantics, inventoried BK parts, sealed descriptor publication and independent recovery exist; recovery now uses the existing M4 source planner and hazard kernel; native namespace/task/protocol-owner admission, ordinary reads, internal-topic lifecycle and cleanup remain OPEN |
| Binding retirement | Bounded active selector plus authenticated immutable history | M5R1 wire 2 binds history root/count and monotonic activation ordinals; a configured native M4/history route now passes source-locked Oxia continuation/restart checks on the existing M4 selector key; unique native namespace/Binding assignment, quota/restart accounting, Cell I/O/metrics and physical-done cache lifecycle remain OPEN |
| Writers and recovery | Target-relevant tickets, local pins, READ_FENCED takeover, current-owner intent/done | In-memory READ_FENCED refresh/takeover now preserves admission fencing and rejects stale observation epochs; native owner adapters, intent capability refresh, visible durable veto and [concrete writer matrix](m5-lifecycle-writer-matrix.md) remain OPEN |
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
read-only BK recovery without expected output or old-source bodies. Native namespace/task authority, M4 read-source-plan
admission, real Oxia control, internal-topic lifecycle and physical deletion remain separate OPEN obligations.
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
unique native namespace/Binding assignment, quota restoration, Cell I/O/metrics, the BK compaction metadata route,
complete protocol writers and source-bound M5 children remain OPEN.
The carrier gate passed 60 executed tasks with 8 inventory/layout, 7 real carrier and 8 real Cell-session tests;
its exact-native-run check now precedes fencing, including the foreign-run zero-fence negative case.

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
