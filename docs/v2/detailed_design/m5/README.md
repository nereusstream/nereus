---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: NotRun
authority: NormativeDesignIndex
sourceTuple: v2-m1
---

# M5 detailed-design index

M5 turns already durable WAL data into a validated long-lived read generation, optionally performs Kafka-semantic
compaction, advances typed logical retention, retires no-longer-needed metadata, and only then reclaims physical
storage. In one sentence: M4 proves an old source is no longer readable; M5 consumes that exact proof and carries the
source through deterministic replacement, reference-free retirement, and retry-safe deletion.

This directory contains the complete M5 hard-freeze and later implementation projections/logs. The immutable I0 and
A through E inputs are design authority only: those documents created no M5 runtime code, execution receipt, scenario
promotion, physical deletion, or production deployment authority. Later implementation descendants do not rewrite
that historical result.

## Current effective contracts

Start with [ADR 0148 and the current contract view](m5-current-contracts.md). The
[lifecycle amendment](m5-lifecycle-contract-amendment.md) and [manifest 3](m5-design-amendment-3.json) select exact
supersession of the historical clauses below. Earlier focused gates do not satisfy revised implementation or Final.

## Historical inputs and implementation predecessors

- [M5-I0 implementation-input closure](m5-i0-implementation-input-closure.md) freezes authority, dependency, module,
  lifecycle, and implementation-order boundaries.
- [M5-A materialization and manifest publication](m5-a-materialization-and-manifest-publication.md) freezes source
  cuts, deterministic identities, Object-WAL reuse/reindex/rewrite selection, validation, and fenced publication.
- [M5-B Kafka compaction and index rebuild](m5-b-kafka-compaction-and-index-rebuild.md) freezes the protocol-semantic
  rewrite contract and every index/recovery invariant affected by sparse, partial, empty, transactional, and
  tombstone output.
- [M5-C retention, reference-free proof, and metadata retirement](m5-c-retention-reference-free-and-metadata-retirement.md)
  freezes logical trim, the complete veto inventory, inline-batch externalization, irreversible batch compaction,
  Pulsar aggregate retirement, and permanent tombstone admission.
- The accepted [M5-C single-Binding retirement authority amendment](m5-c-single-binding-retirement-authority-amendment.md)
  and [ADR 0146](../../../decisions/0146-v2-m5-single-binding-retirement-authority-amendment.md) replace only the
  unavailable multi-key externalization mechanism with one existing authority key, ticket/fence serialization, and
  exact Oxia single-key CAS. The original freeze remains the immutable base.
- [M5-D physical delete, orphan, and GC](m5-d-physical-delete-orphan-and-gc.md) freezes exact M4 `RELEASED`
  consumption, final revalidation, Object/BookKeeper deletion protocols, orphan taxonomy, and per-Cell isolation.
- The accepted [M5-D target-scoped physical-delete authority amendment](m5-d-target-scoped-physical-delete-authority-amendment.md)
  and [ADR 0147](../../../decisions/0147-v2-m5-target-scoped-physical-delete-authority-amendment.md) replace only the
  unavailable conditional multi-key deletion linearization with one permanent authority key per immutable target,
  closed-writer tickets/fencing, and two exact same-key CAS transitions around the external identity read. Its historical acceptance
  recorded `NotStarted`; later foundation/coordinator slices exist with no dispatch or physical-delete authority.
- [M5-E evidence ownership and freeze](m5-e-evidence-ownership-and-freeze.md) freezes child ownership, scenario
  promotion boundaries, exact-source rules, future gate hierarchy, and exclusions.
- The governance-only [freeze manifest](m5-design-freeze.json) binds I0 and A through E by exact SHA-256.
- The additive [amendment manifest](m5-design-amendment-1.json) binds the immutable base manifest, ADR 0146, and the
  exact amendment bytes without rewriting the original I0/A-E records.
- The ordered [M5-D amendment manifest](m5-design-amendment-2.json) binds that base freeze, the exact predecessor
  amendment manifest, ADR 0147, and the exact target-scoped authority amendment bytes. All four authority flags remain
  false at this governance boundary.
- The later [implementation log](m5-implementation-log.md) tracks ordered implementation descendants without
  changing the frozen I0/A-E bytes or promoting focused results.
- The implementation-selected [M5-A wire projection](m5-a-wire-projection.json) fixes the version-1 physical codes,
  identity domains, section caps, flags, and lookup rule used by the first runtime slice.
- The implementation-selected [M5-B wire projection](m5-b-wire-projection.json) fixes the Kafka dependency, magic,
  caps, seven disposition codes, complete eight-index set, gap lookup, fallback suppression, and publication rule.
- The implementation-selected [M5-C capability projection](m5-c-capability-projection.json) records the closed
  reference/admission inventories and the source-locked Oxia adapter's fail-closed lack of the required atomic
  multi-key transaction. It is preserved rejected evidence for the pre-amendment design and is not the future M5-C
  retirement gate.
- The implementation-selected [M5-C Binding authority projection](m5-c-binding-authority-projection.json) fixes the
  `M5R1` envelope, exact legacy migration, M4 projection, closed writer enrollment, durable tickets, one scan fence,
  hard caps, and real Oxia single-key CAS used by the first amended implementation slice. Its focused,
  non-promotable `v2M5BindingAuthorityCheck` result is retained as a predecessor of the complete M5-C implementation
  gate.
- The [M5-C Pulsar aggregate authority projection](m5-c-pulsar-aggregate-authority-projection.json) fixes the distinct
  `M5PA` envelope at the existing incarnation aggregate key, exact NTA1 reader projection, closed tickets/fence,
  permanent exact `DELETED(generation)` selector binding, and the post-cleanup one-key transition to `M5PR`. Its
  `v2M5PulsarAggregateAuthorityCheck` result is also focused and non-promotable: it implements no M5-D cleanup and
  grants no physical-delete, scenario-promotion, or production authority.
- The [M5-C closed writer integration projection](m5-c-closed-writer-integration-projection.json) fixes the canonical
  ownership registry for all 10 floor and 15 reference classes and the shared ticket-before-dispatch guard for both
  authority families. `v2M5ClosedWriterIntegrationCheck` proves missing/duplicate ownership, ticket response loss,
  ambiguous external outcomes, and fence-first races fail closed. It is still a focused, non-promotable gate rather
  than the source-bound `RETENTION_METADATA_RETIREMENT` child.
- The [complete M5-C retention-retirement implementation projection](m5-c-retention-retirement-implementation-projection.json)
  binds those focused predecessors to exact Oxia client/server artifacts and real restart execution.
  `v2M5RetentionRetirementCheck` proves both permanent metadata-retirement families through the accepted one-key CAS
  path. It remains implementation-only and non-promotable: no source-bound child receipt, M5-D deletion result,
  scenario promotion, or production authority exists.
- The focused [M5-D version-match delete projection](m5-d-version-match-delete-projection.json) binds the additive
  transport API, default-unsupported behavior, bucket-versioning admission, exact-version S3 deletion, and complete
  LIST/full-GET reconciliation to fixed-digest MinIO. `v2M5VersionMatchDeleteCheck` is a Provider foundation only;
  the full M5-D state machine, source-bound child, and all delete authority remain absent.
- The focused [M5-D BookKeeper delete projection](m5-d-bookkeeper-delete-projection.json) binds the exact BookKeeper
  4.18.0 client/source and fixed-digest server image to a sealed-ledger metadata fingerprint, stale-target rejection,
  delete-response-loss handling, and authoritative no-such-ledger reconciliation. `v2M5BookKeeperDeleteCheck` is an
  execution adapter gate only: it cannot create intent or done, and grants no dispatch, physical-delete, receipt,
  scenario-promotion, staging, or production authority.
- The focused [M5-D orphan/admission projection](m5-d-orphan-admission-projection.json) fixes all six orphan classes,
  the only three mark-eligible classes, authority-time grace plus complete rescan, live-owner adoption, permanent
  metadata/allocator retention, foreign quarantine, and hard per-Cell resource envelopes. The pure
  `v2M5OrphanAdmissionCheck` can produce only a future-intent candidate and exposes no delete or intent mutation API.
- The focused [M5-D Pulsar cleanup-order projection](m5-d-pulsar-cleanup-order-projection.json) binds an exact sealed
  attempt, NPO1/NPD1 Object bodies and immutable versions, and intent/release/reference/multipart/provider roots to
  the strict root-absence then data-absence then multipart-absence sequence. `v2M5PulsarCleanupOrderCheck` is pure:
  it never invokes the older unconditional delete seam and exposes no external or intent mutation API.
- The focused [M5-D multipart cleanup projection](m5-d-multipart-cleanup-projection.json) binds one non-empty exact
  persisted owned inventory to the Cell/Provider namespace, exact key/upload IDs, bounded two-marker pagination,
  exact abort, and complete relist after every response. `v2M5MultipartCleanupCheck` executes those rules against
  fixed-digest MinIO, including response loss, same-key foreign-upload veto, and the product's explicit
  directory-prefix-listing rejection. It is an execution adapter only: no intent mutation, dispatch authority,
  source-bound receipt, scenario promotion, or production authority follows.
- The [M5-D target authority foundation projection](m5-d-target-delete-authority-foundation-projection.json) fixes the
  `M5DA` wire (now version 4), stable physical-resource permanent key, four irreversible states, ten-class closed writer
  enrollment, monotonically increasing revision/predecessor chain, CAS-1/CAS-2 candidates, fixed-attempt takeover,
  and permanent done shape. `v2M5TargetDeleteAuthorityFoundationCheck` originally exercised 11 pure tests; the current identity slice adds
  typed-resource tests as recorded in its projection. At the foundation boundary there was no metadata
  mutation coordinator. The later generic coordinator below still has no writer runtime integration, external identity
  reader/delete composition, real Oxia result,
  receipt, or physical-delete authority at this foundation.
- The [M5-D target authority coordinator projection](m5-d-target-delete-authority-coordinator-projection.json) binds
  `v2M5TargetDeleteAuthorityCoordinatorCheck` to one exact `compareAndSet` call site, complete post-CAS reread
  reconciliation, and a generic durable ticket-before-writer-dispatch guard. The original nine in-memory tests prove both race
  orders and response-loss ticket retention. All concrete proof-changing writers, external identity/delete adapters,
  real Oxia execution, source-bound receipt, and physical-delete authority remain absent.
- The [typed deletion eligibility projection](m5-delete-eligibility-projection.json) adds explicit replacement,
  logical-expiry and unpublished-artifact predicates, invalidates qualification on writer admission, and requires
  exact fact rereads at qualification, CAS-1 and CAS-2. `v2M5DeleteEligibilityCheck` passed 42 tasks with synthetic
  facts and existing retention regressions. Native semantic proof production and complete deletion composition
  remain OPEN; these focused results create no source-bound child or physical-delete authority.
- The [READ_FENCED recovery projection](m5-read-fenced-recovery-projection.json) adds exact observation-epoch binding,
  same-key takeover/refresh without reopening admission, and mandatory native owner verification that is unsupported
  by default. The later `v2M5ReadFencedOxiaCheck` adds native fact versions, actual competing-client CAS and same-server
  restart recovery: five integration cases plus two separate JVM phases. One bounded same-key recovery veto now
  survives restart, rejects intent and clears only after a qualified observation refresh. The owner, semantic/M4 and
  external identity statements remain synthetic; real protocol owner adapters, external identity readers and intent capability refresh
  remain OPEN. Ordinary authority values retain their exact wire-4 bytes; veto-bearing READ_FENCED uses wire 5.
- The [shared Kafka semantic-core projection](m5-kafka-semantic-core-projection.json) records carrier-independent
  record/disposition/gap/eight-index output. The Object bridge uses the same record compiler and independent validator;
  the [BK carrier projection](m5-bookkeeper-compaction-carrier-projection.json) adds inventoried native ledger
  allocation, sealing and exact part verification. The [BK descriptor projection](m5-bookkeeper-descriptor-projection.json)
  adds typed sealed selection and descriptor-only recovery. The [M4 recovery bridge](m5-bookkeeper-m4-recovery-projection.json)
  protects low-frequency recovery with the existing source planner and generation lease; native protocol-owner/task
  admission, ordinary reads, internal-topic lifecycle and cleanup remain OPEN.
- The [retired-history projection](m5-retired-history-projection.json) adds M5R1 wire 2 and authenticated immutable
  BatchId history. Terminal slots fold through one selector CAS, active holes keep their original ordinal and M4 bytes,
  and admission rejects historical IDs under the current root. Native namespace quota/restart accounting, Cell I/O,
  operator metrics and permanent physical-done cache integration remain OPEN.
  The [real Oxia history projection](m5-retired-history-oxia-projection.json) verifies 1,026 native folds, unknown-result
  retries, current-root admission and a separate server restart fixture. Its historical M4 bridge was test-only.
  The subsequent [Binding route projection](m5-binding-lifecycle-route-projection.json) replaces that bridge with a
  production M4/history adapter using the same native selector key and route-scoped exact version tokens; the native
  continuation and restart checks pass again. Unique native namespace/Binding assignment, quota/restart accounting,
  Cell I/O/metrics and complete writer/evidence composition remain OPEN. External source/reference facts remain fixtures.
- The [BK/Oxia control projection](m5-bookkeeper-oxia-control-projection.json) adds immutable native compaction-record
  transport and protocol-owned task/Binding/namespace validation, composed with the raw Binding envelope for M4.
  Five joint real Oxia/BK cases and two JVM phases across an Oxia server restart verify publication, protected recovery,
  native response-loss retry and rejection of missing or stale inputs. The configured control executor is bounded;
  native namespace/source/protocol admission, task terminal fencing, internal-topic lifecycle and cleanup remain OPEN.

- The [native BK create projection](m5-bookkeeper-native-create-projection.json) adds the explicit `m5zk` metadata SPI
  profile, actual INSTANCEID physical namespace and permanent per-task/per-ledger records. Native ledger creation and
  fence checks share one ZooKeeper transaction. Eight native create cases, three guarded BK/Oxia compaction cases and
  two independent JVM phases across ZooKeeper/bookie/Oxia restarts verify late-create rejection and selected recovery.
  Full task terminal/publication fencing, append drain, all-writer admission, durable quota and cleanup remain OPEN.

- The [BK task-terminal projection](m5-bookkeeper-task-terminal-projection.json) adds one bounded task decision on
  the existing selector and a permanent per-task archive. Cancellation and publication race at the same native CAS;
  capturing the raw authority prevents projected-selector ABA. Native creation fencing plus recovery of existing
  writers produces a durable cancelled physical cut. Seven decision, three route and four terminal unit tests, five
  real BK/Oxia cases and two restart JVM phases pass. Unknown stale tasks, full writer/namespace admission,
  grace/reference rescans, cleanup, quota and source-bound acceptance remain OPEN.

- The [permanent-done projection](m5-permanent-done-projection.json) adds an exact same-key compact terminal and
  a bounded positive cache. Native Oxia samples exceed the resident cap, reject late ticket CAS and new OPEN creation,
  and preserve exact done/intent identities across server restart. These synthetic storage fixtures did not supply
  durable quota or GC scheduling; the later quota slice below extends this path.
- The subsequent [GC quota projection](m5-gc-quota-projection.json) adds native durable byte reservations and permanent
  resource settlement records. One bounded head operation recovers grants/refunds after response loss and server restart;
  existing intents complete with full capacity and another pending grant. Fixed-depth native scans reject legacy
  authority before initialization, and 130 settled records match native canonical byte totals. Explicit expansion is
  required when permanent history leaves insufficient capacity for another complete reservation. This configured route
  still requires unique namespace/all-writer admission, backend disk provisioning, real deletion proofs and GC scheduling.

- The [physical namespace projection](m5-physical-namespace-projection.json) adds permanent actual BK-to-Oxia
  namespace assignment. A fixed native Oxia marker and actual BK INSTANCEID determine identity; endpoint aliases
  converge on one authority root. Binding advances a native gate checked by both ID reservation and ledger create,
  cutting off delayed unbound creates. The guarded quota route rereads both native assignments before operations.
  Three identity unit cases and two native phases pass across two Oxia and four BK service restarts; legacy
  creation/task-terminal results are revalidated by captured source and XML hashes. Object namespace, cross-Cell
  ownership, all concrete writers, offline compatibility and physical deletion remain OPEN.

- The [publication ticket projection](m5-publication-tickets-projection.json) adds sorted physical tickets around
  the actual guarded BK publisher. Every resolved input and sealed output must be ticketed before publication writes;
  a later fence cancels dispatch, while unknown completion retains tickets. Native immutable Task selection/cancellation
  reconciles old invocations, including a losing selector CAS still in flight. Each recovery pass is bounded at 256
  removals. The full physical set enters ticket identity. Logical source catalog/admission and eligibility remain
  fixtures; complete native writer integration, READ_FENCED and physical deletion remain OPEN.

- The [Kafka run-root projection](m5-kafka-run-roots-projection.json) adds persisted root transport under the actual
  namespace binding. A permanent genesis or exact SEALED-parent CAS selects one child; pending unselected roots remain
  invisible. Physical tickets cover the child and its parent, and actual NBKE2 headers/closed footers constrain publication.
  Native internal-topic run bytes and restart recovery are exercised with synthetic resource/owner admission. Complete
  input membership, root metadata quota, ordinary reads, internal-topic compaction and physical deletion remain OPEN.

- The [native run-source projection](m5-kafka-run-source-projection.json) derives raw NBKE2 source extents and input
  batches from selected roots and actual closed ledgers. Complete group/body/index-reference checks run under physical
  read tickets and shared byte/count budgets; exact frozen extents determine publication membership. The existing BK
  compiler/publisher consumes these inputs. Compacted-generation and other source catalogs, protocol-owner/semantic
  admission, read-ticket crash recovery, root quota and physical deletion remain OPEN.
  The focused runner passed 51 archived cases/phases and retained-data restart; its independent legacy regression
  passed 71. Both captured source maps remained unchanged. Decompressed framing is checked before record allocation.

- The [complete input-plan check](m5-kafka-input-plan-projection.json) additionally compares every native source batch,
  ordinal and byte body with the publication plan. Native/restart validation passed 51 archived cases/phases plus
  71 independent legacy cases/phases; omitted input created no candidate pointer or selector CAS. Compacted-generation
  input capture remains OPEN.

- The [selected-generation source reader](m5-kafka-selected-source-projection.json) captures native selected descriptors,
  all data/index ledger members and actual batches under read tickets and shared decode bounds. A typed index-only
  generation can enter the semantic compiler with no invented data batch. Native verification passed exact M4
  fallback closure, new source protection and second publication while retaining old PROTECTED records. Complete M4
  release, mixed catalogs, native owner admission and deletion remain OPEN.
  The final run passed 90 archived cases/phases plus 71 independent legacy cases/phases, with both source maps unchanged.
  First-generation checkpoints are retained; four separate second-generation checkpoints now pass fresh-JVM native
  descriptor/task recovery for the user topic, both internal topics and index-only output, without republishing.
  New fallback protection starts at the introduced E+1, while existing fallback identities inherit their earlier first
  epoch. Shared Object/Kafka validation rejects the old preferred-only epoch; native stored epochs survive restart.

- The [scoped BK read owner](m5-kafka-read-owner-projection.json) holds every physical member's ticket from before
  native session creation through actual read/session termination. It closes local admission before exact M4 closure
  and supports exact retry after unknown response. Five owner tests, seven unchanged M4 recovery tests and a new
  native delayed-read/close case pass within the 90-case/phase source-bound run. It creates no M4 terminal or RELEASED
  record; full native owner admission and crash reconciliation remain required.

At immutable design commit `c86fde3ed6f4319642987fd599022bd32e2cca5e`, the result is exactly
`DESIGN_FROZEN_IMPLEMENTATION_NOT_STARTED`. Current descendants complete the M5-A, M5-B, and M5-C implementation
gates plus focused M5-D Provider/BookKeeper/orphan-admission/Pulsar-order/multipart cores without amending that result.
ADR 0147 and `m5-design-amendment-2.json` accept the next M5-D implementation mechanism but create no runtime or
delete authority. All 17 scenario rows whose milestone names
M5 remain `PLANNED` with null receipts. `docs/v2/evidence/v2-m5/`, `v2M5EvidenceExecutionCheck`,
`v2M5FinalSourceCheck`, and `v2M5Check` remain absent.

## Frozen predecessor

M5 starts only from the following immutable M4 closure and must reject any substitute:

| Item | Frozen value |
| --- | --- |
| M4 tested source | `595c8b34779d1e88187eb0084bf18e65ab2dd742` |
| M4 tested source-lock SHA-256 | `02601b3de76857d5f0c8657b285bc91584486d08077e201a4d9fd34f377b07a2` |
| M4 Final | `docs/v2/evidence/v2-m4/final/final-source-595c8b34779d1e88187eb0084bf18e65ab2dd742/m4-final.json` |
| M4 Final SHA-256 | `31235c738400c71252e1c1c923aabda6f66545767b01c20962c0a881303e1b07` |
| M4 release authority | exact protection key, exact protection generation, state `RELEASED`, matching batch SHA and proof-head SHA |
| M4 exclusions inherited by M5 | M5 physical deletion was excluded from M4; M6 activation, M8 parity, and production authority remain excluded here |

The latest clean-main and hosted-CI verification of that dependency is a prerequisite for implementation, but this
design freeze does not recertify M4 at the M5 source and does not copy M4 evidence into M5.

## Design tree

```text
M5 data lifecycle
├── I0. authority, dependency, modules, phases, and fail-closed state vocabulary
├── A. deterministic materialization and manifest publication
│   └── REFERENCE_REUSE | INDEX_ONLY_GENERATION | REWRITE_GENERATION
├── B. Kafka-semantic compaction and complete index rebuild
├── C. typed trim, reference-free proof, and two metadata-retirement families
│   ├── Object-WAL SourceRetirementBatch FULL_V1 -> RETIRED_V1
│   └── Pulsar TopicBindingAggregateRecord -> RetiredTopicIncarnationTombstone
├── D. exact-source physical deletion and bounded orphan reconciliation
│   ├── Object DELETE_NONE -> DELETE_INTENT -> DELETE_DONE
│   └── Pulsar BK_DELETE_NONE -> BK_DELETE_INTENT -> BK_DELETE_DONE
└── E. five evidence children, scenario ownership, and eventual M5 Final
```

The ordering is normative. A later phase may consume an earlier phase's exact immutable output; it may not infer that
output from absence, age, a local scan, a controller request, a worker receipt, or a batch summary.

## Scope

M5 owns:

- frozen, binding-scoped source cuts and deterministic materialization task/output identity;
- validated read generations and one exact manifest selection CAS;
- Object-WAL payload reuse, index-only generation, and rewrite policy;
- Kafka-semantic compaction and complete affected-index reconstruction;
- typed logical trim and complete authoritative retention/reference snapshots;
- exact `RELEASED` consumption and reference-free proofs;
- Object-WAL batch and Pulsar aggregate metadata retirement with permanent tombstones;
- final provider/source/fence/capability revalidation;
- retry-safe Object, multipart, Pulsar offload pair, and BookKeeper deletion;
- bounded physical-orphan discovery/reconciliation; and
- per-Cell queues, bytes, cache, task, provider, KMS, metadata, and GC budgets.

M5 does not own:

- changing M1/M2/M3/M4 persistent meanings, source locks, or historical Finals;
- acknowledging writes, changing active-tail durability, or reopening an M4 release;
- native broker/controller process activation, placement, or lifecycle integration (M6);
- planned handoff and mixed-profile operational activation (M7);
- AutoMQ/native Pulsar parity, release scale, or noisy-neighbor certification (M8);
- allocator orphan reclamation, tombstone deletion, cross-Cell Object groups, or cross-protocol projection/migration;
- Topic policy authority to weaken a correctness gate; or
- production deployment authority.

## Non-negotiable invariants

1. Logical trim is not physical deletion.
2. A materialized or compacted generation becomes readable only after complete byte, coverage, index, protocol,
   task, policy, and fence validation followed by one exact manifest CAS.
3. Non-compacting materialization preserves protocol payload bytes. Kafka compaction is a separate rewrite kind and
   preserves Kafka-visible semantics rather than byte identity.
4. M5 accepts protection release only by exact M4 `RELEASED`; every weaker observation retains the source.
5. Physical deletion revalidates the current manifest, retention/reference proof, Provider Scope, immutable identity,
   and every owner/worker/storage fence after intent and immediately before dispatch.
6. Unknown create, publication, retirement, or delete outcomes converge by exact authoritative reread; ambiguity never
   becomes success.
7. Permanent tombstones authorize neither protection release nor physical GC and are never deleted in 0.2.
8. Shared executors and transports never share Cell authority or unbounded capacity.

## Implementation order after this freeze

Implementation may start only after `v2M5DesignCheck` passes. The required order is M5-A, then M5-B, then M5-C, then
M5-D, followed by the five M5 evidence children and aggregate Final described in M5-E. A phase may be developed behind
non-promotable tests while a later phase is absent, but no deletion dispatcher may be enabled before A through C are
implemented and their exact prerequisites are wired.

Any change to an identity preimage, persistent state transition, reference-veto class, deletion capability mode,
scenario ownership, or phase order requires an explicit reviewed amendment and a new freeze-manifest digest. Numeric
capacity and performance values marked evidence-selected may be chosen later only within the closed semantic envelope;
they cannot weaken a veto or create a new authority.

## Design-close conditions

The design is closed only when:

1. I0 and A through E contain no blocking semantic or ownership question;
2. the M4 dependency identity and exact `RELEASED` boundary are frozen;
3. all materialization, compaction, retention, metadata-retirement, delete, response-loss, orphan, and Cell-isolation
   state machines have fail-closed terminal rules;
4. the inline M4 batch representation has an exact ADR-0080-compatible migration into a same-key full/tombstone
   record without reconstructing release eligibility;
5. the 14 M5-promotable and three M6-deferred scenario rows are named exactly and remain unpromoted here;
6. active evidence gates remain active and no prose claims their evidence;
7. the documentation and M5 design contract gates pass; and
8. at the immutable hard-freeze source, the repository contains no M5 runtime/evidence/Final gate or evidence
   artifact.

Hard freeze authorizes only later implementation work. It does not authorize running a workload, deleting data,
publishing a receipt, promoting a scenario, or activating a production path.

No blocking design question remains.
