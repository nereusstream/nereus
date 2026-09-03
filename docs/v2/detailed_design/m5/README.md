---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
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

## Current state

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
- [M5-E evidence ownership and freeze](m5-e-evidence-ownership-and-freeze.md) freezes child ownership, scenario
  promotion boundaries, exact-source rules, future gate hierarchy, and exclusions.
- The governance-only [freeze manifest](m5-design-freeze.json) binds I0 and A through E by exact SHA-256.
- The additive [amendment manifest](m5-design-amendment-1.json) binds the immutable base manifest, ADR 0146, and the
  exact amendment bytes without rewriting the original I0/A-E records.
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

At immutable design commit `c86fde3ed6f4319642987fd599022bd32e2cca5e`, the result is exactly
`DESIGN_FROZEN_IMPLEMENTATION_NOT_STARTED`. Current descendants complete the M5-A, M5-B, and M5-C implementation
gates plus focused M5-D Provider/BookKeeper adapters without amending that result. All 17 scenario rows whose milestone names
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
