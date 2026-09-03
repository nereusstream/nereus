---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NormativeDetailedDesign
sourceTuple: v2-m1
---

# M5-E evidence ownership and freeze

## Purpose and present boundary

This document fixes future M5 evidence ownership, source freshness, scenario promotion, aggregate shape, and explicit
exclusions. It creates no evidence now. At this freeze every M5 row remains `PLANNED` with null `evidenceReceipt`, no
M5 evidence directory or runtime aggregate task exists, and the only valid result is
`DESIGN_FROZEN_IMPLEMENTATION_NOT_STARTED`.

## Exact predecessor rule

Every future M5 candidate must bind the immutable M4 Final:

| Field | Exact value |
| --- | --- |
| tested commit | `595c8b34779d1e88187eb0084bf18e65ab2dd742` |
| source-lock SHA-256 | `02601b3de76857d5f0c8657b285bc91584486d08077e201a4d9fd34f377b07a2` |
| Final path | `docs/v2/evidence/v2-m4/final/final-source-595c8b34779d1e88187eb0084bf18e65ab2dd742/m4-final.json` |
| Final SHA-256 | `31235c738400c71252e1c1c923aabda6f66545767b01c20962c0a881303e1b07` |
| required result | `PASS_V2_M4_FINAL` |

This proves the historical prerequisite only. At one future M5 tested source, M5 must freshly execute every affected
M2/M3 predicate and every M4 shared predicate `V2-READ-006/008/009/010/011/012/013/014/015`. Source compatibility,
unchanged code, an older child receipt, or a clean historical Final never substitutes for that rerun.

## Five exclusive evidence children

Each child is non-promotable by itself, uses a closed schema/kind/result, binds one exact Nereus commit and the same
source-lock SHA-256, carries zero failures/errors/skips, and references only allowlisted regular-file attachments
under its own immutable child directory.

| Child kind | Sole ownership | Minimum real boundary | Explicit non-ownership |
| --- | --- | --- | --- |
| `MATERIALIZATION_MANIFEST_PUBLICATION` | source cut, deterministic task/output, three representation modes, NMS1 projection/goldens, complete validation, selector publication and create/CAS response loss | real admitted Object provider plus BookKeeper-to-Object and Object-WAL reuse/index/rewrite paths | no Kafka semantic deletion, M4 release, metadata retirement, or physical delete |
| `KAFKA_COMPACTION_INDEX_REBUILD` | disposition root, partial/sparse/no-data rewrite, producer/transaction/control/tombstone semantics, every rebuilt index, gap lookup, fallback suppression | native Kafka differential corpus plus real BookKeeper/Object carrier | no native broker process activation or M6 scenario promotion |
| `RETENTION_METADATA_RETIREMENT` | typed trim, every floor/reference veto, reference-free proof, inline batch externalization, exact `FULL_V1 -> RETIRED_V1`, Pulsar aggregate replacement, tombstone admission | real Oxia conditional transaction and response-loss reconciliation | no protection release inference, tombstone deletion, or physical-delete claim |
| `PHYSICAL_DELETE_ORPHAN_RECONCILIATION` | exact M4 `RELEASED` consumption, final revalidation, Provider conditional delete, Pulsar root/data/multipart, BookKeeper intent/done, orphan mark/grace/rescan | real admitted S3-compatible providers and BookKeeper with injected response loss/recreation | no allocator-orphan GC, cross-scope deletion, or production credentials |
| `CURRENT_SOURCE_CELL_ISOLATION` | fresh affected M2/M3/M4 regressions, A-D integration, per-Cell task/cache/queue/provider/KMS/metadata/GC isolation and bounded performance | concurrent multi-Cell current-source execution on shared transports with independent quotas | no M6/M7/M8 activation, parity, or production deployment authority |

One assertion may appear in more than one child's regression corpus, but exactly one child owns its promotion
predicate. Aggregate code rejects duplicate attachment paths, duplicate semantic ownership, unreferenced files, child
source mismatch, missing real-boundary rows, or a zero-test/skip-based success.

## Child attachment classes

The future receipt schemas may use only these semantic attachment classes, specialized by child:

- canonical JUnit summary with exact suite/test identities;
- machine-readable format/numeric/capability selection;
- canonical projection, golden/mutation manifest, and native differential manifest;
- deterministic state/race/response-loss trace manifest;
- real Provider/BookKeeper/Oxia execution receipt with pinned product/image/artifact identity;
- capacity/performance/isolation samples with raw inventory and selected thresholds; and
- prerequisite/current-source regression result.

Attachments are evidence inputs, not standalone authority. Raw results are append-only; failed, infrastructure-invalid,
or non-selected candidates are preserved and cannot be overwritten by a later PASS.

## Scenario and predicate ownership

The exact 17 current rows whose milestone contains M5 split into 14 M5-promotable rows and three M6-deferred shared
rows. All remain unpromoted at design freeze.

| Scenario | M5 predicate owner | M4/current-source dependency | M5 Final action |
| --- | --- | --- | --- |
| `V2-META-007` | retention/metadata retirement | exact aggregate/selector/fence regression | promote at M5 Final |
| `V2-FABRIC-003` | Cell isolation | M1 Cell/Provider Scope plus M3 session regression | promote at M5 Final |
| `V2-BK-011` | physical delete | M2 P4/P5/P6 current-source deletion/read-pin predicates | promote at M5 Final |
| `V2-KAF-DATA-011` | Kafka compaction/index | M2 floor/coverage/successor reader regression | promote at M5 Final |
| `V2-READ-002` | retention plus physical delete | current manifest/source-protection regression | promote at M5 Final only when both children pass |
| `V2-READ-006` | physical delete | fresh exact M4 release predicate | promote at M5 Final |
| `V2-READ-008` | retention/metadata retirement | fresh M4 proof-window interval predicate | promote at M5 Final |
| `V2-READ-009` | retention/metadata retirement | fresh M4 capability-generation/digest predicate | promote at M5 Final |
| `V2-READ-010` | materialization plus retirement | fresh M4 selector/takeover predicate | promote at M5 Final only when both children pass |
| `V2-READ-011` | retention/metadata retirement | fresh M4 anchor/terminal/proof predicate | promote at M5 Final |
| `V2-READ-012` | materialization plus retirement | fresh M4 fused fallback-removal/STOPPED predicate | promote at M5 Final only when both children pass |
| `V2-READ-013` | retention/metadata retirement | fresh M4 per-source interval/release predicate | promote at M5 Final |
| `V2-READ-014` | retention/metadata retirement | fresh M4 inline anchor/cleanup predicate | promote at M5 Final |
| `V2-READ-015` | retention/metadata retirement | fresh M4 batch identity/lifecycle regression | promote at M5 Final |
| `V2-KAF-DATA-012` | materialization contributes only | fresh M4 pinned-generation Fetch predicate | preserve `PLANNED`; M6 owns native Fetch promotion |
| `V2-KAF-DATA-013` | physical delete contributes only | fresh M4 Object-preferred/BK-fallback predicate | preserve `PLANNED`; M6 owns native integration promotion |
| `V2-KAF-DATA-022` | Kafka compaction contributes only | affected M2 protocol-state predicate | preserve `PLANNED`; M6 owns native Kafka compaction promotion |

The 14-row promotion set is exact:

```text
V2-META-007
V2-FABRIC-003
V2-BK-011
V2-KAF-DATA-011
V2-READ-002
V2-READ-006
V2-READ-008
V2-READ-009
V2-READ-010
V2-READ-011
V2-READ-012
V2-READ-013
V2-READ-014
V2-READ-015
```

The three deferred rows remain `PLANNED` with null receipts in the M5 Final commit and appear as
`deferredSharedPredicates`, never as promoted scenarios.

## Aggregate Final contract

The eventual canonical `NEREUS_V2_M5_FINAL_V1` must contain exactly:

- schema, kind `V2_M5_FINAL`, result `PASS_V2_M5_FINAL`, and `promotionEligible=true`;
- one source tuple with M5 tested Nereus commit and source-lock SHA-256;
- the exact frozen M4 dependency identity above;
- exactly the five child receipt path/length/SHA/kind/result/test summaries at the same tested source;
- exact machine-readable physical/capacity selection roots chosen by evidence;
- exactly the 14 promoted scenarios and three deferred shared predicates;
- active open-gate state, including permanent `V2-OPEN-READ-15` unless later separately resolved; and
- exclusions `M6_PROCESS_ACTIVATION`, `M7_OPERATIONAL_HANDOFF`, `M8_NATIVE_PARITY`,
  `PRODUCTION_DEPLOYMENT_AUTHORITY`, `TOMBSTONE_DELETION`, and `ALLOCATOR_ORPHAN_GC`.

Final generation independently parses every child and attachment, validates exact bytes and ownership, proves all
required real boundaries, rejects candidate paths or symlinks, and verifies the scenario manifest as one complete
group. A Final cannot bless a child from a different source or a scenario receipt that names only a focused child.

## Future gate hierarchy

These names are reserved for implementation/evidence work and are intentionally not registered now:

```text
v2M5MaterializationCheck
v2M5KafkaCompactionCheck
v2M5RetentionRetirementCheck
v2M5PhysicalGcCheck
v2M5CurrentSourceIsolationCheck
v2M5EvidenceContractTest
v2M5EvidenceExecutionCheck
v2M5FinalSourceCheck
v2M5Check
```

The implementation checks must run non-zero deterministic unit/integration tests. Evidence execution must write only
to an explicit clean-source candidate directory and never update canonical Final/scenarios as a side effect. Final
publication is a separate exact-source action. `v2M5Check` eventually depends on `v2M5FinalSourceCheck` and
`v2DocumentationCheck`; it does not regenerate evidence.

The only aggregate gate created by this change is governance-only `v2M5DesignCheck`. It depends on
`v2M5HistoricalM4DependencyCheck` and its contract tests, the existing M4 evidence-validator contract tests,
documentation validation, and the M5 design checker/tests. The historical dependency reparses the exact immutable M4
Final, children, source binding, scenario ownership, closure blob, and ancestry without applying M4's current-source
descendant policy to the later M5 design commit. It never recertifies current HEAD as M4-tested.

## Active evidence gates and exclusions

This design does not close `V2-OPEN-OBJ-19`, `V2-OPEN-PUL-OBJ-09`, `V2-OPEN-OBJ-22`, `V2-OPEN-OBJ-24`, or
`V2-OPEN-READ-15`. In particular, the permanent tombstone baseline is implementable without tombstone deletion;
`V2-OPEN-READ-15` blocks only a future deletion/frontier authority.

M5 evidence cannot claim M6 broker/controller activation, M7 operational handoff, M8 scale/parity/release, cross-
protocol migration, cross-Cell Object groups, allocator orphan GC, tombstone deletion, or production deployment.

## Design freeze audit

The governance checker must fail when any bound document changes without a digest update; a required front matter
status changes; an M5 scenario is prematurely promoted; a predecessor receipt changes; an active gate is removed; a
runtime/evidence/Final task or evidence path appears; a new Gradle module is added; future child/scenario ownership is
ambiguous; or a required exclusion disappears.

No blocking design question remains. Physical codec offsets/caps, finite budgets, real provider capability selection,
and performance thresholds remain evidence-selected implementation details inside this fixed contract. They cannot
weaken an identity, veto, ordering rule, real-boundary requirement, or scenario boundary.
