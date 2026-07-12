# Phase 1.5 Implementation Plan and Gates

> 状态：P15-M0-M6 complete/final-gated；P15-M6 passed ordinary and Docker-backed gates on 2026-07-12

A milestone is complete only when production code, focused tests, aggregate gate wiring and matching documentation
all exist。Passing the old Phase 1 gate alone cannot prove a new Phase 1.5 contract；a design enum/class alone cannot
be reported as executable profile support。

## 1. Build and Compatibility Rules

The completed implementation preserves these rules：

1. retain Java 21 and current Gradle/module boundaries；
2. keep `checkPhase1L0Dependencies` and namespace guard unchanged and green；
3. `phase15Check` has real API/codec/fake/core dependencies；
4. `phase15FinalCheck` includes real-Oxia mixed-version/recovery/lifecycle coverage through the Docker gates；
5. keep all Phase 1 record/object golden vectors and tasks runnable under their existing names；
6. add no BookKeeper/Pulsar/Kafka dependency to `nereus-api`、`nereus-core`、`nereus-metadata-oxia` or
   `nereus-object-store`；
7. treat the first new generic-target metadata write as a one-way rollout boundary and document old-binary rejection。

Expected aggregate direction：

```text
phase1Check --------------------+
new API/codec/fake/core tests --+--> phase15Check

phase1FinalCheck ----------------------+
real Oxia mixed-version tests ----------+
recovery/lifecycle end-to-end tests ----+--> phase15FinalCheck
F2 L0 prerequisite contract ------------+
```

Implementation note：the production strict coordinator retains the existing `AppendCoordinator` class name for
source compatibility。The legacy `commitStreamSlice` metadata method is retained only for frozen Phase 1 fixtures；
production append uses generic new-write exclusively。These compatibility choices do not create a second runtime
append path。

## 2. P15-M0 — Code-level Design

Deliverables：

- this directory's README and documents 01-05；
- synchronized `README.md`、design index、overall architecture、terminology、commit protocol、F1/F2/F4 roadmap；
- F2 detailed docs updated so L0 implementation ownership no longer remains inside F2-M1/M2；
- explicit support matrix showing no new profile is implemented。

Exit：

```text
Every Phase 1.5 public type, durable record/key, state machine, compatibility path,
implementation milestone and downstream handoff has one authoritative document.
```

Status：complete。

## 3. P15-M1 — Public Target Model and API Evolution

Production targets：

```text
nereus-api/.../target/
  ReadTarget.java
  ReadTargetType.java
  ObjectSliceReadTarget.java
  BookKeeperEntryRangeReadTarget.java
  BookKeeperEntryMapping.java

nereus-api/.../
  ApiLimits.java                     (target/attempt bounds)
  ResolvedRange.java
  AppendAttemptId.java
  AppendRecoveryOptions.java
  SealOptions.java
  DeleteOptions.java
  AppendResult.java                  (generic target)
  ResolveResult.java                 (List<ResolvedRange>)
  ResolvedObjectRange.java           (deprecated checked adapter only)
  ErrorCode.java                     (provider-neutral additions)
```

`AppendAttemptId` and option values are introduced for downstream compilation, but `NereusException` attempt identity
and the three new `StreamStorage` methods land atomically with their P15-M4 semantics。P15-M1 does not expose a public
method whose production implementation is a placeholder。

To keep the milestone runnable before P15-M3, the current Object-only resolver constructs
`ResolvedRange(ObjectSliceReadTarget)` from legacy metadata and the current reader uses the deprecated checked
`ResolvedObjectRange.from(...)` bridge internally。Append result construction similarly wraps the existing
`WalWriteResult/WrittenStreamSlice` as an object target。The bridge rejects every non-object value and is removed from
production dispatch in P15-M3；it never invents object identity。

Tests：

- every validation/golden/immutability case in document 01；
- exhaustive target-type switch and no-fake-object conversions；
- attempt/options value validation；
- API binary/source snapshot and module dependency guard；
- compile migration of all current repository callers。

Exit：

```text
The L0 public result/resolve surface contains no mandatory object-only physical field.
Both target values are representable, but runtime support claims have not expanded.
```

## 4. P15-M2 — Metadata Dual-read / New-write

Production targets：

```text
nereus-metadata-oxia/.../codec/
  MetadataRecordCodecFactory.java
  ReadTargetCodec.java
  ReadTargetCodecRegistry.java
  ObjectSliceReadTargetCodecV1.java
  BookKeeperEntryRangeReadTargetCodecV1.java
  L0TargetMetadataCodecs.java

nereus-metadata-oxia/.../records/
  ReadTargetRecord.java
  StreamCommitTargetRecord.java
  OffsetIndexTargetRecord.java
  CommittedAppendRecord.java

nereus-metadata-oxia/.../
  CommitAppendRequest.java
  StableAppendResult.java
  CommittedAppend.java
  ReachableCommittedAppend.java
  OffsetIndexEntry.java
  OxiaKeyspace.java
  OxiaMetadataStore.java
  OxiaJavaClientMetadataStore.java

nereus-metadata-oxia/src/testFixtures/.../
  FakeOxiaMetadataStore.java
```

Implement：

- envelope record-type dispatch and shared codec factory without changing Phase 1 registry methods/goldens；
- exact target-payload codec/checksum；
- generic commit ID v2；
- commit-log/index dual decode into canonical models；
- generic marker key/value；
- `commitStableAppend` and separate `materializeGenerationZero`；
- lossless legacy index-to-target hydration without manifest/list IO, while append/audit manifest validation remains；
- fake/real `searchAppendReplay` surface with continuation values, even though core scheduling completes in M4。

P15-M2 exposes the new metadata methods to contract tests but does not switch the production append coordinator。
That cutover is P15-M3, avoiding a milestone where old core code must consume new generic values through lossy
object-shaped shims。

Tests：

- all document 02 gates against fake and Docker Oxia；
- alternating legacy/new chain with more than one repair page；
- response loss at intent/head/index/marker/audit writes；
- exact metadata version and partition-key behavior；
- mixed legacy/generic object-reference repair and orphan diagnostic parity；
- fixture old decoder rejects new record type；
- all Phase 1 metadata golden tests unchanged。

Exit：

```text
One unchanged head can anchor a dense mixed legacy/generic chain.
Generic writes are proven through the new metadata API；P15-M3 completed the production cutover.
Old data remains readable and repairable without eager migration.
Stable commit and generation-zero materialization are independently retryable metadata operations.
```

## 5. P15-M3 — Core Primary-WAL Adapter Split

Production targets：

```text
nereus-core/.../profile/...
nereus-core/.../wal/...
nereus-core/.../wal/object/...
nereus-core/.../append/AppendCoordinator.java
nereus-core/.../append/StableAppendCommitter.java
nereus-core/.../append/GenerationZeroIndexMaterializer.java
nereus-core/.../read/ReadTargetDispatcher.java
nereus-core/.../read/ReadResolver.java
nereus-core/.../read/ReadCoordinator.java
nereus-core/.../recovery/MetadataOrphanObjectScanner.java
nereus-core/.../DefaultStreamStorage.java
nereus-core/.../StreamStorageConfig.java
```

Implement：

- profile plan/registry admission before resource and provider IO；
- Object writer/reader adapters over unchanged `nereus-object-store` classes；
- strict pipeline using stable committer + generation-zero materializer；
- atomic production cutover from legacy commit/index/marker writes to generic-target new-write；
- generic resolve/read dispatch and resource accounting；
- target-aware positive cache identity/invalidation；
- explicit rejection of BookKeeper/async/non-strict profiles。

The existing `AppendCoordinator` is the single production owner after cutover；its only path uses the generic adapter、
stable committer and generation-zero materializer。There must not be two production append paths selected by
timing/configuration。

Tests：

- all document 04 core compatibility cases；
- old/new Object WAL metadata read parity；
- current phase1 append/read/trim/failure tests rerun through adapters；
- exact Object WAL binary/object-key/checksum golden parity；
- fake valid BookKeeper target reaches unsupported target error without object calls；
- stable/materialization response-loss certainty matrix；
- bounded lane/cache/adapter/resource lifecycle。

Exit：

```text
OBJECT_WAL_SYNC_OBJECT behaves identically at the public durability boundary through generic abstractions.
No object-shaped field remains in common commit/resolve/core dispatch.
No reserved profile reaches provider IO.
```

## 6. P15-M4 — Exact Append Recovery and Lifecycle

Production targets：

```text
nereus-core/.../append/
  AppendCoordinator.java             (Attempt/TerminalAttempt/StreamLane private state)
  AppendDeadline.java
  StableAppendCommitter.java
  MetadataStableAppendCommitter.java
  GenerationZeroIndexMaterializer.java
  MetadataGenerationZeroIndexMaterializer.java

nereus-core/.../lifecycle/
  StreamLifecycleCoordinator.java

nereus-api/.../
  NereusException.java               (attempt identity invariant)
  StreamStorage.java                 (recoverAppend/seal/delete)

nereus-metadata-oxia/.../
  AppendReplayCursor.java
  AppendReplayStatus.java
  AppendReplaySearchResult.java
  StreamStateTransitionRequest.java
```

Implement every state/timeout/capacity/close rule in document 03, including：

- the implemented `AppendCoordinator` is the sole retained-attempt registry、recovery single-flight and retry owner；
- no standalone `AppendRecoveryCoordinator`、`RetainedAppendRegistry` or facade-owned replay loop is added；

- attempt permit before WAL allocation；
- exact request/provider result retention；
- original-runner quiescence；
- multi-page immutable-anchor replay；
- single-flight public/background recovery；
- bounded terminal cache；
- shared append/recover/seal/delete mutation lane；
- seal and two-step logical delete CAS；
- state-aware read/trim/session admission。

Tests：

- document 03 deterministic/fault-injection matrix against fake metadata；
- real Oxia response-loss/restart for lifecycle and in-process recovery；
- caller callback simulation for one terminal result without importing Pulsar；
- resource/terminal/retry-scheduler bounds；
- no ObjectStore delete/list call in lifecycle tests。

Exit：

```text
Every post-head non-known append exposes and recovers the same retained attempt or leaves a stable fence.
Seal/delete are authoritative stream-head transitions and survive partial failure/restart.
The exact F2 L0 prerequisite surface is production-ready without Pulsar types.
```

## 7. P15-M5 — Final Acceptance and Rollout Gate

Required aggregate scenario with production Oxia + Object WAL：

1. start from a fixture containing Phase 1 legacy commits/indexes/manifests；
2. open with Phase 1.5 and read/repair legacy ranges；
3. append new generic-target records to the same stream；
4. restart and read across the mixed boundary；
5. inject head-response loss, recover the exact attempt across multiple scan pages and return the original result；
6. inject post-head materialization failure and repair strict success；
7. seal, prove final readable LAC and rejected append；
8. logical-delete with response loss between CAS steps, restart and finish `DELETED`；
9. prove no physical object deletion and no object-list correctness dependency；
10. run the protocol-neutral F2 append-recovery/lifecycle fixture；
11. prove all unimplemented profiles/durabilities fail before WAL IO；
12. run every ordinary and Docker-backed Phase 1 gate unchanged。

Deployment acceptance additionally performs the non-rolling drain/snapshot/upgrade/legacy-probe/resume protocol in
document 02。P15-M1-M4 intermediate commits are development milestones, not independently supported production
releases。

Release output must state：

- new metadata is not readable by old binaries；
- rolling downgrade is unsupported after first generic-target write；
- executable profile remains Object WAL sync only；
- BookKeeper target values are reservations, not adapters；
- the narrow P15-M6 handoff below is complete and has been consumed by F2-M1；F4/BK/async remain separate gates。

Exit commands：

```bash
./gradlew phase1Check
./gradlew phase1FinalCheck --rerun-tasks
./gradlew phase15Check
./gradlew phase15FinalCheck --rerun-tasks
./gradlew check
```

## 8. P15-M6 — F2 Result-snapshot Handoff

F2-M0R2 found one protocol-neutral in-memory result gap after P15-M5：`CommittedAppend` already holds exact
`cumulativeSize` at each commit, but `AppendResult` drops it. A facade whose cached head predates another broker's
append cannot reconstruct the exact lifetime size from its own append's `logicalBytes`，and a fallible post-commit
metadata reread cannot be allowed to turn known durable success into failure.

Production targets：

```text
nereus-api/.../AppendResult.java
nereus-core/.../append/AppendCoordinator.java
all production/test AppendResult constructor call sites
```

Changes are limited to adding `long cumulativeSize` after `committedEndOffset`，validating it and passing
`CommittedAppend.cumulativeSize()` in normal and recovered result construction. No durable codec/record、WAL byte、
head CAS、read target、outcome or attempt ID changes.

Gate：

- normal append returns the exact new cumulative total；
- an append whose start is later than a stale caller snapshot still returns the complete prefix size；
- exact recovery returns the original commit's cumulative total and later-head recovery never fabricates the current
  head size；
- every Phase 1/1.5 ordinary and Docker gate remains green and every old durable golden byte is byte-identical；
- a protocol-neutral F2 snapshot fixture can advance end/size without inspecting `ReadTarget` or performing a second
  metadata read。

P15-M6 completed on 2026-07-12。`ApiValueValidationTest` locks the public result invariant；
`DefaultStreamStorageAppendTest` locks normal cumulative advancement and later-head recovery returning the original
commit's cumulative total。`phase15Check` and `phase15FinalCheck --rerun-tasks` both passed with these cases。This is
not a new storage profile or a reopening of P15-M5 durability semantics.

## 9. F2 Handoff

After P15-M6, F2-M1 consumes these facts and must not reimplement them：

| F2 requirement | Phase 1.5 owner |
| --- | --- |
| logical generic `AppendResult` | P15-M1/M3 |
| exact cumulative logical size at the returned commit | P15-M6 |
| `AppendAttemptId` and exception contract | P15-M1/M4 |
| exact `recoverAppend` | P15-M4 |
| paged replay cursor/search | P15-M2/M4 |
| `seal` / logical `delete` | P15-M4 |
| lifecycle state operation matrix | P15-M4 |
| shared metadata codec factory | P15-M2 |

F2 remains owner of projection identity/records、Position/Entry mapping、facade callbacks、hybrid storage-class
binding and broker admission。The Phase 1.5 final fixture validates only the protocol-neutral prerequisite；it does
not claim a ManagedLedger implementation。

## 10. Future 4 and BookKeeper Handoff

Phase 1.5 gives later work：

- tagged target/codecs and adapter registry；
- stable commit vs generation-zero materialization seam；
- generic read dispatcher for mixed target history；
- retained-attempt/lifecycle primitives。

It deliberately does not freeze：

- Future 4 task/checkpoint schema or higher-generation conditional publish；
- WAL retention release rules requiring F3 cursor/group/catalog references；
- BookKeeper ledger creation/rollover/quorum/digest/client ownership；
- `WAL_DURABLE` read-after-ack SLA and background repair admission。

Those items retain their own design/implementation gates。A later adapter must pass the same identity/recovery/read
contracts rather than adding a parallel BookKeeper commit truth。

## 11. Stop-the-line Conditions

Stop the current milestone and revise design before continuing if：

- a generic result requires fake object values for a non-object target；
- new metadata cannot coexist with an unchanged Phase 1 stream head/commit chain；
- any old golden byte changes without a separately approved migration；
- stable commit implementation returns before head CAS；
- strict success can occur without confirmed generation-zero index/marker；
- recovery resubmits before the original runner quiesces or restarts every scan at latest head；
- retained uncertain attempts can be evicted for capacity；
- seal/delete truth must be stored only in F2 projection metadata；
- logical delete needs physical object deletion to complete；
- F2/BookKeeper/Pulsar types enter L0 modules；
- a reserved profile reaches provider IO；
- implementation changes code without updating its matching Phase 1.5 detailed contract。
- F2 can advance a known committed end but must guess cumulative logical size or make callback success depend on a
  second metadata read。
