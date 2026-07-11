# Phase 1.5 Core Storage Foundation

> 状态：Designed；P15-M0 code-level design complete，production implementation not started
> 输入基线：`nereusstream/nereus@8aa684f479994094a612578bbfe27170b077f4ad`
> 实现基线：Future 1 / Phase 1 M0-M8 at `ad8c272787fe77f908397515864ef1f72945e8ee`
> 下一里程碑：P15-M1 public target model and API evolution

Phase 1.5 是插入 Future 1 与 Future 2 production implementation 之间的 L0 foundation delivery。
它不是新的 capability track，也不改变 Future 1-8 的编号。它把 Phase 1 已证明的
`OBJECT_WAL_SYNC_OBJECT` 实现重构成可承载多种 physical read target 的 core，并完成最新
F2-M0R 已锁定但当前 `StreamStorage` 尚不具备的 append recovery 与 stream lifecycle 合同。

Phase 1.5 完成后，运行时支持面仍然只有：

```text
OBJECT_WAL_SYNC_OBJECT
+ WAL_DURABLE_AND_INDEX_COMMITTED
```

BookKeeper writer/reader、`WAL_DURABLE` fast return、async materialization worker、higher generation、
physical GC 和任何 Pulsar facade 都不因 Phase 1.5 自动变成已实现能力。

## 1. Why This Delivery Exists

Phase 1 的 object-shaped 边界存在三个已经被后续设计实际触发的缺口：

1. `AppendResult`、`ResolvedObjectRange`、`CommitSliceRequest`、`StreamCommitRecord` 和
   `OffsetIndexRecord` 把 object identity/layout 直接写进公共或 core metadata boundary，无法诚实表示
   BookKeeper ledger/entry range 或 Future 4 compacted target；
2. append head-CAS、generation-0 index materialization 和 strict result construction 仍耦合在同一
   metadata method/core coordinator 中，不能在保持 stable logical commit 的前提下增加未来
   `WAL_DURABLE` completion policy；
3. F2-M0R 要求 exact in-process append recovery、`seal` 和 logical `delete`，而当前
   `StreamStorage` 没有这些方法，当前 bounded replay 在多页历史链上也不能持续推进。

直接在 F2 facade 内补偿这些缺口会让 Pulsar callback/lifecycle 变成 L0 truth 的替代者；直接开始 F4
则会把 object-only target 固化进 task、generation 和 GC schema。Phase 1.5 先关闭共享的 L0 seam，
再恢复 F2 implementation。

## 2. Scope

Phase 1.5 owns：

- protocol-neutral `ReadTarget` tagged model；
- object slice 与 reserved BookKeeper entry-range target value/codec；
- generic `AppendResult` / `ResolvedRange` API；
- `PrimaryWalAppender`、`PrimaryWalReader`、profile execution-plan 和 adapter registry；
- current Object WAL writer/reader 的 adapter，Object WAL v1 bytes 不变；
- stable head commit 与 generation-0 derived-index materialization 的显式拆分；
- new generic commit/index/marker records，legacy Phase 1 records dual-read，new-write only；
- process-local retained `AppendAttemptId`、paged replay continuation 和 exact `recoverAppend`；
- stream `seal`、logical `delete` 和 shared mutation-lane ordering；
- Phase 1 compatibility/failure gates plus F2 L0 prerequisite gates。

Phase 1.5 does not own：

- a BookKeeper dependency, client, ledger lifecycle, writer or reader；
- execution of `BOOKKEEPER_WAL_*` or `OBJECT_WAL_ASYNC_OBJECT`；
- successful `WAL_DURABLE` return；
- durable materialization task/checkpoint, worker, generation > 0 or retention gate；
- cursor/group low-watermark, object deletion or catalog references；
- ManagedLedger/Position/MessageId types or callbacks；
- producer-sequence deduplication across process restart。

The accepted delivery/compatibility decision is
`../decisions/0004-insert-phase-1-5-generic-storage-foundation.md`。

## 3. Locked Decisions

1. `ReadTarget` describes physical selection only. It never decides whether a range is logically committed。
2. New commits use generic target records. Existing Phase 1 records and golden bytes remain readable and are not
   rewritten eagerly。
3. `StreamHeadRecord` and its key remain unchanged. The head still links one dense mixed-version commit chain。
4. Generation zero means the primary WAL target recorded by append. Generation greater than zero remains Future 4。
5. `StableAppendCommitter` ends at a reachable head commit. `GenerationZeroIndexMaterializer` is a separate,
   idempotent derived-state step。
6. Phase 1.5 strict append still waits for generation-zero index and replay-marker confirmation；the newly separated
   `WAL_DURABLE` boundary is not exposed until its read-after-ack and background-repair gates are implemented。
7. A `BookKeeperEntryRangeReadTarget` value is a durable representation and codec reservation only. No runtime
   registry binding means every BookKeeper profile is rejected before WAL IO。
8. Every post-head-request non-known append failure carries an in-process `AppendAttemptId` and retains the exact
   physical attempt. Recovery never prepares new bytes。
9. `seal` linearizes at `ACTIVE -> SEALED` head CAS. Logical delete linearizes at the first
   `ACTIVE/SEALED -> DELETING` head CAS；`DELETED` records terminal completion and removes no bytes。
10. F2 consumes logical append/read results and must not inspect object IDs/keys to derive Position semantics。

## 4. Document Map

| Document | Authority |
| --- | --- |
| `01-api-and-read-target-contract.md` | exact public value/API shape, validation and compatibility |
| `02-metadata-schema-and-compatibility.md` | generic target codecs, new records/keys, dual-read/new-write and repair |
| `03-append-recovery-and-lifecycle.md` | retained attempts, paged recovery, seal/delete and mutation ordering |
| `04-core-abstractions-and-state-machines.md` | primary-WAL adapter seam, strict append/read dispatch, resources and close |
| `05-implementation-plan-and-gates.md` | P15-M1 through M5 files, tests, release gates and stop conditions |

The implemented Phase 1 contract in `../phase-1-core-stream-storage/` remains authoritative until a Phase 1.5
milestone changes code and tests. When implementation begins, each code change must update the matching document here
and preserve the legacy contract where this design explicitly requires compatibility。

## 5. Delivery Milestones

| Milestone | State | Exit |
| --- | --- | --- |
| P15-M0 code-level design | Complete | Documents 01-05 plus overall/roadmap/F2 handoff alignment |
| P15-M1 target API | Not started | generic target/result values, validation and repository caller migration |
| P15-M2 metadata evolution | Not started | legacy/new mixed-chain fake/real Oxia parity and lazy repair |
| P15-M3 core adapter split | Not started | Object WAL sync behavior through generic adapter and split committer/materializer |
| P15-M4 recovery/lifecycle | Not started | retained exact attempts, multi-page recovery, seal/delete and race gates |
| P15-M5 final acceptance | Not started | Phase 1 regression + production Oxia/Object WAL restart + F2 prerequisite gate |

F2-M1 production implementation resumes only after P15-M5. Pure review work may continue, but F2 must not create a
second temporary L0 API or expose `NereusManagedLedger` against a partially implemented Phase 1.5 surface。

## 6. Support Matrix at Exit

| Capability | P15-M5 state |
| --- | --- |
| legacy Phase 1 Object WAL records | readable/repairable |
| new generic-target Object WAL records | implemented/new-write |
| `OBJECT_WAL_SYNC_OBJECT` strict append/read | implemented and regression-gated |
| exact in-process append recovery | implemented |
| seal/logical delete | implemented |
| BookKeeper target value/codec | implemented reservation, no IO support |
| BookKeeper profiles | reserved/rejected before IO |
| `WAL_DURABLE` success | reserved/rejected before IO |
| async materialization/higher generation | Future 4 designed, not implemented |
| physical object deletion | not implemented |

## 7. Planned Aggregate Gates

Implementation adds the following aggregate tasks；they do not exist merely because this design document exists：

```bash
./gradlew phase15Check
./gradlew phase15FinalCheck --rerun-tasks
```

`phase15Check` must include all ordinary Phase 1 gates plus new API/codec/fake-metadata/core tests。
`phase15FinalCheck` must additionally run real Oxia mixed-version restart/recovery/lifecycle scenarios and the
F2 L0 prerequisite contract suite。Neither gate may weaken or rename the existing `phase1Check` /
`phase1FinalCheck` tasks。
