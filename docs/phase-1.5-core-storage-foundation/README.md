# Phase 1.5 Core Storage Foundation

> 状态：P15-M0-M6 implemented/final-gated；P15-M6 passed ordinary and Docker-backed gates on 2026-07-12
> 输入基线：`nereusstream/nereus@8aa684f479994094a612578bbfe27170b077f4ad`
> 实现基线：Phase 1.5 working tree based on `da49a97`
> 下一里程碑：F2-M5 broker integration（F2-M1-M4 complete）

Phase 1.5 是插入 Future 1 与 Future 2 production implementation 之间的 L0 foundation delivery。
它不是新的 capability track，也不改变 Future 1-8 的编号。它把 Phase 1 已证明的
`OBJECT_WAL_SYNC_OBJECT` 实现重构成可承载多种 physical read target 的 core，并完成最新
F2-M0R 已锁定的 append recovery 与 stream lifecycle 合同。

Phase 1.5 当前运行时支持面仍然只有：

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
11. F2 exact local snapshot advancement also consumes protocol-neutral `AppendResult.cumulativeSize`；the internal
    committed record already owns that value, but the public result addition is P15-M6。

## 4. Document Map

| Document | Authority |
| --- | --- |
| `01-api-and-read-target-contract.md` | exact public value/API shape, validation and compatibility |
| `02-metadata-schema-and-compatibility.md` | generic target codecs, new records/keys, dual-read/new-write and repair |
| `03-append-recovery-and-lifecycle.md` | retained attempts, paged recovery, seal/delete and mutation ordering |
| `04-core-abstractions-and-state-machines.md` | primary-WAL adapter seam, strict append/read dispatch, resources and close |
| `05-implementation-plan-and-gates.md` | P15-M1 through M5 implementation plus P15-M6 follow-up files, tests, release gates and stop conditions |

The implemented Phase 1 contract in `../phase-1-core-stream-storage/` remains the legacy compatibility baseline；
this directory is authoritative for the implemented Phase 1.5 evolution and its preserved compatibility rules。

## 5. Delivery Milestones

| Milestone | State | Exit |
| --- | --- | --- |
| P15-M0 code-level design | Complete | Documents 01-05 plus overall/roadmap/F2 handoff alignment |
| P15-M1 target API | Complete | generic target/result values, validation and repository caller migration |
| P15-M2 metadata evolution | Complete | legacy/new mixed-chain fake/real Oxia parity and lazy repair |
| P15-M3 core adapter split | Complete | Object WAL sync behavior through registry/adapters and split committer/materializer |
| P15-M4 recovery/lifecycle | Complete | retained exact attempts, multi-page recovery, seal/delete and race gates |
| P15-M5 final acceptance | Complete | Phase 1 regression + production Oxia/Object WAL restart + F2 prerequisite gate |
| P15-M6 F2 result-snapshot handoff | Complete | Protocol-neutral `AppendResult.cumulativeSize` from existing committed truth；constructor/normal/later-head-recovery/regression gates |

P15-M5 remains passed for its original generic-target/recovery/lifecycle scope。F2-M0R2 subsequently proved that
`AppendResult` needs one already-known cumulative logical-size field so a facade can advance an exact local snapshot
without a fallible post-commit reread。P15-M6 has implemented and final-gated that surface；F2-M1 must consume it and
must not create a second temporary L0 API。

## 6. Support Matrix at Exit

| Capability | Phase 1.5 exit state |
| --- | --- |
| legacy Phase 1 Object WAL records | readable/repairable |
| new generic-target Object WAL records | implemented/new-write |
| `OBJECT_WAL_SYNC_OBJECT` strict append/read | implemented and regression-gated |
| exact in-process append recovery | implemented |
| seal/logical delete | implemented |
| `AppendResult.cumulativeSize` public handoff | implemented/final-gated |
| BookKeeper target value/codec | implemented；later F1-BK adds exact writer/reader and BK-M3 task/source support |
| BookKeeper profiles | production reserved/rejected before IO；BK_ONLY module-local M2 runtime and BK async M3 source/profile/lag checkpoints now exist |
| `WAL_DURABLE` success | reserved/rejected by Phase 1.5；later implemented/final-gated for F4 `OBJECT_WAL_ASYNC_OBJECT` only |
| async materialization/higher generation | reserved at Phase 1.5 exit；later F4 `OBJECT_WAL_ASYNC_OBJECT` implemented/final-gated；BookKeeper primary variants reserved |
| physical object deletion | absent at Phase 1.5 exit；later F4 implementation is final-gated and exact-activation-only with safe defaults closed |

Phase 1.5 之后的 BookKeeper primary-WAL 实现合同已单独冻结为 F1-BK：
`../phase-bk-bookkeeper-primary-wal/README.md`。它复用这里的 tagged target、registry、stable-head/gen0 split 和
generic recovery。F1-BK 已实现 BK-M1、BK-M2 的 module-local runtime，并推进 BK-M3 source/protection/profile/lag
checkpoint；production rollout 仍未完成，不能把 reserved codec/registry seam 或 partial checkpoint 当成 broker
profile support。

## 7. Aggregate Gates

The implementation provides and passes these aggregate tasks：

```bash
./gradlew phase15Check
./gradlew phase15FinalCheck --rerun-tasks
```

`phase15Check` includes all ordinary Phase 1 gates plus new API/codec/fake-metadata/core tests。
`phase15FinalCheck` additionally runs real Oxia mixed-version restart/recovery/lifecycle scenarios through the
unchanged `phase1FinalCheck` Docker gates。Neither gate weakens or renames `phase1Check` / `phase1FinalCheck`。
The P15-M6 invocations passed on 2026-07-12 with cumulative-result validation、normal append and later-head recovery
fixtures in these same task names；they now serve as the F2-M1 entry gate。
