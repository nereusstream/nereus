# AutoMQ-like Async Materialization Profile

> 状态：Implementation in progress / F4-M1–M3 final-gated、M4 through checkpoint W、M5 through checkpoint AG；
> production Object-WAL resolver/read-repair/materialization runtime 与 Pulsar exact profile/config mapping 已装配
> 前置：Future 1 stable append、Phase 1.5 generic read target/stable-commit split、Phase 3 retention；
> 精确 target contract 见 `../phase-4-compaction-generation/`

本文定义 Nereus 中的 AutoMQ-like 边界。它不是独立 engine，也不是“WAL 成功后跳过 Oxia”：

```text
primary WAL durable
+ stream-head stable commit
+ immediate primary-WAL recovery/read path
+ background object generation
```

## 1. Motivation

Sync object publication 简单、可预测，但把 index/object publication 延迟放在 producer path。
Async profile 允许 topic 优先 producer latency，把 secondary/read-optimized object generation、remote
retention 和 lakehouse-ready work 移到后台，同时保持同一个 offset/session/commit truth。

## 2. Current repository boundary

Already present：

- `BOOKKEEPER_WAL_ASYNC_OBJECT` / `OBJECT_WAL_ASYNC_OBJECT` enum values；
- `DurabilityLevel.WAL_DURABLE` contract name；
- canonical profile persistence in stream metadata；
- Object WAL v1 bytes and Phase 1 head-CAS/commit-log primitives。
- Phase 1.5 generic target/adapter and stable-commit/materialization separation（implemented and final-gated）。
- F4-M0 task/generation/recovery/lease/GC/rollout code-level design and M1–M6 gates。
- F4-M1 API/metadata records/codecs/store surface、guarded object IO、physical reference values and durable
  reader/protection handshakes（implemented and ordinary/Docker-backed final-gated）。
- F4-M2 committed-generation resolver/read path and restart-safe publication, plus F4-M3 planner/task/worker/service、
  exact-source protection、Parquet/NTC1 formats and terminal workflow-metadata retirement（final-gated）。
- F4-M4 through checkpoint W：NRC1/protected append/recovery、root/journal fencing、typed source retirement、
  completed-trim/COMMITTED/TOPIC_COMPACTED eligibility、grace-fenced higher-generation pre-drain and the durable
  generation-activation metadata authority foundation，plus activation-gated future sentinel and five ownerless-global
  reference domains. New strict appends now establish `REACHABLE_APPEND` before head CAS and `VISIBLE_GENERATION`
  before success；registration/backfill/readiness/activation proofs and physical/cursor live-reference backfill are
  implemented.
- F4-M5 checkpoints AD–AE：the opt-in Phase 4 resolver implements `WAL_DURABLE` after the protected stable head；
  generation-zero restart/read repair is durable, and every async append now has an exact per-stream-lane admission
  seam that resolves the F2 projection, obtains/revalidates the generation marker proof, then applies authoritative
  materialization-lag throttle/reject logic before primary Object-WAL preparation or upload.
- F4-M5 checkpoint AF：`DefaultNereusRuntimeProvider` atomically installs that resolver/admission seam with the
  generation-aware read path、NRC1 replay/index repair、generation-zero source repair and owned background
  materialization service. Pulsar maps exact sync/async first-create profiles and the complete bounded
  materialization configuration; sync remains the default and async remains proof-gated.
- F4-M5 checkpoint AG：exact policy/config/evidence values、source-index-verified stable retention planning and the
  ownership/activation/final-authority gated F3 logical-trim delegation are implemented. Missing/incomplete/stale
  evidence can only stop or reduce a candidate, and the returned promise never waits for physical GC.

Not present：

- BookKeeper WAL writer/reader/location types；
- production-composed global-domain source retirement and physical/cursor/root/audit GC completion；
- primary-WAL retention gate and destructive GC daemon composition；
- Pulsar retention policy/admin mapping、shared bounded retention lane、managed-ledger service installation、
  cursor-snapshot candidate/deletion、object inventory and registration retirement；
- mixed primary target resolver。

The production provider now installs the complete Object-WAL Phase 4 unit. Merely setting the async broker default
still does not bypass rollout safety：it affects only first-create projections, and every async append must obtain and
revalidate the durable generation activation/marker proof before primary IO. Existing topics retain their stored
profile.

Phase 4 的首个执行范围只是 `OBJECT_WAL_ASYNC_OBJECT`。该 profile 仍在 success 前完成
primary Object WAL upload 和 stable head commit；后台化的是 secondary/read-optimized generation。
`BOOKKEEPER_WAL_ASYNC_OBJECT` 需要以后的 BookKeeper primary adapter，不能由 F4 的 worker
名义推导为已支持。

## 3. Profile matrix

| Profile | Primary read source immediately after stable commit | Background publication | Default level |
| --- | --- | --- | --- |
| `BOOKKEEPER_WAL_ONLY` | BK range | none | `WAL_DURABLE` |
| `BOOKKEEPER_WAL_SYNC_OBJECT` | object-backed generation | compaction only | `WAL_DURABLE_AND_INDEX_COMMITTED` |
| `BOOKKEEPER_WAL_ASYNC_OBJECT` | BK range | object-backed/read-optimized generation | `WAL_DURABLE` |
| `OBJECT_WAL_SYNC_OBJECT` | Object WAL generation 0 | compaction only | `WAL_DURABLE_AND_INDEX_COMMITTED` |
| `OBJECT_WAL_ASYNC_OBJECT` | Object WAL/repairable generation 0 | read-optimized generation | `WAL_DURABLE` |

`OBJECT_WAL` is the deprecated alias of `OBJECT_WAL_SYNC_OBJECT`。

## 4. Non-negotiable ack contract

`WAL_DURABLE` success requires：

1. primary WAL bytes and checksums durable；
2. immutable commit intent written/reused；
3. exact physical root is ACTIVE and commit-intent-owned `REACHABLE_APPEND` protection has completed its root/owner
   post-check；
4. stream-head CAS succeeds or replay proves it already succeeded；
5. stable offset range、commitVersion and projection identity are available；
6. the commit record contains a recoverable primary read target。

It may return before：

- generation-0 derived index confirmation；
- secondary object upload；
- higher-generation publish；
- compacted/lakehouse object creation。

Read-after-success must either resolve the primary target or repair the missing derived index within the
documented read timeout/budget。Acknowledging data that cannot be recovered/read from committed metadata is
not allowed。

## 5. Shared core abstractions

Target split：

```text
core/common
  StreamProfileResolver
  PrimaryWalAppender / PrimaryWalReader
  StableAppendCommitter
  AppendSessionManager
  ReadTargetResolver
  MaterializationLagTracker

core/sync
  StrictAppendCoordinator

core/async
  AsyncAppendCoordinator
  MaterializationPlanner
  MaterializationWorker
  GenerationCommitter
  WalRetentionGate
```

Required target interfaces（pseudo-code）：

```java
interface PrimaryWalAppender {
    CompletionStage<PrimaryWalAppend> append(PrimaryWalRequest request);
}

interface StableAppendCommitter {
    CompletionStage<StableAppend> commit(StableAppendRequest request);
}

interface Materializer {
    CompletionStage<MaterializationOutput> materialize(MaterializationTask task);
}
```

Phase 1.5 freezes the concrete `ReadTarget` union, generic metadata and adapter seam in
`../phase-1.5-core-storage-foundation/`。P15-M0-M6 are implemented and final-gated；nevertheless,
`WAL_DURABLE` and the async coordinator remain separate F4 gates rather than implied support。F4 replaces the
reader registry's coarse target-type dispatch with an exact target/version/object-type/physical-format key before a
compacted object can share `ObjectSliceReadTarget` safely。
F4 also splits commit-intent preparation from the head CAS so the physical protection can be installed between them；
historical root backfill alone cannot make concurrent new appends safe for GC。

## 6. State machine

```text
ACCEPT
  -> SESSION_VALID
  -> PRIMARY_WAL_DURABLE
  -> COMMIT_INTENT_STORED
  -> PHYSICAL_ROOT_ACTIVE
  -> REACHABLE_APPEND_PROTECTED
  -> HEAD_COMMITTED
  -> ACK_IF_WAL_DURABLE
  -> GENERATION_ZERO_INDEX_DURABLE
  -> VISIBLE_GENERATION_PROTECTED
  -> ACK_IF_INDEX_COMMITTED
  -> TASK_DURABLE
  -> TASK_CLAIMED
  -> OUTPUT_READY
  -> INDEX_PREPARED
  -> INDEX_COMMITTED
  -> TASK_PUBLISHED
  -> CHECKPOINT_ADVANCED
  -> PRIMARY_WAL_RELEASED_WHEN_SAFE
```

Ordering rules：

- materialization task identity derives from committed source range/commitVersion/checksum；
- no head CAS is sent before `REACHABLE_APPEND`; no strict success is returned before the exact generation-zero index
  owns `VISIBLE_GENERATION`；async success skips the index wait only；
- task enqueue can be repaired from committed ranges if crash happens after ack；a 64-shard durable stream registry
  lets a fresh runtime find the stream without broker topic ownership, but head/index/task remain truth；
- output key carries content SHA + durable output-attempt id and is generation-neutral；generation allocation happens
  during publication and a deleted key is never reused；
- every actual provider PUT/retry revalidates its durable owner and physical root；after dual HEAD-absence and owner/
  domain proof, long-grace retirement may remove DELETED root/reference/manifest audit metadata without reopening key
  reuse；
- generation publish is the final index key's idempotent same-key `PREPARED -> COMMITTED` CAS and never changes
  offsets/projection；
- checkpoint lag cannot be used to conclude generation is unpublished；index is authoritative for publish；
- WAL release waits for published generation plus readers/cursors/retention/catalog references。

## 7. Durable metadata

F4-M0 target records（human-readable components shown；implementation uses `F4Keyspace` and encoded components）：

```text
/nereus/clusters/{cluster}/streams/{streamId}/materialization/v1/tasks/{taskId}
/nereus/clusters/{cluster}/streams/{streamId}/materialization/v1/checkpoints/{policyId}/{policyVersion019}
/nereus/clusters/{cluster}/streams/{streamId}/materialization/v1/generation-sequences/{viewId02}
/nereus/clusters/{cluster}/materialization/v1/stream-registry/{shard02}/{streamId}
/nereus/clusters/{cluster}/streams/{streamId}/offset-index/{offsetEnd019}/{generation019}
/nereus/clusters/{cluster}/streams/{streamId}/recovery/v1/root
/nereus/clusters/{cluster}/physical-objects/v1/{shard03}/roots/{objectKeyHash}
/nereus/clusters/{cluster}/physical-objects/v1/{shard03}/objects/{objectKeyHash}/readers/{processRunId}
/nereus/clusters/{cluster}/physical-objects/v1/{shard03}/objects/{objectKeyHash}/protections/{typeId02}/{referenceId}
```

The stream registry uses 64 shards for unloaded-stream work discovery. Physical roots use 256 first-hash-byte shards
so a fresh GC runtime can recover `MARKED/DELETING` state even after object deletion made storage listing empty；
listing remains orphan/audit discovery only.

Task identity includes：

- stream/range and source commitVersion；
- primary target identity/checksum；
- desired output kind/format/policy version；
- deterministic publication id；generation is allocated and recorded later，not part of task/output identity。

Task lifecycle is `PLANNED -> CLAIMED -> OUTPUT_READY -> PUBLISHING -> PUBLISHED`，with
`RETRY_WAIT/CANCELLED/TERMINAL_FAILED` side states。Task state is workflow state，not stream visibility truth。

## 8. Read selection

```text
resolve committed offset
  -> highest valid published generation
  -> otherwise generation-0 primary target
  -> if generation-0 index missing, repair from reachable commit
  -> read and validate target-specific bytes
```

For BK async，this requires a BK range reader。For Object WAL async，Object WAL already supplies primary bytes；
background work produces a larger/per-stream/read-optimized generation。

Reader must never wait indefinitely for materialization。If primary read is temporarily unavailable，it returns
the target-specific retriable error/backpressure defined by policy。

## 9. Retention and backpressure

Required signals：

| Signal | Meaning |
| --- | --- |
| `materializationLagRecords` | committed records without required secondary generation |
| `materializationLagBytes` | primary WAL bytes still protected |
| `oldestUnmaterializedAgeMs` | recovery/retention risk |
| `materializationRetryTotal` | task retries |
| `primaryWalRetentionBlockedTotal` | ranges blocked by generation/read/cursor/catalog refs |
| `primaryWalReadFallbackTotal` | reads served from primary target instead of higher generation |

Policy actions can throttle append、reserve more workers or mark the profile degraded。They cannot discard an
acknowledged primary range merely because materialization lags。

## 10. Remote storage/offload compatibility

- `BOOKKEEPER_WAL_ONLY` may coexist with legacy offload only under explicit adapter policy；legacy remote
  metadata is not Nereus generation truth。
- Any Nereus object-enabled profile owns its object generations；Pulsar offload/Kafka RLS must map to those
  generations or be rejected。
- Profile change applies to new commits after a barrier；existing ranges keep their original primary target and
  retention rules。
- Running two independent remote segment publishers for one range is forbidden。

## 11. Failure and repair

| Failure | Outcome / repair |
| --- | --- |
| crash before head commit | no success；retry stable append |
| head commit succeeds before response | replay returns same stable range |
| ack succeeds before task record / topic unloads | registry scanner reloads projection/head and reconstructs from committed source range |
| task exists before upload | worker retries idempotently |
| upload succeeds before generation publish | reuse deterministic output or orphan GC |
| generation publishes before checkpoint | checkpoint repair from index |
| output corrupt | do not publish；if detected later, retain/fallback to valid lower generation |
| object store outage | primary BK reads may continue；Object-WAL profile degrades according to primary availability |
| WAL GC races worker | `WalRetentionGate` blocks delete until all requirements are safe |

## 12. Implementation gate

Before `OBJECT_WAL_ASYNC_OBJECT` can move from `Reserved` to `Implemented`：

- F4-M1–M4 metadata/object lifecycle、generation、worker、checkpoint and GC gates；
- `WAL_DURABLE` success/replay/read-after-success contract tests；
- frozen durable task/checkpoint/idempotence codecs and higher-generation conditional-publish tests；
- all-shard registry/task-loss scanner and all partial-failure recovery paths；
- primary-WAL retention/reference integration；
- Pulsar monotonic `nereus.generation-protocol=1` activation and async-profile admission；
- metrics/backpressure/close behavior；
- F4-M5/M6 ordinary/final gates and all predecessor regressions；
- Phase 1/1.5/2/3 and overall docs updated in the same change。

BookKeeper async/sync profiles have an additional independent gate: a real BookKeeper writer/reader/location and
ledger-retention implementation. They do not block completion of the explicitly scoped Object-WAL async path。

## 13. References

- `../design/nereus-overall-architecture.md`
- `../design/nereus-commit-protocol.md`
- `../design/nereus-future1-core-stream-storage.md`
- `../design/nereus-future4-compaction-generation.md`
- `../phase-4-compaction-generation/README.md`
- `../phase-1-core-stream-storage/README.md`
