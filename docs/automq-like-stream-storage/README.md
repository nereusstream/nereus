# AutoMQ-like Async Materialization Profile

> 状态：Designed / API-reserved，尚未实现端到端执行路径
> 前置：Future 1 stable append、Phase 1.5 generic read target/stable-commit split、Future 4 generation publish

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
- Phase 1.5 code-level design for generic target/adapter and stable-commit/materialization separation（design only）。

Not present：

- core branch implementing `WAL_DURABLE` success；
- BookKeeper WAL writer/reader/location types；
- materialization task/checkpoint store；
- background worker and generation committer；
- primary-WAL retention gate、lag backpressure and recovery daemon；
- mixed primary target resolver。

Creating metadata with an async profile does not mean the current core can execute it。M4 must fail unsupported
profiles with `UNSUPPORTED_STORAGE_PROFILE` and non-strict durability with
`UNSUPPORTED_DURABILITY_LEVEL` until these gates close。

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
3. stream-head CAS succeeds or replay proves it already succeeded；
4. stable offset range、commitVersion and projection identity are available；
5. the commit record contains a recoverable primary read target。

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
`../phase-1.5-core-storage-foundation/`。They remain design until P15-M1-M5 are implemented；even after P15-M5,
`WAL_DURABLE` and the async coordinator remain separate gates rather than implied support。

## 6. State machine

```text
ACCEPT
  -> SESSION_VALID
  -> PRIMARY_WAL_DURABLE
  -> COMMIT_INTENT_STORED
  -> HEAD_COMMITTED
  -> ACK_IF_REQUESTED_BOUNDARY_MET
  -> TASK_DURABLE
  -> OUTPUT_UPLOADED
  -> GENERATION_PUBLISHED
  -> CHECKPOINT_ADVANCED
  -> PRIMARY_WAL_RELEASED_WHEN_SAFE
```

Ordering rules：

- materialization task identity derives from committed source range/commitVersion/checksum；
- task enqueue can be repaired from committed ranges if crash happens after ack；
- generation publish is idempotent and never changes offsets/projection；
- checkpoint lag cannot be used to conclude generation is unpublished；index is authoritative for publish；
- WAL release waits for published generation plus readers/cursors/retention/catalog references。

## 7. Durable metadata

Target records（key names remain design-level until F4 schema freeze）：

```text
/streams/{streamId}/materialization/tasks/{taskId}
/streams/{streamId}/materialization/checkpoints/{workerId}
/streams/{streamId}/wal-ranges/{offsetEnd}/{commitVersion}
/streams/{streamId}/offset-index/{offsetEnd}/{generation}
/objects/{objectId}/references/materialization/{taskId}
```

Task identity includes：

- stream/range and source commitVersion；
- primary target identity/checksum；
- desired output kind/format/policy version；
- deterministic target generation request or publish token。

Task state is workflow state，not stream visibility truth。

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
| ack succeeds before task record | task scanner reconstructs from committed source range |
| task exists before upload | worker retries idempotently |
| upload succeeds before generation publish | reuse deterministic output or orphan GC |
| generation publishes before checkpoint | checkpoint repair from index |
| output corrupt | do not publish；if detected later, retain/fallback to valid lower generation |
| object store outage | primary BK reads may continue；Object-WAL profile degrades according to primary availability |
| WAL GC races worker | `WalRetentionGate` blocks delete until all requirements are safe |

## 12. Implementation gate

Before async profile can move from `Reserved` to `Implemented`：

- generic primary read-target API and result model；
- real BookKeeper adapter for BK profiles；
- `WAL_DURABLE` success/replay/read-after-success contract tests；
- durable task/checkpoint/idempotence schema；
- higher-generation conditional publish tests；
- task-loss scanner and all partial-failure recovery paths；
- primary-WAL retention/reference integration；
- profile migration and remote-offload compatibility policy；
- metrics/backpressure/close behavior；
- Phase 1 and overall docs updated in the same change。

## 13. References

- `../design/nereus-overall-architecture.md`
- `../design/nereus-commit-protocol.md`
- `../design/nereus-future1-core-stream-storage.md`
- `../design/nereus-future4-compaction-generation.md`
- `../phase-1-core-stream-storage/README.md`
