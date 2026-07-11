# 08 Risk Register And Design Compromises

本文集中记录 Phase 1 当前最容易误伤实现的风险假设和设计妥协。它不是替代前面文档的总纲，
而是后续 Future 编码和 review 前必须复读的风险清单。Phase 1 M0-M8 已实现并通过 final gate；
本文件中的 accepted compromises 仍然有效。

## 1. Oxia Conditional Multi-Write Capability

Status: confirmed unsupported by selected public Java API; Phase 1 design now uses stream-head single-key
CAS as the mitigation.

The original `commitStreamSlice` design depended on a conditional multi-key write inside one stream key
group. A successful append would have atomically checked stream state/session/current committed end and
written offset-index, committed-end, and committed-slice records in one batch. That is no longer the Phase
1 implementation contract.

M0.5 探针结果：`nereus-metadata-oxia` 已增加
`:nereus-metadata-oxia:oxiaCapabilitySpike`，使用 `io.github.oxia-db:oxia-client:0.9.0`、
`io.github.oxia-db:oxia-testcontainers:0.7.4`、`org.testcontainers:junit-jupiter:1.20.4` 和
`oxia/oxia:0.16.3`。该任务验证了 partition-key routing、单 key CAS、fixed-width offset key
ordering 和 sequence key 行为，同时通过 public API reflection 确认当前选定 Java API 没有可支撑
Nereus 原始设计的 multi-key conditional write/transaction primitive。报告结果为
`NOT_SUPPORTED_BY_PUBLIC_JAVA_API`。

Confirmed constraint: Oxia public Java client 当前直接可见的是单 key conditional put/delete/list/rangeScan
API；底层 proto 有 batched write request，但这不等价于 Java client 已经提供 Nereus 需要的
single-key-group conditional multi-write API。Pulsar 能使用 Oxia 作为 metadata backend，也不能
证明 Pulsar generic metadata API 足以承载 Nereus append visibility commit。

旧的 multi-key atomic commit 设计已归档到 `09-legacy-oxia-multi-key-commit-design.md`。除非新的
Oxia capability spike 证明 public Java API 已支持所需事务语义，否则该归档不能作为实现依据。

Current design:

- `/streams/{streamId}/head` is the authoritative stream record and the only append linearization key；
- `StreamCommitRecord` is an immutable commit-log intent written before head CAS；
- head CAS advances committed end, cumulative size, commitVersion, and lastCommitId；
- `/offset-index/{offsetEnd}/{generation}` and `/committed-slices/{objectId}/{sliceId}` are materialized
  derived records copied from a reachable commit-log record；
- producer ack waits for both head CAS and materialization of this slice's offset-index/marker；
- exceptional append outcomes are explicit：pre-head `KNOWN_NOT_COMMITTED`，unconfirmed head response
  `MAY_HAVE_COMMITTED`，and confirmed head with incomplete index/result `KNOWN_COMMITTED`；same-slice
  retry/read repair must finish materialization before any new physical append。

Residual risks introduced by this mitigation:

- read/resolve has a new repair path when stream head is ahead of offset-index；
- commit-log chain walking can be more expensive than a pure offset-index lookup after failures；
- normal new append at current committed end and compatible metadata-only head updates skip historical
  replay walking；the bound applies only when resolving an older possibly committed attempt；
- orphan commit intents can exist when a commit-log put succeeds but head CAS loses；
- orphan intents are never visible by existence alone and can be reused only when their predecessor,
  offset range, cumulative size, and commitVersion match the current head snapshot；
- same-writer renew and trim now contend on the same head key as append; append must retry compatible
  head-version conflicts instead of treating them as fencing or generic condition failure；
- derived-index repair must remain bounded; repair budget exhaustion is operational backpressure, not
  metadata corruption；
- the bound counts every scanned commit record，not only missing records written；budget exhaustion returns a
  continuation cursor with the original observed-head anchor and exact next chain tuple so retries progress
  without rescanning the newest chain segment or accepting a different/orphan commit；
- fake metadata must model failure between head CAS and materialization, otherwise tests will miss the
  most important new recovery boundary；
- 2026-07-06 M2 hardening pass added tests and implementation for canonical `commitId` event-time/projection
  coverage without delimiter collisions, full canonical replay validation, decoded metadata
  `offset + length` overflow rejection, committed-slice-marker-first replay, and a fake-store
  object-audit/reference failure injection point。

实现门禁：

- M0.5 spike task 必须保持可编译，并在 Docker/Testcontainers 可用时可运行；
- `phase1Check` 只编译 spike，不启动 Docker；
- M2 必须按 stream-head CAS + commit-log + derived index 协议实现 fake store；
- fake store 只能模拟真实 public adapter 可表达的能力，不能让 fake semantics 比真实 adapter 更强；
- M4 `AppendCoordinator` 不能调用或假设任何 multi-key Oxia commit primitive；
- read resolver 必须能在 offset-index gap below head 时触发 derived-index repair。
- head CAS 失败分类必须区分 compatible head-version conflict、stale epoch/token、offset conflict 和
  metadata corruption。
- M4 exceptional completion 必须携带 `AppendOutcome`；message text 不能充当 commit certainty contract。
- fake 与 M7 real adapter 必须共用 `Phase1ObjectManifestValidator`、dense reachable-chain validation、
  monotonic object-reference repair 和 tuple-bound continuation contract。

## 2. Read Amplification From Full-Slice Verification

Status: implemented Phase 1 compromise with hard resource guards and exact metrics；M3 reader and M5 core
guards are connected, and focused M5 tests cover amplification, timeout/cancel and backpressure release.

Phase 1 read 为了校验 slice checksum，必须读取完整 resolved slice payload 和 entry index 后再按
`startOffset`、`maxRecords`、`maxBytes` clip。这样能保证返回的 clipped bytes 来自一个完整校验过
的 slice，但会带来读放大：例如 16 MiB WAL slice 中只返回 100 bytes，也要下载并校验完整 slice。

实现契约：

- M3 `DefaultWalObjectReader` 不允许无界分配完整 slice buffer；它在读取 payload/index 前调用
  injected `ReadResourceGuard.reserve(objectLength + entryIndexLength)`；
- done: reader now treats a later positive entry that exceeds remaining `maxBytes` after earlier records
  were returned as a normal stop, not `READ_LIMIT_TOO_SMALL`；
- done: `DefaultStreamStorage` 配置并执行 `maxConcurrentObjectReads` 和 `maxReadBufferBytes`；
- done: M5 对 default reader 的顺序 range list 持有一个 permit，并按所有候选 range 中最大的
  `ResolvedObjectRange.objectLength + entryIndexLength` checked sum 预留 buffer；
- 预留失败时返回 retriable `BACKPRESSURE_REJECTED`，不启动 object range read；
- `maxObjectBytes <= maxReadBufferBytes` 是 Phase 1 full-slice reader 的启动校验；
- reader 必须在 success、decode failure、checksum failure、cancel、timeout、close 所有终态释放预留；
- M3 `WalReadObserver` 已暴露 slice payload bytes、entry-index bytes 和 returned payload bytes；
- done: M5 `ReadMetricsObserver` receives exact verified slice/index/returned byte counts from
  `WalReadResult`，并保留 backpressure/cache hit/miss hooks；observer failure does not alter read outcome。

观测指标：

```text
nereus_stream_read_full_slice_bytes_total
nereus_stream_read_entry_index_bytes_total
nereus_stream_read_returned_payload_bytes_total
nereus_stream_read_amplification_bytes_total
nereus_stream_read_inflight_slice_bytes
nereus_stream_read_backpressure_rejections_total
```

未来演进：块级 checksum 或 payload block index 可以降低小读放大，但 Future 启用前不能弱化
Phase 1 的完整 slice corruption detection。

## 3. Append Idempotency And Producer Dedup

Status: explicit non-goal for Phase 1.

Phase 1 只提供同一物理 slice 的 commit replay 幂等。它不识别 producer identity、producer
sequence、broker message id、Kafka producer epoch，因而不实现 producer-level dedup。

风险点：如果 `ACK_RESULT` 超时、响应丢失或调用方丢失同一物理 attempt 上下文，caller-level retry
会创建新的 WAL object/slice，并可能把相同业务 payload 追加到新的 offset range。

实现契约：

- `TIMEOUT/CANCELLED` after an unconfirmed head send 必须携带 `MAY_HAVE_COMMITTED`；confirmed head
  success 后的 materialization/result failure 必须携带 `KNOWN_COMMITTED`；
- irreversible boundary 后不能伪装成 `KNOWN_NOT_COMMITTED`；
- metadata layer 必须先通过 committed-slice marker 或 stream-head commit chain 尝试恢复同一物理
  slice 的原始结果；
- 丢失同一物理 attempt 上下文后的重复提交风险必须保留给上层协议 projection 解决；
- 不允许在 core 中用 payload checksum、schema refs、tags、object id 近似推断 producer dedup。

测试证据：append timeout/cancellation tests 必须区分 before upload、during upload、during manifest、
after stream-head CAS sent、after head CAS before materialization confirmed、after ack lost 六类边界，
并断言后三类不会返回确定失败。

## 4. No GC And Object Deletion In Phase 1

Status: accepted correctness boundary；M3 local cleanup and M6 metadata-driven diagnostics are implemented，
with no production delete API.

Phase 1 `trim` 只推进 low-watermark，不删除 offset index，也不删除 object bytes。Upload 后 manifest
失败、commit 失败、process crash、caller timeout 都可能留下 orphan WAL objects。

实现契约：

- production `ObjectStore` Phase 1 不暴露 delete；
- read/recovery correctness 不依赖 object list；
- `ObjectReferenceRecord` 和 manifest state 只是审计/未来 GC 输入；
- M6 `MetadataOrphanObjectScanner` 只接受外部提供的 object id，先用 reachable head chain repair
  references，再产生 diagnostic classification；它不 list object store；
- `MISSING_MANIFEST` / `UNREFERENCED_MANIFEST` 也不构成 delete proof；
- `OrphanObjectAssessment.deletionAllowed()` 在 Phase 1 永远返回 false；
- M3 `LocalFileObjectStore` 测试实现支持隔离 root 下的 test-only cleanup helper:
  `deleteAllForTesting()`；
- done: final symlink targets and symlink parents are rejected before `putObject`/`readRange`/`headObject`
  can follow them outside the injected root；
- cleanup helper 必须只在 testing package 暴露，并且拒绝清理注入 root 之外的路径。

本地/CI 风险：长时间跑 append/timeout/orphan 测试会快速堆积 WAL files。测试必须为每个 test class
或 test method 使用独立 temp root，并在 teardown 调用 cleanup helper。

## 5. Strategy Versus Mechanism For Single-Stream Objects

Status: implemented M4 planner simplification, not a WAL format limitation; M3/M4 writer mechanism supports
multi-slice/multi-stream while core submits one stream slice per object.

Phase 1 core planner 可以先设置 `forceSingleStreamObject=true`，即一个 WAL object 只承载一个 stream
的 append work item。这能降低 append coordinator、manifest commit loop 和 failure classification
的复杂度。

机制层契约：

- M3 `DefaultWalObjectWriter` 已从第一版支持多个 `WalStreamSliceInput`，并按
  `streamId.value()` 确定性排序编码；
- WAL header、slice directory、entry index、manifest slice list 都必须按 multi-slice 编码；
- `forceSingleStreamObject=true` 只能做 validation，不能让 writer split/rewrite request；M3 已在
  pre-upload guard 中覆盖；
- `forceSingleStreamObject=false` 的 direct writer tests 必须覆盖 multi-stream/multi-slice object；M3
  已覆盖 direct writer multi-slice round trip；
- `DefaultStreamStorage` 初期使用单 stream object 是 planner 策略，后续可在不改 WAL format 的前提下
  改成 cross-stream batching；M4 currently sets `forceSingleStreamObject=true` and validates the returned
  prepared result contains exactly one slice for the requested stream。

停线条件：如果实现过程中发现 writer/reader/manifest 只能处理一个 slice 或一个 stream，必须先补齐
WAL 机制层，再继续 core append。

## 6. Reserved Profiles And Object-Shaped Result API

Status: accepted Phase 1 limitation；must be resolved before BookKeeper or async execution。

`StorageProfile` and `DurabilityLevel` expose target names，but current `AppendResult` and
`ResolvedObjectRange` require object id/key/range。Phase 1 therefore supports only
`OBJECT_WAL_SYNC_OBJECT + WAL_DURABLE_AND_INDEX_COMMITTED`。

Risks：

- metadata can contain a reserved profile even though core cannot execute it；
- treating `WAL_DURABLE` as WAL-only ack would violate stable offset/read-after-ack semantics；
- representing BK ranges with synthetic object ids would leak an invalid durable identity；
- silent fallback to Object WAL sync would violate topic policy。

Controls：

- done: M4 rejects unsupported profile/durability before reservation/WAL prepare/upload with
  `UNSUPPORTED_STORAGE_PROFILE` / `UNSUPPORTED_DURABILITY_LEVEL`；
- profile metadata persistence is documented as reservation only；
- future `WAL_DURABLE` still requires intent + stream-head CAS + recoverable primary target；
- BookKeeper implementation is blocked on a generic primary read-target/result abstraction；
- no compatibility alias exists except deprecated `OBJECT_WAL -> OBJECT_WAL_SYNC_OBJECT`。

## 7. Review Checklist Before Coding

开始实现前逐项确认：

- M0.5 Oxia capability spike task 已存在；Docker/Testcontainers 可用时已经对真实 Java client
  跑过，并记录 `NOT_SUPPORTED_BY_PUBLIC_JAVA_API` 或新的能力结果；
- M2/M4 没有继续依赖原始 multi-key conditional write 假设；
- stream-head CAS、commit-log reachability、derived-index repair 已在 fake metadata 语义中覆盖；
- `FakeOxiaMetadataStore` 没有比真实 Oxia adapter 更强的 commit 语义；
- done: `commitId` 和 same-physical-slice replay validation 覆盖所有 canonical identity fields，且不使用可
  被合法 value 撞开的 delimiter-only 子编码；
- done: decoded metadata records 与 public API request values 一样拒绝 byte range `offset + length`
  overflow；
- done: fake metadata 能分别注入 head-CAS 后 derived-index 失败和 post-commit object-audit/reference
  失败；
- done: `EntryIndexReferenceRecord` decoded metadata validation enforces the same
  `INLINE`/`OBJECT_FOOTER`/`INDEX_OBJECT` shape rules as public `EntryIndexRef`；
- done: `Phase1MetadataCodecs` has strict UTF-8 decode plus per-record round-trip/golden/error-path tests；
- done: fake metadata stored values are codec-backed and use `Phase1MetadataCodecs` envelopes；
- done: M7 real adapter uses the same `Phase1MetadataCodecs` registry as the fake store；
- `StreamStorageConfig` 包含 read memory/concurrency 限流项；
- `WalObjectReader` full-slice read 先预留内存，后读取对象；
- read amplification metrics 已在 `StreamStorageMetrics` 命名；
- timeout/cancellation 的 `AppendOutcome` 不会被映射丢失或折叠成普通 retriable conflict；
- M4 validates canonical profile and strict durability before WAL IO；
- no reserved profile uses sentinel object identity or silent fallback；
- LocalFileObjectStore cleanup helper 只在 test package 可见；
- multi-slice writer tests 在 core planner 仍为 single-stream 策略时也必须存在。

## 8. Post-M8 Metadata And Resident-Memory Risks

Status: review findings fixed on 2026-07-11，with focused ordinary tests and the Docker-backed final gate
required before release。

The first M7 adapter and M5 cache implementation had four operational risks that did not change the
stream-head linearization protocol but could fail under production scale or concurrency:

- watch callbacks and blocking Oxia requests shared an executor，creating a pool-saturation deadlock when
  callbacks synchronously reloaded head state；
- fixed operation/watch executors used unbounded queues，so overload could move the memory risk from caches
  into pending tasks；
- `scanOffsetIndex(limit)` listed the complete key range and fetched every value before applying `limit`；
- core independently loaded stream metadata、trim and committed-end views and could assemble a torn head
  snapshot；
- cache/session/watch/lane maps retained stream state without a cardinality bound。

Current contract:

- request、operation and watch executors are isolated；
- `OxiaClientConfiguration.maxPendingOperations` bounds operation/watch queues；saturation returns retriable
  `BACKPRESSURE_REJECTED` and dropped watch hints do not affect correctness；
- Oxia range iteration stops and closes at `limit`；
- `StreamMetadataSnapshot` is hydrated from one head value and all views share one metadata version；
- `maxCachedStreams` bounds stream-level cache/session/watch state，per-stream index records are capped by
  `maxCommitChainScan`，and terminal append lanes are released；
- suspended append lanes remain resident on purpose because evicting an unresolved physical attempt would
  permit an unsafe new append；an operational reconciliation API is still a post-Phase-1 requirement。

The remaining full-slice payload allocation risk is still governed by Section 2; these metadata/cache fixes
do not introduce block-level checksums or production GC。
