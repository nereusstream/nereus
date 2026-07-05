# 08 Risk Register And Design Compromises

本文集中记录 Phase 1 当前最容易误伤实现的风险假设和设计妥协。它不是替代前面文档的总纲，
而是进入 M2/M3/M4/M5 编码前必须复读的风险清单。

## 1. Oxia Conditional Multi-Write Capability

Status: high technical risk.

Phase 1 当前 `commitStreamSlice` 设计依赖 single stream key group 内的条件批提交。一次成功提交
必须原子完成：

- 校验 stream state、append session epoch/token、current committed end；
- 写入 `/offset-index/{offsetEnd}/{generation}`；
- 更新 `/committed-end-offset`；
- 写入 `/committed-slices/{objectIdComponent}/{sliceIdComponent}`；
- 使用同一个 durable `commitVersion` 填入三类记录。

风险点：Oxia public Java client 当前直接可见的是单 key conditional put/delete/list/rangeScan
API；底层 proto 有 batched write request，但这不等价于 Java client 已经提供 Nereus 需要的
single-key-group conditional multi-write API。Pulsar 能使用 Oxia 作为 metadata backend，也不能
证明 Pulsar generic metadata API 足以承载 Nereus append visibility commit。

实现门禁：

- M2 必须先写 capability spike 探针代码，直接使用计划采用的 Oxia Java client 版本；
- spike 必须证明多 key put/CAS 在同一 partition key/key group 内是原子提交；
- spike 必须验证失败时没有 partial apply，并能把 version conflict 映射为 Nereus 错误；
- fake store 只能模拟 spike 已证明的能力，不能让 fake semantics 比真实 adapter 更强；
- M4 `AppendCoordinator` 不能在该 spike 通过前进入真实 append 线性化实现。

失败动作：如果 Java client 没有该能力，停止 `commitStreamSlice` 当前设计，重新设计线性化点。
首选替代方向是一个 authoritative stream-head record CAS，把 offset index 和 committed-slice
marker 改成可修复派生索引；具体协议必须回写到 `02`、`04`、`06`、`07` 后再继续。

## 2. Read Amplification From Full-Slice Verification

Status: accepted Phase 1 compromise with hard resource guards.

Phase 1 read 为了校验 slice checksum，必须读取完整 resolved slice payload 和 entry index 后再按
`startOffset`、`maxRecords`、`maxBytes` clip。这样能保证返回的 clipped bytes 来自一个完整校验过
的 slice，但会带来读放大：例如 16 MiB WAL slice 中只返回 100 bytes，也要下载并校验完整 slice。

实现契约：

- `WalObjectReader` 不允许无界分配完整 slice buffer；
- `DefaultStreamStorage` 必须配置 `maxConcurrentObjectReads` 和 `maxReadBufferBytes`；
- 每个 resolved range read 前按 `ResolvedObjectRange.objectLength + entryIndexLength` 的 checked sum
  预留 read buffer bytes；
- 预留失败时返回 retriable `BACKPRESSURE_REJECTED`，不启动 object range read；
- `maxObjectBytes <= maxReadBufferBytes` 是 Phase 1 full-slice reader 的启动校验；
- reader 必须在 success、decode failure、checksum failure、cancel、timeout、close 所有终态释放预留；
- read amplification metrics 必须区分 slice payload/index bytes downloaded 和 payload bytes returned。

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

- `TIMEOUT` after `commitStreamSlice` sent 必须明确表示 unknown final state；
- `CANCELLED` after irreversible boundary 也不能伪装成未提交；
- metadata layer 必须先通过 committed-slice marker 尝试恢复同一物理 slice 的原始结果；
- 丢失同一物理 attempt 上下文后的重复提交风险必须保留给上层协议 projection 解决；
- 不允许在 core 中用 payload checksum、schema refs、tags、object id 近似推断 producer dedup。

测试证据：append timeout/cancellation tests 必须区分 before upload、during upload、during manifest、
after commit RPC sent、after ack lost 五类边界，并断言最后一类不会返回确定失败。

## 4. No GC And Object Deletion In Phase 1

Status: accepted correctness boundary, local test cleanup required.

Phase 1 `trim` 只推进 low-watermark，不删除 offset index，也不删除 object bytes。Upload 后 manifest
失败、commit 失败、process crash、caller timeout 都可能留下 orphan WAL objects。

实现契约：

- production `ObjectStore` Phase 1 不暴露 delete；
- read/recovery correctness 不依赖 object list；
- `ObjectReferenceRecord` 和 manifest state 只是审计/未来 GC 输入；
- orphan scanner 可以用于诊断或测试断言，但不能让 committed read 依赖 list 结果；
- `LocalFileObjectStore` 测试实现必须支持隔离 root 下的 test-only cleanup helper，例如
  `deleteAllForTesting()` 或 fixture-level `cleanupRoot()`；
- cleanup helper 必须只在 testing package 暴露，并且拒绝清理注入 root 之外的路径。

本地/CI 风险：长时间跑 append/timeout/orphan 测试会快速堆积 WAL files。测试必须为每个 test class
或 test method 使用独立 temp root，并在 teardown 调用 cleanup helper。

## 5. Strategy Versus Mechanism For Single-Stream Objects

Status: accepted implementation simplification, not a WAL format limitation.

Phase 1 core planner 可以先设置 `forceSingleStreamObject=true`，即一个 WAL object 只承载一个 stream
的 append work item。这能降低 append coordinator、manifest commit loop 和 failure classification
的复杂度。

机制层契约：

- `WalObjectWriter` 必须从第一版支持多个 `WalStreamSliceInput`；
- WAL header、slice directory、entry index、manifest slice list 都必须按 multi-slice 编码；
- `forceSingleStreamObject=true` 只能做 validation，不能让 writer split/rewrite request；
- `forceSingleStreamObject=false` 的 direct writer tests 必须覆盖 multi-stream/multi-slice object；
- `DefaultStreamStorage` 初期使用单 stream object 是 planner 策略，后续可在不改 WAL format 的前提下
  改成 cross-stream batching。

停线条件：如果实现过程中发现 writer/reader/manifest 只能处理一个 slice 或一个 stream，必须先补齐
WAL 机制层，再继续 core append。

## 6. Review Checklist Before Coding

开始实现前逐项确认：

- Oxia capability spike 已经对真实 Java client 跑过，并记录结果；
- `FakeOxiaMetadataStore` 没有比真实 Oxia adapter 更强的 commit 语义；
- `StreamStorageConfig` 包含 read memory/concurrency 限流项；
- `WalObjectReader` full-slice read 先预留内存，后读取对象；
- read amplification metrics 已在 `StreamStorageMetrics` 命名；
- timeout/unknown-final-state 错误不会被映射成普通 retriable conflict；
- LocalFileObjectStore cleanup helper 只在 test package 可见；
- multi-slice writer tests 在 core planner 仍为 single-stream 策略时也必须存在。
