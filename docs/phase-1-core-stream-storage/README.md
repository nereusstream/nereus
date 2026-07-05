# Phase 1 Core StreamStorage Detailed Design

本文档目录是 Future 1 / Phase 1 的代码级设计记录。目标是在不接 Pulsar、不实现
ManagedLedger facade 的前提下，先独立实现 L0 Core StreamStorage：

- stream 与 offset truth；
- Oxia metadata 与 append fencing；
- object WAL；
- offset index；
- read resolver；
- trim 与 GC 引用模型的 Phase 1 边界。

当前目录是持续迭代的 working design。后续代码实现与设计出现偏差时，应优先更新这里，
再修改实现。

## 1. Design Inputs

权威输入来自：

- `docs/design/nereus-design-index.md`
- `docs/design/nereus-terminology.md`
- `docs/design/nereus-future1-core-stream-storage.md`
- `docs/design/nereus-overall-architecture.md`
- `docs/design/nereus-commit-protocol.md`
- `docs/design/nereus-storage-object-format.md`
- `docs/phase0/repository-plan.md`

Phase 1 继承这些不变量：

1. 内部正确性只以 `streamId + offset` 为准。
2. Oxia 是 offset、visibility、fencing 的 authority。
3. Object store 只存 bytes，不决定可见性。
4. Producer ack 只能发生在 WAL bytes durable 且 Oxia offset index commit 成功之后。
5. 每个 stream 的可见 offset range 必须 dense。
6. Multi-stream WAL object 的可见性按 stream slice 独立提交。
7. Broker 或调用方本地状态只能是 cache，不能是 durable truth。
8. Read resolver 必须从 Oxia offset index 或经过校验的 index cache 开始。
9. Phase 1 不依赖 Pulsar、KoP、ManagedLedger、cursor、subscription、lakehouse catalog。

## 2. Repository Boundary

当前 Phase 0 仓库已有模块：

| Module | Phase 1 role |
| --- | --- |
| `nereus-api` | 对外暴露 protocol-neutral L0 API、value types、共享 key/hash helpers |
| `nereus-core` | `StreamStorage` 主实现、append/read/trim 状态机 |
| `nereus-metadata-oxia` | Oxia keyspace、CAS/commit adapter、metadata records |
| `nereus-object-store` | Object WAL layout、object IO、range read abstraction |
| `nereus-managed-ledger` | Phase 1 不接入 |
| `nereus-pulsar-adapter` | Phase 1 不接入 |
| `nereus-kop-adapter` | Phase 1 不接入 |

M0 scaffold migration 已完成。当前代码状态：

- `nereus-api.StreamStorage` 已迁移为 Phase 1 full API surface，旧 byte-array append/read skeleton
  已移除；
- `AppendResult` 已原地扩展为 Phase 1 result shape；
- `nereus-api` 已包含 Phase 1 的 protocol-neutral value records、错误模型和共享 key/hash helpers；
- `nereus-core` 已通过 Gradle 依赖连接到 `nereus-metadata-oxia` 和 `nereus-object-store`；
- `nereus-metadata-oxia` 已启用 `java-test-fixtures`，为 M2 的 `FakeOxiaMetadataStore` 保留测试夹具
  出口；
- root `phase1Check` 已存在，并包含 `checkPhase0`、L0 module tests 和 Phase 1 L0 dependency guard。

M0 只迁移脚手架和类型边界，不实现 append/read/trim 状态机。后续 M1 继续加固 API validation 和
单元测试，M2/M3 分别落 metadata 与 object WAL。

Phase 1 允许的依赖方向：

```text
nereus-core -> nereus-api
nereus-core -> nereus-metadata-oxia
nereus-core -> nereus-object-store
nereus-metadata-oxia -> nereus-api
nereus-object-store -> nereus-api
```

Shared helpers that affect durable identity or paths must live in `nereus-api` or a dependency-neutral
package owned by `nereus-api`, not in `nereus-metadata-oxia` or `nereus-object-store`. This includes
`KeyComponentCodec`, deterministic stream-name hashing, fixed-width offset/generation key encoding, and
writer/run hash helpers. Both metadata and object modules must call the same helpers rather than carrying
parallel implementations.

Phase 1 不允许：

- `nereus-core` 依赖 Pulsar 或 KoP；
- `nereus-api` 出现 Pulsar `Position`、`MessageId`、Kafka record classes 等外部协议类型；
- object store list 参与恢复或读路径正确性；
- object manifest 可见但 offset index 未提交的数据被 `read` 返回；
- `trim` 直接删除 object。

## 3. Document Map

| Doc | Purpose |
| --- | --- |
| `01-api-and-domain-model.md` | API、value object、错误模型、包结构 |
| `02-oxia-metadata-and-commit.md` | Oxia keyspace、metadata records、append session、offset index commit |
| `03-object-wal-and-index.md` | object WAL 格式、slice、entry index、object store abstraction |
| `04-core-state-machines.md` | append、read、resolve、trim、recovery 状态机 |
| `05-implementation-plan-and-tests.md` | 代码落地顺序、测试矩阵、验收条件 |
| `06-metadata-oxia-position-and-pulsar-reference.md` | `nereus-metadata-oxia` 的横切定位和 Pulsar Oxia 参考边界 |
| `07-implementation-contract-checklist.md` | 实现前必须遵守的支持范围、stop-the-line 条件和测试 gate |
| `08-risk-register-and-design-compromises.md` | 当前已知高风险假设、Phase 1 设计妥协和实现门禁 |

## 4. Phase 1 Public Surface

Phase 1 的目标 API 不是 Pulsar adapter API，而是 protocol-neutral stream API：

```java
public interface StreamStorage extends AutoCloseable {
    CompletableFuture<StreamMetadata> createOrGetStream(
            StreamName streamName,
            StreamCreateOptions options);

    CompletableFuture<AppendSession> acquireAppendSession(
            StreamId streamId,
            AppendSessionOptions options);

    CompletableFuture<AppendResult> append(
            StreamId streamId,
            AppendBatch batch,
            AppendOptions options);

    CompletableFuture<ReadResult> read(
            StreamId streamId,
            long startOffset,
            ReadOptions options);

    CompletableFuture<ResolveResult> resolve(
            StreamId streamId,
            long startOffset,
            ResolveOptions options);

    CompletableFuture<Void> trim(
            StreamId streamId,
            long beforeOffset,
            TrimOptions options);

    CompletableFuture<StreamMetadata> getStreamMetadata(StreamId streamId);

    @Override
    void close();
}
```

M0 后，`nereus-api` 里的 `StreamStorage` 已按此 API 形状迁移；append/read/trim 的实际状态机仍在
后续 M4/M5/M6 实现。

## 5. Initial Code Package Plan

建议包结构：

```text
io.nereus.api
  StreamStorage
  StreamId
  StreamName
  OffsetRange
  StreamMetadata
  StreamCreateOptions
  AppendSessionOptions
  AppendSession
  AppendBatch
  AppendOptions
  AppendResult
  ReadOptions
  ReadResult
  ResolveOptions
  ResolveResult
  TrimOptions
  ObjectId / ObjectKey / ObjectRange
  EntryIndexRef
  SchemaRef
  ProjectionRef
  ErrorCode
  NereusException
  keys/
    KeyComponentCodec
    DeterministicIds

io.nereus.core
  DefaultStreamStorage
  append/
    AppendCoordinator
    AppendSessionManager
    WalFlushPlanner
  read/
    ReadResolver
    OffsetIndexCache
  trim/
    TrimCoordinator
  recovery/
    OrphanObjectScanner
  metrics/
    StreamStorageMetrics

io.nereus.metadata.oxia
  OxiaMetadataStore
  OxiaKeyspace
  records/
    StreamMetadataRecord
    StreamNameRecord
    AppendSessionRecord
    CommittedEndOffsetRecord
    OffsetIndexRecord
    EntryIndexReferenceRecord
    ObjectManifestRecord
    StreamSliceManifestRecord
    ObjectReferenceRecord
    CommittedSliceRecord
    TrimRecord

io.nereus.objectstore
  ObjectStore
  PutObjectOptions
  PutObjectResult
  RangeReadOptions
  RangeReadResult
  HeadObjectOptions
  HeadObjectResult
  wal/
    WalObjectWriter
    WalObjectReader
    WalObjectLayout
    StreamSliceDescriptor
    EntryIndex
```

## 6. Design Decisions Captured Here

### D1. Offset index key includes generation from day one

Future 1 文档中出现过 `/offset-index/{offsetEnd}`，commit/object-format 文档中使用
`/offset-index/{offsetEnd}/{generation}`。Phase 1 采用后者，避免 Future 4 引入
generation replacement 时迁移 keyspace。

Phase 1 append 写入 WAL generation：

```text
generation = 0
```

`metadataVersion` 表示 Oxia key version，用于 cache validation；`commitVersion` 表示 Nereus stream
commit 的 durable sequence/version，用于 offset index、committed-slice 和诊断。两者都不是
compaction generation。

### D2. Phase 1 append API is session-aware but can auto-acquire

`append` 的 correctness 必须校验 append session epoch/token。为了让 API 易用：

- `AppendOptions` 可以携带 `AppendSession`；
- 若未携带，`DefaultStreamStorage` 可按配置自动 acquire/renew；
- 内部 commit 始终以 Oxia session record 为准。

### D3. Phase 1 read returns opaque batches plus offset metadata

Phase 1 不解析 Pulsar entry 或 Kafka record 语义。public `append` 只接受
`OPAQUE_RECORD_BATCH`，`read` 返回已经按 offset index 可见的 opaque payload slice、offset
range、payload format、entry index metadata。Future 2/5 再做 protocol projection；在那之前，
非支持 payload format 必须 `UNSUPPORTED_FORMAT`，不能静默当 opaque 处理。

### D4. Object manifest is not visibility truth

Object manifest 只描述 object durable 状态、checksum、slice directory 和 GC 引用。读路径
不能因为 manifest 存在而返回数据，必须先命中 offset index。

### D5. Same-stream visible commits are sequenced per writer

`DefaultStreamStorage` 内部必须为每个 stream 维护 append sequencer。Object upload 可以在后续优化为
并发，但同一个 stream 的 `commitStreamSlice` 必须按 sequencer 顺序进入 Oxia，避免同一个 writer 的
两个 append 读到同一个 `committedEndOffset` 后互相制造 offset conflict。

### D6. Opaque Phase 1 records are one record per entry

Phase 1 不解析协议 payload。为了让 `read(startOffset)` 能精确返回 offset 边界，
`OPAQUE_RECORD_BATCH` 的每个 `AppendEntry` 必须满足 `recordCount=1`。多 record entry 只能用于
未来有协议 decoder 或 entry-level projection 的 payload format。

### D7. Producer ack atomically commits only stream-scoped metadata

`commitStreamSlice` 的线性化点只更新同一个 stream key group 内的 metadata：append session
校验、committed end offset、offset index 和 stream-scoped committed-slice marker。Object
manifest 初始记录需要在 stream commit 前写入并供 commit 校验，但它不是可见性真相。
producer ack 之后的 object manifest state/reference 更新是可修复审计/GC 状态，不能要求跨
stream/object key group 的 Oxia 原子事务。

### D8. Phase 1 stream id is deterministic from stream name

为了让 `createOrGetStream` 不依赖跨 key group 原子事务，Phase 1 从 exact `StreamName.value()`
确定性生成 `streamId`，并用 `PartitionKey(streamId)` 原子创建 by-name record、stream metadata、
committed-end 和 trim 初始记录。后续如果要随机或 sequence stream id，必须先设计单独的分配协议。

## 7. Required Capability Spikes

这些项目不改变 Phase 1 的语义合同，但在实现真实 Oxia adapter 前必须证明或触发
`07-implementation-contract-checklist.md` 的 stop-the-line 改设计流程：

1. Phase 1 的真实 Oxia 集成是否先做 Testcontainers/fake contract test，再接远端 Oxia。
2. M0.5 已增加 `:nereus-metadata-oxia:oxiaCapabilitySpike`，使用真实 Oxia Testcontainers 验证
   partition key、单 key CAS、sequence key 和 fixed-width offset key ordering。
3. M0.5 当前结论是 `NOT_SUPPORTED_BY_PUBLIC_JAVA_API`：选定 public Oxia Java client API 不支持
   原设计需要的单 stream key group 条件批提交。因此 M2/M4 前必须把 `commitStreamSlice` 重设为
   Oxia public API 支持的线性化协议，例如单 authoritative stream-head record CAS。

这个 spike 不是优化项，而是 M2/M4 前置门禁。当前公开 Oxia Java client API 只直接暴露了单 key
conditional put/delete 形态；底层 proto 有 batched write request 并不能自动证明 Java client
提供了 Nereus 需要的 single-key-group conditional multi-write 语义。M0.5 把这个风险确认成当前
设计阻塞：fake metadata 不能模拟比真实 public adapter 更强的原子提交能力。

Already settled for Phase 1:

- entry index writer emits only `OBJECT_FOOTER`；
- `INLINE` and `INDEX_OBJECT` are reserved locations and return `UNSUPPORTED_FORMAT` if encountered；
- entry index stores record/entry boundaries and entry attributes only. Protocol-specific projection hints
  stay in `ProjectionRef` or future higher-layer metadata, not in Phase 1 entry index semantics。
- `StreamName` is an exact opaque logical name. Phase 1 does not parse Pulsar topic syntax or store a
  separate external topic string inside the L0 stream id contract；
- durable `streamId` is `s-` plus full `base32lower_nopad(sha256(UTF-8 exact StreamName.value()))`.
  Human-readable aliases may be attributes, never the durable id；
- `WalObjectWriter` supports multi-slice objects from the first implementation. `DefaultStreamStorage`
  may initially flush one append work item per object; cross-stream batching is planner optimization,
  not a WAL format blocker。

## 8. Latest Implementation Audit Notes

当前细化稿新增以下实现约束，避免后续编码时出现隐性返工：

- metadata record 必须有显式 envelope 和 schema version；`metadataVersion` 只表示 Oxia 版本；
- `AppendResult` 暴露 slice id、generation、payload format、record/entry count、logical bytes 和
  checksums，便于诊断与同 slice 幂等 replay；
- `AppendSessionOptions.allowStealExpiredSession` 必须传入 metadata acquire 逻辑，不允许被 API
  层吞掉；
- Phase 1 不默认把外部 offset conflict 后的 uploaded slice 换到新 offset 再提交，只允许同一物理
  slice 的幂等 replay；
- Phase 1 public append 不携带 producer identity/sequence；producer-level dedup 属于后续协议层，
  丢失 ack 后的 caller-level 重试可能产生重复数据；
- `nereus-object-store` 只返回 neutral WAL descriptors，不依赖 Oxia metadata record；
- offset index 必须携带 read 所需的 slice checksum，并由 resolver 传入
  `ResolvedObjectRange`，读路径不依赖 object manifest 获取校验信息；
- WAL canonical object checksum 和 object-store exact-bytes storage checksum 是两个域，manifest
  必须分别保存，不能把二者当成同一个 checksum；
- WAL object 从第一版开始保留 header length、format major/minor、section envelope 和 writer
  version manifest 字段，避免后续 reader/repair 工具无法按版本解码；
- stream attributes 和 append schema refs 必须闭环存储：attributes 进入 stream metadata，
  schema refs 进入 WAL slice、commit request、offset index、resolve/read result；
- WAL object id/key 必须包含 writer process incarnation hash，避免进程重启后 sequence 重置造成
  object id 碰撞；
- append timeout 必须按最后不可逆边界分类；`commitStreamSlice` RPC 发出后的 timeout 是 unknown
  final state，不得假装未提交；
- Phase 1 WAL object 只支持 `CompressionType.NONE`；`ZSTD` 枚举保留但实现必须拒绝，直到压缩块
  offset/checksum/read-result 语义单独设计；
- Phase 1 public append 只接受 `OPAQUE_RECORD_BATCH`；其他 `PayloadFormat` 是未来标签，遇到
  已提交的非支持格式也应 `UNSUPPORTED_FORMAT`；
- Phase 1 entry index writer 只输出 `OBJECT_FOOTER`；`INLINE`/`INDEX_OBJECT` 是保留枚举，reader
  遇到时返回 `UNSUPPORTED_FORMAT`；
- Phase 1 `resolve(includeEntryIndex=true)` 不读取 object footer 或 index object，只返回 offset
  index 中已提交的 `EntryIndexRef`；
- adapter-private Oxia helpers 必须显式携带 partition key；fake/real contract 测试要覆盖
  get/put/scan/watch/commit 的 partition key 传递；
- `OffsetIndexCache` 只能缓存正向命中的 committed records，不能缓存 EOF/negative lookup；watch
  事件只是 invalidation hint，丢失/乱序/合并都不能影响正确性；
- fake store、real adapter、repair/migration 工具必须共用同一 metadata codec registry，并为每个
  Phase 1 record type 保留 golden-byte 测试；
- `cluster`、`objectId`、`sliceId` 等所有动态路径组件进入 Oxia path 或 object key 前都必须经过
  统一 key component 编码，不能裸拼字符串；
- caller cancellation 和 timeout 一样按最后不可逆边界处理；commit RPC 发出后不能保证撤销；
- Phase 1 object store 必须固定 ByteBuffer ownership、local-file atomic write、path traversal 拒绝和
  object/metadata exception mapping；
- `StreamStorageConfig` 必须有明确正值/大小/lease 关系校验；`maxObjectBytes` 是完整编码 WAL object
  的硬上限，`targetObjectSizeBytes` 只是聚合目标；
- close、timeout、backpressure 和 read-limit-too-small 都是明确 API 错误，不留给实现临时决定；
- offset 和 `cumulativeSize` 推进必须做 overflow check。
- same physical slice replay 在 marker-missing 条件竞争失败后必须 re-read committed-slice marker；
  append sequencer 在 `OFFSET_CONFLICT` 或 unknown final state 后必须 refresh，不得继续使用旧的本地
  expected offset；
- `commitStreamSlice` pre-commit validation 必须校验 manifest writer id、writer run id、writer
  epoch、object key/type/format 和 slice 字段，避免 objectId 异常碰撞或错误 manifest 进入可见路径。
- zero-byte opaque entry 是合法 record：它消耗一个 stream offset，read 必须按 record 推进而不是只按
  payload bytes 推进；metadata/WAL map 编码必须按 UTF-8 key deterministic order，避免 golden bytes
  不稳定。
- `schemaRefs` 是 slice-level set-like metadata，Phase 1 必须按 `(namespace,id,version)` canonical
  order 存储并拒绝重复 tuple，不能把 caller order 当成 durable 语义。
- Phase 1 read 为了校验 slice checksum 会先读取完整 resolved slice 再 clip。实现必须用
  `maxConcurrentObjectReads` 和 `maxReadBufferBytes` 做内存/并发门禁，并暴露 read amplification
  metrics，不能让零散小读无界放大到对象存储带宽和堆内存压力。
- `LocalFileObjectStore` 可以提供 test-only cleanup helper 来清理隔离 root，避免本地和 CI 被
  orphan WAL objects 填满；这个 helper 不能变成 Phase 1 production object delete 语义。
- `forceSingleStreamObject=true` 是 `DefaultStreamStorage` 初期 planner 策略，不能写死到
  `WalObjectWriter` 机制层。Writer 必须继续支持 multi-slice/multi-stream WAL object。

## 9. Phase 1 Exit Criteria

设计和实现进入下一阶段前应满足：

- 能创建 stream，并在 Oxia metadata 中恢复 metadata；
- 能 acquire/renew/fence append session；
- append 完成 object upload、manifest 写入、offset index commit、producer ack result；
- stale session commit 被拒绝；
- broker/process crash 在 upload 前后、commit 前后都有明确恢复行为；
- read resolver 只读 offset index 已发布的 range；
- trim 只推进 low-watermark，不删除 object；
- 单元测试覆盖 offset dense、partial slice visibility、orphan object、cache stale、checksum mismatch；
- 代码不依赖 Pulsar/KoP/ManagedLedger。
