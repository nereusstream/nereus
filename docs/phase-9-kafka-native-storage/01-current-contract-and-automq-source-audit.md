# 01 — Current Contract and AutoMQ Source Audit

> 状态：F9-M1 source lock active；ranged foundation blob set refreshed
> Nereus fork point：`7d89eea`
> AutoMQ checkout：`/Users/liusinan/apps/ideaproject/GITHUB/automq`
> AutoMQ source lock：`main@1c648d84819d5c3fef2af585f02149c397584870`
> AutoMQ version：`3.9.0-SNAPSHOT`
> 审计日期：2026-07-23

本文只记录本地源码可证明的事实，以及 F9 从这些事实推导出的 target gap。它不把 AutoMQ 的实现选择
自动升级成 Nereus requirement，也不声称任一 F9 class 已存在。

## 1. 审计方法与约束

输入只有两个本地 checkout：

- Nereus：当前工作树；
- AutoMQ：用户提供路径下的 Kafka fork。

审计遵循以下规则：

1. 先锁 Git commit、版本与关键 blob hash；
2. 从 public API 向实际 append/fetch/recovery call path 追踪；
3. 区分代码中执行路径、reserved enum 与设计文档；
4. 对 AutoMQ 只提炼 boundary/pattern，不拷贝源码或格式；
5. 任何目标语义都必须说明“复用”“改造”或“明确不采用”；
6. 实现开始前若任一 checkout 变化，F9-M0 必须重跑 source diff 并更新 source lock。

AutoMQ 根 `AGENTS.md` 与 `.agents/coding-conventions.md` 已纳入阅读约束。未来修改 Kafka fork 时，
Nereus-specific change 应保持窄注入区域；本设计采用成对 `// Nereus inject start/end` marker，避免把参考
项目的 `AutoMQ` marker 名称写进我们的 fork。

## 2. AutoMQ source lock

### 2.1 Repository identity

```text
commit  = 1c648d84819d5c3fef2af585f02149c397584870
subject = perf(jvm): enable generational ZGC on JDK 21 (#3472)
version = 3.9.0-SNAPSHOT
branch  = main
```

本地 AutoMQ checkout 有一个无关的 untracked `repomix-output.xml`；审计没有读取、修改或纳入 hash。

### 2.2 Locked files

| AutoMQ file | Git blob | F9 audit purpose |
| --- | --- | --- |
| `gradle.properties` | `88d63be55094287cd8858320f7e00db427e488d5` | version lock |
| `core/src/main/scala/kafka/server/BrokerServer.scala` | `3fb41dee1221c28adadbfa7f6c9549e080ce9cd1` | process bootstrap/shutdown injection |
| `core/src/main/scala/kafka/log/LogManager.scala` | `3514c7f8ffb9ff6c45a4c7b79d0551b3e8fa61e9` | local vs elastic log factory/recovery boundary |
| `core/src/main/scala/kafka/cluster/Partition.scala` | `01a7aa38cdfeb864c09c4ad05f4a297b43fb488e` | HW confirmation and leader behavior |
| `metadata/.../ReplicationControlManager.java` | `d1cdbd50e4c55372ea652bba75f4ad5ef6d605fd` | RF=1/controller enforcement |
| `core/.../ElasticLogManager.scala` | `12e67f979390b8d21eb3fd5e05b308f4e8f0c0f2` | topic-partition log creation/destruction |
| `core/.../ElasticUnifiedLog.scala` | `4c02ed9acc5b0655114135593eb8a64d8ada7dfb` | UnifiedLog integration/recovery |
| `core/.../ElasticLog.scala` | `c506fcc6e7f47e21146e67932f4d8ffbc0ce606c` | LocalLog integration/confirm offset |
| `core/.../ElasticLogSegment.java` | `fc4e8daec6e50548196c4fe0ff0914ac6a9de104` | virtual segment/index boundary |
| `core/.../ElasticLogFileRecords.java` | `8d9d9affaa05469fb0b5c582d25403a213e63426` | `MemoryRecords` to stream batch mapping |
| `core/.../ElasticLogStreamManager.java` | `c44fb8cbce699f02ea77276c95221dddfcc4abdc` | named stream/slice management |
| `core/.../MetaStream.java` | `0818ab04cb3bc9b9f1723a330f11a41a1661dc0a` | log metadata KV stream |
| `core/.../ElasticProducerStateManager.java` | `e37004653af647982025269b20c808c2adf092e0` | producer snapshot persistence |
| `core/.../ElasticTransactionIndex.java` | `67f1a24b8722cd831c0f9a3947072f3c5213c70f` | transaction-index persistence |
| `core/.../ElasticTimeIndex.java` | `10b7aa0a4b2be5955cb164e26484c2e627029c25` | time-index persistence |
| `core/.../ElasticReplicaManager.scala` | `145db8c5446b0753168e8392a08597ed4bb91c7a` | async read/open/close partition boundary |
| `core/.../ElasticKafkaApis.scala` | `5839b6de798371845159dd842f88b779098e22db` | API routing/traffic boundary |
| `s3stream/.../api/Stream.java` | `5e25faeb46e3a4cbb82245516e66b2aba0946b75` | stream offsets/append/fetch/trim API |
| `s3stream/.../api/StreamClient.java` | `8ac8672c83b9de84bbf2cb197488f5b14857135f` | stream create/open contract |
| `s3stream/.../DefaultRecordBatch.java` | `5c540118e39167c5a8fae83e00e0a44fdaefd2dd` | record count + raw payload carrier |

表中的 `...` 只是为版面缩短共同前缀；实现 source-lock 工具必须使用完整 repository-relative path。

## 3. AutoMQ executable architecture

### 3.1 Stream primitive

AutoMQ `Stream` 暴露：

- `streamId`、`streamEpoch`；
- `startOffset`、`confirmOffset`、`nextOffset`；
- append record batch；
- `[startOffset,endOffset)` fetch；
- trim、close、destroy。

`DefaultRecordBatch` 把 `count` 与 raw payload 分开。这证明共享存储 primitive 不必理解 Kafka record；
Kafka layer 可以用 count 把一个 opaque batch 映射为一段 logical offsets。

### 3.2 Exact `MemoryRecords` append

`ElasticLogFileRecords.append` 把一个 Kafka `MemoryRecords` buffer 作为 stream batch raw payload，count 来自
Kafka offsets span。它没有把每条 Kafka record 拆成一个 S3Stream object。这是 F9 ranged-entry mapping 的
直接参考：保持 compressed/transactional/control batch bytes 可避免普通 append 的解压重编码和双 CRC ownership。

F9 采用此边界，但粒度更精确：一个 Nereus `AppendEntry` 对应一个 Kafka `RecordBatch`，一个
`AppendBatch` 对应一次 `MemoryRecords` append。这样 entry index 可表达 request 内多个 batch 的各自 span。

### 3.3 Kafka log subclass seam

AutoMQ 的 log stack 为：

```text
ElasticUnifiedLog extends UnifiedLog
  -> ElasticLog extends LocalLog
      -> ElasticLogSegment extends LogSegment
          -> ElasticLogFileRecords / ElasticTimeIndex / ElasticTransactionIndex
```

`LogManager` 在 elastic mode 创建这套实现，跳过以本地 log directory 为 truth 的 recovery/checkpoint；
`ElasticLogManager` 管理 `TopicPartition` 到 elastic log 的 lifecycle。这证明 native Kafka integration 可以
保留大量 stock `UnifiedLog` validation/state logic，同时替换最底层 durable log 与 segment/index implementation。

F9 采用同类 seam，但 `NereusKafkaRuntime` 与 `KafkaPartitionStorageManager` 是 Nereus adapter owner；不能把
Kafka fork 直接依赖 Oxia/Object provider 的具体实现。

### 3.4 LEO/confirm/HW

AutoMQ `ElasticLog` 在 async append 发出后推进 local LEO，async flush completion 再推进 confirm offset；
`Partition.maybeIncrementLeaderHW` 在 elastic log 下把 HW 对齐 confirm offset。这个设计提供了吞吐 pipeline，
也形成 LEO 领先 durable confirmation 的窗口。

F9 首版明确不采用该窗口。Nereus stable append 已包含 profile-specific acknowledgement boundary；让
`NereusLocalLog.append` 在返回 stock `UnifiedLog` 前等待 stable result，可保证 ProducerStateManager、LEO 与
committed stream 不分叉。pipeline 只能在后续 optimization 中增加，且必须证明 state rollback/replay。

### 3.5 Meta stream and indexes

AutoMQ `MetaStream` 在另一个 stream 中保存 log、producer snapshots、partition 与 leader-epoch checkpoint；
time/transaction index 也用独立 elastic stream/slice。该方案适合 AutoMQ 的 stream manager，但 Nereus 已有：

- 一个 stream head/reachable commit chain 作为 data truth；
- Oxia root/CAS 作为小型 durable binding；
- immutable object + guarded publication；
- higher-generation materialization/GC。

F9 因而不建立“每 append 再写一个 meta stream”的第二 durable log。它把 Kafka recovery state 存成 derived
immutable checkpoint object，并由一个 partition root 引用。checkpoint 失败只增加 replay 工作，不能阻塞
已提交 bytes 的解释。

### 3.6 RF=1 controller projection

AutoMQ controller path 在 elastic mode 强制 replication factor/min ISR 为 1，拒绝长度不为 1 的 ISR 与
replica assignment，并允许可用 broker 成为 single leader。F9 采用 RF=1 metadata envelope：Kafka replica
protocol 不重复 Nereus durability；任何 broker 通过共享存储和 KRaft epoch 接管。

F9 还要求 activation record 与 exact capability digest，避免 controller 把 leader 分配给不认识 Nereus
formats/session protocol 的旧 broker。

### 3.7 Cleaner difference

AutoMQ 能让 stock `LogCleaner` 对 elastic virtual segments 做 replacement。Nereus stream 的 source offsets
与 stable commit chain 不允许原地替换；F4 已定义 higher generation/read view。因此 F9 禁用 stock local
cleaner，使用 Kafka-aware F4 topic compaction。这是有意差异，不是遗漏。

## 4. Locked/current Nereus files

M0 的初始 blobs 保留在 Git 历史。下表在 F9-M1 format/final-gate slice 后刷新；新增
API/core/primary-reader/NCP2/NTC2/materialization rows 是当前已实现事实。

| Nereus file | Git blob | Current fact |
| --- | --- | --- |
| `nereus-api/.../AppendBatch.java` | `c6bac3e4efcb0e597c3441071a638ebf8489934a` | OPAQUE stays one-per-offset；Kafka format accepts checked ranged counts；reserved formats fail closed |
| `nereus-api/.../AppendEntry.java` | `ffa36d92a0e5bbe225358cde8ee9cdd1c829e6ac` | positive `recordCount` plus 64 MiB hard payload cap |
| `nereus-api/.../AppendOptions.java` | `ffecb3eb26aac56c47372a195d3cf1e6a3631808` | no caller expected-start precondition |
| `nereus-api/.../ReadOptions.java` | `40d7bff17bb6490b341d4613c8c8fd9777633f2c` | records/bytes/isolation/timeout；no boundary mode |
| `nereus-api/.../ReadBatch.java` | `1f3ccc4344de26bae9b4dd245cf1dd5cf6db7fed` | result already carries an `OffsetRange` |
| `nereus-api/.../StreamStorage.java` | `6666f23aef7c6b96687ec379d5d6939fe18d6fe5` | binary-safe append/read/authority/session-renew/head-snapshot/commit-reachability overloads；legacy-equivalent delegates，new unsupported semantics fail closed |
| `nereus-api/.../StableStreamHeadSnapshot.java` | `a642b6488cf6f12c488e68949b8ec09a32d28451` | exact commit/trim/session/authority/canonical durable-head digest；genesis commit version 0 is explicit |
| `nereus-api/.../StreamCommitAnchor.java` | `41fc221d99a1ec612a121739a2d01ef59af0758c` | canonical genesis/non-genesis descendant facts for exact append-only ancestor proof |
| `nereus-api/.../AppendPrecondition.java` | `799caf45554ffaab72927bf18e869cce9ba02d2a` | implemented optional non-negative expected start |
| `nereus-api/.../ReadRequest.java` | `e43bd0bb99f1762cc4893e20da18450c9e94b76f` | implemented view/boundary/first-entry/options request |
| `nereus-api/.../SemanticReadResult.java` | `3b2605616150ffb9efada7287df90fa557b92a34` | implemented request/result boundary、dense COMMITTED and sparse TOPIC_COMPACTED coverage validation |
| `nereus-api/.../ErrorCode.java` | `2be9e0ca8ac3638755a294ef4380665ff03a0225` | unsupported append/read semantics codes appended at enum tail |
| `nereus-core/.../DefaultStreamStorage.java` | `14155bedbf37add50388a67a33379a665390a5e6` | production overrides conditional append、semantic read、authority-bound acquire/renew、exact stable-head and commit-reachability overloads |
| `nereus-core/.../AppendCoordinator.java` | `68c0457cddf7db051da12f676c8c0dc599458be7` | conditional calls refresh head in-lane before WAL prepare；legacy calls preserve the old none-precondition path |
| `nereus-core/.../AppendResultValidator.java` | `336d08fa1078248b31925f5a6391932f177999ec` | post-commit/recovery result is checked against exact stream/range/count/bytes/format/schema/expected-start facts |
| `nereus-core/.../AppendSessionManager.java` | `bc4553fa84e8cba2ef0c70b3812aef7876dc014e` | exact writer/token renewal delegates the authority-preserving metadata CAS；invalid writer/TTL fails closed |
| `nereus-core/.../ReadCoordinator.java` | `293b5bacd47c2908e226cadec32a6f67b01bdfb5` | public request owns exact/containing boundaries、global first overflow、source/coverage/result validation |
| `nereus-core/.../ParquetV2CompactedTargetReader.java` | `fddda498fc94e5a02a4bbc029e1403cdd4ee4b8a` | exact NCP2/NTC2 adapter；sparse topic coverage is explicit |
| `nereus-core/.../PhysicalFormatCapabilityRegistry.java` | `e92e604b377ea8a481d5bece22600073c6fc0235` | exact physical/logical/view capability admission；old capability cannot write V2 |
| `nereus-core/.../StreamViewReader.java` | `54aad465e8d2d78e5d7460085223b794394e5ca4` | deprecated compatibility adapter delegates to the public semantic path |
| `nereus-core/.../ViewReadResult.java` | `3b6b03b79c4da37913dacda46f6e0979daba78d7` | deprecated compatibility value；public owner is `SemanticReadResult` |
| `nereus-object-store/.../DefaultWalObjectWriter.java` | `1e698cc90b4442b606aef2dd5f963b6b7562c50f` | entry index already writes relative base + record count |
| `nereus-object-store/.../DefaultWalObjectReader.java` | `6ee7fb5ffcc0b1ff6c9d669ab16b69403b45e9f4` | exact/containing clipping、checked ranged offsets、global first overflow and Kafka format implemented |
| `nereus-object-store/.../CompactedObjectFormatV2.java` | `c9334acbdd8e01c3a4505cf3b12c6c9cdfbe755e` | closed NCP2/NTC2 schemas、metadata registry、paths and limits |
| `nereus-object-store/.../ParquetRangedCompactedObjectWriter.java` | `327e835599992f579373c342487af77c26b1ed1a` | backpressured dense NCP2 writer |
| `nereus-object-store/.../ParquetRangedCompactedObjectReader.java` | `99636d5b397088ed9f59ff359964901664c8f3f7` | strict exact/containing NCP2 reader |
| `nereus-object-store/.../ParquetKafkaTopicCompactedWriter.java` | `187cfd5b7210c106e39601d0636837b191a32014` | backpressured sparse NTC2 writer |
| `nereus-object-store/.../ParquetKafkaTopicCompactedReader.java` | `a6e9a0ffe576c88bebd67765864509df5d6680d5` | strict sparse NTC2 reader with source coverage |
| `nereus-bookkeeper/.../BookKeeperPrimaryWalReader.java` | `92871c04d0e767f5252038b142aeab32f1fac18d` | raw mapping remains one-offset；NBKE1 mapping provides ranged boundary/limit parity |
| `nereus-bookkeeper/.../BookKeeperRangedEntryCodecV1.java` | `dd6233c9a084b4473ba9bc9b1c77fbbf85511c75` | closed magic/count/length/CRC32C/payload framing with strict allocation checks |
| `nereus-api/.../BookKeeperEntryMapping.java` | `5236dde94dddb663c0229a0b2f23045686c386a9` | legacy enum stays first；ranged mapping appended without reinterpreting old targets |
| `nereus-materialization/.../LosslessMaterializationRowPublisher.java` | `ec71fad507a57178ee848426028a3d7c1adf125e` | NCP1 explicitly requires one record per offset |
| `nereus-materialization/.../RangedLosslessMaterializationRowPublisher.java` | `f4e35b83ffeb0fb630a9b45893f2f9630608dc3e` | one exact Kafka source batch maps to one dense NCP2 row |
| `nereus-materialization/.../DefaultMaterializationPlanner.java` | `d1715b79b3080483e6ffe8389922840e89eda42d` | normalizes only the exact Kafka V1 Object mapping into the Kafka-batch compatibility domain, so a higher NCP2 prefix supersedes a retired BookKeeper gen0 prefix while a readable BK tail remains selectable |
| `nereus-bookkeeper/.../BookKeeperPrimaryWalRuntime.java` | `32ef63f4979fb1af4b4e00acd69e56e58a4eddc9` | default client construction delegates to one explicit borrowed operations boundary used consistently by append/read/recovery/activation/retention |
| `nereus-kafka-adapter/.../NereusKafkaBookKeeperWalRuntimeContext.java` | `988fe111e021d7d9d660abe301fe9d8ff54b0872` | three-argument production compatibility constructor supplies the standard adapter；explicit operations decorators remain borrowed |
| `nereus-kafka-adapter/.../NereusKafkaObjectWalRuntimeFactory.java` | `13a766aa9773bcbc4f34b32719c2218bc70bad08` | passes partition/root/object-store/clock repair dependencies into the activated-generation authority while preserving provider ownership |
| `nereus-kafka-adapter/.../DefaultKafkaPartitionStorage.java` | `cd53d97ee4a632a9057f165bcad91db1f53381c7` | a durable authority/head conflict always moves the old leader to `WRITE_FENCED_RECOVERY_REQUIRED`；internal-topic open also exposes the mandatory compacted-read probe |
| `nereus-materialization/.../DefaultTopicCompactionEngine.java` | `c4849680050a2be4b0059161509564b36daa350d` | NTC1 collector assumes one logical row per batch |
| `nereus-metadata-oxia/.../OxiaJavaClientMetadataStore.java` | `3c7d29d7a2b7f4f87e240c65503f46f05c03e464` | live other-writer session rejected until expiry |

完整 path 可从表中 module 与 basename 唯一解析；未来 source-lock task 必须输出完整 path、blob、API
signature digest 和 clean-worktree assertion。

## 5. Current Nereus append facts

### 5.1 Public domain、conditional append and generation-zero readers are range-ready

`AppendEntry`、`AppendResult` 与 `PayloadFormat.KAFKA_RECORD_BATCH` 原有 ranged shape 之上，F9-M1 API slice 已：

- 保留 `OPAQUE_RECORD_BATCH` 每 entry count=1；
- 为 `KAFKA_RECORD_BATCH` 接受 positive ranged counts 并做 int checked sum；
- 对所有 executable formats 要求 projection hints empty；
- 拒绝 reserved formats、超过 65,536 entries 与超过 64 MiB 的单 entry payload；
- 建立 146-row scenario manifest 和 API focused gate。

这仍不等于完整 Kafka-native storage 已支持：production append、Object WAL 与 BookKeeper generation-zero readers
已消费 ranged semantics，NCP2/NTC2 strict physical IO、verification、reader dispatch 与 lossless NCP2 row mapping
也已实现。默认第三方 provider 对 non-empty precondition/non-legacy read fail closed；generation publication、
Kafka survivor production 与 native broker integration 仍未实现。

### 5.2 Stable append already has the needed internal CAS

当前 `DefaultStreamStorage`/append pipeline 已：

- 按 stream 通过 `StreamLane` 串行；
- 读取 committed end；
- 取得 append session；
- 构造含 internal `expectedStartOffset` 的 primary append request；
- 写 primary WAL；
- CAS protected stream head；
- 对 retained attempt 执行 exact recovery。

Public caller 现在可通过 additive `AppendPrecondition` overload 声明 Kafka 已分配的 expected start，且旧
`AppendOptions` record/old method descriptor 不变。Production `DefaultStreamStorage` 已 override 新方法：条件调用
在同一 stream lane 内取得 session、刷新 metadata head，并在 WAL prepare 前比较 expected/actual；不匹配返回
`OFFSET_CONFLICT`/`KNOWN_NOT_COMMITTED` 且消息不带 payload。相等时同一个 actual 值进入既有
`PrimaryAppendRequest`/protection/head-CAS/recovery 流程；commit 与 recovery 返回都经过 exact result validator。
旧三参数 append 仍等价于 `AppendPrecondition.none()`，保留原有 lane offset cache 路径。

### 5.3 Session cannot immediately fence a live Kafka leader

`OxiaJavaClientMetadataStore.acquireAppendSession` 当前行为：

- same writer/live session 可续租相同 epoch/token；
- live other writer 返回 fenced；
- 只有过期后新 writer 才能 steal。

Kafka KRaft leader epoch 变化要求新 leader 立即阻止旧 leader，而不是等待 TTL。target 要在 protocol-neutral
session record 中增加 optional monotonic authority，不以 broker clock 代替 epoch ordering。Pulsar caller 不传
authority，行为不变。

## 6. Current Nereus read facts

### 6.1 Object WAL writer already preserves ranges

`DefaultWalObjectWriter.prepareSlice` 为每个 entry 写：

```text
entryOrdinal
relativeBaseOffset
recordCount
payloadOffset / payloadLength
eventTime / checksum / attributes
```

每写一项按 `recordCount` 增加 relative base；`EntryIndex` 校验 range dense 且总 count 一致。Object WAL v1
的 entry-index bytes 因而已经能描述 ranged entry，不需要为了 Kafka 改写 WAL container version。

### 6.2 Generation-zero reader boundary is range-aware

`DefaultWalObjectReader.clip` 已按 entry start/end 执行 EXACT/CONTAINING，并在 EXACT start 落入 ranged entry
内部时返回 `OFFSET_NOT_AVAILABLE`。`ALLOW_FIRST_ENTRY_OVERFLOW` 只在整个 dispatcher 尚未返回 entry 时有效，
一次越过 records/bytes 后立即停止；旧 STRICT 路径保留原有 byte/record limit 差异。

BookKeeper 旧 mapping 没有 per-entry record count，继续只接受 one-offset entries。Kafka append 使用新增
`RANGED_NEREUS_ENTRY_V1` target mapping 与 `NBKE1` closed frame；reader 先验证 whole physical range SHA-256，再验证
每个 frame 的 magic/count/length/CRC32C，最后返回 exact payload。resource reservation 计入 frame overhead。

### 6.3 Public semantic read owns the production path

F9-M1 API slice 已增加 `ReadRequest`、`SemanticReadResult` 和 binary-safe `StreamStorage.read` overload。旧 provider
只对 legacy-equivalent COMMITTED/EXACT/STRICT request 委托旧方法，其余 fail closed。Production
`DefaultStreamStorage` 已 override；F4 generation selection/fallback 也走 `SemanticReadResult` owner。
`StreamViewReader`/`ViewReadResult` 标记 deprecated-for-removal，仅把旧内部调用适配到同一 public pipeline。

## 7. Current materialization/compaction facts

### 7.1 NCP1 is intentionally single-offset

`LosslessMaterializationRowPublisher.row(ReadBatch)` 明确拒绝 `range.recordCount != 1`；`CompactedObjectRow`
只存单一 `streamOffset`。这使 NCP1 可以保持 exact one-entry-per-offset，但不能 lossless 表示一个 compressed
Kafka batch 覆盖多个 offsets。

结论：新增 `NEREUS_COMPACTED_PARQUET_V2`（NCP2），旧 NCP1 reader/writer 不改变；format dispatch 必须按
exact discriminator 选择，不允许“V1 reader 猜 V2”。

### 7.2 NTC1 compaction is record-per-batch

`TopicCompactionDecoder.decode(long offset, ByteBuffer exactPayload)` 只收到一个 logical offset；
`DefaultTopicCompactionEngine.Collector.accept(ReadBatch)` 使用 batch start 并把 records 增加 1。它无法在
compressed Kafka batch 内按 key 留存/删除 individual records。

结论：新增 Kafka-specific decoder/rewrite strategy 与 NTC2 mapping。decoder 展开 logical records，engine
按 absolute offset 建 survivor set，writer 重写合法 Kafka batch；source payload 仍是审计/恢复 truth。

## 8. F5 current boundary

`nereus-kop-adapter` 当前只有 marker module。F5 design 使用 one Kafka record per L0 offset、KoP facade 和独立
Oxia coordinator projection。F9 不能把“F5 尚未实现”当作合并理由：两者 server ownership、batch format
和 coordinator truth 已经不同。roadmap 需要同时保留 F5 与 F9，依赖共享 primitives 而不是互相依赖。

## 9. Pattern adoption matrix

| AutoMQ pattern | F9 decision | Reason |
| --- | --- | --- |
| raw Kafka batch + count in stream | adopt with one entry per `RecordBatch` | preserves exact compression/CRC and offset span |
| `UnifiedLog`/`LocalLog`/segment subclasses | adopt seam | maximizes stock Kafka validation/state reuse |
| elastic log manager per partition | adapt to adapter-owned storage manager | keeps provider/Oxia details outside fork |
| RF=1 controller mode | adopt + activation proof | no duplicate Kafka replica durability |
| any broker can lead shared log | adopt with authority epoch | shared bytes plus immediate stale-leader fencing |
| LEO advances before confirm | reject for initial release | avoids producer state ahead of stable Nereus head |
| per-partition metadata stream | replace | Oxia root + immutable checkpoint; no second append truth |
| separate time/txn streams | replace | checkpoint/derived indexes; one partition data stream |
| stock cleaner segment replacement | replace | Nereus higher generation is the only replacement model |
| elastic API subclass for async optimizations | defer/minimize | first release targets correctness with narrow fork |
| local log directory recovery skipped | adopt | local disk is cache only in Nereus mode |

## 10. Gap inventory and code owner

| ID | Gap | Required target owner | Design document |
| --- | --- | --- | --- |
| GAP-01 | `AppendBatch` rejects ranged/Kafka | `nereus-api` validation | 02 |
| GAP-02 | no public expected-start | `nereus-api` overload + core coordinator | 02 |
| GAP-03 | no containing-entry read | API/core/object reader | 02 |
| GAP-04 | no first-entry overflow | API/core/readers | 02 |
| GAP-05 | view reader internal | public semantic view API | 02 |
| GAP-06 | NCP1 cannot store range | object store/materialization NCP2 | 02 |
| GAP-07 | NTC1 cannot explode Kafka batch | Kafka codec + materialization NTC2 | 02, 05 |
| GAP-08 | no native Kafka module | new `nereus-kafka-adapter` | 03, 07 |
| GAP-09 | no Kafka log subclasses | Kafka fork | 03 |
| GAP-10 | no leader-authority preemption | API/metadata/core session | 04 |
| GAP-11 | no topicId partition binding | Kafka metadata store | 04 |
| GAP-12 | no shared recovery checkpoint | checkpoint object/store | 04, 05 |
| GAP-13 | local producer/index state not failover-safe | replay + derived checkpoint | 05 |
| GAP-14 | stock cleaner incompatible | disable + F4 Kafka strategy | 05 |
| GAP-15 | Kafka retention mapping absent | Kafka retention coordinator | 05 |
| GAP-16 | no cluster activation/capability | runtime/controller/metadata | 06 |
| GAP-17 | no Kafka-specific config/metrics/runbook | adapter/fork operations | 06 |
| GAP-18 | no executable gates | Gradle/Kafka test suites | 07, 08 |

## 11. Design consequences

1. F9-M1 是强前置；不能先写 Kafka adapter 再“以后补 ranged read”。
2. stable append 与 producer state 顺序是首版吞吐取舍，必须在 benchmark 前先固定 correctness。
3. append authority 是 Nereus 通用能力，但只在 optional field 存在时改变 preemption。
4. checkpoint schema 必须能独立演进，不能序列化 Kafka implementation object graph。
5. internal topics 是 F9 transaction/group compatibility 的关键验收对象，不是普通 topic 之后的附加项。
6. F4 compaction 需要第二代 physical schema；只接 Produce/Fetch 不能宣称完整 Kafka integration。
7. 所有 fork changes 必须有 stock-disabled fallback test：`nereus.kafka.storage.enabled=false` 时行为与锁定 Kafka
   baseline 一致。

## 12. Source-lock regeneration target

F9-M0 实现前新增只读 Gradle task `phase9SourceLockCheck`，预期执行：

```text
assert Nereus tracked worktree input matches recorded blobs
assert AutoMQ checkout exists and HEAD/version match
hash every locked full path
extract required class/method signatures
assert Kafka/Nereus inject markers are balanced
emit deterministic JSON report under build/reports (not committed)
```

这个 task 不是代码兼容测试；它只防止后续实现建立在漂移的参考源码上。升级 Kafka baseline 必须提交新的
source-lock diff、method map、format/API compatibility review 和 scenario impact。

### 12.1 Current Apache Kafka baseline probe（2026-07-23）

`phase9KafkaBaselineSourceLockCheck` 当前只读锁定本地 clean Apache Kafka
`trunk@427b409cf440f745ad6195673d3342f6bd3974d4`（`4.3.0-SNAPSHOT`）和 10 个 M3/M6 seam source blobs：
`DefaultRecordBatch`、`MemoryRecords`、`UnifiedLog`、`LocalLog`、`Partition`、`ReplicaManager`、`BrokerServer`、
`LogManager`、`BrokerMetadataPublisher`、`ReplicationControlManager`。它还要求 `origin` 精确指向
`apache/kafka` 且 worktree clean。

这是 fork API/format probe，不是 `nereusstream/kafka` production fork lock。任务名、输出和 M3 partial gate 均不得
把它解释为完成 KF-SRC-004；组织 fork 建立后必须新增 fork commit/remote/marker/signature lock，并替换这里的
baseline-only completion evidence。

### 12.2 Current organization-fork development lock（2026-07-28）

`github.com/nereusstream/kafka` 已存在且是 `apache/kafka` fork；审计时 remote `trunk` 为
`c300006a7705c240642db6950b5a95fec982bfc5`，并包含锁定 Apache base
`427b409cf440f745ad6195673d3342f6bd3974d4`。当前 working clone
`/Users/liusinan/apps/ideaproject/nereusstream/kafka` 的 branch
`nereus/future9-native-kafka-storage` 从该 base 创建；首个 exact-record bridge commit 是
`2379c63933dd0a155d5a5bf90fca85c7b24db58b`，第二个 async ListOffsets/error-mapping commit 是
`c2b1b4b3a00fb7cfa222a3e6df659011795f3b3e`，第三个 stock request-path seam commit 是
`f36b9123a6322c41ea25ee4544196f7e689ed625`，第四个 recovered-storage lookup lifecycle commit 是
`16377ac44b20b7c010e697b22fce5a2e55cb02ac`，第五个 async partition metadata reconciliation commit 是
`c3af5f30facc27dcaf26e2de6e566fc9dd062d0c`，第六个 inert config surface/typed snapshot/pure validation commit 是
`d312e8e58d64f326261dd36592a1b5e6398fa5a3`，第七个 explicit BrokerServer runtime lifecycle seam commit 是
`46e67037615a60a39320836cc5f34ddaf4a9b347`，第八个 adapter-backed typed runtime bridge commit 是
`617451957c886d4247f6d2f1a88e44a35edfbba7`，第九个 side-effect-free product runtime configuration mapper commit 是
`94ecf8c105ad2d765aa9fd4a4929ff86c20882a1`，第十个 deferred activation-backed product runtime composition commit 是
`c27305a7ad955ebc876de20da0fd045e97beba55`，第十一个 Spotless import-order alignment commit 是
`752953d0ef9c38ae50340147bb4f3172ee9ddc5a`，第十二个 concrete M3 recovery-state rebuild/publication commit 是
`672429d94fd82679d7091585ef27c38a7a39f743`，第十三个 stock-without-artifacts recovery-state boundary fix 是
`9a6ebed6d94a2d91fcf581b70c72ac71201c305b`，第十四个 authoritative log-shell/factory selection commit 是
`cfcdd55fbc571bc7187379d65504caa4fe23586e`，第十五个 stock-file inject-marker alignment commit 是
`7739351b7cccd1fa388ad151bfb3e2cc78d7a8a4`，第十六个 stable UnifiedLog append/read correctness bridge commit 是
`dc8c66388a8b093f219d314d97188feb8fd93f92`，第十七个 bounded Produce request-path handoff commit 是
`ee608625e4dde95089b25765e874d50edec044d4`，第十八个 whole-request async Fetch handoff commit 是
`bba3ef01217a9f6728104e45f1b7d0e867e02459`，第十九个 admitted-Fetch wakeup capacity fix 是
`47d36a1d9fd3ae670e6b799b90df42fb86502e41`，第二十个 producer/transaction state restore 是
`ec7f0db991`，第二十一个 transaction handoff/coordinator ordering test 是 `032974067c`，第二十二个
checkpoint-before-DeleteRecords trim 是 `4c060aec89`，第二十三个 partition retention scheduling 是
`feabf6c686`，第二十四个 virtual log segment state 是 `378e9f8967`，第二十五个 compaction partition
authority capture 是 `58342d9dca31009bb63b2c6a3be8a7c7ff68f9ec`，第二十六个 stock maintenance
source-isolation fix 是 `faaffc8a75`，第二十七个 native-storage launcher 是
`3bd92c7244d84870f48e4819bf2930ca9248c278`，第二十八个 controller storage activation scheduling 是
`9773c8f817a8f9f81d8beadf072d8d0e1345be99`，第二十九个 durable storage feature/control commit 是
`d23dc5c7877eac89366e3552b56cae4a87bdad4a`，第三十个 aggregate Spotless import alignment 是
`5ebf31cde8a5ef58597f9a2e0f05d9c37e5ceb41`，第三十一个 cache-directory KRaft identity commit 是
`ecde6964c5b3a52163ef2776e63d3b0b7093c358`，第三十二个 typed BookKeeper runtime/client-ownership commit 是
`50b46aab2dad56850fc5d76ec11cf929b14c1bd9`，第三十三个 async Object-WAL profile mapping commit 是
`80445853a3ee718089933615776d910f0414470f`，第三十四个 BookKeeper Object-profile/cache-root NCP2 mapping
commit 是 `116052aa53867e4f41bdb4b61f4dc171923a0659`，第三十五个 BookKeeper ledger-GC typed
configuration/digest mapping commit 是 `b443750be4ea34eb7dd5b827104b7eec7db65292`，第三十六个 materialization
retirement policy/configuration-digest mapping commit 是
`5169b57986f9b940d6f2c96ab3e1f777d4aa9cfa`，第三十七个 stock config-fixture alignment commit 是
`33f988a83ef026773772df0e6ad45160520f3646`，第三十八个 logging-runtime isolation commit 是
`a1b1e3482de875b15dc42ecda0ce500a65eb37b5`，第三十九个 enabled-format default-BookKeeper-profile
fixture completion commit 是 `ebf1d7616309a26ca95cffa3a2434bf9d5a20868`。该 head 已通过 SSH 发布到
`origin/nereus/future9-native-kafka-storage`。第四十个 controller singleton shared-storage reassignment commit
是 `fe308359b6edaec26819a3c207f7308f1cc15918`；第四十一个 local-replica-removal lifecycle fix 是
`bb7e8937c5ec361b7e8bb6b79ea3833fe4e4a20e`；第四十二个 controller-epoch activation reconciliation
observability commit 是 `df238bb387706f60bc020e43c8dc6878fbf41051`；第四十三个 durable log-start
publication commit 是 `1897f07fcd`；第四十四个 broker-registration-epoch-ready recovery commit 是
`b300a169ee`；第四十五个 pre-trim checkpoint hydration/current-trim pruning commit 是
`1cbe8b65a8a802e8fd06503af3d5449ea79353e0`；第四十六个 mandatory internal-topic compacted-read
coordinator gate commit 是 `89b66ab03b724ba6835faa810cdd7da1b678fe14`；第四十七个 full-bridge
Spotless import-grouping correction 是 `712bbf414dae88ef08f9a99e07bcdb0d6f3b85e0`；第四十八个 maintenance
capture-drift diagnostic commit 是 `768924da60f10b2b9611d19c0c4cb7df2a10947f`，并且是当前已发布 head。

`phase9KafkaForkDevelopmentSourceLockCheck` 锁定 branch/local+published head/base ancestry/forty-eight-commit
count/version、组织 fork fetch/push identity、cached organization trunk ancestry、一百二十一文件 exact change set/blob、
成对 inject marker、adapter/async bridge/
exception-mapper/ListOffsets lifecycle/topic-delta lifecycle/metadata-publisher/config snapshot/validator method signature 和
BrokerServer runtime create/ready/drain/close signature、controller activation success log、typed adapter factory/ReplicaManager binding、stock-only
maintenance interfaces、共享 `Kafka.run` dual-factory 传递、artifact-only launcher 与 executable script，以及 package-wide
no-reflection/no-service-loader/no-stock-product-import 规则；新增 runtime composition 还锁定 executable-profile、explicit-provider、
broker-capability、activation-backed Object/BookKeeper creator、typed BookKeeper binding/client ownership、borrowed scheduler、single-image KRaft snapshot、
post-registration broker epoch、per-operation admission recheck、one-time recovery-state-factory binding、stock
`RecordBatch` CRC/fresh-state/frozen-source validation、exact `Partition` publication，以及 stock-local
logging-runtime marker：启用 development artifacts 时只允许 SLF4J 2.x provider，排除 1.x binding、Logback
provider 和 BookKeeper 传递 provider，并从 release tar 过滤 Kafka 原 1.7 API；
`UnifiedLogFactory.Local` fallback、Nereus no-local-scan/no-local-maintenance factory、recovered-state/storage publication
和 `RequiredAcksAwareAppend` exact routing、stable append-before-LEO、post-stable fencing、bounded adapter read 与
`MemoryRecords` Fetch assembly signatures，以及 request-wide Produce validation、stock optional append executor、
owned buffer capture、`RequestLocal.noCaching` worker、per-partition FIFO、公平 reschedule、post-worker action-queue
completion，以及 stock optional fetch executor、whole-request `readFromLog` routing、initial/event
`readFromPurgatory` mode、response completion、bounded worker/callback pools、logical operation admission、
exact partition subscription 和 combined runtime drain signatures，以及 stock-owned
`ControllerStorageRuntime`/context/factory、`ControllerServer` create/start/publisher-install/close 顺序、artifact-only
controller mapper/factory/creator/runtime、current-controller-only attempt、single in-flight/coalesced metadata callback、
retriable-only retry、leadership-loss retry cancellation、per-controller-epoch durable fault suppression、minimal
shared-Oxia resource graph 和 production launcher 的 fresh broker/controller factories。
M4/M5/F9 compaction 还锁定 canonical producer/transaction replay/freeze、virtual log/config state、DeleteRecords
authority、typed compaction config/two-pass limits、one-time product composition、leader-only owned-partition registration、
internal/user work class、partition-read-lock capture 和 stock `CleanedTransactionMetadata` marker oracle。
`publishPhase9DevelopmentArtifacts` 只把 `0.1.0-f9-dev` 发布到 Nereus build 目录的隔离 Maven repository；
fork build 必须显式同时传入 repository 与 version，缺任一参数即 configuration failure，不读取 Maven local。
`phase9M3KafkaForkStockCheck` 不传参数从头验证 stock server/core compile/static analysis、完整 `KafkaConfigTest`、
6 个 typed-config tests、4 个 enabled-only validator tests、3 个 runtime-factory tests、stock single-node KRaft
start/shutdown/restart、五个 `Partition` seam tests、一个 `ReplicaManager` leader-publication test 和七个完整
`BrokerMetadataPublisherTest`；
`phase9M3KafkaForkBridgeCheck` 传 exact 参数运行 record inspector、async ListOffsets、error mapping 的 12 个 tests、
manager-to-Partition lookup lifecycle 的 7 个 tests、topic-delta lifecycle 的 7 个 tests、五个 stock `Partition`
seam tests、一个 `ReplicaManager` test、七个完整 `BrokerMetadataPublisherTest`、完整 `KafkaConfigTest`、上述
13 个 config/runtime-specific tests、4 个 product-mapper tests、9 个 context/deferred/recovery tests、
5 个 adapter-backed runtime tests、authoritative log-shell/factory tests、borrowed scheduler test 和 stock single-node KRaft restart，以及
server/core/storage checkstyle、SpotBugs
与 Spotless。lifecycle 只消费 manager 返回的已恢复 writable storage，不重建 storage/recovery；安装失败会按
旧 epoch 调用 manager resign，stale resign 和 late old open 都不能移除新 lookup。metadata seam 还验证 stock state
先发布、同 epoch recovery-pending fail closed、coordinator election 等待 exact recovered-storage installation、
delete→同名 recreation 串行以及 `firstPublishFuture` 不承担 partition readiness。
At local head `617451957c`，`phase9M3KafkaForkCheck --rerun-tasks` passed 78/78 outer tasks；its nested stock and
artifact-enabled Kafka invocations completed 92/92 and 95/95 actionable tasks respectively，including both stock KRaft
restart and all four `NereusBrokerStorageRuntimeTest` methods。
At local head `752953d0ef`，the four mapper、three KRaft context adapter、four deferred runtime、two recovery bridge、
five adapter-backed runtime and borrowed-scheduler focused methods passed with core/server checkstyle and core/server SpotBugs；
after the formatter/source-lock update，`phase9M3KafkaForkCheck --rerun-tasks` passed 71/71 outer tasks；its nested stock and
artifact-enabled Kafka invocations passed 92/92 and 95/95 actionable tasks respectively，including Spotless and the new runtime
composition tests。The inherited manifest-only timeout test was made deterministic by reserving two seconds for its local
object/manifest writes before the intentionally non-completing head-commit future。At `672429d94f`，focused
fork tests additionally pass three stock-RecordBatch recovery codec cases、two exact Partition recovery-state factory cases、
two one-time state-factory bridge cases and seven topic lifecycle cases，with core Checkstyle、SpotBugs and Spotless。The
aggregate rerun result for this exact head is recorded only after the updated source lock and product recovery changes are
committed；the earlier 71/92/95 figures must not be attributed to `672429d94f` before that rerun。
That rerun exposed a stock-without-artifacts compile dependency from `Partition` to an excluded Nereus class；current head
`9a6ebed6d9` fixes it with stock `LeaderEpochAwareRecoveryState` in `storage`，retains exact topicId/topic-partition/leader-epoch/
frozen validation and passes both stock and artifact-enabled focused compilation/tests。The exact current-head aggregate is
recorded against Nereus `main@6f2ab6f`：`phase9M3KafkaForkCheck --rerun-tasks` passed 80/80 outer tasks，its nested
stock-without-artifacts and artifact-enabled Kafka builds passed 92/92 and 95/95 actionable tasks respectively。The run
includes exact source locks、146/146 scenario-manifest synchronization、real provider gate、Checkstyle、SpotBugs、Spotless and
stock single-node KRaft restart。

At local head `7739351b7c`，the stock-without-artifacts compile plus stock `KafkaConfigTest`/`LogManagerTest`/
`BrokerStorageRuntimeFactoryTest` pass；artifact-enabled compile and focused `NereusUnifiedLogFactoryTest`、
`NereusListOffsetsLifecycleTest`、`NereusKafkaRecoveryStateFactoryTest`、`NereusBrokerStorageRuntimeTest` and
`BrokerStorageRuntimeFactoryTest` pass with core Checkstyle、SpotBugs and Spotless。The factory selects only
`${cacheDir}/{brokerId}/partition-logs`，does not scan stale local logs or run stock local maintenance，and requires a non-zero
topic ID。The shell becomes writable only after exact frozen recovery state and manager-returned storage are both published；
Produce/Fetch then still fail with a storage error until the next adapter-I/O slice，so this evidence is not an M3 completion
claim。Against this exact head，`phase9M3KafkaForkCheck --rerun-tasks` passed 80/80 outer tasks；its nested
stock-without-artifacts and artifact-enabled Kafka builds passed 92/92 and 95/95 actionable tasks respectively，including
146/146 scenario-manifest synchronization、real provider recovery、stock KRaft restart、Checkstyle、SpotBugs and Spotless。

At local head `dc8c66388a`，stock `Partition` uses the optional stock-package `RequiredAcksAwareAppend` only when the selected
log implements it；ordinary `UnifiedLog` retains the exact original call。The Nereus implementation runs stock validation、
offset/leader-epoch assignment and producer-state pre-validation before its `NereusLocalLog` callback delegates exact validated
bytes to `KafkaPartitionStorage.append`；only an exact stable result returns to advance shell LEO and later stock derived state。
`requiredAcks=-1/0/1` is preserved unchanged，other values fail before adapter I/O。Fetch maps LOG_END/HW/LSO to one frozen
stable upper bound、per-partition/hard byte limits and timeout，then converts exact adapter assembly back to
`MemoryRecords` without writing the synthetic segment。Timeout/interrupt、invalid stable result and any post-stable stock
failure resign the storage；M3 producer/transaction/control batches map to `UNSUPPORTED_FOR_MESSAGE_FORMAT`。Focused
artifact-enabled append/fetch/fencing tests、the required-acks `Partition` test、static checks and stock-without-artifacts
`Partition`/`LogManager` regressions pass。Because this bridge still blocks its `UnifiedLog` caller and does not yet use the
bounded ReplicaManager handoff or multi-partition Fetch operation，it is correctness evidence，not an M3 or production claim。
Against exact product source and this fork head，`phase9M3KafkaForkCheck --rerun-tasks` passes 80/80 outer tasks；its nested
stock-without-artifacts and artifact-enabled Kafka builds pass 92/92 and 95/95 actionable tasks respectively，including
146/146 scenario synchronization、real provider recovery、stock KRaft restart、the new required-acks/fencing cases、
Checkstyle、SpotBugs and Spotless。

At local head `ee608625e4`，`BrokerStorageRuntime.appendExecutor` is an optional stock-owned seam：disabled runtime returns
`None` and stock `ReplicaManager.appendRecords` retains its synchronous branch；enabled runtime supplies
`NereusBrokerStorageAppendExecutor`。Before any partition submit，ReplicaManager validates the checked request-byte sum。
Each submit copies the exact remaining `MemoryRecords` bytes and acquires the product byte lease before returning；the bounded
executor admits at most `threads + queueCapacity` logical tasks，serializes equal `TopicIdPartition` keys，yields between
same-key tasks so another partition cannot starve on a one-thread pool，and permits different keys to run concurrently。Workers
rebuild an owned `MemoryRecords` view and invoke singleton stock `appendRecordsToLeader` with
`RequestLocal.noCaching`，then drain `defaultActionQueue` because the original KafkaApis epilogue has already returned。All
partition terminals are normalized before the original produce-status/validation-stats/delayed-acks/response completion runs
exactly once。Queue/byte/request rejection maps before append I/O；canceling the response handle cannot cancel admitted work。
`beginDrain` closes Produce admission first，and `awaitDrained` combines executor termination with product runtime drain under
the caller timeout。Focused product/stock/artifact-enabled executor and runtime tests plus core
Spotless/Checkstyle/SpotBugs pass。Against exact product source and this fork head，fresh
`phase9M3KafkaForkCheck --rerun-tasks` passes 80/80 outer tasks；nested stock-without-artifacts and artifact-enabled Kafka
builds pass 92/92 and 95/95 actionable tasks，including 146/146 scenario synchronization、real provider recovery、stock
KRaft restart and all format/static gates。Fetch still lacks the ReplicaManager-level async handoff，so this is a bounded
Produce slice rather than M3 completion。

At local head `bba3ef0121`，`BrokerStorageRuntime.fetchExecutor` is the second optional stock-owned seam。Disabled mode
returns `None` and executes the original `DelayedFetch` purgatory branch unchanged；enabled mode submits the complete ordered
request to `NereusBrokerStorageFetchExecutor`，whose source closure invokes stock `ReplicaManager.readFromLog` for every wave。
The initial wave uses normal read side effects；event/deadline waves use `readFromPurgatory=true`，so partition order、
request-wide byte budget、first-entry overflow、leader-epoch divergence、preferred-replica、remote-fetch and per-partition
error semantics remain stock-owned。The product `KafkaFetchWaveOperation` subscribes before initial read，coalesces events
behind one in-flight wave，applies `minBytes` to actual returned record bytes，limits event rereads without suppressing one
deadline-final read，and cleans listeners/timer/read before completing on a separate bounded callback executor。Logical
admission is capped at `threads + queueCapacity` for the whole wait lifetime；missing current storage is not converted into
request-wide failure，so the stock wave still returns the exact partition error。Every worker wave drains
`defaultActionQueue` because the KafkaApis epilogue has returned；final completion validates order/cardinality，marks fetch
metrics once and invokes the original response callback once。Runtime start/drain/close now owns both append and fetch
executors。Focused product state-machine、stock two-partition ReplicaManager、artifact executor saturation/event/deadline and
runtime drain tests pass with core Spotless/Checkstyle/SpotBugs。
The first fresh aggregate exposed a race in the capacity test and a corresponding executor defect：a burst of wakeups for
already-admitted operations could momentarily fill the configured excess queue before an idle worker dequeued the first
control task。`47d36a1d9f` retains the same `threads + queueCapacity` logical admission cap but provisions that many internal
control queue slots，so at most one runner for every admitted operation can always be retained without caller-thread fallback
or unbounded growth。The deterministic test now holds two accepted requests below a long deadline，proves a third submit is
rejected before read，then wakes both listeners and requires four exact waves plus drain。The focused executor rerun passes。
Against exact product source and local head `47d36a1d9f`，the fresh
`phase9M3KafkaForkCheck --rerun-tasks` passes 80/80 outer tasks；nested stock-without-artifacts and artifact-enabled Kafka
builds pass 92/92 and 95/95 actionable tasks，including 146/146 scenario synchronization、real provider recovery、stock
KRaft restart、the simultaneous-wakeup regression and all Checkstyle/SpotBugs/Spotless gates。

At the published fork head，the product-side process gate now builds the real release distribution and launches the explicit
native-storage entrypoint twice over the same formatted KRaft directories、four-shard Oxia and pinned LocalStack S3。The first
process creates one topic、appends/fetches offset 0、commits transactional data at offset 1 plus marker at offset 2、joins a
real consumer group and commits group offset 2，then verifies earliest=0/latest=3 and exits through SIGTERM。The fresh second
JVM registers a higher broker epoch；the ACTIVE coordinator reloads that exact capability and CAS-refreshes the readiness broker
set/epoch without reapplying the first-activation empty-cluster rule，after which the broker concurrently recovers the user
partition、`__consumer_offsets` and `__transaction_state` from remote state。It reloads group offset 2、reinitializes the same
transactional ID、commits data offset 3 plus marker offset 4、resumes the group at visible offset 3 and commits offset 4，then
verifies earliest=0/latest=5 before another normal shutdown。The process gate first exposed concurrent recovery read-budget
backpressure as a terminal metadata publication fault；the product recovery coordinator now retries only retriable page-read
failures with 10–250 ms exponential backoff under the original frozen-head deadline and publishes no partial state。The same
gate deliberately uses four Fetch workers：cold restart can concurrently load the user log、`__consumer_offsets` and
`__transaction_state`，and the fourth slot keeps the client probe from competing with those three mandatory recovery reads；
this remains below the production default of at least 16 and makes the gate's minimum-resource contract deterministic。The same
gate then starts a third JVM，stably appends open-transaction data at offset 5 and forcibly kills the process。A fourth JVM
recovers that coordinator state；reinitializing the same transactional ID writes ABORT marker 6 before committed data/marker
7/8，`read_committed` skips offset 5 and the group advances to offset 8 with latest=9。The same work corrected the root Gradle task
selection so M6 feature/process gates publish current `0.1.0-f9-dev` bytes；the regression was initially exposed because the
release tarball had consumed an older same-coordinate adapter artifact。This is same-node graceful cold-restart evidence，not
multi-broker live preemption、checkpoint/virtual-segment transaction cuts、multi-controller failover or provider-profile
matrix evidence。

The same published fork head now also has an independent `BOOKKEEPER_WAL_ONLY` release-distribution gate。Product-side
provisioning writes the F1-BK ledger-ID namespace、ACTIVE protocol publication and exact broker readiness to the same
four-shard Oxia authority，while a two-bookie fixture uses BookKeeper's stock ZooKeeper
`LongHierarchicalLedgerManagerFactory` for BookKeeper metadata。The first real combined-node Kafka JVM Admin-creates a
topic、produces/fetches offset 0、verifies earliest=0/latest=1 and shuts down normally；a fresh JVM over the same formatted
KRaft directories and Nereus state recovers offset 0、produces/fetches offset 1、verifies earliest=0/latest=2 and shuts down
normally。The same release fixture now runs the two BookKeeper Object profiles：async appends four batches、waits for a real
NCP2 S3 object and then performs fresh-JVM recovery/continued append；sync appends one batch only after its required NCP2
generation is COMMITTED/readable，then performs the same cold recovery。This proves the typed mapping/client ownership and
all three BookKeeper profiles reach real processes without introducing Pulsar's Oxia BookKeeper metadata driver into the Kafka
distribution；it is still single-node evidence rather than the complete multi-broker/live-takeover matrix。

The same published head now has an independent `OBJECT_WAL_ASYNC_OBJECT` release-distribution gate。The Nereus factory
registers one real Object provider resolver under both Object sync and async profiles；the Kafka mapper freezes the selected
async profile as default without constructing another provider graph。The first real JVM writes/fetches offset 0 and verifies
earliest=0/latest=1；after normal shutdown a fresh JVM recovers offset 0、writes/fetches offset 1 and verifies
earliest=0/latest=2 over the same formatted KRaft directories、Oxia and LocalStack S3。

The same product head now has a real-Oxia live authority gate independent of the release cold-restart gate。
`f9MultiBrokerTakeoverProviderIntegrationTest` seeds two compatible broker capabilities/readiness identities and starts two
separately owned production Object-WAL runtimes over one durable authority/root。Broker A commits offset 0 under leader epoch
7；broker B acquires leader epoch 8 before A's 30-second session TTL、replays the exact committed batch and installs stable end
1。A's next old-token append returns durable `FENCED_APPEND` and the local storage enters
`WRITE_FENCED_RECOVERY_REQUIRED` even though the rejected append is `KNOWN_NOT_COMMITTED`；B commits offset 1 and Fetches
both RecordBatches byte-exactly。The companion deterministic regression fixes the same classification seam。This is an
R-tier two-runtime provider proof；the later release gates now add two Kafka-process/KRaft reassignment、a three-process
already-in-flight Object-WAL append cut and BookKeeper three-profile post-handoff P evidence，while the BookKeeper
already-in-flight cut remains outside this evidence。
Fresh `phase9M3ProviderCheck --rerun-tasks` passes 64/64 actionable tasks and composes this gate with the existing Object
and two-bookie BookKeeper provider gates、M1/M2/M3 deterministic predecessors、146/146 scenario synchronization and the
updated 29-source Nereus lock。

The published fork head `1cbe8b65a8` and product process harness now close the narrower two-release-process/KRaft
reassignment boundary。The controller must not reuse stock reassignment's temporary `[old,new]` RF2 state under Nereus
feature level 1，because the same feature contract rejects follower ISR and non-singleton assignments。
`ReplicationControlManager.changeNereusPartitionReassignment` therefore accepts only a stable RF1 current partition and one
registered/active target broker，then emits one `PartitionChangeRecord` whose replicas、ISR and preferred leader are the
target singleton and whose adding/removing replicas are empty。Level 0/absent remains on the stock reassignment path。

The first real process attempt reached this atomic KRaft handoff but failed new-leader recovery with
`Kafka partition binding is deleted or deleting`。The source trace showed that
`TopicsDelta.localChanges(brokerId).deletes()` means both local assignment removal and durable topic/partition deletion。
`NereusTopicDeltaLifecycle` now reads the new metadata image before choosing the operation：if the same topic name、topic ID
and partition still exist，the old broker calls `resign(previousIdentity, newLeaderEpoch, ...)` and preserves the shared
binding；if the identity is absent or the same name has a new topic ID，it calls durable
`delete(previousIdentity, metadataOffset, ...)`。This retains delete-before-open ordering for same-name recreation while
preventing a departing replica from destroying cluster-wide data authority。

`f9MultiBrokerTakeoverProcessIntegrationTest` formats two independent release nodes with one Kafka cluster ID；node 1 is
combined controller/broker and node 2 is broker-only，with one shared controller quorum、Nereus cluster、four-shard Oxia
authority and LocalStack Object root but isolated metadata/log/cache directories。It creates a partition assigned to `[1]`，
commits/reads offset 0，starts node 2，Admin-reassigns to `[2]`，requires exact
`leader=2, replicas=[2], ISR=[2]` and no ongoing reassignment while node 1 remains alive，then commits offset 1 and reads
both batches from the cluster。Fresh execution passed 73/73 actionable tasks in 1m04s and the task is part of
`phase9M6KafkaProcessCheck`。This first task is P-tier Object-WAL post-handoff evidence。

Product `7c25d2e` adds `f9CoordinatorMigrationProcessIntegrationTest` without changing the published fork
`1cbe8b65a8`。The same two-release-process topology first writes user offsets 0/1/2、group committed offset 2 and one
completed transactional ID on node 1，then atomically reassigns the user partition、`__consumer_offsets-0` and
`__transaction_state-0` together from `[1]` to `[2]` while node 1 remains alive。All three must converge to exact
`leader/replicas/ISR=[2]` with no ongoing reassignment before any client assertion。Broker 2 then reloads group offset 2，
reinitializes the same transactional ID and commits data/marker offsets 3/4；READ_COMMITTED sees both committed values and
the same group resumes at visible offset 3 and commits offset 4，with final earliest/latest `0/5`。Fresh execution passes
73/73 actionable tasks in 1m07s；the related baseline/takeover rerun passes 74/74 in 1m50s。This is completed-state
internal-topic P/K takeover evidence；the following gate covers the live OPEN-state path separately。

Product `efe782d` adds `f9OngoingTransactionMigrationProcessIntegrationTest` on the same published fork and Object-WAL
topology。It opens a transaction on broker 1 at data offset 0，holds LSO/read-committed end at 0，then moves the user
partition and `__transaction_state-0` together to broker 2 while both processes stay alive。The original producer commits
through the migrated coordinator，the COMMIT marker occupies offset 1 and the same transactional ID continues at data/marker
2/3。A second OPEN transaction starts at data offset 4 on broker 2；the two partitions move back to broker 1，the original
producer aborts through the reverse-migrated coordinator and the asynchronous ABORT marker releases LSO at offset 6。The
same aborted transactional ID then commits data/marker 6/7；READ_COMMITTED from 4 skips directly to 6 and final
earliest/latest is `0/8`。Both handoffs require exact singleton leader/replicas/ISR and an empty reassignment map。Fresh
execution passes 64/64 tasks in 47s；the deterministic mandatory NTC2 admission gate is covered by product
`b6b02f4` + fork `89b66ab03b`，and product `0ae8ca9` + fork `768924da60` now add the real Object-WAL
deletion/corruption + exact repair/re-election gate。Injected marker/EndTxn failure and non-Object profiles remain open。

`f9InFlightTakeoverProcessIntegrationTest` adds the C cut with an independent controller JVM、a Toxiproxy-held
single-attempt Produce、`jcmd` proof of `NereusUnifiedLog.appendStable` waiting on the provider future and a broker-1
`SIGSTOP` before reassignment。After `[2]` is stable，`SIGCONT` makes the old append fail with
`append session changed before guarded object upload`；the WAL key set/latest offset do not move and broker 2 continues at
offset 1。`f9BookKeeperProfileTakeoverProcessIntegrationTest` now repeats the live singleton handoff with real stock
ZooKeeper metadata、two Bookies and two Kafka release processes for WAL-only、async-object and sync-object。Each profile
commits offset 0 on `[1]`，requires exact `[2]` leader/replicas/ISR and empty reassignment while node 1 remains alive，then
recovers offset 0 and commits/fetches offset 1 on node 2。WAL-only proves its bucket remains empty；both Object profiles prove
real NCP2 objects exist across the handoff。Fresh execution passes 64/64 actionable tasks in 2m17s。This closes BookKeeper
post-handoff P coverage。`f9BookKeeperInFlightTakeoverProcessIntegrationTest` supplies the matching shared-appender C cut
without changing production code：a test-only Java agent allows the real Bookie future to succeed、captures exact
`ledgerId/entryId` evidence while the Oxia reservation remains `WRITING`，then withholds completion from
`BookKeeperPrimaryWalAppender`。After broker 1 is frozen and KRaft installs `[2]`，broker 2's first append invokes
`BookKeeperLedgerRecovery`，which must transition that exact reservation to `ABANDONED` and the old root to `SEALED` before
committing offset 1。Resuming broker 1 releases the delayed future but its stale metadata CAS cannot publish；LEO remains 2
and WAL-only publishes no Object bytes。Fresh execution passes 66/66 actionable tasks in 1m30s。Because this cut precedes
`DURABLE` and the profile-specific materialization branch，the same production boundary is shared by WAL-only、async and
sync；the prior three-profile P matrix supplies the profile-specific half。Completed and live OPEN Object-WAL
transaction/internal-topic coordinator migration are covered by the gates above；injected resolution cuts、profile expansion
and broader chaos remain open。The native
checkpoint/virtual-segment trim/restart subset is now covered separately by
`f9CheckpointTrimRecoveryProcessIntegrationTest`：stock DeleteRecords publishes a rooted NKC1、advances durable trim，
survives forced broker death，hydrates the checkpoint under its captured pre-trim window，prunes canonical state to the
current trim and continues Produce/Fetch/ListOffsets。
`f9TrimResponseLossProcessIntegrationTest` and `f9TrimProfileMatrixProcessIntegrationTest` then place a one-shot cut after
the real `DefaultStreamStorage.trim` provider future succeeds but before the retention barrier observes completion。Across
Object sync/async and BookKeeper WAL-only/async/sync，the old process shows stream `trim/end=3/6` while binding start stays
`0` and DeleteRecords remains pending；after forced death，a fresh process restores `3/6`，same-target retry leaves
stream/binding versions、checkpoint references and NKC1 key set unchanged，and continuation reaches `3/7`。The focused
tasks pass 66/66 and 75/75 actionable tasks；this closes the five-profile KF-RET-005/010 process slice without claiming the
remaining DeleteRecords boundary/oracle or M7 aggregate。

The same published head and
`f9MultiControllerFailoverProcessIntegrationTest` now close the ACTIVE steady-state controller-kill subset of the
multi-controller gap。Three real combined broker/controller processes use one three-voter KRaft quorum and shared
Oxia/S3/Nereus authority but isolated per-process directories。The harness observes exact voter set、initial leader/epoch and
the initial leader's per-epoch
`Nereus Kafka storage activation reconciled by controller <id> at epoch <epoch>` log，then reads ACTIVE activation/readiness
directly from Oxia。It places the RF1 data partition on a node other than the controller leader，commits offset 0 and forcibly
kills the controller process。The survivors must elect a different leader at a higher epoch without changing the voter set；
that leader must emit the same success marker for its new exact epoch，while the durable activation record remains immutable
and readiness epoch does not regress。Native Produce/Fetch/ListOffsets then continue through offset 1 with final `0/2` and a
positive S3 object count。The fresh direct task passes 64/64 actionable tasks in 36s and is aggregated by
`phase9M6KafkaProcessCheck`。This is the ACTIVE steady-state P/C subset of KF-OPS-005。

`f9ActivationCutFailoverProcessIntegrationTest` adds the complete three-operation store-publication boundary matrix with
three dedicated controller release JVMs and one dedicated broker JVM。A test-only Byte Buddy agent is installed on every
controller but the harness arms only the direct-Admin-observed current leader。For each of the real Oxia
`createReadiness`、`createActivation` and `compareAndSetActivation` methods，the before-provider phase skips the provider、writes `blocked` and
returns an incomplete future；the after-provider phase requires the underlying future to succeed、writes `applied` and keeps
the coordinator-visible future incomplete。The harness directly freezes activation absent/PREPARED/ACTIVE plus readiness，
asserts the leader has not logged reconciliation success and forcibly kills that exact process。A different higher-epoch
controller must reconcile：an empty control plane recreates readiness after revalidation，readiness-only state reuses the
existing readiness epoch/metadata offset，PREPARED preserves every immutable preparation fact while advancing to ACTIVE，
and an already-applied ACTIVE record remains exactly equal。All six cuts preserve or safely create readiness for the stable
broker set `[4]`，then admit node 4 and pass native Produce/Fetch/ListOffsets at `0/1` with a positive Object count。Fresh
`--rerun-tasks` regression passes 75/75 actionable tasks in 2m40s and the task is aggregated by
`phase9M6KafkaProcessCheck`。

The first before-PREPARED execution found that replacement recovery reused valid readiness at offset `r` but built PREPARED
with a newer snapshot offset `s`，which immediately violated the exact PREPARED/readiness tuple。Production
`createPrepared` now stores `readiness.kraftMetadataOffset()` and
`resumesAbsentActivationFromExistingReadinessAfterControllerFailure` provides a narrow deterministic regression。The
store-publication matrix is closed。

`f9ActivationProofCutFailoverProcessIntegrationTest` closes the adjacent four initial-proof cuts on the same
three-dedicated-controller/one-broker release topology。The agent targets
`KafkaStorageFirstActivationCoordinator.currentSnapshot()` and `loadCapabilities(...)`，before invocation or after the
first successful returned-stage completion。The successful snapshot boundary includes the fork-owned KRaft/local-log fact
and all 64 binding registry scans；the successful capability boundary includes broker `[4]` identity、epoch、expiry、
five-profile compatibility and provider-scope digest aggregation。Exceptional attempts are forwarded without consuming the
after-provider one-shot capture。Each cut observes readiness/activation absent，kills the exact armed leader and requires a
different higher-epoch controller to repeat the proof、publish ACTIVE/readiness `[4]` and pass native IO/Object persistence。
Fresh execution passes 66/66 actionable tasks in 1m49s and is aggregated by `phase9M6KafkaProcessCheck`。

`f9ActivationTransportRecoveryProcessIntegrationTest` now closes the adjacent actual Oxia transport-reset boundary。It routes
one dedicated controller and one dedicated broker through Toxiproxy，establishes the controller's exact KRaft epoch，then
installs a downstream connection reset before the broker publishes capability。During the four-second cut both processes
stay alive and direct Oxia inspection requires readiness/activation absent；after proxy recovery the same controller epoch
must reconcile to ACTIVE and node 2 must pass native Produce/Fetch/ListOffsets `0/1` plus Object persistence。This process
gate exposed that `OxiaJavaKafkaStorageActivationMetadataStore` returned arbitrary runtime transport failures unchanged，
causing the fork controller runtime to mark the epoch terminal。The store now preserves typed Nereus/condition/invariant
failures but normalizes unknown read/write provider failures to retriable `METADATA_UNAVAILABLE`，including synchronous
provider throws converted to failed futures。The narrow store regression and its complete contract pass；the process gate
passes 73/73 actionable tasks in 1m10s。The initial snapshot/capability-aggregation cuts now pass separately；the M7
aggregate remains before KF-OPS-005 can leave `PLANNED`。

The 2026-07-28 fresh partial aggregate exposed a second-generation BookKeeper materialization planner defect after the first
source ledger had already completed terminal retirement and physical deletion。The next wider task must select the committed
NCP2 higher-generation prefix plus the still-readable BookKeeper generation-zero tail；comparing raw
`KAFKA_RECORD_BATCH` directly with the versioned NCP2 logical format `KAFKA_RECORD_BATCH_V1` instead selected the deleted
BookKeeper prefix and failed closed as an unreadable source。`DefaultMaterializationPlanner` now normalizes only that exact
byte-equivalent Kafka mapping to the payload compatibility name while retaining strict equality for every other mapping。
`MaterializationPlannerTest.prefersKafkaNcp2ReplacementBeforeAReadableBookKeeperTail` fixes the source identities、
generations and coverage boundary；the real-Oxia/two-bookie physical deletion gate then completes both tasks and proves the
old ledger remains deleted while NCP2 remains byte-readable。Against fork `ebf1d76163` and the exact product working source，
the combined
`phase9M3KafkaForkCheck phase9M5CompactionCoreCheck phase9M6ActivationMetadataCheck phase9M6KafkaFeatureCheck
phase9M6CheckpointQuarantineCheck --rerun-tasks`
run passes 109/109 outer tasks；nested Kafka builds pass 92/92 stock and 95/95 artifact-enabled actionable tasks，with
86/86、42/42、74/74 and 20/20 focused feature/control/static invocations also successful。This is a fresh aggregate for
the implemented slice，not KF-FINAL-001/002 release evidence。

该段执行时 HTTPS credential 对组织 fork 的 API permission 是 `read`，因此当时只能称为 development source
lock。2026-07-28 已通过本机 SSH identity 发布完整 branch；当前远端
`nereus/future9-native-kafka-storage` 与工作 clone HEAD 均为
`768924da60f10b2b9611d19c0c4cb7df2a10947f`。Executable source-lock expectation 已更新到该 reviewed、
published head；KF-SRC-004 仍须随完整 final gate 一起执行后才能标记 complete。
