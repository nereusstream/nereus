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
| `nereus-api/.../StreamStorage.java` | `d182633ce4c341343aa31009bd82ae30c1c887e9` | binary-safe append/read overloads；legacy-equivalent delegates，new unsupported semantics fail closed |
| `nereus-api/.../AppendPrecondition.java` | `799caf45554ffaab72927bf18e869cce9ba02d2a` | implemented optional non-negative expected start |
| `nereus-api/.../ReadRequest.java` | `e43bd0bb99f1762cc4893e20da18450c9e94b76f` | implemented view/boundary/first-entry/options request |
| `nereus-api/.../SemanticReadResult.java` | `3b2605616150ffb9efada7287df90fa557b92a34` | implemented request/result boundary、dense COMMITTED and sparse TOPIC_COMPACTED coverage validation |
| `nereus-api/.../ErrorCode.java` | `2be9e0ca8ac3638755a294ef4380665ff03a0225` | unsupported append/read semantics codes appended at enum tail |
| `nereus-core/.../DefaultStreamStorage.java` | `7faae39346d90c1310ae9a0515a379a2e13d33b8` | production overrides conditional append and semantic read overloads |
| `nereus-core/.../AppendCoordinator.java` | `68c0457cddf7db051da12f676c8c0dc599458be7` | conditional calls refresh head in-lane before WAL prepare；legacy calls preserve the old none-precondition path |
| `nereus-core/.../AppendResultValidator.java` | `336d08fa1078248b31925f5a6391932f177999ec` | post-commit/recovery result is checked against exact stream/range/count/bytes/format/schema/expected-start facts |
| `nereus-core/.../AppendSessionManager.java` | `67a5a9202f8ab0caeca994ec359a0eb5ee5a379b` | writer-id/TTL session；no KRaft authority epoch |
| `nereus-core/.../ReadCoordinator.java` | `293b5bacd47c2908e226cadec32a6f67b01bdfb5` | public request owns exact/containing boundaries、global first overflow、source/coverage/result validation |
| `nereus-core/.../ParquetV2CompactedTargetReader.java` | `11294d4965e76c94cf34a5d66d455c7ace25dad4` | exact NCP2/NTC2 adapter；sparse topic coverage is explicit |
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
