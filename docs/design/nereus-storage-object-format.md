# 技术细节：Nereus Storage Object Format

> 这是 `pip/Nereus/nereus-overall-architecture.md` 的配套技术细节文档。
> 总体架构一步到位；本文只拆解 object/index/table 文件格式，便于后续按模块实施。

## 1. 目标

本文定义共享存储数据面的格式：

- multi-stream WAL object；
- stream slice index；
- Pulsar entry index；
- Oxia offset index entry；
- per-stream compacted object；
- Stream-Backed Table (SBT) metadata；
- Stream-Delivered-to-Table (SDT) metadata；
- cursor/transaction snapshot object。

设计目标是对齐 Ursa 的关键能力：一个 WAL object 聚合多个 streams，metadata service
维护 offset index，compaction 把 row-based WAL objects 转为 per-stream columnar
objects，并让同一份 object 同时服务 streaming read 和 lakehouse query。
同一套格式也服务 AutoMQ-like profile：append ack 可以早于 read-optimized object publish，
但后台 materializer 发布的对象、index generation、SBT/SDT metadata 仍使用本文定义的格式。

## 2. 不变量

1. `streamId + offset` 是唯一内部坐标。
2. Oxia offset index 决定数据是否可见。
3. Object store 只存 bytes，不决定可见性。
4. Object 一旦 commit 即不可变。
5. Object list 不能参与正确性判断。
6. Oxia 不存 per-message metadata。
7. 一个物理 WAL object 可以包含多个 stream slices。
8. 每个 stream slice 的可见性由自己的 offset index entry 决定。
9. Pulsar `MessageId` 是 virtual ledger projection，不是内部排序依据。
10. Compacted object 替换的是 offset index 指向，不改变 stream offset。
11. AutoMQ-like async materialization 不能引入第二套对象格式或第二套 offset truth；它只改变
    这些对象何时被后台发布。

## 3. 编码约定

控制元数据使用 Protobuf 或等价 schema-first 编码。下面的 JSON 只是字段说明，不是磁盘
格式要求。

二进制 object 使用小端编码，所有可变长 section 都带：

```text
sectionType: uint16
sectionVersion: uint16
sectionLength: uint32
sectionChecksum: uint32  // CRC32C
payload: bytes
```

公共 header：

```text
magic: bytes[4]          // "NRS1"
formatVersion: uint16
objectType: uint16       // WAL, COMPACTED, INDEX, CURSOR_SNAPSHOT, TXN_SNAPSHOT
flags: uint32
headerLength: uint32
headerChecksum: uint32
footerOffset: uint64
footerLength: uint32
objectChecksum: bytes    // CRC32C required, SHA-256 optional
encryptionInfoRef: bytes // optional
```

兼容规则：

- reader 必须拒绝未知 major version；
- reader 可以跳过未知 optional section；
- writer 只能 append 新 section，不能改变已有 section 语义；
- object metadata 中必须记录 writer version 和 format version；
- checksum 覆盖 header、payload blocks、footer 和 index sections。

## 4. Multi-stream WAL Object

WAL object 是 row-based durable log。它面向写入效率，一个 object 可以包含多个
stream 的 slices。

### 4.1 Layout

```text
WALObject
  CommonHeader
  WALObjectHeader
  StreamSliceDirectory
  PayloadBlock[0..N]
  EntryIndexSection[0..N]
  Footer
```

### 4.2 WALObjectHeader

```json
{
  "objectId": "wo-20260703-000001",
  "clusterId": "prod-a",
  "writerBrokerId": "broker-7",
  "writerEpoch": 42,
  "createdAtMillis": 1783036800000,
  "walProfile": "OBJECT",
  "compression": "ZSTD",
  "encryption": "AES_GCM_256",
  "streamSliceCount": 128,
  "payloadBlockCount": 64,
  "minPublishTime": 1783036800000,
  "maxPublishTime": 1783036810000
}
```

### 4.3 StreamSliceDirectory

Stream slices 按 `streamId` 排序。排序不是正确性要求，但能改善 footer/index 查询和
compaction 顺序读。

```json
{
  "slices": [
    {
      "streamId": "s-123",
      "sliceId": "wo-20260703-000001/s-123/0",
      "relativeBaseOffset": 0,
      "entryCount": 4096,
      "recordCount": 65536,
      "payloadOffset": 8388608,
      "payloadLength": 67108864,
      "entryIndexOffset": 259522560,
      "entryIndexLength": 8192,
      "checksum": "crc32c:...",
      "schemaIds": ["schema-1", "schema-2"],
      "minEventTime": 1783036800000,
      "maxEventTime": 1783036810000
    }
  ]
}
```

字段语义：

| 字段 | 含义 |
| --- | --- |
| `relativeBaseOffset` | slice 内第一条 record 的相对 offset，最终 base offset 由 Oxia commit 返回 |
| `entryCount` | Pulsar entry 数量 |
| `recordCount` | 展开 batch 后的 record 数量 |
| `payloadOffset/Length` | 该 slice 在 object 内的物理范围 |
| `entryIndexOffset/Length` | entry index 在 object 内的位置 |
| `schemaIds` | slice 中出现的 schema id 集合，用于 compaction 转列式 |

### 4.4 PayloadBlock

PayloadBlock 保存 Pulsar entry 或 Kafka record batch 的原始行式数据。

```json
{
  "blockId": 12,
  "streamId": "s-123",
  "compression": "ZSTD",
  "uncompressedLength": 33554432,
  "compressedLength": 8388608,
  "checksum": "crc32c:...",
  "payloadFormat": "PULSAR_ENTRY_BATCH"
}
```

`payloadFormat` 可选：

- `PULSAR_ENTRY_BATCH`
- `KAFKA_RECORD_BATCH`
- `NORMALIZED_ROW_BATCH`

目标格式是 `PULSAR_ENTRY_BATCH`，KoP 在协议层做 Kafka projection。若后续为了
Kafka 性能引入 `KAFKA_RECORD_BATCH`，也必须映射回同一个 stream offset truth。

## 5. Pulsar Entry Index

Entry index 负责把 Pulsar `entryId + batchIndex` 映射到 stream record offset。

```json
{
  "virtualLedgerId": 9007199254740993,
  "entryBaseId": 0,
  "segmentStartOffset": 1048576,
  "segmentEndOffset": 1114112,
  "entries": [
    {
      "entryId": 10,
      "relativeBaseOffset": 34,
      "recordCount": 20,
      "payloadOffset": 8192,
      "payloadLength": 16384,
      "batchMetadataOffset": 1024,
      "publishTime": 1783036800010,
      "eventTime": 1783036800000,
      "schemaId": "schema-1",
      "txnId": "optional",
      "flags": ["BATCHED"]
    }
  ]
}
```

映射规则：

```text
entryBaseOffset = offsetIndex.offsetStart + entry.relativeBaseOffset
recordOffset = entryBaseOffset + batchIndex
```

非 batch entry 的 `recordCount = 1`，`batchIndex` 为空或 0。Batch entry 的
`recordCount > 1`，`batchIndex` 必须落在 `[0, recordCount)`。

Entry index 存放策略：

| 大小 | 存放位置 |
| --- | --- |
| 小于 16 KiB | 内联在 Oxia offset index value |
| 16 KiB 到 4 MiB | WAL object footer |
| 大于 4 MiB | 独立 index object，Oxia 只存引用 |

## 6. Oxia Offset Index Entry

Offset index entry 是读路径、cursor、retention、compaction、SBT/SDT 的共同索引。

Key：

```text
/streams/{streamId}/offset-index/{offsetEnd}/{generation}
```

Value：

```json
{
  "streamId": "s-123",
  "generation": 17,
  "offsetStart": 1048576,
  "offsetEnd": 1114112,
  "cumulativeSize": 9876543210,
  "objectId": "wo-20260703-000001",
  "objectKey": "prod-a/wal/2026/07/03/wo-20260703-000001",
  "objectType": "MULTI_STREAM_WAL_OBJECT",
  "physicalFormat": "ROW_WAL",
  "logicalFormat": "PULSAR_ENTRY_BATCH",
  "objectOffset": 8388608,
  "objectLength": 67108864,
  "entryIndexRef": {
    "location": "OBJECT_FOOTER",
    "offset": 259522560,
    "length": 8192,
    "checksum": "crc32c:..."
  },
  "virtualLedgerId": 9007199254740993,
  "entryBaseId": 0,
  "recordCount": 65536,
  "entryCount": 4096,
  "minEventTime": 1783036800000,
  "maxEventTime": 1783036810000,
  "supersedes": [],
  "commitVersion": 88
}
```

读路径：

1. 找到第一个 `offsetEnd > targetOffset` 的候选 entry。
2. 如果存在多个 generation，选择可见 generation 最大且覆盖目标 offset 的 entry。
3. 根据 `objectType` 选择 WAL reader 或 compacted object reader。
4. 根据 `entryIndexRef` 定位 Pulsar entry 或 Kafka record。

`cumulativeSize` 是该 stream 到 `offsetEnd` 为止的累计 logical bytes，用于 backlog、
quota、retention、compaction window 和 billing。

## 7. Per-stream Compacted Object

Compacted object 是 per-stream、read-optimized、columnar 的物理对象。默认格式是
Parquet，表格式由 Iceberg/Delta/Hudi catalog 管理。

```json
{
  "objectId": "co-s-123-1048576-1114112-g18",
  "streamId": "s-123",
  "generation": 18,
  "offsetStart": 1048576,
  "offsetEnd": 1114112,
  "recordCount": 65536,
  "entryCount": 4096,
  "physicalFormat": "PARQUET",
  "compression": "ZSTD",
  "schemaId": "schema-1",
  "rowGroupIndex": [
    {
      "rowGroupId": 0,
      "offsetStart": 1048576,
      "offsetEnd": 1064960,
      "fileOffset": 4096,
      "length": 8388608,
      "minEventTime": 1783036800000,
      "maxEventTime": 1783036805000
    }
  ],
  "checksum": "crc32c:..."
}
```

Compacted object 必须保留 offset、publish time、event time、key、producer metadata、
schema id、transaction marker、batch 信息等字段，以便投影回 Pulsar/Kafka 语义。

## 8. SBT Metadata

SBT 是内建 lakehouse 表。每个 stream 至少对应一个系统管理表：

```json
{
  "tableType": "STREAM_BACKED_TABLE",
  "streamId": "s-123",
  "tableName": "tenant.namespace.topic.partition0",
  "catalog": "iceberg",
  "snapshotId": 202607030001,
  "offsetStart": 1048576,
  "offsetEnd": 1114112,
  "indexGeneration": 18,
  "dataFiles": [
    {
      "objectId": "co-s-123-1048576-1114112-g18",
      "path": "prod-a/compacted/s-123/1048576-1114112.parquet",
      "format": "PARQUET",
      "recordCount": 65536,
      "fileSizeBytes": 67108864
    }
  ],
  "commitState": "COMMITTED"
}
```

SBT 规则：

- stream 写入只通过 append path；
- external engine 对 SBT read-only；
- SBT snapshot 必须包含 `indexGeneration`；
- catalog snapshot 落后于 stream offset index 是允许的，但不能领先到引用未提交 object；
- repair worker 可以根据 Oxia offset index 重建 SBT metadata。

## 9. SDT Metadata

SDT 是外部交付模式。它可以把同一 stream range 交付到用户指定 catalog/table。

```json
{
  "tableType": "STREAM_DELIVERED_TO_TABLE",
  "streamId": "s-123",
  "targetCatalog": "customer-iceberg",
  "targetTable": "analytics.events",
  "deliveryId": "sdt-20260703-000001",
  "offsetStart": 1048576,
  "offsetEnd": 1114112,
  "sourceIndexGeneration": 18,
  "state": "COMMITTED",
  "retryCount": 0
}
```

SDT 失败不影响 stream visibility。SDT 使用至少一次提交 + 幂等 delivery id，外部表必须
能通过 `(streamId, offsetStart, offsetEnd, deliveryId)` 去重。

## 10. Cursor Snapshot Object

大的 individual ack holes 不进入 Oxia value。

```json
{
  "objectType": "CURSOR_SNAPSHOT",
  "streamId": "s-123",
  "subscription": "sub-a",
  "snapshotVersion": 42,
  "markDeleteOffset": 1048576,
  "ranges": [
    {"start": 1048610, "end": 1048620}
  ],
  "checksum": "crc32c:..."
}
```

Oxia cursor state 只存 snapshot ref、version 和小 range cache。

## 11. Transaction Snapshot Object

Transaction buffer、abort range 或 pending ack 过大时写入 snapshot object。

```json
{
  "objectType": "TXN_SNAPSHOT",
  "coordinatorId": "tc-1",
  "txnId": "txn-123",
  "state": "ABORTED",
  "affectedRanges": [
    {
      "streamId": "s-123",
      "offsetStart": 1048576,
      "offsetEnd": 1048600
    }
  ],
  "checksum": "crc32c:..."
}
```

Reader 必须在 dispatch 前加载可见 transaction/abort metadata，避免暴露 aborted records。

## 12. GC 引用模型

一个 object 可被多类引用持有：

- Oxia offset index entry；
- SBT catalog snapshot；
- SDT delivery metadata；
- active cursor/read handle；
- compaction task；
- recovery task。

GC 只能删除满足全部条件的 object：

1. 不再被任何 active offset index entry 引用；
2. 不再被 SBT/SDT active snapshot 引用；
3. 不再被 cursor low-watermark 保护；
4. 不再被 compaction/recovery task 引用；
5. orphan TTL 到期；
6. checksum/audit 状态明确。

## 13. Ursa-like 与 AutoMQ-like 对齐状态

Ursa-like sync profile 已对齐：

- multi-stream WAL object；
- metadata/offset index 驱动读路径；
- row-based WAL 到 per-stream columnar compacted object；
- stream-table duality；
- compaction 替换 index 而不是改写 stream offset。

AutoMQ-like async profile 复用：

- primary WAL object / BK range 到 read-optimized object 的后台 materialization；
- generation replacement 作为后台发布点；
- SBT/SDT metadata 作为已 materialized ranges 的 lakehouse 投影；
- GC 以 published generation、cursor、reader 和 task 引用为准。

增强点：

- 支持 Pulsar virtual ledger projection；
- 支持 Pulsar entry/batch index；
- 支持 cursor snapshot 和 pending ack snapshot；
- 支持 SBT/SDT 两种 lakehouse 暴露。

当前设计关注点：

- multi-stream WAL object 必须允许每个 stream slice 独立可见；
- entry index 必须能在 Oxia inline、object footer、独立 index object 之间平滑切换；
- compacted object 必须保留足够字段以投影回 Pulsar/Kafka 语义；
- SBT catalog snapshot 不能领先 Oxia offset index；
- SDT metadata 必须支持幂等 delivery；
- GC 必须以 Oxia offset index、cursor low-watermark、catalog snapshot 和 active task 引用为准。

## 14. 参考

- 总体架构：`pip/Nereus/nereus-overall-architecture.md`
- Ursa VLDB paper: <https://www.vldb.org/pvldb/vol18/p5184-guo.pdf>
- Oxia documentation: <https://oxia-db.github.io/docs/what-is-oxia>
