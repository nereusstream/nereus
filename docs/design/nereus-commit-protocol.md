# 技术细节：Nereus Commit Protocol

> 这是 `pip/Nereus/nereus-overall-architecture.md` 的配套技术细节文档。
> 本文定义 append、offset index、compaction、SBT/SDT、cursor/txn 的提交协议和恢复语义。

## 1. 目标

本文要回答：

- producer ack 的线性化点在哪里；
- multi-stream WAL object 如何允许部分 stream slice 可见；
- Oxia 如何分配 offset 并推进 `committedEndOffset`；
- compaction 如何把 WAL index entries 替换成 compacted object entries；
- SBT/SDT catalog commit 如何与 Oxia offset index 对齐；
- broker crash、Oxia failover、object orphan、catalog partial commit 如何恢复。

## 2. 核心不变量

1. Producer ack 之前，WAL bytes 必须 durable，且对应 stream slice 的 Oxia offset index
   entry 必须 committed。
2. `committedEndOffset` 对每个 stream 单调递增。
3. Append path 生成的 offset range 必须 dense，不允许可见 gap。
4. Multi-stream WAL object 的物理 durable 不代表所有 slices 可见。
5. 每个 stream slice 独立提交 offset index entry。
6. Compaction 不改变 offset，只改变 offset range 指向的 physical object。
7. Compaction 发布新 generation 后，旧 generation 保持可读直到 GC 安全。
8. SBT/SDT 不能影响 producer ack。
9. 所有 fencing 都以 Oxia epoch/token 为准。
10. Object list 不能参与 recovery 正确性判断。

## 3. 参与者

| 参与者 | 职责 |
| --- | --- |
| Broker | 协议处理、batching、WAL object 写入、append session、producer ack |
| Oxia | offset authority、append fencing、offset index、cursor/txn/routing metadata |
| Object store | WAL/compacted/index/snapshot bytes durability |
| Compaction planner | 生成 compaction tasks，维护 checkpoint |
| Compaction worker | 读取 WAL/old compacted objects，生成 compacted object |
| Committer | 提交 offset index replacement、SBT/SDT metadata |
| Lakehouse catalog | Iceberg/Delta/Hudi snapshot metadata |
| GC worker | 删除 orphan 和不再被引用的 objects |
| Repair worker | 修复 catalog/index partial commit |

## 4. Append Session Protocol

### 4.1 Acquire

Broker 写某个 stream 前必须持有 append session：

```text
AcquireAppendSession(streamId, brokerId, previousEpoch?)
```

Oxia 操作：

```json
{
  "path": "/streams/{streamId}/append-session",
  "condition": "missing OR expired OR brokerId == requester OR epoch < requestedEpoch",
  "put": {
    "brokerId": "broker-7",
    "epoch": 42,
    "fencingToken": "token-42",
    "ttlMs": 30000,
    "leaseVersion": 128
  }
}
```

规则：

- append session 是性能和 fencing 工具，不是 durable ownership；
- session 过期后，其他 broker 可以获得更高 epoch；
- 所有 offset index commit 都必须校验 epoch/token；
- stale broker 的 commit 被 Oxia 拒绝。

### 4.2 Renew

Broker 周期性 renew session。Renew 失败后 broker 必须：

- 停止为该 stream 生成新的 WAL slices；
- 已上传但未 commit 的 slices 不 ack；
- 重新 lookup routing/session；
- 对 client 触发 retry 或 backpressure。

## 5. Object WAL Append Commit

### 5.1 Prepare WAL Object

Broker 聚合多个 streams 的 entries：

```text
Buffer(streamId -> entries)
flush when size >= walObjectFlushSize or age >= walObjectFlushInterval
```

生成 WAL object：

```json
{
  "objectId": "wo-20260703-000001",
  "slices": [
    {
      "streamId": "s-123",
      "epoch": 42,
      "entryCount": 4096,
      "recordCount": 65536,
      "relativeBaseOffset": 0,
      "payloadOffset": 8388608,
      "payloadLength": 67108864,
      "entryIndexRef": "..."
    }
  ]
}
```

### 5.2 Upload WAL Object

Broker 上传 object，并校验 object checksum。上传成功后，broker 写入 object manifest：

```text
Put /wal-objects/{objectId}/manifest
state = UPLOADED
checksum = ...
sliceCount = N
ttl = orphanTtl
```

此时 object 仍不可见。可见性只由 per-stream offset index entry 决定。

### 5.3 Commit Stream Slice

每个 stream slice 独立提交：

```text
CommitSlice(streamId, objectId, sliceId, expectedStartOffset, epoch, fencingToken)
```

Oxia 线性化操作：

```json
{
  "conditions": [
    "/streams/{streamId}/append-session.epoch == epoch",
    "/streams/{streamId}/append-session.fencingToken == fencingToken",
    "/streams/{streamId}/committed-end-offset == expectedStartOffset"
  ],
  "sequence": {
    "offset": "+recordCount",
    "generation": "+1"
  },
  "puts": [
    "/streams/{streamId}/offset-index/{offsetEnd}/{generation}",
    "/streams/{streamId}/committed-end-offset",
    "/streams/{streamId}/virtual-ledgers/{virtualLedgerId}",
    "/wal-objects/{objectId}/slices/{streamId}"
  ],
  "notify": [
    "stream-data-available",
    "offset-index-updated"
  ]
}
```

Commit result：

```json
{
  "baseOffset": 1048576,
  "endOffset": 1114112,
  "generation": 17,
  "virtualLedgerId": 9007199254740993,
  "entryBaseId": 0,
  "commitVersion": 88
}
```

Producer ack 使用 commit result 生成：

- Pulsar `MessageId(virtualLedgerId, entryId, batchIndex)`；
- Kafka offset = stream record offset。

### 5.4 Partial Slice Commit

同一个 WAL object 中，slice A commit 成功、slice B commit 失败是合法状态。

规则：

- 成功 slice 可见并可 ack；
- 失败 slice 不可见，不 ack；
- 失败原因为 stale epoch 时，broker 必须重新 acquire session；
- 失败原因为 offset conflict 时，broker 必须重新读取 committedEndOffset 并判断是否由 retry/dedup 覆盖；
- WAL object GC 不能删除仍被任一成功 slice 引用的 object。

## 6. BookKeeper WAL Commit

Latency profile 下，BK 作为 WAL durable layer：

1. Broker 写 BK ledger entry。
2. BK quorum ack 后 bytes durable。
3. Broker 对 stream 提交 Oxia offset index entry，`objectType=BK_WAL_RANGE`。
4. Producer ack 来自 Oxia offset index commit result。
5. Compaction service 可以把 BK range 读出并写成 compacted object，然后替换 offset index。

BK ledger id 不是内部 truth，只是 WAL object location 的一种。

## 7. Offset Index Read Resolution

Offset index 使用 generation overlay，避免 compaction 依赖大范围删除。

Key：

```text
/streams/{streamId}/offset-index/{offsetEnd}/{generation}
```

Read algorithm：

```text
resolve(streamId, offset):
  candidates = scan offset-index where offsetEnd > offset, limited by small range
  filter candidates where offsetStart <= offset
  choose highest generation that is not tombstoned and covers offset
  return object location + entryIndexRef
```

Append generation 和 compaction generation 都是可见 generation。Compaction 写入更高
generation 的覆盖 entry；旧 entries 保持可读，直到 GC 判断不再需要。

## 8. Compaction Commit

### 8.1 Task Planning

Planner 从 Oxia 读取 offset index：

```text
sourceEntries = offset-index entries for [startOffset, endOffset)
```

生成 task：

```json
{
  "taskId": "cmp-s-123-1048576-1114112",
  "streamId": "s-123",
  "offsetStart": 1048576,
  "offsetEnd": 1114112,
  "sourceEntries": [
    {"offsetEnd": 1050000, "generation": 10, "checksum": "..."}
  ],
  "targetFormat": "PARQUET"
}
```

Task checkpoint 写入 Oxia，planner 可恢复。

### 8.2 Worker Output

Worker 读取 source entries 指向的 WAL objects，只抽取目标 stream slices，生成
per-stream compacted object。

输出 metadata：

```json
{
  "objectId": "co-s-123-1048576-1114112-g18",
  "streamId": "s-123",
  "offsetStart": 1048576,
  "offsetEnd": 1114112,
  "recordCount": 65536,
  "checksum": "crc32c:...",
  "rowGroupIndex": [...]
}
```

### 8.3 Publish Compacted Generation

Committer 提交新 generation：

```json
{
  "conditions": [
    "sourceEntries still exist",
    "sourceEntries not tombstoned",
    "compactedObject checksum verified"
  ],
  "sequence": {
    "generation": "+1"
  },
  "put": "/streams/{streamId}/offset-index/{offsetEnd}/{generation}",
  "value": {
    "objectType": "STREAM_COMPACTED_OBJECT",
    "offsetStart": 1048576,
    "offsetEnd": 1114112,
    "supersedes": ["1050000/10", "1060000/11"],
    "objectId": "co-s-123-1048576-1114112-g18"
  }
}
```

线性化点是新 generation 的 offset index entry 可见。读路径选择最高 generation，
因此新 compacted object 生效；旧 WAL entries 仍可作为 fallback 直到 GC。

### 8.4 Topic Compaction

Kafka/Pulsar topic compaction 复用同一协议：

- Worker 对 key 保留最高 offset record；
- discarded records 的 offsets 仍在 index 覆盖范围内；
- compacted object 保留 key -> latest offset 结果；
- 新 generation 覆盖旧 range；
- 旧对象在 cursor/reader 安全后 GC。

## 9. SBT Commit Protocol

SBT 是内建 table view。SBT commit 不进入 producer ack 路径。

### 9.1 Commit Order

目标语义：

```text
stream visibility truth = Oxia offset index
table query visibility = catalog snapshot
catalog snapshot must not reference uncommitted object
```

推荐顺序：

1. Compacted object upload succeeds.
2. Oxia publishes compacted offset index generation.
3. SBT committer creates catalog snapshot referencing compacted object and index generation.
4. Oxia records `/sbt/{streamId}/snapshots/{snapshotId}`.

如果 step 3 失败，stream read 不受影响，SBT lag 增加。Repair worker 根据 Oxia offset
index 补交 catalog snapshot。

### 9.2 Catalog Snapshot Metadata

Catalog snapshot 必须包含：

- `streamId`；
- `offsetStart/end`；
- `indexGeneration`；
- `objectId`；
- `schemaId`；
- `commitVersion`。

外部 query engine 不需要理解 Oxia，但内部 repair/audit 必须能从 snapshot 追溯到
Oxia index generation。

## 10. SDT Commit Protocol

SDT 面向外部表交付，不影响 stream truth。

1. SDT planner 选择 committed stream offset range。
2. Worker 生成目标表文件。
3. Committer 使用 deterministic delivery id 提交外部 catalog。
4. Oxia 记录 delivery state。

幂等 key：

```text
deliveryId = hash(streamId, offsetStart, offsetEnd, targetCatalog, targetTable)
```

失败处理：

- 外部 catalog commit timeout：查询 catalog 确认 delivery id；
- 外部 catalog commit 成功但 Oxia 记录失败：repair worker 补 Oxia state；
- Oxia 记录成功但外部 catalog 失败：state 标记 `NEEDS_RETRY`。

## 11. Cursor Commit Protocol

Cursor update 使用 Oxia CAS：

```json
{
  "path": "/streams/{streamId}/cursors/{subscription}/state",
  "condition": "version == expectedVersion",
  "put": {
    "markDeleteOffset": 1048576,
    "readPositionOffset": 1052672,
    "ackRangesInline": [],
    "ackRangesSnapshotRef": "optional",
    "version": 89
  }
}
```

大 ack ranges：

1. 写 cursor snapshot object；
2. 校验 checksum；
3. CAS 更新 cursor state 指向 snapshot；
4. 旧 snapshot 由 cursor version 和 GC 引用计数保护。

## 12. Transaction Commit Protocol

Transaction state 是 explicit state machine：

```text
OPEN -> COMMITTING -> COMMITTED
OPEN -> ABORTING -> ABORTED
```

Commit 规则：

- transaction marker 写入 stream offset index 覆盖的 metadata；
- committed transaction 的 records 才可 dispatch；
- aborted transaction 的 ranges 写入 abort index；
- reader 必须在 dispatch 前应用 abort/commit visibility。

跨 stream transaction 不依赖 Oxia 全局事务。事务协调器记录 affected stream ranges，
每个 stream 的 marker/visibility 按 stream key group 提交，最终 transaction state
进入 `COMMITTED` 或 `ABORTED`。

## 13. Recovery

### 13.1 Broker Crash

| 崩溃点 | Recovery |
| --- | --- |
| object upload 前 | 无 durable bytes，无可见数据 |
| object upload 后、manifest 前 | object orphan，TTL 后 GC |
| manifest 后、slice commit 前 | object uploaded，但 slice 不可见；repair 可重试 slice commit 或 GC |
| slice commit 后、ack 前 | 数据可见；producer retry 依赖 sequence/dedup |
| partial slice commit | 成功 slices 可见，失败 slices 不可见 |

### 13.2 Oxia Failover

Oxia shard leader failover 后：

- committed offset index entry 必须仍可读；
- append session 可能需要 renew；
- stale token commit 被拒绝；
- broker 重新读取 committedEndOffset 后继续。

### 13.3 Compaction Failure

| 故障 | Recovery |
| --- | --- |
| worker 写 compacted object 前失败 | task retry |
| compacted object 写成功、index publish 前失败 | object orphan 或 task retry reuse |
| index publish 成功、SBT catalog 失败 | stream read 使用 compacted object，SBT repair 补 snapshot |
| SBT catalog 成功、Oxia snapshot state 失败 | repair 从 catalog snapshot 补 Oxia state |

## 14. 线性化点

| 操作 | 线性化点 |
| --- | --- |
| Produce append | Oxia offset index entry commit |
| Producer ack | append 线性化点之后 |
| Cursor update | Oxia cursor CAS success |
| Transaction commit | transaction state `COMMITTED` CAS success after stream markers |
| Transaction abort | transaction state `ABORTED` CAS success after abort ranges |
| Compaction replacement | higher-generation offset index entry visible |
| SBT table visibility | lakehouse catalog snapshot commit |
| SDT delivery visibility | target catalog commit |
| GC delete | object reference check + delete marker commit |

## 15. 与 Ursa 的对齐状态

已对齐：

- Oxia/metadata service 作为 offset authority；
- broker stateless/leaderless；
- produce ack 等待 WAL durable + metadata commit；
- multi-stream WAL object；
- offset index 驱动读路径；
- distributed compaction 替换 offset index；
- lakehouse catalog 与 stream storage 共享 compacted objects。

增强点：

- 明确 partial slice commit 语义；
- 明确 Pulsar MessageId projection 的 ack 生成点；
- 明确 cursor snapshot 和 transaction state；
- 明确 SBT/SDT 的不同 commit 语义。

当前设计关注点：

- generation overlay 必须有清晰的 reader 选择规则和 fallback 规则；
- compaction publish 的线性化点必须是更高 generation offset index entry 可见；
- SBT/SDT 不得进入 producer ack 主路径；
- cursor update 必须使用 Oxia CAS，snapshot 只由 cursor state 引用生效；
- transaction marker、abort range 和 pending ack 状态必须与 stream offset visibility 对齐；
- cross-stream transaction 只能作为显式协调状态，不能隐式依赖 broker owner。

## 16. 参考

- 总体架构：`pip/Nereus/nereus-overall-architecture.md`
- Object format：`pip/Nereus/nereus-storage-object-format.md`
- Ursa VLDB paper: <https://www.vldb.org/pvldb/vol18/p5184-guo.pdf>
- Oxia documentation: <https://oxia-db.github.io/docs/what-is-oxia>
