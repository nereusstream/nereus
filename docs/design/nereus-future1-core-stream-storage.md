# Nereus Future 1：Core StreamStorage + Object WAL

> 本文是 Nereus L0 设计文档。它定义 Core Stream Storage 的 API、metadata schema、
> append/read 状态机和故障语义。当前阶段只冻结设计边界，不要求实现、benchmark、
> chaos 或 real evidence。

## 1. Motivation

Nereus 的核心不是把 Pulsar ledger offload 到对象存储，而是建立一套对标 Ursa 的
shared-storage stream engine：

```text
streamId + offset
+ Oxia offset authority
+ shared WAL/data objects
+ stateless broker correctness
+ offset-index-driven read path
```

Future 1 要先把 L0 做清楚。只要 L0 的 stream offset、append session、object WAL、
offset index 和 read resolver 没有稳定，后面的 ManagedLedger facade、KoP、cursor、
compaction、lakehouse、routing 都会互相牵扯。

## 2. Scope

Future 1 覆盖：

- `StreamStorage` API；
- stream metadata；
- append session；
- fencing token；
- object WAL writer；
- BookKeeper WAL profile boundary；
- object manifest；
- Oxia commit-time offset assignment；
- Oxia offset index；
- read resolver；
- trim state；
- basic virtual ledger / MessageId projection reference；
- orphan object and GC reference model。

## 3. Non-scope

Future 1 不解决：

- 完整 ManagedLedger facade；
- ManagedCursor 的复杂 ack holes；
- Shared/Key_Shared dispatch；
- pending ack transaction；
- KoP group coordinator；
- Kafka transaction protocol；
- SBT/SDT catalog commit；
- distributed compaction worker；
- zone-aware routing 和 brown-out。

这些能力只能依赖 Future 1 的 L0 truth，不能反向修改 L0 的坐标模型。

## 4. Layer Boundary

L0 只认识：

```text
cluster
streamId
offset
append epoch
fencing token
WAL object
object manifest
offset index entry
read resolver
trim state
```

L0 不认识：

```text
subscription name
Kafka group id
Pulsar ledger ownership
broker partition leader
lakehouse table snapshot
```

如果 L0 必须保存与上层相关的引用，只能保存 projection reference，例如：

- `virtualLedgerId`；
- `entryIndexRef`；
- `schemaId`；
- `transactionMarkerRef`。

这些引用不能改变 `streamId + offset` 的内部排序和可见性。

## 5. API

### 5.1 StreamStorage

```java
interface StreamStorage {
    CompletableFuture<StreamMetadata> createOrGetStream(
            TopicIdentity topic,
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
            Offset startOffset,
            ReadOptions options);

    CompletableFuture<ResolveResult> resolve(
            StreamId streamId,
            Offset startOffset,
            ResolveOptions options);

    CompletableFuture<Void> trim(
            StreamId streamId,
            Offset beforeOffset,
            TrimOptions options);

    CompletableFuture<StreamMetadata> getStreamMetadata(StreamId streamId);
}
```

API 约束：

- 所有方法异步，返回 `CompletableFuture` 的方法不能同步抛异常。
- `append` ack 必须来自 WAL durable + Oxia offset index commit。
- `read` 只读取已被 Oxia offset index 发布的 ranges。
- `resolve` 是读路径核心，可被 broker cache、ManagedLedger facade、KoP facade 复用。
- `trim` 只更新 trim low-watermark，不直接删除 object。

### 5.2 AppendBatch

```text
AppendBatch
  producerIdentity
  payloadFormat
  records
  entryIndex
  recordCount
  entryCount
  minEventTime
  maxEventTime
  schemaRefs
  protocolProjectionHints
  checksum
```

L0 不解析 Pulsar subscription 或 Kafka group metadata。它只需要知道 batch 内 record
数量、entry 边界、payload format 和 projection hint。

### 5.3 AppendResult

```text
AppendResult
  streamId
  offsetStart
  offsetEnd
  committedEndOffset
  objectId
  objectKey
  objectOffset
  objectLength
  virtualLedgerProjectionRef
  commitVersion
```

`offsetStart` 和 `offsetEnd` 必须来自 Oxia commit result。Broker 不能在本地提前生成
final visible offset。

## 6. Oxia Metadata Schema

### 6.1 Stream keys

```text
/nereus/clusters/{cluster}/streams/{streamId}/meta
/nereus/clusters/{cluster}/streams/{streamId}/append-session
/nereus/clusters/{cluster}/streams/{streamId}/committed-end-offset
/nereus/clusters/{cluster}/streams/{streamId}/offset-index/{offsetEnd}
/nereus/clusters/{cluster}/streams/{streamId}/trim
/nereus/clusters/{cluster}/streams/{streamId}/virtual-ledgers/{virtualLedgerId}
```

### 6.2 Object keys

```text
/nereus/clusters/{cluster}/objects/{objectId}/prepared
/nereus/clusters/{cluster}/objects/{objectId}/manifest
/nereus/clusters/{cluster}/objects/{objectId}/references
/nereus/clusters/{cluster}/objects/{objectId}/gc
```

### 6.3 StreamMetadata

```json
{
  "streamId": "s-123",
  "topic": "persistent://tenant/ns/topic-partition-0",
  "state": "ACTIVE",
  "profile": "OBJECT_WAL",
  "createdAt": 1783036800000,
  "policyVersion": 9,
  "schemaNamespaceRef": "schema://tenant/ns/topic",
  "metadataVersion": 17
}
```

### 6.4 AppendSession

```json
{
  "streamId": "s-123",
  "brokerId": "broker-7",
  "epoch": 42,
  "fencingToken": "token-42",
  "leaseVersion": 128,
  "expiresAt": 1783036830000
}
```

Append session 是性能和 fencing 机制，不是 partition durable ownership。

### 6.5 OffsetIndexEntry

```json
{
  "streamId": "s-123",
  "offsetStart": 1048576,
  "offsetEnd": 1114112,
  "generation": 0,
  "cumulativeSize": 9876543210,
  "physicalFormat": "MULTI_STREAM_WAL_OBJECT",
  "objectId": "wo-20260703-000001",
  "objectKey": "prod-a/wal/2026/07/03/wo-20260703-000001",
  "objectOffset": 8388608,
  "objectLength": 67108864,
  "recordCount": 65536,
  "entryCount": 4096,
  "entryIndexRef": "object-footer:offset=67000000,length=8192,checksum=crc32c:...",
  "projectionRef": "virtual-ledger:9007199254740993",
  "commitVersion": 17
}
```

规则：

- key 使用 `{streamId, offsetEnd}`，便于查找第一个 `offsetEnd > targetOffset`。
- value 必须包含 `offsetStart`，用于检测 overlap 和读 resolver。
- `generation=0` 表示 WAL generation；Future 4 会引入 compacted generation。
- `entryIndexRef` 可以指向 object footer、独立 index object 或小型内联 index。

## 7. Object Format Boundary

Future 1 只定义 L0 必须依赖的 object 类型：

| Object type | Owner | Visible by | Notes |
| --- | --- | --- | --- |
| `MULTI_STREAM_WAL_OBJECT` | WAL writer | Per-stream offset index entry | 一个 object 可包含多个 stream slices |
| `INDEX_OBJECT` | WAL writer / resolver | Offset index reference | 大 entry index 外置 |
| `CURSOR_SNAPSHOT_OBJECT` | Future 3 | Cursor state ref | Future 1 只保留类型边界 |
| `STREAM_COMPACTED_OBJECT` | Future 4 | Higher-generation offset index | Future 1 只保留 resolver extension point |

Object store key 不决定语义。所有可见性都来自 Oxia offset index。

## 8. Append State Transition

### 8.1 Acquire session

```text
IDLE
  -> AcquireAppendSession
  -> SESSION_ACTIVE(epoch, token)
```

Oxia 条件：

- append session missing；
- append session expired；
- requester already owns current session；
- requester obtains higher epoch through CAS。

### 8.2 Write object bytes

```text
SESSION_ACTIVE
  -> build WAL object with relative offsets
  -> upload object bytes
  -> OBJECT_DURABLE
```

Rules：

- object bytes can contain multiple stream slices；
- stream slice stores relative offset and entry index；
- object checksum must be known before offset index commit；
- uploaded object is not visible yet。

### 8.3 Commit visible range

```text
OBJECT_DURABLE
  -> CommitOffsetIndex(streamId, expectedStartOffset, object slice)
  -> OFFSET_COMMITTED(offsetStart, offsetEnd)
  -> PRODUCER_ACK
```

Oxia commit must atomically:

1. verify append epoch and fencing token；
2. verify `expectedStartOffset == committedEndOffset`；
3. allocate `[offsetStart, offsetEnd)`；
4. write offset index entry；
5. advance `committedEndOffset`；
6. write or reference virtual ledger projection；
7. publish stream notification。

The linearization point for producer ack is the successful Oxia offset index commit.

### 8.4 Partial stream slice commit

Multi-stream WAL object does not require a cross-stream transaction:

```text
object contains slices A, B, C
A commit succeeds
B commit fails due to stale epoch
C commit waits or retries
```

Only A is visible. B and C remain invisible until their own stream offset index entries commit.

## 9. Read Resolver

Input:

```text
streamId
startOffset
maxRecords / maxBytes
visibility options
projection options
```

Steps:

1. Check local resolver cache for offset index entries.
2. If cache misses, query Oxia for first `offsetEnd > startOffset`.
3. Choose highest visible generation for the target offset.
4. Verify `offsetStart <= startOffset < offsetEnd`.
5. Resolve object range using `objectOffset`, `objectLength`, and `entryIndexRef`.
6. Range-read object bytes.
7. Decode entry index.
8. Return protocol-neutral records plus projection hints.

Output:

```text
ResolveResult
  streamId
  resolvedStartOffset
  resolvedEndOffset
  objectRefs
  entryRefs
  projectionRefs
  generation
```

Resolver rules:

- It cannot use object store list for correctness.
- It must tolerate stale local cache by validating index version/generation.
- It must support future compacted generations without changing caller API.
- It must expose enough entry boundary information for Future 2 MessageId projection and Future 5 Kafka fetch.

## 10. Trim and GC Reference Model

`trim(streamId, beforeOffset)` updates:

```text
/streams/{streamId}/trim
```

It does not delete objects. GC can delete an object only when all are true:

- no active offset index entry references the object range；
- no higher-level cursor low-watermark needs the range；
- no active reader lease protects the object；
- no compaction generation fallback needs the object；
- retention policy allows deletion；
- object manifest ownership matches the stream/cluster。

Future 1 defines the reference model. Actual GC worker policy can be implemented after cursor and
compaction futures define their references.

## 11. Failure Model

| Failure | Expected behavior |
| --- | --- |
| Broker crashes before object upload | No visible data; client retries |
| Broker crashes after object upload before offset commit | Object is orphan/uncommitted; no reader sees it |
| Broker crashes after offset commit before ack | Data is visible; retry relies on producer sequence/dedup in upper layer |
| Stale broker commits with old token | Oxia rejects commit |
| Object upload succeeds but checksum mismatch | Offset index commit must reject object reference |
| Oxia commit conflict on `committedEndOffset` | Append retries from fresh metadata |
| Partial slice commit in multi-stream object | Committed slices visible; uncommitted slices invisible |
| Object read fails transiently | Resolver retries or returns retriable read error |
| Offset index cache stale | Broker refreshes from Oxia and retries resolve |

## 12. Compatibility Impact

### Pulsar

Future 1 exposes enough information for Future 2:

- offset range；
- entry count；
- entry index reference；
- virtual ledger projection reference；
- payload format；
- schema references。

It does not implement full ManagedLedger behavior.

### Kafka / KoP

Future 1 makes Kafka offset mapping straightforward:

```text
Kafka offset == stream record offset
```

It does not implement group coordinator, transaction marker handling, or Kafka leader epoch projection.

### Lakehouse

Future 1 records object and offset metadata needed by future compaction and SBT/SDT. It does not
commit lakehouse catalog metadata and does not put catalog operations on the producer ack path.

## 13. Design Invariants

1. `streamId + offset` is the only internal ordering coordinate.
2. Oxia is the offset and visibility authority.
3. Object store stores bytes, not truth.
4. Producer ack happens only after WAL durable and Oxia offset index commit.
5. Offset ranges must be dense per stream.
6. Multi-stream object visibility is per stream slice.
7. Broker local state is cache only.
8. Object list is never used for correctness.
9. Read resolver always starts from Oxia offset index or a validated cache of it.
10. L0 does not depend on Pulsar subscription or Kafka group semantics.

## 14. Future Gate

Future 1 design is ready to move into implementation planning when the following are reviewed:

- `StreamStorage` API and async error semantics；
- Oxia key schema；
- append session state transition；
- object WAL visibility rule；
- offset index entry schema；
- read resolver algorithm；
- trim and GC reference model；
- failure model；
- compatibility contract with Future 2, Future 4, and Future 5。

This gate is a design gate. It does not require benchmark, chaos, real evidence, CI, or production
profile data.
