# Nereus Future 4：Compaction + Generation Replacement

> 状态：Designed；worker/task/higher-generation publish 尚未实现
> 前置：Future 1 generation-0 contract、Phase 1.5 P15-M5 generic target/stable-commit split、cursor/reader reference hooks

本文定义 Nereus L3 compaction 和 generation replacement 设计。Future 4 的核心目标是
> 把 multi-stream WAL object 转换为 per-stream read-optimized object，并通过 Oxia offset
> index 的 generation overlay 条件发布并切换读路径。Compaction 不改变 `streamId + offset`。
> 对 AutoMQ-like profile，Future 4 的 worker 也是 append ack 之后的 async object
> materialization 路径。

## 1. Motivation

Future 1 的 object WAL 面向写入效率，一个物理 object 可以包含多个 stream slices。这个形态
适合 append 和低成本写入，但不适合长时间 catch-up、historical read、topic compaction 和
lakehouse 查询。

Future 4 引入 compaction service：

- 将 row-based WAL object 转换为 per-stream compacted object；
- 将多个小 offset index entries 合并为更大的 read-optimized range；
- 对同一个 offset range 发布更高 generation；
- 让 reader 在不中断、不改变 offset 的情况下切换到新 object；
- 为 AutoMQ-like profile 消费 primary WAL ranges 并后台发布 read-optimized object；
- 为 Future 6 的 SBT/SDT 和 Future 8 的 topic compaction 提供底层 primitive。

## 2. Scope

Future 4 覆盖：

- compaction planner；
- compaction task metadata；
- WAL object reader；
- compacted object writer；
- generation overlay；
- highest-generation read resolver；
- old generation fallback；
- topic compaction primitive；
- compaction checkpoint；
- materialization lag checkpoint；
- GC protection and object reference rules。

## 3. Non-scope

Future 4 不解决：

- 外部 lakehouse catalog 完整提交；
- SDT delivery；
- 每个 table format 的 writer 细节；
- Kafka compaction protocol 的完整兼容；
- Pulsar topic compaction 的全部 broker 行为；
- benchmark、chaos、real workload profile。

这些能力依赖 Future 4 的 generation replacement，但分别在 Future 5、Future 6、Future 8
和后续验证阶段处理。

当前实现边界：Phase 1 只有 generation 0 的 Object WAL records，且其 offset index 可从
stream-head reachable commit repair。F4 实现前必须冻结 higher-generation conditional
publish/overlap schema；compaction 不能改写 `StreamHeadRecord.committedEndOffset` 或 commit chain。

Phase 1.5 code-level design freezes only the tagged target/adapter、generic generation-zero record compatibility and
stable-commit/materializer seam。It does not freeze this document's task/checkpoint/source-generation CAS schema and
does not implement a worker。F4 production starts only after P15-M5 and the remaining reference/publish entry gates。

## 4. Layer Boundary

Future 4 位于 L3：

```text
Oxia offset index
  -> compaction planner
  -> compaction task
  -> WAL/old compacted object reader
  -> compacted object writer
  -> generation replacement commit
  -> read resolver highest-generation selection
```

Future 4 可以做：

- 读取已提交 offset index；
- 为 offset range 生成 compaction task；
- 读取 WAL object 中目标 stream slice；
- 写 per-stream compacted object；
- 发布更高 generation 的 offset index entry；
- 标记旧 generation 被 supersede；
- 为 GC 输出引用保护信息。

Future 4 不能做：

- 改变 stream offset；
- 改变 `committedEndOffset`；
- 改变 producer ack 已经返回的 protocol projection；
- 让 object list 决定可见性；
- 删除仍被 cursor、reader、catalog 或 task 引用的 object；
- 让 lakehouse catalog commit 进入 producer ack path。

## 5. Internal API

```java
interface CompactionPlanner {
    CompletableFuture<List<CompactionTask>> plan(
            StreamId streamId,
            OffsetRange range,
            CompactionPolicy policy);
}

interface CompactionWorker {
    CompletableFuture<CompactionOutput> compact(CompactionTask task);
}

interface GenerationCommitter {
    CompletableFuture<GenerationCommitResult> publish(
            CompactionTask task,
            CompactionOutput output);
}

interface GenerationReadResolver {
    CompletableFuture<ResolvedRange> resolve(StreamId streamId, Offset offset);
}
```

`CompactionTask` 必须包含 source index entries 的 identity 和 checksum，避免 worker 在旧
输入变化后错误发布 replacement。

## 6. Oxia Metadata Schema

```text
/nereus/clusters/{cluster}/streams/{streamId}/compaction/policies/default
/nereus/clusters/{cluster}/streams/{streamId}/compaction/tasks/{taskId}
/nereus/clusters/{cluster}/streams/{streamId}/compaction/checkpoints/{plannerId}
/nereus/clusters/{cluster}/streams/{streamId}/offset-index/{offsetEnd}/{generation}
/nereus/clusters/{cluster}/objects/{objectId}/manifest
/nereus/clusters/{cluster}/objects/{objectId}/references/{refType}/{refId}
```

### 6.1 CompactionTask

```json
{
  "taskId": "cmp-s-123-1048576-1114112",
  "streamId": "s-123",
  "offsetStart": 1048576,
  "offsetEnd": 1114112,
  "sourceEntries": [
    {
      "offsetEnd": 1064960,
      "generation": 17,
      "objectId": "wo-1",
      "checksum": "crc32c:..."
    }
  ],
  "targetGeneration": 18,
  "targetFormat": "PARQUET",
  "state": "PLANNED",
  "plannerEpoch": 7,
  "createdAt": 1783036800000
}
```

Task state：

```text
PLANNED -> RUNNING -> OUTPUT_UPLOADED -> PUBLISHED -> GC_READY
PLANNED -> CANCELLED
RUNNING -> FAILED -> PLANNED
OUTPUT_UPLOADED -> FAILED -> PLANNED
```

Task state 只是 compaction workflow state，不决定 logical append visibility。Offset index
generation 决定已提交 range 的 physical read target；stream head + reachable commit log 仍决定该
range 是否逻辑提交。

## 7. Compacted Object Format

Compacted object 是 per-stream object，默认物理格式为 Parquet，逻辑上保留 Nereus record
fields。

```json
{
  "objectId": "co-s-123-1048576-1114112-g18",
  "streamId": "s-123",
  "offsetStart": 1048576,
  "offsetEnd": 1114112,
  "generation": 18,
  "recordCount": 65536,
  "entryCount": 4096,
  "physicalFormat": "PARQUET",
  "logicalFormat": "NEREUS_STREAM_RECORD",
  "sourceIndexEntries": ["1064960/17", "1114112/17"],
  "rowGroups": [
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

Compacted object 必须保留：

- stream offset；
- publish time / event time；
- key；
- producer metadata；
- schema id；
- transaction marker / abort visibility reference；
- Pulsar entry projection metadata；
- Kafka record batch projection metadata where needed。

## 8. Generation Replacement

Offset index key：

```text
/streams/{streamId}/offset-index/{offsetEnd}/{generation}
```

Generation replacement 发布一个更高 generation 的 index entry：

```json
{
  "streamId": "s-123",
  "offsetStart": 1048576,
  "offsetEnd": 1114112,
  "generation": 18,
  "objectType": "STREAM_COMPACTED_OBJECT",
  "objectId": "co-s-123-1048576-1114112-g18",
  "supersedes": ["1064960/17", "1114112/17"],
  "commitVersion": 92
}
```

Reader selection rule：

```text
resolve(streamId, offset):
  find entries where offsetStart <= offset < offsetEnd
  ignore tombstoned entries
  choose highest visible generation
  if chosen generation read fails checksum/object validation:
      fallback only to still-visible lower generation
      surface data-integrity error if no safe fallback exists
```

Fallback 不是正常读路径的 correctness 依赖，只是发布新 generation 后的安全垫。长期依赖
fallback 说明 compacted object 或 GC policy 有问题。

## 9. State Transitions

### 9.1 Planning

```text
OFFSET_INDEX_READY
  -> planner selects contiguous range
  -> writes CompactionTask(PLANNED)
```

Planner 必须保证 source entries 覆盖连续 offset range。可以跨多个 WAL objects，但不能跨
stream 改写 logical ordering。

### 9.2 Worker output

```text
PLANNED
  -> RUNNING
  -> read source entries
  -> write compacted object
  -> verify checksum and row group index
  -> OUTPUT_UPLOADED
```

Worker 输出 object 后，它还不是 active read target。必须等到 generation replacement publish。

### 9.3 Publish

```text
OUTPUT_UPLOADED
  -> CAS source entries still active
  -> put higher generation offset index entry
  -> mark task PUBLISHED
```

发布线性化点是更高 generation offset index entry 可见；这是 physical-target switch，不是新的
logical append commit。

### 9.4 GC readiness

```text
PUBLISHED
  -> reference scan confirms old generation no longer protected
  -> GC_READY
```

GC readiness 不等于立即删除 object。实际删除仍由 GC worker 按引用模型执行。

## 10. Topic Compaction Primitive

Pulsar topic compaction 和 Kafka topic compaction 可以复用 generation replacement：

1. Planner 选择 compactable offset range。
2. Worker 根据 key 保留最高 offset record。
3. Worker 写 compacted object，并保留 offset coverage。
4. Committer 发布更高 generation。
5. Reader 读取最高 generation，并按 topic compaction visibility 过滤。

被 compact 掉的 record 不再返回给 compacted read，但 offset range 仍然连续覆盖，不能制造
offset gap。

## 11. Failure Model

| Failure | Expected behavior |
| --- | --- |
| Planner crash after task write | Other planner resumes from Oxia task state |
| Worker crash before object upload | Task retries |
| Worker crash after object upload before publish | Object is orphan until task retry or GC |
| Publish CAS conflict | Task refreshes source entries; old output may become orphan |
| New generation object checksum failure | Reader falls back only if lower generation still visible |
| Old generation deleted too early | GC invariant violation; Future 4 must prevent this by reference rules |
| Cursor lags behind compacted range | Cursor reads through highest valid generation; old object kept only if needed |
| Catalog references old generation | Future 6 catalog reference protects object from GC |

## 12. Compatibility Impact

### Pulsar

- `MessageId` / `Position` projection must remain stable across generation replacement.
- ManagedCursor backlog and retention use offsets and cumulative size, not object identity.
- Topic compaction can use the same generation primitive but keeps Pulsar observable semantics.

### KoP / Kafka

- Kafka offset remains stream offset.
- Fetch from compacted object may return finalized record batch offsets.
- Kafka topic compaction uses the same offset coverage rule.

### Lakehouse

- Future 6 SBT can point table snapshots at compacted objects.
- Catalog snapshots must include index generation so repair can trace back to Oxia truth.

## 13. Future Gate

Future 4 may enter implementation planning only after the following are reviewed:

- compaction task schema；
- generation replacement CAS conditions；
- highest-generation read resolver；
- compacted object required fields；
- fallback and checksum behavior；
- topic compaction primitive；
- old generation reference and GC protection；
- AutoMQ-like materialization lag and primary WAL retention protection；
- interactions with Future 3 cursor low-watermark and Future 6 catalog snapshots。

This is a design gate. It does not require benchmark, chaos, compatibility certification, CI, or
real evidence.
