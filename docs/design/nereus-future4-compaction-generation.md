# Nereus Future 4：Compaction + Generation Replacement

> 状态：Designed；worker/task/higher-generation publish 尚未实现
> 前置：Future 1 generation-0 contract、Phase 1.5 generic target/stable-commit split、
> Phase 3 cursor retention/snapshot-reference contract、reader reference hooks

本文定义 Nereus L3 compaction 和 generation replacement 设计。Future 4 的核心目标是
> 把 multi-stream WAL object 转换为 per-stream read-optimized object，并通过 Oxia offset
> index 的 generation overlay 条件发布并切换读路径。Compaction 不改变 `streamId + offset`。
> 对 AutoMQ-like profile，Future 4 的 worker 也是 append ack 之后的 async object
> materialization 路径。

“Compaction”在本文中包含两个不能混用的 domain：`COMMITTED` view 的 generation replacement 是无损
physical-layout compaction；Pulsar/Kafka 按 key 丢弃旧值属于独立的 `TOPIC_COMPACTED` semantic view。后者
永远不能仅凭更高 generation 成为普通 committed read 的 physical target。

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
- view-scoped topic compaction primitive；
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

Phase 1.5 implements the tagged target/adapter、generic generation-zero record compatibility and
stable-commit/materializer seam。It does not freeze this document's task/checkpoint/source-generation CAS schema and
does not implement a worker。Phase 1.5 P15-M0-M6 has passed；F4 production still requires the remaining
reference/publish entry gates。

Phase 3 F3-M0/M0R 已冻结 F4 必须消费的边界：cursor ack truth 是单 Oxia root + immutable snapshot ref；
new/recreated cursor 和 backward reset 在 cursor CAS 全窗口保持 `PROTECTION_PENDING`；logical trim 经过
保存 exact offset/attempt/composed reason 的 recoverable `TRIM_PENDING`；normal
dispatch read position 不参与 retention；`ackStateEpoch` 只围住 destructive cursor replacement，F4 不得
重置、推导或复用它。每次 writable ledger open 必须先用 fresh owner session claim retention 和全部 ACTIVE
cursor roots；topic-owned trim mutation 必须携带当前 session。只读 planner/GC worker 不 claim cursor
ownership，而是通过 versioned `CursorMetadataStore` read/scan surface 读取并在执行边界重验 root
version/session；任何 owner change 都使本轮 snapshot 失效重试。
它们不能把 Pulsar ownership/watch 当作 cursor CAS fence。F3-M1-M6 尚未实现，因此 F4 不得先行启用 generation publish 或
physical GC。

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
- 将丢失 Entry/record 的 semantic-compaction output 发布到普通 committed-read offset index；
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

enum ReadView {
    COMMITTED,
    TOPIC_COMPACTED
}

interface GenerationReadResolver {
    CompletableFuture<ResolvedRange> resolve(
            StreamId streamId,
            Offset offset,
            ReadView view);
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
/nereus/clusters/{cluster}/streams/{streamId}/views/topic-compacted/index/{offsetEnd}/{generation}
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
  "readView": "COMMITTED",
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

## 7. Read-optimized Object Format

Read-optimized object 是 per-stream object，默认物理格式为 Parquet。其 schema 由 `readView` 和 payload
mapping 共同约束；“包含可重建字段”不能替代 observable bytes 合同。

```json
{
  "objectId": "co-s-123-1048576-1114112-g18",
  "streamId": "s-123",
  "offsetStart": 1048576,
  "offsetEnd": 1114112,
  "generation": 18,
  "readView": "COMMITTED",
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

所有 view 的 object 必须保留：

- stream offset；
- publish time / event time；
- key；
- producer metadata；
- schema id；
- transaction marker / abort visibility reference；
- Pulsar entry projection metadata；
- Kafka record batch projection metadata where needed。

For `COMMITTED + PULSAR_ENTRY_V1`, every covered stream offset must additionally contain exactly one complete opaque
Pulsar ManagedLedger Entry byte sequence and its entry boundary. Reading the replacement must return byte-for-byte the
same Entry as generation 0. The object container may apply lossless compression/encryption, but the worker cannot
split、merge、rebatch、re-encode or semantically reconstruct that Entry. `Position.entryId` remains the stream offset;
batch index remains inside those exact bytes. A replacement that cannot prove exact Entry count、offset coverage and
payload checksum is not publishable to the `COMMITTED` view.

`TOPIC_COMPACTED` objects may omit superseded keyed records while retaining declared logical coverage/tombstones, but
they are eligible only for an explicitly compacted read. They use their separate view index and cannot shadow a
`COMMITTED` generation regardless of generation number.

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
  "readView": "COMMITTED",
  "objectType": "STREAM_COMPACTED_OBJECT",
  "objectId": "co-s-123-1048576-1114112-g18",
  "supersedes": ["1064960/17", "1114112/17"],
  "commitVersion": 92
}
```

The ordinary offset-index namespace contains only `COMMITTED` lossless targets. Reader selection is view-scoped：

```text
resolve(streamId, offset, requestedView):
  select only requestedView's index namespace
  find entries where offsetStart <= offset < offsetEnd
  ignore tombstoned entries
  choose highest visible generation within that view
  if chosen generation read fails checksum/object validation:
      fallback only to still-visible lower generation
      surface data-integrity error if no safe fallback exists
```

Generation counters and supersession sets are scoped to one view. A `TOPIC_COMPACTED` generation can never outrank or
supersede a `COMMITTED` target. Fallback 不是正常读路径的 correctness 依赖，只是发布新 generation 后的安全垫。长期依赖
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

Source index and object retirement is a separate two-stage protocol:

```text
publish replacement in one view
  -> tombstone superseded source index keys with expected generation/checksum
  -> keep source object while any visible index, reachable-commit recovery root,
     reader lease/cache pin, logical trim/reference domain or task protects it
  -> mark GC candidate with the complete observed reference/version set
  -> wait for pre-existing bounded reader leases to release/expire
  -> re-read and CAS-validate every root
  -> delete object idempotently
  -> delete/tombstone retired metadata keys and record audit result
```

Resolve-to-read must close the deletion race: before object IO, the reader acquires a bounded lease/pin conditioned on
the selected `(streamId, range, view, generation, target identity, checksum)` still being visible; GC snapshots these
leases, waits out older leases and revalidates after the wait. A cache reference is equivalent only if it participates
in the same authoritative pin protocol. Until this protocol and a reachable-commit recovery checkpoint/root are
implemented, higher generation may be published but source bytes and source index keys are retention-protected.

This retirement also bounds the F2 `O(committed append ranges)` offset-index count. Old incarnation projection mirrors
may be removed only after current topic authority points elsewhere and no reader/task/audit/recovery reference needs
the old stream; their absence never permits an old MessageId to address the new incarnation.

Cursor integration is two distinct checks：

- stream data eligibility starts from completed L0 trim produced through F3 `CursorRetentionRecord` pending protocol；
  PROTECTION_PENDING or TRIM_PENDING blocks a newer decision，and a one-shot cursor minimum/projection hint is never
  deletion proof；a topic-owned trim request must belong to the current writable-ledger owner session，while a
  read-only GC snapshot must include and revalidate the observed root version/session before delete；
- cursor snapshot object eligibility starts from the current generation's `CursorStateRecord.snapshotReference` and
  F4 reader/reference grace；an old/CAS-lost snapshot is not deletable from age alone。

F4 is also the first phase that may admit the F3-rejected `NereusAdminOperation.TRIM_TOPIC` and replace F3's no-op
`ManagedLedger.trimConsumedLedgersInBackground(promise)`。Every broker housekeeping、policy or admin candidate must
funnel through `CursorRetentionCoordinator.requestTrim` with the current owner session；the promise may complete after the recoverable logical trim
reaches ACTIVE/completed truth，but physical source/object deletion remains a later GC-worker boundary。No F4 call
site may invoke `StreamStorage.trim` directly。If the topic has no cursor marker yet，that coordinator first runs the
F3 capability guard and activates the projection marker；the current F3 writable open must already have created or
claimed the owner-only retention root at current L0 trim。“currently no cursor” is not permission to race a first
cursor create with direct trim，and an absent/mismatched root forces a fresh writable open rather than direct trim。

F4 may route a lagging cursor to a lossless higher `COMMITTED` generation because offsets/Entry bytes are unchanged；
cursor lag protects the logical range from trim, not a particular old physical generation。

## 10. Topic Compaction Primitive

Pulsar topic compaction 和 Kafka topic compaction reuse the immutable-object/task/CAS machinery but publish only to
the separate `TOPIC_COMPACTED` view：

1. Planner 选择 compactable offset range。
2. Worker 根据 key 保留最高 offset record。
3. Worker 写 compacted object，并保留 offset coverage。
4. Committer publishes a higher generation under `views/topic-compacted/index`。
5. Only a reader that explicitly requests `TOPIC_COMPACTED` selects that generation and applies topic-compaction
   visibility；ordinary readers continue to resolve the lossless `COMMITTED` view。

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
| Lossy or byte-rewritten Pulsar output proposed for `COMMITTED` | Publish invariant failure; no index mutation |
| Semantic-view generation visible to ordinary resolver | Namespace/view validation failure; never fall back across views |
| Old generation deleted too early | GC invariant violation; Future 4 must prevent this by reference rules |
| Cursor lags behind compacted range | Cursor reads through highest valid generation; old object kept only if needed |
| Catalog references old generation | Future 6 catalog reference protects object from GC |

## 12. Compatibility Impact

### Pulsar

- `MessageId` / `Position` projection must remain stable across generation replacement.
- `PULSAR_ENTRY_V1` ordinary reads return the exact original Entry bytes and one offset remains one Entry across every
  physical generation.
- ManagedCursor backlog and retention use offsets and cumulative size, not object identity.
- Force reset below ordinary L0 trim remains unavailable until F4 defines an explicit compacted-view generation and
  reference contract；it does not change F3's durable mark-delete coordinate.
- Topic compaction uses only the separate compacted-read view and keeps Pulsar observable semantics.

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
- exact opaque-entry preservation for `COMMITTED + PULSAR_ENTRY_V1` and equivalent mapping-specific lossless rules；
- explicit read-view enum、separate index namespaces and prohibition on cross-view supersession/fallback；
- fallback and checksum behavior；
- topic compaction primitive；
- old generation reference and GC protection；
- reader resolve/pin/read/release protocol、reachable-commit recovery root and source-index retirement；
- AutoMQ-like materialization lag and primary WAL retention protection；
- interactions with Future 3 completed logical trim/protection root/current snapshot references and Future 6 catalog
  snapshots。

Before F4 production starts, Phase 3 M1-M6 must provide executable owner-session claim/fencing、cursor-generation、
snapshot-reference、protected create/backward-reset and pending-trim gates；the design-only M0R contract is necessary
but not final implementation evidence。

This is a design gate. It does not require benchmark, chaos, compatibility certification, CI, or
real evidence.
