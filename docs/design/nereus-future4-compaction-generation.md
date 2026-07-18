# Nereus Future 4：Compaction + Generation Replacement

> 状态：Implementation in progress；F4-M0 complete，F4-M1–M3 implemented/final-gated；F4-M3 real Parquet
> read/write、deterministic planner/task/recovery、exact-source worker、protection/checkpoint/service、Pulsar Entry/NCP1
> exact-byte round trip、topic-compaction SPI/registry、terminal workflow-metadata retirement、COMMITTED-source
> bootstrap、tagged-v1/sorted-spill topic engine/worker/publication passed deterministic and real Oxia/LocalStack gates；
> F4 milestones 4–6 pending
> 前置：Future 1 generation-0 contract、Phase 1.5 generic target/stable-commit split、
> Phase 3 cursor retention/snapshot-reference contract、reader reference hooks

Phase 4 的代码级实现合同以
[`docs/phase-4-compaction-generation/`](../phase-4-compaction-generation/README.md) 及其 `01`–`07`
编号文档为准。本文是 north-star 摘要；两者冲突时，已实现代码/测试优先，其次是代码级合同。

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
- 64-shard durable stream-work registration（discovery hint only）；
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
stream-head reachable commit repair。F4-M0 已冻结 higher-generation conditional publish、overlap、
reader lease、recovery checkpoint 和 physical-GC schema；compaction 不能改写
`StreamHeadRecord.committedEndOffset` 或 append 线性化语义。
Broker unload/restart 后的 work discovery 由代码级合同中的
`MaterializationStreamRegistrationRecord` 完成；scanner 必须重新读取 projection、head、index 和 task，
registration/watch 不能成为 correctness owner。

Phase 1.5 implements the tagged target/adapter、generic generation-zero record compatibility and
stable-commit/materializer seam。It does not implement a worker。Phase 1.5 P15-M0-M6 has passed；the F4-M0
design set now freezes the task/checkpoint/source-generation CAS schema that Phase 1.5 deliberately left open。

Phase 3 F3-M0/M0R 已冻结 F4 必须消费的边界：cursor ack truth 是单 Oxia root + immutable snapshot ref；
new/recreated cursor 和 backward reset 在 cursor CAS 全窗口保持 `PROTECTION_PENDING`；logical trim 经过
保存 exact offset/attempt/composed reason 的 recoverable `TRIM_PENDING`；normal
dispatch read position 不参与 retention；`ackStateEpoch` 只围住 destructive cursor replacement，F4 不得
重置、推导或复用它。每次 writable ledger open 必须先用 fresh owner session claim retention 和全部 ACTIVE
cursor roots；topic-owned trim mutation 必须携带当前 session。只读 planner/GC worker 不 claim cursor
ownership，而是通过 versioned `CursorMetadataStore` read/scan surface 读取并在执行边界重验 root
version/session；任何 owner change 都使本轮 snapshot 失效重试。
它们不能把 Pulsar ownership/watch 当作 cursor CAS fence。F3-M1-M6 已完成 implementation/final gates，
包括 real two-broker recovery/retention、10,000-root scale、stable MessageId/incarnation、read-only
`CursorSnapshotInventory` 与 pending-lifecycle deletion veto。这些 Phase 3 前置已满足。F4 可以从 M1
开始实现，但在自己的 generation/reference-revalidation/physical-GC gates 完成前，
不得对 production topic 激活 higher-generation publish 或 physical deletion。

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
interface MaterializationPlanner {
    CompletableFuture<List<MaterializationTask>> plan(
            StreamId streamId,
            OffsetRange range,
            MaterializationPolicy policy);
}

interface MaterializationWorker {
    CompletableFuture<MaterializationOutput> materialize(MaterializationTask task);
}

interface GenerationCommitter {
    CompletableFuture<GenerationCommitResult> publish(
            MaterializationTask task,
            MaterializationOutput output);
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

`MaterializationTask` 必须包含 source index entries 的 identity 和 checksum，避免 worker 在旧
输入变化后错误发布 replacement。

## 6. Oxia Metadata Schema

```text
/nereus/clusters/{cluster}/streams/{streamId}/materialization/v1/tasks/{taskId}
/nereus/clusters/{cluster}/streams/{streamId}/materialization/v1/checkpoints/{policyId}/{policyVersion019}
/nereus/clusters/{cluster}/streams/{streamId}/materialization/v1/generation-sequences/{viewId02}
/nereus/clusters/{cluster}/materialization/v1/stream-registry/{shard02}/{streamId}
/nereus/clusters/{cluster}/streams/{streamId}/recovery/v1/root
/nereus/clusters/{cluster}/streams/{streamId}/offset-index/{offsetEnd}/{generation}
/nereus/clusters/{cluster}/streams/{streamId}/views/v1/topic-compacted/offset-index/{offsetEnd}/{generation}
/nereus/clusters/{cluster}/physical-objects/v1/{shard03}/roots/{objectKeyHash}
/nereus/clusters/{cluster}/physical-objects/v1/{shard03}/objects/{objectKeyHash}/readers/{processRunId}
/nereus/clusters/{cluster}/physical-objects/v1/{shard03}/objects/{objectKeyHash}/protections/{typeId02}/{referenceId}
```

上述是人类可读形式；实现必须经 `F4Keyspace`/`KeyComponentCodec` 生成。完整键空间、
partition key 和扫描边界见
[`03-oxia-metadata-and-publication.md`](../phase-4-compaction-generation/03-oxia-metadata-and-publication.md)。
Registry 固定为 64 个 SHA-256 shard，只负责让无 topic ownership 的 runtime 找到 stream；
`lastHintCommitVersion` 不能跳过 authoritative head/task/index scan。
Physical roots 固定为 256 个 object-hash-first-byte shard；root scan 是 MARKED/DELETING restart recovery 的
权威发现路径，对象存储 listing 只补充发现尚未建立 root 的 orphan bytes。

### 6.1 Materialization task

```json
{
  "taskId": "mt1-<canonical-source-policy-sha256>",
  "taskSequence": 92,
  "streamId": "s-123",
  "offsetStart": 1048576,
  "offsetEnd": 1114112,
  "readViewId": 1,
  "taskKindId": 1,
  "sources": [
    {
      "offsetStart": 1048576,
      "offsetEnd": 1064960,
      "generation": 17,
      "indexKey": "<encoded-index-key>",
      "indexMetadataVersion": 41,
      "targetIdentitySha256": "<sha256>"
    }
  ],
  "sourceSetSha256": "<sha256>",
  "policyId": "committed-default-v1",
  "policyVersion": 1,
  "policySha256": "<sha256>",
  "lifecycle": "PLANNED",
  "createdAtMillis": 1783036800000,
  "metadataVersion": 0
}
```

Task state：

```text
PLANNED -> CLAIMED -> OUTPUT_READY -> PUBLISHING -> PUBLISHED
PLANNED/CLAIMED/OUTPUT_READY/PUBLISHING -> RETRY_WAIT -> CLAIMED
PLANNED/RETRY_WAIT -> CANCELLED
PLANNED/CLAIMED/OUTPUT_READY/PUBLISHING/RETRY_WAIT -> TERMINAL_FAILED
```

Task state 只是 compaction workflow state，不决定 logical append visibility。Offset index
generation 决定已提交 range 的 physical read target；stream head + reachable commit log 仍决定该
range 是否逻辑提交。

## 7. Read-optimized Object Format

Read-optimized object 是 per-stream object，默认物理格式为 Parquet。其 schema 由 `readView` 和 payload
mapping 共同约束；“包含可重建字段”不能替代 observable bytes 合同。

```json
{
  "objectId": "co1-<sha256-of-canonical-object-key>",
  "objectKey": "<cluster>/compacted/v1/committed/.../<contentHash>-<outputAttemptId>.parquet",
  "outputAttemptId": "<worker-claim-id>",
  "streamId": "s-123",
  "offsetStart": 1048576,
  "offsetEnd": 1114112,
  "readView": "COMMITTED",
  "recordCount": 65536,
  "entryCount": 4096,
  "physicalFormat": "NEREUS_COMPACTED_PARQUET_V1",
  "logicalFormat": "PULSAR_ENTRY_V1",
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
  "sourceSetHash": "sha256:...",
  "policyHash": "sha256:...",
  "contentSha256": "sha256:...",
  "checksum": "crc32c:..."
}
```

Output object 由 content SHA 与 durable output-attempt id 定址，且 generation-neutral。Generation 只属于后续的 index
publication；同一个已验证 output 可在丢失/中止发布后以新分配的 generation 重用，
不得因 generation 改变 object bytes 或 identity。精确 `NCP1`、`NTC1`、`NRC1`
格式见
[`02-domain-api-and-object-format.md`](../phase-4-compaction-generation/02-domain-api-and-object-format.md)。

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
  "objectId": "co1-<sha256-of-canonical-object-key>",
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

### 9.2 Generation-zero append publication

F4 physical deletion requires the current combined stable append call to become a two-stage protocol：

```text
upload + manifest/HEAD
  -> prepare/reload exact generic commit intent; head unchanged
  -> create/revalidate PhysicalObjectRoot
  -> create commit-intent-owned REACHABLE_APPEND protection
  -> stream-head CAS/replay proof
  -> WAL_DURABLE may complete
  -> materialize exact generation-zero index
  -> create index-owned VISIBLE_GENERATION protection
  -> WAL_DURABLE_AND_INDEX_COMMITTED may complete
```

The first protection remains until an NRC1 recovery root replaces live commit replay；the second remains until the
exact generation-zero index retires. An abandoned pre-head intent is removed only after orphan grace plus unchanged
head/recovery/index/domain proof. Backfill covers pre-F4 objects, but cannot replace this new-write hook.

### 9.3 Worker output

```text
PLANNED
  -> CLAIMED
  -> read source entries
  -> write compacted object
  -> verify checksum and row group index
  -> OUTPUT_READY
```

Worker 输出 object 后，它还不是 active read target。必须等到 generation replacement publish。

### 9.4 Higher-generation publish

```text
OUTPUT_READY
  -> CAS task to PUBLISHING(stable publicationId, no generation)
  -> allocate a unique generation for (streamId, readView)
  -> CAS the allocated generation into that exact task
  -> create task-owned visible-generation object protection
  -> put final index key as PREPARED
  -> re-read task, stream head/recovery root, sources and physical-object root
  -> CAS the same final index key PREPARED -> COMMITTED
  -> CAS-transfer protection owner from task to exact committed index
  -> mark task PUBLISHED and advance advisory checkpoint
```

发布线性化点是最终 generation-index 键的同键、同版本
`PREPARED -> COMMITTED` CAS。`PREPARED`、task state、object upload 和 advisory checkpoint
都不可见且不是 correctness owner。该 CAS 只是 physical-target switch，不是新的
logical append commit。

### 9.5 GC readiness

```text
PUBLISHED
  -> reference scan confirms old generation no longer protected
  -> GC_READY
```

GC readiness 不等于立即删除 object。实际删除仍由 GC worker 按引用模型执行。

Source index and object retirement is a separate two-stage protocol:

```text
publish replacement in one view
  -> publish/verify NRC1 recovery root before deleting any replaced live-commit evidence
  -> transition superseded generation index COMMITTED -> DRAINING
  -> keep source object while any visible/admitted index, reachable-commit recovery root,
     reader lease/cache pin, logical trim/reference domain or task protects it
  -> mark the PhysicalObjectRoot with the complete observed reference/version set
  -> wait for pre-existing bounded reader leases to release/expire
  -> re-read and CAS-validate every authoritative root/domain
  -> CAS object root MARKED -> DELETING; delete object idempotently
  -> CAS object root -> DELETED and generation index DRAINING -> RETIRED
  -> conditionally delete replaceable commit/index metadata only after checkpoint proof
  -> after tombstoneAuditGrace, two exact HEAD-absence windows and unchanged owner/domain proofs,
     conditionally delete Phase 1 object references, manifest and finally the DELETED root
```

Legacy generation-zero indexes have no F4 lifecycle field；they are conditionally deleted only after the same
recovery-root、trim and reference proof. A task/checkpoint/registration scan never authorizes retirement by itself.
Every first/retried provider PUT is guarded by the exact durable owner and physical-root state；a stale attempt sends
no bytes, and a later attempt uses a fresh key. Thus DELETED-root audit retirement bounds metadata without making key
reuse or late resurrection legal.

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
4. Committer publishes a higher generation under `views/v1/topic-compacted/offset-index`。
5. Only a reader that explicitly requests `TOPIC_COMPACTED` selects that generation and applies topic-compaction
   visibility；ordinary readers continue to resolve the lossless `COMMITTED` view。

被 compact 掉的 record 不再返回给 compacted read，但 offset range 仍然连续覆盖，不能制造
offset gap。

## 11. Failure Model

| Failure | Expected behavior |
| --- | --- |
| Topic unload/process loss before task create | 64-shard registration scan reloads projection/head/index and recreates deterministic work |
| Registration stale/missing/mismatched | Hint cannot activate a stream；mutation/async admission fails closed or open repairs exact identity |
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
- Force reset below ordinary L0 trim remains unavailable in Phase 4 even though F4-M0 now defines an explicit
  compacted-view/reference contract；admission and user-visible semantics remain a later Pulsar track and do not
  change F3's durable mark-delete coordinate.
- Topic compaction uses only the separate compacted-read view and keeps Pulsar observable semantics.

### KoP / Kafka

- Kafka offset remains stream offset.
- Fetch from compacted object may return finalized record batch offsets.
- Kafka topic compaction uses the same offset coverage rule.

### Lakehouse

- Future 6 SBT can point table snapshots at compacted objects.
- Catalog snapshots must include index generation so repair can trace back to Oxia truth.

## 13. F4-M0 Gate Outcome

F4-M0 已完成以下代码级评审并冻结合同：

- compaction task schema；
- 64-shard registered-stream discovery、resolvable projection ref and registration-before-marker recovery；
- generation replacement CAS conditions；
- two-stage generation-zero intent/protection/head ordering and protected sync/async acknowledgement boundaries；
- highest-generation read resolver；
- compacted object required fields；
- exact opaque-entry preservation for `COMMITTED + PULSAR_ENTRY_V1` and equivalent mapping-specific lossless rules；
- explicit read-view enum、separate index namespaces and prohibition on cross-view supersession/fallback；
- fallback and checksum behavior；
- topic compaction primitive；
- old generation reference and GC protection；
- reader resolve/lease/revalidate/read/release protocol、reachable-commit recovery root and source-index retirement；
- AutoMQ-like materialization lag and primary WAL retention protection；
- interactions with Future 3 completed logical trim/protection root/current snapshot references and Future 6 catalog
  snapshots。

Phase 3 M1-M6 所需的 owner-session claim/fencing、cursor-generation、snapshot-reference、protected
create/backward-reset 和 pending-trim gates 已实现并 final-gated。Phase 4
[`F4-M1–M3`](../phase-4-compaction-generation/07-implementation-plan-and-gates.md) 的 API/metadata/object IO、
physical reference values、durable reader pin/protection、authoritative generation resolve/read 和 restart-safe
publication 已落地，并通过 ordinary/Docker-backed final gates；F4-M3 当前已落地 real compacted Parquet
writer/strict-reader/full verifier、NTC1 storage facade、core adapter，以及 deterministic policy/planner、durable
task policy snapshot、claim/publication recovery 和 64-shard registered-stream scanner。其后新增
stream-scoped exact-source reader、无损 single-source row stream，以及包含
claim heartbeat、source/output protection、guarded upload、strict output verification 和 typed durable failure
transition 的 worker checkpoint。随后又实现同 logical task owner 的单调 acquire-or-transfer、
source/output protection 恢复重建、publication 前后 owner 收敛和重复 expired-claim CAS
恢复。随后又实现单调 advisory checkpoint reconcile、bounded dispatcher 和可 deadline-close 的 service
lifecycle。随后完成 Pulsar opaque Entry 经 NCP1 的 byte-for-byte 与 middle-batch MessageId 往返。terminal
workflow-metadata retirement 已用 exact task/index/checkpoint/root/protection proof 与 conditional delete 落地。
最新 checkpoint 固定 topic task 从 lossless `COMMITTED` sources bootstrap、只向 `TOPIC_COMPACTED` view
发布，并实现 tagged-v1 keyed/unkeyed namespace、共享 staging budget 的 checksum-verified sorted spills、
survivor bitmap、两遍 exact-source replay 及 NTC1 worker/isolated publication tests。`phase4M3Check` 与真实
Oxia/LocalStack-backed `phase4M3FinalCheck --rerun-tasks` 已于 2026-07-15 通过，覆盖双 worker claim 收敛、
重启/response-loss、完整 Parquet bytes 与全 64 分片 pagination/watch-loss。F4-M4 的第一个 checkpoint 又已
实现 NRC1 spill-backed one-at-a-time writer、strict header/footer/directory/range reader、attempt/key identity、
body/content 双 SHA，以及 authoritative generation-index/generic-commit metadata verifier；golden、self-consistent
corruption、cancellation 和跨 3 个 sparse blocks 的 focused tests 已加入。

F4-M4 随后的 checkpoints B–W 已继续实现 protected generation-zero append、recovery-root publication、
anchor-aware replay/index repair、bounded GC plan/root/journal fencing、root-authenticated destructive recovery
skeleton、typed generation-zero source retirement，以及 completed-trim/COMMITTED/TOPIC_COMPACTED source
eligibility、grace-fenced higher-generation pre-drain/reproof和 durable generation-activation metadata authority
foundation、future sentinel、五个 activation-gated ownerless-global domains，以及持久化 dual-absence、
references-before-manifest、root-last 的 DELETED-root audit retirement，并把 cursor snapshot 新写入/读取接入
guarded PUT、pending/permanent protection、response-loss repair 和 durable reader lease。Checkpoint W 又实现
strict NPR1 projection identity、protocol-neutral projection authority capture，以及在 completed registration proof
前提下遍历全部 64 registry shards 的 physical-root/cursor-root live-reference backfill；它使用 exact HEAD、
commit/index/cursor owner protection handshake、最终 authority revalidation 和双 activation-proof CAS 收口。
Checkpoint AJ further implements strict cursor-snapshot key inversion、complete bounded retention/root/object/
protection inventory、canonical candidate evidence and post-drain final revalidation in the central GC fence.
Checkpoint AK normalizes that evidence so an exact MARKED root can reconstruct the same plan after process restart,
adds exact drift rollback, and composes the cursor path through mark/drain/revalidate/DELETING/source retirement with
all six reference domains、the durable journal and owned provider/runtime lifecycle. Broker cold-topic registration
proof is now implemented by the M5 checkpoints below；periodic physical-root/registration/inventory scheduling、
registration retirement、broker GC configuration mapping、delete activation 和 final M4 gate 仍待完成；production
deletion 保持关闭且兼容默认值仍为 `enabled=false, dryRun=true`。

Checkpoint AL now implements the current-writer inventory slice：strict inverses cover Object-WAL、both compacted
views、NRC1 and NCS1, and an owned complete-pass scanner registers only grace-old exact-HEAD missing-root objects with
a second full orphan grace. Listing remains discovery-only；the scanner has no MARK/delete operation and is not
scheduled. Therefore the remaining scheduling item includes the metadata-root、registration and inventory passes,
while registration retirement and activation remain separate unfinished correctness work.

M5 checkpoint X 已进一步实现共享 canonical projection-ref encoder、exact durable registration
create/refresh/final revalidation、topic create/open/recreate return barrier，以及 production shared generation-store
ownership。它关闭 concurrent new-topic registration frontier，但不广告 generation capability、不设置 marker、
不遍历 cold topics，也不写 cluster registration backfill proof。

M5 checkpoint Y 已在 locked local Pulsar fork 发布 reserved generation capability，并以两次完整 persistent-broker
snapshot、broker registry key/advertised broker id/start timestamp/三项协议版本的 canonical SHA-256 identity
生成 bounded readiness epoch；broker registry notification 会使 cached readiness 失效。它仍不遍历 cold topics、
不写 backfill proof、不设置 topic marker，也不构成 production activation guard。

M5 checkpoint Z 已实现 exact unloaded projection candidate，以及按 tenant/namespace/topic canonical
排序、one-namespace-at-a-time、bounded concurrency/deadline 的 cold-topic registration traversal/report。
Registration 后会重读 binding，run 末尾要求完整 broker readiness 不变；coverage digest 覆盖完整 traversal，
只保留前 100 个 redacted failures。它仍不写 durable backfill proof、不设置 topic marker，也不构成
production activation guard。

M5 checkpoint AA 已把 Pulsar readiness 转换为 product-neutral exact identity，并只在 report
`failureCount == 0`、traversal 前后完整 readiness 相同且 proof CAS 前后 readiness 仍有效时，持久化
`streamRegistrationBackfill`。Same-epoch completed coverage 不可变；newer epoch 会把 physical-root/cursor
proof 重置为该 epoch 的 incomplete，并清空旧 object-store capability；deletion 已启用时拒绝单独推进。
CAS condition failure 与 lost response 通过 durable reload 收敛。Broker traversal 只提交 completion fact，
不直接持有 activation store。它仍不设置 topic marker，也不构成 production activation guard。

M5 checkpoint AB 已实现 product-owned `ManagedLedgerGenerationProtocolActivationGuard`。它要求 ACTIVE cluster
record、current readiness epoch、complete registration proof 和 exact six-domain set，重读 strict NPR1
projection、L0 与 registration 后才可创建/验证 monotonic marker，并把 topic/cluster metadata versions、
readiness/domain digest/capability bits 冻结进 short-lived proof；mutation CAS 前再次重证。Pulsar
`nereusGenerationProtocolEnabled` 默认 false，只控制首次 marker。Physical delete 还要求同 epoch delete
proofs/object capability 和 exact `projection-generation-v1` snapshot。

M5 checkpoint AC 已实现 product-owned `ManagedLedgerGenerationProtocolActivationCoordinator`。它只在显式
开关开启、current readiness 与 durable epoch 相同、same-epoch registration proof 已完成且 exact six-domain
set 匹配时执行 publication-only `PREPARED -> ACTIVE` CAS；condition conflict bounded retry，lost response 只从
durable ACTIVE 收敛，CAS 后还会重证 cached readiness 与 durable authority。Broker 的 zero-failure backfill
promise 在 proof 完成后等待 activation；失败 report 或默认关闭状态不调用 coordinator。具体 mutation call
sites 和 topic marker 尚未接入。

M5 checkpoints AD–AF 已继续实现 protected Object-WAL `WAL_DURABLE` acknowledgement、generation-zero
restart/read repair、per-stream pre-I/O activation/lag admission，以及把 resolver/read-repair/materialization
lifecycle 与 exact Pulsar profile/config mapping 作为一个 production unit 装配。Sync 仍为默认，async 仍要求
durable generation activation proof；BookKeeper primary path 未实现。

M5 checkpoint AG 已实现 product-neutral exact retention policy/config/evidence values、source-index-verified
stable candidate planning 和 ownership/activation/final-authority gated F3 logical-trim delegation。它不会直接
调用 L0 trim，也不会等待 physical GC；Pulsar retention policy/admin mapping、shared bounded plan lane 和
managed-ledger production installation 尚未实现。

M5 checkpoint AH 已实现 shared bounded/coalescing retention lane、whole-operation timeout/close、per-ledger
production service/facade routing，以及 Pulsar typed retention config mapping。M5 checkpoint AI 又实现 exact
immutable effective retention/backlog snapshot、stable generation readiness、registration-backed marker admission、
post-activation policy reload 和 loaded/unloaded/partition-child `TRIM_TOPIC` route；physical deletion 仍关闭。

F4-M0 只是 design gate；F4-M1–M3 final gates、M4 through checkpoint AL 和 M5 through checkpoint AI 也不声称 production physical GC、
async/Pulsar rollout、benchmark、chaos 或 Phase 4 compatibility certification。F4-M4–M6 的确切文件、测试、
故障点和 release gates 见代码级实施计划。
