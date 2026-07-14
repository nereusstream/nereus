# Nereus Future 8：Advanced Pulsar Semantics

> 状态：Designed；当前仓库没有 advanced-semantics implementation
> 前置：Future 2/3/4/7 的 projection、cursor、generation、routing contracts

本文定义 Nereus L1/L4 Pulsar 高级语义设计边界。Future 8 的目标是让 Nereus 不只是
> basic topic storage，而是完整面向 Pulsar-native 商业产品：Key_Shared、delayed delivery、
> pending ack transaction、replicated subscription、schema/system topic bootstrap、topic
> compaction、geo-replication 和 policy interaction 都必须投影到 `streamId + offset`。

## 1. Motivation

Future 1-3 能定义 stream storage、ManagedLedger facade 和 durable cursor。Future 8 解决
Pulsar 产品真正难的部分：

- Key_Shared ordering；
- delayed delivery durable recovery；
- transaction pending ack；
- transaction buffer；
- replicated subscription；
- schema and system topic bootstrap；
- Pulsar topic compaction compatibility；
- geo-replication；
- namespace/topic policy interaction。

这些能力不能回退到 BookKeeper ledger truth，也不能为每个高级语义引入独立 durable log。

当前实现边界：schema/system-topic、transaction、delayed、replication 和 Key_Shared records/API 都
尚未存在。下面的 keys、JSON 和 state machines 是 target design，不能作为当前 Phase 1 行为声明。

## 2. Scope

Future 8 覆盖：

- Key_Shared hash range ownership and ordering boundary；
- delayed delivery durable index；
- pending ack transaction state；
- transaction buffer visibility；
- replicated subscription state；
- schema/system topic bootstrap；
- topic compaction compatibility；
- geo-replication offset translation；
- namespace/topic policy interaction；
- broker failover recovery for advanced semantics。

## 3. Non-scope

Future 8 不解决：

- 改变 L0 stream offset truth；
- 引入第二套 durable log；
- 重写 Pulsar protocol；
- 重写 KoP；
- 外部 lakehouse delivery；
- benchmark、compatibility certification、chaos validation。

## 4. Layer Boundary

Future 8 横跨 L1 和 L4：

```text
Pulsar broker runtime
  -> ManagedLedger / ManagedCursor facade
  -> advanced semantic state
  -> Oxia metadata and snapshot objects
  -> StreamStorage read/write truth
```

Future 8 可以做：

- 为 Pulsar 高级语义定义 Oxia-backed durable state；
- 使用 snapshot object 承载大状态；
- 定义 broker failover recovery；
- 定义 dispatch/runtime ownership；
- 定义 system topic bootstrap order。

Future 8 不能做：

- 让 broker local memory 成为 durable truth；
- 让 BookKeeper cursor ledger 或 ledger id 重新成为内部 truth；
- 让 pending ack 或 delayed delivery 旁路 cursor CAS；
- 让 geo-replication 隐式假设 source/target offset 相同。

## 5. Shared Metadata Schema

```text
/nereus/clusters/{cluster}/streams/{streamId}/key-shared/{subscription}/state
/nereus/clusters/{cluster}/streams/{streamId}/delayed/index/{bucketId}
/nereus/clusters/{cluster}/streams/{streamId}/transactions/{txnId}/buffer
/nereus/clusters/{cluster}/streams/{streamId}/transactions/{txnId}/pending-ack/{subscription}
/nereus/clusters/{cluster}/streams/{streamId}/replicated-subscriptions/{subscription}
/nereus/clusters/{cluster}/schemas/{tenant}/{namespace}/{topic}
/nereus/clusters/{cluster}/system-topics/{topicName}/bootstrap-state
/nereus/clusters/{cluster}/streams/{streamId}/topic-compaction/state
/nereus/clusters/{cluster}/replication/{sourceCluster}/{targetCluster}/{streamId}
/nereus/clusters/{cluster}/policies/{tenant}/{namespace}/{topic}
```

Large states use immutable snapshot objects referenced from Oxia state.

## 6. Key_Shared

Key_Shared durable state:

```json
{
  "streamId": "s-123",
  "subscription": "sub-key",
  "assignmentEpoch": 42,
  "hashRanges": [
    {
      "rangeStart": 0,
      "rangeEnd": 16383,
      "consumerId": "c1",
      "drainUntilOffset": 1048576
    }
  ],
  "cursorVersion": 88
}
```

Rules:

- cursor ack truth remains Future 3 cursor state；
- hash range assignment is broker/runtime coordination state with Oxia epoch；
- assignment epoch is not the F3 writable-ledger owner session and cannot authorize cursor mutation；
- rebalance must define drain boundary by stream offset；
- messages for the same key must not be concurrently dispatched across consumers before drain completes；
- broker failover recomputes assignment from Oxia state and cursor state。

Key ordering boundary:

```text
old owner can dispatch key K until drainUntilOffset
new owner can dispatch key K after drainUntilOffset is reached or old owner fenced
```

## 7. Delayed Delivery

Delayed delivery durable index:

```json
{
  "streamId": "s-123",
  "bucketId": "2026-07-04T10:00:00Z",
  "entries": [
    {
      "offset": 1048576,
      "deliverAt": 1783159200000,
      "ledgerProjection": "optional"
    }
  ],
  "snapshotRef": "delayed/s-123/bucket-42"
}
```

Rules:

- delayed index is derived from committed stream entries；
- timer wheel can live in broker memory, but durable delayed state is Oxia/snapshot；
- broker failover rebuilds timers from delayed index；
- delayed message is not lost if broker crashes；
- cursor ack state remains separate from delayed visibility。

Read behavior:

```text
if entry.deliverAt > now:
  skip for dispatch
  keep in delayed index
else:
  dispatch through normal cursor path
```

## 8. Pending Ack Transaction

Pending ack transaction state:

```json
{
  "txnId": "txn-123",
  "streamId": "s-123",
  "subscription": "sub-a",
  "state": "OPEN",
  "ackRanges": [
    {"startOffset": 1048576, "endOffset": 1048600}
  ],
  "cursorVersionAtOpen": 88,
  "snapshotRef": null
}
```

State machine:

```text
OPEN -> COMMITTING(decision durable) -> COMMITTED
OPEN -> ABORTING -> ABORTED
```

Commit rules:

1. CAS `OPEN -> COMMITTING` and persist an immutable commit decision/apply id；after this point abort is not
   allowed.
2. Apply pending ack ranges through the current Future 3 cursor handle/owner session with CAS，recording the apply id
   so retry is idempotent；a stale owner fails fenced rather than claiming the cursor.
3. CAS transaction state `COMMITTING -> COMMITTED` after cursor state proves the apply id.
4. Release pending-ack snapshot references only after terminal state and reader leases are safe.

The `apply id` is an F8 schema/capability extension around the F3 one-root CAS；it is not a cursor property、owner
session or transaction epoch and is not present in F3 V1 bytes。F8 must preserve current F3 owner-session fencing、
cursor generation、`ackStateEpoch`、normalization、snapshot-reference and retention ordering。

Recovery of `COMMITTING` always completes steps 2-3。A crash after cursor CAS but before terminal transaction
CAS therefore cannot later choose abort or silently roll back acknowledged progress。

Abort rules（only before a commit decision）：

1. Mark transaction `ABORTED`.
2. Do not advance cursor ack truth.
3. Redelivery is allowed from durable cursor state.

Pending ack cannot bypass cursor CAS.

## 9. Transaction Buffer

Transaction buffer stores records written by transactions before commit visibility:

```json
{
  "txnId": "txn-123",
  "streamId": "s-123",
  "offsetStart": 1048576,
  "offsetEnd": 1048600,
  "state": "OPEN",
  "markerOffset": null
}
```

Visibility:

| Transaction state | `read_uncommitted` | normal Pulsar dispatch |
| --- | --- | --- |
| OPEN | optional/debug only | hidden |
| COMMITTED | visible | visible |
| ABORTED | hidden or abort marker only | hidden |

Reader must apply transaction visibility before dispatch.

## 10. Replicated Subscription

Replicated subscription state:

```json
{
  "subscription": "sub-a",
  "sourceCluster": "us-east",
  "targetCluster": "us-west",
  "sourceStreamId": "s-east-123",
  "targetStreamId": "s-west-456",
  "sourceMarkDeleteOffset": 1048576,
  "targetAppliedOffset": 2097152,
  "version": 12
}
```

Rules:

- source and target stream offsets are different coordinates；
- replication state must explicitly store translation；
- cursor progress replication is idempotent by source offset and version；
- target broker must not infer source progress from local target offset alone。

## 11. Schema and System Topic Bootstrap

Nereus must support Pulsar internal topics without circular dependency.

Bootstrap options:

| Option | Description | Tradeoff |
| --- | --- | --- |
| Classic bootstrap | keep selected system topics on existing storage during bootstrap | easiest migration, split storage |
| Nereus self-hosted bootstrap | all system topics use `nereus` after Oxia is ready | clean target, harder boot order |
| Hybrid bootstrap | minimal bootstrap metadata in Oxia, system topics later migrate to Nereus | balanced |

Recommended default:

```text
hybrid bootstrap
```

Bootstrap order:

1. Oxia cluster reachable.
2. Nereus cluster metadata initialized.
3. Minimal schema/system-topic bootstrap state written.
4. `nereus` storage class enabled.
5. System topics open through Nereus ManagedLedger facade.
6. Topic policies allow per-namespace migration.

Schema state belongs to Pulsar metadata layer, not L0 per-record storage truth.

## 12. Topic Compaction Compatibility

Pulsar topic compaction uses Future 4's separate `TOPIC_COMPACTED` view and immutable-object/task/CAS machinery. It
never publishes a lossy target into the ordinary `COMMITTED` offset index:

- compact by key；
- preserve offset coverage；
- latest value by key remains visible in compacted read；
- tombstone semantics follow Pulsar policy；
- MessageId projection remains stable；
- compacted object must retain the exact entry/projection information required by the compacted-read contract；ordinary
  `PULSAR_ENTRY_V1` reads continue to use a lossless generation containing the exact opaque Entry bytes。

Topic compaction state:

```json
{
  "streamId": "s-123",
  "compactionId": "topic-cmp-42",
  "offsetStart": 1048576,
  "offsetEnd": 1114112,
  "generation": 18,
  "state": "PUBLISHED"
}
```

## 13. Geo-replication

Geo-replication writes target stream offsets independently:

```json
{
  "sourceCluster": "us-east",
  "targetCluster": "us-west",
  "sourceStreamId": "s-east-123",
  "targetStreamId": "s-west-456",
  "sourceOffsetStart": 1048576,
  "sourceOffsetEnd": 1048600,
  "targetOffsetStart": 2097152,
  "targetOffsetEnd": 2097176,
  "replicationVersion": 9
}
```

Rules:

- source offset and target offset are never assumed equal；
- deduplication uses source cluster + source stream + source offset range；
- replicated MessageId projection must be explicit；
- cursor/replicated subscription state uses translation table。

## 14. Policy Interaction

Nereus policy surface must handle:

- storage class `nereus`；
- WAL profile selection；
- SBT/SDT enablement；
- topic compaction policy；
- delayed delivery policy；
- transaction enablement；
- replication policy；
- retention and backlog quota；
- namespace-level migration policy。

Policy changes are metadata updates in Oxia-backed namespace/topic policy state. Runtime caches are
discardable and must be invalidated through notifications.

## 15. Failure Model

| Failure | Expected behavior |
| --- | --- |
| Broker crashes during Key_Shared rebalance | New broker recovers assignment epoch and cursor state |
| Broker crashes with delayed timers in memory | Timers rebuild from delayed index |
| Pending ack txn CAS loses before commit decision | Refresh and retry or abort from `OPEN`；after `COMMITTING`, recovery must finish the idempotent cursor apply and terminal commit |
| Transaction buffer marker missing | Reader keeps records hidden until transaction state resolves |
| Replicated subscription update duplicated | Target applies idempotently by source offset/version |
| System topic bootstrap interrupted | Bootstrap state resumes from Oxia stage marker |
| Topic compaction publish succeeds before broker response | New generation visible; retry is idempotent |
| Geo-replication target write succeeds but state update fails | Repair uses source offset range dedup key |

## 16. Compatibility Impact

### Pulsar

Future 8 must preserve observable behavior for:

- Key_Shared ordering；
- delayed delivery redelivery behavior；
- transactions and pending ack；
- replicated subscription；
- schema lookup/update；
- system topic lifecycle；
- topic compaction；
- geo-replication；
- namespace/topic policies。

### Storage

All advanced semantics remain projections over:

```text
streamId + offset
```

No advanced feature may introduce a second durable log or use broker local disk as correctness source.

## 17. Future Gate

Future 8 may enter implementation planning only after the following are reviewed:

- Key_Shared assignment and drain boundary；
- delayed delivery durable index and recovery；
- pending ack transaction state machine；
- transaction buffer visibility；
- replicated subscription offset translation；
- schema/system topic bootstrap order；
- topic compaction compatibility；
- geo-replication translation table；
- policy update and cache invalidation rules。

This is a design gate. It does not require benchmark, compatibility certification, CI, or real evidence.
