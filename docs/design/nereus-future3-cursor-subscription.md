# Nereus Future 3：Cursor / Subscription State

> 本文是 Nereus L1 cursor/subscription 设计文档。它定义 Pulsar durable cursor progress
> 如何从 BookKeeper cursor ledger 迁移到 Oxia + object snapshot，同时保持
> `streamId + offset` 是内部 truth。当前阶段只冻结设计边界，不要求实现或验证。

## 1. Motivation

Pulsar 的差异化能力很大一部分来自 subscription 和 cursor：

- Exclusive / Failover；
- Shared individual ack；
- batch ack；
- read position 和 mark-delete；
- seek/reset；
- backlog/retention；
- cursor properties；
- broker failover 后恢复。

在 Nereus 中，cursor 不能继续依赖 BookKeeper cursor ledger。Cursor durable progress 必须
由 Oxia small state 和 object snapshot 承载，并且所有 progress 都投影到：

```text
streamId + offset
```

Future 3 的目标是定义这套 cursor state model。它不改变 Future 1 的 stream offset truth，
也不要求改写 broker dispatcher。

## 2. Scope

Future 3 覆盖：

- cursor metadata schema；
- mark-delete offset；
- read-position offset；
- individual ack ranges；
- partial batch ack representation；
- cursor CAS；
- cursor snapshot object；
- cursor properties；
- Exclusive / Failover cursor；
- Shared cursor durable progress；
- cursor open/recovery；
- seek/reset/rewind/clear backlog/skip；
- backlog and retention low-watermark；
- broker failover recovery。

## 3. Non-scope

Future 3 不解决：

- Key_Shared ordering and hash range ownership；
- delayed delivery timer index；
- pending ack transaction；
- replicated subscription；
- KoP consumer group offsets；
- geo-replication offset translation；
- dispatcher fairness and consumer flow-control policy。

这些能力依赖 Future 3 的 durable cursor state，但在 Future 8 或 Future 5 中设计。

## 4. Layer Boundary

Future 3 位于 L1：

```text
ManagedCursor facade
  -> Nereus cursor state
  -> Oxia cursor metadata
  -> optional cursor snapshot object
  -> StreamStorage read resolver
```

Future 3 可以做：

- 把 `Position` / `MessageId` 转为 stream offset；
- 维护 durable ack progress；
- 维护 cursor read-position hint；
- 为 broker 暴露 ManagedCursor-compatible methods；
- 计算 backlog and retention low-watermark。

Future 3 不能做：

- 改变 stream offset 分配；
- 用 BookKeeper cursor ledger 做 durable truth；
- 让 broker local memory 成为 durable cursor state；
- 让 object snapshot 自己决定 cursor 可见性；
- 把 pending ack transaction 的 commit/abort 放进普通 ack path。

## 5. Cursor Coordinate Model

Nereus 使用 half-open stream offset range：

```text
[startOffset, endOffset)
```

Cursor state 中的核心字段：

| Field | Meaning |
| --- | --- |
| `markDeleteOffset` | first not cumulatively acknowledged record offset |
| `readPositionOffset` | next offset the broker should try to dispatch; recovery hint, not ack truth |
| `individualAckRanges` | acked ranges above `markDeleteOffset`, represented as half-open stream offsets |
| `ackSnapshotRef` | object snapshot reference when ack ranges are too large for Oxia value |
| `cursorVersion` | CAS version for cursor update |

This means:

```text
all offsets < markDeleteOffset are acknowledged
offsets in individualAckRanges are acknowledged
other offsets >= markDeleteOffset are not acknowledged
```

`readPositionOffset` may move ahead of `markDeleteOffset` while messages are dispatched but not acked.
After broker failover it can be used as a recovery hint, but correctness must be recomputed from
`markDeleteOffset + individualAckRanges`.

## 6. API Mapping

### 6.1 ManagedCursor methods

| ManagedCursor method group | Nereus mapping |
| --- | --- |
| `readEntries` / `asyncReadEntries` | read from `readPositionOffset`, skip acknowledged ranges, advance read-position hint |
| `readEntriesOrWait` | register waiter on stream notification when no committed offset is available |
| `markDelete(Position)` | convert position to offset range end, CAS-advance `markDeleteOffset` |
| `delete(Position)` | add individual ack range, normalize, maybe advance `markDeleteOffset` |
| `delete(Iterable<Position>)` | batch individual ack update |
| `getReadPosition` | project `readPositionOffset` to `Position` |
| `getMarkDeletedPosition` | project `markDeleteOffset - 1` to `Position` if available |
| `rewind` | set read position to first unacked offset |
| `seek(Position)` | map position to offset and set read position, clamped by mark-delete unless forced |
| `resetCursor(Position)` | reset read position and optionally mark-delete according to Pulsar semantics |
| `clearBacklog` | set mark-delete to stream committed end offset and clear ack holes |
| `skipEntries` | move read position forward through resolver, respecting deleted ranges |
| `findNewestMatching` | scan via read resolver over active or all available entries |
| `getNumberOfEntriesInBacklog` | compute from committed end offset minus acked progress |
| cursor properties | store in Oxia cursor property map with CAS |
| `close` | close local facade and watchers; durable state remains |

### 6.2 CursorStorage internal API

```java
interface CursorStorage {
    CompletableFuture<CursorState> openCursor(
            StreamId streamId,
            String cursorName,
            CursorOpenOptions options);

    CompletableFuture<CursorState> readCursor(
            StreamId streamId,
            String cursorName);

    CompletableFuture<CursorUpdateResult> markDelete(
            StreamId streamId,
            String cursorName,
            Offset markDeleteOffset,
            CursorUpdateOptions options);

    CompletableFuture<CursorUpdateResult> individualAck(
            StreamId streamId,
            String cursorName,
            List<OffsetRange> ackRanges,
            CursorUpdateOptions options);

    CompletableFuture<CursorUpdateResult> updateReadPosition(
            StreamId streamId,
            String cursorName,
            Offset readPositionOffset,
            CursorUpdateOptions options);

    CompletableFuture<CursorUpdateResult> seek(
            StreamId streamId,
            String cursorName,
            Offset readPositionOffset,
            SeekOptions options);
}
```

All update methods use Oxia CAS against `cursorVersion`.

## 7. Oxia Metadata Schema

```text
/nereus/clusters/{cluster}/streams/{streamId}/cursors/{cursorName}/state
/nereus/clusters/{cluster}/streams/{streamId}/cursors/{cursorName}/ack-ranges
/nereus/clusters/{cluster}/streams/{streamId}/cursors/{cursorName}/properties
/nereus/clusters/{cluster}/streams/{streamId}/cursors/{cursorName}/snapshots/{snapshotId}
/nereus/clusters/{cluster}/streams/{streamId}/cursors/{cursorName}/leases/{brokerId}
```

### 7.1 CursorState

```json
{
  "streamId": "s-123",
  "cursorName": "sub-a",
  "subscriptionType": "Shared",
  "durable": true,
  "markDeleteOffset": 1048576,
  "readPositionOffset": 1052672,
  "individualAckInline": [
    {"startOffset": 1049600, "endOffset": 1049664},
    {"startOffset": 1050000, "endOffset": 1050100}
  ],
  "ackSnapshotRef": "cursor-snapshots/s-123/sub-a/snap-42",
  "ackSnapshotVersion": 42,
  "cursorPropertiesVersion": 7,
  "lastActiveBroker": "broker-7",
  "cursorVersion": 88
}
```

### 7.2 Cursor properties

Cursor properties are compatibility metadata:

```json
{
  "cursorName": "sub-a",
  "properties": {
    "app": "payments",
    "owner": "team-a"
  },
  "positionProperties": {
    "eventTime": 1783036800000
  },
  "version": 7
}
```

Properties do not affect stream visibility or ack truth.

## 8. Cursor Snapshot Object

Small ack ranges stay inline in Oxia. Large ack state moves to immutable object snapshot.

```text
CursorSnapshotHeader
  streamId
  cursorName
  snapshotId
  baseMarkDeleteOffset
  cursorVersion
  createdAt
  checksum

AckRangeSection
  rangeCount
  ranges[startOffset,endOffset)

BatchAckSection
  entryProjectionRef
  batchAckSet

Footer
  minAckOffset
  maxAckOffset
  checksum
```

Snapshot rules:

- Snapshot object is immutable.
- Oxia cursor state stores the snapshot reference and version.
- Snapshot visibility is controlled by Oxia cursor state, not by object existence.
- If snapshot upload succeeds but CAS fails, the snapshot is orphan and can be GC'd.
- Snapshot compaction can merge inline ranges and old snapshot ranges into a new snapshot.

## 9. State Transitions

### 9.1 Open cursor

```text
MISSING
  -> OpenCursor(initialPosition)
  -> ACTIVE_CURSOR
```

Initial position:

| Initial position | `markDeleteOffset` | `readPositionOffset` |
| --- | --- | --- |
| Earliest | first available offset after trim | first available offset after trim |
| Latest | current `committedEndOffset` | current `committedEndOffset` |
| Explicit position | position mapped offset, clamped by trim | same |

Open is idempotent. Multiple brokers may open the same durable cursor facade, but updates must use CAS.

### 9.2 Read

```text
ACTIVE_CURSOR
  -> read from readPositionOffset
  -> skip acknowledged ranges
  -> return entries
  -> advance readPositionOffset hint
```

Read position update can be coalesced. It is not ack truth.

### 9.3 Cumulative ack / mark-delete

Input:

```text
Position p
```

Steps:

1. Map `Position` to entry offset range.
2. Compute `newMarkDeleteOffset = entryEndOffset`.
3. Verify `newMarkDeleteOffset >= current.markDeleteOffset`.
4. Remove individual ack ranges below new mark-delete.
5. If the first ack range starts at new mark-delete, fold it into mark-delete and continue.
6. CAS update cursor state.
7. Publish cursor notification.

### 9.4 Individual ack

Input:

```text
Position or MessageId
```

Steps:

1. Map position/batchIndex to one or more stream offset ranges.
2. Add ranges to `individualAckRanges`.
3. Merge overlapping or adjacent ranges.
4. Advance `markDeleteOffset` while the first ack range covers it.
5. Spill to snapshot if inline state exceeds threshold.
6. CAS update cursor state.

Partial batch ack is represented at record offset granularity. The batch entry remains readable until all
unacked records inside it are no longer needed.

### 9.5 Seek / reset

Seek changes read position, not ack truth:

```text
readPositionOffset = max(targetOffset, markDeleteOffset)
```

If force reset is allowed by Pulsar semantics, the target can move before mark-delete only when the caller
explicitly requests replay and retention still protects the range.

### 9.6 Clear backlog

```text
markDeleteOffset = committedEndOffset
readPositionOffset = committedEndOffset
individualAckRanges = empty
ackSnapshotRef = null
```

This is a cursor update, not a stream trim. GC still waits for all retention and reference rules.

## 10. Subscription Modes

| Subscription mode | Durable state | Notes |
| --- | --- | --- |
| Exclusive | one cursor state, read/mark-delete offsets | consumer exclusivity remains broker policy |
| Failover | one cursor state, active consumer chosen by broker | durable progress same as Exclusive |
| Shared | one cursor state with individual ack ranges | ack holes and snapshot are required |
| Key_Shared | cursor state same base as Shared | key ordering/range ownership belongs to Future 8 |

Future 3 does not make broker ownership durable. Consumer assignment remains broker runtime state.

## 11. Retention and GC Low-watermark

For a stream, cursor low-watermark is:

```text
min(cursor.markDeleteOffset for all durable cursors)
```

Rules:

- Data before all durable cursor `markDeleteOffset`s can be eligible for trim, subject to retention policy.
- Individual ack ranges above `markDeleteOffset` do not move low-watermark by themselves.
- Unacked gaps above `markDeleteOffset` must remain readable.
- Cursor snapshot objects are protected by cursor state references.
- Reader leases and future Kafka group offsets can add additional protection.

Backlog estimate:

```text
backlogRecords =
  committedEndOffset
  - markDeleteOffset
  - acknowledgedRecordsInIndividualAckRanges
```

Backlog bytes should use offset index `cumulativeSize` when possible and fall back to estimates when exact
range accounting would require scanning too many entries.

## 12. Broker Failover Recovery

On broker failover:

1. New broker opens `NereusManagedCursor`.
2. It reads Oxia cursor state.
3. If `ackSnapshotRef` exists, it loads snapshot object and validates checksum/version.
4. It computes first dispatchable offset:
   ```text
   firstUnackedOffset(markDeleteOffset, individualAckRanges)
   ```
5. It sets read-position hint to first dispatchable offset or a safe value near it.
6. It resumes dispatch through Future 1 read resolver.

Recovery must prefer correctness over avoiding redelivery. Redelivery is allowed; losing an acked message or
advancing mark-delete incorrectly is not.

## 13. Failure Model

| Failure | Expected behavior |
| --- | --- |
| Broker crashes after read before ack | Messages redeliver from durable cursor state |
| Broker crashes after ack CAS succeeds before client response | Ack is durable; duplicate ack is idempotent |
| Broker crashes before ack CAS | Ack may be lost; message redelivers |
| Concurrent mark-delete updates | One CAS succeeds; loser refreshes and retries if still valid |
| Snapshot upload succeeds but cursor CAS fails | Snapshot is orphan; cursor state still points to old snapshot |
| Snapshot object missing or checksum fails | Cursor recovery fails safe; no mark-delete advancement |
| Read position hint stale | Recompute from mark-delete and ack ranges |
| Trim races with cursor update | Trim uses cursor low-watermark from Oxia; CAS/version prevents unsafe advancement |
| Ack range grows beyond inline limit | Spill to snapshot and CAS cursor state |

## 14. Compatibility Impact

### ManagedCursor

Future 3 must preserve the observable behavior of:

- `readEntries` and `readEntriesOrWait`；
- `markDelete`；
- `delete(Position)` and `delete(Iterable<Position>)`；
- `getReadPosition`；
- `getMarkDeletedPosition`；
- `rewind`；
- `seek` / `resetCursor`；
- `clearBacklog`；
- `skipEntries`；
- `getNumberOfEntriesInBacklog`；
- cursor properties；
- `isDurable`。

### PersistentTopic / Subscription

PersistentTopic and subscription dispatchers should see a ManagedCursor-compatible object. Nereus owns
durable progress; broker still owns dispatch policy and flow control.

### Future boundaries

- Future 8 adds Key_Shared ordering, delayed delivery, pending ack transaction, and replicated subscription.
- Future 5 adds Kafka group offsets and retention interaction.
- Future 4 adds compaction generation and cursor-safe GC.

## 15. Design Invariants

1. Cursor durable truth is Oxia cursor state plus referenced snapshot object.
2. `markDeleteOffset` is the first not cumulatively acknowledged stream offset.
3. Individual ack ranges are half-open stream offset ranges above mark-delete.
4. Cursor updates use CAS on `cursorVersion`.
5. Snapshot object visibility comes from Oxia cursor state reference.
6. Read position is a dispatch hint, not ack truth.
7. Broker local state is discardable.
8. Cursor low-watermark protects stream data from unsafe trim/GC.
9. Redelivery after failover is allowed; losing acked progress after successful CAS is not.
10. Cursor state cannot change stream offset ordering or visibility.

## 16. Future Gate

Future 3 design is ready to move into implementation planning when the following are reviewed:

- cursor offset conventions；
- ManagedCursor method mapping；
- Oxia cursor metadata schema；
- inline ack range representation；
- cursor snapshot object format；
- mark-delete and individual ack state transitions；
- read-position recovery semantics；
- retention and GC low-watermark rules；
- broker failover recovery；
- compatibility boundaries with Future 4, Future 5, and Future 8。

This is a design gate. It does not require benchmark, chaos, compatibility certification, CI, or real evidence.
