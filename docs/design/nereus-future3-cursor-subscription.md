# Nereus Future 3：Cursor / Subscription State

> 状态：Designed；仓库已有 F2 broker-local non-durable/durable-boundary cursor，但尚无 F3 durable CursorStorage implementation
> 前置：Future 2 Position projection、Future 1 read/trim semantics

本文定义 Pulsar durable cursor progress 如何从 BookKeeper cursor ledger 迁移到 Oxia + immutable
object snapshot，同时保持 `streamId + offset` 是逻辑坐标。Cursor state 是独立 commit domain，不能
改变 stream-head append truth。

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

当前实现边界：`StreamStorage` 还没有 cursor methods，`CURSOR_SNAPSHOT_OBJECT` 只有 object-type
reservation。实现 F3 前必须先冻结 projection-to-record-range API、snapshot bytes 和 cursor CAS record；
不能把本设计中的 pseudo-code 当成已存在 Java surface。

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
| `markDeleteOffset` | first persisted-entry offset not cumulatively acknowledged |
| `readPositionOffset` | next offset the broker should try to dispatch; recovery hint, not ack truth |
| `individualAckRanges` | fully acknowledged persisted-entry ranges above `markDeleteOffset`, represented as half-open stream offsets |
| `partialBatchAckSets` | remaining-message bitset keyed by persisted-entry offset for partially acknowledged Pulsar batch entries |
| `ackSnapshotRef` | object snapshot reference when ack ranges are too large for Oxia value |
| `cursorVersion` | CAS version for cursor update |

This means:

```text
all offsets < markDeleteOffset are acknowledged
offsets in individualAckRanges are acknowledged
an offset in partialBatchAckSets is still readable, but its cleared message indexes are acknowledged
other offsets >= markDeleteOffset are not acknowledged
```

`readPositionOffset` may move ahead of `markDeleteOffset` while messages are dispatched but not acked.
After broker failover it can be used as a recovery hint, but correctness must be recomputed from
`markDeleteOffset + individualAckRanges + partialBatchAckSets`.

Future 2 persists one Pulsar entry as exactly one stream offset. It does **not** split a batch entry into one
offset per message. Consequently, a batch index can never be projected to a stream sub-range. The persisted
`BatchAckSet` uses Pulsar's convention: a set bit means “still unacknowledged”; updates merge with bitwise AND;
an empty bitset promotes that whole entry offset into `individualAckRanges`.

## 6. API Mapping

### 6.1 ManagedCursor methods

| ManagedCursor method group | Nereus mapping |
| --- | --- |
| `readEntries` / `asyncReadEntries` | read from `readPositionOffset`, skip acknowledged ranges, advance read-position hint |
| `readEntriesOrWait` | register waiter on stream notification when no committed offset is available |
| `markDelete(Position)` | whole entry: CAS-advance `markDeleteOffset` to `entryOffset + 1`; position with ack set: cumulatively acknowledge earlier entries and persist the target entry's partial batch set |
| `delete(Position)` | whole entry: add one entry range; position with ack set: merge the target entry's partial batch set; normalize and maybe advance `markDeleteOffset` |
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

    CompletableFuture<CursorUpdateResult> cumulativeAck(
            StreamId streamId,
            String cursorName,
            EntryAck acknowledgement,
            CursorUpdateOptions options);

    CompletableFuture<CursorUpdateResult> individualAck(
            StreamId streamId,
            String cursorName,
            List<EntryAck> acknowledgements,
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

record EntryAck(
        Offset entryOffset,
        Optional<BatchAckSet> partialBatchAckSet) {
}

record BatchAckSet(int batchSize, long[] remainingIndexes) {
    BatchAckSet {
        remainingIndexes = remainingIndexes.clone();
        // validate batchSize > 0 and every set bit < batchSize
    }

    @Override
    public long[] remainingIndexes() {
        return remainingIndexes.clone();
    }
}
```

`partialBatchAckSet = Optional.empty()` means the whole persisted entry is acknowledged. `remainingIndexes`
must be copied on API ingress/egress, must not contain a set bit `>= batchSize`, and is merged with an existing
set using bitwise AND. The facade obtains `batchSize` by resolving the persisted entry and decoding its Pulsar
metadata before calling `CursorStorage`; an ack-set `Position` alone cannot reliably reveal trailing cleared
indexes. A resolution/parse failure fails the ack and performs no cursor CAS. All update methods use Oxia CAS
against `cursorVersion`.

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
  "partialBatchAckInline": [
    {"entryOffset": 1050120, "batchSize": 10, "remainingIndexes": [960]}
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
  entryOffset
  batchSize
  remainingIndexes

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

This table stores the **first not cumulatively acknowledged offset**, not a raw ManagedLedger mark-delete Position.
At the API boundary `newNonDurableCursor(P)` follows the F2/stock rule that `P` is already consumed, so F2 first maps
it once to `max(trimOffset, P.entryId + 1)`. F3 persists that resulting offset directly and must not apply another
increment. Conversely, `seek(P)` and `resetCursor(P)` treat `P` as a direct read target; they never reuse the
cursor-creation conversion.

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

1. Map `Position` to exactly one persisted-entry offset.
2. If the position has no ack set, compute `newMarkDeleteOffset = entryOffset + 1`.
3. If the position has an ack set, cumulatively acknowledge offsets before `entryOffset`, merge the target
   entry's `BatchAckSet`, and keep `markDeleteOffset <= entryOffset` until its remaining bitset is empty.
4. Verify the resulting `newMarkDeleteOffset >= current.markDeleteOffset`.
5. Remove whole-entry ranges and partial batch sets below new mark-delete.
6. If the first fully acknowledged entry range starts at new mark-delete, fold it into mark-delete and continue.
7. CAS update cursor state.
8. Publish cursor notification.

### 9.4 Individual ack

Input:

```text
Position or MessageId
```

Steps:

1. Map each position to exactly one persisted-entry offset; reject a foreign incarnation or invalid ledger id.
2. A position without an ack set adds `[entryOffset, entryOffset + 1)` to `individualAckRanges`.
3. A position with an ack set validates `batchSize`, then merges `remainingIndexes` with the existing set using
   bitwise AND; it never creates a sub-entry stream offset.
4. When a merged remaining bitset becomes empty, remove it from `partialBatchAckSets` and promote
   `[entryOffset, entryOffset + 1)` to `individualAckRanges`.
5. Merge overlapping or adjacent whole-entry ranges.
6. Advance `markDeleteOffset` while the first whole-entry ack range covers it; discard partial sets below it.
7. Spill both whole-entry ranges and partial batch sets to the same immutable snapshot if inline state exceeds
   the configured byte threshold.
8. CAS update cursor state; on CAS loss, reload and recompute the merge from the original immutable request.

The batch entry remains readable until its remaining bitset is empty. On dispatch, the facade returns the
entry with that remaining bitset through `getBatchPositionAckSet(Position)` / broker batch-index metadata so
already acknowledged messages are not delivered again during a healthy session. Recovery may conservatively
redeliver only when a documented persistence limit was explicitly chosen; the default F3 contract persists all
partial sets required for correctness.

### 9.5 Seek / reset

Seek changes read position, not ack truth:

```text
readPositionOffset = max(targetOffset, markDeleteOffset)
```

If force reset is allowed by Pulsar semantics, the target can move before mark-delete only when the caller
explicitly requests replay and retention still protects the range.

Resetting to a trimmed offset cannot recreate bytes: without a compacted view that still protects that coordinate,
the target advances to trim or fails according to the public method's force contract. This is separate from
non-durable cursor behavior, which never contributes a retention low-watermark and always advances its next read to
the first retained offset after trim overtakes it.

### 9.6 Clear backlog

```text
markDeleteOffset = committedEndOffset
readPositionOffset = committedEndOffset
individualAckRanges = empty
partialBatchAckSets = empty
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
- Broker-local/non-durable cursors do not enter this minimum and cannot block trim or GC; reads from an overtaken local
  position resume at `trimOffset` while preserving the surviving MessageIds.

Backlog estimate:

```text
backlogRecords =
  committedEndOffset
  - markDeleteOffset
  - acknowledgedEntriesInIndividualAckRanges
```

This is an entry backlog, matching the ManagedLedger entry coordinate. Message-level backlog additionally
subtracts cleared indexes in `partialBatchAckSets` and requires the persisted `batchSize`. Backlog bytes should
use offset index `cumulativeSize` when possible and fall back to estimates when exact range accounting would
require scanning too many entries.

## 12. Broker Failover Recovery

On broker failover:

1. New broker opens `NereusManagedCursor`.
2. It reads Oxia cursor state.
3. If `ackSnapshotRef` exists, it loads snapshot object and validates checksum/version.
4. It computes first dispatchable offset:
   ```text
   firstUnackedOffset(markDeleteOffset, individualAckRanges, partialBatchAckSets)
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
| Ack range or partial batch state grows beyond inline limit | Spill both structures to snapshot and CAS cursor state |

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
2. `markDeleteOffset` is the first not cumulatively acknowledged persisted-entry offset.
3. Individual ack ranges are half-open, whole-entry stream offset ranges above mark-delete.
4. Partial batch ack is an entry-keyed remaining-message bitset; it never changes Future 2 offset allocation.
5. Cursor updates use CAS on `cursorVersion`.
6. Snapshot object visibility comes from Oxia cursor state reference.
7. Read position is a dispatch hint, not ack truth.
8. Broker local state is discardable.
9. Cursor low-watermark protects stream data from unsafe trim/GC.
10. Redelivery after failover is allowed; losing acked progress after successful CAS is not.
11. Cursor state cannot change stream offset ordering or visibility.
12. Backlog/high-watermark comes from stream-head committed end；offset index只用于 size/range resolution，
    不是 cursor 可自行推进的 append truth。

## 16. Future Gate

Future 3 may enter implementation planning only after the following are reviewed:

- cursor offset conventions；
- ManagedCursor method mapping；
- Oxia cursor metadata schema；
- inline ack range representation；
- partial batch ack encoding and merge semantics；
- cursor snapshot object format；
- mark-delete and individual ack state transitions；
- read-position recovery semantics；
- retention and GC low-watermark rules；
- broker failover recovery；
- compatibility boundaries with Future 4, Future 5, and Future 8。

This is a design gate. It does not require benchmark, chaos, compatibility certification, CI, or real evidence.
