# Nereus Future 3：Cursor / Subscription State

> 状态：F3-M0 / F3-M0R design-gated；F3-M1 metadata/snapshot foundation complete/final-gated；F3-M2+ pending
>
> 代码级合同：[Phase 3 Cursor / Subscription Detailed Design](../phase-3-cursor-subscription/README.md)
>
> 前置：Future 2 final-gated ManagedLedger facade、Future 1 committed read/trim semantics

本文是 Future 3 的 north-star 摘要。字段、key、binary layout、Java interface、状态机、测试和 gate 的
authoritative specification 在 `docs/phase-3-cursor-subscription/`。发生冲突时以该目录为准。

## 1. Goal

F3 把 Pulsar durable cursor progress 从 BookKeeper cursor ledger 迁移为：

```text
one Oxia CursorStateRecord CAS root
  + optional immutable cursor snapshot object
  + F1 committed Entry/read/trim truth
```

它支持：

- durable/non-durable Exclusive、Failover、Shared；
- cumulative、individual 和 batch-index acknowledgement；
- durable subscription open/recovery/delete/recreate；
- seek、rewind、reset、clear backlog、skip、TTL/expiration；
- cursor/position properties；
- conservative retention floor 和 recoverable logical-trim barrier；
- broker unload/restart/failover 后稳定 MessageId 和 ack truth。

F3 不改变 append offset、commit、fencing、visibility 或物理 GC。

## 2. Non-goals

- Key_Shared ordering/hash ownership；
- transaction pending ack；
- replicated subscription；
- delayed delivery；
- read-compacted/compaction materialized view；
- KoP group offsets；
- Lakehouse metadata/reference；
- physical object deletion and policy retention。

这些能力分别属于 F4/F5/F6/F8，不能通过 cursor properties 或未声明 key 偷渡进 F3。

## 3. Coordinate Contract

F2 has already locked：

```text
one persisted Pulsar Entry = one Nereus stream offset
Position.ledgerId = stable F2 virtualLedgerId
Position.entryId = stream offset
batchIndex = sub-index inside immutable Entry payload
```

F3 never creates a virtual cursor ledger or a batch sub-offset。Cursor generation、Oxia version 和 snapshot ID
也不进入 client MessageId。

Core coordinates：

```text
markDeleteOffset
  first offset not cumulatively acknowledged

wholeAckRanges
  sorted/disjoint/non-adjacent half-open fully-acked ranges above mark-delete

partialBatchAcks[offset]
  Pulsar remaining-message long[]; set bit means unacknowledged

localReadOffset
  next broker-local dispatch candidate; never durable
```

At API boundary：

```text
getMarkDeletedPosition = project(markDeleteOffset - 1)
getReadPosition = project(localReadOffset)
```

Normal dispatch advancement is deliberately not persisted。After failure, the new broker rebuilds
`localReadOffset` from the first whole-unacked Entry。Redelivery is legal；restoring a dispatch-ahead durable read
position and skipping unacked messages is not。

## 4. Durable Authority

Per cursor：

```text
/nereus/clusters/{cluster}/streams/{stream}/facade/managed-ledger/
  cursors/v1/by-hash/{cursorNameHash}/state
```

The single `CursorStateRecord` includes：

- exact F2 `ManagedLedgerProjectionIdentity`；
- exact cursor name + stable hash；
- current writable-ledger `ownerSessionId`；
- `cursorGeneration` and ACTIVE/DELETED lifecycle；
- `mutationSequence`、`ackStateEpoch`、last protection-attempt proof and Oxia CAS version；
- `markDeleteOffset`；
- optional immutable snapshot reference；
- bounded inline whole-range deltas and partial overrides；
- position properties and cursor properties；
- created/updated/deleted timestamps。

Correctness is not split into state/range/property/ref keys。Encoded metadata version remains zero；the Oxia-returned
version is the CAS token。

Delete keeps a tombstone。Same-name recreate increments generation so a stale handle cannot mutate the new cursor；
the open-time owner-session claim separately fences crash-delayed mutations from an earlier writable broker instance。

## 5. Batch Ack Contract

Pulsar `AckSetState.long[]` uses Java BitSet word layout：

```text
1 = still unacknowledged
0 = acknowledged
merge = bitwise AND
```

Distinct forms：

```text
no ack-set extension       -> whole Entry ack
present nonempty ack set   -> partial batch ack
present empty ack set      -> whole batch Entry ack
```

The facade reads and decodes the committed Pulsar Entry before a partial CAS because Position cannot prove trailing
cleared indexes or exact batch size。Invalid/foreign/future/trimmed positions fail without cursor mutation。

F3 does not truncate individual ranges or partial-batch maps。When inline state grows, it spills to snapshot；when the
configured total cap is exceeded, the mutation fails before CAS/callback。

## 6. Snapshot Contract

Small state stays in the root。Large state uses：

```text
immutable full normalized ack snapshot
  + current root markDelete
  + bounded root whole-range deltas
  + bounded root partial overrides
```

Object key：

```text
{clusterComponent}/cursor-snapshots/v1/{streamIdComponent}/
  {cursorNameHash}/{cursorGeneration019}/{random128SnapshotId}.ncs
```

The V1 object has `NCS1` header、identity/generation/source sequence、normalized ranges/partial words、per-section
CRC32C and `NCF1` footer format CRC。A separate full-stored-byte CRC32C is used by PUT/HEAD and recorded in the root
reference。Lengths/counts are strictly bounded and trailing bytes fail。

Publish order：

```text
encode full state
  -> immutable PUT ifAbsent
  -> HEAD exact length/checksum/type
  -> CAS root to new ref and clear deltas
```

Only the root CAS makes the object visible。CAS-lost uploads are orphans for F4。A stable missing/corrupt reference
fails cursor/topic open；there is no older/inline fallback。

## 7. Mutation Semantics

Every local cursor serializes mutations through a bounded lane；cross-broker order is the open-time owner-session
claim plus Oxia version-CAS。Every ordinary cursor/retention mutation must carry and preserve the writable ledger's
exact owner session；a different session fails fenced and cannot steal ownership through retry。

### Cumulative ack

- whole Entry `o` advances first-unacked to `o+1` and removes/folds earlier holes；
- partial batch at `o` cumulatively acks every earlier Entry, keeps `markDeleteOffset=o`, and stores merged remaining
  bits；empty result advances to `o+1`；
- supplied position properties commit in the same root CAS。

### Individual ack

- whole Entry adds `[o,o+1)`；
- partial Entry AND-merges remaining words；
- empty partial result becomes a whole range；
- normalized range beginning at mark-delete is folded forward。

Ack retry after CAS success/response loss is idempotent。Monotonic ack may reload/recompute after CAS conflict。
It may do so only when the latest root retains the request's `ackStateEpoch`；reset/clear-backlog increments that epoch，
so a non-subsumed old ack fails conflict instead of reappearing after destructive replacement。

### Seek / rewind / reset

- seek and rewind move only local read position；
- reset is a destructive durable transition: target is the direct next-read offset, becomes mark-delete, clears old
  ranges/ref and optionally retains the target's validated partial state；it increments `ackStateEpoch`；
- reset CAS conflict can reapply only across same-epoch monotonic/property updates；a changed epoch fails busy；
- ordinary reset normalizes EARLIEST/LATEST and trimmed/future inputs to current trim/end；before F4 compacted bytes，
  force reset cannot target outside that retained/tail range。

### Clear / skip / delete

- clear snapshots committed end `E` and sets mark-delete to E；concurrent later append remains backlog；
- skip is durable cumulative progress through the resolved Nth Entry, not a local seek；
- delete CASes ACTIVE to DELETED, clears ref/state/properties, and succeeds for missing/deleted cursor。

Callback success is emitted exactly once, on the callback executor, only after the authoritative CAS covers the
effect。No callback-before-persist throttling is allowed。

## 8. Topic Open and Failover

`PersistentTopic.initialize` rebuilds subscriptions from `ledger.getCursors()`。Pulsar topic ownership controls who
may initiate a writable open，but ownership/watch/graceful drain alone cannot fence a crash-delayed old Oxia CAS。
Therefore every writable ManagedLedger open：

1. invokes the exact broker-supplied ownership checker and requires true；
2. validates F2 projection/L0 bounds；
3. generates a fresh random 128-bit `ownerSessionId`；
4. opens invalidation watch before the cursor-key scan；
5. claims the stream retention root before any cursor root while preserving lifecycle/pending intent/floors；
6. CAS-claims every ACTIVE cursor root to that same session while preserving semantic state/generation/epoch/ref；
7. strictly hydrates every claimed ACTIVE snapshot and recovers pending retention work under the new session；
8. rescans until retention owner and the ACTIVE-root set/version fingerprint stabilize；
9. constructs all durable cursor facades and rechecks broker ownership；
10. only then completes ledger open and permits subscription initialization/dispatch。

At that callback the live cursor registry contains the complete durable set and no non-durable cursor can yet have
been admitted。Later non-durable creates join `getCursors()/getActiveCursors()` as in stock but never enter metadata
hydration、retention or slowest-durable ordering。

A corrupt one cannot be silently omitted。Non-durable cursors never appear in durable metadata hydration or
retention ordering，although stock-compatible live `getCursors()` includes them after local creation。

After broker failover：

```text
new writable open claims retention + every ACTIVE root before publication
old existing-root CAS before that root claim -> new owner reloads its result
old existing-root CAS after that root claim -> version/session mismatch, fenced
old already-pending CREATE/RECREATE target CAS -> may race on the unclaimed target key,
  but cannot stale-finalize/callback; new owner recovers and claims the winner before publication
ack CAS succeeded -> new broker observes ack
ack CAS not succeeded -> message redelivers
dispatch without ack -> local read hint lost, message redelivers
response lost after CAS -> duplicate ack succeeds idempotently
```

An open that loses its retention owner claim during stabilization fails fenced；ordinary mutation paths never replace
`ownerSessionId`。DELETED tombstones need no bulk claim because recreate is serialized through the claimed retention
root and writes the current session into the new ACTIVE generation。

## 9. Retention and F4 Handoff

A one-time `min(cursor.markDelete)` scan is insufficient because backward reset/create can race with trim。F3 adds one
stream-level `CursorRetentionRecord`：

```text
ownerSessionId = current writable-ledger owner

ACTIVE
  protectedFloorOffset
  lastCompletedTrimOffset

PROTECTION_PENDING
  complete create/recreate/backward-reset intent + attempt ID
  lowered/frozen protection floor

TRIM_PENDING
  frozen pending offset + attempt ID + exact composed L0 reason
```

Every new/recreated cursor and every backward reset first CASes ACTIVE -> PROTECTION_PENDING，even when the numeric
floor does not decrease；it sets `protectedFloorOffset=min(currentFloor,target)` and stores enough immutable intent to
finish after a crash。Floor raise and trim remain blocked until the cursor root carries the same attempt ID and the
retention root finalizes back to ACTIVE。A momentary version bump is insufficient because a new raise could start after
that bump but before cursor CAS；the pending lifecycle closes the full window。Raising the floor requires a stable full
cursor scan and CAS on an ACTIVE retention root。

Every F3 writable open creates or claims an ACTIVE retention owner root before publication，including an unactivated
topic with no cursor。With no projection marker，the only legal root shape is ACTIVE/no-pending with an empty cursor
scan；it fences earlier writable sessions but does not activate durable cursor semantics or block an F2 reader。First
durable create persists the topic marker，then writes PROTECTION_PENDING、the cursor attempt and the ACTIVE finalize
in order。A marker-preserving new stream incarnation safely bootstraps a fresh owner root，and either protection crash
cut is resumable。A cursor without marker/root or a marker-absent pending root fails closed。

Safe trim：

```text
ACTIVE -> CAS TRIM_PENDING(candidate,id,composedReason)
  -> idempotent F1 trim(candidate)
  -> verify L0 trim truth
  -> CAS the same TRIM_PENDING -> ACTIVE completed(candidate)
```

A crash/uncertain response leaves the corresponding pending lifecycle。Protection recovery completes/proves the exact
cursor attempt；trim recovery reissues the same offset with the exact persisted composed reason。An existing ACTIVE
record must equal L0's completed trim；trim pending recovery accepts only the prior completed offset or exactly its
frozen target，so an uncoordinated trim fails closed。

F3 broker admission still rejects policy retention/backlog eviction until F4。`TRIM_TOPIC` is rejected at the loaded
admin route before the facade call，while periodic `trimConsumedLedgersInBackground` remains a no-op。F4 is the first
phase allowed to admit that operation and route a policy/admin candidate carrying the current owner session through
the retention coordinator；it owns
physical GC and must consume：

- completed L0 trim truth；
- current cursor snapshot references/generations；
- F4 reader/reference grace；
- later KoP/Lakehouse reference domains。

It cannot delete from projection hints or object age alone。

## 10. Pulsar Compatibility Boundary

F3 fork admission allows durable/non-durable Exclusive、Failover、Shared and their valid nontransactional acks。
Shared cumulative ack、Key_Shared、transaction pending ack、replicated/read-compacted remain rejected before mutation。

The current F2 validator/admin boundaries are changed to allow：

- durable subscription create/recovery/delete；
- read-only analyze-backlog through a mark-delete-based non-durable duplicate；
- clear backlog、skip/expire、reset；
- TTL and subscription expiration。

Compaction/offload/truncate/migration、policy retention/backlog eviction remain denied。

`Consumer.messageAcked` must await persistence for every Nereus durable ack：

```text
requirePersistedAck || (nereusTopic && cursor.isDurable())
```

Counters、redelivery tracker and pending-state effects that imply success happen only after cursor future success。

## 11. Rollout

F3 reserves：

```text
nereus.cursor-protocol=1
```

The property is published only after cursor metadata/snapshot/runtime/retention recovery is ready and F2 storage
binding capability is ready。First durable cursor creation requires two stable all-persistent-broker capability
snapshots，then a monotonic CAS adds internal `nereus.cursor-protocol=1` to the F2 topic projection before cursor
creation。The locked F2 decoder rejects that reserved property during record construction，so it fails topic open
before exposing an empty cursor view；F3 filters/preserves the marker。Downgrade is unsupported from the activation
CAS onward，even if the initiating cursor/trim operation later fails before its callback。The marker-aware decoder、
M3 writable claim/hydration and M4 broker admission are one deployable release boundary；M1/M2 alone are not broker
artifacts。

## 12. Design Invariants

1. F1 stream head/commit/fence/visibility remains append truth。
2. F2 projection only maps committed facts to Pulsar coordinates。
3. One cursor root CAS owns all durable ack correctness fields。
4. Snapshot visibility comes only from current root reference。
5. Normal dispatch read position is not durable。
6. Mark-delete is first cumulatively unacknowledged offset。
7. Batch index never becomes a stream offset。
8. Ack state is never silently truncated。
9. Destructive reset/clear advances `ackStateEpoch`；monotonic ack never rebases across it。
10. Delete/recreate generation fences stale brokers。
11. Every writable open claims retention first and every ACTIVE root under one fresh owner session before publication；
    an earlier owner cannot mutate claimed roots，and the new owner never dispatches from a mixed-owner view；the
    Pulsar topic-ownership lifecycle separately stops the earlier dispatcher。
12. Broker ownership is checked before claim and immediately before publication/first-create callback；it is admission
    evidence，while root owner session/version remains the durable CAS fence。
13. New/recreated cursor and backward reset remain inside recoverable PROTECTION_PENDING through cursor CAS。
14. Redelivery is allowed；skipping unacked data is not。
15. F4 can replace bytes and reclaim objects without changing MessageId/cursor coordinates；topic-owned trim carries
    the current cursor owner session，while read-only planner/GC snapshots revalidate every observed root version/session。

## 13. Gate Result

F3-M0 and the narrow M0R gate passed on 2026-07-14 against the locked local Pulsar master source。The result is：

```text
Future 2's compatibility model can carry Future 3 and Future 4.
Future 3's durable protocol is implementation-ready.
F3-M1 metadata/snapshot foundation and its ordinary/real-service gates pass; M2-M6 remain pending.
```

Implementation continues through F3-M2 state machines、M3 facade、M4 broker integration、M5
real recovery and M6 final compatibility gate。See the detailed [implementation plan](../phase-3-cursor-subscription/06-implementation-plan-and-gates.md)。
