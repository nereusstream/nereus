# Cursor State Machines, Recovery, and Retention

## 1. State-machine Owners

| State | Durable owner | Local owner |
| --- | --- | --- |
| stream committed end / trim / lifecycle | F1 `StreamStorage` | F2 `StreamSnapshotTracker` cache |
| projection identity / virtual ledger | F2 projection root | `NereusManagedLedger` |
| writable cursor owner session | F3 retention + ACTIVE cursor roots | one writable `NereusManagedLedger` instance |
| cursor ack/properties/generation | F3 `CursorStateRecord` | hydrated `CursorHandle` cache |
| cursor protection / trim pending | F3 `CursorRetentionRecord` | `CursorRetentionCoordinator` |
| next dispatch read | none | `NereusManagedCursor.localReadOffset` |
| snapshot bytes | immutable object | decoded effective ack-state cache |

Every transition below preserves this ownership. Watchers, local lanes, metrics and projection state are not durable
commit authorities。

## 2. Ledger Open and Durable Cursor Hydration

### 2.1 Open sequence

`NereusManagedLedgerOpenCoordinator` extends the F2 open sequence：

```text
1. Invoke the broker-supplied ownership guard and require true for this writable open.
2. Inspect/validate F2 storage state and exact ManagedLedgerProjectionIdentity.
3. Read L0 StreamMetadata snapshot: lifecycle, trimOffset, committedEndOffset.
4. Read/validate the topic-projection cursor activation marker；an activated marker requires cursor runtime protocol 1.
5. Generate one fresh random CursorOwnerSession for this writable ledger instance.
6. Open cursor watch/invalidation epoch before cursor scan.
7. Apply the marker/root/cursor bootstrap matrix；absent-create an owner-only ACTIVE root when legal，otherwise
   CAS-claim the existing root's ownerSessionId while preserving lifecycle/pending/floors. This version change fences
   every old retention request.
8. Page-scan all cursor keys，bounded by cursorRecordsPerStreamMax；bounded-parallel CAS-claim every ACTIVE root to the
   same owner session，preserving semantic state. On condition loss reload/retry that key.
9. Recover any claimed retention pending lifecycle to a proved terminal state or fail open，then restart the scan.
10. Repeat scan/claim until every ACTIVE root has the new owner ID and the key/version fingerprint is stable；validate
   key/hash/exact name/projection/generation for every ACTIVE/tombstone record.
11. For every claimed ACTIVE root, load and strictly validate the referenced snapshot.
12. Build effective CursorState and NereusManagedCursor with localReadOffset=firstUnacked.
13. Repeat scan/hydration if invalidation epoch changed; require two equal key/version fingerprints when watch is
    unavailable.
14. Re-read retention and require the same owner session；reconcile a conservative floor when the root exists。Raising
    it is best-effort and never blocks safe open after the authoritative claimed scan has succeeded.
15. Invoke the ownership guard again；false/error fails fenced and publishes no ledger，although the claimed roots
    remain safe for the next owner to reclaim.
16. Publish NereusWritableLedgerOpenResult wrapping the base projection/L0 result plus owner session、the immutable
    durable-cursor collection and claimed retention view.
17. Only now invoke ManagedLedger open callback / allow PersistentTopic.initialize().
```

Pulsar topic ownership still controls which broker may initiate a writable open，but the F3 owner-session claim is the
durable cursor fence。An old mutation of an existing root either linearizes before that root is claimed and is included
in the claim/reload，or loses the changed root version/session。A protected CREATE/RECREATE whose target is still
ABSENT/DELETED is the explicit cross-key exception：if its pending intent linearized before retention claim，the old
target-key CAS may race takeover recovery，but it cannot finalize the now-claimed retention root or publish success；
the new owner recovers、claims and stabilizes the winner before callback。The exact broker-supplied ownership checker is required before claim and before publication；it is not inferred
from timeout or watch state。M4 also keeps stock graceful unload ordering：old topic close stops admission and drains accepted cursor lanes。
The new broker does not hydrate/dispatch until every ACTIVE root is claimed and stabilized。A watch is not a fence；M5
injects delayed old-owner retention/reset CAS at every claim cut。

### 2.2 Retention-root bootstrap

The stabilized open scan applies this exact matrix：

| Activation marker | Cursor records | Retention root | Result |
| --- | ---: | --- | --- |
| absent | none | absent | absent-create ACTIVE owner-only root at current L0 trim，then continue |
| absent | one or more | any | corruption；fail open |
| absent | none | present | require ACTIVE/no pending，validate and claim as preactivation owner root |
| present | none | absent | safe bootstrap for a marker-preserving new topic incarnation；create the root below |
| present | one or more | absent | corruption；a cursor root can only follow the retention barrier |
| present | any/none | present | validate identity/lifecycle/L0 relation and continue |

For either legal root-absent case，create absent-only：

```text
lifecycle = ACTIVE
ownerSessionId = current writable-open owner
protectedFloorOffset = current L0 trimOffset
lastCompletedTrimOffset = current L0 trimOffset
mutationSequence = 1
```

Starting at trim is conservative；later reconciliation may raise it。If any cursor record exists while the retention
root is missing or the marker is absent，open fails corruption；a marker-absent retention root is legal only in ACTIVE
with no pending intent and no cursor records。The implementation does not infer whether an old trim raced with
upgrade。A concurrent writable open may win the absent-only root put；the loser reloads，then must claim that winner to
its own session and stabilize before it may publish。

If an existing ACTIVE or PROTECTION_PENDING retention root's `lastCompletedTrimOffset` differs from current L0 trim，
open fails corruption。PROTECTION_PENDING recovery follows section 14.4 before any floor raise/trim or broker callback。
If the root is `TRIM_PENDING(P)`，L0 trim may equal either the prior completed offset or exactly P；a value greater than
P proves an uncoordinated trim and fails rather than being accepted as equivalent。

### 2.3 Hydration failures

| Failure | Open behavior |
| --- | --- |
| unknown cursor record/snapshot version | fail topic open |
| key/name/hash/projection mismatch | fail topic open |
| root points to missing/corrupt stable object | fail topic open |
| object read fails but root ref/version changed | retry new root |
| ACTIVE cursor below current L0 trim | fail corruption; never clamp durable ack truth |
| DELETED tombstone with old ref/properties | fail corruption |
| stable pending protection intent cannot be proved/applied | keep pending and fail topic open |
| ACTIVE root cannot be claimed/stabilized before deadline | fail topic open；never dispatch under a mixed owner set |
| scan exceeds count/page/deadline | fail topic open |
| metadata/watch transient failure | bounded retry, then fail open |

There is no “skip one bad subscription and open the topic” mode. Such a mode changes the durable subscriptions
visible to broker and can cause accidental delete/recreate or retention advancement。

## 3. Cursor Create / Existing Open

### 3.1 State diagram

```text
ABSENT --create generation 1--> ACTIVE(g=1)
ACTIVE(g=n) --open------------> ACTIVE(g=n), initial request ignored
ACTIVE(g=n) --delete----------> DELETED(g=n)
DELETED(g=n) --recreate-------> ACTIVE(g=n+1)
```

Tombstone is never converted back without generation increment。

### 3.2 Algorithm

```text
open(ownerSession, exactName, initial):
  require the ledger ownership guard before starting a durable create/open
  validate name/projection/owner and read current L0 bounds
  resolve initial markDelete target T

  loop until deadline:
    R = get cursor key
    if R is ACTIVE:
       verify exact identity; hydrate; return handle(R)

    if R is ABSENT or DELETED:
       read authoritative topic projection
       if cursor protocol marker is absent:
          activationGuard.acquireFirstActivationPermit(projection)
          CAS topic projection to cursor protocol 1
       else require the exact supported marker and identity
       reread retention; require it exists、is ACTIVE、matches current ownerSession and
           lastCompletedTrimOffset == current L0 trim
       I = immutable CREATE/RECREATE intent(
             random attemptId, exact name/hash, expected/target generation,
             T, initial properties)
       beginProtection(I): CAS ACTIVE -> PROTECTION_PENDING,
             protectedFloor=min(currentFloor,T)
       reread L0 trim and require T >= trim
       candidate = ACTIVE(
           ownerSessionId = ownerSession.id,
           generation = R absent ? 1 : R.generation + 1,
           sequence = R absent ? 1 : R.sequence + 1,
           ackStateEpoch = 1,
           createdAtMillis = operation nowMillis,
           updatedAtMillis = R absent ? nowMillis : max(R.updatedAtMillis, nowMillis),
           lastProtectionAttemptId = I.attemptId,
           markDelete = T,
           empty ack state,
           initial properties)
       absent-put or CAS tombstone
       require the authoritative cursor root proves I.attemptId/target generation
       CAS the same PROTECTION_PENDING(I) -> ACTIVE, preserving the lowered floor
       require the ledger ownership guard again
       only then return handle(candidate)
       on condition loss recover/finish the authoritative pending intent, then reload
```

`beginProtection(I)`：

- the topic activation marker and a valid retention root must already exist；
- retention must be ACTIVE and owned by the supplied writable session；
- the post-marker owner re-read above is mandatory；a stale first-create may activate the monotonic marker but cannot
  enter PROTECTION_PENDING after another writable open claimed the root；
- when an old session already entered PROTECTION_PENDING before a later owner claim，no cross-key atomic condition is
  assumed：its CREATE/RECREATE target CAS may race the new owner's recovery，but the old session cannot CAS the claimed
  pending root to ACTIVE or callback success；recovery reloads、claims/rebuilds the winning target before publication；
- ownership-guard failure after a durable transition suppresses callback/registration but never rolls metadata back；
  the next owner claims and hydrates the result；
- every absent/tombstone create freezes PROTECTION_PENDING before cursor create，even when the numeric floor does not
  decrease；the pending lifecycle，not a momentary version bump，closes the entire interval through cursor CAS；
- CREATE reads ACTIVE at V，performs a bounded cursor-key scan，then CASes pending only against the same V；it rejects
  when `cursorRecordsPerStreamMax` keys already exist。A concurrent create/pending/finalize changes V and forces the
  whole count scan to retry，so distinct-name creates cannot overshoot the hard count；RECREATE reuses its tombstone
  slot；
- PROTECTION_PENDING and TRIM_PENDING both block another new/recreated cursor and floor raise/trim；a caller may help
  finish the existing pending intent before retrying；
- callback success requires both the cursor root attempt proof and pending -> ACTIVE CAS；a lost callback is retried by
  matching generation + `lastProtectionAttemptId`；
- a failed/lost client leaves a recoverable intent and lower floor，not an untracked gap。Recovery completes it and
  later reconciliation may raise the conservative floor。

If two callers under the same claimed owner create the same missing name，one protection intent wins；the loser
helps/observes that exact intent and then opens the cursor，ignoring its own initial request，matching stock
open-idempotence。If two writable opens race，only the session that completes retention + all ACTIVE-root claims may
publish a ledger；the other fails fenced。Different exact names with a theoretical same hash fail collision rather than
sharing one key。

## 4. Per-cursor Mutation Template

All durable transitions use：

```text
1. Copy/validate request at facade ingress.
2. Submit to local CursorMutationLane.
3. Read authoritative root and require ACTIVE + same exact identity/generation + handle ownerSessionId；a different
   session fails fenced and is never CAS-rebased.
4. Hydrate effective ack state (snapshot + inline delta).
5. Read/revalidate L0 bounds needed by this operation.
6. Read retention state when the operation can race a targeted PROTECTION_PENDING；a non-owner operation on that
   cursor fails/retries busy。An operation already in flight may still race one CAS，which pending recovery handles.
7. Pure CursorStateMachine.apply(current, request) -> normalized candidate，preserving
   `lastProtectionAttemptId` unless this is the owning protected transition，and preserving `ackStateEpoch` unless
   reset/clear-backlog destructively replaces the ack state.
8. CursorStatePersistencePlanner chooses inline root or replacement full snapshot.
9. If snapshot needed, PUT + HEAD immutable object before root CAS.
10. CAS one CursorStateRecord against the read Oxia version，preserving ownerSessionId.
11. On success publish local state, update local read offset if the operation requires it, then callback exactly once；
    protected create/backward-reset first proves the attempt and finalizes retention ACTIVE，so cursor CAS alone is not
    its callback boundary.
12. On CAS loss follow the operation-specific retry rule; never callback from stale candidate.
```

Pure state-machine functions do not perform IO, read clocks, invoke callbacks or mutate inputs。Timestamps are passed
as explicit operation inputs so deterministic tests can compare exact candidate records。They clamp replacement time
to `max(previous.updatedAtMillis, nowMillis)` and use checked increment for every sequence/epoch/generation。

## 5. Cumulative Acknowledgement

### 5.1 Whole Entry

Input whole Entry offset `o`：

```text
require trim <= o < committedEnd

if o < current.markDeleteOffset:
    ALREADY_APPLIED
else:
    markDelete = o + 1
    remove ranges whose end <= markDelete
    clip/remove range portions below markDelete
    remove partial offsets < markDelete
    while first range starts == markDelete:
        markDelete = first.end
        remove first range
        remove partial offsets < markDelete
    persist supplied mark-delete position properties
```

A whole cumulative ack supersedes every earlier hole/partial state. It cannot move backward。

### 5.2 Partial Batch

Input offset `o`, exact `batchSize`, request remaining words `W`：

```text
require trim <= o < committedEnd
require entry metadata batchSize exactly matches request

if o < current.markDeleteOffset:
    ALREADY_APPLIED
else:
    markDelete = o
    discard ranges/partials below o

    if whole range covers o:
        target is already whole-acked
    else:
        existing = effective partial[o] if present else all-ones(batchSize)
        merged = existing AND W
        if merged empty:
            add whole [o,o+1)
        else if merged is exact all-ones(batchSize):
            remove partial[o]
        else:
            partial[o] = canonical merged

    normalize; whole target may advance markDelete to o+1 and fold following ranges
    persist supplied mark-delete position properties
```

`all-ones(batchSize)` masks the final word above batch size。The request words mean resulting remaining state from
the client's cumulative batch acknowledgement；AND makes retries and duplicate delivery idempotent。

### 5.3 CAS conflict

Cumulative ack is monotonic. On condition loss the lane reloads, and：

- if latest state already cumulatively covers the request/has an equal-or-smaller remaining set at target, return
  `ALREADY_APPLIED` after validating generation；
- otherwise recompute against latest state only when its `ackStateEpoch` equals the epoch captured by the request；
- if a concurrent destructive reset/clear advanced that epoch and subsumption cannot be proven, fail conflict instead
  of moving from an ambiguous base。

The implementation records mutation kind/target and observed epoch in the local pending operation。Subsumption is
based on current ack truth and same cursor generation；`ackStateEpoch` is the durable destructive-history discriminator，
not an operation ID or second CAS token。

## 6. Individual Acknowledgement

### 6.1 Canonical request batch

Before IO：

1. cap count at `cursorAckPositionsPerRequestMax`；
2. validate all positions refer to the same F2 projection；
3. sort by offset；
4. merge duplicate whole/partial requests：whole wins；partials require same batchSize and AND words；
5. decode each distinct partial Entry exactly once；
6. produce immutable offset requests。

The root CAS applies the entire request list atomically。

### 6.2 State transition

For each request ascending：

```text
if offset < markDelete or covered by whole range:
    continue

if whole:
    remove partial[offset]
    union [offset,offset+1) into whole ranges
else:
    existing = partial[offset] or all-ones(batchSize)
    require batchSize equal
    merged = existing AND request.remaining
    if merged empty:
        remove partial[offset]
        union whole range
    else if merged is exact all-ones(batchSize):
        remove partial[offset]
    else:
        partial[offset] = merged

after all requests:
    merge adjacent whole ranges
    fold ranges starting at markDelete
    remove partials below/covered by resulting whole state
```

Individual acknowledgement does not change mark-delete position properties。The planner may publish a new full
snapshot as part of this same logical CAS。

### 6.3 Retry and response loss

If root CAS succeeded but broker/client response was lost, retry requests are already subsumed and succeed without a
second semantic change。If another broker concurrently acked other offsets without changing `ackStateEpoch`，
recomputation unions both sets。A changed epoch requires subsumption or conflict。This is the only allowed automatic
CAS rebase for Shared subscriptions。

## 7. Read, Wait, and Replay State

### 7.1 First unacked function

```text
firstUnacked(state):
  p = state.markDeleteOffset
  for ranges ordered:
    if range.start > p: return p
    if range.start == p: p = range.end
  return p
```

Partial map never causes skipping of an entire offset。The function is pure and used at open、rewind、post-reset and
when a mark-delete overtakes local read。

### 7.2 Normal dispatch

```text
read candidate = max(localReadOffset, current trimOffset)
skip whole-acked offsets
read committed Entry bytes from F2
return partial remaining words separately to dispatcher
advance localReadOffset after accepted read result
```

Only a monotonic remote ack or property-only root change may coexist with a legally active dispatcher；an older local
view of such an ack can cause duplicate dispatch，which Pulsar permits，but cannot skip newly unacknowledged data。
Reset、clear-backlog、delete and recreate are admitted only on the fenced topic owner and serialize with that owner's
cursor read/mutation lane；ownership transfer stops new admission on the old topic，while the fresh F3 root claims
durably order any already accepted destructive CAS before the new dispatcher hydrates。Graceful lane drain helps the
normal unload path but is not the crash fence。A watch is only an optimization and cannot establish this ordering。If a local read path
observes a generation/lifecycle/`ackStateEpoch` change，it blocks whole-ack skipping until authoritative reload；it
never treats a pre-destructive cache as a safe filter。Before skipping an Entry solely due to a remotely observed
whole ack，the cache must be from a strictly decoded root/version。

Tail wait uses F2 committed-end notification/polling. Cursor metadata watch does not replace stream tail watch。

### 7.3 Replay

For each requested Position：

- foreign projection、trimmed or whole-acked -> returned in skipped set；
- future -> invalid/skipped according to locked method behavior, never read；
- partial -> read Entry and expose remaining words；
- otherwise -> read Entry through F1 resolver。

Replay does not change local read or durable state。

## 8. Local Seek and Rewind

```text
rewind():
  localReadOffset = firstUnacked(current effective state)

seek(target, force=false):
  validate projection and direct target
  localReadOffset = max(target, firstUnacked(state), trimOffset)

seek(target, force=true):
  validate projection
  require target >= trimOffset before F4 compacted support
  localReadOffset = target
```

These operations are synchronized with local reads through the cursor lane/local read lock but perform no metadata
CAS。A broker restart discards them。

## 9. Durable Reset

### 9.1 Forward and backward reset

Reset target `T` is the next Entry to read。The new durable mark-delete coordinate is therefore `T`，equivalent to
stock `newMarkDeletePosition = previous(newReadPosition)` at the Position API boundary。
Before this state machine，non-force EARLIEST/LATEST/trimmed/future inputs normalize to current trim/end exactly as in
document 02；force accepts only the retained/tail range until F4 provides an explicit compacted view。Thus every
request reaching the root transition has `trimOffset <= T <= committedEndOffset`。

```text
candidate:
  markDeleteOffset = T
  wholeAckRanges = empty
  partialBatchAcks = optional validated target ack set
  snapshotReference = empty
  inline deltas = empty
  positionProperties = empty
  cursorProperties = preserved
  ackStateEpoch = previous.ackStateEpoch + 1
  lastProtectionAttemptId = preserved for forward reset；pending intent ID for backward reset
```

If target partial words are empty, target is whole-acked and normalization advances mark-delete to `T+1`。

A forward/same-offset reset captures the root's `ackStateEpoch`。On CAS loss or uncertain completion it accepts an
already-applied result only when the latest root has `capturedEpoch+1` and exactly the requested mark-delete、empty
holes/ref、target partial and empty position properties；cursor-property-only changes after that result are preserved
and ignored by this proof。Otherwise it may rebuild from the latest root only when the epoch is still the captured
value，preserving the latest cursor properties while replacing ack/position state。Any other epoch means another
reset/clear won or the outcome was superseded and maps to `ConcurrentFindCursorPositionException`。Thus a reset can
serialize after an in-flight monotonic ack/property CAS but never blindly overwrite another destructive transition。

### 9.2 Backward protection protocol

If `T < current.markDeleteOffset`, the reset is backward and always publishes a recoverable protection intent，even
when `T >= current protectedFloorOffset`：

```text
1. Copy/validate the full target，including optional partial batch；encode the intent and enforce its cap.
2. Read retention root；require ACTIVE，T >= L0 trim and no completed trim beyond T.
3. CAS ACTIVE -> PROTECTION_PENDING(BACKWARD_RESET,I)，setting
   protectedFloor=min(currentFloor,T) even when the numeric value is unchanged.
4. Reread L0 trim；require T still retained.
5. Reread the latest cursor root；require ACTIVE and I.expected generation.
6. Build the destructive reset from that latest root，preserve latest cursor properties，clear ack/ref/position
   properties，checked-increment latest `ackStateEpoch` and set lastProtectionAttemptId=I.attemptId.
7. CAS the cursor root。An already-in-flight ack/property CAS may win first；reload and reapply the same durable intent
   until its attempt proof is present or the operation deadline expires.
8. Require authoritative cursor root has the target generation and I.attemptId.
9. CAS the same PROTECTION_PENDING(I) -> ACTIVE，preserving the lower floor.
10. Only then update local read state and complete the reset callback.
```

If a delete tombstone wins before step 7，the stale reset CAS cannot succeed；recovery finalizes the pending root with
the conservative lower floor and returns `CursorAlreadyClosedException`。A second reset/create that cannot acquire
ACTIVE maps to `ConcurrentFindCursorPositionException` / subscription-busy。Once step 3 succeeds，ordinary CAS loss is
not a blind destructive rebase：the complete reset intent is durable，same-generation operations serialize before it，
and recovery must finish exactly that intent。A deadline may therefore report an uncertain reset while leaving
PROTECTION_PENDING for startup recovery；it never clears the barrier。

While retention is either pending lifecycle，another backward reset fails busy。A forward reset/ack whose target
remains at or above the current cursor mark-delete may proceed unless it targets the cursor currently protected by an
intent；then it waits/fails busy so it cannot starve recovery。

### 9.3 Force reset

F3 has no compacted object view。Therefore `forceReset=true` only bypasses stock logical mark-delete clamping；it does
not bypass physical retention。`T < L0 trimOffset` fails。F4 may later route such reads to compacted bytes only by
adding an explicit generation/reference contract; it cannot change the durable cursor coordinate model。

### 9.4 Local completion

After root CAS：

```text
localReadOffset = firstUnacked(new state)
cancel/fail prior pending read waiter
publish reset callback on callback executor
```

No local position changes before CAS success。

## 10. Clear Backlog and Skip

### 10.1 Clear backlog

```text
1. Read one L0 committedEndOffset E.
2. Apply durable reset-like forward transition:
     markDelete=E, no holes/ref, preserve cursor properties,
     clear mark-delete position properties, ackStateEpoch=previous+1.
3. CAS root；on loss/uncertain completion accept a result only when the latest root has captured epoch + 1 and exact
   clear target/empty ack-ref-position-property state（later cursor-property-only changes are preserved）；otherwise
   recompute only when latest end snapshot is still E and the epoch is still the captured value；else fail conflict.
4. localReadOffset=max(localReadOffset,E).
```

An append after step 1 has offset `>= E` and remains backlog。Clear does not trim stream bytes or delete snapshot
objects synchronously。

### 10.2 Skip entries

`skipEntries(n, mode)`：

```text
1. Start from current markDeleteOffset/first unacked.
2. Resolve retained committed offsets through F1.
3. Include mode counts whole individually acked entries; Exclude mode skips them while finding N.
4. Find the last position covered by N.
5. Perform a whole cumulative ack through that offset.
```

This is durable and serialized with ack operations。A scan that reaches committed end skips only available entries；
tests lock exact stock-compatible callback behavior at end-of-ledger。

## 11. Cursor Property Transitions

Cursor-property operations read the latest root and CAS one replacement value：

```text
PUT(k,v):        map[k]=v; the locked single-key API allows external or internal-prefix keys
REMOVE(k):       absent is idempotent; the locked single-key API also allows internal-prefix keys
SET_EXTERNAL(M): preserve current #pulsar.internal.*; replace all external keys with M
```

Public `setCursorProperties` rejects any requested internal key。Concurrent replacement never auto-merges because the
call means replace；CAS loss maps to `BadVersionException` unless exact map already exists。

Position-property staging is attached atomically to the next cumulative mark-delete or explicit flush。Individual
acks do not consume it。Close-time flush is a root CAS with no ack change and increments mutation sequence。

## 12. Delete and Recreate

### 12.1 Delete

This state machine is entered only for a durable cursor name。At the ManagedLedger facade，a registered non-durable
cursor with that name is atomically removed/closed locally and returns success without `CursorStorage.delete`、Oxia
or retention reconciliation。The shared local name registry does not permit durable/non-durable aliasing。

```text
delete(name):
  missing -> success after stabilized exact-key absence; no owner claim/write
  DELETED -> success after exact identity/lifecycle validation; no owner claim/write
  ACTIVE -> require current owner session, then CAS same generation to DELETED:
      clear snapshot ref, inline state and all properties
      keep projection/name/hash/generation/ackStateEpoch/lastProtectionAttemptId/final markDelete
      sequence += 1; updatedAt=max(previous.updatedAt,now); deletedAt=updatedAt
  after CAS:
      invalidate/close local handle
      schedule retention-floor reconciliation
      record old snapshot as unreferenced metric/handoff
```

Delete callback occurs after tombstone CAS, not after eventual snapshot GC。A concurrent ack that loses to delete sees
DELETED and fails closed；it cannot reopen the cursor。

### 12.2 Recreate

Recreate runs the create protocol, including lowering protection before its initial position and CASing tombstone to
`generation+1`。All old snapshot IDs and stale handles remain tied to prior generation。

### 12.3 Topic delete/recreate

F2 topic projection creates a new stream/incarnation。F3 does not write a tombstone for every cursor during topic
delete；old cursor prefix and snapshots remain tied to old stream ID for F4 reclamation。A recreated topic cannot
resolve those keys because its F2 projection identity differs。

## 13. Snapshot Replacement State Machine

```text
INLINE(root V)
  --threshold exceeded--> UPLOAD(snapshot S)
  --put/head success-----> CAS root V -> REF(S, empty delta)
  --CAS loss-------------> ORPHAN(S), reload/recompute

REF(S, delta)
  --small mutation-------> CAS same ref + updated bounded delta
  --threshold exceeded--> hydrate S+delta, UPLOAD full S2, CAS ref S2 + empty delta
  --reset/delete---------> CAS no ref; S becomes unreferenced
```

Root values never point to partially uploaded objects。Snapshot upload does not lock ack mutation on another broker；
version-CAS selects exactly one result。Snapshot size over hard cap fails the originating mutation before root CAS；
the implementation never truncates ranges or partials。

## 14. Retention Coordinator

### 14.1 Protection invariant

At all stable and crash-intermediate states：

```text
retention.ownerSessionId == writable ledger owner for retention/protection/trim operations and at open publication
cursor.ownerSessionId == handle owner for every ordinary cursor mutation
L0 trimOffset <= retention.protectedFloorOffset
retention.protectedFloorOffset <= every ACTIVE cursor markDeleteOffset
```

The second relation may be temporarily stricter because PROTECTION_PENDING lowers the floor before changing/creating
a cursor。For a pending create，the additional invariant is `protectedFloorOffset <= intent.targetMarkDeleteOffset`。
Neither relation may be violated in the unsafe direction。

### 14.2 Raise reconciliation

Only the reconciliation protocol in document 03 raises the floor。In compact form：

```text
read retention V ACTIVE owned by current CursorOwnerSession
stable bounded scan/hydrate all ACTIVE cursors
candidate = min markDelete, or committedEnd when none
reread retention V
CAS floor=max(currentFloor,candidate) only if V unchanged
```

A create/backward operation CASes ACTIVE -> PROTECTION_PENDING before cursor CAS，even if the floor value remains
unchanged。A raise can win before that transition or lose its ACTIVE/version CAS；after pending is visible，no raise or
trim can start until the cursor root proves the same attempt and retention returns to ACTIVE。This persistent
seqlock-like interval，rather than a one-shot version bump，closes the post-barrier/pre-cursor window without a
multi-key transaction。

### 14.3 Safe logical trim

F3 implements a safe coordinator API for F4/policy callers, but F3 broker admission keeps Pulsar retention/backlog
eviction disabled until F4 owns physical reclamation/accounting。

```text
requestTrim(candidate, reason):
  require retention/root operations use the current CursorOwnerSession
  require the owner-only/activated retention root already exists and is owned by that session
  validate candidate >= 0 and caller reason nonblank/no-NUL/strict-UTF-8；require it can fit after the fixed prefix,
      32-hex attempt ID and colon within cursorTrimReasonMaxUtf8Bytes
  if marker absent:
      acquire activation permit; CAS marker
      reread projection + retention and require exact identity/current owner
  if root absent or cursor exists without marker/root: fail corruption/fenced and require fresh writable open
  require retention ACTIVE
  if candidate <= lastCompletedTrim: return idempotent success with the current view
  require lastCompletedTrim < candidate <= protectedFloor
  require candidate <= current committedEnd
  generate random 128-bit lowercase-hex trimAttemptId
  composedReason = "nereus-cursor-retention/" + attemptId + ":" + reason
  validate the complete composedReason within cursorTrimReasonMaxUtf8Bytes
  CAS ACTIVE -> TRIM_PENDING(
      pendingOffset=candidate,
      protectedFloor=candidate,
      attemptId,
      composedReason)
  call StreamStorage.trim(streamId, candidate,
      new TrimOptions(cursorMetadataOperationTimeout, composedReason))
  verify L0 StreamMetadata.trimOffset == candidate
  CAS same TRIM_PENDING -> ACTIVE(
      lastCompletedTrim=candidate,
      protectedFloor=candidate,
      clear pending)
  schedule raise reconciliation
```

The existing L0 trim is monotonic/idempotent by stream+offset；F3 attempt ID is for recovery/observability and does not
change the L0 API。

The activation lines are required for the future F4 caller even when a topic has never had a cursor。The owner root
already serializes policy trim with concurrent first-cursor activation；the marker then establishes the minimum-reader
fence before TRIM_PENDING。No F3 broker path calls this method，so F3 alone does not activate a cursorless topic merely
because housekeeping runs。

### 14.4 Pending recovery

On open or coordinator startup：

```text
claim retention ownerSessionId first；claim any existing ACTIVE target cursor before applying/proving the intent

if retention PROTECTION_PENDING(I):
  block floor raise、trim and another protected transition
  read exact cursor key from I
  if cursor root has targetGeneration + lastProtectionAttemptId=I.id:
      CAS-claim it to the current retention owner when its prior owner differs，then reload
      transition is proved (later ack/property changes may already be present)
  else if I is CREATE and cursor is absent:
      absent-put the complete create candidate from I
  else if I is RECREATE and cursor is the expected DELETED generation:
      CAS the complete recreate candidate from I
  else if I is BACKWARD_RESET and cursor is ACTIVE at expected generation:
      rebuild reset from latest root and CAS with I.id
  else if I is BACKWARD_RESET and cursor is DELETED at expected generation:
      treat delete as winner；do not resurrect
  else:
      fail corruption/conflict and keep pending
  on any target-key condition loss，reload and restart this target branch
  reread cursor and require current owner + proof，except the proved delete-winner case
  CAS exact PROTECTION_PENDING(I) -> ACTIVE，keep conservative floor

if retention TRIM_PENDING(P,id):
  block new cursor creation and every backward reset
  validate and retain the exact persisted composedReason bound to id
  read L0 trim
  if trim == prior lastCompleted: reissue exactly trim(P) with the persisted composedReason
  if trim == P: CAS pending -> ACTIVE completed=P
  if trim has any other value: fail corruption; do not clear pending
```

For CREATE/RECREATE，the intent carries exact initial maps and generation；for BACKWARD_RESET it carries the target and
optional partial state while current cursor properties are preserved from the latest root。Every applied protected
cursor candidate writes the current claimed `ownerSessionId` and `lastProtectionAttemptId=I.id`，which remains through
later ack/property/delete CAS and is the
recovery proof。Timeout、broker crash、executor rejection or uncertain response leaves the corresponding pending
lifecycle。`TRIM_PENDING` retains the exact composed L0 reason as well as offset/attempt ID，so recovery does not
manufacture a different audit identity。No path speculatively clears it on an error。An operator can observe/retry
recovery；a manual unsafe override is outside F3 API。

### 14.5 F4 handoff

F4 may reclaim/materialize only from：

- completed L0 trim truth；
- a bounded retention/current-ACTIVE-root scan whose exact versions/session IDs are stable and revalidated at the
  F4 action boundary；an owner change invalidates/retries the snapshot；
- current ACTIVE cursor snapshot references/generations from that scan；
- F4's own object read/reference grace；
- other future reference domains such as KoP/Lakehouse。

It must not treat `protectedFloorOffset` alone as proof that physical bytes are unreferenced。A read-only F4
planner/GC worker does not claim cursor ownership；it consumes the versioned `CursorMetadataStore` read/scan surface，
not owner-scoped `CursorStorage.retentionView`。Only topic-owned `requestTrim` carries the current owner session。

## 15. Broker Failover and Cross-broker Races

### 15.1 Failover

```text
old broker dispatches entries, localReadOffset ahead, no ack
  -> crash
new broker claims retention + every ACTIVE cursor root under fresh ownerSessionId
  -> an old existing-root CAS is observed-before that root's claim or loses version/session
  -> an already-pending CREATE/RECREATE target CAS may race, but takeover recovery owns finalize and claims the winner
new broker hydrates the claimed durable ack root
  -> starts firstUnacked
  -> messages may redeliver, none are skipped
```

If old ack CAS succeeded before crash, new broker observes it。If CAS did not succeed, message redelivers。If response
was lost, client duplicate ack is idempotent。An old destructive CAS cannot land after its cursor root claim；a pending
retention CAS cannot land after the retention claim。The new owner never dispatches during the mixed-claim interval。

### 15.2 Concurrent Shared ack clients under one owner

- disjoint individual acks through two storage clients carrying the same claimed owner session: one wins, loser
  reloads and unions both；
- same partial ack: AND is idempotent/commutative；
- cumulative vs individual: winner establishes latest truth, loser recomputes only if its monotonic request is still
  meaningful；
- reset vs ack: once the protection intent is durable，an in-flight ack may serialize before it；reset recovery then
  applies the exact intent，while later non-owner mutations observe busy；
- delete vs any mutation: tombstone winner closes old generation；
- recreate vs stale old mutation: generation mismatch rejects stale candidate。

A client carrying another owner session fails fenced before any of these CAS-rebase rules。

## 16. Failure Matrix

| Failure point | Durable outcome | Client/broker outcome | Recovery |
| --- | --- | --- | --- |
| batch Entry read/parse fails | unchanged | ack fails | retry after source repair |
| snapshot PUT fails | unchanged | mutation fails | retry mutation |
| snapshot PUT succeeds, HEAD fails | root unchanged; possible orphan | mutation fails/uncertain object only | root remains authority |
| snapshot upload succeeds, root CAS loses | winner root only; uploaded orphan | reload/recompute | F4 reclaims orphan |
| monotonic ack root CAS succeeds, callback process crashes | ack durable | response lost | duplicate ack proves ALREADY_APPLIED；destructive retry follows exact-result/epoch rules |
| watch event lost | root correct, local cache stale | possible redelivery | mutation GET/CAS and polling refresh |
| old mutation reaches Oxia while new writable open claims roots | one total order per root | new open remains hidden | old CAS is observed before claim or loses changed version/session |
| old first-create resumes after an empty-topic new owner claim | owner-only retention version/session changed；marker CAS may already be durable | old create fails fenced before pending/cursor write | marker-only state is safe；current owner alone may begin protection |
| old protected CREATE/RECREATE resumes after new owner claims its pending retention root | target cursor CAS may still win because no cross-key atomic condition is assumed；old pending finalize cannot win | old operation is fenced and cannot callback success；new open remains hidden | new owner reloads/rebuilds and claims the target winner，proves the attempt，then alone finalizes ACTIVE before rescan/publication |
| ownership checker becomes false/errors after root claims | roots may carry unpublished session | ledger callback fails fenced | next legitimate open claims the roots under a fresh session |
| new open crashes after retention claim or a subset of cursor claims | mixed owner IDs, no published ledger | topic open fails/absent | next fresh owner reclaims retention + every ACTIVE root and restabilizes |
| stable referenced snapshot missing/corrupt | root references invalid bytes | topic/cursor open fails | restore exact object or operator repair; no fallback |
| close races accepted mutation | root determined by CAS | one terminal callback per op | late result cannot reopen handle |
| protection pending CAS succeeds, crash before cursor CAS | lower floor + durable complete intent | create/reset response uncertain | startup applies exact intent; no raise/trim |
| protected cursor CAS succeeds, crash before finalize | cursor root carries attempt ID + pending root | response uncertain | prove attempt, CAS pending -> ACTIVE |
| in-flight ack wins reset cursor CAS | ack durable + reset still pending | ack may succeed; reset waits | rebuild exact reset from latest same generation |
| delete wins pending backward reset | tombstone + conservative pending floor | reset closes/fails | prove delete version, finalize without resurrection |
| trim pending then crash before L0 call | pending barrier | backward ops blocked | reissue same offset |
| L0 trim succeeds then broker crashes before completion CAS | L0 advanced, pending remains | no unsafe reset | observe L0, finish pending |
| cursor delete succeeds before snapshot cleanup | tombstone visible, old object unreferenced | delete succeeds | F4 cleanup later |
| protection-intent/metadata/snapshot limit exceeded before pending/root CAS | unchanged | mutation fails explicitly | operator changes workload/limit |

## 17. Required Deterministic State-machine Tests

Pure tests must cover at least：

- range union、adjacency fold、mark-delete chain fold and partial/whole precedence；
- all three ack-set shapes and exact final-word mask；
- duplicate/reordered individual request canonicalization；
- cumulative partial at mark-delete、ahead of mark-delete、already below mark-delete；
- monotonic ack CAS rebase at unchanged `ackStateEpoch`，and reset/clear epoch change causing subsumption-or-conflict；
- snapshot base + inline override + whole-range win + newer root mark-delete clipping；
- reset with/without target partial words and property preservation/clearing；
- clear backlog with concurrent append end snapshot；
- delete/recreate/stale generation；
- callback success ordering after CAS；monotonic response-loss retry versus destructive exact-result/epoch conflict；
- owner-session claim at every retention/ACTIVE-root CAS cut，including old forward/backward reset delayed until before
  and after claim；no mixed-owner open callback，existing-root stale CAS is fenced，and protected CREATE/RECREATE
  target-key races can never finalize/callback under the stale session and are claimed by recovery；
- unactivated empty-topic owner-root absent-create/claim races，including a delayed old-session first-create after the
  new owner publishes；a stale marker CAS may succeed idempotently，but pending/cursor roots remain absent until the
  current owner proceeds；
- every crash cut in protection-pending create/backward-reset and trim-pending protocols；
- post-protection/pre-cursor floor-raise attempt is blocked，including unchanged numeric floor；
- pending attempt proof survives ack/property/delete and finalizes exactly once；
- same-owner two-client disjoint/shared partial ack interleavings；
- no persisted read-position field and restart redelivery from first unacked。
