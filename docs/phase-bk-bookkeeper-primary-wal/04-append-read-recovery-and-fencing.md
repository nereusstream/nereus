# Append, Read, Recovery, and Fencing

## 1. Common logical contract

All three profiles use the same logical commit protocol：

```text
primary bytes durable
  -> immutable generic commit intent
  -> durable physical-range protection
  -> current session/provider revalidation
  -> stream-head CAS                         linearization / visibility
  -> stable AppendResult
  -> generation-zero index publish/repair
  -> optional higher-generation completion
```

BookKeeper changes the primary target and provider lifecycle only. It does not change offset allocation、stream head、
F2 virtual positions、F3 cursor state or F4 generation selection.

## 2. Append state machine

### 2.1 Admission before IO

The serialized per-stream append lane performs, in order：

1. resolve durable stream `StorageProfile`、configuration and exclusive ledger-id namespace binding；
2. require profile/namespace capability for the exact broker/readiness epoch；
3. require current append session and expected start offset；
4. run F4 lag admission for async/sync profiles；BK_ONLY has no materialization lag gate；
5. compute exact payload physical bytes and validate record/entry/byte/append-range limits plus mandatory protection
   capacity so the entire batch fits one permitted ledger；
6. acquire global/per-stream memory + request permits；
7. only then prepare/allocate/reserve/write BookKeeper。

Unsupported profile、capability drift、invalid config/batch or lag rejection produces zero BookKeeper calls. Admission
is revalidated immediately before provider write and before head CAS; policy is not cached across appends.

### 2.2 `BOOKKEEPER_WAL_ONLY` normal append

```text
A. prepare
   exact AppendEntry bytes -> retained buffers
   checked sum exact physical bytes
   compute NBKR1 range checksum
   select current ACTIVE segment or run allocation protocol
   rollover first if full batch would cross configured boundary

B. reserve
   create BookKeeperAppendReservation
   writer-state CAS reserves [firstEntryId, firstEntryId + entryCount)
   and advances durable activePhysicalBytes/activeAppendRangeCount (old range count is ledgerRangeSlot)
   create/reload fixed mandatory protection slots 0..2 as RESERVED before provider write

C. persist
   WriteAdvHandle.writeAsync(firstEntryId + i, entry[i])
   await every ack-quorum durable result under one monotonic deadline
   reservation -> DURABLE

D. prepare logical commit
   create BookKeeperEntryRangeReadTarget(alias, ledger, first, count, mapping, checksum)
   metadata.prepareOrReuseStableAppend(generic target)
   create/reuse REACHABLE_APPEND protection bound to exact commit key/version/SHA

E. commit
   BookKeeper root/range/session/protection exact reload
   current append-session validation
   stream-head CAS
   reload exact commit marker/head on response loss

F. complete
   build stable AppendResult from reachable committed facts
   publish generation-zero BK index synchronously or through the configured repair boundary
   clear reservation from writer state
   ack according to AppendAckBoundary
```

The minimum `WAL_DURABLE` success cut is after E：quorum-durable bytes、recoverable target/intent/protection and a
reachable head. It is never after C alone. If a caller/profile requires generation-zero confirmation, F must also
reload the exact index/protection before success.

### 2.3 Write ordering and buffers

V1 initially sets `maxWritesInFlight=1` for a ledger. Entry ids are explicit and consecutive. A later bounded pipeline
is legal only if it preserves these rules：

- no next append reservation until all entries in the current reservation are terminal；
- every successfully or ambiguously transmitted entry makes a non-complete reservation taint the ledger；
- the provider future does not complete durable until every entry result is successful；
- each retained/derived `ByteBuf` is released exactly once after BookKeeper has stopped using it；
- cancellation does not imply the RPC did not reach BookKeeper。

`DEFERRED_SYNC` is rejected at configuration/runtime creation. Normal `writeAsync` completion is the V1 physical
durability predicate.

## 3. Rollover

### 3.1 Trigger

Before reservation, checked arithmetic evaluates：

```text
nextEntryId + batch.entryCount > maxEntriesPerLedger
activePhysicalBytes + sum(exact entry payload lengths) > maxBytesPerLedger
activeAppendRangeCount + 1 > maxAppendRangesPerLedger
now - openedAt >= maxLedgerAge
BookKeeper/Nereus hard entry or ledger byte bound would be exceeded
```

If any condition is true and the current ledger is nonempty, seal it before allocating the next. If the batch itself
exceeds one configured ledger/entry/physical-byte bound, reject before IO; V1 does not split an append batch across
ledgers. Logical/cumulative Pulsar size never substitutes for the physical-byte counter.

### 3.2 Rollover sequence

```text
require no active reservation
CAS root ACTIVE -> SEALING(reason=ROLLOVER)
CAS writer ACTIVE -> RECOVERING
close/recovery-open and verify closed metadata
CAS root -> SEALED(lastEntryId,length)
CAS writer -> IDLE(nextSegmentSequence preserved)
allocate next ledger -> ACTIVE
continue original append admission revalidation
```

Rollover itself never advances logical offset. A timeout before new reservation is `KNOWN_NOT_COMMITTED` for the
append. A crash is resumed from writer/root lifecycle; it never reopens a SEALING ledger for writes.

## 4. Exact read state machine

### 4.1 Generation-zero BK read

```text
ReadResolver
  -> exact COMMITTED generation index or reachable-commit repair
  -> ResolvedRange(logical range, generation=0, commitVersion, BK target)
  -> claim one fixed whole-ledger reader slot below maxReaderLeasesPerLedger
  -> validate root identity/lifecycle/config binding
  -> open ledger withRecovery(false)
  -> readUnconfirmedAsync(firstEntryId, lastEntryIdInclusive)
  -> require exact count/consecutive ids
  -> NBKR1 SHA-256 over full target
  -> clip logical entries to ReadOptions
  -> provider-neutral batches/stats
  -> revalidate lease/root/generation pin
  -> return; close entries/buffers; release local lease ref
```

Readable root lifecycles are ACTIVE、SEALING、SEALED and an already-pinned MARKED root. A new pin is denied after MARKED.
DELETING/DELETED/ABORTED/QUARANTINED never return bytes.

### 4.2 Entry and offset validation

For target `T` and resolved logical range `R`：

```text
T.entryCount == R.recordCount
physicalEntry(offset) = T.firstEntryId + offset - R.startOffset
logicalOffset(entryId) = R.startOffset + entryId - T.firstEntryId
```

The reader uses checked arithmetic and rejects any entry id outside
`[firstEntryId,lastEntryIdInclusive]` or logical offset outside `[R.start,R.end)`。It emits exact payload bytes; it
does not parse/re-encode Pulsar metadata and does not expose physical ledger id to F2.

### 4.3 Clipping and checksum

The target checksum covers the entire committed range, so a read beginning in the middle still reads/verifies the
complete target before returning a clipped suffix in V1. The core may coalesce adjacent calls only if each target's
checksum and accounting remain independently verified. A future per-entry Merkle/checksum target requires a target
version bump; it cannot silently weaken V1.

`maxRecords` and `maxBytes` apply to returned logical payload. Provider read-byte permits cover the full target and
are acquired before IO. A single target larger than the hard read bound fails closed rather than deadlocking a limiter.

### 4.4 BookKeeper exception mapping

| Provider result | Nereus classification | Retry/fallback rule |
| --- | --- | --- |
| `BKException.BKNoSuchLedgerExistsException` with live protection/root | physical not found / invariant | higher healthy generation may be selected; gen0 alone fails |
| digest/password/unauthorized | authentication/config invariant | non-retriable, quarantine/admission block |
| entry missing/out-of-range/count mismatch | metadata invariant or physical loss | no empty result |
| range checksum mismatch | checksum mismatch | quarantine affected generation/ledger; higher healthy fallback allowed |
| timeout/not-enough-bookies/transient client error | transient IO | bounded retry under same deadline; no recovery-open |
| fenced/closed on normal read | closed is readable; fenced writer signal is irrelevant to read handle | reload metadata/root; never mutate |
| unknown target/config alias | unsupported target/profile | fail before BK open |

Failure handling works through provider-neutral `PhysicalReadFailureKind`; core methods named
`isObjectReadFailure`/`OBJECT_*` are generalized in BK-M1.

## 5. Generation-zero index repair

If head/commit is reachable but its generation-zero index is missing：

```text
walk existing append recovery root/tail
decode generic StreamCommitTargetRecord
require BookKeeper target + exact reachable head proof
reload ledger root and REACHABLE_APPEND protection
create/reuse VISIBLE_GENERATION protection for the desired index
put/reload exact generation-zero index
return the same logical range/target
```

No ledger handle, writer state or reservation is required. Repair cannot change the target, checksum, offset or
MessageId. If the root/protection is absent or physical range fails verification, repair fails; it does not synthesize
a new BK write.

## 6. Append recovery cuts

### 6.1 Before writer-state reservation CAS

An immutable append reservation row may exist, but no BK entry write was allowed. Exact writer-state/head/commit
absence makes it terminal ABANDONED after grace. An earlier transmitted ledger create with unknown outcome is handled
separately as durable `CREATE_UNCERTAIN`：detach its writer allocation by CAS, consume the segment/id, admit a fresh
candidate only below the shared fixed-slot bound, and fence/seal any matching late physical ledger. The allocation
slot plus monotonic `lateCreateHazard` remain permanent and veto physical deletion in BK-M0–M6, even if matching bytes
later appear. Both append branches are `KNOWN_NOT_COMMITTED`；neither converts provider absence into an `ABORTED`
create proof.

### 6.2 After reservation CAS, before/among writes

Recovery reads the exact reserved entry range after fencing/sealing as needed：

- no entries：ABANDON reservation, seal ledger；
- strict prefix/partial entries：ABANDON, seal; never reuse remaining ids；
- all entries present and NBKR1 matches：physical durability is reconstructable；continue only if the same append
  session is still current, otherwise do not publish a new head for the fenced writer；
- unexpected id/bytes/checksum：QUARANTINE root and fail closed。

The original input need not remain in memory because the reservation holds logical metadata and the full physical
range supplies exact bytes/checksum. It does not promise producer dedup across a newly generated `AppendAttemptId`.

The current BK-M2 recovery checkpoint additionally closes the metadata-only cut before writer detach：while
`activeReservationId` is still selected, it reloads that exact reservation and idempotently creates/reloads mandatory
slots `0..2` from reservation/root facts。Only after the inventory is complete may RESERVED/WRITING become
`ABANDONED` and the writer become IDLE。DURABLE or later reservations retain their recovery owner and activate slot 2；
they are left for generic commit/head recovery rather than being misclassified as abandoned。

### 6.3 After full writes, before commit intent

Same-session recovery may construct the exact BK target from the reservation and call the existing deterministic
generic commit preparation. A newer append session cannot commit the old writer's previously unreachable range；it
abandons/seals it. This preserves metadata fencing even though physical orphan entries may exist.

### 6.4 After commit intent/protection, before head CAS

Reload exact commit/reservation/protection. If the original session remains current, retry the same head CAS. If the
session was replaced, the old commit remains unreachable and later retirement removes its protection/metadata before
ledger deletion. Never allocate another offset or target within the same recovery attempt.

### 6.5 Head CAS response loss

```text
reload committed marker + generic commit + current head
if exact commit is reachable:
    return same stable AppendResult / KNOWN_COMMITTED
else if exact expected predecessor is still current and session valid:
    retry same CAS
else:
    do not claim success; classify through generic append recovery
```

Generation-zero or higher-generation failure after a reachable head cannot make the append uncommitted.

### 6.6 Head committed, generation-zero missing

Return/retain `KNOWN_COMMITTED` and run §5 repair. `WAL_DURABLE` callers may receive success once their boundary is
met；strict gen0 callers wait/fail with committed outcome. A retry must recover the same target, not write BookKeeper
again.

## 7. Broker restart

Startup/recovery does not enumerate process-local handles. It uses：

1. owned/registered BK streams from durable profile/stream registration；
2. writer state and nonterminal allocation/reservation rows per stream；
3. all-shard roots in ALLOCATING/ACTIVE/SEALING/MARKED/DELETING for global recovery；
4. existing append-recovery tail for reachable commits/gen0 repair；
5. F4 task registry for async/sync work。

Recovery order is metadata-first：finish allocation identity, fence/seal old active ledgers, resolve reservations,
repair reachable generation zero, then permit new writer allocation. An append lane is not published writable until
its writer state has no previous-owner ACTIVE/RECOVERING ambiguity.

## 8. Ownership transfer and fencing

### 8.1 Transfer sequence

```text
new broker obtains current Oxia append session
  -> CAS writer state from old owner to RECOVERING(new session, old ledger identity retained)
  -> CAS root ACTIVE -> SEALING
  -> recovery-open old ledger withRecovery(true) and close
  -> reconcile old reservation against reachable head
  -> root SEALED
  -> writer IDLE under new session
  -> allocate a new ledger for the new session
  -> publish facade writable
```

New owner never appends to the old owner's ledger. This makes BookKeeper fencing and Nereus session fencing align at a
segment boundary.

### 8.2 Stale owner races

The exact guarantee is：

- old owner after Oxia session replacement cannot pass pre-head session validation/CAS；
- recovery-open eventually fences its old `WriteAdvHandle`；
- an already transmitted stale write may leave unreachable physical entries；
- those entries cannot alter logical offset/MessageId and are reclaimed only after sealing/reference proof。

Documentation/tests must not claim distributed prevention of every physical stale packet.

### 8.3 Two contenders

Writer-state/root CAS serializes recovery. A loser reloads the winning owner/session/lifecycle and closes any local
handle it opened. Recovery-open is safe to repeat for the same SEALING root; allocation of the next ledger occurs only
after exact writer IDLE under the current session.

## 9. Profile-specific producer completion

Introduce internal：

```java
enum AppendAckBoundary {
    STABLE_HEAD,
    GENERATION_ZERO_VISIBLE,
    REQUIRED_OBJECT_GENERATION
}
```

`StorageExecutionPlan` carries this separately from `DurabilityLevel` and `ObjectPublicationMode`：

| Profile/default | Durability | Ack boundary |
| --- | --- | --- |
| BK_ONLY | `WAL_DURABLE` | `STABLE_HEAD` (or explicit strict gen0 request) |
| BK_ASYNC_OBJECT | `WAL_DURABLE` | `STABLE_HEAD` |
| BK_SYNC_OBJECT | existing WAL/gen0 durability facts | `REQUIRED_OBJECT_GENERATION` |

The exact sync barrier is specified in document 05. Consumers use head visibility and may read BK bytes while a sync
producer still waits for higher generation.

## 10. MessageId invariants

For every profile and physical generation：

```text
logical offset
  -> F2 virtual ledger/entry/batch projection
  -> Pulsar MessageIdAdv
```

The projection never receives `clusterAlias`、physical `ledgerId` or `entryId`. Tests freeze ordinary entry、batched
entry and middle-of-batch MessageIds before/after：rollover、gen0 repair、Object generation publication、seek、cursor
hydrate、broker unload/restart/failover and source-ledger deletion. Reading the same logical entry from BK or Object
must produce byte-identical Pulsar Entry and MessageId.

## 11. Close and failure outcomes

| Cut | Exposed append outcome |
| --- | --- |
| admission/reservation CAS definitely failed before write | `KNOWN_NOT_COMMITTED` |
| provider write timeout/partial and head not attempted | `MAY_HAVE_COMMITTED` until reservation reconciliation; then known not committed unless exact head found |
| commit/head CAS uncertain | generic recovery decides `KNOWN_COMMITTED` or remains may-have |
| reachable head, gen0 repair failed | `KNOWN_COMMITTED` |
| reachable head, async task later failed | producer success remains valid; lag/admission reacts |
| reachable head, sync Object barrier failed/timed out | `KNOWN_COMMITTED`; retry/recovery resumes same task |

Runtime close stops new admission and allows active attempts only within the bounded close budget. Any unfinished
provider transmission taints the ledger; durable rows allow the next process to converge. Borrowed BookKeeper client
is never closed by Nereus.
