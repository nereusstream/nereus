# Facade State Machines and Compatibility

This document assigns behavior to the locked Pulsar interfaces. A method not listed as implemented or
explicitly unsupported is a design gap and blocks the milestone that exposes the class.

## 1. Runtime Components

```text
NereusManagedLedgerFactory
  -> ledgers[name] = CompletableFuture<NereusManagedLedger>
  -> readOnlyLedgers[ProjectionHandleKey(name, incarnation)]
       = CompletableFuture<NereusReadOnlyManagedLedger>
  -> NereusManagedLedgerRuntime (shared owner)
  -> NereusManagedLedger
       -> StreamStorage
       -> ManagedLedgerProjectionMetadataStore
       -> PositionProjection
       -> PulsarEntryCodec
       -> StreamSnapshotTracker / TailPollCoordinator
       -> NereusReadOnlyCursor / NereusNonDurableCursor / NereusManagedCursorBoundary
```

Factory and facade classes are thread-safe. Per-ledger append submission uses one ordered asynchronous
lane. Metadata/property updates use CAS loops. User callbacks are never invoked while holding a lock or
inside a map compute function.

## 2. Factory Open

`ledgers` is the single
`ConcurrentHashMap<String, CompletableFuture<NereusManagedLedger>>` registry for writable opens. There is no second
facade map whose publication/removal can race this future. `readOnlyLedgers` is a separate get-only registry because
`ReadOnlyManagedLedger` exposes no public close method. Every read-only open first reads the current topic projection,
then deduplicates by exact name **and incarnation**; it closes/removes an older same-name handle before publishing a
new-incarnation handle. Its concrete implementation has a package-private idempotent `factoryClose()`; factory
shutdown closes every read-only cursor/handle before the shared runtime. Read-only open never enters the writable
registry and never creates durable state.

`asyncOpen`:

1. reject blank names, null config/callback and a closed factory through the callback;
2. retain the exact config reference and capture one immutable `ManagedLedgerConfigView`; reject storage-class,
   interceptor/offload/shadow/auto-skip/retention violations through the callback; read-compacted is checked later
   from subscribe/cursor args;
3. first join an existing exact-name future; only when absent, reserve one distinct-name open permit and
   `putIfAbsent` a candidate future. A losing same-name candidate releases its unused permit and joins the winner;
4. the winning candidate acquires one fork storage-class permit and retains its binding generation for this open;
5. read the authoritative topic projection before any L0 create decision and require its binding generation to match;
6. if present, get the exact current L0 stream without create; if missing and create-if-missing is false, fail;
7. for first create or deleted-topic recreate, create/validate the deterministic empty incarnation stream and
   publish the topic record with the recoverable protocol in `03`;
8. repair derived projection records and validate one L0/profile/projection/lifecycle snapshot;
9. construct the ledger and complete the same registered future;
10. invoke each waiting callback exactly once on the callback executor.

Open does not eagerly acquire an append session. L0 lazily acquires/renews it on the first append. A read-capable
facade may therefore open while a previous writer lease is live; broker ownership does not become a durable lease.

Failure removes the same failed future with compare-and-remove and releases its permit so a later open can retry.
Final ledger close removes only `ledgers.remove(name, sameFuture)` and releases that permit. Closing while
open is in flight makes close win: the newly built ledger is closed and callbacks fail with
`ManagedLedgerFactoryClosedException`.

Repeated opens in one factory return the same live facade. Reopen after ledger close creates a new local
facade over the same durable stream/projection. Open never changes the virtual ledger ID within one incarnation.
Open after logical delete with create-if-missing true creates the next incarnation/new stream/new ledger ID;
create-if-missing false reports not found and writes nothing.

An existing registry future is interpreted by facade state: `OPEN`/`TERMINATED` returns that instance;
`CLOSING/CLOSED` is compare-removed and retried; `DELETING` remains registered until its delete future finishes, then
open restarts from projection authority; `DELETED` is removed only after terminal mirror scheduling;
`WRITE_FENCED` remains readable but is never replaced merely to bypass its suspended L0 attempt. This prevents an
open/delete race from constructing a facade over an incarnation whose tombstone is still in flight.

The synchronous `open` methods are stock-shaped compatibility wrappers that wait for the async core's one terminal
callback；they add no second timer, while the async metadata operations share the configured deadline. They restore
interruption and throw `ManagedLedgerException`. Broker hot paths must use async methods; no
sync wrapper is called from event-loop/ordered callback threads.

## 3. Ledger Lifecycle

```text
OPEN -> TERMINATING -> TERMINATED
OPEN -> WRITE_FENCED -> OPEN (only after exact recovery commits or proves non-commit)
OPEN -> CLOSING -> CLOSED
WRITE_FENCED -> CLOSING -> CLOSED
TERMINATED -> CLOSING -> CLOSED
OPEN/TERMINATED -> DELETING -> DELETED
```

Admission changes state before an operation is enqueued, under one short facade lock/atomic transition。The exact
matrix is：

| State | append | read/getter | terminate | logical delete | local close |
| --- | --- | --- | --- | --- | --- |
| `OPEN` | admit | admit | install barrier | install delete barrier | install close barrier |
| `TERMINATING` | reject | admit | join same future | delete supersedes after terminate resolution | join/queue close |
| `TERMINATED` | terminated failure | admit | return final LAC | admit delete | admit close |
| `WRITE_FENCED` | reject with current fence | admit | fail before L0 CAS | fail before L0 CAS | admit/detach |
| `CLOSING/CLOSED` | reject | reject | reject | reject | join/idempotent |
| `DELETING/DELETED` | reject | reject | reject | join/idempotent | join local cleanup |

Once a terminate/delete/close barrier is installed, a later append is rejected before byte copying even if the
barrier has not yet reached the ordered lane。Delete may supersede a pending terminate, but both share the same lane
and only the L0 head decides whether the final path is `SEALED -> DELETING` or `ACTIVE -> DELETING`。Close never
supersedes durable lifecycle work；it waits for an already accepted terminate/delete and then performs local cleanup。

- `close` releases only local lanes, waiters, caches, cached session references and watches.
- `terminate` performs an L0 seal and then reconciles projection state.
- `delete` performs an L0 logical tombstone and then projection state; it does not delete objects.
- closed/deleted facades reject new operations.
- close is idempotent; concurrent close callbacks all terminate once.
- terminate is idempotent and returns the same final LAC.
- delete wins over close/terminate and prevents future open of that incarnation；same-name creation uses a new
  incarnation after the tombstone is confirmed.
- terminate/delete do not bypass `WRITE_FENCED`: they wait for exact append recovery or fail without starting a
  lifecycle CAS. Local close is still allowed because it does not mutate durable stream state.

Lifecycle completion rules close timeout/CAS ambiguity：

| Observation after operation failure/timeout | Facade result/state |
| --- | --- |
| fresh L0 remains `ACTIVE` and no lifecycle CAS could have landed | fail mapped error；remove barrier and return `OPEN` |
| fresh L0 is `SEALED` during terminate | succeed with its final LAC；state `TERMINATED`；repair mirror asynchronously |
| fresh L0 is `DELETING/DELETED` during delete | continue/resume to `DELETED`, then succeed；state `DELETED` |
| fresh L0 is later than requested transition | accept the later authoritative state；never report rollback |
| L0 state cannot be reread before deadline | fail operation and keep the lifecycle barrier/facade non-writable；open-time reconciliation is required before reuse |
| projection mirror alone fails after L0 terminal | operation still succeeds；bounded mirror repair is scheduled |

The implementation never restores `OPEN` from a timeout based only on the original exceptional completion。

F2 consumes the protocol-neutral L0 methods defined in `06` and implemented by Phase 1.5 P15-M4 before
terminate/delete can be implemented:

```java
CompletableFuture<StreamMetadata> seal(
        StreamId streamId,
        SealOptions options);

CompletableFuture<StreamMetadata> delete(
        StreamId streamId,
        DeleteOptions options);
```

Both are stream-head single-key CAS state transitions. `delete` is logical only. Accepted appends drain through the
per-ledger ordered lane before the terminate/delete barrier is allowed to call L0. A remote append races only at the
same stream-head CAS and is either included in the final LAC or rejected after the lifecycle transition.

## 4. Append State Machine

```text
validate arguments
  -> reserve pending-operation/attempt capacity
  -> copy bytes
  -> enqueue ordered append
  -> validate live ACTIVE snapshot/session
  -> StreamStorage.append
  -> classify AppendResult or AppendOutcome + AppendAttemptId
  -> validate one-entry result
  -> create Position(virtualLedgerId, startOffset)
  -> update local monotonic end/cumulative snapshot from P15-M6 result
  -> terminal callback
```

The facade does not assign an entry ID before L0 success. It does not infer success from object upload,
offset index alone or local queue order.

Exactly-once completion uses one terminal guard owned by the operation. L0-classified timeout, recovery completion
and process-level forced shutdown race through that guard. A normal facade close does not preempt an accepted append;
it drains the L0 attempt plus exact-recovery budget. A late completion after forced process shutdown may update
internal recovery state but cannot invoke a callback in the ended domain.

Outcome handling:

| L0 outcome | Facade action |
| --- | --- |
| Normal `AppendResult` | Validate, return `addComplete` |
| `KNOWN_NOT_COMMITTED` | `addFailed` with mapped error |
| `KNOWN_COMMITTED` + attempt ID | `recoverAppend` the exact retained physical attempt, then `addComplete` |
| `MAY_HAVE_COMMITTED` + attempt ID | `recoverAppend`; never submit a new WAL object |
| Recovery proves `KNOWN_NOT_COMMITTED` | One failure callback if still pending; release the retained attempt/lane and permit the local write fence to clear |
| Recovery remains retryably uncertain at callback deadline | One failure callback; ledger remains `WRITE_FENCED` while background recovery continues |
| Recovery reaches permanent invariant/corruption | One failure callback if still pending; stop retrying and leave the facade permanently `WRITE_FENCED` |
| Non-known outcome without attempt ID | invariant failure and permanent local `WRITE_FENCED` |

F2 has no producer-level dedup. A caller retry after a genuinely uncertain publish can create a second
entry. The facade must preserve the uncertainty classification; it cannot promise deduplication.

Each transition into `WRITE_FENCED` allocates a monotonically increasing facade fence generation and publishes the
attempt ID through the product-owned `NereusWriteFenceView` defined in `06`。The original callback may fail once at
its recovery budget, but the fence-resolution future remains attached to the core exact-recovery attempt：

- committed recovery completes it with `COMMITTED`；
- proven-not-committed completes it with `PROVEN_NOT_COMMITTED`；
- permanent invariant/corruption completes it exceptionally and never reopens this facade；
- retryable uncertainty keeps it incomplete；
- a stale completion whose generation is no longer current cannot change facade state。

The Pulsar fork consumes this view before its stock pending-write auto-unfence branch。It must not infer readiness from
`pendingWriteOps == 0` or from a void `readyToCreateNewLedger()` call。

## 5. Callback and Buffer Rules

- Validate callback and arguments before enqueuing, but report async-overload failures through callback.
- Run callbacks on the configured callback executor, never an Oxia client, object IO or broker event-loop
  thread by accident.
- Invoke callbacks outside synchronization and catch/log callback exceptions.
- Preserve `ctx` identity.
- Success callback receives a read-only exact-byte buffer valid for callback duration.
- Release facade-owned callback/read buffers on every terminal path.
- A callback exception never changes a successful durable append into failure.

For read callbacks, entry ownership transfers when the callback is invoked. The facade releases all entries only when
failure occurs before callback invocation; it does not double-release caller-owned entries if callback code throws.

Tests race success/failure/timeout/close in a loop and assert one terminal callback and zero buffer leaks.

## 6. Read and Waiters

`asyncReadEntry` follows the projection read contract. Cursor reads hold a local next-offset and issue
bounded L0 reads. There is at most one pending `readEntriesOrWait` callback per cursor, matching the
locked interface's concurrent-wait failure behavior.

`StreamSnapshotTracker` accepts only snapshots for the same stream/name/profile and separates backend metadata version
from append `commitVersion`。A remote snapshot enforces monotonic backend version、committed end、cumulative size、trim
and lifecycle state；equal backend version with different remote content or a newer-version numeric regression is an
invariant failure。An older watch/poll result is dropped。Append success installs a marked local overlay using exact
P15-M6 `AppendResult.committedEndOffset/cumulativeSize/commitVersion`，so a stale facade never guesses missing remote
bytes and never waits on a second post-commit read。That overlay retains the last remote version/trim/lifecycle and is
cleared only when remote metadata catches up；it cannot synthesize trim or lifecycle fields。

When a read reaches current LAC:

- ordinary read returns an empty/no-more-entries result according to the called API;
- read-or-wait registers a local waiter;
- successful append signals waiters after LAC visibility is updated;
- a periodic metadata poll provides correctness for remote-broker append; watch/local append only wake it early;
- close removes the waiter and fails its callback once;
- `cancelPendingReadRequest()` removes the waiter without invoking the cancelled callback, matching stock behavior;
- Oxia watch is an invalidation/wakeup hint, not the proof that data is committed.

`TailPollCoordinator` is one per ledger, not one scheduled task per cursor。The first installed waiter starts a single
poll timer；all cursor waiters share at most one in-flight `getStreamMetadata`。A poll refreshes
`StreamSnapshotTracker`, signals only waiters whose bound can now progress, and reschedules while at least one waiter
remains。Removing the last waiter or closing the ledger cancels the timer；late poll completion is generation-checked
and cannot invoke a removed callback。

Two terminal cases do not install a waiter: an exhausted inclusive `maxPosition` completes with an empty list, while
a sealed ledger at final LAC fails read-or-wait with `NoMoreEntriesToReadException` because no append can wake it.

`maxPosition` is inclusive and must be null, match the projection ledger ID or be a recognized Pulsar sentinel.
Every read uses one metadata snapshot so trim/LAC comparisons are internally consistent.

Waiter installation is register-then-recheck. This prevents an append between the first EOF observation and waiter
publication from being lost.

## 7. Method Compatibility Overview

The exhaustive signature-by-signature contract is `06-code-level-interface-contract.md`. The tables below are only a
review summary; they do not replace the complete matrix.

### ManagedLedgerFactory

| Group | F2 behavior |
| --- | --- |
| open/asyncOpen | Implement |
| read-only cursor/ledger open | Implement |
| get info/async info | Synthesize one virtual ledger and mark it Nereus in Nereus-specific metrics/logs |
| exists | Read topic projection plus exact L0 state; missing current L0 is corruption, DELETING/DELETED is false |
| properties | Implement with topic-record CAS |
| delete/asyncDelete | Implement logical delete after L0 lifecycle API exists |
| open-ledger map | Return immutable snapshot |
| cache manager/stats/config thresholds | F2 zero-capacity compatibility manager; no BookKeeper cache delegation |
| unloaded backlog estimate | Exact retained entry count/lifetime logical size from L0 metadata; add one virtual detail when accurate |
| shutdown/shutdownAsync | Implement idempotent lifecycle |

### ManagedLedger

| Group | F2 behavior |
| --- | --- |
| add overloads | Implement through one async core；sync overloads add no independent timer and wait for that core's deadline/recovery terminal |
| direct read | Implement |
| LAC, first/next/previous/after-N, counts and size | Role-aware exact formulas; first position is before-first; size is exact lifetime logical bytes |
| properties | Implement through authoritative topic record |
| open read-only/non-durable cursor | Implement; a concrete non-durable start Position is already consumed and the first read is its next retained Entry; null/`LATEST`/future consult `InitialPosition`, and the two-argument overload defaults Latest |
| open durable cursor | Basic boundary only; mutation methods explicitly unsupported until F3 |
| terminate/isTerminated | Implement with L0 seal |
| close | Implement local close |
| delete | Implement logical delete |
| ledger info map | Synthesize exactly one virtual ledger |
| offload methods/properties | Fail unsupported; never call a BookKeeper offloader |
| migrate/truncate | Fail unsupported in F2 |
| rollover/create-new-ledger methods | No physical rollover; never clear an unresolved write fence |
| interceptor | Reject every non-null interceptor configuration in F2 before IO; getter returns null |

Returning `null`, silently succeeding, or fabricating BookKeeper metadata is not an unsupported-method
implementation.

For the synthetic `LedgerInfo`, the entry extent is `committedEndOffset` so a retained Position keeps
its original entry ID。Facade `getNumberOfEntries` reports the retained active range
`committedEndOffset - trimOffset`。`getTotalSize` reports L0 `cumulativeSize`: exact lifetime logical payload still
physically protected because F2 has no GC. It is not post-trim bytes or actual ObjectStore bytes.
Admin output must expose Nereus/trim context separately and must not interpret the synthetic entry
extent as a real BookKeeper ledger。

### ManagedCursor

| Group | F2 behavior |
| --- | --- |
| read/read-or-wait/skip/seek/rewind | Implement for non-durable and durable-boundary cursor; direct seek has no start-position `+1`, non-force seek cannot cross local mark-delete, and rewind uses next-valid after mark-delete; ReadOnlyCursor has its locked subset |
| read position/count/backlog | Exact offset formulas for read-only/non-durable cursor |
| active/inactive/close/cancel waiter | Implement local state |
| durable open/state view | Expose basic local boundary and report `isCursorDataFullyPersistable=false` |
| non-durable cumulative mark-delete/clear/reset | Implement locally only; reset treats its Position as the direct read target and normalizes sentinels/trim/tail separately from cursor creation |
| durable mark-delete and all individual delete/ack sets | Fail unsupported until F3 |
| durable reset/properties persistence | Fail unsupported until F3 |
| replay overloads | Direct non-mutating reread; return locally mark-deleted positions as skipped, honor sort flag, and do not infer individual ack holes |

At the broker boundary F2 accepts only a non-durable cumulative acknowledgement covering a whole Entry：one Position,
no transaction bits and no `AckSetState` extension。`Consumer.messageAcked` performs this validation before ack
counters、pending-ack/transaction paths or `PersistentSubscription.acknowledgeMessageAsync`。Individual ack and
partial-batch cumulative ack fail explicitly；therefore F2 claims Reader and limited non-durable consumption, not a
general Consumer acknowledgement implementation。

The F2 broker acceptance scenario uses producer plus Reader/non-durable cursor. It does not claim
durable Consumer subscription recovery.

## 8. Error Mapping

| Nereus error | Managed-ledger exception |
| --- | --- |
| `INVALID_ARGUMENT` | `ManagedLedgerException` or `InvalidCursorPositionException` by context |
| `STREAM_NOT_FOUND` | `ManagedLedgerNotFoundException` |
| `STREAM_NOT_ACTIVE` when sealed | `ManagedLedgerTerminatedException` |
| `FENCED_APPEND`, expired session | `ManagedLedgerFencedException` |
| `BACKPRESSURE_REJECTED` | `TooManyRequestsException` |
| `STORAGE_CLOSED` | `ManagedLedgerAlreadyClosedException` / factory-closed variant |
| `OFFSET_TRIMMED` | `InvalidCursorPositionException` |
| `OFFSET_NOT_AVAILABLE` | `NoMoreEntriesToReadException` for terminal reads; an ordinary tail cursor returns empty/waits by method contract |
| metadata unavailable/condition failed | `MetaStoreException` / `BadVersionException` |
| metadata invariant/checksum/corruption | `NonRecoverableLedgerException` |
| primary-WAL write/read availability failure | generic managed-ledger storage exception preserving retriable/cause |
| primary-WAL target not found/checksum mismatch | `NonRecoverableLedgerException` |
| unsupported profile/format/operation | explicit `ManagedLedgerException` with stable Nereus error code in message/cause |
| timeout/cancel | managed-ledger timeout/cancel wrapper preserving cause |

A callback-time unresolved exact recovery uses the ordinary mapped storage exception for the Pulsar callback while
the concrete ledger exposes its nonempty `NereusWriteFenceView`。It is intentionally not mapped to
`ManagedLedgerFencedException`：the stock handler closes immediately on that subtype and loses the explicit
committed-vs-proven-not-committed readiness handoff。The fork checks the view first and follows the special fenced
path defined in `07`。

Retriability comes from the Nereus exception classification, not string matching.

## 9. Cache Boundary

F2 caches are discardable:

| Cache | Truth |
| --- | --- |
| topic projection | authoritative topic Oxia record |
| derived projection | topic record + repair |
| stream snapshot/LAC | L0 stream head |
| Position/entry | L0 offset index + Object WAL bytes |

The first F2 implementation locks the Pulsar `EntryCacheManager` to zero capacity. L0 resolver/object caches may still
operate under their own bounds. BookKeeper `ReadHandle` methods on the compatibility cache fail explicitly; a virtual
ledger ID never enters stock BookKeeper cache/read code.
