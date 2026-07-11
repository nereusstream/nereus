# Facade State Machines and Compatibility

This document assigns behavior to the locked Pulsar interfaces. A method not listed as implemented or
explicitly unsupported is a design gap and blocks the milestone that exposes the class.

## 1. Runtime Components

```text
NereusManagedLedgerFactory
  -> openFutures[name]
  -> NereusManagedLedger
       -> StreamStorage
       -> ManagedLedgerProjectionMetadataStore
       -> PositionProjection
       -> PulsarEntryCodec
       -> NereusReadOnlyCursor / NereusNonDurableCursor
```

Factory and facade classes are thread-safe. Per-ledger append submission uses one ordered asynchronous
lane. Metadata/property updates use CAS loops. User callbacks are never invoked while holding a lock or
inside a map compute function.

## 2. Factory Open

`openFutures` is a `ConcurrentHashMap<String, CompletableFuture<NereusManagedLedger>>`.

`asyncOpen`:

1. reject blank names, null config/callback and a closed factory through the callback;
2. reject `config.storageClassName` values other than null/`nereus`;
3. create or join one open future for the exact name;
4. create/get the domain-separated L0 stream with `OBJECT_WAL_SYNC_OBJECT`;
5. create/get and repair the projection;
6. validate L0/profile/projection identity and lifecycle;
7. acquire/refresh the append session only for writable open;
8. construct the ledger and publish it in the open map;
9. invoke each waiting callback exactly once on the callback executor.

Failure removes the same failed future with compare-and-remove so a later open can retry. Closing while
open is in flight makes close win: the newly built ledger is closed and callbacks fail with
`ManagedLedgerFactoryClosedException`.

Repeated opens in one factory return the same live facade. Reopen after ledger close creates a new local
facade over the same durable stream/projection. Open never changes the virtual ledger ID.

The synchronous `open` methods are compatibility wrappers with the configured metadata deadline. They
restore interruption and throw `ManagedLedgerException`. Broker hot paths must use async methods; no
sync wrapper is called from event-loop/ordered callback threads.

## 3. Ledger Lifecycle

```text
OPEN -> TERMINATING -> TERMINATED
OPEN -> CLOSING -> CLOSED
TERMINATED -> CLOSING -> CLOSED
OPEN/TERMINATED -> DELETING -> DELETED
```

- `close` releases only local lanes, waiters, caches, session renewal and watchers.
- `terminate` performs an L0 seal and then reconciles projection state.
- `delete` performs an L0 logical tombstone and then projection state; it does not delete objects.
- closed/deleted facades reject new operations.
- close is idempotent; concurrent close callbacks all terminate once.
- terminate is idempotent and returns the same final LAC.
- delete wins over close/terminate and prevents future open.

F2 requires new protocol-neutral L0 methods before terminate/delete can be implemented:

```java
CompletableFuture<StreamMetadata> seal(
        StreamId streamId,
        SealOptions options);

CompletableFuture<StreamMetadata> delete(
        StreamId streamId,
        DeleteOptions options);
```

Both are stream-head single-key CAS state transitions. `delete` is logical only. These additions belong
in `nereus-api`/`nereus-core`/`nereus-metadata-oxia`; they contain no Pulsar types.

## 4. Append State Machine

```text
validate/copy bytes
  -> enqueue ordered append
  -> validate live ACTIVE snapshot/session
  -> StreamStorage.append
  -> classify AppendResult or AppendOutcome
  -> validate one-entry result
  -> create Position(virtualLedgerId, startOffset)
  -> update local monotonic snapshot/cache
  -> terminal callback
```

The facade does not assign an entry ID before L0 success. It does not infer success from object upload,
offset index alone or local queue order.

Exactly-once completion uses one terminal guard owned by the operation. Timeout, cancellation, L0
completion and close race through that guard. A late completion may update internal recovery state but
cannot invoke a second callback.

Outcome handling:

| L0 outcome | Facade action |
| --- | --- |
| Normal `AppendResult` | Validate, return `addComplete` |
| `KNOWN_NOT_COMMITTED` | `addFailed` with mapped error |
| `KNOWN_COMMITTED` without response | Resolve committed identity/result, then `addComplete` |
| `MAY_HAVE_COMMITTED` | Suspend/refresh the per-stream lane and recover before terminal choice |
| Recovery deadline expires uncertain | One failure callback; ledger is fenced/closed for new writes until reopen |

F2 has no producer-level dedup. A caller retry after a genuinely uncertain publish can create a second
entry. The facade must preserve the uncertainty classification; it cannot promise deduplication.

## 5. Callback and Buffer Rules

- Validate callback and arguments before enqueuing, but report async-overload failures through callback.
- Run callbacks on the configured callback executor, never an Oxia client, object IO or broker event-loop
  thread by accident.
- Invoke callbacks outside synchronization and catch/log callback exceptions.
- Preserve `ctx` identity.
- Success callback receives a read-only exact-byte buffer valid for callback duration.
- Release facade-owned callback/read buffers on every terminal path.
- A callback exception never changes a successful durable append into failure.

Tests race success/failure/timeout/close in a loop and assert one terminal callback and zero buffer leaks.

## 6. Read and Waiters

`asyncReadEntry` follows the projection read contract. Cursor reads hold a local next-offset and issue
bounded L0 reads. There is at most one pending `readEntriesOrWait` callback per cursor, matching the
locked interface's concurrent-wait failure behavior.

When a read reaches current LAC:

- ordinary read returns an empty/no-more-entries result according to the called API;
- read-or-wait registers a local waiter;
- successful append signals waiters after LAC visibility is updated;
- close/cancel removes the waiter and completes/cancels it exactly once;
- Oxia watch is an invalidation/wakeup hint, not the proof that data is committed.

`maxPosition` is inclusive and must match the projection ledger ID or be a recognized Pulsar sentinel.
Every read uses one metadata snapshot so trim/LAC comparisons are internally consistent.

## 7. Method Compatibility Matrix

### ManagedLedgerFactory

| Group | F2 behavior |
| --- | --- |
| open/asyncOpen | Implement |
| read-only cursor/ledger open | Implement |
| get info/async info | Synthesize one virtual ledger and mark it Nereus in Nereus-specific metrics/logs |
| exists | Read authoritative topic projection; DELETED is false |
| properties | Implement with topic-record CAS |
| delete/asyncDelete | Implement logical delete after L0 lifecycle API exists |
| open-ledger map | Return immutable snapshot |
| cache manager/stats/config thresholds | Implement Nereus compatibility adapters; no BookKeeper cache delegation |
| unloaded backlog estimate | Compute from stream metadata/index summaries or fail as unsupported when accurate scan is requested |
| shutdown/shutdownAsync | Implement idempotent lifecycle |

### ManagedLedger

| Group | F2 behavior |
| --- | --- |
| add overloads | Implement through one async core; sync overloads are bounded wrappers |
| direct read | Implement |
| LAC, first/next/previous/after-N, counts and size | Exact formula/snapshot behavior |
| properties | Implement through authoritative topic record |
| open read-only/non-durable cursor | Implement |
| open durable cursor | Basic boundary only; mutation methods explicitly unsupported until F3 |
| terminate/isTerminated | Implement with L0 seal |
| close | Implement local close |
| delete | Implement logical delete |
| ledger info map | Synthesize exactly one virtual ledger |
| offload methods/properties | Fail unsupported; never call a BookKeeper offloader |
| migrate/truncate | Fail unsupported in F2 |
| rollover/create-new-ledger methods | No physical action; validate live state and emit debug metrics only |
| interceptor | Expose configured interceptor only if entry bytes still satisfy its contract; otherwise reject configuration |

Returning `null`, silently succeeding, or fabricating BookKeeper metadata is not an unsupported-method
implementation.

For the synthetic `LedgerInfo`, the entry extent is `committedEndOffset` so a retained Position keeps
its original entry ID。Facade `getNumberOfEntries` reports the active range
`committedEndOffset - trimOffset`，and retained logical size comes from exact L0 index summaries。
Admin output must expose Nereus/trim context separately and must not interpret the synthetic entry
extent as a real BookKeeper ledger。

### ManagedCursor

| Group | F2 behavior |
| --- | --- |
| read/read-or-wait/skip/seek/rewind | Implement for read-only/non-durable cursor |
| read position/count/backlog | Exact offset formulas for read-only/non-durable cursor |
| active/inactive/close/cancel waiter | Implement local state |
| durable open/state view | Expose basic boundary |
| mark-delete/individual delete/ack sets | Fail unsupported until F3 |
| durable reset/properties persistence | Fail unsupported until F3 |
| replay with individual deletes | Fail unsupported until F3 |

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
| `OFFSET_NOT_AVAILABLE` | `NoMoreEntriesToReadException` |
| metadata unavailable/condition failed | `MetaStoreException` / `BadVersionException` |
| metadata invariant/checksum/corruption | `NonRecoverableLedgerException` |
| unsupported profile/format/operation | explicit `ManagedLedgerException` with stable Nereus error code in message/cause |
| timeout/cancel | managed-ledger timeout/cancel wrapper preserving cause |

Retriability comes from the Nereus exception classification, not string matching.

## 9. Cache Boundary

F2 caches are discardable:

| Cache | Truth |
| --- | --- |
| topic projection | authoritative topic Oxia record |
| derived projection | topic record + repair |
| stream snapshot/LAC | L0 stream head |
| Position/entry | L0 offset index + Object WAL bytes |

The `EntryCacheManager` required by the Pulsar interface is a Nereus implementation backed by the
facade entry cache, or a zero-capacity implementation when caching is disabled. It never routes a
virtual ledger ID into stock BookKeeper cache/read code.
