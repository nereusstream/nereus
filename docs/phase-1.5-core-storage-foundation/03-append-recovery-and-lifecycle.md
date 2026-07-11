# Append Recovery and Stream Lifecycle

> 状态：Phase 1.5 target contract；not implemented
> F2 source：current `../phase-2-managed-ledger-facade/06-code-level-interface-contract.md`

本文把 F2-M0R 锁定的 callback-safety requirements 落到 protocol-neutral L0。Append recovery 解决当前
进程内一个已提交状态不确定的 physical attempt；stream lifecycle 解决 seal/logical-delete truth。二者都
不能由 facade-local state 代替。

## 1. Attempt Identity and Capacity

`StreamStorageConfig` supplies a per-process `processRunId` with at least 128 bits of entropy。The core generates：

```text
AppendAttemptId = processRunId + "/" + Long.toUnsignedString(sequence)
```

The sequence starts from zero and uses `AtomicLong.getAndIncrement()`。Unsigned wrap is a terminal
`BACKPRESSURE_REJECTED`/configuration-capacity failure；an ID is never reused in one process。The ID is generated and
capacity is reserved before WAL prepare/upload, but it is attached to a public error only after the head request is
submitted。

Two limits apply：

- at most one unresolved post-head attempt per stream mutation lane；
- at most `StreamStorageConfig.maxRetainedAppendAttempts` unresolved attempts in one runtime。

The global permit is acquired before WAL buffer/object allocation。Exhaustion fails
`KNOWN_NOT_COMMITTED` with no attempt ID and performs no WAL IO。An unresolved attempt is never evicted to admit
another append。

## 2. RetainedAppendAttempt

Core-private target state：

```java
enum RetainedAttemptState {
    ADMITTED,
    PRIMARY_WAL_DURABLE,
    HEAD_REQUEST_IN_FLIGHT,
    SUSPENDED,
    RECOVERING,
    COMMITTED,
    PROVEN_NOT_COMMITTED,
    PERMANENT_FAILURE,
    CLOSED
}

final class RetainedAppendAttempt {
    AppendAttemptId id();
    StreamId streamId();
    CommitAppendRequest commitRequest();
    DurablePrimaryAppend durablePrimaryAppend();
    CompletableFuture<Void> originalRunnerQuiesced();
    Optional<AppendReplayCursor> replayCursor();
    RetainedAttemptState state();
}
```

The retained object owns the exact immutable `CommitAppendRequest` and provider result/read target needed to
construct the original `AppendResult`。It never retains caller buffers or a mutable `AppendBatch`。Before primary WAL
durability, fields not yet available remain core-private staged state；a known-not-committed terminal releases them。

`originalRunnerQuiesced` completes only after every task owned by the original append call has reached a point where
it can no longer submit/resubmit the head CAS。Timeout/cancellation of the caller future does not imply runner
quiescence。

## 3. Public Recovery Semantics

```text
recoverAppend(streamId, attemptId, options)
  -> validate exact process-local ID and stream
  -> join/start one single-flight recovery runner
  -> wait up to options.timeout for a terminal/current failure
```

Rules：

1. A wrong stream, unknown or expired ID is `METADATA_INVARIANT_VIOLATION`；it never authorizes a fresh append。
2. Concurrent calls for the same ID join one runner and cannot submit parallel head requests。
3. `AppendRecoveryOptions.timeout` bounds that caller's wait only。A timeout does not cancel the underlying exact
   recovery or release capacity。
4. Committed terminal returns the same logical range/read target/count/schema/projection/commit version as the
   original append would have returned。
5. Proven-not-committed terminal completes exceptionally with `KNOWN_NOT_COMMITTED` and releases the stream/global
   retained permit。
6. Retryable uncertainty completes the current waiter exceptionally with `MAY_HAVE_COMMITTED` and the same ID；the
   attempt/lane remain suspended。
7. Permanent chain/identity/corruption failure stops retries, releases large retained payload resources, and leaves a
   small permanent terminal entry so a fresh writer cannot silently bypass the invariant in that runtime。
8. A bounded terminal-result cache preserves idempotent repeated recovery after capacity release。It is created only
   for an attempt whose ID was exposed after a non-known result or used by `recoverAppend`；an ordinary append that
   returns success releases its internal ID without caching an unreachable terminal。The cache is keyed by
   attempt ID, bounded separately by `maxAppendRecoveryTerminals`, expires only after
   `appendRecoveryTerminalTtl`, and stores no payload bytes beyond the final result/diagnostic。After expiry the ID is
   unknown/invariant as specified above。

Phase 1.5 does not persist attempt IDs or retained state。A process crash ends the callback obligation；durable commit
identity still makes normal restart read/repair safe, but this is not cross-process producer deduplication。

## 4. Paged Replay Contract

Metadata-private types：

```java
public record AppendReplayCursor(
        StreamId streamId,
        String commitId,
        long expectedStartOffset,
        String observedHeadCommitId,
        long observedHeadOffsetEnd,
        long observedHeadCumulativeSize,
        long observedHeadCommitVersion,
        String nextCommitId,
        long nextOffsetEnd,
        long nextCumulativeSize,
        long nextCommitVersion) {
}

public enum AppendReplayStatus {
    FOUND,
    PROVEN_NOT_COMMITTED,
    CONTINUE
}

public record AppendReplaySearchResult(
        AppendReplayStatus status,
        Optional<ReachableCommittedAppend> committedAppend,
        Optional<AppendReplayCursor> continuation,
        int scannedRecords) {
}
```

Legal shapes：

| Status | committed append | continuation |
| --- | --- | --- |
| `FOUND` | present | empty |
| `PROVEN_NOT_COMMITTED` | empty | empty |
| `CONTINUE` | empty | present |

`scannedRecords` is within `[0,maxCommitsToScan]`。A continuation validates request identity, immutable observed-head
anchor and the exact dense tuple expected at `nextCommitId`。It is not accepted from public/protocol code and is
stored only inside the retained attempt。

## 5. Recovery State Machine

```text
SUSPENDED
  -> wait originalRunnerQuiesced
  -> read current head
     -> head end == expected start: resubmit exact CommitAppendRequest
     -> head end  > expected start: freeze head anchor and search backward
     -> head end  < expected start: permanent dense-history invariant
  -> FOUND: validate full identity -> materialize generation zero -> COMMITTED
  -> crossing different commit/genesis: PROVEN_NOT_COMMITTED
  -> page exhausted: store cursor -> CONTINUE -> schedule next page
  -> broken anchor/chain/target: PERMANENT_FAILURE
```

Important rules：

- the exact request may be resubmitted only after the old mutation runner quiesces；
- observing a later head does not prove this request committed because another writer may own that range；
- once historical search starts, later remote appends do not change its immutable anchor；
- each page validates `previousCommitId` and dense offset/cumulative-size/commit-version progression；
- `FOUND` compares every logical field and `ReadTargetRecord` identity checksum；
- a different reachable commit proves absence only when its range crosses the request's only possible start, or the
  scan reaches a valid genesis boundary；
- page budget exhaustion is progress (`CONTINUE`), not a retriable terminal that restarts from the newest head；
- committed recovery calls `materializeGenerationZero` before returning strict success。

## 6. Background Retry and Fencing

`AppendRecoveryCoordinator` owns one scheduled retry per unresolved attempt。Backoff starts at
`appendRecoveryBackoffMin`, doubles with saturation to `appendRecoveryBackoffMax`, and each metadata attempt is bounded
by `appendRecoveryAttemptTimeout`。There is no unbounded blocking task or overlapping timer for one ID。

The public recovery caller may retry sooner and join the same flight。A stream lane stays write-suspended until：

- committed recovery advances/refreshes the lane expected offset；or
- complete proof of non-commit invalidates/reloads the expected offset。

A permanent failure keeps the lane fenced for its lifetime。Creating a second facade or calling a
BookKeeper-shaped “new ledger” method cannot clear a core lane。

Observers are optional, weakly held runtime callbacks used only to wake a facade/local waiter after a terminal。
Correctness does not require observer delivery；a later `recoverAppend` or metadata read obtains the terminal truth。

## 7. Stream Lifecycle Metadata CAS

Core-private request：

```java
public record StreamStateTransitionRequest(
        StreamId streamId,
        StreamState expectedState,
        StreamState targetState,
        long expectedMetadataVersion) {
}
```

Allowed edges in Phase 1.5：

```text
ACTIVE   -> SEALED
ACTIVE   -> DELETING
SEALED   -> DELETING
DELETING -> DELETED
```

`OxiaMetadataStore.transitionStreamState` performs exactly one head-key `putIfVersion` after validating stream
identity, current state and metadata version。It never creates a missing head and never modifies commit anchor,
committed end, cumulative size, trim or append-session snapshot。The public options reason is logged/observed by core
but is not a durable field in `StreamHeadRecord`。

## 8. Seal Protocol

```text
join shared stream mutation lane
  -> wait older accepted append/exact recovery terminal
  -> read one head snapshot
  -> ACTIVE: CAS ACTIVE -> SEALED
  -> SEALED: return current snapshot
  -> conflict: reread/retry until deadline
```

State results：

| Observed state | `seal` result |
| --- | --- |
| `CREATING` | retriable `STREAM_NOT_ACTIVE` |
| `ACTIVE` | CAS then return sealed metadata |
| `SEALED` | idempotent current metadata |
| `DELETING/DELETED` | non-retriable `STREAM_NOT_ACTIVE` |
| missing | `STREAM_NOT_FOUND` |

The successful `ACTIVE -> SEALED` CAS is termination linearization。Reads/resolves and metadata-only trim remain
allowed；append/session acquisition are rejected。Final LAC is the committed end in the CAS winner snapshot。

## 9. Logical Delete Protocol

```text
join shared stream mutation lane
  -> wait older accepted append/exact recovery terminal
  -> ACTIVE/SEALED: CAS -> DELETING       // logical delete point
  -> DELETING: resume
  -> CAS DELETING -> DELETED              // terminal completion
  -> DELETED: idempotent result
```

State results：

| Observed state | `delete` result |
| --- | --- |
| `CREATING` | retriable `STREAM_NOT_ACTIVE` |
| `ACTIVE/SEALED` | transition through `DELETING` to `DELETED` |
| `DELETING` | resume terminal CAS |
| `DELETED` | idempotent tombstone snapshot |
| missing | `STREAM_NOT_FOUND` |

After the first `DELETING` CAS, new append、session、read、resolve and trim operations fail；already returned read
buffers remain caller-owned and valid。An already admitted read/resolve whose authoritative snapshot observed
`ACTIVE/SEALED` before the delete CAS may finish against that immutable committed range；delete does not revoke or
corrupt an in-flight buffer。No offset-index, manifest or object bytes are deleted。A crash between the two CAS
operations is resumed by delete/open reconciliation。

## 10. Local and Remote Ordering

`StreamMutationLaneRegistry` replaces the append coordinator's private lane ownership。Append、recover、seal and
delete for one local stream use the same ordered lane。Rules：

- an append accepted before a lifecycle barrier either reaches a terminal exact outcome first or causes the
  lifecycle call to fail before any lifecycle CAS when its recovery budget is exhausted；
- a lifecycle call never reports success while an earlier local append may still land a head CAS；
- appends accepted after seal/delete admission are rejected without WAL IO；
- trim/session operations may race through head CAS and retry, but every retry revalidates lifecycle state；
- remote broker append/lifecycle operations are ordered only by the same authoritative head CAS；the winner's state
  is re-read and classified, never inferred from local queue order。

Canceling a public lifecycle future stops only work before its irreversible CAS。After a lifecycle CAS is sent,
response uncertainty is resolved by reading the head；the API never reports a known rollback merely because the
caller deadline elapsed。

## 11. Operation Matrix by State

| Operation | `ACTIVE` | `SEALED` | `DELETING` | `DELETED` |
| --- | --- | --- | --- | --- |
| get metadata | yes | yes | yes | yes |
| append/acquire session | yes | reject | reject | reject |
| read/resolve | yes | yes | reject | reject |
| trim | yes | yes | reject | reject |
| seal | transition | idempotent | reject | reject |
| delete | transition | transition | resume | idempotent |

`CREATING` is not returned by normal Phase 1 create-or-get today；if observed, no Phase 1.5 operation adopts it as
active。F2 projection publication requires a canonical empty `ACTIVE` snapshot。

## 12. Close Semantics

Closing a facade does not close shared `StreamStorage` and therefore does not stop core recovery。Runtime-level
`StreamStorage.close()`：

1. stops new append/recovery/lifecycle admission；
2. lets accepted original/recovery/lifecycle runners use the remaining `shutdownGrace`；
3. cancels scheduled retries only after the grace expires；
4. completes public waiters once with `STORAGE_CLOSED` while preserving their last append certainty/attempt ID；
5. releases buffers/permits and closes metadata/WAL resources in dependency order。

Forced runtime shutdown does not claim that an uncertain durable attempt is safely retryable。Restart relies on
head/commit truth, not retained process memory。

## 13. Required Tests

P15-M4 requires deterministic fake and real-Oxia cases for：

- permit reservation before WAL IO and no eviction under global exhaustion；
- every legal/illegal outcome + attempt-ID combination；
- original runner response loss followed by recovery, including quiescence before resubmit；
- a found commit, direct resubmit, complete non-commit proof and permanent identity/chain failure；
- history older than multiple `maxCommitChainScan` pages with monotonic continuation progress；
- a later remote append while scanning cannot cause false commit/non-commit；
- concurrent recovery callers and timeout views join one runner；
- terminal cache idempotence, expiry and bounded memory；
- close/retry timer races with one terminal signal and no retained resource leak；
- append-before-seal, seal-before-append, append-before-delete and remote CAS races；
- crash/response loss at both delete CAS steps and idempotent resume；
- state operation matrix, including readable sealed streams and terminal deleted streams；
- lifecycle option cancellation before/after irreversible request；
- no lifecycle path invokes ObjectStore delete or object listing。
