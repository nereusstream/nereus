# Phase 1 Final Code And Design Review (2026-07-11)

This review was performed after M8 against the public API, core state machines, fake/real Oxia adapters,
Object WAL implementation, ordinary tests, Docker-backed gates, publishing coordinates, and the active
Phase 1 documents. It is a post-completion hardening pass, not a new milestone.

## 1. Resolved Findings

### P1: Oxia watch saturation could deadlock metadata reads

`OxiaJavaClientBackend` originally scheduled watch callbacks on the same fixed executor used by blocking
`SyncOxiaClient` calls. A callback calls `getHead` and waits for another task on that executor; enough
concurrent callbacks could occupy every worker and starve the queued gets. Requests, metadata operations,
and watch delivery now use separate executors. A focused single-thread regression test proves that a watch
callback can synchronously read metadata without starving the request executor.

### P1: `scanOffsetIndex(limit)` did not bound Oxia work

The real adapter previously listed every key in the requested range, fetched each value separately, and
only then applied the Java stream limit. A small read against a long-lived stream therefore had unbounded
key-list memory and N+1 metadata requests. The adapter now uses Oxia `rangeScan` and stops/ closes its
iterator as soon as `limit` values have been decoded. The fixed-width `Long.MAX_VALUE/~` upper endpoint is
unchanged and still includes every legal generation at the largest offset.

### P1: core assembled torn stream-head snapshots

`ReadResolver` and `DefaultStreamStorage.getStreamMetadata` independently read metadata, committed end, and
trim views. Concurrent append/session/trim CAS operations could produce a combination that never existed in
one stream-head version. `OxiaMetadataStore.getStreamSnapshot` now hydrates `StreamMetadataSnapshot` from one
authoritative head read. The snapshot validates stream identity, equal metadata versions, and
`trimOffset <= committedEndOffset`; fake and real adapters share the contract.

### P1: local stream state could grow without a bound

The positive offset-index cache merged records indefinitely, expired streams were removed only when read
again, append sessions stayed cached per stream, watch registrations stayed resident, and successful append
lanes were never removed. `StreamStorageConfig.maxCachedStreams` now bounds cache/session/watch stream
cardinality. Offset-index records are additionally capped per stream by `maxCommitChainScan`, with fresh
scan results retained first. Append lanes are reference-counted and removed after terminal known outcomes;
lanes with `MAY_HAVE_COMMITTED` or `KNOWN_COMMITTED` suspension remain resident intentionally so a new
physical append cannot bypass the unknown attempt.

The adapter's fixed operation/watch pools also used unbounded queues. `OxiaClientConfiguration` now exposes
`maxPendingOperations`; operation saturation fails with retriable `BACKPRESSURE_REJECTED`, while watch hints
may be dropped and shutdown races remain `STORAGE_CLOSED`.

### P2: close and derived-audit races could report the wrong surface

An executor shutdown racing method admission could throw `RejectedExecutionException` synchronously,
including from append methods that require a structured outcome. The real adapter now returns failed
futures with `STORAGE_CLOSED`, and append failures carry `KNOWN_NOT_COMMITTED`. Stream-name audit failure
after a successful deterministic head create is also isolated as repairable audit state instead of turning
an already-created stream into an apparent create failure.

The real adapter also now validates durable key/value identity at every typed read boundary. A head under
another stream key，an index whose stream/end/generation does not reconstruct its key，or mismatched
commit/object records fail immediately as `METADATA_INVARIANT_VIOLATION` instead of entering core or being
misread as a repairable gap。

### P1: package and Maven coordinates did not match an owned domain

The repository had `io.nereus.*` packages and an `io.nereusstream` Maven group. Before any tag or stable
release, all Java source/resource paths and examples were migrated to `com.nereusstream.*`, the Maven group
to `com.nereusstream`, and the POM project URL to `https://nereusstream.com`. SCM remains on the
`nereusstream` GitHub organization. ADR 0003 records the compatibility decision. Metadata and WAL golden
tests verify that durable bytes did not change.

The `nereus-bom` platform has an independent publication block, so it also now declares the same website、
Apache-2.0 license and GitHub SCM metadata instead of generating a sparse POM. Generated API and BOM POMs
are verified during this review。

## 2. Added Regression Coverage

- bounded Oxia iterator consumption and close-at-limit;
- watch callback metadata read on separate one-thread executors;
- bounded operation-queue saturation and error classification;
- durable stream-head key/value identity mismatch rejection;
- one-head fake/real metadata snapshot contract;
- offset-index cache per-stream record cap and stream-cardinality eviction;
- append lane release after known terminal outcomes and retention after a known-committed suspension;
- all ordinary and Docker-backed tests under the `com.nereusstream.*` namespace.

## 3. Accepted Phase 1 Boundaries

The review does not remove the documented Phase 1 compromises: full-slice checksum read amplification,
no producer-sequence deduplication, no production object deletion/GC, single-stream core planning over a
multi-stream WAL mechanism, and advisory cancellation that reports a structured terminal exception instead
of relying on `CompletableFuture.isCancelled()`.

## 4. Verification

The release gate remains:

```text
./gradlew phase1FinalCheck --rerun-tasks
```

Result：all 29 tasks passed. Ordinary suites contain 23 API、45 core、63 metadata and 23 object-store tests；
Docker-backed suites contain 5 production-adapter、5 capability-spike and 2 final
core/Oxia/Object-WAL end-to-end tests. API and BOM generated POMs were also checked for
`com.nereusstream`、`https://nereusstream.com`、Apache-2.0 and the GitHub SCM coordinates。

### CI timing follow-up

The append upload-timeout regression originally used a 40 ms end-to-end deadline and then dereferenced the
prepared object unconditionally. On a loaded GitHub Actions runner the deadline could expire before the
asynchronous prepare stage ran, so the test failed with its own `NullPointerException` rather than testing
the intended during-upload boundary. The test now waits for the writer's `uploadStarted` latch, uses a
two-second deadline, and only then asserts `TIMEOUT + KNOWN_NOT_COMMITTED` plus absence of an object
manifest. Production deadline and commit behavior are unchanged.
