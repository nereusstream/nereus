# 07 Implementation Contract Checklist

本文是 Phase 1 开始写代码前的实现合同。它不替代前面的设计文档，只把已经收敛的硬约束集中
成 checklist，避免实现时靠记忆补语义。

## 1. Supported Surface For Phase 1

| Area | Phase 1 supports | Phase 1 rejects or defers |
| --- | --- | --- |
| Stream lifecycle | exact opaque `StreamName`, deterministic full-hash `StreamId`, create-or-get as `ACTIVE`, read/trim `SEALED` | public seal/delete API, parsing Pulsar topic syntax in L0 |
| Durable key helpers | `nereus-api` shared key/hash helpers used by metadata and object modules; `.`/`..` are encoded; writer run id has at least 128 bits entropy | duplicate local key encoders, raw cluster in paths, truncated hashes without migration plan |
| Storage profile | metadata/API reserve all names；execution accepts only canonical `OBJECT_WAL_SYNC_OBJECT` (`OBJECT_WAL` alias) | BK/async/local execution；fail with `UNSUPPORTED_STORAGE_PROFILE` before WAL IO |
| Durability | `WAL_DURABLE_AND_INDEX_COMMITTED` only；success includes WAL durable、head commit、generation-0 index/marker confirmation | `WAL_DURABLE` execution；fail with `UNSUPPORTED_DURABILITY_LEVEL` before WAL IO |
| Payload | `OPAQUE_RECORD_BATCH`, one record per entry, one read batch per opaque entry | non-opaque public append, opaque entry with `recordCount > 1`, concatenating opaque entries |
| Zero-byte records | empty payload consumes one offset and can be returned after byte budget is exactly consumed | read loops that advance only by payload bytes or stop solely on `remainingBytes == 0` |
| Projection | public append returns empty `ProjectionRef` | non-empty public `projectionHints` without a durable mapping |
| WAL object shape | `WalObjectWriter` supports multi-slice; core planner may initially flush one append work item per object | implicit request splitting inside writer |
| WAL object identity | per-writer-run object sequence is monotonic and never reused; same physical retry reuses same object id | sequence rewind after timeout/failure, caller-level retry reusing object id |
| WAL object limits | sizing pass computes final encoded length; `maxObjectBytes` is hard, `targetObjectSizeBytes` is advisory | checking only payload bytes or treating target as the hard limit |
| Object upload deadline | `WalWriteOptions.uploadTimeout` reaches `PutObjectOptions.timeout` and concrete client calls | caller-only timeout wrapper with unbounded object-store upload |
| Read memory boundary | full-slice reads guarded by `maxConcurrentObjectReads` and `maxReadBufferBytes` | unbounded full-slice allocation for clipped reads |
| WAL format metadata | common header length, major/minor version, section envelopes, writer version in manifest | ad hoc raw section concatenation without version/checksum headers |
| WAL compression | `CompressionType.NONE` | `ZSTD` with `UNSUPPORTED_FORMAT` |
| Entry index location | writer emits and reader supports `OBJECT_FOOTER` | `INLINE` and `INDEX_OBJECT` with `UNSUPPORTED_FORMAT` |
| Checksum domains | caller payload checksum, WAL canonical object checksum, storage checksum, slice checksum, entry-index/footer checksums | treating distinct checksum domains as interchangeable |
| Metadata truth | stream head + reachable commit log are append truth；offset index/marker are repairable read/replay materializations | treating derived index, manifest, watch, or list as the append linearization truth |
| Object metadata | manifest/reference as repairable audit/GC inputs | cross-stream/object atomic producer ack |
| Read path | offset-index-driven resolve, full-slice checksum before clipping | object list, manifest-only reads, negative cache |
| Read amplification | explicit metrics for full-slice bytes, entry-index bytes, returned bytes, amplification, and read backpressure | hiding 16 MiB-to-100 byte reads as ordinary object read volume |
| Offset-index cache | positive scan results only; watch events invalidate but never populate cache | EOF caching, watch-created positive records |
| Trim | monotonic stream-head low-watermark CAS，deadline/cancel response，explicit cache invalidation | offset-index/object deletion，treating timeout as rollback proof |
| Orphan diagnostics | caller-supplied object id，manifest + repaired reachable references，diagnostic-only classification | object listing in correctness path，any Phase 1 deletion authorization |
| Local object cleanup | test-only cleanup under injected `LocalFileObjectStore` root | production object delete in Phase 1 |
| Protocol integration | protocol-neutral API and metadata labels | Pulsar, KoP, ManagedLedger classes |

## 2. Must-Have Before Real Adapter Work

Before implementing the real `OxiaMetadataStore` adapter:

1. Keep the M0.5 Oxia Java client capability spike from
   `06-metadata-oxia-position-and-pulsar-reference.md` compiling and runnable.
2. Treat the current M0.5 result, `NOT_SUPPORTED_BY_PUBLIC_JAVA_API`, as a hard input: same stream key
   group conditional multi-write is not available through the selected public Java client API.
3. Use the documented stream-head single-key CAS protocol from `02-oxia-metadata-and-commit.md` for M2
   fake metadata and M4 core append.
4. Do not make `FakeOxiaMetadataStore` stronger than the selected public Oxia Java client API.
5. Prove every get/put/scan/watch/head-CAS call passes the expected partition key.
6. Decide and freeze the Phase 1 `MetadataCodecRegistry`.
7. Add golden-byte codec tests for every Phase 1 metadata record type.
8. Make `FakeOxiaMetadataStore` and the real adapter share the same metadata codecs.
9. Freeze `nereus-api` deterministic id/key helpers: exact `StreamName.value()` hashing, full `streamId`
   hash, key-safe component escaping, writer/run hash components, and 19-digit offset/generation keys.

## 2.5 Build And Scaffold Gates

M0 scaffold status before coding state machines:

- done: Phase 0 `StreamStorage` byte-array skeleton is removed and the full Phase 1 API surface exists；
- done: the existing `AppendResult` record is expanded instead of adding a parallel result type；
- done: `nereus-core` is wired to `nereus-metadata-oxia` and `nereus-object-store` with Gradle `api`；
- done: `nereus-metadata-oxia` exposes a `java-test-fixtures` boundary for the future fake metadata store；
- done: `phase1Check` exists and keeps `checkPhase0` passing；
- done: the dependency guard is scoped to Phase 1 L0 modules: `nereus-api`, `nereus-core`,
  `nereus-metadata-oxia`, and `nereus-object-store`.
- done: M0.5 adds `:nereus-metadata-oxia:oxiaCapabilitySpike` as a Docker-backed Testcontainers task；
- done: `phase1Check` compiles `:nereus-metadata-oxia:compileOxiaCapabilitySpikeJava` but does not run
  Docker；
- done: M0.5 records the selected public Oxia Java API result as
  `NOT_SUPPORTED_BY_PUBLIC_JAVA_API` for multi-key conditional write.
- done: M1 adds `ApiLimits` and `MetadataCanonicalizer` in `nereus-api` so stream attributes, entry
  attributes, schema refs, and map ordering use one shared API-boundary contract；
- done: M1 `AppendBatch` rejects non-opaque public appends, opaque entries with `recordCount > 1`,
  non-empty `projectionHints`, duplicate or oversized schema refs, invalid event-time ranges, empty
  batches, and mismatched optional CRC32C payload checksums；
- done: M1 `EntryIndexRef` validates location-specific field combinations instead of allowing ambiguous
  inline/object index references；
- done: M1 physical object ranges reject negative values and `offset + length` overflow in API values
  that expose object byte ranges；
- done: M1 `AppendResult` requires positive visible counts, so an append success result cannot represent
  an empty range；
- done: `./gradlew :nereus-api:test`, `./gradlew phase1Check`, and `./gradlew check` pass after M1.
- done: M2 foundation adds `OxiaMetadataStore`, `OxiaKeyspace`, metadata records, codec
  envelope/registry scaffolding plus `Phase1MetadataCodecs`, and a test-fixture `FakeOxiaMetadataStore`
  that uses stream-head single-key CAS rather than multi-key atomic commit；
- done: M2 fake store tests cover deterministic create-or-get, append session renew,
  live-owner fencing/expiry/steal/reacquire,
  `commitStreamSlice`, derived-index materialization, failure after head CAS before derived-index
  materialization, repair recovery, object-reference repair, watch drop/duplicate/stale/collapsed/
  reconnect events, fixed-width offset scan ordering, offset conflict classification, canonical commit
  identity/replay hardening, metadata-record range overflow validation, committed-slice-marker-first replay,
  post-commit object-audit failure injection, and adapter-private partition-key helper enforcement；
- reviewed 2026-07-07: no P0 was found that invalidates the current stream-head CAS direction；
- done: M2 codec/validation pass added entry-index decoded shape validation, strict UTF-8 metadata decode,
  per-record codec round-trip/error-path/golden-byte tests, and deterministic map-order coverage；
- done: M2 fake store persists stored metadata values as encoded envelopes through `Phase1MetadataCodecs`；
- done: M2 linearizability test matrix covers deterministic compatible head-version interleavings
  (renew/trim), bounded repair retry progress after budget exhaustion, commitVersion cross-record
  equality, stale-epoch fencing priority over an also-present offset conflict, and commit-log retry；
- M2 baseline is complete; its pre-hardening gate run passed `./gradlew :nereus-metadata-oxia:test`,
  `./gradlew phase1Check`, and `./gradlew check`.
- done in pre-M4 source hardening: structured `AppendOutcome`, shared manifest validation, bounded replay
  exhaustion classification, head-derived orphan-intent reuse validation, dense chain tuple validation,
  target-bound repair continuation tuple tests, logical-offset overflow rejection, and monotonic
  object-reference rebuild checks were added；the complete post-hardening Gradle gates passed on
  2026-07-11.
- done: M3 adds the production `ObjectStore` API, local test-fixture object store, WAL write/read records,
  CRC32C helper, WAL binary layout encoder/decoder, `DefaultWalObjectWriter`, and
  `DefaultWalObjectReader`；
- done: M3 WAL writer computes final encoded object length before upload, treats `maxObjectBytes` as the
  hard bound, propagates `uploadTimeout`, separates WAL canonical checksum from storage checksum, and
  returns neutral descriptors without importing Oxia metadata records；
- done: M3 WAL reader supports only `MULTI_STREAM_WAL_OBJECT` + `OPAQUE_RECORD_BATCH` +
  `OBJECT_FOOTER`, reserves full slice-plus-index bytes before object IO, verifies slice/index checksums,
  clips after verification, and reports amplification byte counts through `WalReadObserver`；
- done: M3 tests cover one-slice and multi-slice round trips, force-single-stream validation before
  upload, sizing failure before upload, checksum corruption, entry-index golden bytes, read-resource
  rejection before object IO, checksum-consistent invalid WAL/index metadata, basic local path traversal
  rejection, duplicate `ifAbsent`, and test-only cleanup.
- done: M3 review-found blockers were fixed: multi-range read byte-budget classification now stops after
  previously returned data instead of returning `READ_LIMIT_TOO_SMALL`, and local-store final/parent
  symlink escape is rejected before put/read/head.
- done on 2026-07-10: `./gradlew :nereus-object-store:test phase1Check check` passed after the blocker
  fix pass；M3 is complete。
- done on 2026-07-11: `./gradlew :nereus-api:test :nereus-metadata-oxia:test` and
  `./gradlew phase1Check check` passed after the final pre-M4 hardening；the M4 entry gate opened.
- done on 2026-07-11: M4 adds exact WAL `prepare/upload`, `StreamStorageConfig`, session manager,
  per-stream append lanes, resource/deadline/cancellation guards, WAL -> manifest -> stream-head commit ->
  result orchestration, and structured lane suspension；focused `:nereus-core:test` and
  `:nereus-object-store:test` pass.
- done on 2026-07-11: final `./gradlew phase1Check check --rerun-tasks` executed 28 tasks successfully；
  the L0 dependency guard passed and Oxia capability-spike sources compiled without starting Docker.
- done on 2026-07-11: M5 adds index-only resolve, positive cache/watch invalidation, bounded derived-index
  repair, generation selection, full-slice read resource guards, exact `WalReadResult` amplification stats,
  decreasing read deadlines, stale-cache refetch and trim/EOF/gap classification；focused core and
  object-store suites pass 32 and 23 tests respectively.
- done on 2026-07-11: the post-M5 `./gradlew phase1Check check --rerun-tasks` gate executed 28 tasks
  successfully；the L0 dependency guard passed and capability-spike sources compiled without Docker.

## 3. Stop-The-Line Conditions

Stop implementation and update design first if any of these are discovered:

- The Phase 0 skeleton API remains as the only `StreamStorage` surface after M0.
- Core tests need fake metadata classes from production packages instead of test fixtures.
- A design or fake implementation assumes Oxia public Java client can do single-key-group conditional
  multi-write without a new passing spike proving that exact API.
- M4 core append starts without using the documented stream-head CAS plus commit-log protocol.
- M4 can complete an exceptional append without a machine-readable `AppendOutcome`, or derives commit
  certainty from `ErrorCode`/message text alone.
- M4 acknowledges a BK/async profile or `WAL_DURABLE` request instead of rejecting it before WAL IO.
- Any future `WAL_DURABLE` design attempts to return before a stable stream-head commit or without a
  recoverable primary read target.
- The real adapter cannot route all stream-scoped operations with `PartitionKey(streamId)`.
- Offset index scan cannot preserve 19-digit zero-padded offset/generation ordering.
- commit protocol cannot assign a durable `commitVersion` that can be validated across stream head,
  commit-log, offset-index, and committed-slice state.
- Decoded head/commit/index/marker/reference records can carry an impossible head anchor，zero
  commitVersion，a non-dense logical range，cumulative size below logical bytes，or a zero-length WAL slice
  into append/replay/read state machines.
- L0 code needs to parse Pulsar topic syntax or use a human-readable alias as the durable `StreamId`.
- Metadata and object modules need separate implementations of durable key/hash helpers.
- Object keys or Oxia paths need raw `cluster`, raw `writerId`, or truncated writer/run hashes.
- A new physical WAL object attempt needs to reuse an object sequence from a failed, cancelled, or timed-out
  attempt.
- `commitStreamSlice` would require object-scoped keys or multi-key Oxia atomicity in the producer-ack
  linearization path.
- A new `commitStreamSlice` path would accept a manifest slice already marked `VISIBLE` while the
  stream-scoped committed-slice marker is missing without first proving replay through the committed chain.
- Fake or real metadata bypasses `Phase1ObjectManifestValidator`，or accepts duplicate slice ids/ordinals，
  non-contiguous ordinal/list order，cross-stream slices，overlapping/out-of-object slice ranges，or
  unsupported Phase 1 format/state.
- Replay/repair limits only newly written records rather than every commit-log record scanned，or a bounded
  repair retry restarts from head instead of using its continuation cursor.
- Marker-missing replay walks history for a normal new append whose `expectedStartOffset` equals current
  committed end，turning `maxCommitChainScan` into a stream lifetime write limit.
- Historical replay keeps scanning after a different reachable range reaches/crosses the request's
  expected start，and reports budget exhaustion after the chain already proved the request absent.
- An existing orphan commit can be reused without matching the current head's predecessor, offset range,
  cumulative size, and next commitVersion.
- A repair continuation does not retain the original observed head and exact expected next
  `(offsetEnd, cumulativeSize, commitVersion)` tuple, or a broken chain can end below the target without an
  invariant failure.
- Object-reference repair can silently omit an existing visible reference or accept a reachable commit
  whose object/writer/checksum/slice identity conflicts with the manifest.
- Read path would need object manifest to resolve visibility or checksum.
- A watcher callback exception can fail or reclassify an already-applied metadata mutation，or new watch
  registration succeeds after metadata store close.
- WAL canonical object checksum and object-store storage checksum have to be collapsed into one field.
- WAL object sections cannot carry type/version/length/checksum from the first encoder.
- WAL writer cannot know or enforce final encoded object length before `putObject`.
- Object-store `putObject` cannot receive a concrete timeout/deadline.
- Backpressure cannot release in-flight count or buffered-byte reservations on every terminal path.
- Full-slice read cannot be guarded by read permits and byte reservations before object IO starts.
- Read amplification cannot be measured as full-slice bytes versus returned payload bytes.
- `WalObjectReader` would return `READ_LIMIT_TOO_SMALL` after at least one prior batch was already selected
  in the same read call. That error is reserved for the first readable positive entry when no record has
  been returned.
- WAL binary object/section encoding would use Java enum ordinal instead of fixed durable ids.
- Writer needs a non-`OPAQUE_RECORD_BATCH` payload format, `CompressionType.ZSTD`, `INLINE`, or
  `INDEX_OBJECT` before their contracts are designed.
- Local object-store fixture can read, head, or write through a symlink that escapes the injected root.
- A Phase 1 module needs Pulsar, BookKeeper, Kafka, or KoP classes.
- Object deletion appears necessary for a Phase 1 correctness test.
- `WalObjectWriter`, reader, or manifest code becomes single-stream-only because the initial core planner
  uses `forceSingleStreamObject=true`.

## 4. Commit Contract

Producer ack is allowed only after:

1. WAL object bytes are durable.
2. Object manifest is written or idempotently confirmed.
3. `commitStreamSlice` succeeds through the stream-head CAS protocol:
   - stream state is `ACTIVE`;
   - append epoch/token match;
   - head committed end equals `expectedStartOffset`;
   - immutable `StreamCommitRecord` is written or idempotently confirmed;
   - stream head CAS advances committed end, cumulative size, commitVersion, and lastCommitId;
   - compatible head CAS conflicts caused only by same-writer renew, trim, or other non-append head
     updates are retried with the latest head instead of fencing or reporting a condition failure;
   - for same-slice replay, the existing marker or reachable head-chain commit is validated and the
     original result is returned;
   - after marker-missing condition failure, the head chain is searched before classifying the error;
   - object manifest object id, writer id, writer run id, writer epoch, object key/type/format, aggregate
     visibility state, and slice fields match the commit request;
   - an existing commit intent matches both the canonical request and the current head-derived predecessor,
     offset range, cumulative size, and next commitVersion before CAS reuse;
   - every reachable-chain scan validates dense offset, cumulative-size, and commitVersion progression;
   - offset index and committed-slice marker for the committed record are materialized or idempotently
     confirmed before success is returned;
   - durable `commitVersion` is the same in stream head, commit-log, offset-index, and committed-slice
     records.

Every exceptional append carries `AppendOutcome`，including missing-stream/closed-store rejection。Before head send is `KNOWN_NOT_COMMITTED`；unconfirmed head
response or replay-proof budget exhaustion is `MAY_HAVE_COMMITTED`；confirmed head success followed by
index/result failure is `KNOWN_COMMITTED`。The implementation must not report either latter state as an
ordinary retryable conflict；same physical slice replay/repair must finish from the reachable commit record。
Unexpected runtime/validation failures are also structured，and certainty is monotonic after replay/head
proof。

Post-commit object reference and manifest state updates are best-effort CAS merges. Failure there does not
block ack，must be observable，and must be repairable from the stream-head commit chain and materialized
offset index。Runtime/validation failures at this boundary cannot escape as a known-not-committed append。

## 5. Read Contract

Read and resolve must follow this chain:

```text
trim/state check
-> cache positive offset-index records only, or scan Oxia
-> if scan shows a gap below stream head, repair derived indexes from commit-log and rescan
-> if repair scan budget is exhausted before target coverage, continue from returned cursor within read
   budget or fail retriably
-> choose highest non-tombstoned generation covering target offset
-> build ResolvedObjectRange from offset index only
-> read full slice payload and entry index
-> verify slice checksum over slicePayloadBytes || entryIndexBytes
-> clip to startOffset/maxRecords/maxBytes
```

Never infer EOF from cache. Empty scans are not cached.

Full-slice object reads must reserve read concurrency and full checksum-domain bytes before object IO.
Exhausted read limits fail with retriable `BACKPRESSURE_REJECTED`; they must not start a range read and
then rely on OOM or downstream throttling. Metrics must show slice payload bytes, entry-index bytes, and
payload bytes actually returned after clipping.

## 6. Timeout, Cancellation, And Close Contract

- Timeout/cancellation before WAL upload starts leaves no object and no offset index.
- Timeout/cancellation during or after upload may leave an orphan object but must not start a new offset
  commit after the operation has been timed out/cancelled.
- Timeout/cancellation after stream-head CAS is sent but not confirmed carries `MAY_HAVE_COMMITTED`.
- Timeout/cancellation after head CAS is known successful but before index materialization confirms carries
  `KNOWN_COMMITTED`.
- Both states must remain distinct from `KNOWN_NOT_COMMITTED` and must not surface as ordinary offset
  conflict, object upload failure, or generic metadata condition failure.
- Same physical slice retry can discover a successful commit through the committed-slice marker or the
  stream-head commit chain.
- Per-stream append sequencer must refresh after `OFFSET_CONFLICT` and must not choose new expected offsets
  while a prior commit final state is unknown.
- `close()` rejects new work, stops background tasks, waits up to `shutdownGrace` for irreversible in-flight
  work, then closes owned clients.
- Close is two-phase：all public coordinator admission closes first，then append/trim share one global grace
  budget rather than waiting one full grace period per coordinator.
- Trim timeout/cancellation has no `AppendOutcome` and is advisory：an already-issued head CAS can still
  succeed. Coordinator lifecycle follows the source metadata future；late success invalidates read cache.

## 7. Required Test Gates

`phase1Check` should not pass without:

- no Pulsar/KoP/BookKeeper dependency guard for Phase 1 modules;
- compiling the M0.5 Oxia capability spike without starting Docker;
- fake metadata invariant tests;
- stream-head CAS and commit-log reachability tests;
- compatible head-CAS conflict retry tests for same-writer renew and trim;
- derived offset-index repair tests for failure after head CAS;
- derived-index scan-budget exhaustion and continuation-progress tests;
- append outcome certainty tests for pre-head、unconfirmed-head and known-head failures;
- shared manifest-validator tests for state/format/stream/id/ordinal/bounds and audit-state idempotency;
- exact stream-name hash and deterministic stream-id tests;
- shared key/hash helper parity tests across Oxia keyspace and object key generation;
- non-opaque payload format rejection tests;
- M7 real Oxia shared-contract and independent Testcontainers integration tests before final Phase 1 exit;
- WAL object golden encode/decode tests;
- WAL section envelope and format-version manifest tests;
- checksum-domain tests for WAL canonical checksum versus storage checksum;
- metadata golden-byte tests for absent optional fields without null encodings;
- deterministic map encoding tests and zero-byte record read tests;
- canonical schema refs order and duplicate rejection tests;
- multi-slice WAL writer tests plus `forceSingleStreamObject` validation tests;
- WAL sizing tests for payload-plus-index/footer/header limits, advisory target behavior, and overflow;
- object upload timeout propagation tests;
- backpressure reservation release tests for success, validation failure, upload failure, and cancellation;
- read permit/buffer reservation tests for success, read failure, checksum failure, timeout, and
  cancellation;
- read amplification metric tests for small clipped reads from large resolved slices;
- checksum corruption tests for payload, entry index, footer, header, and whole object;
- stream `commitVersion` monotonicity and cross-record equality tests;
- concurrent same-slice replay condition-race tests;
- append timeout/cancellation `AppendOutcome` tests;
- object reference repair tests;
- trim monotonic/bounds，read-cache invalidation，physical-retention，timeout/cancel and close-wait tests;
- orphan diagnostic tests proving manifest-only candidates remain invisible/no-delete and reachable
  post-head commits repair missing indexes before classification;
- offset index cache missed/duplicate/collapsed/out-of-order watch tests;
- local object store path traversal, atomic-write, and test-only cleanup tests.
