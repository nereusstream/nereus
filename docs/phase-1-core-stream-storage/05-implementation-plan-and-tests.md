# 05 Implementation Plan and Tests

本文把 Phase 1 代码落地拆成可执行步骤和测试矩阵。M0-M4 已完成，M5 Resolve/Read 是下一入口；
2026-07-11 M4 完成后的 `./gradlew phase1Check check --rerun-tasks` 运行 28 个执行任务并全部通过；
后续每个里程碑完成时都要同步更新本文件，确保文档描述的是当前实现最新版。

## 1. Milestones

### M0 Phase 0 scaffold migration

M0 implementation status:

- `nereus-api` has the full Phase 1 `StreamStorage` method surface；
- the old byte-array `append/read` skeleton methods have been removed；
- `AppendResult` has the Phase 1 fields for object/slice/checksum/schema/index metadata；
- `nereus-api` contains the M0 value-record shell for identity, append, read, resolve, trim, object refs,
  checksum, projection, error model, and shared key/hash helpers；
- `nereus-core/build.gradle.kts` depends on `nereus-api`, `nereus-metadata-oxia`, and
  `nereus-object-store`；
- `nereus-metadata-oxia` applies `java-test-fixtures`; the future `FakeOxiaMetadataStore` implementation
  still belongs to M2；
- root `phase1Check` runs `checkPhase0`, L0 module tests, and `checkPhase1L0Dependencies`；
- `checkPhase1L0Dependencies` is a direct-dependency scaffold guard over the L0 module build files。

M0 migration tasks:

- done: replace the minimal `StreamStorage` interface with the Phase 1 API from
  `01-api-and-domain-model.md`；
- done: remove the old byte-array append/read shape instead of keeping it as a parallel API；
- done: expand `AppendResult` in place instead of introducing a second result type；
- done: add M0 shells for the missing `nereus-api` value records needed before core state machines；
- done: wire `nereus-core` to `nereus-metadata-oxia` and `nereus-object-store` using Gradle `api`；
- done: keep `nereus-managed-ledger`, `nereus-pulsar-adapter`, and `nereus-kop-adapter` outside the
  Phase 1 L0 dependency guard；
- done: prepare `nereus-metadata-oxia` test fixtures as the future fake metadata exposure path for
  `nereus-core` tests；
- done: add the root `phase1Check` task and a small L0 dependency guard。

Exit:

- `./gradlew phase1Check` passes and includes `checkPhase0`；
- `./gradlew :nereus-api:test :nereus-metadata-oxia:test :nereus-object-store:test :nereus-core:test`
  are wired without adapter modules；
- `checkPhase1L0Dependencies` proves the Phase 1 L0 module build files do not declare Pulsar,
  BookKeeper, Kafka, or Confluent dependencies。

### M0.5 Oxia capability spike

M0.5 implementation status:

- `gradle/libs.versions.toml` uses the official Oxia Java client coordinates
  `io.github.oxia-db:oxia-client:0.9.0`；
- `nereus-metadata-oxia` has an independent `oxiaCapabilitySpike` test source set and Gradle task；
- the spike uses `io.github.oxia-db:oxia-testcontainers:0.7.4`,
  `org.testcontainers:junit-jupiter:1.20.4`, and Oxia image `oxia/oxia:0.16.3`；
- root `phase1Check` compiles the spike source through
  `:nereus-metadata-oxia:compileOxiaCapabilitySpikeJava` but does not run Docker；
- `:nereus-metadata-oxia:oxiaCapabilitySpike` starts real Oxia through Testcontainers and writes
  `build/reports/oxia-capability-spike/summary.md` plus `summary.json`；
- the current public Java API probe result is `NOT_SUPPORTED_BY_PUBLIC_JAVA_API` for the original
  same-key-group conditional multi-write assumption；
- Phase 1 design has been updated to use stream-head single-key CAS plus immutable commit-log records and
  materialized offset-index/committed-slice indexes.

M0.5 spike coverage:

- partition-key routed put/get/list/rangeScan；
- single-key conditional put with `IfVersionIdEquals` and stale-version conflict mapping；
- offset-index style fixed-width numeric key ordering；
- sequence key generation with partition key；
- reflection over public `AsyncOxiaClient` and `SyncOxiaClient` APIs for transaction/multi-key/batch
  write primitives。

Exit:

- `./gradlew phase1Check` compiles the spike without requiring Docker；
- `./gradlew :nereus-metadata-oxia:oxiaCapabilitySpike` passes when Docker/Testcontainers is available；
- a passing spike with `NOT_SUPPORTED_BY_PUBLIC_JAVA_API` is still a valid M0.5 result, and M2/M4 must
  implement the redesigned stream-head CAS protocol rather than the original multi-key commit design；
- docs `06`、`07`、`08` record the exact dependency versions, container image, and stop-line result。

### M1 API validation hardening

Module:

```text
nereus-api
```

M1 implementation status:

- `nereus-api` now exposes `ApiLimits` for the three Phase 1 16 KiB encoded metadata limits:
  stream attributes, entry attributes, and schema refs；
- `MetadataCanonicalizer` provides the shared UTF-8-key map ordering, schema-ref canonical ordering,
  duplicate rejection, encoded-size guard, and unmodifiable defensive copies；
- `AppendEntry`, `StreamCreateOptions`, `StreamMetadata`, `AppendOptions`, `AppendBatch`, `AppendResult`,
  `ReadBatch`, and `ResolvedObjectRange` use the shared canonicalization helper where their API values can
  be copied into durable metadata；
- `AppendBatch` now enforces Phase 1 public append constraints at construction time:
  `OPAQUE_RECORD_BATCH` only, non-empty entries, `entryCount == entries.size()`, one record per opaque
  entry, empty `projectionHints`, canonical bounded `schemaRefs`, valid event-time bounds, and optional
  CRC32C payload checksum over concatenated entry payloads；
- `EntryIndexRef` now validates the `INLINE`, `OBJECT_FOOTER`, and `INDEX_OBJECT` field combinations and
  keeps byte-array defensive copies for inline index data；
- API values that expose physical object ranges now reject negative ranges and `offset + length`
  overflow, including `ObjectRange`, `EntryIndexRef`, `AppendResult`, `ReadBatch`, and
  `ResolvedObjectRange`；
- `AppendResult` now rejects empty success ranges by requiring positive `recordCount` and `entryCount`；
- no byte-array convenience append/read wrapper was added in M1. The only public durable surface remains
  the full Phase 1 API；
- M1 unit tests live under `nereus-api/src/test/java` and cover key encoding edge cases, deterministic
  stream id generation, offset ranges, checksum formatting, defensive copies, metadata limits, schema-ref
  canonicalization, append batch validation, entry-index reference validation, physical range overflow,
  and positive append-result semantics；
- verified commands:
  `./gradlew :nereus-api:test`, `./gradlew phase1Check`, and `./gradlew check`。

Tasks:

- review the M0 API shells against `01-api-and-domain-model.md` and tighten validation where M0 stayed
  intentionally lightweight；
- add constants for encoded metadata limits such as stream attributes, entry attributes, and schema refs；
- add deterministic/canonical ordering helpers for schema refs and map encodings where they belong in API
  helpers；
- add defaults or factories for common option values only where they do not hide correctness settings；
- add unit tests for `KeyComponentCodec`, `DeterministicIds`, `OffsetRange`, checksum validation, byte-array
  defensive copies, and basic value validation；
- keep all types protocol-neutral；
- decide whether a byte-array convenience wrapper is useful; if added, it must be a default wrapper over
  the full API and preserve Phase 1 opaque-entry rules。

Exit:

- done: `nereus-api` compiles；
- done: no Pulsar/KoP dependencies in Phase 1 L0 modules, verified by `checkPhase1L0Dependencies`；
- done: API tests cover invalid ranges, blank ids, empty batches, key encoding edge cases, stream id
  generation, checksum formatting, byte-array defensive copies, schema-ref canonicalization, metadata
  encoded-size rejection, physical range overflow, positive append-result semantics, and `EntryIndexRef`
  shape validation。

### M2 Fake metadata and Oxia key model

Module:

```text
nereus-metadata-oxia
```

M2 foundation implementation status:

- production `nereus-metadata-oxia` now contains:
  `OxiaMetadataStore`, `OxiaKeyspace`, `PartitionKey`, `MetadataWatcher`, `WatchRegistration`,
  `CommitSliceRequest`, `CommitSliceResult`, `DerivedIndexRepairCursor`, `DerivedIndexRepairResult`, and
  shared `Phase1ObjectManifestValidator`；
- metadata record classes exist under `io.nereus.metadata.oxia.records` for stream head/name/metadata,
  append session, commit-log, committed end, offset index, entry-index reference, object manifest,
  object reference, committed slice, and trim；
- codec code exists under `io.nereus.metadata.oxia.codec`: `MetadataRecordCodec`,
  `MetadataCodecRegistry`, `MapMetadataCodecRegistry`, `MetadataRecordEnvelope`,
  `MetadataCodecException`, and `Phase1MetadataCodecs`；
- `FakeOxiaMetadataStore` lives in test fixtures and implements the stream-head single-key CAS model. It
  does not expose a multi-key commit primitive；
- fake store supports deterministic create-or-get, append session acquire/renew, object manifest put/get,
  `commitStreamSlice`, derived offset-index/committed-slice materialization, repair after head CAS,
  offset-index scan, committed-end view, trim view/update, object references produced from successful
  commits, object-reference repair from stream-head commit reachability, and watch registration；
- fake watch controls can drop the next event, duplicate the next event, collapse one event into the next,
  emit reconnect-before-current, or deliver a stale event before the current event；
- current tests cover key encoding and offset ordering, metadata envelope corruption/truncation,
  strict metadata UTF-8 decode, per-record codec round-trip/error-path/golden bytes,
  fake-store codec-backed value persistence,
  deterministic stream id create-or-get, session renew/fencing/expiry/steal/reacquire, commit materialization, partition-key access
  recording, failure after head CAS before derived index, repair-derived-index recovery, same-slice retry,
  object-reference repair, manifest-only slices not becoming visible, watch drop/duplicate/stale/collapsed/
  reconnect events，watch-callback failure isolation/closed-store rejection，offset conflict classification, commit identity delimiter/event-time/projection
  coverage, metadata record range overflow rejection, committed-slice-marker-first replay, post-commit
  object-audit failure leaving visible data repairable, and adapter-private partition-key helper
  enforcement；
- 2026-07-07 review status: no P0 was found that would invalidate the stream-head CAS direction；
- 2026-07-08 M2 completion pass completed the remaining M2 test matrix:
  `sameWriterRenewIsCompatibleHeadVersionChange`, `sameWriterTrimIsCompatibleHeadVersionChange`,
  `repairBudgetExhaustionStopsAtMaxScannedCommitsAndContinues`, `commitVersionIsMonotonicAcrossSequentialCommits`,
  `staleEpochIsFencedBeforeOffsetConflict`, and
  `retryReusesStoredCommitLogAfterFirstAttemptFailedHeadCasAndSessionRenew`；
- these tests now use deterministic before-head-CAS interleavings for renew/trim, prove repeated bounded
  repair calls continue toward the target offset, and check `commitVersion` equality across stream head,
  commit-log, offset-index, and committed-slice records；
- 2026-07-10 pre-M4 hardening closed the latest review P1s：append failures now carry machine-readable
  `AppendOutcome`；manifest validation checks commit-eligible state、format、stream identity、unique
  slice id/ordinal and object bounds；replay budget exhaustion is `MAY_HAVE_COMMITTED` rather than
  `OFFSET_CONFLICT`；derived repair limits every scanned commit and returns a continuation cursor so bounded
  calls make progress even when newer indexes already exist；the follow-up review added stored object-id
  and aggregate/slice-state checks, head-derived orphan-intent validation, dense reachable-chain tuple
  validation, continuation `nextOffsetEnd/nextCumulativeSize/nextCommitVersion` checks, logical-offset
  overflow rejection, and monotonic object-reference rebuild protection；
- 2026-07-11 final pre-M4 verification passed after the cursor-tuple/chain-validation/object-rebuild
  follow-up：`./gradlew :nereus-api:test :nereus-metadata-oxia:test` and
  `./gradlew phase1Check check`；
- verified in the baseline M2 gate run before the latest pre-M4 hardening:
  `./gradlew :nereus-metadata-oxia:test`, `./gradlew phase1Check`, and `./gradlew check`；
- M2 foundation and pre-M4 hardening are done；this gate opened M4, which is now implemented.

M2 review follow-ups before exit:

- done: replace delimiter-concatenated `EntryIndexRef` commit identity with nested length-prefix canonical
  fields, because API string values can legally contain delimiter characters；
- done: include event-time range and projection identity consistently in the documented `commitId` inputs
  and same-slice replay validation；
- done: validate every canonical persisted identity field when an existing commit-log record or reachable
  head commit is reused；
- done: apply `offset + length` overflow checks to decoded metadata records, not only to public API request
  values；
- done: reject zero-length persisted WAL slices，non-dense `offsetEnd/recordCount` pairs，cumulative size
  below logical bytes，zero commitVersion in committed/index/marker/reference records，and impossible empty/
  non-empty stream-head anchors；
- done: correct offset-index codec golden sample from `0..10 + recordCount=2` to dense `recordCount=10`；
  correct commit golden sample to a valid non-first `previous-commit / 1..10 / recordCount=9` record；
  refresh only the affected envelope checksums/payload goldens；
- done: add a fake-store path that checks committed-slice marker replay before walking the head chain；
- done: add failure injection and tests for object manifest/reference audit update failure after the
  stream-head commit has succeeded；
- done: route actual runtime/validation audit-update failures through the same best-effort boundary and
  expose fake `objectAuditFailureCount`，so no post-head audit error escapes with false append certainty；
- done: add an internal monotonic append-stage guard so unexpected exceptions always carry structured
  certainty and no error after replay/head proof can be downgraded below `KNOWN_COMMITTED`；
- done: add adapter-private helper tests that can fail when a real Oxia get/put/scan/watch/head-CAS call
  omits or misroutes the required partition key。
- done: make metadata `EntryIndexReferenceRecord` enforce the same location-specific shape rules as
  API `EntryIndexRef`；
- done: freeze current `Phase1MetadataCodecs` with round-trip, wrong-type, unsupported-version,
  malformed-UTF-8, absent-field, deterministic-map-order, and per-record golden-byte tests；
- done: make fake metadata value persistence codec-backed instead of relying only on in-memory records；
- done: add the complete M2 linearizable single-process test matrix covering deterministic compatible
  head-version interleavings from same-writer renew and trim, bounded repair retry progress,
  commitVersion cross-record equality, stale epoch fenced before an also-present offset conflict, and
  commit-log retry after first-attempt head CAS failure；
- done: complete append-session negative coverage for live different-writer fencing，stale renew token，
  expired renew，expired different-writer steal policy，and same-writer post-expiry epoch/token rollover；
- done: add pre-M4 tests for append outcome certainty，cross-stream/invalid manifest rejection，manifest
  retry after audit-state changes，bounded replay exhaustion，and continuation-based repair progress；
- done: validate an existing orphan commit against the current head-derived predecessor/range/cumulative
  size/commitVersion tuple before reuse, including reuse after a same-writer session renew；
- done: bind repair pages to the original observed head and the exact next chain tuple, validate every
  scanned reachable record, and reject a tampered cursor or a chain that ends before the target；
- done: avoid historical replay walks for a normal new append at current committed end and for compatible
  renew/trim-only head updates，so `maxCommitChainScan` does not become a stream lifetime write limit；
- done: make object-reference rebuild reject manifest/commit identity conflicts and any result that would
  remove an existing visible reference；
- done: require manifest ordinal/list-order agreement and ordered non-overlapping positive slice ranges，
  reject duplicate stream/slice identities in object-reference records，and canonicalize reference-list
  order independent of commit arrival order；
- review-open: future real adapter must use the same `MetadataCodecRegistry` as the fake store。

Tasks:

- consume the M0.5 Oxia Java client capability result from
  `:nereus-metadata-oxia:oxiaCapabilitySpike`；
- treat `NOT_SUPPORTED_BY_PUBLIC_JAVA_API` as the current design input: fake metadata must not implement
  a stronger multi-key atomic commit than the public real adapter can express；
- implement `commitStreamSlice` around the documented Oxia-supported linearization point: one
  authoritative stream-head record CAS, immutable commit-log intent records, and repairable derived
  offset-index plus committed-slice records；
- add `OxiaKeyspace` using the shared `nereus-api` key/hash helpers；
- add key-safe path component encoding for cluster, stream names, object ids, slice ids, offsets, and
  generations；
- add versioned metadata record codec/envelope；
- add adapter-private Oxia client helpers that require partition key on get/put/scan/watch and commit；
- add metadata record classes；
- add `OxiaMetadataStore` interface；
- add `FakeOxiaMetadataStore` under test fixtures or test package；
- implement deterministic stream id generation from exact `StreamName.value()`；
- store `streamNameHash` in both by-name and stream metadata records；
- store stream attributes in stream metadata；
- store `logicalBytes` and slice checksum in offset index records；
- store slice-level schema refs in offset index records；
- store durable stream `commitVersion` in stream head, commit-log, offset-index, and committed-slice
  records；
- implement stream create-or-get；
- implement append session acquire/renew/fence, including `allowStealExpiredSession` semantics；
- implement object manifest put；
- use `Phase1ObjectManifestValidator` for both fake and future real-adapter manifest checks；
- implement object manifest/reference read and reference repair operations；
- implement `commitStreamSlice` head-CAS semantics；
- implement commit-log reachability checks for same physical slice replay；
- implement derived offset-index/committed-slice materialization and `repairDerivedStreamIndexes`；
- bound replay/repair by scanned commit-log records，and return explicit continuation rather than restarting
  every repair page from head；
- enforce that producer-ack cannot require object-scoped keys or multi-key Oxia batches；
- use overflow-checked offset and cumulative-size arithmetic；
- implement offset index scan；
- implement trim update；
- add `watchStream` boundary, with a no-op implementation allowed when watch is disabled。

Exit:

- fake store passes linearizable single-process tests；
- M2 implements the updated commit protocol design that does not depend on an unavailable public
  multi-key conditional write API；
- offset scan ordering is tested with offsets like `9`, `10`, `100`；
- stale epoch and offset conflict are distinguishable；
- fake store has no multi-key commit primitive and records/validates partition keys for
  get/put/scan/watch/head-CAS；
- fake store can inject failure after head CAS and before derived-index materialization, and same-slice
  retry/read repair can recover；
- fake and real codecs reject wrong record type, unsupported schema version, checksum mismatch, and
  truncated payloads；
- codec registry has golden-byte tests for every Phase 1 metadata record type；
- metadata golden bytes cover absent optional fields encoded as empty string or empty byte array, not null。

### M3 Object WAL local implementation

Module:

```text
nereus-object-store
```

M3 implementation status:

- production `nereus-object-store` now contains `ObjectStore`, operation options/results,
  `Crc32cChecksums`, and `RangeChecks`；
- `LocalFileObjectStore` is a `java-test-fixtures` test implementation with immutable put, head, exact
  range read, expected checksum validation, path traversal rejection, duplicate `ifAbsent` behavior, and
  `deleteAllForTesting()` under the injected temp root only；
- `DefaultWalObjectWriter` builds the full Phase 1 WAL object in memory, computes final encoded length
  before upload, rejects `maxObjectBytes` violations before `putObject`, copies `uploadTimeout` into
  `PutObjectOptions.timeout`, and verifies returned length/storage checksum before exposing a
  `WalWriteResult`；
- WAL binary encoding now uses fixed durable ids, common header, variable-length section envelopes,
  footer checksum, header checksum, canonical object checksum, exact-bytes storage checksum, deterministic
  slice descriptors, and `OBJECT_FOOTER` entry indexes；
- `DefaultWalObjectReader` supports `MULTI_STREAM_WAL_OBJECT`, `OPAQUE_RECORD_BATCH`, and
  `OBJECT_FOOTER`; it reserves full checksum-domain bytes through `ReadResourceGuard` before object IO,
  verifies slice and entry-index CRC32C, clips entries by requested offset/record/byte limits, and reports
  read amplification inputs through `WalReadObserver`；
- writer mechanism supports multi-slice and multi-stream objects. `forceSingleStreamObject=true` remains
  only a validation guard for the future core planner；
- object-store production code still has no delete/list correctness path, and `nereus-object-store` does
  not import Oxia metadata record classes.
- 2026-07-08 M3 review found blockers that were fixed with tests；2026-07-10 reran
  `./gradlew :nereus-object-store:test phase1Check check` successfully. M3 is complete. The dated review
  remains historical context in `11-m3-object-wal-review-2026-07-08.md`.

Tasks:

- done: add `ObjectStore` interface；
- done: add local filesystem test implementation；
- done: add a test-only cleanup helper for the local filesystem implementation or its fixture, scoped strictly
  to the injected test root；
- done: include write timeout in `PutObjectOptions` and propagate WAL upload timeout into concrete object-store
  calls；
- done: add WAL layout encoder/decoder；
- done: add WAL header/footer/checksum golden tests and object-bounds validation；
- done: use the shared `nereus-api` key/hash helpers for cluster, writer id, writer run id, and object key
  components；
- done: encode variable-length WAL sections with section headers and section checksums；
- done: add `WalObjectWriter`；
- done: require `WalObjectWriter` to support multi-slice requests even if the first core planner sends one
  append work item per object；
- done: add a WAL writer sizing pass that computes final encoded object length before buffer allocation and
  before `ObjectStore.putObject`；
- done: pass `maxObjectBytes` through `WalWriteOptions` and treat `targetObjectSizeBytes` only as a
  flush/grouping target；
- done: keep WAL canonical object checksum separate from object-store exact-bytes storage checksum；
- done: pass `writerRunIdHash` through WAL write request and object header；
- done: pass WAL format version and writer version into object header and manifest；
- done: add `WalObjectReader` for footer entry index；
- done: make `WalObjectReader` accept or use bounded read memory/concurrency guards supplied by core tests；
- done: add CRC32C checksum helper；
- done: return neutral `WalWriteResult` and `WrittenStreamSlice` descriptors only; do not import Oxia metadata
  record classes。

Exit:

- done: write one-slice WAL object and read it back；
- done: write multi-slice WAL object and read each slice by range；
- done: checksum mismatch test fails before metadata commit；
- done: no test uses object list for correctness。
- done: local object store cleanup can remove orphan files under the isolated test root without exposing
  production delete semantics。

M3 completion checklist:

- done: fix `DefaultWalObjectReader` so a positive entry after previously returned data exceeds remaining
  `maxBytes` by stopping, not by returning `READ_LIMIT_TOO_SMALL`；
- done: fix `LocalFileObjectStore` so final symlink and symlink-parent escapes are rejected before
  put/read/head；
- done: add tests for those two blockers；
- done: add tests for upload-timeout propagation, target-size advisory behavior,
  zero-byte entry reads, unsupported entry-index locations, non-zero source object offsets, descriptor
  bounds, range-read-past-EOF, and failed-write invisibility；
- done: wrap checksum-consistent invalid WAL object and entry-index metadata as `UNSUPPORTED_FORMAT`,
  with focused decoder tests；
- done on 2026-07-10: rerun `./gradlew :nereus-object-store:test phase1Check check`.

### M4 Core append path

Status: implemented on 2026-07-11 with fake Oxia metadata and local Object WAL fixtures.

Module:

```text
nereus-core
```

Tasks:

- done: add `DefaultStreamStorage`；
- done: add `AppendSessionManager` with supplied-session validation, cache, auto-acquire and pre-upload /
  pre-commit minimum-lease renewal；
- done: add `AppendCoordinator`；
- done: add per-stream append sequencer with local expected-offset initialization/advance, conflict
  invalidation, and uncertain/known-committed lane suspension；
- done: accept only canonical `OBJECT_WAL_SYNC_OBJECT` and
  `WAL_DURABLE_AND_INDEX_COMMITTED` in Phase 1；reject BK/async profiles and `WAL_DURABLE` before WAL IO；
- done: initially allow the core flush planner to send one append work item per WAL object with
  `forceSingleStreamObject=true`；
- done: validate `StreamStorageConfig` positive durations/limits, millisecond lease representation and
  object/read/append memory relationships；
- done: split WAL writer into exact `prepare` and `upload` boundaries while preserving `write` convenience；
- done: wire WAL writer, manifest put and metadata commit；
- done: map `WalWriteResult` to `ObjectManifestRecord` in `nereus-core`；
- done: pass canonical schema refs from append batch through WAL write result, commit request, offset index,
  resolve, and read；
- done: implement auto acquire session；
- done: implement append result assembly；
- done: keep Phase 1 default behavior of failing external offset conflicts instead of rebasing uploaded slices；
- done: map metadata/object failures into `NereusException`；
- done: preserve or assign `AppendOutcome` on every exceptional append completion；never infer commit certainty
  from `ErrorCode` or message text。
- done: count accepted/queued work separately from active prepared-object bytes；reserve the object hard cap
  before prepare and shrink to exact encoded length before upload；release both counters on every terminal path；
- done: revalidate the prepared hard cap and append-batch/slice identity at the core module boundary before
  upload instead of trusting an injected writer blindly；
- done: implement one end-to-end append deadline, advisory cancellation, pre/post-head certainty mapping,
  close rejection and `shutdownGrace` wait for accepted work；
- done: add focused tests for dense sequential/concurrent offsets, manifest/index/result mapping, profile and
  durability pre-IO rejection, upload timeout/cancellation, unconfirmed commit response, known-committed
  materialization failure, lane suspension, close rejection, config memory relationships, max-in-flight
  release, pre-upload lease renewal, explicit-session mode, prepared-slice validation and external offset
  conflict refresh without slice rebasing。

Exit:

- append returns Oxia-assigned offset range；
- committed end offset advances densely；
- uploaded but uncommitted object is not readable；
- stale token append fails and does not advance offset。
- a reserved profile/durability combination fails explicitly and cannot create an acknowledged append；
- producer success waits for reachable head commit and generation-0 offset-index/marker confirmation。

Implementation boundary after M4：`DefaultStreamStorage.read/resolve/trim` return explicit asynchronous
milestone errors until M5/M6. Constructor-injected clients are externally owned; owned-client factory and
watch/background-renew lifecycle wiring remain M6. The production Oxia adapter remains M7.

Verification completed on 2026-07-11:

```text
./gradlew :nereus-core:test --rerun-tasks
./gradlew :nereus-core:test :nereus-object-store:test --rerun-tasks
./gradlew phase1Check check --rerun-tasks
```

The final combined gate executed 28 tasks successfully, including all L0 tests, the dependency guard and
`compileOxiaCapabilitySpikeJava`. It did not launch the Docker-backed `oxiaCapabilitySpike` task, which
remains an independent environment check by design.

### M5 Resolve and read path

Module:

```text
nereus-core
```

Tasks:

- add `ReadResolver`；
- add optional `OffsetIndexCache`；
- implement generation selection；
- implement `read` using resolver and WAL reader；
- implement `maxConcurrentObjectReads` and `maxReadBufferBytes` reservations around full-slice reads；
- emit read amplification metrics: full-slice bytes downloaded, entry-index bytes downloaded, clipped
  payload bytes returned, and the difference；
- implement EOF vs gap distinction；
- implement trimmed offset check。

Exit:

- read after append returns exact bytes and offset metadata；
- read at committed end returns `endOfStream=true`；
- metadata gap below committed end fails as corruption；
- resolver ignores manifest-only object；
- read under exhausted read buffer/permit fails before object IO with retriable `BACKPRESSURE_REJECTED`；
- small clipped reads from large slices emit payload/index download and amplification metrics。

### M6 Trim and recovery boundaries

Module:

```text
nereus-core
```

Tasks:

- implement `trim`；
- add orphan object scanner interface but do not delete by default；
- add recovery documentation assertions in tests；
- add metrics hooks。

Exit:

- trim low-watermark advances monotonically；
- read below trim fails；
- object bytes remain after trim；
- manifest/object bytes without a reachable head commit are orphan/invisible；a reachable head commit with a
  missing index is committed and repairable。

### M7 Real Oxia adapter and integration gate

Module:

```text
nereus-metadata-oxia
```

Tasks:

- implement the production Oxia Java client binding using the selected public single-key APIs；
- reuse `Phase1MetadataCodecs` and `Phase1ObjectManifestValidator`，with no fake-only commit capability；
- run the same stream/session/manifest/head-CAS/replay/repair/watch contract suite against fake and real
  adapters；
- add Docker/Testcontainers tests for restart persistence，CAS conflict mapping，partition-key routing，
  replay scan exhaustion，repair continuation and post-head failure recovery；
- define production client lifecycle/configuration and exception-to-`NereusException`/`AppendOutcome` mapping。

Exit:

- real adapter passes the shared fake/real contract suite；
- a fresh adapter process recovers stream head、commit log and derived indexes from the Oxia container；
- no operation assumes multi-key conditional atomicity；
- `phase1Check` keeps Docker optional，while a separate real-adapter integration task is mandatory for the
  final Phase 1 release gate。

## 2. Core Test Matrix

### API/value tests

| Test | Expected |
| --- | --- |
| blank `StreamId` | constructor fails |
| negative offset range | constructor fails |
| negative read or resolve start offset | failed future with `INVALID_ARGUMENT` |
| `endOffset < startOffset` | constructor fails |
| stream attributes contain null or exceed 16 KiB encoded size | constructor or validation fails |
| append session option with blank writer, sub-millisecond/non-positive TTL, or millisecond overflow | constructor or validation fails before metadata write |
| decoded non-empty append session has zero epoch, leaseVersion, or expiry | record validation fails before fencing use |
| append with session for another stream or writer id | append fails before WAL upload |
| invalid `StreamStorageConfig` duration/count relationship | construction fails with `INVALID_ARGUMENT` |
| empty append batch | constructor fails |
| record count mismatch | constructor fails |
| append entry with null payload | constructor fails |
| zero-byte append entry | succeeds, consumes one offset, and can be read without infinite loop |
| append batch event time range invalid | constructor fails |
| append entry event time outside batch range | constructor fails |
| append batch checksum mismatch | constructor fails |
| schema refs contain null, duplicate tuple, or exceed 16 KiB encoded size | constructor fails |
| schema refs supplied in different orders | WAL metadata and offset index use the same canonical order |
| entry attributes contain null or exceed 16 KiB encoded size | constructor fails |
| non-empty `AppendOptions.tags` | not persisted in WAL or metadata |
| non-empty `projectionHints` in public append | constructor fails |
| `OPAQUE_RECORD_BATCH` entry with `recordCount > 1` | rejected until a decoder exists |
| public append with non-`OPAQUE_RECORD_BATCH` payload format | constructor fails before WAL upload |
| `EntryIndexRef` with `INDEX_OBJECT` but no object key | constructor or validation fails |
| `EntryIndexRef` with `INLINE` but no inline bytes | constructor or validation fails |
| physical object range where `offset + length` overflows | constructor or validation fails |
| append result with empty range or zero counts | constructor or validation fails |
| malformed checksum text | constructor or validation fails |
| `ReadOptions.maxRecords <= 0` or `maxBytes <= 0` | constructor or validation fails |
| `ResolveOptions.maxRanges <= 0` | constructor or validation fails |
| object-store operation timeout option is non-positive | constructor or validation fails |
| `WalWriteOptions.targetObjectSizeBytes <= 0`, `maxObjectBytes <= 0`, or target greater than max | constructor or validation fails |
| append after `StreamStorage.close()` | failed future with `STORAGE_CLOSED` |
| first readable entry exceeds `ReadOptions.maxBytes` | `READ_LIMIT_TOO_SMALL` |

### Metadata tests

| Test | Expected |
| --- | --- |
| concurrent create same stream name | same `StreamId` |
| create same stream name after restart | same deterministic `StreamId` |
| stream names differ only by caller-provided whitespace | different `streamNameHash` and different `StreamId` unless caller normalized before construction |
| deterministic stream id format | `s-` plus full `base32lower_nopad(sha256(UTF-8 exact StreamName.value()))`; no durable readable alias |
| shared key/hash helper parity | metadata keyspace and object key generation use the same helper outputs |
| create stream with attributes | `getStreamMetadata` returns the same attributes |
| create stream head | one `putIfAbsent` creates authoritative `StreamHeadRecord` |
| by-name cache write fails after head create | create-or-get still returns stream from deterministic head |
| stream name hash collision | non-retriable invariant/collision error |
| deterministic stream id collision with different metadata name | non-retriable invariant/collision error |
| existing sealed stream | create-or-get returns metadata; append/session acquire rejected; read/trim allowed |
| deleting/deleted stream metadata | append/read/trim rejected according to API state table |
| slice id contains `/` | committed-slice marker uses encoded component, not nested raw path |
| dynamic component starts with reserved `b32-` prefix | key encoder escapes it instead of storing raw |
| dynamic component is `.` or `..` | key encoder escapes it instead of emitting filesystem-special segments |
| dynamic component looks like a Windows drive designator | key encoder escapes it instead of emitting a platform-special segment |
| cluster contains `/` or starts with reserved `b32-` prefix | Oxia paths and object keys use `clusterComponent`, not raw cluster |
| writer id contains path separators | object id uses full key-safe writer/run hashes from shared helpers, not raw writer id |
| writer hash helper truncation attempt | rejected by tests unless a collision-budget migration document exists |
| writer run id entropy | generated from at least 128 bits of strong randomness per process incarnation |
| writer restarts in same timestamp bucket | object id changes because `writerRunIdHash` changes |
| object upload timeout then new attempt in same process | new attempt uses a fresh object sequence and object id |
| validation failure before upload | consumed object sequence is not reused by later attempts |
| acquire when missing | epoch created |
| acquire by same writer | session reused or renewed |
| acquire expired different-writer session without allow-steal | `APPEND_SESSION_EXPIRED` |
| acquire expired different-writer session with allow-steal | higher epoch |
| two live writers use same `writerId` | documented invalid configuration; test rejects duplicate in local harness when detectable |
| same writer live renew | epoch and fencing token stay stable |
| same writer renew after expiry | higher epoch and new fencing token |
| acquire by different writer before expiry | fenced |
| acquire by different writer after expiry | requires allow-steal; otherwise `APPEND_SESSION_EXPIRED` |
| renew after stream becomes non-active | rejected with `STREAM_NOT_ACTIVE` |
| commit with stale token | `FENCED_APPEND` |
| same-writer live renew during in-flight append | append can still commit with same epoch/token |
| head CAS loses to same-writer live renew | retries with latest head and preserves renewed session fields |
| head CAS loses to trim-only head update | retries with latest head and preserves updated trim offset |
| append starts with lease below minimum remaining | renews before WAL upload or fails without upload |
| lease falls below minimum before commit | renews before `commitStreamSlice` or fails without sending commit |
| commit with wrong expected offset | `OFFSET_CONFLICT` |
| offsetEnd overflow while committing | rejected before commit-log put and head CAS |
| cumulativeSize overflow while committing | rejected before commit-log put and head CAS |
| `CommitSliceRequest.expectedStartOffset + recordCount` overflow | constructor rejects it before metadata access |
| commit valid slice | commit-log record written, stream-head CAS advances, offset-index and marker materialized before ack |
| committed offset index value | contains logical bytes and slice checksum needed for resolve/read |
| committed offset index value schema refs | contains canonical schema refs derived from append batch |
| hydrated offset index record | exposes Oxia `metadataVersion` separately from durable `commitVersion` |
| valid sequential commits | `commitVersion` increments by one and matches stream head, commit-log, offset-index, and committed-slice records |
| commit same visible slice again after lost response | returns original commit result; no second offset range |
| new append at current committed end after history exceeds replay budget | commits normally without historical replay scan |
| compatible renew/trim changes only head metadata version on a long stream | retries CAS without historical replay scan |
| historical replay reaches a different commit occupying/crossing expected start at the scan boundary | proves not committed and returns classified conflict, not `MAY_HAVE_COMMITTED` |
| concurrent same-slice replay loses marker-missing condition race | re-reads marker or head chain and returns original commit result |
| head CAS succeeds but offset index materialization fails | same physical retry repairs index and returns original result |
| resolver sees gap below stream head committed end | repairs derived offset index from commit-log before returning data |
| resolver repair scan budget exhausted before covering target | continues from returned cursor within read budget or fails retriably, not as metadata corruption |
| repair continuation tuple is altered or points at an inconsistent record | `METADATA_INVARIANT_VIOLATION`; no derived record is materialized |
| reachable commit chain ends before a below-head repair target | `METADATA_INVARIANT_VIOLATION` |
| committed-slice marker exists but offset index missing | repair from head chain or `METADATA_INVARIANT_VIOLATION` if bytes conflict |
| committed-slice marker exists but offset index fields differ from request | `METADATA_INVARIANT_VIOLATION` |
| object-reference rebuild conflicts with manifest identity or would drop an existing visible slice | `METADATA_INVARIANT_VIOLATION` |
| manifest ordinal/order differs, slice ranges overlap, or object references duplicate one stream/slice identity | record/manifest validation fails before commit or repair write |
| commit path tries to use multi-key batch | fake store contract rejects the unsupported primitive |
| missing partition key in adapter call | fake contract test fails before write |
| wrong metadata record type or unsupported schema version | decode fails before state machine uses the value |
| decoded head/commit/index/marker/reference has impossible anchor, zero commitVersion, non-dense logical range, or zero physical slice length | record decode/validation fails before append, replay, or read |
| metadata codec golden bytes | stable for every Phase 1 record type |
| metadata map encoding | maps are encoded in deterministic UTF-8 key order |
| schema refs encoding | schema refs are encoded in canonical tuple order |
| offset/generation key encoding | non-negative long encoded as 19-digit zero-padded decimal |
| scan offset index for offset inside first range | returns covering entry |
| scan offset index for offset at range boundary | returns next range |
| scan offset index for one-record range `[9, 10)` at offset `9` | does not skip `offsetEnd=10` |
| trim decreases | rejected |
| trim beyond committed end | rejected |
| trim negative offset | rejected |
| null trim reason or invalid trim range | `INVALID_ARGUMENT` before head mutation |
| trim sealed stream | allowed if within committed bounds |
| manifest exists but object reference missing | no Phase 1 delete; data visibility follows offset index |
| repair object references | rebuilds from validated manifest + stream-head chain, materializes derived indexes, and never drops an existing visible reference |
| repair object references for missing manifest | fails with metadata invariant error |

### Object WAL tests

| Test | Expected |
| --- | --- |
| write/read one-slice object | payload and index match |
| write/read multi-slice object | each slice has independent range |
| `forceSingleStreamObject=true` with multiple stream ids | writer rejects before object upload |
| `forceSingleStreamObject=true` with one stream id | writer may write one or more slices for that stream without changing layout |
| WAL write request | carries `writerRunIdHash` into object id/key/header |
| WAL section envelope | each variable-length section has type, version, length, and checksum |
| WAL binary ids | object/section ids use fixed constants, never Java enum ordinal |
| object manifest format metadata | stores format major/minor version and writer version |
| deterministic slice id and order | slices have zero-based ordinals and stable object-local slice ids |
| Phase 1 entry index location | writer always emits `OBJECT_FOOTER` |
| read `INLINE` or `INDEX_OBJECT` entry index | fails with `UNSUPPORTED_FORMAT` in Phase 1 |
| WAL header/footer checksum | decoder rejects wrong header, footer, or object checksum |
| WAL canonical checksum versus storage checksum | writer computes both domains and manifest stores both without comparing them as equal |
| WAL descriptor bounds | decoder rejects offsets/lengths outside object bounds |
| entry payload offset inside non-zero slice offset | reader computes `slicePayloadOffset + entryPayloadOffset` |
| entry index deterministic encoding | checksum golden test is stable |
| zero-byte entry index item | payload length zero is valid and still advances relative offset |
| malformed entry index ordering | reader rejects non-contiguous or overlapping opaque entries |
| slice checksum domain | checksum covers `concat(slicePayloadBytes, entryIndexBytes)` |
| WAL upload timeout propagation | `WalWriteOptions.uploadTimeout` is copied into `PutObjectOptions.timeout` |
| final encoded object length exceeds `maxObjectBytes` | writer fails with `INVALID_ARGUMENT` before object upload |
| object payload fits but footer/index/header push encoded length over max | writer fails before upload |
| request exceeds `targetObjectSizeBytes` but fits `maxObjectBytes` | writer succeeds because target is advisory |
| WAL sizing arithmetic overflows | writer fails before allocation and before upload |
| duplicate object key with `ifAbsent` | fails |
| manifest retry with same object id but different checksum/length/slices | non-retriable corruption |
| object id generation after process restart | no collision with previous same-writer sequence |
| concurrent WAL object id generation | object sequences are unique and monotonic within one writer run |
| local object store path traversal key | rejected before filesystem write |
| local object store absolute key, empty segment, or symlink escape | rejected before final object path is visible |
| local object store failed write | final object path is not visible |
| local object store test cleanup | removes only files under injected root and is unavailable from production `ObjectStore` |
| range read past EOF | fails as object read error |
| `CompressionType.ZSTD` write request | rejected with `UNSUPPORTED_FORMAT` in Phase 1 |
| corrupted payload checksum | read fails |
| corrupted entry index checksum | read fails |
| unsupported major version | reader rejects |
| unsupported minor version | reader rejects until optional-section compatibility tests exist |
| committed slice uses non-supported payload format | reader fails with `UNSUPPORTED_FORMAT` in Phase 1 |

### Append path tests

| Test | Expected |
| --- | --- |
| stream uses canonical `OBJECT_WAL_SYNC_OBJECT` + strict durability | append enters WAL path |
| stream uses deprecated `OBJECT_WAL` alias | metadata canonicalization yields sync object behavior |
| stream uses BK or async profile | append fails before WAL IO with `UNSUPPORTED_STORAGE_PROFILE` |
| options request `WAL_DURABLE` | Phase 1 fails before WAL IO with `UNSUPPORTED_DURABILITY_LEVEL`；future support cannot return before head commit |
| append one batch | range starts at previous committed end |
| append two batches | second starts at first end |
| concurrent appends through one `DefaultStreamStorage` for same stream | sequenced dense ranges |
| different writer takes over session before commit | stale attempt fails with retriable `FENCED_APPEND + KNOWN_NOT_COMMITTED` before offset conflict |
| same-session concurrent commit wins offset race | losing attempt fails with retriable `OFFSET_CONFLICT + KNOWN_NOT_COMMITTED`; uploaded slice is not rebased |
| append after offset conflict in same instance | sequencer refreshes committed end before next append |
| backpressure limit reached | append fails before object upload with `BACKPRESSURE_REJECTED` |
| `maxBufferedBytes` exact-size adjustment would exceed limit | append fails before object upload with `BACKPRESSURE_REJECTED` and releases reservation |
| accepted append later fails validation or upload | in-flight count and buffered-byte reservations are released |
| append encoded object exceeds `maxObjectBytes` | append fails before object upload with `INVALID_ARGUMENT` |
| append caller-cancelled before WAL upload | no object and no offset index; caller future may be cancelled |
| append service-cancelled before WAL upload | no object and no offset index; `CANCELLED` |
| append cancelled after stream-head CAS sent | final state is unknown; same physical slice retry can discover marker or head-chain commit |
| close after stream-head CAS sent | waits up to shutdown grace for commit/materialization result before closing owned clients |
| object upload fails | no manifest and no offset index |
| manifest put fails | no offset index |
| head CAS rejects after manifest | object invisible because no committed head-chain record is reachable |
| head CAS succeeds but materialization fails | `KNOWN_COMMITTED`; retry/read repair materializes offset index from commit-log |
| commit manifest slice fields mismatch request | no offset index; non-retriable metadata invariant or checksum-style failure |
| commit manifest slice state already visible but committed-slice marker missing | search head chain first; if no matching committed record, `METADATA_INVARIANT_VIOLATION` |
| commit manifest writer id/run/epoch mismatch request | no offset index; non-retriable metadata invariant |
| stream becomes non-active before commit | no offset index; append fails as `STREAM_NOT_ACTIVE` |
| timeout before stream-head CAS starts | no committed head-chain record is created by that attempt |
| timeout after stream-head CAS is sent | `MAY_HAVE_COMMITTED`; same physical slice replay/repair resolves the result |
| next append after `MAY_HAVE_COMMITTED/KNOWN_COMMITTED` | remains suspended until the original physical attempt is resolved |
| commit succeeds but ack future interrupted in test | read sees data; retry same slice returns original result |
| process crashes after ack loss and caller resubmits batch | may append duplicate data; producer-level dedup is outside Phase 1 |
| caller-level retry without same physical slice context | may append duplicate data; no producer identity/sequence dedup in Phase 1 |
| CAS response timeout mapping | append fails with `TIMEOUT + MAY_HAVE_COMMITTED`, not `OFFSET_CONFLICT` or generic metadata failure |
| head known committed, index confirmation fails | append fails with `KNOWN_COMMITTED`; a new physical append is forbidden |
| object reference update fails after stream commit | read sees data and repair can rebuild reference |
| concurrent post-commit object reference updates | CAS merge does not lose another visible slice; conflict exhaustion leaves repairable stale metadata |
| stale session after upload | no visible data |
| manifest object key/type/format mismatch request | no offset index; non-retriable metadata invariant |
| partial multi-slice commit | committed slice readable, failed slice invisible |

### Read path tests

| Test | Expected |
| --- | --- |
| read first offset | returns first batch |
| read middle offset | clips result to requested offset |
| read middle offset in a larger resolved slice | `WalObjectReader` uses explicit `startOffset` |
| resolve committed range | `ResolvedObjectRange` includes slice checksum from offset index |
| resolve committed range schema refs | `ResolvedObjectRange` includes schema refs from offset index |
| resolve across two adjacent offset index entries | returns contiguous ranges and advances `resolvedEndOffset` |
| resolve with `includeEntryIndex=true` | does not read object footer or index object in Phase 1 |
| read clipped batch | `ReadBatch.sourceObjectOffset/sourceObjectLength` matches returned payload bytes |
| read 100 bytes from 16 MiB resolved slice | reader downloads/verifies full slice plus entry index, returns clipped bytes, and records amplification metrics |
| read buffer limit exhausted before range read | fails with retriable `BACKPRESSURE_REJECTED` and does not call `ObjectStore.readRange` |
| read permit limit exhausted before range read | fails with retriable `BACKPRESSURE_REJECTED` and releases no extra buffer reservation |
| read adjacent opaque entries | returns one `ReadBatch` per entry and does not concatenate payload bytes |
| read zero-byte entry | returns one record and advances `nextOffset` even though payload bytes are empty |
| read positive entry exactly consumes `maxBytes` followed by zero-byte entry | returns both entries if `maxRecords` allows |
| read positive entry exactly consumes `maxBytes` followed by positive entry | stops before the second positive entry |
| read batch schema refs | copied from resolved range, not decoded from payload |
| read clipped batch entry index ref | still references source committed slice index, not a rewritten clipped index |
| clipped read with corrupted bytes outside returned subrange | fails because full resolved slice checksum is verified |
| read at committed end | empty EOF result |
| read below trim | `OFFSET_TRIMMED` |
| offset index cache stale | refetch and return correct data |
| offset index cache empty scan | does not cache EOF/negative lookup |
| watch offset update without follow-up scan | invalidates only and does not populate positive cache data |
| watch event missed or collapsed | cache still becomes correct through TTL/read-through scan |
| watch event out of order or duplicate | cache invalidation remains safe |
| watcher callback throws during session/trim/index notification | mutation result is unchanged; failure metric increments; other deliveries continue |
| register watch after metadata store close | synchronous `STORAGE_CLOSED` rejection |
| `enableMetadataWatch=false` | no-op registration; cache remains correct through TTL/read-through |
| index points to missing object | retriable object read failure |
| gap below committed end | repair from commit-log first; invariant only if chain/record/materialized bytes are corrupt |

## 3. Failure Injection Points

Test fakes should inject failures at these exact boundaries:

```text
before object put
after object put before checksum return
before manifest put
after manifest put before return
before commit-log put
after commit-log put before head CAS
after head CAS before derived-index materialization
after derived-index materialization before return
before object range read
after entry index read with corrupted bytes
```

Each failure test should assert:

- whether offset index exists；
- whether stream head committed end advanced；
- whether the commit-log record is reachable from stream head；
- whether object manifest exists；
- whether `read` can see the data；
- whether the append future is retriable。

## 4. Invariant Tests

These should be named and kept stable:

```text
AppendAckRequiresOffsetIndexCommitTest
CommittedOffsetsAreDenseTest
ObjectManifestIsNotVisibilityTest
StaleAppendSessionIsFencedTest
PartialWalObjectSliceVisibilityTest
ReadResolverStartsFromOffsetIndexTest
TrimDoesNotDeleteObjectTest
ObjectListNotUsedForCorrectnessTest
MetadataRecordCodecCompatibilityTest
AppendConflictDoesNotRebaseUploadedSliceTest
StreamStorageCloseRejectsNewCallsTest
```

The names intentionally mirror design invariants.

## 5. No-Pulsar Dependency Guard

Add a test or Gradle check for Phase 1 modules:

```text
nereus-api
nereus-core
nereus-metadata-oxia
nereus-object-store
```

Guard:

```text
forbidden packages:
  org.apache.pulsar
  org.apache.bookkeeper
  org.apache.kafka
  io.streamnative.pulsar.handlers.kop
```

Phase 1 may mention `PULSAR_ENTRY_BATCH` as an enum value, but must not depend on Pulsar classes.

## 6. Gradle Tasks

Current M0-M3 verification commands:

```text
./gradlew checkPhase0
./gradlew :nereus-api:test
./gradlew :nereus-metadata-oxia:test
./gradlew :nereus-object-store:test
./gradlew :nereus-core:test
./gradlew phase1Check
./gradlew check
```

`phase1Check` currently depends on:

- `checkPhase0`；
- L0 module tests for `nereus-api`, `nereus-core`, `nereus-metadata-oxia`, and `nereus-object-store`；
- dependency guard scoped to `nereus-api`, `nereus-core`, `nereus-metadata-oxia`, and
  `nereus-object-store`；
- `:nereus-metadata-oxia:compileOxiaCapabilitySpikeJava`, which compiles the M0.5 Oxia spike without
  starting Docker。

Future milestones should add these gates to `phase1Check` when the corresponding code exists:

- formatting/checkstyle if introduced；
- API validation unit tests；
- done: object WAL round-trip tests；
- done: fake metadata invariant tests；
- M7 real Oxia shared-contract tests and an independent Docker/Testcontainers integration task；the latter
  stays outside ordinary `phase1Check` but is mandatory for final Phase 1 release。

`phase1Check` must not require `nereus-managed-ledger`, `nereus-pulsar-adapter`, or `nereus-kop-adapter`
to implement their future behavior. Those modules may keep marker classes or compile-only boundaries while
L0 is being built.

## 7. Implementation Order Rationale

Recommended order:

1. API first, because every other module consumes the same value types.
2. Fake metadata second, because correctness lives in commit semantics.
3. Object WAL third, because append cannot commit references without durable bytes.
4. Append coordinator fourth, because it is the first end-to-end write path.
5. Resolver/read fifth, because it validates offset index shape.
6. Trim/recovery next, because they depend on committed index and read semantics.
7. Real Oxia adapter last, after the state-machine contract is executable against the fake and can be reused
   as a production-adapter contract suite.

Avoid starting with Pulsar facade. It would force L1 concerns into L0 before the offset truth is stable.

## 8. Phase 1 Done Definition

A Phase 1 implementation is done when this local scenario works without Pulsar:

```text
create stream "orders-0"
acquire append session as writer-a
append batch with 3 records
append batch with 2 records
read from offset 0 returns 5 records
resolve offset 3 returns object range covering [3, 5)
trim before offset 2
read offset 1 fails as trimmed
read offset 2 returns remaining records
restart DefaultStreamStorage with same fake/local stores
read offset 2 still returns committed data
```

And these negative cases hold:

```text
uploaded object without a reachable head commit is invisible
head-reachable commit without an offset index is repaired and remains logically committed
stale epoch cannot commit
offset conflict cannot create a gap
WAL canonical object checksum mismatch cannot commit
object listing is never used to discover readable data
```

The local fake/local-store scenario closes M6 but does not by itself close Phase 1。Final Phase 1 completion
also requires M7：the production Oxia adapter passes the shared contract suite and its independent
Docker/Testcontainers integration gate，including process restart and post-head recovery。

## 9. Deferred Beyond Phase 1

Do not implement these while completing Phase 1:

- ManagedLedger facade；
- Pulsar `Position` or `MessageId` mapping beyond opaque `ProjectionRef`；
- ManagedCursor and individual ack holes；
- Kafka group offset；
- transaction markers；
- compaction worker；
- lakehouse catalog；
- routing brown-out；
- object GC delete policy。

The design keeps references and extension points so these futures can attach later without changing
`streamId + offset` truth.
