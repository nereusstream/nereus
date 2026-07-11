# 10 Current Progress Review 2026-07-07

本文最初记录 2026-07-07 对 M0/M1/M2 进度的代码和文档 review；2026-07-08 已追加 M2 completion
pass 的修正和验证结果。当前结论：M2 已完成。

## 1. Scope

本次重点检查：

- `nereus-api` M1 validation hardening 是否继续支撑 M2；
- `nereus-metadata-oxia` 的 stream-head CAS fake store 是否仍遵守 M0.5 之后的真实 Oxia public API
  边界；
- keyspace、partition-key helper、watch simulation、commit replay 和 repair 路径是否和文档一致；
- metadata codec 相关代码是否已经达到 M2 exit；
- `docs/phase-1-core-stream-storage` 是否有过期状态描述。

本次实际运行：

```bash
./gradlew :nereus-metadata-oxia:test
./gradlew phase1Check
./gradlew check
```

结果：全部通过。Docker-backed `:nereus-metadata-oxia:oxiaCapabilitySpike` 未在本次 review 运行。

## 2. Verdict

当前没有发现需要推翻 M0.5 后 stream-head CAS 方案的 P0 问题。`FakeOxiaMetadataStore` 的主线方向是
正确的：不可见 commit-log intent 先写，stream head CAS 作为唯一 append 线性化点，offset-index 和
committed-slice 作为可修复派生索引。

在 2026-07-07 原始 review 时，M2 exit gates 尚未全部关闭：当时的主要问题是 metadata codec 和
decoded-record validation 还没有冻结到可让 fake/real adapter 共用的程度，且 fake-store codec
persistence parity 和更广的 linearizability tests 仍未关闭。后续 M2 completion pass 已关闭这些门禁。

Follow-up implementation status from the next M2 pass:

- resolved: `EntryIndexReferenceRecord` now validates location-specific decoded metadata shape；
- resolved: metadata envelope/payload string decode now rejects malformed UTF-8；
- resolved: `Phase1MetadataCodecs` now has per-record round-trip, error-path, deterministic map-order, and
  golden envelope hex tests for every Phase 1 metadata record type；
- resolved: `FakeOxiaMetadataStore` stored metadata values now persist encoded envelopes through the
  shared codec；
- resolved: broader M2 linearizability tests covering deterministic before-head-CAS compatible
  head-version interleavings (renew/trim), bounded repair retry progress after budget exhaustion,
  commitVersion equality across stream head/commit-log/offset-index/committed-slice, stale-epoch fencing
  priority over an also-present offset conflict, and commit-log retry after first-attempt head CAS failure；
- M2 is complete.

## 3. Findings

### P1: `EntryIndexReferenceRecord` validation is weaker than the API contract

Status: resolved by the follow-up M2 codec/validation pass.

Code reference:
`nereus-metadata-oxia/src/main/java/io/nereus/metadata/oxia/records/EntryIndexReferenceRecord.java`

The public API `EntryIndexRef` validates `INLINE`, `OBJECT_FOOTER`, and `INDEX_OBJECT` field combinations.
At review time, the metadata record checked only non-null fields, non-blank checksum/location strings, and
`offset + length` overflow. It does not reject:

- unknown `location` strings until `locationEnum()` is called later；
- `INLINE` without non-empty `inlineData`；
- `INLINE` with object id/key；
- `OBJECT_FOOTER` with inline bytes or non-positive length；
- `INDEX_OBJECT` without object id/key or with inline bytes。

Impact: corrupt or malformed persisted metadata can be decoded into a record successfully and fail later
inside read/resolve code. That violates the M2 goal that decoded metadata records enforce the same shape
invariants as public API values.

Required before M2 exit:

- done: move location-specific validation into `EntryIndexReferenceRecord` construction；
- done: add metadata-record tests mirroring the M1 `EntryIndexRef` shape tests；
- done: ensure codec decode fails before state machines consume invalid entry-index metadata。

### P1: `Phase1MetadataCodecs` exists, but codec freeze is not backed by tests

Status: resolved for the current M2 record set by the follow-up codec/golden test pass.

Code references:
`nereus-metadata-oxia/src/main/java/io/nereus/metadata/oxia/codec/Phase1MetadataCodecs.java`
and `nereus-metadata-oxia/src/test/java/io/nereus/metadata/oxia/codec/MetadataCodecSamples.java`

At review time, the code contained a reflection-based `binary-v1` codec registry for all Phase 1 metadata
records, plus a sample catalog, while tests still covered only `MetadataRecordEnvelope` and registry
behavior. No test exercised `Phase1MetadataCodecs.encodeEnvelope/decodeEnvelope` for real metadata records.

Impact: field order, record type names, nested record handling, map ordering, byte-array copying, absent
optional fields, wrong record type, unsupported schema version, and golden bytes are not locked. A future
record edit could silently change persisted bytes without any test failure.

Required before M2 exit:

- done: round-trip every Phase 1 metadata record type through `Phase1MetadataCodecs`；
- done: add golden envelope hex for every record type；
- done: test wrong record type, unsupported schema version, truncated payload, checksum mismatch, and invalid
  payload type tags；
- done: test deterministic UTF-8 map key ordering and absent optional fields encoded as empty string/empty byte
  array, not null；
- done for current M2 record set: the current binary-v1 reflection codec is treated as the M2 codec and
  its field names/record simple names are locked by golden tests.

### P1: Metadata string decode should reject malformed UTF-8

Status: resolved by the follow-up strict UTF-8 pass.

Code references:
`Phase1MetadataCodecs.PayloadReader.readString()` and `MetadataRecordEnvelope.readShortString()`

At review time, both methods used `new String(bytes, StandardCharsets.UTF_8)`. Java replaces malformed UTF-8 with
the replacement character instead of failing. For durable metadata values, malformed string bytes should
be treated as corrupt metadata.

Impact: a corrupted or incorrectly produced value can pass decode with changed string content. The payload
CRC only proves byte preservation, not that the bytes are a valid encoding of a metadata string.

Required before codec freeze:

- done: use a strict UTF-8 `CharsetDecoder` with `CodingErrorAction.REPORT` for envelope strings and payload
  string fields；
- done: add corruption tests that inject invalid UTF-8 into an otherwise checksum-correct envelope/payload。

### P1: Fake store still bypasses the metadata value codec

Status: resolved by the follow-up fake-store codec-backed persistence pass.

Code reference:
`nereus-metadata-oxia/src/testFixtures/java/io/nereus/metadata/oxia/testing/FakeOxiaMetadataStore.java`

At review time, the fake store stored hydrated Java records directly in maps. That was acceptable for
early M2 state-machine work, but it did not prove fake/real codec parity.

Impact: fake tests can pass while the real adapter fails to encode, decode, or reject the same record
payloads. This is especially risky for nested records like `StreamHeadRecord.appendSession`,
`ObjectManifestRecord.slices`, and `OffsetIndexRecord.entryIndexRef`.

Required before M2 exit:

- done: route fake stored values through the shared codec on write/read, or add a fake/real codec contract
  suite that exercises the exact same `MetadataCodecRegistry` for every persisted value；
- done: keep in-memory helpers allowed only for state-machine tests that do not claim codec coverage。

### P2: Projection field naming is easy to misread

Code reference:
`CommitSliceRequest.projectionIdentity()` and `StreamCommitRecord.projectionRef`

Committed metadata currently stores the canonical projection identity string in `projectionRef`, not the
raw projection value and not simply an empty string for absence. This is valid for replay identity, but the
field name and earlier docs can make readers expect raw projection content.

Required before M2 exit:

- document that `OffsetIndexRecord.projectionRef` and `StreamCommitRecord.projectionRef` contain the
  durable canonical projection identity；
- done: include absent and present projection identity samples in golden-byte tests；
- consider renaming the metadata field before codec freeze if we want to avoid a durable naming mismatch。

### P2: More linearizability cases remain uncovered

Status: resolved by the follow-up M2 linearizability pass.

These are now covered in `FakeOxiaMetadataStoreTest`:
`sameWriterRenewIsCompatibleHeadVersionChange`, `sameWriterTrimIsCompatibleHeadVersionChange`,
`repairBudgetExhaustionStopsAtMaxRecords`, `commitVersionIsMonotonicAcrossSequentialCommits`,
`staleEpochIsFencedBeforeOffsetConflict`, `retryReusesStoredCommitLogAfterFirstAttemptFailedHeadCas`.

The 2026-07-08 completion pass tightened the first version of these tests: renew/trim now use an explicit
before-head-CAS interleaving hook instead of pre-mutating the head before commit starts; repair budget
exhaustion now proves repeated bounded repairs skip already-materialized records and continue toward the
target offset; stale-epoch priority is checked after another writer has also advanced the committed end.

M2 is now complete.

## 4. Confirmed Good Direction

- The M0.5 result is correctly reflected in code direction: fake metadata does not expose a multi-key
  atomic commit primitive.
- `OxiaKeyspace` uses shared `KeyComponentCodec` and fixed-width 19-digit offset/generation keys.
- `PartitionedOxiaClient` makes missing partition keys fail before backend calls.
- Watch simulation explicitly treats notifications as invalidation hints and covers drop, duplicate,
  stale-before-current, collapsed, and reconnect-before-current events.
- Same-slice replay now checks committed-slice marker before walking the head chain.
- Post-commit object manifest/reference audit failure is modeled as repairable and does not invalidate
  stream visibility.

## 5. Documentation Changes From This Review

This review updates the surrounding docs to say:

- `Phase1MetadataCodecs` is now covered by per-record round-trip/golden/error-path tests；
- decoded metadata validation now covers entry-index location-specific shape rules；
- fake-store stored metadata values now use shared codec-backed envelopes；
- the M2 completion pass added the broader linearizability test matrix before claiming M2 completion。
