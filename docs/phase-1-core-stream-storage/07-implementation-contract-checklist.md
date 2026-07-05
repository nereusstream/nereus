# 07 Implementation Contract Checklist

本文是 Phase 1 开始写代码前的实现合同。它不替代前面的设计文档，只把已经收敛的硬约束集中
成 checklist，避免实现时靠记忆补语义。

## 1. Supported Surface For Phase 1

| Area | Phase 1 supports | Phase 1 rejects or defers |
| --- | --- | --- |
| Stream lifecycle | exact opaque `StreamName`, deterministic full-hash `StreamId`, create-or-get as `ACTIVE`, read/trim `SEALED` | public seal/delete API, parsing Pulsar topic syntax in L0 |
| Durable key helpers | `nereus-api` shared key/hash helpers used by metadata and object modules; `.`/`..` are encoded; writer run id has at least 128 bits entropy | duplicate local key encoders, raw cluster in paths, truncated hashes without migration plan |
| Storage profile | `OBJECT_WAL` | BookKeeper/local WAL profiles |
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
| Metadata truth | Oxia offset index, committed end, append session, committed-slice marker | object manifest as read visibility truth |
| Object metadata | manifest/reference as repairable audit/GC inputs | cross-stream/object atomic producer ack |
| Read path | offset-index-driven resolve, full-slice checksum before clipping | object list, manifest-only reads, negative cache |
| Read amplification | explicit metrics for full-slice bytes, entry-index bytes, returned bytes, amplification, and read backpressure | hiding 16 MiB-to-100 byte reads as ordinary object read volume |
| Offset-index cache | positive scan results only; watch events invalidate but never populate cache | EOF caching, watch-created positive records |
| Local object cleanup | test-only cleanup under injected `LocalFileObjectStore` root | production object delete in Phase 1 |
| Protocol integration | protocol-neutral API and metadata labels | Pulsar, KoP, ManagedLedger classes |

## 2. Must-Have Before Real Adapter Work

Before implementing the real `OxiaMetadataStore` adapter:

1. Keep the M0.5 Oxia Java client capability spike from
   `06-metadata-oxia-position-and-pulsar-reference.md` compiling and runnable.
2. Treat the current M0.5 result, `NOT_SUPPORTED_BY_PUBLIC_JAVA_API`, as a hard input: same stream key
   group conditional multi-write is not available through the selected public Java client API.
3. Redesign `commitStreamSlice` before M2 fake metadata or M4 core append rely on a real adapter contract.
4. Do not make `FakeOxiaMetadataStore` stronger than the selected public Oxia Java client API.
5. Prove every get/put/scan/watch/commit call passes the expected partition key.
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

## 3. Stop-The-Line Conditions

Stop implementation and update design first if any of these are discovered:

- The Phase 0 skeleton API remains as the only `StreamStorage` surface after M0.
- Core tests need fake metadata classes from production packages instead of test fixtures.
- A design or fake implementation assumes Oxia public Java client can do single-key-group conditional
  multi-write without a new passing spike proving that exact API.
- M4 core append starts before partition-key routing has real-client evidence and the commit protocol has
  been redesigned to avoid the unavailable multi-key conditional write primitive.
- The real adapter cannot route all stream-scoped operations with `PartitionKey(streamId)`.
- Offset index scan cannot preserve 19-digit zero-padded offset/generation ordering.
- redesigned commit protocol cannot assign a durable `commitVersion` that can be validated across
  committed-end, offset-index, and committed-slice state.
- L0 code needs to parse Pulsar topic syntax or use a human-readable alias as the durable `StreamId`.
- Metadata and object modules need separate implementations of durable key/hash helpers.
- Object keys or Oxia paths need raw `cluster`, raw `writerId`, or truncated writer/run hashes.
- A new physical WAL object attempt needs to reuse an object sequence from a failed, cancelled, or timed-out
  attempt.
- `commitStreamSlice` would require object-scoped keys in the producer-ack atomic batch.
- A new `commitStreamSlice` path would accept a manifest slice already marked `VISIBLE` while the
  stream-scoped committed-slice marker is missing.
- Read path would need object manifest to resolve visibility or checksum.
- WAL canonical object checksum and object-store storage checksum have to be collapsed into one field.
- WAL object sections cannot carry type/version/length/checksum from the first encoder.
- WAL writer cannot know or enforce final encoded object length before `putObject`.
- Object-store `putObject` cannot receive a concrete timeout/deadline.
- Backpressure cannot release in-flight count or buffered-byte reservations on every terminal path.
- Full-slice read cannot be guarded by read permits and byte reservations before object IO starts.
- Read amplification cannot be measured as full-slice bytes versus returned payload bytes.
- WAL binary object/section encoding would use Java enum ordinal instead of fixed durable ids.
- Writer needs a non-`OPAQUE_RECORD_BATCH` payload format, `CompressionType.ZSTD`, `INLINE`, or
  `INDEX_OBJECT` before their contracts are designed.
- A Phase 1 module needs Pulsar, BookKeeper, Kafka, or KoP classes.
- Object deletion appears necessary for a Phase 1 correctness test.
- `WalObjectWriter`, reader, or manifest code becomes single-stream-only because the initial core planner
  uses `forceSingleStreamObject=true`.

## 4. Commit Contract

M0.5 redesign gate: this section describes the original visibility contract that the redesigned M2
metadata protocol must preserve, not the current implementation recipe. Do not implement it as a multi-key
conditional Oxia write unless a future spike proves a supportable public Java API for that primitive.

Producer ack is allowed only after:

1. WAL object bytes are durable.
2. Object manifest is written or idempotently confirmed.
3. Stream-scoped `commitStreamSlice` succeeds atomically:
   - stream state is `ACTIVE`;
   - append epoch/token match;
   - committed end equals `expectedStartOffset`;
   - for a new physical slice, the committed-slice marker is missing;
   - for same-slice replay, the existing marker and offset index are validated and the original result is
     returned;
   - after marker-missing condition failure, the marker is re-read before classifying the error;
   - object manifest writer id, writer run id, writer epoch, object key/type/format, and slice fields match
     the commit request;
   - offset index, committed end, and committed-slice marker are written in one stream key group;
   - durable `commitVersion` is the same in committed-end, offset-index, and committed-slice records.

Post-commit object reference and manifest state updates are best-effort CAS merges. Failure there does not
block ack and must be repairable from offset index.

## 5. Read Contract

Read and resolve must follow this chain:

```text
trim/state check
-> cache positive offset-index records only, or scan Oxia
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
- Timeout/cancellation after `commitStreamSlice` RPC is sent has unknown final state.
- Unknown final state must surface as `TIMEOUT` or `CANCELLED` semantics, not as ordinary offset conflict,
  object upload failure, or generic metadata condition failure.
- Same physical slice retry can discover a successful commit through the committed-slice marker.
- Per-stream append sequencer must refresh after `OFFSET_CONFLICT` and must not choose new expected offsets
  while a prior commit final state is unknown.
- `close()` rejects new work, stops background tasks, waits up to `shutdownGrace` for irreversible in-flight
  work, then closes owned clients.

## 7. Required Test Gates

`phase1Check` should not pass without:

- no Pulsar/KoP/BookKeeper dependency guard for Phase 1 modules;
- compiling the M0.5 Oxia capability spike without starting Docker;
- fake metadata invariant tests;
- exact stream-name hash and deterministic stream-id tests;
- shared key/hash helper parity tests across Oxia keyspace and object key generation;
- non-opaque payload format rejection tests;
- real Oxia contract tests, once the adapter exists;
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
- append timeout/cancellation unknown-final-state tests;
- object reference repair tests;
- offset index cache missed/duplicate/collapsed/out-of-order watch tests;
- local object store path traversal, atomic-write, and test-only cleanup tests.
