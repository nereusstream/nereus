# Nereus Future 1：Core Stream Storage

> 状态：Phase 1 M0-M8 + Phase 1.5 P15-M0-M6 implemented/final-gated
> 交付映射：`docs/phase-1-core-stream-storage/`
> 后继交付：Phase 1.5 final-gated；`docs/phase-1.5-core-storage-foundation/`
> 当前里程碑：F2-M1-M4 have consumed the completed L0 foundation；F2-M5 active

本文定义 L0 目标边界，并把总体架构映射到当前 Phase 1。精确 Java records、binary layout、
Oxia keys、failure injection 和测试 gate 以代码级文档为准；本文不复制那些合同。

## 1. Motivation

L0 必须在不依赖 Pulsar/KoP 类型的前提下回答：

- 谁为 append 分配 stable offset；
- primary WAL durability 与 logical visibility 如何组合；
- current public Oxia API 下如何线性化、replay 和 repair；
- offset 如何解析到物理 bytes；
- profile 选择如何不产生第二套 truth；
- 后续 ManagedLedger、cursor、Kafka、compaction 如何建立在稳定 L0 上。

## 2. Scope

F1 owns：

- `StreamStorage` API and errors；
- `StreamId` / `StreamName` / offset ranges；
- stream metadata and immutable profile selection；
- append session、epoch、fencing token；
- primary WAL adapter boundary；
- Object WAL v1、object manifest、checksums；
- stream head / commit log / generation-0 indexes；
- append/resolve/read/trim/recovery/close state machines；
- object/reference/GC boundary；
- opaque projection references for later layers。

F1 does not own：

- Pulsar Position/MessageId encoding；
- ManagedCursor/subscription/group semantics；
- producer dedup and transaction protocols；
- higher-generation compaction worker；
- broker routing and lakehouse catalogs。

## 3. Current Phase 1 boundary

### 3.1 Implemented through M8

| Area | Implemented |
| --- | --- |
| API | values、validation、canonicalization、errors、profile/durability enums |
| Metadata | fake + production Oxia adapter、binary-v1 codec、partition wrapper、one-head snapshot、bounded scan、head-CAS、repair/watch tests |
| Object WAL | v1 writer/reader、multi-slice object、footer entry index、CRC32C、local fixture |
| Core | append、resolve/read、trim/recovery、resource/deadline/close state machines |
| Build | protocol-neutral dependency guard、ordinary/Docker Phase 1 tasks |

### 3.2 M8 final acceptance

- done: full `DefaultStreamStorage` + production Oxia + local Object WAL end-to-end scenario；
- done: facade/client restart、orphan invisibility、post-head repair and trim-retention acceptance；
- done: final support-matrix/document freeze and `phase1FinalCheck` aggregate gate；
- done after M8: package/Maven namespace migration to `com.nereusstream` plus metadata concurrency、bounded
  scan and resident-memory hardening recorded in the Phase 1 final review。

### 3.3 Supported profile

Phase 1 execution supports only：

```text
OBJECT_WAL / OBJECT_WAL_SYNC_OBJECT
+ WAL_DURABLE_AND_INDEX_COMMITTED
```

Other enum values can be canonicalized and persisted as metadata today, but core must reject their execution
until the corresponding writer/reader/coordinator exists。This is `Reserved`, not partial support。

### 3.4 Phase 1.5 implemented extension

Phase 1.5 is an implemented F1 delivery extension inserted before F2 production implementation。It adds generic
read-target and primary-WAL adapter boundaries、split stable head commit from generation-zero materialization、
dual-read/new-write metadata、exact in-process append recovery、seal/logical delete and exact cumulative-result
handoff。P15-M0-M6 code and gates pass。

Phase 1.5 P15-M0-M6 deliberately keeps the same supported profile/durability as Phase 1。BookKeeper adapters、non-strict success
and Future 4 workers remain deferred beyond it。

## 4. Layer and module boundary

```text
nereus-api
    ^
    |-- nereus-metadata-oxia
    |-- nereus-object-store
    `-- nereus-core -> metadata + object-store
```

Forbidden in F1：

- Pulsar、BookKeeper ManagedLedger、Kafka protocol classes in L0 API/core；
- duplicated durable key/hash encoders outside `nereus-api`；
- object list or watch as a correctness source；
- broker local state as committed offset/session/cursor truth。

## 5. Current API

The implemented public surface is：

```java
public interface StreamStorage extends AutoCloseable {
    CompletableFuture<StreamMetadata> createOrGetStream(
            StreamName streamName, StreamCreateOptions options);

    CompletableFuture<AppendSession> acquireAppendSession(
            StreamId streamId, AppendSessionOptions options);

    CompletableFuture<AppendResult> append(
            StreamId streamId, AppendBatch batch, AppendOptions options);

    CompletableFuture<ReadResult> read(
            StreamId streamId, long startOffset, ReadOptions options);

    CompletableFuture<ResolveResult> resolve(
            StreamId streamId, long startOffset, ResolveOptions options);

    CompletableFuture<Void> trim(
            StreamId streamId, long beforeOffset, TrimOptions options);

    CompletableFuture<StreamMetadata> getStreamMetadata(StreamId streamId);

    void close();
}
```

API rules：

- async methods complete exceptionally with `NereusException` and must not synchronously leak operational
  failures；
- value-construction validation can throw `IllegalArgumentException`；
- `AppendBatch` is Phase 1 `OPAQUE_RECORD_BATCH`, one record per entry；
- public append does not provide producer-level dedup identity；
- `resolve(includeEntryIndex=true)` returns committed references, not decoded footer bytes；
- trim advances metadata only；
- after close, new work fails with `STORAGE_CLOSED`。

### 5.1 Phase 1.5 target API evolution

Current `AppendResult` and `ResolvedObjectRange` are object-shaped。Before BookKeeper profiles are
implemented, the Phase 1.5 target introduces a tagged `ReadTarget`、generic `ResolvedRange` and target-aware durable metadata。
Its code-level contract is `../phase-1.5-core-storage-foundation/01-api-and-read-target-contract.md`。Using fake
object ids/keys for BK ranges remains forbidden；the BookKeeper target value/codec does not itself register IO support。

## 6. Durable metadata model

### 6.1 Authoritative head

`StreamHeadRecord` holds one stream's current state：identity、state/profile/attributes、committed end、
cumulative size、commit version、trim、last commit id and append-session snapshot。

One head key is the only append/session/trim CAS target in Phase 1。This is a deliberate response to the
selected Oxia Java client's lack of required conditional multi-key writes。

### 6.2 Commit log

`StreamCommitRecord` is immutable and deterministic。It contains every field needed to：

- prove same physical-slice replay；
- reconstruct generation-0 offset index；
- reconstruct committed-slice marker；
- validate checksums、schema refs、projection identity and commitVersion；
- diagnose exact `AppendOutcome` certainty。

An intent is committed only if reachable from `StreamHeadRecord.lastCommitId`。

### 6.3 Derived indexes

| Record | Purpose | Repair source |
| --- | --- | --- |
| `OffsetIndexRecord` | resolve offset to generation-0 physical slice | reachable commit log |
| `CommittedSliceRecord` | fast replay lookup | reachable commit log |
| object references | audit/GC references | committed slice/index + manifest |

Derived record failure after head CAS cannot roll back the append。Strict append waits for them or returns
`AppendOutcome.KNOWN_COMMITTED`；an unconfirmed head response returns `MAY_HAVE_COMMITTED`。Read/replay uses
scan-bounded continuation repair。

## 7. Append protocol

### 7.1 Common states

```text
ACCEPTED
  -> SESSION_VALID
  -> WAL_DURABLE
  -> COMMIT_INTENT_STORED
  -> HEAD_COMMITTED
  -> PRIMARY_INDEX_CONFIRMED        // strict boundary
  -> RESULT_RETURNED
```

### 7.2 Linearization

The append linearization point is：

```text
putIfVersion(/streams/{streamId}/head, nextHead)
```

The CAS validates：

- stream active；
- session epoch/token；expiry is checked before CAS，while atomic fencing relies on the current head snapshot；
- expected offset equals head committed end；
- next offsets/cumulative size/commitVersion do not overflow；
- `lastCommitId` points to the deterministic intent。

### 7.3 Durability levels

All successful levels include `HEAD_COMMITTED`。

| Level | Return may happen after | Requirement |
| --- | --- | --- |
| `WAL_DURABLE` | `HEAD_COMMITTED` | commit record contains a recoverable primary read target；index may need repair |
| `WAL_DURABLE_AND_INDEX_COMMITTED` | `PRIMARY_INDEX_CONFIRMED` | offset index and committed-slice marker present/equal |

Phase 1 M4 implements only the second row。The first row is a target contract for async/BK-only profiles，
not “WAL put/quorum only”。

Phase 1.5 will split the internal stable commit/materializer operations but continues rejecting the first row before
IO。That refactor is a prerequisite, not implementation of the non-strict public boundary。

### 7.4 Replay and append outcome certainty

- before head CAS is sent：`KNOWN_NOT_COMMITTED`；
- after it is sent but the response is unavailable：`MAY_HAVE_COMMITTED`；
- after head success is confirmed：`KNOWN_COMMITTED` even when index confirmation/result delivery fails；
- retry of the same physical slice recomputes the same commit id；
- replay checks marker, then head/reachable chain；
- if committed, it repairs derived records and returns the existing result；
- a different durable identity cannot reuse the same committed range；
- bounded chain-search exhaustion remains `MAY_HAVE_COMMITTED`，and repair pages use explicit continuation
  cursors rather than restarting from head；
- every chain page validates dense offset/cumulative-size/commitVersion progression，and the continuation
  retains the original observed head plus the exact expected tuple for its next commit。

### 7.5 Multi-stream objects

WAL writer may encode several stream slices in one object。Each slice uses its own session、intent and head
CAS。No cross-stream atomicity is required，and GC preserves the object while any committed slice references it。

## 8. Object WAL v1 boundary

Current format summary：

```text
magic NRS1
format 1.0
little-endian fixed-width fields
48-byte common header
16-byte section envelope
CRC32C header / section / footer / object checks
MULTI_STREAM_WAL_OBJECT
compression NONE
entry index in OBJECT_FOOTER
payload OPAQUE_RECORD_BATCH
```

`ZSTD`、inline index、independent index object、encryption and other payload formats are reserved。Readers
must fail closed with `UNSUPPORTED_FORMAT` until their exact byte/checksum semantics are implemented。

The exact durable format and golden tests are documented in
`../phase-1-core-stream-storage/03-object-wal-and-index.md`。

## 9. Resolve and read

```text
validate start against trim/head
  -> scan first index key where offsetEnd > target
  -> choose highest supported generation
  -> if gap below head committed end, repair from commit log and retry
  -> range-read slice + entry index
  -> validate checksums and bounds
  -> clip records/bytes
```

Phase 1 rules：

- generation 0 only；
- cache stores positive committed entries only；
- cache watches only invalidate；
- full resolved slice is read to validate slice checksum before clip；
- resource guard bounds concurrent object reads and buffered bytes；
- zero-byte entry consumes one offset and can be returned after byte budget reaches zero；
- first positive entry exceeding the original byte budget raises `READ_LIMIT_TOO_SMALL`。

## 10. Trim and GC boundary

`trim(beforeOffset)` CAS-updates `StreamHeadRecord.trimOffset`。It neither deletes offset-index entries nor
object bytes。

F1 exposes references needed by future GC：

- active/reachable committed ranges；
- trim and retention boundary；
- object manifest/reference state；
- reader/repair/task hooks。

Actual safe deletion requires F3 cursor low-watermarks and F4 generation/catalog/task references。

## 11. Failure model

| Failure | Outcome |
| --- | --- |
| WAL upload fails | no head commit；append fails |
| object checksum/manifest mismatch | rejected before head CAS |
| intent write succeeds, head CAS loses | orphan intent；no visibility unless reachable |
| stale epoch/token | `FENCED_APPEND` before offset conflict takes precedence |
| compatible renew/trim changes head version | refresh and retry CAS against semantically compatible head |
| head commits, index write fails | `KNOWN_COMMITTED`; strict index boundary requires replay/repair |
| read sees index gap below head | bounded continuation repair；budget exhaustion is retriable, not corruption |
| object/index bytes corrupt | fail closed; never synthesize from list |
| cancellation/timeout after irreversible boundary | same structured `AppendOutcome` rules as failure |

## 12. Compatibility outputs

F1 provides later layers with：

- committed offset range and end；
- record/entry counts and opaque payload format；
- schema refs and projection ref；
- entry-index reference；
- physical checksums/location for supported target；
- commitVersion and generation。

F2 and F5 own parsing/projection。They cannot reinterpret F1 offset allocation or visibility。

## 13. Phase 1 implementation gates

### M4 append

- object write -> manifest validation -> intent -> head CAS -> derived index confirmation；
- per-stream sequencer、same-slice replay、timeout/cancellation/close；
- reject unsupported profile/durability before WAL IO with `UNSUPPORTED_STORAGE_PROFILE` /
  `UNSUPPORTED_DURABILITY_LEVEL`，and keep payload/layout failures under `UNSUPPORTED_FORMAT`；
- post-head repair and exact error classification。

### M5 resolve/read

- scan/repair/cache/watch behavior；
- range/checksum/resource/read-limit contracts；
- end-of-stream and trim behavior。

### M6 trim/recovery

- trim CAS and conflict behavior；
- orphan/audit boundaries；
- close and in-flight completion semantics；
- final Phase 1 failure-injection matrix。

### M7 real Oxia adapter

- selected public Java client only，with `PartitionKey` on every scoped operation；
- shared codecs、manifest validator、append outcomes and bounded replay/repair contract tests；
- Docker/Testcontainers persistence、restart、failure and exception-mapping gate。

Status: implemented and verified on 2026-07-11.

### M8 final acceptance

- aggregate core + production Oxia + Object WAL restart/failure scenario；
- no new feature or support-surface expansion；
- final docs/status freeze and one explicit release gate。

Status: implemented and verified on 2026-07-11 through `./gradlew phase1FinalCheck --rerun-tasks`.

The full test matrix and done definition remain authoritative in
`../phase-1-core-stream-storage/05-implementation-plan-and-tests.md`。

## 14. F1 exit and deferred work

F1 Phase 1 exits when M8 proves Object WAL sync append/read/trim end to end with production Oxia，including
restart and repair boundaries. M7's adapter gates are complete prerequisites，not by themselves the final
facade-level release scenario。

Explicitly deferred：

- Phase 1.5 generic target/result/adapter seams are implemented；BookKeeper writer/reader registration remains deferred；
- `WAL_DURABLE` non-strict return implementation remains after the Phase 1.5 internal split；
- async materialization tasks and retention gate；
- higher generations/compacted readers；
- protocol projections and durable cursors。

These deferrals must remain visible in docs and runtime errors；they cannot be hidden behind reserved enums。

The exact Phase 1.5 scope、metadata migration、state machines and P15-M1-M6 gates are authoritative in
`../phase-1.5-core-storage-foundation/README.md`。
