# 02 — Ranged-Entry API and Object Format

> 状态：Implementation in progress；public values/default overloads/Kafka batch validation complete；core/read/V2 formats pending
> 前置：Phase 1.5 generic L0、F4 generation/read-view、F1-BK profiles 均保持现有已实现合同
> 核心原则：先把“一个 entry 覆盖多个 logical offsets”做成 protocol-neutral 能力，再允许 Kafka adapter 使用

## 1. 问题定义

一个 Kafka `RecordBatch` 可以包含多个 records，并且 compressed batch 不能按 Nereus read limit 任意拆开。
当前 F9-M1 public slice 已让 `AppendBatch` 校验支持 ranged Kafka entries，并增加 public request/result values；
Object WAL entry index 原本也保存 `relativeBaseOffset + recordCount`。但 production conditional append、reader
clipping、core result validation、NCP1 与 NTC1 仍假设或只执行 one-entry-per-offset。

F9-M1 必须一次关闭整条链：

```text
append validation
  -> expected-start CAS
  -> commit metadata / WAL index
  -> exact or containing read boundary
  -> read limits
  -> COMMITTED NCP2
  -> TOPIC_COMPACTED NTC2
  -> publication/read/retirement dispatch
```

只放开任一单点都不构成可用能力。

## 2. Compatibility strategy

### 2.1 不修改现有 record constructor

`AppendOptions` 和 `ReadOptions` 是 public Java records。向 record 增加 component 会删除旧 canonical constructor，
造成 source/binary break。target 保留它们原样，新增独立 value object 和 overload。

### 2.2 Existing methods remain authoritative defaults

以下现有方法不删除、不改默认语义：

```java
CompletableFuture<AppendResult> append(
        StreamId streamId, AppendBatch batch, AppendOptions options);

CompletableFuture<ReadResult> read(
        StreamId streamId, long startOffset, ReadOptions options);
```

现有 append 等价于 `AppendPrecondition.none()`；现有 read 等价于：

```text
view             = COMMITTED
boundaryMode     = EXACT_START
firstEntryPolicy = LEGACY_STRICT_LIMIT
```

Pulsar/F2/F3/F4 已实现调用无需修改。任何 regression 都阻断 F9-M1。

### 2.3 New interface methods are binary-safe defaults

`StreamStorage` 新 overload 是 `default` method。旧 provider binary 在只调用旧 method 时仍工作；调用新语义
且 provider 未 override 时，以新增的稳定错误码拒绝，不静默降级：

```text
UNSUPPORTED_APPEND_PRECONDITION
UNSUPPORTED_READ_SEMANTICS
```

两个 enum constant 只追加到 `ErrorCode` 尾部；禁止依赖 Java ordinal 做 wire encoding。Nereus production
`DefaultStreamStorage` 必须 override 两个 overload，所有 primary-WAL reader 通过 shared contract suite。

## 3. Target append API

### 3.1 `AppendPrecondition`

目标文件：
`nereus-api/src/main/java/com/nereusstream/api/AppendPrecondition.java`

```java
// implemented in the F9-M1 public API slice
public record AppendPrecondition(OptionalLong expectedStartOffset) {
    public AppendPrecondition {
        Objects.requireNonNull(expectedStartOffset, "expectedStartOffset");
        if (expectedStartOffset.isPresent() && expectedStartOffset.getAsLong() < 0) {
            throw new IllegalArgumentException("expectedStartOffset must be non-negative");
        }
    }

    public static AppendPrecondition none();
    public static AppendPrecondition expectedStartOffset(long value);
}
```

它只表达 stream offset CAS，不表达 Kafka producer sequence 或 leader epoch。leader authority 属于 append session，
见文档 04。

### 3.2 `StreamStorage.append` overload

```java
// default overload implemented；DefaultStreamStorage override pending
default CompletableFuture<AppendResult> append(
        StreamId streamId,
        AppendBatch batch,
        AppendOptions options,
        AppendPrecondition precondition) {
    if (precondition.equals(AppendPrecondition.none())) {
        return append(streamId, batch, options);
    }
    return failedFuture(UNSUPPORTED_APPEND_PRECONDITION, false, ...);
}
```

`DefaultStreamStorage` override 的顺序：

1. validate args，任何 IO 前拒绝负值/closed runtime；
2. 进入现有 `StreamLane`；
3. resolve active profile + append session；
4. 读取 current committed end；
5. 若 expected 存在且不同，返回 `OFFSET_CONFLICT`，异常必须带 expected/actual 但不能含 payload；
6. 用同一个 expected 值构造 `PrimaryAppendRequest`；
7. primary write、protection、head CAS、completion policy 沿用现有协议；
8. 验证 result start 就是 expected；
9. 对 known response loss 保留相同 attempt，不能用新 expected 自动重试。

检查与 head CAS 在同一 stream lane 内，但 correctness 仍由 metadata CAS 保底；lane 不是跨进程锁。

### 3.3 `AppendBatch` validation matrix

目标修改文件：`nereus-api/.../AppendBatch.java`。

| `PayloadFormat` | Executable after F9-M1 | Per-entry count | Projection hints |
| --- | --- | --- | --- |
| `OPAQUE_RECORD_BATCH` | existing | exactly 1 | empty |
| `KAFKA_RECORD_BATCH` | new | `1..Integer.MAX_VALUE` | empty |
| `PULSAR_ENTRY_BATCH` | unchanged reserved unless its own activation says otherwise | existing rule | existing rule |
| `NORMALIZED_ROW_BATCH` | unchanged reserved | rejected | rejected |

共同 invariant：

```text
entryCount == entries.size
recordCount == exact sum(entry.recordCount)
recordCount > 0
sum does not overflow int
entries preserve caller order
event-time range includes every entry
optional batch CRC32C == CRC32C(concat exact entry payloads in order)
```

`KAFKA_RECORD_BATCH` 的 API 层不解析 Kafka bytes。它只允许 ranged counts；batch syntax/CRC/offset consistency
由 `KafkaRecordBatchCodec` 在 adapter 层双向验证。这样 `nereus-api` 没有 Kafka dependency。

### 3.4 Append result checks

`AppendResult` 已允许非 opaque 的 `entryCount != recordCount`。F9-M1 增加 shared validator：

```java
// target
AppendResultValidator.requireExactRequest(
    AppendBatch request,
    AppendPrecondition precondition,
    AppendResult result);
```

它检查 stream、payload format、record/entry count、logical bytes、schema refs、range count 和 expected start；
不比较 physical target 类型，因为不同 storage profile 合法返回不同 generation-zero target。

## 4. Target read API

### 4.1 New public values

目标文件均在 `nereus-api/...`：

```java
// public values implemented；provider semantics pending
public enum ReadBoundaryMode {
    EXACT_START,
    CONTAINING_ENTRY
}

public enum FirstEntryPolicy {
    LEGACY_STRICT_LIMIT,
    ALLOW_FIRST_ENTRY_OVERFLOW
}

public record ReadRequest(
        long startOffset,
        ReadView view,
        ReadBoundaryMode boundaryMode,
        FirstEntryPolicy firstEntryPolicy,
        ReadOptions options) { ... }

public record SemanticReadResult(
        ReadView view,
        ReadResult result,
        long sourceCoverageEndOffset) { ... }
```

Constructor invariants：

- start non-negative；所有 enum/options non-null；
- `sourceCoverageEndOffset >= result.nextOffset`；
- result.requestedOffset 等于 request start；
- `COMMITTED` coverage 等于 `result.nextOffset`；
- `TOPIC_COMPACTED` coverage 可以大于 next offset，表示没有输出 row 的 source hole；
- coverage 不得越过 resolver 冻结的 selected-generation source coverage。

### 4.2 `StreamStorage.read` overload

```java
// default overload implemented；DefaultStreamStorage override pending
default CompletableFuture<SemanticReadResult> read(
        StreamId streamId,
        ReadRequest request);
```

default implementation 只接受 legacy-equivalent request，调用旧 read 后包装 `COMMITTED` result；其他组合返回
`UNSUPPORTED_READ_SEMANTICS`。`DefaultStreamStorage` override 并替代 core-internal `StreamViewReader` 暴露面。

现有 `com.nereusstream.core.read.ViewReadResult` 在一个 release 内保留为 deprecated adapter；生产 owner 迁移到
API `SemanticReadResult` 后删除，不能维护两份不同 validation。

### 4.3 Boundary semantics

给定 logical entry `[entryStart,entryEnd)` 与 request `s`：

| Mode | `s == entryStart` | `entryStart < s < entryEnd` | no entry at/containing `s` |
| --- | --- | --- | --- |
| `EXACT_START` | return entry | `OFFSET_NOT_AVAILABLE`，不得跳到下一 entry | follow existing trimmed/end rules |
| `CONTAINING_ENTRY` | return entry | return complete entry | follow existing trimmed/end rules |

containing result 保持：

```text
ReadResult.requestedOffset = s
first ReadBatch.range.start <= s < first.range.end
ReadResult.nextOffset = last returned batch.range.end
```

第二个及后续 committed batch 必须从前一个 end 开始。storage 不切 payload、不改 range；Kafka adapter 决定
是否屏蔽 batch 内低于 log start/fetch offset 的 records。

### 4.4 Limit semantics

`LEGACY_STRICT_LIMIT` 精确保留现有行为，包括已有 `READ_LIMIT_TOO_SMALL`/empty result 差异，不借 F9 顺便修改
旧 caller contract。

`ALLOW_FIRST_ENTRY_OVERFLOW`：

- 仅当整个 read 尚未返回任何 entry 时适用；
- 第一 entry 可以同时使 records 和 bytes 超过 limit；
- 返回该 entry 后立即停止，不再返回第二项；
- payload 仍受 hard format/runtime allocation limit，不能绕过 64 MiB entry cap；
- metrics 分别记录 requested limit 与 actual returned；
- 后续 entry 永远不能 overflow。

这与 Kafka fetch 的 first-batch progress rule 对齐，并避免 compressed batch 被拆分。

### 4.5 Object-WAL reader algorithm

`DefaultWalObjectReader.clip` target pseudo-code：

```java
for (EntryIndexItem item : index.entries()) {
    long start = range.start + item.relativeBaseOffset;
    long end = addExact(start, item.recordCount);

    boolean candidate = switch (boundaryMode) {
        case EXACT_START -> start >= requestedStart;
        case CONTAINING_ENTRY -> end > requestedStart;
    };
    if (!candidate || start >= resolvedRange.end) continue;

    if (firstCandidateStartsBeforeRequest && boundaryMode == EXACT_START) {
        fail(OFFSET_NOT_AVAILABLE);
    }
    if (wouldExceed && !(firstEntryPolicy == ALLOW_FIRST_ENTRY_OVERFLOW && nothingReturned)) stopOrLegacyFail();
    copy exact payload;
    emit ReadBatch([start,end), ...);
}
```

`returnedBeforeRange` 改名 `returnedEntryBeforeSlice`，它必须跨 resolved ranges 传播；否则第二个 slice 会错误地
获得 first-entry overflow。records/bytes 使用 `long` 做 checked accumulation，再安全转换 metrics。

### 4.6 Core coordinator validation

`ReadCoordinator.buildReadResult` 新增 request-aware validator：

- EXACT_START：首 batch start == requested；
- CONTAINING_ENTRY：首 batch range contains requested；
- empty result：next == requested，除非 semantic sparse caller 只通过 coverage 前进；
- committed batches dense；topic-compacted rows strictly increasing/non-overlapping；
- payload format 在一个 resolved candidate 内一致；
- batch range 被 source resolved range 包含；
- next/coverage/end-of-stream 由 resolver frozen snapshot 计算，不读取结束后的新 head。

`ReadTargetDispatcher` 不自行增加 offsets；它使用 reader 返回的 `ReadBatch.range.end`。所有 Object、BookKeeper
和 higher-generation adapters 执行同一 contract suite。

## 5. Object WAL V1 compatibility

Object WAL container 和 entry-index v1 已保存 `recordCount`，F9-M1 **不 bump WAL object format**。必要修改：

- writer validation 接受 `KAFKA_RECORD_BATCH`；
- reader validation 接受已激活 stream binding 下的该 format；
- entry range checked-add，禁止 relative/end overflow；
- `recordCount` 与 enclosing slice/commit range 完全一致；
- payload mapping version 不由 WAL 推测；Kafka partition binding 冻结 `KAFKA_RECORD_BATCH_V1`；
- WAL slice checksum、entry-index checksum、per-entry payload CRC 与 Kafka internal CRC 分层验证。

Compatibility：旧 reader 遇到 `KAFKA_RECORD_BATCH` 仍 fail closed；activation 必须先证明所有 eligible brokers
支持 payload format 和 containing read，再允许写。旧 OPAQUE objects bytes/goldens 不改变。

## 6. Layered checksum contract

| Layer | Algorithm/source | Protects | Failure owner |
| --- | --- | --- | --- |
| Kafka batch | Kafka magic-specific CRC | Kafka batch header/records after CRC field | adapter/Kafka codec |
| `AppendBatch.checksum` | CRC32C over exact concatenated entry payloads | caller handoff/request | API/adapter |
| entry index payload field | current exact payload checksum where present | one entry extraction | physical reader |
| WAL slice checksum | current Nereus checksum | slice payload + entry index | primary reader |
| Object provider checksum/etag facts | provider contract | uploaded bytes | object store |
| NCP2/NTC2 row | CRC32C over exact row payload | Parquet row extraction | compacted reader |
| generation/object identity | SHA-256 facts | immutable output/source identity | F4 verifier |

任何一层失败都不能用下一 generation 自动掩盖后继续写。reader 可以按 F4 fallback 规则选择同 view 的另一个
健康 generation，但 corruption metric/audit 必须保留；没有健康 source 时 partition offline。

## 7. NCP2 lossless committed format

### 7.1 Identity

```text
physical format = NEREUS_COMPACTED_PARQUET_V2
format id       = NCP2
format version  = 2
object prefix   = {cluster}/compacted/v2/committed/
logical format  = KAFKA_RECORD_BATCH_V1 (for F9 streams)
```

NCP2 不替代/重解释 NCP1。format registry 按 exact physical format dispatch；同一 generation record 只引用一种
格式。NCP1 继续只接受 single-offset row。

### 7.2 Closed Parquet schema

```text
message nereus_committed_generation_v2 {
  required int64  stream_offset_start;
  required int32  record_count;
  required int32  entry_ordinal;
  required binary payload;
  required int32  payload_crc32c;
  optional int64  event_time_millis;
}
```

规则：

- row range 是 `[stream_offset_start, addExact(start, record_count))`；
- `record_count > 0`，ordinal 从 0 连续增加；
- row ranges dense，第一 row start == metadata coverage start，最后 end == coverage end；
- payload 长度 `0..64 MiB`，Kafka adapter 另要求至少 Kafka batch header 长度；
- CRC32C 对 exact payload；
- `event_time_millis` 是 Nereus append fact；Kafka timestamp 仍以 payload 为权威；
- 不保存可从 exact payload 验证出的 producer/compression 字段副本，避免双 truth。

### 7.3 Required metadata

NCP2 继承 NCP1 common facts并改变/新增：

| Key | Required value/meaning |
| --- | --- |
| `nereus.format` | `NCP2` |
| `nereus.format.version` | `2` |
| `nereus.read.view` | `COMMITTED` |
| `nereus.payload.format` | exact `PayloadFormat.name()` |
| `nereus.logical.format` | immutable stream mapping, F9=`KAFKA_RECORD_BATCH_V1` |
| `nereus.range.model` | `ENTRY_START_PLUS_RECORD_COUNT_V1` |
| `nereus.offset.start/end` | dense source coverage |
| `nereus.source.record.count` | long decimal = coverage length |
| `nereus.output.record.count` | same as source for lossless view |
| `nereus.entry.count` | row count，may differ from record count |
| `nereus.source.set.sha256` | exact committed source set identity |
| `nereus.policy.sha256` | NCP2 policy identity |
| `nereus.output.attempt.id` | exact task attempt |
| `nereus.cumulative.size.at.end` | committed logical cumulative bytes snapshot |

unknown `nereus.*` key、missing key、non-canonical decimal、count mismatch 或 trailing row 一律 format error。

### 7.4 Writer and reader classes

计划新增：

```text
nereus-object-store/.../compacted/CompactedObjectFormatV2.java
nereus-object-store/.../compacted/RangedCompactedObjectRow.java
nereus-object-store/.../compacted/RangedCompactedObjectMetadata.java
nereus-object-store/.../compacted/ParquetRangedCompactedObjectWriter.java
nereus-object-store/.../compacted/ParquetRangedCompactedObjectReader.java
nereus-object-store/.../compacted/RangedCompactedObjectVerifier.java
```

不要给 V1 class 加一串 optional branches。共享 provider staging/range IO 可抽到 internal helper，但 V1/V2 schema、
metadata registry、validation 和 golden tests 保持闭合。

## 8. NTC2 Kafka topic-compacted format

### 8.1 Identity

```text
physical format = NEREUS_TOPIC_COMPACTED_KAFKA_PARQUET_V2
format id       = NTC2
format version  = 2
read view       = TOPIC_COMPACTED
object prefix   = {cluster}/compacted/v2/topic-compacted-kafka/
key encoding    = KCK2
rewrite codec   = KAFKA_RECORD_REWRITE_V1
```

NTC2 是 Kafka-aware physical format，不让 generic NTC1 decoder 猜 Kafka bytes。F4 task/policy identity 必须包含
decoder、strategy、key encoding、rewrite codec 与 Kafka message-format compatibility digest。

### 8.2 Closed Parquet schema

```text
message nereus_topic_compacted_kafka_v2 {
  required int64  stream_offset_start;
  required int32  record_count;
  required int32  disposition;
  required binary compaction_key;
  required binary payload;
  required int32  payload_crc32c;
  required int64  source_batch_base_offset;
  required int32  source_record_index;
  required binary source_batch_sha256;
  optional int64  event_time_millis;
}
```

Output row 只代表 survivor；被 compact 掉的 offset 不写 row，由 file metadata 的 source coverage 证明 hole。
首版 normal data survivor rewrite 为一个合法 single-record Kafka batch，所以 `record_count=1`。control batch 或
未来不可拆 semantic unit 可以大于 1，但必须经过 codec 显式 disposition/validator，不能由 generic writer 猜测。

`source_batch_sha256` 固定 32 bytes；它用于 audit/determinism，不作为 source existence reference。所有 current
source references 仍由 F4 durable protections/generation records 管理。

### 8.3 Disposition wire IDs

| ID | Name | Rule |
| --- | --- | --- |
| 1 | `RETAIN_VALUE` | keyed latest live record |
| 2 | `RETAIN_TOMBSTONE` | tombstone within delete-retention window |
| 3 | `RETAIN_UNKEYED` | null-key record，unique by absolute offset |
| 4 | `RETAIN_CONTROL` | transaction/control record required by Kafka semantics |

ID 是 durable int，不使用 Java enum ordinal。unknown ID fail closed。

### 8.4 `KCK2` key encoding

```text
0x01 || raw Kafka key bytes                 keyed record, empty key is legal
0x02 || big-endian uint64 absolute offset   null-key record
0x03 || big-endian uint64 absolute offset   control/semantic-retain record
```

keyed equality 对 raw bytes 做 unsigned lexicographic compare only for external-sort order；dedup equality 是 exact
byte equality。tag 避免 empty key、null key 和 control identity 碰撞。encoded key hard limit 为 1 MiB；Kafka
config 必须更严格或相等。

### 8.5 Required metadata additions

除 NCP2 common metadata 外：

```text
nereus.source.coverage.start/end
nereus.compaction.strategy
nereus.compaction.strategy.version
nereus.compaction.key.codec=KAFKA_KEY_BYTES_V1
nereus.compaction.key.encoding=KCK2
nereus.kafka.batch.mapping=KAFKA_RECORD_BATCH_V1
nereus.kafka.rewrite.codec=KAFKA_RECORD_REWRITE_V1
nereus.kafka.message.format.digest=<sha256>
nereus.source.batch.count=<canonical long>
nereus.output.batch.count=<canonical long>
```

`offset.start/end` 与 source coverage 相同；output rows 可 sparse。`output.record.count` 是 survivor logical record
count，不等于 coverage length。reader 验证 rows strictly increasing/non-overlapping 且完全位于 coverage。

## 9. Format dispatch and generation compatibility

`ReadTarget` 已含 physical-format discriminator。F9-M1 registry target：

```text
OBJECT_SLICE + WAL_OBJECT_V1 + OPAQUE/KAFKA -> WalObjectReaderAdapter
OBJECT_SLICE + NEREUS_COMPACTED_PARQUET_V1 -> NCP1 adapter
OBJECT_SLICE + NEREUS_TOPIC_COMPACTED_PARQUET_V1 -> NTC1 adapter
OBJECT_SLICE + NEREUS_COMPACTED_PARQUET_V2 -> NCP2 adapter
OBJECT_SLICE + NEREUS_TOPIC_COMPACTED_KAFKA_PARQUET_V2 -> NTC2 adapter
```

registry key 必须包含 target type + physical format + logical format/version；仅按 `OBJECT_SLICE` 覆盖注册是 bug。

Publication：

- NCP2 只发布到 `COMMITTED`；
- NTC2 只发布到 `TOPIC_COMPACTED`；
- candidate generation format 必须在 protocol activation allowlist；
- reader capability digest 覆盖 exact formats；
- F4 fallback 只能在同一 view 内，允许 NCP2 → WAL committed，不允许 NTC2 → committed source 伪装为 compacted；
- retirement replacement proof 比较 source coverage/view，不要求旧新 physical format 相同，但新 format 必须是
  activated、verified 且 semantic-compatible。

## 10. Hard limits

| Limit | Initial target | Enforcement |
| --- | --- | --- |
| one entry payload | 64 MiB | API + every reader/writer before allocation |
| entries per append request | 65,536 | adapter + API config hard cap |
| records per Kafka batch/entry | `Integer.MAX_VALUE` API；Kafka codec/config lower | checked count |
| total records per AppendBatch | `Integer.MAX_VALUE` | exact sum |
| total request logical bytes | 128 MiB default，256 MiB hard | adapter admission |
| compacted object bytes | existing 1 GiB | V2 writer |
| Parquet footer | existing 16 MiB | strict reader |
| row groups | existing 65,536 | strict writer/reader |
| compaction key | 1 MiB | decoder before spill |
| coverage/count arithmetic | signed 64-bit non-overflow | all layers |

Kafka configs `message.max.bytes`、`replica.fetch.max.bytes`（尽管 RF1）与 fetch response limits 必须在 runtime
activation 前验证不超过这些 hard bounds。不能依赖 OOM 把超限 request 变成可恢复 error。

## 11. Planned classes and method ownership

| Module | Class/method | Change |
| --- | --- | --- |
| API | `AppendPrecondition` | new immutable value |
| API | `ReadBoundaryMode` / `FirstEntryPolicy` | new enums without wire ordinals |
| API | `ReadRequest` / `SemanticReadResult` | public semantic read |
| API | `StreamStorage.append/read` | binary-safe default overloads |
| API | `AppendBatch` | format-specific validation table |
| API | `ErrorCode` | append two unsupported-semantics codes |
| core | `DefaultStreamStorage` | override overloads |
| core | `AppendCoordinator` | caller expected-start propagation and result check |
| core | `ReadCoordinator` | request-aware range/coverage validation |
| core | `ReadTargetDispatcher` | propagate request semantics |
| core | `StreamViewReader` | deprecating adapter to public result |
| object | `DefaultWalObjectReader` | containing/overflow + Kafka format |
| object | `DefaultWalObjectWriter` | activated Kafka format validation |
| object | V2 compacted classes | closed NCP2/NTC2 implementation |
| materialization | `RangedLosslessMaterializationRowPublisher` | one source entry → one NCP2 row |
| materialization | Kafka topic-compaction publisher | decoded survivor → NTC2 row |
| metadata/core | target format registry/capability | exact V2 dispatch/admission |

## 12. Test contract

### 12.1 API/property tests

- random entry counts sum exactly to range；overflow/zero/negative rejected；
- old opaque constructor behavior byte-for-byte unchanged；
- expected-start none delegates old provider；unsupported provider rejects non-none；
- boundary truth table for every relative request point；
- first-entry overflow allowed once only；
- result/coverage constructors reject every invalid inequality。

### 12.2 Primary reader contract suite

Parameterized over Object WAL、BookKeeper generation-zero、NCP2 and NTC2 where applicable：

- one entry spans 1, 2, 1,000 and `Integer.MAX_VALUE` logical count without allocating records；
- request at start/middle/last offset/end；
- request crosses resolved slices；
- first batch exceeds bytes、records、both；second batch cannot overflow；
- trim start in middle of entry；
- checksum/index corruption and checked arithmetic overflow；
- source response loss/retry returns identical ranges/bytes。

### 12.3 Golden formats

至少提交：

- NCP2 one-row range、multi-row mixed ranges、max metadata；
- NTC2 keyed empty key、null key、tombstone、control、sparse holes；
- exact SHA-256、Parquet metadata map、schema string 与 selected rows；
- unknown metadata、truncated payload、bad CRC、bad source digest length、overlap/out-of-order/count mismatch fixtures；
- V1 reader rejects V2，V2 reader rejects V1 when exact format is requested。

### 12.4 Non-regression gates

F9-M1 final gate must compose：

```text
existing phase1/phase15/F2/F3/F4/F1-BK API and format suites
+ ranged API unit/property tests
+ every primary reader contract suite
+ real Object store NCP2/NTC2 round trip
+ old-broker capability exclusion fixture
```

F9-M1 完成仍不代表 Kafka integration 可用；它只建立所有上层设计可依赖的中立合同。
