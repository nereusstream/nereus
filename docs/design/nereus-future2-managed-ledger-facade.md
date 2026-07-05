# Nereus Future 2：ManagedLedger Facade

> 本文是 Nereus L1 设计文档。它定义 Nereus 如何通过 Pulsar 的 ManagedLedger API
> 暴露给 `PersistentTopic`、dispatcher、subscription 和 admin runtime，同时保持
> `streamId + offset` 是内部 truth。当前阶段只冻结设计边界，不要求实现或验证。

## 1. Motivation

Pulsar broker 大量运行时逻辑依赖 ManagedLedger：

- topic open/load/unload；
- append entry；
- dispatcher read；
- cursor open/update；
- retention/backlog；
- topic compaction；
- transaction/system topic bootstrap。

Nereus 不能要求第一步重写 `PersistentTopic` 和 dispatcher。Future 2 的目标是提供一个
ManagedLedger-compatible facade，让 broker 继续使用熟悉的接口，同时把底层 truth
切换到 Future 1 定义的 `StreamStorage`。

关键原则：

```text
ManagedLedger is compatibility facade.
StreamStorage is storage truth.
```

Ledger、entry、position、cursor 都是 projection；不能反向决定 L0 的 offset 分配、
可见性、trim 或 compaction。

## 2. Scope

Future 2 覆盖：

- broker-level `ManagedLedgerStorage` provider；
- storage class `nereus`；
- `ManagedLedgerFactory` facade；
- `ManagedLedger` facade；
- virtual ledger projection；
- `Position(ledgerId, entryId)` 到 `streamId + offset` 的映射；
- append entry 到 `StreamStorage.append` 的映射；
- read entry 到 `StreamStorage.resolve/read` 的映射；
- topic open/load/unload；
- read-only cursor boundary；
- basic cursor open boundary；
- BookKeeper WAL profile 与 Object WAL profile 共用 facade 的方式。

## 3. Non-scope

Future 2 不解决：

- 完整 ManagedCursor ack holes；
- Shared/Key_Shared 全量语义；
- delayed delivery；
- pending ack transaction；
- geo-replication；
- KoP group coordinator；
- lakehouse catalog；
- compaction worker。

这些能力在 Future 3、Future 5、Future 6、Future 8 中作为 projection 或上层状态处理。

## 4. Layer Boundary

Future 2 位于 L1：

```text
PersistentTopic / dispatcher / broker runtime
    -> ManagedLedger-compatible facade
    -> StreamStorage
    -> Oxia + WAL/object data plane
```

L1 可以做：

- 把 Pulsar entry 编码为 `AppendBatch`；
- 把 `AppendResult` 转成 `Position` / `MessageId`；
- 把 `Position` 转成 stream offset range；
- 暴露 `ManagedLedgerFactory` / `ManagedLedger` / basic `ManagedCursor` 接口；
- 缓存 projection metadata。

L1 不能做：

- 在 broker 本地生成 final offset；
- 让 `ledgerId + entryId` 成为内部排序依据；
- 通过 BookKeeper cursor ledger 决定 durable cursor truth；
- 使用 object store list 推断可见数据；
- 为 Pulsar 和 Kafka 建两套 durable log。

## 5. Pulsar Integration Points

当前 Pulsar 有两层容易混淆的配置：

| 配置/概念 | 含义 | Nereus 设计 |
| --- | --- | --- |
| Broker `managedLedgerStorageClassName` | `ManagedLedgerStorage` 实现类名 | 指向一个 Nereus-aware storage provider |
| Policy `PersistencePolicies.managedLedgerStorageClassName` | 某个 topic/namespace 选择的 storage class name | 设置为 `nereus` |
| `ManagedLedgerStorage` | 持有多个 `ManagedLedgerStorageClass` | 同时注册 default BookKeeper 和 Nereus |
| `ManagedLedgerStorageClass` | 名字 + `ManagedLedgerFactory` | `getName() == "nereus"` |
| `ManagedLedgerFactory` | broker 打开 managed ledger 的入口 | 返回 Nereus ManagedLedger facade |

建议形态：

```text
NereusManagedLedgerStorage implements ManagedLedgerStorage
  - storageClass: bookkeeper/default
  - storageClass: nereus

NereusManagedLedgerStorageClass implements ManagedLedgerStorageClass
  - getName() -> "nereus"
  - getManagedLedgerFactory() -> NereusManagedLedgerFactory
```

这样可以保留 hybrid profile：

- existing low-latency topics keep BookKeeper；
- selected namespaces/topics switch to `nereus`；
- rollback can be policy-level if data migration rules allow。

## 6. API Mapping

### 6.1 ManagedLedgerFactory

| ManagedLedgerFactory method | Nereus mapping |
| --- | --- |
| `open(name, config)` | resolve topic identity, create/get stream, return `NereusManagedLedger` |
| `asyncOpen(name, config, ...)` | async variant of open |
| `openReadOnlyCursor(name, startPosition, config)` | create read-only projection cursor over stream offset |
| `asyncOpenReadOnlyManagedLedger(...)` | return read-only ledger facade |
| `getManagedLedgerInfo(name)` | synthesize info from stream metadata, offset index summary, trim state |
| `delete(name)` | transition stream state to deleting; actual object deletion follows GC rules |
| `asyncExists(name)` | check topic-to-stream mapping in Oxia |
| `getEntryCacheManager()` | return Nereus-aware cache manager or compatibility adapter |
| `shutdown()` | close facade resources, caches, watchers, StreamStorage clients |

`getManagedLedgerInfo` is compatibility metadata. It must not imply real BookKeeper ledgers exist.

### 6.2 ManagedLedger

| ManagedLedger method group | Nereus mapping |
| --- | --- |
| `addEntry` / `asyncAddEntry` | encode Pulsar entry, call `StreamStorage.append`, map `AppendResult` to `Position` |
| `openCursor` | create/open `NereusManagedCursor` boundary; full cursor semantics in Future 3 |
| read by position | map `Position` to stream offset using virtual ledger projection, call resolver |
| `getLastConfirmedEntry` | synthesize from `committedEndOffset` and latest virtual ledger projection |
| `getNumberOfEntries` | derive from offset index summary, approximate if exact count requires scan |
| `getTotalSize` | use `cumulativeSize` from offset index |
| `terminate` | set stream state to sealed/terminated in Oxia |
| `close` | close facade, release local cache/session, not stream data |
| `deleteCursor` | delete cursor state in Future 3 schema |
| offload APIs | no-op, unsupported, or remapped to native compaction depending on compatibility mode |

Synchronous methods can be implemented as compatibility wrappers over async operations, but they must not block
event-loop threads. The broker-facing hot path should use async methods.

### 6.3 ManagedCursor Boundary

Future 2 only defines the facade boundary:

```text
NereusManagedCursor
  name
  streamId
  markDeleteOffset
  readPositionOffset
  individualAckRef
  cursorVersion
```

Future 2 supports:

- open cursor；
- read current cursor state；
- basic read position initialization；
- mapping cursor positions to `Position` for broker compatibility。

Future 3 owns:

- individual ack ranges；
- mark-delete CAS details；
- cursor snapshot object；
- Shared cursor recovery；
- cursor retention low-watermark。

## 7. Virtual Ledger Projection

### 7.1 Projection model

Every committed stream range visible to Pulsar must be addressable through a stable virtual ledger projection:

```text
VirtualLedger
  streamId
  virtualLedgerId
  startOffset
  endOffset
  entryBaseId
  entryCount
  entryIndexRef
  commitVersion
```

Projection options:

| Option | Pros | Cons |
| --- | --- | --- |
| Oxia allocated `virtualLedgerId` | simple uniqueness, flexible rollover | requires one metadata write/reference |
| deterministic from `streamId + startOffset` | easy recovery, fewer sequence dependencies | encoding must fit Pulsar expectations |
| epoch-based | aligns with append session/rollover | session churn can create many ledgers |

Future 2 does not need to choose the final encoding if it defines the projection contract:

- `virtualLedgerId` is stable after published；
- `entryId` is ordinal within the virtual ledger；
- each entry maps to `[entryBaseOffset, entryBaseOffset + recordCount)`；
- batch index maps within the entry；
- projection can be rebuilt from offset index + entry index。

### 7.2 Position mapping

```text
Position(ledgerId, entryId)
  -> virtualLedger(ledgerId)
  -> entryIndex[entryId]
  -> streamId + entryBaseOffset
```

For batched messages:

```text
MessageId(ledgerId, entryId, batchIndex)
  -> streamId + entryBaseOffset + batchIndex
```

Rules:

- `entryId` is not equal to record offset when batching is used.
- `ledgerId` is not a BookKeeper ledger id in Nereus; it is a virtual projection id.
- A `Position` returned to Pulsar must remain resolvable until retention/trim legally removes it.
- Compaction must preserve projection or provide a projection-preserving replacement.

## 8. Topic Open / Load / Unload

### 8.1 Open

```text
BrokerService
  -> get ManagedLedgerConfig
  -> storageClassName == "nereus"
  -> NereusManagedLedgerFactory.open(name, config)
  -> create/get stream mapping in Oxia
  -> load stream metadata
  -> initialize projection cache
  -> return NereusManagedLedger
```

Open must be idempotent. Multiple brokers may open the same topic facade, but correctness still relies on
Future 1 append session fencing and offset index commit.

### 8.2 Load

Topic load initializes:

- stream metadata cache；
- latest offset index summary；
- virtual ledger projection cache；
- cursor state handles；
- optional append session if broker becomes preferred writer。

Load does not transfer durable ownership.

### 8.3 Unload / Close

Unload closes local resources:

- projection cache；
- read cache references；
- cursor handles；
- append session renew loop；
- watchers。

Unload must not:

- delete stream metadata；
- delete object bytes；
- advance cursor；
- seal stream unless explicitly terminating the topic。

## 9. Metadata Schema

Future 2 adds projection and facade metadata on top of Future 1:

```text
/nereus/clusters/{cluster}/topics/{tenant}/{namespace}/{topic}/partitions/{partition}/stream
/nereus/clusters/{cluster}/streams/{streamId}/virtual-ledgers/{virtualLedgerId}
/nereus/clusters/{cluster}/streams/{streamId}/position-index/{virtualLedgerId}/{entryId}
/nereus/clusters/{cluster}/streams/{streamId}/facade/managed-ledger
/nereus/clusters/{cluster}/streams/{streamId}/facade/cursors/{cursorName}
```

### 9.1 Topic-to-stream mapping

```json
{
  "topic": "persistent://tenant/ns/topic-partition-0",
  "streamId": "s-123",
  "storageClass": "nereus",
  "profile": "OBJECT_WAL",
  "createdAt": 1783036800000,
  "version": 12
}
```

### 9.2 VirtualLedgerProjection

```json
{
  "virtualLedgerId": 9007199254740993,
  "streamId": "s-123",
  "startOffset": 1048576,
  "endOffset": 1114112,
  "entryBaseId": 0,
  "entryCount": 4096,
  "entryIndexRef": "object-footer:offset=67000000,length=8192,checksum=crc32c:...",
  "sourceOffsetIndexEnd": 1114112,
  "commitVersion": 17
}
```

### 9.3 ManagedLedger facade state

```json
{
  "streamId": "s-123",
  "managedLedgerName": "persistent://tenant/ns/topic-partition-0",
  "state": "OPEN",
  "storageClass": "nereus",
  "profile": "OBJECT_WAL",
  "lastProjectionLedgerId": 9007199254740993,
  "metadataVersion": 31
}
```

## 10. Entry Cache and Read Cache

Nereus should not depend on BookKeeper ledger cache semantics. It needs a compatibility cache layer:

| Cache | Key | Value | Truth |
| --- | --- | --- | --- |
| Projection cache | `ledgerId` | virtual ledger metadata | Oxia / offset index |
| Entry cache | `Position` | decoded Pulsar entry | object bytes + entry index |
| Resolver cache | `streamId + offset range` | offset index entries | Oxia offset index |
| Block cache | object key + range | object bytes | object store |

Cache invalidation comes from:

- Oxia stream notification；
- offset index commit version；
- trim updates；
- compaction generation updates；
- cursor/retention low-watermark updates from later futures。

All cache entries are discardable.

## 11. Compatibility Impact

### Broker runtime

`PersistentTopic` should continue to treat the returned object as a ManagedLedger. Nereus facade absorbs the
translation.

Impact areas to design carefully:

- assumptions that ledger id is BookKeeper ledger id；
- stats derived from `LedgerInfo`；
- offload admin operations；
- topic compaction interactions；
- cursor recovery semantics；
- termination and last confirmed entry semantics。

### Admin / stats

Admin APIs that expose ledger metadata need compatibility behavior:

- either synthesize virtual ledger stats；
- or expose Nereus-specific fields under a new admin/stats section；
- never claim virtual ledgers are BookKeeper ledgers。

### Migration

Future 2 should allow three deployment modes:

| Mode | Meaning |
| --- | --- |
| BookKeeper only | default Pulsar behavior |
| Nereus only | namespace/topic uses `managedLedgerStorageClassName=nereus` |
| Hybrid | some topics remain BookKeeper, some use Nereus |

Migration from existing BookKeeper topics is a separate future. Future 2 only ensures both storage classes can coexist.

## 12. Failure Model

| Failure | Expected behavior |
| --- | --- |
| Broker opens topic while another broker has cache | Both can open facade; append correctness relies on L0 fencing |
| Broker unloads topic | Local facade closes; durable stream state remains |
| Projection cache stale | Refresh from Oxia virtual ledger metadata / offset index |
| Append succeeds in L0 but broker crashes before returning `Position` | Data visible; producer retry handled by upper-layer dedup |
| `Position` references trimmed data | Return managed-ledger-compatible trimmed/invalid position error |
| Facade cannot resolve `ledgerId` | Refresh projection; if still missing, return data-not-available error |
| BookKeeper storage class missing in hybrid provider | Broker startup/config error, not topic runtime fallback |
| Nereus storage class missing for policy | Same behavior as current storage class not found |

## 13. Design Invariants

1. ManagedLedger facade never owns storage truth.
2. `ledgerId + entryId` is a stable external coordinate, not internal ordering.
3. `Position` maps to `streamId + offset` through virtual ledger projection.
4. `addEntry` ack maps to Future 1 `AppendResult`.
5. `readEntries` resolves through Future 1 read resolver.
6. Cursor state exposed by Future 2 is a boundary; durable cursor semantics belong to Future 3.
7. BookKeeper WAL and Object WAL profiles share the same projection contract.
8. Broker local cache is discardable.
9. Admin stats must distinguish virtual ledgers from BookKeeper ledgers.
10. Hybrid storage class selection must be explicit through policy.

## 14. Future Gate

Future 2 design is ready to move into implementation planning when the following are reviewed:

- broker-level `ManagedLedgerStorage` provider shape；
- storage class name `nereus` and coexistence with BookKeeper；
- `ManagedLedgerFactory` method mapping；
- `ManagedLedger` append/read/terminate/delete mapping；
- virtual ledger projection contract；
- `Position` and `MessageId` mapping；
- topic open/load/unload lifecycle；
- cache and invalidation boundaries；
- compatibility behavior for stats/admin/offload APIs；
- explicit boundary with Future 3 cursor semantics。

This is a design gate. It does not require benchmark, compatibility certification, CI, or real evidence.
