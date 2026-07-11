# Nereus Future 2：ManagedLedger Facade

> 状态：In progress；F2-M0 design/API spike complete，F2-M1 implementation next
> 前置：Future 1 append/read/trim contract 和 projection reference 稳定
> Active code-level contract：`../phase-2-managed-ledger-facade/README.md`

本文定义 Nereus 如何通过 Pulsar ManagedLedger API 暴露给 `PersistentTopic`、dispatcher、
subscription 和 admin runtime，同时保持 `streamId + offset` 是逻辑坐标、stream head + reachable
commit log 是 append truth。

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
- broker 内 stock BookKeeper storage class 与 Nereus storage class 的 hybrid selection；
- 首版 `OBJECT_WAL_SYNC_OBJECT` facade，以及未来 profile 共用 projection contract 的版本边界。

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
- Nereus BookKeeper primary WAL profile 和 async object materialization profile。

这些能力在 Future 3、Future 5、Future 6、Future 8 中作为 projection 或上层状态处理。

当前实现约束：`nereus-managed-ledger` 还没有真实 facade；Phase 1 payload 是 one-record-per-entry
opaque batch，`AppendResult`/`ResolvedObjectRange` 仍是 object-shaped。F2 首版显式只接受
`OBJECT_WAL_SYNC_OBJECT`。Broker hybrid mode 中的 `bookkeeper` 继续走 stock BookKeeper factory，
不等于 Nereus BookKeeper profile 已实现；后者进入 facade 前必须先完成 generic primary read-target
设计，不能用伪造 object identity 兼容。

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
| `getNumberOfEntries` | exact `committedEndOffset - trimOffset` under F2 one-entry/one-offset contract |
| `getTotalSize` | derive exact retained logical bytes from L0 index summary; no correctness-path approximation |
| `terminate` | set stream state to sealed/terminated in Oxia |
| `close` | close facade, release local cache/session, not stream data |
| `deleteCursor` | delete cursor state in Future 3 schema |
| offload APIs | fail explicitly in F2；never route virtual ledger IDs to BookKeeper/offloader code |

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

F2-M0 locks projection mapping version 1:

```text
one stream                  <-> one stable virtual ledger
one persisted Pulsar Entry  <-> one stream offset
Position.ledgerId           == projection.virtualLedgerId
Position.entryId            == stream offset
```

`virtualLedgerId` is allocated once from an Oxia single-key CAS allocator in the positive high range.
Future 2 does not roll the virtual ledger at physical object, append-session, broker or compaction
boundaries. L0 `committedEndOffset` and `trimOffset` supply mutable range state；projection metadata
must not copy them into a second visibility truth。

For a batched Pulsar Entry, `MessageId.batchIndex` remains a sub-index inside the same persisted Entry:

```text
MessageId(ledgerId, entryId, batchIndex)
  -> Position(ledgerId, entryId)
  -> streamId + offset(entryId)
  -> decode batchIndex inside the returned Entry bytes
```

`batchIndex` never adds to the stream offset。This follows the implemented Phase 1
`OPAQUE_RECORD_BATCH` constraint where every `AppendEntry` has `recordCount == 1`。

Rules:

- empty LAC is `Position(virtualLedgerId, -1)`；
- non-empty LAC is `Position(virtualLedgerId, committedEndOffset - 1)`；
- trim never renumbers an entry or changes the ledger id；
- position mapping is formula-based and does not add one Oxia record/write per entry；
- a returned `Position` remains resolvable until retention/trim legally removes its offset；
- virtual ledger IDs are never passed to BookKeeper clients, handles, offloaders or metadata paths；
- a later rollover scheme requires a new mapping version and mixed-version compatibility design。

The complete model, sentinel behavior and entry byte contract are defined by
`../phase-2-managed-ledger-facade/02-projection-and-entry-contract.md`。

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
Future 1 append session fencing and stream-head commit。Generation-0 offset index 可以从 reachable commit
repair，不能被 facade 当成唯一 append truth。

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
/nereus/clusters/{cluster}/facade/managed-ledger/ledger-id-allocator
/nereus/clusters/{cluster}/facade/managed-ledger/topics/{managedLedgerNameHash}
/nereus/clusters/{cluster}/streams/{streamId}/facade/managed-ledger/virtual-ledger
/nereus/clusters/{cluster}/streams/{streamId}/facade/managed-ledger/position-index
```

The topic projection record is authoritative and contains the exact broker-supplied managed-ledger
name、deterministic stream ID、storage class/profile、one virtual ledger ID、mapping version、facade
state and properties。The virtual-ledger and position-index records are repairable derived records。

Create/open uses a recoverable sequence: create/get deterministic L0 stream、allocate ledger ID with
allocator-key CAS、put-if-absent authoritative topic record、then idempotently repair the two derived
records。A crash can leak an unused ledger ID but cannot replace a published projection。No step assumes
multi-key atomic commit，and append does not write projection metadata。

The position-index is one formula record per stream (`ENTRY_ID_EQUALS_STREAM_OFFSET`)，not one Oxia
record per entry。Mutable committed/trim offsets remain in the L0 stream head。Detailed records、codecs、
CAS state machine and failure tests are defined in
`../phase-2-managed-ledger-facade/03-oxia-metadata-and-recovery.md`。

## 10. Entry Cache and Read Cache

Nereus should not depend on BookKeeper ledger cache semantics. It needs a compatibility cache layer:

| Cache | Key | Value | Truth |
| --- | --- | --- | --- |
| Projection cache | `ledgerId` | virtual ledger metadata | authoritative topic projection + repairable derived records |
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
2. `ledgerId + entryId` is a stable, order-preserving external coordinate；L0 truth remains `streamId + offset`.
3. `Position` maps to `streamId + offset` through virtual ledger projection.
4. `addEntry` ack maps to Future 1 `AppendResult`.
5. `readEntries` resolves through Future 1 read resolver.
6. Cursor state exposed by Future 2 is a boundary; durable cursor semantics belong to Future 3.
7. F2 execution accepts only `OBJECT_WAL_SYNC_OBJECT`；future Nereus primary profiles may reuse the versioned projection contract only after their L0 readers/writers exist.
8. Broker local cache is discardable.
9. Admin stats must distinguish virtual ledgers from BookKeeper ledgers.
10. Hybrid storage class selection must be explicit through policy.
11. Facade callback follows the requested L0 durability boundary exactly once；`WAL_DURABLE` still implies a
    stable head commit and never exposes a broker-local temporary position。

## 14. Future Gate

F2-M0 is complete. The review locked:

- `nereusstream/pulsar@100d3ef0...` / `5.0.0-M1-SNAPSHOT` and exact interface blobs；
- fork-owned hybrid `ManagedLedgerStorage` with stock BookKeeper as default；
- Nereus-owned factory/ledger/projection metadata implementation boundary；
- one stream/one virtual ledger and `entryId == stream offset` mapping v1；
- batch index as an in-entry sub-index；
- authoritative topic record plus repairable derived records under single-key CAS rules；
- exact callback、buffer、open/close/terminate/delete and unsupported-method behavior；
- read-only/non-durable cursor delivery boundary and Future 3 durable-ack boundary；
- object-only rollout and explicit rejection of every unimplemented profile/offload path。

The executable API probe passed against interface blobs identical to the locked fork。Code-level
contracts and F2-M1 through F2-M6 gates are in
`../phase-2-managed-ledger-facade/README.md`。This closes the design gate only；Future 2 remains
in progress until all implementation/final-acceptance gates pass。
