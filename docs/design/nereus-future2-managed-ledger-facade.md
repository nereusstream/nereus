# Nereus Future 2：ManagedLedger Facade

> 状态：In progress；F2-M0/M0R/M0R2、P15-M6 and F2-M1-M4 complete；F2-M5 broker integration active
> 前置：Future 1 append/read/trim contract + Phase 1.5 recovery/lifecycle + P15-M6 complete logical result handoff
> Active code-level contract：`../phase-2-managed-ledger-facade/README.md`

本文定义 Nereus 如何通过 Pulsar ManagedLedger API 暴露给 `PersistentTopic`、dispatcher、
subscription 和 admin runtime，同时保持 `streamId + offset` 是逻辑坐标、stream head + reachable
commit log 是 append truth。

## 1. Motivation

Pulsar broker 大量运行时逻辑依赖 ManagedLedger：

- topic open/load/unload；
- logical delete and safe same-name projection incarnation recreation；
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
- deployable S3-compatible ObjectStore provider、binding/capability rollout guard and broker write-fence handoff；
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

当前实现约束：`nereus-managed-ledger` 已完成 F2-M1-M4 projection metadata、factory/ledger facade、
append/read/recovery/lifecycle 和 read-only/non-durable cursor boundary。Product-side runtime 与 deployable
S3-compatible provider 已实现；Pulsar fork 已实现 hybrid binding/open/delete、feature/operation admission、
cluster capability convergence 和 namespace/topic storage-policy serialization。Generation-safe broker
write-fence handoff、multi-broker restart/failover 和 broker E2E acceptance 仍属于 active F2-M5/M6 工作。
Phase 1 payload 是 one-record-per-entry opaque batch。Phase 1.5 已实现 generic target/result、exact recovery
和 lifecycle，并保持 Object WAL strict parity；P15-M6 已把 internal commit 的 cumulative logical size 交给
public result。F2 首版仍只接受
`OBJECT_WAL_SYNC_OBJECT`，并只从 generic result 的 logical range 构造 Position。Broker hybrid mode 中的
`bookkeeper` 继续走 stock BookKeeper factory，不等于 Nereus BookKeeper profile 已实现。

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
- newly created selected namespaces/topics use `nereus`；
- changing an existing topic's storage class is rejected in F2; migration/rollback needs a later explicit data and
  projection migration contract。

## 6. API Mapping

### 6.1 ManagedLedgerFactory

| ManagedLedgerFactory method | Nereus mapping |
| --- | --- |
| `open(name, config)` | read projection first; get existing stream or create the allowed first/next incarnation |
| `asyncOpen(name, config, ...)` | async variant of open |
| `openReadOnlyCursor(name, startPosition, config)` | get-only read-only projection cursor over stream offset; never create missing state |
| `asyncOpenReadOnlyManagedLedger(...)` | get-only read-only ledger facade; never create missing state |
| `getManagedLedgerInfo(name)` | synthesize info from stream metadata, offset index summary, trim state |
| `delete(name)` | transition stream state to deleting; actual object deletion follows GC rules |
| `asyncExists(name)` | read topic projection, then verify exact current L0 stream state without create |
| fork-only `inspectStorageState(name)` | get-only `MISSING/ACTIVE/SEALED/DELETING/DELETED` binding probe；never create/repair |
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
| `getTotalSize` | exact L0 lifetime `cumulativeSize` still protected in F2; not post-trim/object physical bytes |
| `terminate` | L0 stream-head seal CAS first, then reconcile the topic projection's monotonic mirror |
| `close` | close facade, release local cache/session, not stream data |
| `deleteCursor` | remove the F2 local cursor; Future 3 owns durable cursor deletion |
| product `NereusWriteFenceView` | expose generation + exact recovery terminal so stock topic auto-unfence cannot bypass an uncertain append |
| offload APIs | fail explicitly in F2；never route virtual ledger IDs to BookKeeper/offloader code |

Synchronous methods can be implemented as compatibility wrappers over async operations, but they must not block
event-loop threads. The broker-facing hot path should use async methods.

### 6.3 ManagedCursor Boundary

Future 2 only defines the facade boundary:

```text
NereusManagedCursor
  name
  streamId
  localMarkDeleteOffset
  localReadPositionOffset
  durableProgress = absent in F2
```

Future 2 supports:

- open cursor；
- read current cursor state；
- basic read position initialization；
- mapping cursor positions to `Position` for broker compatibility。
- local non-durable whole-entry cumulative mark-delete；broker admits only its one-position nontransactional ack
  shape and rejects individual/batch/durable ack before mutation。

Future 3 owns:

- individual ack ranges；
- mark-delete CAS details；
- cursor snapshot object；
- Shared cursor recovery；
- cursor retention low-watermark。

## 7. Virtual Ledger Projection

F2-M0R locks projection mapping version 1:

```text
one topic incarnation       <-> one stream <-> one stable virtual ledger
one persisted Pulsar Entry  <-> one stream offset
Position.ledgerId           == projection.virtualLedgerId
Position.entryId            == stream offset
```

`virtualLedgerId` is allocated once from an Oxia single-key CAS allocator in the positive high range.
Future 2 does not roll the virtual ledger within an incarnation at physical object, append-session, broker or
compaction boundaries. Logical delete plus same-name create publishes the next incarnation/new deterministic stream
and a new virtual ledger ID, so old MessageIds cannot alias new data. L0 `committedEndOffset` and `trimOffset` supply
mutable range state；projection metadata must not copy them into a second visibility truth。

For a batched Pulsar Entry, `MessageId.batchIndex` remains a sub-index inside the same persisted Entry:

```text
MessageId(ledgerId, entryId, batchIndex)
  -> Position(ledgerId, entryId)
  -> streamId + offset(entryId)
  -> decode batchIndex inside the returned Entry bytes
```

`batchIndex` never adds to the stream offset。This follows the implemented Phase 1
`OPAQUE_RECORD_BATCH` constraint where every `AppendEntry` has `recordCount == 1`。
This durable payload mapping is named `PULSAR_ENTRY_V1` and is stored in the immutable L0 stream attribute
`nereus.payloadMapping`; it is distinct from the Position formula's `positionMappingVersion`.

Rules:

- empty LAC is `Position(virtualLedgerId, -1)`；
- non-empty LAC is `Position(virtualLedgerId, committedEndOffset - 1)`；
- `ManagedLedger.getFirstPosition()` is `(virtualLedgerId, trimOffset - 1)`，the position before the first retained
  entry；
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
  -> cluster capability + namespace first-create guard
  -> durable per-topic storage-class binding permit
  -> NereusManagedLedgerFactory.open(name, config)
  -> read authoritative topic projection first
  -> existing: get exact L0 stream, never recreate a missing head
  -> missing/deleted + create allowed: create first/next-incarnation empty stream and publish projection
  -> load one stream metadata snapshot
  -> initialize projection cache
  -> return NereusManagedLedger
```

Open must be idempotent. Multiple brokers may open the same topic facade, but correctness still relies on
Future 1 append session fencing and stream-head commit。Open does not eagerly acquire the append session; the first
append performs lazy acquire/renew. Generation-0 offset index 可以从 reachable commit repair，不能被 facade
当成唯一 append truth。

### 8.2 Load

Topic load initializes:

- stream metadata cache；
- latest offset index summary；
- virtual ledger projection cache；
- cursor state handles；
- tail-wait polling/watch hint and local append lane state。

Load does not transfer durable ownership.

### 8.3 Unload / Close

Unload closes local resources:

- projection cache；
- read cache references；
- cursor handles；
- local callback/operation lane references；runtime-owned unresolved append recovery is not discarded；L0 owns
  session cache/renew-on-append；
- watchers。

Unload must not:

- delete stream metadata；
- delete object bytes；
- advance cursor；
- seal stream unless explicitly terminating the topic。

If an append callback has already failed retryably uncertain, unload detaches only the facade callback/lane;
runtime-owned exact recovery and its retained attempt continue until a terminal result or runtime shutdown. Unload
never discards that core state merely to reopen the topic.

## 9. Metadata Schema

Future 2 adds projection and facade metadata on top of Future 1:

```text
broker metadata store:
  /managed-ledger-storage-bindings/v1/{namespaceHash}/{managedLedgerNameHash}

Oxia:
/nereus/clusters/{cluster}/facade/managed-ledger/ledger-id-allocator
/nereus/clusters/{cluster}/facade/managed-ledger/topics/{managedLedgerNameHash}
/nereus/clusters/{cluster}/streams/{streamId}/facade/managed-ledger/virtual-ledger
/nereus/clusters/{cluster}/streams/{streamId}/facade/managed-ledger/position-index
```

The fork-owned binding is the single-topic-key authority for one topic lifetime's selected storage class and follows
`CLAIMED -> ACTIVE -> DELETING -> DELETED`，with `CLAIMED -> DELETING` for an aborted/unactivated claim. It prevents concurrent BookKeeper/Nereus first-create and rejects a live
class switch. It does not contain offsets or bytes and does not replace either storage's lifecycle truth. A Nereus
projection stores the binding generation captured for its open.

The topic projection record is authoritative for the exact broker-supplied managed-ledger name to current
incarnation、deterministic stream name/ID、storage class/profile、one virtual ledger ID and mapping version。Its facade
state is only a monotonic mirror of authoritative L0 lifecycle state. The virtual-ledger and position-index records are
repairable derived records。

Open reads the topic record first. Existing projections use L0 get-only, so a missing current head cannot be silently
recreated empty. First create and deleted-topic recreation create/validate an empty deterministic incarnation stream、
allocate a ledger ID、publish the topic record with put-if-absent/versioned CAS、then repair the two derived records。
A crash can leak an empty unpublished incarnation or unused ledger ID but cannot replace a published current
projection。No step assumes multi-key atomic commit，and append does not write projection metadata。

The position-index is one formula record per stream (`ENTRY_ID_EQUALS_STREAM_OFFSET`)，not one Oxia
record per entry。Mutable committed/trim offsets remain in the L0 stream head。Detailed records、codecs、
CAS state machine and failure tests are defined in
`../phase-2-managed-ledger-facade/03-oxia-metadata-and-recovery.md`。

## 10. Entry Cache and Read Cache

Nereus should not depend on BookKeeper ledger cache semantics. It needs a compatibility cache layer:

| Cache | Key | Value | Truth |
| --- | --- | --- | --- |
| Projection cache | `ledgerId` | virtual ledger metadata | authoritative topic projection + repairable derived records |
| Pulsar Entry cache | `Position` | F2 effective capacity is zero; compatibility object only | no cached truth |
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
| Projection cache stale | Refresh the authoritative topic record, validate the same incarnation, then repair derived records |
| Append succeeds in L0 but broker crashes before returning `Position` | Data is visible; F2 has no producer-sequence dedup, so a later producer retry may append a duplicate |
| Append response is `KNOWN_COMMITTED`/`MAY_HAVE_COMMITTED` in-process | Recover the exact retained attempt ID; otherwise write-fence, never re-append physical bytes |
| Producer callback fails while exact recovery remains uncertain | Keep `PersistentTopic` fenced through the matching generation；stock pending-write drain cannot auto-unfence |
| `Position` references trimmed data | Return managed-ledger-compatible trimmed/invalid position error |
| Facade receives another `ledgerId` | Reject against its fixed incarnation; never adopt a newer topic lifetime to satisfy the old request |
| BookKeeper storage class missing in hybrid provider | Broker startup/config error, not topic runtime fallback |
| Nereus storage class missing for policy | Same behavior as current storage class not found |
| Current projection exists but L0 head is missing | Metadata invariant; never call create-or-get |
| Binding key missing while a Nereus projection exists | Metadata invariant；Nereus embeds a generation and its binding key is never removed, so no adoption is allowed |
| Topic is deleted then recreated | Publish next incarnation/new stream/new virtual ledger; old positions fail ledger-ID validation |
| Existing topic policy switches storage class | Reject before creating/opening an empty second storage view; F2 has no migration |

## 13. Design Invariants

1. ManagedLedger facade never owns storage truth.
2. `ledgerId + entryId` is stable and order-preserving within an incarnation；L0 truth remains `streamId + offset`.
3. `Position` maps to `streamId + offset` through virtual ledger projection.
4. `addEntry` ack maps to Future 1 `AppendResult`.
5. `readEntries` resolves through Future 1 read resolver.
6. Cursor state exposed by Future 2 is a boundary; durable cursor semantics belong to Future 3.
7. F2 execution accepts only `OBJECT_WAL_SYNC_OBJECT`；future Nereus primary profiles may reuse the versioned projection contract only after their L0 readers/writers exist.
8. Broker local cache is discardable.
9. Admin stats must distinguish virtual ledgers from BookKeeper ledgers.
10. Hybrid storage class selection must be explicit through policy.
11. Non-known append outcomes carry a recoverable attempt ID; the facade never infers its append from head movement.
12. Facade callback follows the requested L0 durability boundary exactly once；`WAL_DURABLE` still implies a
    stable head commit and never exposes a broker-local temporary position。
13. One broker-metadata binding generation selects BookKeeper or Nereus for a topic lifetime; class migration is not
    inferred from policy and cannot create two live durable views。
14. F2 never branches on `ReadTarget` to allocate Position；physical target selection/reading stays in L0。
15. Runtime bootstrap does not require a not-yet-created Pulsar broker ID；writer identity is
    `pulsar-f2/{cryptographicProcessRunId}`。
16. Cluster capability version `1` and the namespace first-create guard are required before Nereus policy can create
    durable state；a policy scan alone is insufficient。
17. Production `ifAbsent` ObjectStore upload is one conditional S3 PUT，CRC32C is independent of ETag，and local files
    remain test-only。
18. A local append overlay advances end and lifetime size from one generic result；`logicalBytes` is never added to a
    potentially stale total and known success never depends on a second metadata read。

## 14. Future Gate

F2-M0 API spike and F2-M0R/M0R2 code-level reviews are complete. The review locked:

- `nereusstream/pulsar@100d3ef0...` / `5.0.0-M1-SNAPSHOT` and exact interface blobs；
- fork-owned hybrid `ManagedLedgerStorage` with stock BookKeeper as default；
- Nereus-owned factory/ledger/projection metadata implementation boundary；
- one incarnation/one stream/one virtual ledger and `entryId == stream offset` mapping v1；
- safe same-name recreation through a new incarnation/stream/ledger namespace；
- batch index as an in-entry sub-index；
- authoritative topic record plus repairable derived records under single-key CAS rules；
- role-specific Position conversion, including stock-compatible before-first `getFirstPosition`；
- exact callback、buffer、open/close/terminate/delete, append-recovery and unsupported-method behavior；
- read-only/non-durable cursor delivery boundary and Future 3 durable-ack boundary；
- object-only rollout and explicit rejection of every unimplemented profile/offload path；
- hybrid runtime construction before broker-ID creation，code-level S3 conditional/range/checksum contract and
  broker-side unsupported-feature admission；
- single-key cross-storage binding、`NSB1` codec、get-only durable state inspection and explicit rejection of live
  BookKeeper/Nereus policy switching；
- lookup-data capability protocol plus namespace policy/first-create serialization；
- generation-safe `PersistentTopic` write-fence handoff and pre-mutation limited acknowledgement admission。

Phase 1.5 P15-M6 has passed and proves the complete F2 production prerequisite：

- generic logical append/resolve results and Object WAL parity；
- exact retained attempt ID/recovery with multi-page progress；
- authoritative L0 seal and logical delete；
- legacy/new metadata restart compatibility and unchanged strict durability；
- no new BookKeeper/async profile claim。
- public `AppendResult.cumulativeSize` comes from existing committed truth so the facade never guesses complete size
  or makes a known-success callback depend on a second read（P15-M6，implemented/final-gated）。

The executable API probe passed against interface blobs identical to the locked fork。Code-level
contracts and F2-M1 through F2-M6 gates are in
`../phase-2-managed-ledger-facade/README.md`。This closes the L0 prerequisite only；Future 2 remains
in progress until F2-M1 through F2-M6 implementation/final-acceptance gates pass。
