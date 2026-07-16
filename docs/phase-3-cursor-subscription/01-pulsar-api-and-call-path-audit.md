# F3-M0 Pulsar API and Call-path Audit

## 1. Gate Purpose

F3-M0 不是完整 Pulsar 架构评审。它只回答实现 Future 3 前必须确定的四件事：

1. broker 实际调用哪些 `ManagedLedger` / `ManagedCursor` method 和 callback；
2. ack-set、read position、durable cursor enumeration 在锁定 master 中的真实语义；
3. 哪些改变留在 Nereus，哪些必须进入 Pulsar fork；
4. F3 是否能复用 F1/F2 truth，而不增加新的 append correctness owner。

审计对象是本地 checkout，不依赖网上文档或已发布 artifact。

## 2. Locked Source Evidence

Pulsar checkout：

```text
/Users/liusinan/apps/ideaproject/nereusstream/pulsar
master@7efae25af39a15407c1397d9e1f4ac4658d09daa
```

该 checkout 的 Gradle source-project string 是 `5.0.0-M1-SNAPSHOT`，但本 gate 把它解释为本地
master source selector，不把它描述为已发布版本。

以上 commit 是 F3-M0 的历史 API/blob 审计快照。F3-M4 broker integration 的历史 checkpoint 是
`master@12edc9381c147ceec8bedd530acb5be7db339707`，F3-M5 recovery checkpoint 是
`master@a2bad4cfa260cc4575ae759f8a345ce969c8ec3a`；当前 F3-M6 implementation/source lock 是
`master@ff6e4fdfc03ffd8535ab2ece58d247dd1c64e8b4`。`phase3M6Check` / `phase3M6FinalCheck` 对该 clean checkout
重新执行 composite compile、loaded/unloaded/namespace route audit、focused/real-service tests、spotless and
checkstyle gates。M0 的 blob 表不回写为后续实现文件 hash，以保留可复核的原始 gate evidence。

| Source file | Git blob at locked commit |
| --- | --- |
| `managed-ledger/.../ManagedCursor.java` | `1e6ab76c80c939cd51a29a590a50b2d079fa1c80` |
| `managed-ledger/.../ManagedLedger.java` | `0455f0efa8bb6d0ef248b870b1a68166cdcef2c8` |
| `managed-ledger/.../ManagedLedgerFactory.java` | `b9adbe3b353fd92e541f9fd75b5c788b0468a859` |
| `managed-ledger/.../AsyncCallbacks.java` | `70db427afce4f811670dd018c6dd98d44230d4bc` |
| `managed-ledger/.../Position.java` | `d0d6d865c9558a7a02707c48fb19b09d7fc1014b` |
| `managed-ledger/.../impl/AckSetState.java` | `03ff50c1c7fe5487812fbe6485eed66debfe4b74` |
| `managed-ledger/.../impl/AckSetStateUtil.java` | `11ab520b68e925fdc0882ec6ef4be316b27489f3` |
| `managed-ledger/.../impl/ManagedCursorImpl.java` | `00c2f4013796c50202bfa4814049572bcc03b1fd` |
| `managed-ledger/.../impl/ManagedCursorContainerImpl.java` | `b632ee7f21c39b3649e81ccdbe6292d00a3fb110` |
| `managed-ledger/.../impl/ManagedLedgerImpl.java` | `26fdb458a21b2edfcdbaed049681e69fd4b99077` |
| `managed-ledger/.../mledger/ScanOutcome.java` | `c679c40e85e9174da793cc99ebd73009a55e7dfe` |
| `managed-ledger/src/main/proto/MLDataFormats.proto` | `825da1e414cea2ad6955b44b8ccd3b5cf624b4e6` |
| `pulsar-broker/.../service/Consumer.java` | `b6bbf68dfe50b27dcd072f3b8fb540cdbd696aea` |
| `pulsar-broker/.../persistent/PersistentSubscription.java` | `e77ced8da168faffa73f39eb3fc900c77a5fe6b6` |
| `pulsar-broker/.../persistent/PersistentTopic.java` | `67a403584bed85e9be7d39e0edab150d5a5f10b2` |
| `.../PersistentDispatcherMultipleConsumers.java` | `388536907b9aa667e3dff6099b29871d850c32e9` |
| `.../PersistentDispatcherSingleActiveConsumer.java` | `08356f378335d135bb4a7da9ce1dd953be07971d` |
| `.../AbstractBaseDispatcher.java` | `be33c8ced295c3fd893a698b00ade8132c9d9aeb` |
| `pulsar-broker/.../admin/impl/PersistentTopicsBase.java` | `1a21a6295ccf14d2636f6d88666bb61caab45955` |
| `pulsar-broker/.../admin/impl/NamespacesBase.java` | `a30d50a1ae8ec8c2492df7026e1c5deab6e711f6` |
| `pulsar-broker/.../admin/v2/PersistentTopics.java` | `77b9a83f505d3205ae29be54e06cb9465736e5f2` |
| `pulsar-broker/.../service/BrokerService.java` | `02abfa5f17ec761b5a08348ecb015712b71fa297` |
| fork `NereusManagedLedgerStorage.java` | `74075fd08e049e93de0439843f450af5e73517d2` |
| fork `NereusBrokerCapabilityCoordinator.java` | `4639aebb06e6915922b60b10112a6fec60632cf9` |
| fork `NereusStorageBindingCapability.java` | `5a9659a57e5d8e96474bd0cecfbdc4a01f4ec243` |
| fork `BrokerRegistryImpl.java` | `8a987add4adade315ac4dd784ca5a27ce04162d2` |
| fork `NereusAcknowledgeValidator.java` | `d4fe15ffc5f5ce5884d7494afa38a7e175d22735` |
| fork `NereusTopicFeatureValidator.java` | `10e06f4b56c08a64b945462b140f5f886760dc6a` |
| fork `NereusAdminOperation.java` | `93d3e6383fc7efdabca7ff19a8b95e12b4c295ef` |

Relevant Nereus F2 downgrade-fence inputs at baseline `623662d9...`：

| Source file | Git blob |
| --- | --- |
| `ProjectionCreateRequest.java` | `80015a798bae4017dfbc83b82c1939941807f860` |
| `records/TopicProjectionRecord.java` | `f5621d099875d1b3fd7377eb1363bbb6c86d78a9` |
| `ProjectionMetadataStoreCore.java` | `308f34fb8349cac2e291fed82f142a2cb6571e01` |
| `NereusManagedLedgerOpenCoordinator.java` | `af90e404652f059e4de1b5ff5d72e0297942b12e` |
| `NereusManagedLedgerFactory.java` | `c472545a28b5614ee0caa4c360202ef94e75f86a` |

M0 对 checkout 中现有编译产物执行了 `javap -public`，确认 `ManagedCursor`、`ManagedLedger`、
`ManagedLedgerFactory` 和 `AsyncCallbacks.ResetCursorCallback` 的 member surface 与 source 一致。该动作不生成或提交 F3 code。

## 3. Exact ManagedLedger Cursor Surface

锁定接口要求 `NereusManagedLedger` 保持以下 overload：

```java
ManagedCursor openCursor(String name);
ManagedCursor openCursor(String name, InitialPosition initialPosition);
ManagedCursor openCursor(
        String name,
        InitialPosition initialPosition,
        Map<String, Long> properties,
        Map<String, String> cursorProperties);

void asyncOpenCursor(String name, OpenCursorCallback callback, Object ctx);
void asyncOpenCursor(
        String name,
        InitialPosition initialPosition,
        OpenCursorCallback callback,
        Object ctx);
void asyncOpenCursor(
        String name,
        InitialPosition initialPosition,
        Map<String, Long> properties,
        Map<String, String> cursorProperties,
        OpenCursorCallback callback,
        Object ctx);

void deleteCursor(String name);
void asyncDeleteCursor(String name, DeleteCursorCallback callback, Object ctx);
Iterable<ManagedCursor> getCursors();
Iterable<ManagedCursor> getActiveCursors();
```

`deleteCursor` 的 stock contract 明确规定 missing cursor 也成功。F3 因此不能把 missing/deleted
tombstone 映射成 `CursorNotFoundException`。

`newNonDurableCursor(...)` 仍由 F2 local cursor 处理。Locked `ManagedLedgerImpl` adds a created non-durable cursor
to the same live `getCursors()` container but does not track it in the durable slowest-position heap。This is safe for
subscription reconstruction because `PersistentTopic.createPersistentSubscriptions()` consumes the initial
collection before the topic admits any non-durable create。F3 preserves that runtime enumeration behavior while
ensuring the collection at open completion already contains every hydrated durable cursor。

## 4. ManagedCursor Method Families

F3 必须实现或继续保持下列锁定行为。完整逐方法合同在文档 02。

| Family | Locked methods | F3 owner |
| --- | --- | --- |
| identity/local activity | `getName`, `getLastActive`, `updateLastActive`, active/inactive methods, `isDurable` | facade local state |
| reads | sync/async `readEntries`, `readEntriesOrWait`, skip variants, cancel/hasMore | F2 resolver + local read offset |
| durable cumulative ack | sync/async `markDelete` overloads | `CursorStorage` single-root CAS |
| durable individual ack | sync/async `delete(Position/Iterable)` | `CursorStorage` single-root CAS |
| positions | `getReadPosition`, `getMarkDeletedPosition`, `getPersistentMarkDeletedPosition`, `rewind`, `seek` | local read offset + durable mark-delete |
| destructive progress | `resetCursor`, `asyncResetCursor`, clear backlog, skip entries | serialized durable transition |
| random/read-only access | nth/find/scan/replay | F1 resolver, filtered by durable ack state |
| properties | position-property and cursor-property methods | root record; position properties commit with next mark-delete |
| stats/accounting | backlog/range/estimated-size/stats methods | exact entry count where available; documented estimates otherwise |
| batch helpers | `trimDeletedEntries`, `getDeletedBatchIndexesAsLongArray`, `getBatchPositionAckSet` | effective ack state; defensive array copy |
| lifecycle | sync/async close, `isClosed`, duplicate non-durable | facade lifecycle |

Callbacks are exactly-once terminal pairs from `AsyncCallbacks`; `asyncResetCursor` uses
`ResetCursorCallback.resetComplete(Object)` / `resetFailed(ManagedLedgerException,Object)`.

The locked source constant is exactly：

```java
ManagedCursor.CURSOR_INTERNAL_PROPERTY_PREFIX = "#pulsar.internal.";
```

`ManagedCursorImpl.setCursorProperties` tests this constant。The nearby interface Javadoc says
`#pulsar_internal.`，but that text is stale and is not the executable contract。F3 preserves/rejects the dot-form
prefix exactly and pins it in the composite API/source gate。

`ManagedCursorImpl.putProperty/removeProperty` return `true` whenever an initialized position-property stage accepts
the call，including same-value put and absent-key remove；the boolean is not a map-changed flag。F3 keeps that public
surface while using an internal revision only for actual staged-map changes。

## 5. Broker Call-path Findings

### 5.1 Topic open and subscription reconstruction

Locked call path：

```text
BrokerService opens ManagedLedger
  -> PersistentTopic.initialize()
     -> ledger.getCursors()
     -> createPersistentSubscriptions()
        -> Codec.decode(cursor.getName())
        -> new PersistentSubscription(..., cursor, cursor.getCursorProperties())
        -> cursor.setInactive()
```

Consequences：

- `NereusManagedLedger` open completion must happen only after a bounded scan has hydrated every ACTIVE durable
  cursor and referenced snapshot；
- one corrupt/missing referenced snapshot must fail topic open, not silently omit that subscription；
- at `openLedgerComplete`，`getCursors()` must already contain the complete hydrated durable set；later locally
  created non-durable cursors may join the live registry as in stock；
- F2 fork 的 `validateExistingDurableCursors` blanket rejection must be replaced by an F3 capability/admission check。

The locked `ManagedLedgerFactory.asyncOpen(name, config, callback, mlOwnershipChecker, ctx)` receives
`BrokerService`'s `() -> isTopicNsOwnedByBrokerAsync(topicName)` supplier。Current F2
`NereusManagedLedgerFactory` validates that supplier non-null but then drops it before `openFuture`。F3 must thread it
through writable open：check before owner claim，again after all root claims immediately before the ledger callback，
and around first durable cursor creation。A false/error result fences publication/callback；it is not a replacement
for the durable owner-session root claim。

### 5.2 Durable subscription create

`PersistentTopic` calls：

```java
ledger.asyncOpenCursor(
        Codec.encode(subscriptionName),
        initialPosition,
        properties,
        subscriptionProperties,
        callback,
        ctx);
```

The ManagedLedger API does **not** pass `SubscriptionType`. Exclusive/Failover/Shared is dispatcher policy,
not durable cursor identity. F3 therefore does not store `subscriptionType` in `CursorStateRecord`。

### 5.3 Acknowledgement

`PersistentSubscription.acknowledgeMessageAsync` maps：

```text
AckType.Cumulative -> cursor.asyncMarkDelete(position, mergedPositionProperties, callback, callback)
AckType.Individual -> cursor.asyncDelete(positions, callback, callback)
```

`AckCallback` completes the broker future only from cursor callback. F3 must never callback before Oxia CAS covers
the ack. The current fork waits for individual persistence only under `requirePersistedAck`; F3 changes the condition
to `requirePersistedAck || (nereusTopic && cursor.isDurable())` so all and only Nereus durable individual acks wait
for persistence。

Pending-ack counters、redelivery tracker mutation and other visible accounting must occur after persistence success。
Transactions remain rejected before entering this call path。

### 5.4 Dispatch/replay

Single-active and shared dispatchers use：

- `asyncReadEntriesWithSkipOrWait`；
- `trimDeletedEntries`；
- `getDeletedBatchIndexesAsLongArray` / `getBatchPositionAckSet`；
- `isMessageDeleted` and replay callbacks。

The cursor must return partial batch remaining-bit state on every read/replay. A partial entry is not
`isMessageDeleted`; only whole ranges and offsets below mark-delete are whole-deleted。

### 5.5 Admin paths

`PersistentSubscription` uses：

- `asyncClearBacklog`；
- `asyncSkipEntries(..., IndividualDeletedEntries.Exclude, ...)`；
- `asyncFindNewestMatching` followed by `asyncResetCursor` for timestamp reset；
- direct `resetCursor(Position)`；
- `ManagedLedger.deleteCursor` when deleting subscription。

The read-only `analyzeBacklog` route calls `cursor.duplicateNonDurableCursor(...)`，then scans that duplicate。Stock
duplication starts from the durable mark-delete boundary and copies whole individual-delete ranges plus partial-batch
ack state；it does not copy a dispatch-ahead read position。F3 adds `ANALYZE_BACKLOG` to the closed Nereus admin enum
and validates the loaded-topic route before constructing the duplicate。Its completion path calls
`ManagedLedger.asyncDeleteCursor` on that temporary non-durable name；the F3 facade must remove/close the local
instance without writing a durable tombstone。

`BrokerService.checkConsumedLedgers` housekeeping and
`PersistentTopicsBase.internalTrimTopic -> trimNonPartitionedTopic` can call
`ManagedLedger.trimConsumedLedgersInBackground(...)` directly。The F2 Nereus implementation is a normal-completing
no-op。F3 keeps the method a no-op for periodic housekeeping，but adds `TRIM_TOPIC` to the closed admin enum and makes
the loaded-topic route validate/reject it before calling the method；admin trim must not report a misleading no-op
success。Only F4 may admit that enum item and replace the facade behavior，and its implementation must call
`CursorRetentionCoordinator.requestTrim` rather than `StreamStorage.trim` directly。

F3 fork admission must allow exactly these durable subscription operations. Compaction/offload/truncate/migration
remain denied until their owning future。

## 6. Ack-set Finding

`Position` can carry an `AckSetState` extension whose `long[]` is a Java `BitSet` word layout：

```text
bit = 1  -> batch message remains unacknowledged
bit = 0  -> batch message has been acknowledged
merge    -> bitwise AND
```

Three states are distinct：

```text
no AckSet extension          -> whole entry acknowledgement
AckSet present and nonempty  -> partial batch acknowledgement
AckSet present and empty     -> whole batch entry acknowledged
```

`Position` alone does not encode the trailing cleared bits and therefore cannot prove `batchSize`. Before a partial
ack CAS, the facade resolves and decodes the persisted Pulsar Entry metadata, validates the exact batch size and
rejects any word or set bit outside that size。`ManagedLedgerConfig.deletionAtBatchIndexLevelEnabled` defaults true
at the locked baseline；when disabled, a partial-batch request fails explicitly rather than being coerced into a
whole-entry ack。

## 7. Read-position Finding

Stock `ManagedCursorImpl` recovers durable mark-delete and individual-delete state, then derives normal read position
from the next valid entry. It does not persist every dispatch advancement。

F3 therefore locks：

```text
durable: markDeleteOffset + wholeAckRanges + partialBatchAck + properties
local:   next read offset, pending waiter, active flag, lastActive, read stats
```

Persisting and restoring a dispatch-ahead `readPosition` would skip delivered-but-unacked messages after broker
failure. Redelivery from durable ack truth is compatible；skip is not。

## 8. Existing Nereus Handoff

| Existing component | F3 use | Forbidden reinterpretation |
| --- | --- | --- |
| `ManagedLedgerProjectionIdentity` | embeds stream/incarnation/virtual-ledger identity in cursor root | no new cursor virtual ledger |
| `PositionProjection` | strict Position/offset validation and projection | no batch sub-offset |
| `StreamStorage.getStreamMetadata/read/trim` | committed bounds, payload validation and idempotent trim operation | cursor cannot publish append visibility |
| `ProjectionMetadataStore` / shared Oxia runtime | pattern for strict codec, CAS, range scan, watch | no multi-key atomicity assumption |
| `ObjectStore` | immutable snapshot put/head/read | snapshot existence is not visibility |
| F2 local `NereusManagedCursor` | read/wait/direct replay foundation | local map is not durable enumeration |

No F3 cursor method is added to `nereus-api` or `StreamStorage`. Durable cursor state belongs to L1 modules；L0
only exposes its existing committed metadata/read/trim contracts。

### 8.1 Downgrade-fence finding

An F2-only `NereusManagedLedger` does not scan the future cursor keyspace，so broker lookup capability alone cannot
make it notice an existing F3 subscription。Without a durable fence，an old broker could see an empty local cursor map
and treat a reconnect as a new Latest cursor。

The locked F2 `TopicProjectionRecord` constructor canonicalizes `properties` through
`ProjectionCreateRequest.canonicalProperties`，which rejects every `nereus.*` key during decode。F3 therefore uses an
internal monotonic projection marker `nereus.cursor-protocol=1` before writing the first durable cursor。The F3 reader
explicitly permits/filters/preserves that one key；the locked F2 reader fails topic open while constructing the record。
This source behavior is part of M0 and must be retested on baseline drift。

## 9. Repository Ownership

| Repository/module | Owned F3 change |
| --- | --- |
| `nereus-metadata-oxia` | cursor keyspace、records、codec、single-key CAS/scan/watch、retention record |
| `nereus-managed-ledger` | cursor domain/state machine、snapshot store、facade、ledger hydration、local read path |
| `nereus-pulsar-adapter` | cursor config/runtime wiring and capability-ready view |
| `nereus-object-store` | no generic API change；F3 adds snapshot codec/store consumer in managed-ledger |
| `nereus-api` / `nereus-core` | no cursor API change；reuse existing L0 truth |
| Pulsar fork | broker validation、ack completion ordering、admin allowlist、capability publication、integration gates |

Fork code must stay thin: it decides admission and composes Pulsar-private objects; cursor correctness remains in
Nereus modules。

## 10. M0 Exit Decision

| Question | Evidence-based answer |
| --- | --- |
| API surface known? | Yes；source blobs and `javap -public` locked |
| broker durable reconstruction known? | Yes；synchronous `getCursors()` enumeration before subscription recreation |
| batch semantics known? | Yes；remaining-bit `long[]`, AND merge, entry decode needed for batch size |
| read-position recovery known? | Yes；normal dispatch position is local, durable ack truth drives restart |
| F1/F2 ownership preserved? | Yes；no L0 cursor API or projection append authority added |
| Broker open ownership signal available? | Yes；the locked async factory overload already receives it，and F3 must stop dropping it |
| implementation blocker remains? | No |

**F3-M0：PASS。** Any Pulsar commit or relevant source-blob change invalidates this pass and requires rerunning the
member and call-path audit before implementation continues。
