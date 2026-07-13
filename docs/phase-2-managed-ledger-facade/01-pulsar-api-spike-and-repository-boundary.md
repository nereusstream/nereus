# Pulsar API Spike and Repository Boundary

This document records the F2-M0 source and compile evidence. It is intentionally tied to one Pulsar
fork commit; interface names remembered from another release are not accepted evidence.

## 1. Locked Pulsar Surface

Target: `nereusstream/pulsar@100d3ef0ff7c7da36d497453b141ddff6f34a9d3`,
`version=5.0.0-M1-SNAPSHOT`。This is the fork source project's Gradle version and a composite-build selector，not a
published Maven snapshot coordinate。

| File | Locked Git blob |
| --- | --- |
| `ManagedLedgerStorage.java` | `720798123e7b41d19c69a1863ec45389627325af` |
| `ManagedLedgerStorageClass.java` | `8cbe5c3b411e5fe37c5db1ff35321a7d5e9186be` |
| `ManagedLedgerFactory.java` | `b9adbe3b353fd92e541f9fd75b5c788b0468a859` |
| `ManagedLedger.java` | `0455f0efa8bb6d0ef248b870b1a68166cdcef2c8` |
| `ManagedCursor.java` | `1e6ab76c80c939cd51a29a590a50b2d079fa1c80` |
| `ManagedLedgerClientFactory.java` | `3dece00e89a7f0d2f72bff71eabe9d2dff519d37` |
| `ReadOnlyCursor.java` | `016298cb108bb2f3abc3e9b9a48c0c45486dcfef` |
| `ReadOnlyManagedLedger.java` | `91b8f92eb637e6350c659f4a44df24800696c88b` |
| `Entry.java` | `24ea5c17c0d66c324e73b60d3c53bf991cd456fd` |
| `Position.java` | `d0d6d865c9558a7a02707c48fb19b09d7fc1014b` |
| `PositionFactory.java` | `481619b8db7a60ff8d7e70a5e211f606eba71c65` |
| `ImmutablePositionImpl.java` | `06245a6b5f33af1c4d38ca9da73655a5e1bb0165` |
| `AckSetState.java` | `03ff50c1c7fe5487812fbe6485eed66debfe4b74` |
| `AsyncCallbacks.java` | `70db427afce4f811670dd018c6dd98d44230d4bc` |
| `ManagedLedgerException.java` | `1fa565d6ec788df22f5fe725afdaea73141e024a` |
| `ManagedLedgerInfo.java` | `3e4e56187e5eed531a33e1d0cf8d9bab0c94116c` |
| `EntryCacheManager.java` | `816ccd4a3e459f296a7b5775bd3d48bcd163f463` |
| `EntryCache.java` | `b2ebf7430560cb37f420ff5cd21984ba04b00c96` |
| `ManagedLedgerFactoryMXBean.java` | `43e8196daa9ae4cb1e4299abe2b5f3425009445c` |
| `ManagedLedgerMXBean.java` | `1d978e23785690bd17c2f0a9a898397361c933cb` |
| `ManagedCursorMXBean.java` | `7402bd65f793eddab8b34b0ac5ed0c40c24cf6a1` |
| `ManagedLedgerInternalStats.java` | `c109e269202115ada9c032c6168d9f6420a3eeec` |
| `ManagedLedgerConfig.java` | `9dd5ced2dc787cae110b535fa89e91af40ba03f1` |
| `ManagedLedgerFactoryConfig.java` | `a915d651bd6b2bf01eb9aadb26d1c82fa6f60a0b` |
| `ScanOutcome.java` | `c679c40e85e9174da793cc99ebd73009a55e7dfe` |
| `PersistentOfflineTopicStats.java` | `f1cb0dfd088803be1cf4c9b88525d31c0af6ddc8` |
| `Commands.java` | `a9626a3b4cb669a3aba481446be926aaf1fa9128` |

F2-M0R2 verified these blobs directly in the clean target checkout
`/Users/liusinan/apps/ideaproject/nereusstream/pulsar@100d3ef0...`。There is no longer a compatible-checkout delta in
the active evidence：the target source itself is the compile and review authority。

The first six rows are the original provider/core facade probe. The added rows are also locked because F2 production
code directly implements or returns them, including cache and statistics compatibility objects. An upgrade gate
compares all rows.

`javap -public` reports the following total declared API members, including default/static members:

| Interface | Declared members | Consequence |
| --- | ---: | --- |
| `ManagedLedgerStorage` | 6 | Broker-private provider lifecycle must be implemented in the fork |
| `ManagedLedgerStorageClass` | 2 | Name and factory only |
| `ManagedLedgerFactory` | 26 | A facade must cover admin/cache/properties methods, not only `open` |
| `ManagedLedger` | 87 | Many BookKeeper-shaped compatibility methods need explicit behavior |
| `ManagedCursor` | 89 methods plus one constant | F2 cannot claim a full cursor implementation |
| `ReadOnlyCursor` | 11 | Every method is implemented in F2 |
| `ReadOnlyManagedLedger` | 4 | Read-only open returns a concrete read-only type, not a writable cast |
| `Entry` | 13 including defaults | Buffer ownership and metadata parsing are broker compatibility contracts |
| `Position` | 10 including defaults/bridge | One-ledger ordering and sentinels require an explicit audit |
| `EntryCacheManager` / `EntryCache` | 11 / 8 | Zero-capacity compatibility objects still need exact invalidation/read behavior |
| factory / ledger / cursor MXBeans | 16 / 37 / 13 | Stats cannot fabricate BookKeeper replication or cursor-ledger persistence |
| `ManagedLedgerInternalStats` | mutable admin DTO | Every BookKeeper-shaped field needs a Nereus meaning or neutral value |
| config / scan / offline-stats values | locked source | Mutable config and admin behavior must be re-audited on target drift |

Counts are drift detectors, not acceptance criteria. A default method can still be semantically wrong
for Nereus, and a source-compatible signature can still change broker behavior.

### Broker-private call-site lock

F2-M0R also changes fork-private call paths, so compilation against the managed-ledger interfaces alone is
insufficient. The target commit source audit locks:

| File | Locked Git blob | F2 reason |
| --- | --- | --- |
| `ServiceConfiguration.java` | `8b2e28f80f25a09ce2d079712a392f8f5a001a3e` | typed broker config/default feature inputs |
| `PulsarService.java` | `f80465ed3c65dad72fd4fad06a4483ef99fb706d` | storage initialization order、late broker ID and capability startup hook |
| `BrokerService.java` | `f618ed206b613e005bf6948930c1f7624fd04cc8` | config/storage-class selection path |
| `AbstractTopic.java` | `3975b180cf6cae9c78ceaf971323366800f40e5e` | effective policy hierarchy/update ordering |
| `PersistentTopic.java` | `5eba52c8da50383bf28c5267eb454b6277269687` | open, producer, publish, subscribe, txn and delete hooks |
| `Topic.java` | `684e5c6c83a102d77a8c10b02547d622bcebe643` | `PublishContext`/topic operation signatures |
| `Producer.java` | `3649da70c2ab97f9f80d340f88c730428f7de106` | remote producer and publish metadata behavior |
| `Consumer.java` | `fdad3200482d023cd41186dbb5d04064a1c65899` | ack admission before counters/txn/cursor mutation |
| `PersistentSubscription.java` | `e77ced8da168faffa73f39eb3fc900c77a5fe6b6` | cumulative/individual cursor mutation call path |
| `MessageDeduplication.java` | `a980556f49baad059a0e57460cd8649ab78dcc17` | dedup cursor/state mutation boundary |
| `TopicPolicyListenerWrapper.java` | `da22b44a632c2375db321202d5966c047418477f` | live policy listener admission ordering |
| `ManagedLedgerInterceptorImpl.java` | `f598b3ebf6a302d0ec09dfe1ebfde73343b12881` | broker metadata/payload processor composition |
| `TransactionBufferDisable.java` | `0184a5af3afd51fe8e6d0000ebe6b663e0d68384` | Nereus non-transactional topic behavior |
| `PersistentTopicsBase.java` | `a991ca715d8a10c8db348ba4fd5fe0778954196b` | topic admin mutation gates |
| `NamespacesBase.java` | `911d3de2b04c26100f7da9b6fc1eeb594393ed46` | namespace policy/storage-class gates |
| `Policies.java` | `df3cf53a3ca59c809df07a4287d3ba39dedf1d93` | namespace effective feature inputs |
| `TopicPolicies.java` | `356e2a4e072f84fa795092de2f42c6d302d0533e` | topic local/global feature inputs |
| `HierarchyTopicPolicies.java` | `b476c9d290052b510ff4ff4b96b1a4a76dcee330` | effective precedence parity oracle |
| `PersistencePolicies.java` | `684c202f15852edec092772974b1c1be9bc0c404` | selected storage-class field |
| `BrokerRegistry.java` | `d154edfbb320ea30486735d899f5462860b1c3a4` | active-broker set for rollout convergence |
| `BrokerRegistryImpl.java` | `dbc3c52be5cc805ef035220451b76ded72bdfbde` | broker visibility ordering |
| `BrokerLookupData.java` | `f3dbec5fadd7d705cd8c51de1e6faecc0f58812a` | capability evidence carried by active broker data |
| `MetadataStore.java` | `00c8e205e0e0ef6fdfd13f6a21adabbd71c6c4e6` | binding sync/get/conditional-put contract |
| `MetadataStoreExtended.java` | `22838154098b23828023f118680fd3faa9d55a79` | broker-supplied shared metadata handle |
| `CoordinationService.java` | `708f91f4a33cacc423afae8938a78d77fe9ce17c` | namespace first-create lock-manager factory |
| `LockManager.java` | `cb6601b170135465d3ae27f6eef35080e79780f6` | bounded namespace resource-lock acquisition |
| `ResourceLock.java` | `c85b13cdc1440986859dec61ec9458fcb74e0fae` | async permit release/session-loss behavior |
| `GetResult.java` | `6bb42202b1d2254506912c73c0e59d70265a027c` | binding bytes plus stat |
| `Stat.java` | `36874e5bc387a0205215248178e2487f507d4cbe` | binding expected-version source |

These are source locks, not claims that private broker classes are stable APIs. Any changed blob requires re-auditing
the exact hook ordering in `07` and updating broker integration tests even when the facade probe still compiles.

## 2. Confirmed Broker Call Path

```text
ServiceConfiguration.managedLedgerStorageClassName
  -> ManagedLedgerStorage.create(...)
  -> Reflections.createInstance(..., TCCL)
  -> ManagedLedgerStorage.initialize(...)

BrokerService.getManagedLedgerConfig(topic)
  -> ManagedLedgerConfig.storageClassName from PersistencePolicies
  -> managedLedgerStorage.getManagedLedgerStorageClass(storageClassName)
  -> storageClass.getManagedLedgerFactory()
  -> factory.asyncOpen(topicName.getPersistenceNamingEncoding(), ...)
```

The broker passes a persistence name such as
`tenant/namespace/persistent/encoded-topic`, not the canonical
`persistent://tenant/namespace/topic` URI. The facade must treat this string as the exact external
identity. It must not split on `/`, URL-decode it, remove a partition suffix or infer a domain.

The stock `ManagedLedgerClientFactory` returns only one `bookkeeper` class and makes it the default.
The fork-owned hybrid provider must compose that stock factory rather than reimplement its
BookKeeper client/configuration setup.

## 3. Interface Findings

### 3.1 ManagedLedgerStorage

The exact initialize signature is:

```java
void initialize(
    ServiceConfiguration conf,
    MetadataStoreExtended metadataStore,
    BookKeeperClientFactory bookkeeperProvider,
    EventLoopGroup eventLoopGroup,
    OpenTelemetry openTelemetry) throws Exception;
```

The hybrid provider returns `[bookkeeper, nereus]` in that order, keeps `bookkeeper` as default,
and maps a null class name to the default exactly as the stock implementation does. A configured
`nereus` class that fails initialization is a broker startup failure; it must not silently fall back
to BookKeeper for a topic whose policy names `nereus`.

### 3.2 ManagedLedgerFactory

Required groups are:

- synchronous and asynchronous open;
- read-only cursor and read-only managed-ledger open;
- info, exists, delete and shutdown;
- properties and the open-ledger map;
- cache manager/config/stats compatibility;
- unloaded-topic backlog estimation.

The ownership checker passed to `asyncOpen` does not grant a Nereus append lease. Broker topic
ownership and the L0 append-session epoch are separate checks. The facade may call the ownership
checker only when classifying an open-time ownership conflict; it cannot use a successful result as a
durable fencing token.

`ManagedLedgerConfig.isCreateIfMissing()` is part of the open contract. A false value must not call
`createOrGetStream` or publish projection metadata.

### 3.3 ManagedLedger

The broker hot append overload is:

```java
void asyncAddEntry(
    ByteBuf buffer,
    int numberOfMessages,
    AddEntryCallback callback,
    Object ctx);
```

`PersistentTopic.addComplete` consumes `entryData` to extract message metadata before it completes
the producer publish context. A Nereus callback therefore must supply the exact persisted entry bytes,
not an empty placeholder or only the payload after parsing.

The direct read boundary is:

```java
void asyncReadEntry(Position position, ReadEntryCallback callback, Object ctx);
```

The interface also requires synthetic ledgers info, first/previous/next positions, stats/properties,
termination, truncation, migration and offload-shaped methods. Each is classified in
`06-code-level-interface-contract.md`; unspecified no-op behavior is forbidden.

The stock one-ledger implementation returns the position immediately before the first retained entry from
`getFirstPosition()`。F2 therefore returns `(virtualLedgerId, trimOffset - 1)`; returning the first readable entry,
as the reviewed `pulsar-storage` prototype does, breaks stock cursor initialization.

### 3.4 ManagedCursor

Dispatcher-facing bounded reads include both:

```java
void asyncReadEntries(
    int maxEntries, long maxSizeBytes, ReadEntriesCallback callback,
    Object ctx, Position maxPosition);

void asyncReadEntriesOrWait(
    int maxEntries, long maxSizeBytes, ReadEntriesCallback callback,
    Object ctx, Position maxPosition);
```

The current interface also contains durable mark-delete, individual delete/replay, reset, ack-set,
cursor properties and backlog methods. F2 implements read-only/non-durable behavior and the basic
open boundary only. It must not return successful callbacks for durable mutations that Future 3 has
not implemented.

Locked defaults are not automatically safe. F2 overrides `ManagedLedger.getLastDispatchablePosition` (whose default
returns `EARLIEST`), `ManagedCursor.isCursorDataFullyPersistable` (whose default returns true), scan and
read-with-skip defaults where inherited behavior would misstate F2 capability.

### 3.5 F2-M0R2 broker-path findings

The exact target source adds four implementation constraints that were not closed by the original compile probe：

1. `PulsarService.newManagedLedgerStorage()` runs before the final bound addresses and `brokerId` are created。
   Runtime construction therefore generates its own cryptographically random `processRunId` and uses
   `pulsar-f2/{processRunId}` as the diagnostic writer ID；it cannot require a broker ID during
   `ManagedLedgerStorage.initialize`。A separate late PulsarService hook starts broker capability publication only
   after `brokerId` exists and before the broker becomes visible through load management。
2. `PersistentTopic.addFailed` normally fences, disconnects producers and then
   `decrementPendingWriteOpsAndCheck()` calls `readyToCreateNewLedger()` and unfences when pending writes reach zero。
   Nereus must install the write-fence handoff in documents 06/07 so this stock path waits for exact recovery rather
   than clearing an unresolved attempt。
3. `Consumer.messageAcked` reaches `PersistentSubscription.acknowledgeMessageAsync` after transaction/batch-ack
   interpretation。F2 admission must run before those mutations and accept only non-transactional, non-durable,
   one-position whole-entry cumulative ack with no batch fields/persisted-ack request；individual or ack-set input is
   rejected。The admitted Nereus path waits for local async mark-delete before recording ack counters but does not
   claim restart persistence。
4. The locked `PersistencePolicies.equals` compares `managedLedgerStorageClassName` by reference identity。F2 fork
   work must replace that comparison with `Objects.equals` and add an equal-content/different-instance regression
   test before storage-class policy updates are trusted。

## 4. Compile Probe

`spikes/PulsarManagedLedgerApiProbe.java` uses exact overloads with real Pulsar types. The companion
init script creates an isolated `f2ApiProbe` source set in `:pulsar-broker`. It neither copies a
source into Pulsar nor changes the checkout.

Run:

```bash
export NEREUS_F2_PROBE_DIR="$NEREUS_REPO/docs/phase-2-managed-ledger-facade/spikes"
"$PULSAR_REPO/gradlew" \
  -p "$PULSAR_REPO" \
  -I "$NEREUS_F2_PROBE_DIR/pulsar-api-probe.init.gradle" \
  :pulsar-broker:compileF2ApiProbeJava
```

F2-M0 result: `BUILD SUCCESSFUL`。F2-M0R2 repeated the command against the exact target checkout on 2026-07-12；
all 84 scheduled tasks, including `:pulsar-broker:compileF2ApiProbeJava`, completed with `BUILD SUCCESSFUL`。

For a target upgrade:

1. update the commit and version lock;
2. record every target blob ID in section 1 again;
3. run the compile probe against a checkout containing those blobs;
4. inspect broker call sites, not only interfaces;
5. update compatibility classifications before changing production code.

The upgrade gate also regenerates a `javap -public` signature snapshot and verifies that every member is assigned in
`06-code-level-interface-contract.md`.

## 5. Code Ownership

### Nereus repository

- Formula-based Position projection and validation.
- Pulsar Entry byte codec and `Entry` implementation.
- `ManagedLedgerFactory`, `ManagedLedger`, read-only and non-durable cursor facades.
- Projection metadata records/codecs/fake/real Oxia adapter.
- Consume Phase 1.5 L0 stream lifecycle and append-recovery extensions；their implementation/metadata truth remains
  in protocol-neutral modules, not in the facade.
- Product configuration values that do not import broker-private classes.

It is acceptable for `nereus-managed-ledger` to depend on the locked managed-ledger artifact. It must
not pull `pulsar-broker` into `nereus-api`, `nereus-core`, `nereus-metadata-oxia` or
`nereus-object-store`.

### Pulsar fork

- The `ManagedLedgerStorage` implementation and hybrid composition.
- Broker configuration fields/default config documentation.
- Server distribution/runtime dependency packaging.
- Policy-selection, load/unload, restart, failover and stats integration tests.
- Any adaptation required because a broker-private interface changes.
- Topic feature admission while a `TopicName` and full policies are still available.
- Broker-metadata storage-class binding that prevents a live BookKeeper/Nereus class switch or dual first-create.

The fork must depend on released/pinned Nereus artifacts or an explicit composite-build development
mode. It must not duplicate Nereus projection or metadata algorithms.

### No-code assumptions

No change is currently required to `ManagedLedgerStorage`,
`ManagedLedgerStorageClass`, `ManagedLedgerFactory`, `ManagedLedger` or `ManagedCursor`
interfaces. If implementation later requires a new method on these Pulsar extension points, stop and
perform the compatibility/PIP review before editing the interface.
