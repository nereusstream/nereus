# Future 2 Implementation Plan and Gates

F2-M0/M0R/M0R2 design、the complete Phase 1.5 P15-M6 final gate、F2-M1 and F2-M2 have passed。P15-M6 carries
`AppendResult.cumulativeSize` from existing committed truth。F2-M3 is the next implementation milestone.
A milestone is complete only when its code, focused tests and listed gate exist; documentation alone is not
implementation evidence.

## 1. Build and Version Gate

F2-M1 build evidence:

1. keep the completed P15-M6 `phase15FinalCheck` and protocol-neutral F2 prerequisite fixtures from
   `../phase-1.5-core-storage-foundation/05-implementation-plan-and-gates.md` green；
2. the managed-ledger compile dependency resolves from the locked Pulsar composite build；
3. `checkPulsarSourceLock` verifies the side-by-side checkout is clean and exactly `100d3ef0...`；
4. choose an organization-owned published artifact/version for release builds before publishing any
   Nereus facade artifact;
5. keep `pulsar-broker` off the Nereus L0 modules' compile/runtime classpaths;
6. run the API compile probe in CI against the exact clean checkout
   `/Users/liusinan/apps/ideaproject/nereusstream/pulsar@100d3ef0ff7c7da36d497453b141ddff6f34a9d3`；
7. lock and diff the additional read-only/Entry/Position/callback/exception/config/scan/admin/cache/MXBean blobs in
   document 01 and keep their method classifications in document 06;
8. generate a `javap -public` inventory and fail if a locked method has no `I/L/N/U/D` assignment.
9. verify the broker-private call-site blobs in document 01 and require a source/admission-order re-review on drift.

A floating `master`, unverified `mavenLocal()` or an Apache snapshot that does not contain the locked
fork commit is not an acceptable dependency lock.

The implemented composite lookup order is `-PpulsarCheckout=...`，then `NEREUS_PULSAR_CHECKOUT`，then the
development convention `../../nereusstream/pulsar` when present。`checkPulsarSourceLock` fails unless the selected
checkout is clean and exactly at the locked commit；without a composite, dependency resolution intentionally requires
the future organization-owned published artifact rather than silently compiling against another Pulsar version。

Expected dependency direction:

```text
nereus-managed-ledger
  -> nereus-api
  -> nereus-metadata-oxia projection metadata contract
  -> nereus-object-store provider/object lifecycle contract
  -> Pulsar managed-ledger/common/client-admin-api artifacts (locked；admin DTOs come from client-admin-api)

nereus-pulsar-adapter
  -> nereus-managed-ledger

Pulsar fork broker provider
  -> pulsar-broker
  -> pinned Nereus artifacts
```

## 2. F2-M1 — Projection Foundation

Production targets:

```text
nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/
  ManagedLedgerProjectionNames.java

nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/projection/
  VirtualLedgerProjection.java
  StreamPositionBounds.java
  PositionProjection.java
  F2L0RequestFactory.java
  ProjectionValidationException.java

nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/entry/
  PulsarEntryCodec.java
  EncodedAppend.java
  NereusEntry.java
```

Consume the implemented Phase 1.5 lifecycle/recovery/generic-result surface。F2-M1 adds no protocol-neutral L0 type
and does not duplicate a target, attempt or lifecycle value in `nereus-managed-ledger`。The pure exact-name/hash
helper is metadata-owned so M2 keyspace and the facade share it without a circular module dependency。
`AppendResult.cumulativeSize` must already exist；F2 must not reconstruct it from local `logicalBytes` or make known
append success depend on a second metadata read。

Tests:

- every mapping/encoding case in document 02;
- golden projection record/domain-separated name vectors;
- immutable `nereus.payloadMapping=PULSAR_ENTRY_V1` stream-attribute validation and mismatch rejection；
- incarnation-1 and next-incarnation stream-name/ID vectors；
- separate readable-entry, next-read, mark-delete and inclusive-max Position roles；
- stock-compatible `getFirstPosition == beforeFirstAvailable` behavior；
- `ByteBuf` reference-count leak tests;
- no Pulsar imports in L0 modules;
- only `OBJECT_WAL_SYNC_OBJECT` accepted.
- every `F2L0RequestFactory` method freezes the exact create/append/read/recover/seal/delete option fields and no
  facade call site constructs those options independently；
- `EncodedAppend` defensively copies on construction/access and its CRC32C golden vectors match JDK CRC32C。

Exit:

```text
Position round trip is deterministic and restart-stable from an encoded projection.
One Pulsar Entry consumes exactly one L0 offset.
Batch index never participates in offset conversion.
Delete/recreate positions cannot alias because incarnation changes stream and virtual-ledger IDs.
```

Implementation evidence（2026-07-12）：

- every production target above exists；`ManagedLedgerProjectionNames` is metadata-owned and strict-UTF-8；
- `PositionProjection` implements separate readable/read/mark-delete/max roles against the locked Pulsar `Position`；
- `PulsarEntryCodec` and `NereusEntry` preserve exact bytes、indices、reference ownership、CRC32C and zero-byte
  progress；
- the L0 dependency guard scans both build declarations and source imports；
- `phase2M1Check`、root `check` and the exact Pulsar API compile probe pass；
- release publication remains forbidden until an organization-owned non-floating Pulsar artifact is selected as
  required by item 4。

## 3. F2-M2 — Projection Metadata

Production targets:

```text
nereus-metadata-oxia/.../
  ManagedLedgerProjectionKeyspace.java
  ManagedLedgerProjectionMetadataStore.java
  FakeManagedLedgerProjectionMetadataStore.java (test fixture)
  OxiaJavaManagedLedgerProjectionMetadataStore.java
  SharedOxiaClientRuntime.java
  records/LedgerIdAllocatorRecord.java
  records/TopicProjectionRecord.java
  records/ManagedLedgerProjectionIdentity.java
  records/VirtualLedgerProjectionRecord.java
  records/PositionIndexRecord.java
  ManagedLedgerProjectionIdentityMismatchException.java
  ProjectionPublishGuard.java
  ProjectionMetadataStoreConfig.java
  codec/F2MetadataCodecs.java
```

Phase 1.5 already owns `MetadataRecordCodecFactory`、append replay/recovery and L0 lifecycle。F2-M2 implements only
projection metadata/open/recreate/mirror repair and consumes the implemented L0 operations。The projection adapter
and L0 adapter share one Oxia client runtime and close it once.

Implementation evidence（2026-07-12，foundation sub-stage）：

- `ManagedLedgerProjectionKeyspace`、protocol-neutral store/config/request/result surface and all four record types
  are implemented with exact-name/hash/full-identity validation and shared constants；
- `F2MetadataCodecs` is the third explicit registry after Phase 1 and Phase 1.5，rejects nonzero durable
  `metadataVersion`，and has frozen golden envelopes for all four record types；
- the package codec factory preserves Phase 1 bytes while dispatching F2 without decoder probing；canonical empty
  creation rejects legacy/async profiles、nonzero stream truth、attribute drift and reserved properties；
- focused metadata and managed-ledger regressions pass。The fake sub-stage below now consumes this foundation；shared
  runtime and the real Oxia adapter remain required before F2-M2 can be marked complete。

Implementation evidence（2026-07-12，fake contract sub-stage）：

- production `ProjectionMetadataStoreCore` owns the allocator、topic CAS、identity-guarded property/state updates
  and two-key idempotent repair protocol；the fake and real adapters consume this same algorithm；
- restartable `FakeManagedLedgerProjectionMetadataStore` stores codec envelopes plus backend versions，supports
  failure after allocator/topic/virtual-ledger/position-index writes and never uses object serialization；
- focused tests cover 32-way first create、24-way deleted-topic recreation、losing ID gaps、restart hydration、every
  write-after fault、hash/exact-name and derived-identity conflicts、stale identity、legal lifecycle edges、one
  monotonic deadline、bounded admission and idempotent close；
- this fake sub-stage was followed by the shared client ownership、real Java Oxia adapter and Docker parity evidence
  below；no fake-only behavior is accepted as F2-M2 completion evidence。

Implementation evidence（2026-07-12，real adapter/final sub-stage）：

- `SharedOxiaClientRuntime` owns exactly one sync client plus client/watch executors；legacy L0 `connect/close`
  remains owning，while both adapters' `usingSharedRuntime` factories close only their local operation admission；
- `OxiaJavaManagedLedgerProjectionMetadataStore` consumes the same production CAS/repair core as the fake and never
  closes the caller-owned runtime；runtime/adapter configuration drift is rejected before operation admission；
- `phase2M2Check` is the ordinary gate and `phase2M2FinalCheck` adds the real Oxia Docker suite；the F2 Docker case
  covers shared L0/projection use、16-way first create、derived deletion/repair、12-way recreation、adapter-local
  close and full runtime restart；
- F2-M2 is complete。The projection store still does not orchestrate L0 open/recovery itself；that facade-level
  protocol begins in F2-M3 and continues to consume Phase 1.5 lifecycle/recovery as a black box。
- the F2-M3 open implementation review added the mandatory protocol-neutral `ProjectionPublishGuard` to both
  publication calls；the core now evaluates it after allocator CAS and immediately before topic put/CAS under the
  same deadline，and rejects orphan derived keys before first allocation。Focused fake and real contracts retain the
  M2 completion gate with this stricter interface。

Tests:

- all document 03 fake/real contract cases;
- fake and Docker Oxia concurrent open/race/restart;
- existing topic projection with missing L0 head never calls create-or-get；
- concurrent deleted-topic recreation publishes one next incarnation；
- golden bytes and registry backward compatibility;
- prove F2 registers a third explicit codec registry without shadowing Phase 1/1.5 record types；enum state persists as
  its canonical name string, unknown values fail decode, and backend versions hydrate every F2 `metadataVersion`；
- exact managed-ledger name plus full identity is present in each derived record and is validated after hash-key
  lookup, including an injected hash-collision test；
- one monotonic operation deadline and bounded pending-operation permit cover every retry/repair path；
- single-key CAS assertions and fault injection after every write;
- existing Phase 1/1.5 gates remain green;
- projection open/reconciliation exercises Phase 1.5 seal/delete/recovery as a black-box contract without
  reimplementing its replay cursor, retained registry or lifecycle CAS。

Exit:

```text
One authoritative topic projection survives restart and concurrent creation.
Derived projection records repair without a multi-key transaction.
Seal/delete state is reconciled from L0 truth.
Same-name recreation publishes a new stream/virtual ledger without resurrecting the old stream.
Projection repair never substitutes for Phase 1.5 exact append recovery or lifecycle truth.
```

## 4. F2-M3 — ManagedLedger Facade

Production targets:

```text
nereus-managed-ledger/.../
  NereusManagedLedgerFactory.java
  NereusManagedLedgerFactoryConfig.java
  NereusManagedLedgerRuntime.java
  NereusDurableStorageState.java
  NereusStorageStateSnapshot.java
  NereusWriteFenceResolution.java
  NereusWriteFenceSnapshot.java
  NereusWriteFenceView.java
  integration/NereusCreationGuard.java
  integration/NereusCreationPermit.java
  config/ManagedLedgerConfigView.java
  config/ManagedLedgerOpenConfigView.java
  config/ManagedLedgerConfigValidator.java
  NereusManagedLedger.java
  NereusReadOnlyManagedLedger.java
  ManagedLedgerInfo builder (implemented inside NereusManagedLedgerFactory)
  cache/NereusNoopEntryCacheManager.java
  stats/NereusManagedLedgerFactoryStats.java
  NereusManagedLedgerStats.java
  callbacks/TerminalCallback.java
  callbacks/SerialCallbackLane.java
  snapshot/StreamSnapshotView.java
  snapshot/StreamSnapshotTracker.java
  snapshot/PendingReadWaiter.java
  snapshot/TailPollCoordinator.java
  errors/ManagedLedgerErrorMapper.java
```

Implement:

- factory open/dedup/reopen/shutdown;
- all add overloads through one async core;
- direct read, LAC, exact count/size and Position navigation;
- properties, terminate, close and logical delete;
- every factory/ledger/read-only/Entry signature and audited default in document 06;
- one synthetic virtual `LedgerInfo` and exact Nereus stats semantics;
- a get-only `inspectStorageState` surface for the broker binding protocol and a generation-safe
  `NereusWriteFenceView` for broker unfence decisions；
- one per-facade monotonic snapshot tracker and one coalesced tail-poll coordinator；
- zero-capacity Pulsar entry cache while retaining bounded L0 caches;
- explicit failures for unsupported offload/migrate/truncate paths.

Focused tests:

- 100+ concurrent opens return one local facade and one durable projection;
- a fake creation permit is acquired once by the winning local open; binding-generation mismatch fails before L0 IO,
  and publish-time revalidation failure leaves no topic projection；
- read-only ledger/cursor open never creates a missing topic even when its config has `createIfMissing=true`；
- append callback success/failure/timeout/close races complete once;
- append callbacks preserve admission order across out-of-order completions; pending-operation and retained-attempt
  limits reject before byte/WAL allocation；
- append success callback bytes parse identically to input;
- a stale facade snapshot advances to the result's exact committed end/cumulative size before success callback；no
  `ReadTarget` field or post-commit metadata read participates；
- `KNOWN_COMMITTED` recovery returns the committed Position;
- `MAY_HAVE_COMMITTED` recovery uses the same physical target/commit identity or write-fences the facade；
- background recovery that later proves `KNOWN_NOT_COMMITTED` releases the lane/capacity without a second callback,
  while permanent invariant/corruption remains fenced；
- uncertain append fences the local writer rather than inventing a result;
- the implemented Phase 1.5 `AppendCoordinator` remains the sole retained-attempt owner；no F2 recovery registry or
  second physical replay runner is constructed；
- `inspectStorageState` performs no create/repair/write, distinguishes all durable states, and treats present
  projection plus missing/mismatched L0 truth as corruption；
- stale write-fence generation completion and `readyToCreateNewLedger()` cannot clear a current fence；
- read releases every facade-owned entry/buffer on success, partial failure and callback exception; a throwing test
  callback releases the entry ownership it accepted;
- close/unload does not seal/delete/trim;
- terminate and delete partial failures reconcile after restart;
- delete followed by create-if-missing opens the next incarnation; stale positions fail；
- non-null interceptor/offload/shadow/auto-skip config fails before storage IO; read-compacted cursor args fail before
  read IO；
- `getConfig()` preserves stock reference identity; `setConfig()` publishes only a fully validated replacement, and a
  concurrent direct mutation after operation-view capture cannot change that admitted operation's behavior；
- no-config factory opens reuse the constructor-supplied default `ManagedLedgerConfig`；admin/protobuf `LedgerInfo`
  field sets are tested as distinct DTOs；
- runtime construction succeeds before Pulsar assigns a broker ID and freezes a fresh
  `pulsar-f2/{processRunId}` writer identity。

Exit:

```text
Factory and ledger compile against every non-default method in the locked interfaces.
Every locked default is audited; supported methods are deterministic and unsupported methods use their exact failure channel.
No callback or buffer leak is detected.
```

Implementation evidence（2026-07-12，writable-facade sub-stage）：

- product-owned factory config、immutable stock-config operation/open views、creation permit、durable inspection and
  write-fence value surfaces are implemented with closed validation；
- `StreamSnapshotTracker` preserves the P15-M6 exact end/cumulative-size overlay across stale metadata and rejects
  equal-version drift、newer numeric regression and identity/profile changes；
- `TerminalCallback` and bounded `SerialCallbackLane` provide exactly-once cleanup and admission-ordered callback
  delivery across out-of-order operation completions；
- `NereusManagedLedgerRuntime` validates process/writer identity、owns distinct dependencies、bounds callback
  admission and closes all resources in dependency-reverse order while aggregating failures；
- the exact `ObjectStoreProvider`/configuration/secret-resolver bootstrap protocols are present so the runtime public
  constructor is code-complete；the deployable S3 implementation and provider wiring remain assigned to F2-M5；
- `NereusManagedLedgerFactory` now implements exact-name single-flight open、bounded handles、inspection、delete、
  properties/info、shutdown、non-null zero-capacity cache and stock zero-value factory MXBean；every currently
  unsupported read-only factory method fails through the locked callback/exception channel rather than inheriting an
  unsafe default。
- `AbstractNereusManagedLedger` explicitly covers every locked non-default `ManagedLedger` method；the concrete
  `NereusManagedLedger` overrides append overloads、single-entry direct read、exact Position/count/size、synthetic
  `LedgerInfo`、topic properties、terminate/delete/close and generation-safe write-fence handoff。
- append input is copied before L0 ownership transfer，callback buffers are read-only and released after callback，and
  one runtime permit plus `SerialCallbackLane` preserves bounded admission and callback order；the concrete Object WAL
  integration test covers open dedup、append/read bytes、properties、seal/read/delete/close，while a deterministic
  uncertainty test covers callback failure followed by same-generation background recovery resolution。
- the facade's post-callback terminal observer joins the core retained attempt with one saturated timeout view；it does
  not own a replay registry、physical request or retry scheduler。The P15 core remains the only exact-recovery runner。
- `NereusReadOnlyManagedLedger` now performs strict get-only open、refresh-before-direct-read、retained count and
  immutable property exposure；cursor construction remains deliberately assigned to F2-M4。
- `NereusManagedLedgerStats` implements the complete locked `ManagedLedgerMXBean` surface without claiming
  BookKeeper replicas/cache/pending operations；admin info、offline backlog、internal stats、range counts and bounded
  predicate scans use the current complete L0 snapshot。
- 41 clean rerun tests cover F2-M3 foundations plus real Object WAL facade flow、100-way exact-name dedup、get-only
  missing rejection、out-of-order append completion/callback order、uncertain write-fence terminal handoff、local-only
  unload、lifecycle-aware exists/properties and the stable error table。
- F2-M3 is complete；F2-M4 owns read-only/non-durable/durable-boundary cursor classes、tail wait coalescing and all
  cursor-backed overrides currently routed through audited unsupported channels。
- `NereusManagedLedgerOpenCoordinator` now implements permit-first first/open/recreate、binding mismatch rejection
  before L0 IO、canonical candidate adoption、missing-head corruption、forward lifecycle mirror reconciliation and
  get-only durable inspection；focused tests cover first create、no-create missing、reopen、sealed mirror、deleted
  next-incarnation recreation and nonempty orphan-candidate rejection。

## 5. F2-M4 — Cursor Boundary

Production targets:

```text
nereus-managed-ledger/.../cursor/
  AbstractNereusCursor.java
  ../NereusReadOnlyCursor.java (implemented beside ReadOnlyManagedLedger)
  NereusNonDurableCursor.java
  NereusManagedCursorBoundary.java
  NereusManagedCursorStats.java
```

Implement read-only and non-durable cursor read/seek/skip/wait/close behavior. Basic durable cursor open
may expose identity and initial/read positions, but every durable mutation remains unsupported until Future 3.
Non-durable cumulative mark-delete/clear/reset and properties are local-only; individual ack holes remain unsupported.

Tests:

- earliest/latest/trimmed initialization;
- bounded max entries/bytes/Position;
- append wakes a waiter once;
- remote-broker append wakes a waiter through metadata polling with watches disabled；
- many cursors share one timer/one in-flight metadata poll and late generation completion cannot wake a removed
  waiter；
- `Predicate<Position>` skip is evaluated before L0 IO/Entry allocation；all-skipped visible positions install a
  register-then-recheck waiter and a predicate exception fails once；
- cancel/close/append races;
- cancel does not invoke the pending callback, while close fails it exactly once；
- wrong-ledger and trimmed position errors;
- durable mark-delete/individual-delete/reset/property mutations never report success; direct replay remains a
  non-mutating read and follows document 06.
- `isCursorDataFullyPersistable` is false for the durable boundary and batch ack getters return null.
- scan returns `COMPLETED/ABORTED/USER_INTERRUPTED` on its three non-error terminals; replay honors sort/skipped
  positions；
- sealed-tail read-or-wait fails instead of installing an unwakeable waiter.

Exit:

```text
Reader/non-durable cursor can tail across broker-local facade reopen.
No test claims durable subscription acknowledgement recovery.
```

Implementation evidence（2026-07-12，read-only sub-stage）：

- `NereusReadOnlyManagedLedger.createReadOnlyCursor` normalizes `EARLIEST/LATEST` and validates current-ledger next
  positions after a get-only metadata refresh；factory sync/async read-only cursor opens reuse the separate bounded
  read-only handle registry and never create missing storage。
- `NereusReadOnlyCursor` serializes position mutation，bounds entry count by `maxReadEntries`，honors byte and inclusive
  max-position limits，releases partial results on failure and implements read position、backlog count、skip、bounded
  newest-match scan、range count and ordered close。The real Object WAL facade test reads exact persisted bytes through
  this cursor and verifies tail position/ownership。
- `NereusManagedCursor` implements the complete locked `ManagedCursor` surface：durable-boundary mutations fail with
  the stable unsupported prefix，non-durable cumulative mark-delete/clear/reset/properties stay local，individual ack
  holes stay unsupported，and replay/scan/read/seek/skip/stats/batch-ack compatibility channels are explicit。
- `TailPollCoordinator` gives every writable ledger one timer and at most one refresh for all pending cursors；
  register-then-recheck closes the local-append race，local append signals without waiting for a poll，cancel removes
  without callback，close fails once and sealed tail fails instead of waiting forever。
- real Object WAL coverage now includes durable read with rejected durable mark-delete、local non-durable mark-delete、
  read-only cursor ownership and append-wakes-tail-read；a focused two-waiter test proves one shared metadata poll。
- F2-M4 is complete；F2-M5 broker integration is next。

## 6. F2-M5 — Pulsar Fork Integration

Fork-owned targets:

```text
pulsar-broker/.../storage/nereus/
  NereusManagedLedgerStorage.java
  NereusManagedLedgerStorageClass.java
  NereusBrokerStorageConfiguration.java
  NereusTopicFeatureValidator.java
  NereusTopicFeatureResolver.java
  NereusResolvedTopicFeatures.java
  NereusTopicOpenContext.java
  NereusTopicPolicyUpdateCoordinator.java
  NereusStorageClassMigrationGuard.java
  NamespaceStorageClassPolicyGuard.java
  NamespaceStorageClassPermit.java
  NamespaceStorageClassLockData.java
  NereusBrokerCapabilityCoordinator.java
  NereusAcknowledgeValidator.java
  NereusWriteFenceBridge.java
  NereusWriteFenceCompletion.java
  NereusStorageClassBindingStore.java
  StorageClassBindingKeyspace.java
  StorageClassBindingCodec.java
  StorageClassBindingState.java
  StorageClassBindingRecord.java
  StorageClassOpenPermit.java
  StorageClassDeletePermit.java
  NereusAdminOperation.java

fork call-site modifications (exact blobs locked in document 01):
  pulsar-broker-common/.../ServiceConfiguration.java
  pulsar-client-admin-api/.../PersistencePolicies.java
  pulsar-broker/.../PulsarService.java
  pulsar-broker/.../loadbalance/extensions/BrokerRegistryImpl.java
  pulsar-broker/.../service/BrokerService.java
  pulsar-broker/.../service/AbstractTopic.java
  pulsar-broker/.../service/Consumer.java
  pulsar-broker/.../service/persistent/PersistentTopic.java
  pulsar-broker/.../admin/impl/NamespacesBase.java
  pulsar-broker/.../admin/impl/PersistentTopicsBase.java

distribution/server build/runtime dependency wiring
conf/broker.conf and standalone documentation
broker integration tests

nereus-pulsar-adapter/.../
  NereusRuntimeProvider.java
  DefaultNereusRuntimeProvider.java
  NereusRuntimeConfiguration.java
  NereusRuntimeContext.java
  NereusProcessIdentity.java

nereus-object-store/.../
  ObjectStoreProvider.java
  ObjectStoreConfiguration.java
  ObjectStoreSecretResolver.java
  NoopObjectStoreSecretResolver.java
  S3CompatibleObjectStoreProvider.java
  S3CompatibleObjectStore.java
  S3ObjectKeyMapper.java
  S3ObjectErrorMapper.java
```

Provider rules:

- initialize and retain a stock `ManagedLedgerClientFactory` for `bookkeeper`;
- initialize one Nereus factory for `nereus`;
- return BookKeeper first/default;
- reject duplicate/missing class names;
- close Nereus and BookKeeper resources once, aggregating failures;
- never fall back across storage classes at topic runtime;
- reject an existing topic's BookKeeper/Nereus class switch in both directions and recheck BookKeeper absence before
  publishing a first/recreated Nereus projection；
- construct one shared Nereus runtime/factory and close every partially initialized resource once；
- reject unsupported system-topic, durable-subscription, deduplication, compaction, offload, transaction,
  managed-ledger payload-processor and replication features at the operation-specific gates locked in document 07；
- reject transactional, marker and delayed-delivery publishes before pending-write accounting, dedup lookup,
  transaction-buffer access or facade append；
- select `TransactionBufferDisable` for Nereus topics even when the broker transaction coordinator is globally
  enabled; reject end-txn before buffer access without changing the BookKeeper topic path；
- require a deployable S3-compatible ObjectStore provider; local filesystem remains test-only.
- require the S3 provider to map keys under one normalized prefix, issue asynchronous conditional create with
  `If-None-Match:*` for `ifAbsent`, verify exact CRC32C independently of ETag, implement strict range/zero-length/error
  semantics and resolve secrets without retaining plaintext configuration；
- require cluster-wide binding-capable broker convergence before enabling any Nereus policy；publish the exact
  capability only after hybrid storage initialization and before load-manager advertisement；
- serialize empty-namespace storage policy transition against first topic creation so an old/default-class create
  cannot cross the policy decision；
- validate Nereus acknowledgements before broker counters、transaction handling or cursor mutation, admitting only
  nontransactional whole-entry cumulative ack on a non-durable cursor；
- intercept stock `PersistentTopic` auto-unfence and await the matching `NereusWriteFenceView` generation before
  calling `unfence()`；permanent recovery failure closes/unloads the topic；
- compare storage class strings with value equality (`Objects.equals`) in `PersistencePolicies.equals`。

Broker tests:

- BookKeeper-only default remains unchanged;
- a Nereus policy selects only Nereus;
- BookKeeper and Nereus topics coexist in one broker;
- concurrent first-create with conflicting class selections produces one `CLAIMED/ACTIVE` binding and never two live
  durable views; crash at claim/open/activation/delete transitions resumes only the bound class；
- an `ACTIVE` binding with missing selected storage is corruption, while a `CLAIMED` binding may resume creation；
- a missing binding adopts existing BookKeeper as `ACTIVE` but never adopts a live Nereus projection；present Nereus
  storage without its generation-bearing binding is corruption；`DELETING` always resumes the bound factory delete；
- policy selecting an absent/failed Nereus class fails load;
- Nereus topic load/unload/reload preserves Position;
- broker restart/failover preserves projection and reads;
- producer plus Reader/non-durable cursor works;
- individual ack, batch AckSet, transaction ack and durable-subscription ack are rejected before counters/transaction
  buffer/cursor mutation；the one admitted non-durable cumulative whole-entry ack advances only local state；
- generic add failure cannot trigger stock auto-unfence while exact Nereus recovery is unresolved；stale recovery
  generation cannot unfence a later attempt；
- durable subscription creation is rejected before cursor open; the direct facade boundary remains unit-tested for
  explicit mutation failure；
- virtual ledger ID never reaches mocked/real BookKeeper read/delete/offload APIs;
- default broker stats remain valid and Nereus-specific stats do not claim BookKeeper ledgers;
- namespace-level Nereus selection cannot silently pull system topics onto an unsupported path；
- cluster capability checks reject Nereus policy enablement while any active broker lacks protocol version `1`，and
  first topic creation cannot race an empty-namespace policy update；
- namespace binding scan is namespace-prefixed/bounded and fails closed on cap、decode or backend failure；
- S3 integration covers create collision (HTTP 412)、conditional conflict (HTTP 409), CRC32C mismatch,
  partial/zero-length reads, timeout/cancellation,
  credential/raw-key redaction、resolver-array zeroing and restart against MinIO/equivalent；
- reflection/TCCL construction, partial initialize cleanup and aggregated close behavior are deterministic.
- binding-store creation guard is available before facade construction, get-only inspection is unavailable before the
  one exact factory attach, and null/double attach prevents startup。

Exit:

```text
Hybrid selection is explicit and restart-safe.
The stock BookKeeper path remains backward compatible.
```

Implementation evidence（2026-07-12，object-provider sub-stage）：

- `ObjectStoreConfiguration`/provider/secret protocols now have a deployable `S3CompatibleObjectStoreProvider` built
  on the same AWS SDK v1 baseline as the locked Pulsar fork，plus ambient/no-op secret resolution and resolver-array
  zeroing for explicit credentials。
- `S3ObjectKeyMapper` enforces one canonical prefix and rejects absolute、empty、traversal and control segments；
  `S3CompatibleObjectStore` implements conditional `If-None-Match:*` puts、independent CRC32C metadata/verification、
  exact inclusive HTTP ranges、zero-length existence/bounds checks、head and lifecycle/error mapping。
- the object-store test suite reruns cleanly with canonical-key tests；MinIO conditional/range/restart gates and the
  `nereus-pulsar-adapter`/fork broker wiring remain pending，so F2-M5 is still in progress。

## 7. F2-M6 — Final Acceptance

Ordinary Nereus gate:

```bash
./gradlew phase2Check
```

Required composition:

- Phase 1 ordinary gate;
- managed-ledger API probe;
- M1 projection/codec tests;
- M2 fake metadata tests;
- M3 facade tests;
- M4 cursor boundary tests;
- dependency guard.

Final Docker/broker gate:

```bash
./gradlew phase2FinalCheck --rerun-tasks
```

Required real environment:

```text
Pulsar fork broker
  + real Oxia
  + Nereus OBJECT_WAL_SYNC_OBJECT
  + S3-compatible object store provider and MinIO/equivalent fixture
```

Scenarios:

1. open, append multiple single and batched Pulsar Entries, read exact bytes;
2. record Positions, unload/restart/fail over, resolve the same Positions;
3. lose append response after L0 commit and observe one callback/one Position;
4. remove derived projection records, restart and repair them;
5. close the facade, invoke L0 trim from the test harness, reopen, and prove old Positions fail without renumbering
   retained entries; F2 exposes no broker trim command;
6. terminate and prove stable final LAC/rejected append;
7. logical delete without physical object deletion;
8. recreate the same topic name and prove new incarnation/new Position namespace;
9. run BookKeeper and Nereus topics together;
10. race conflicting first-create class selections and prove one binding permits only one durable view; reject a live
    class switch and allow a new class only after binding/storage delete completes;
11. instrument BookKeeper APIs and prove no virtual ledger ID is passed to them;
12. disable watch delivery and prove a remote append wakes a waiter through polling;
13. reject every unsupported broker feature at its topic/producer/publish/subscribe/admin gate before the relevant mutation
    or storage IO;
14. fail broker-metadata/Oxia/object IO at each boundary and prove binding/callback/resource recovery；
15. hold a Nereus append in retryably uncertain recovery and prove the stock pending-write path does not auto-unfence
    until the matching generation reaches committed/proven-not-committed；
16. exercise every allowed/rejected F2 acknowledgement shape and prove rejection precedes broker/cursor mutation；
17. attempt Nereus policy enablement with a mixed-capability broker set and race an empty-namespace policy update with
    first topic creation；both must preserve one durable storage-class truth；
18. run S3 conditional-create/range/checksum/timeout/restart cases and prove no production path selects local files。

F2 completion requires all scenarios. A green projection unit test or a broker that only starts is not
evidence for the end-to-end milestone.

## 8. Stop-the-line Conditions

Stop implementation and update design if:

- Phase 1.5 P15-M6 API/behavior differs from the locked F2 prerequisite and cannot be reconciled without changing
  Position/callback/lifecycle semantics；
- P15-M6 cumulative-size handoff is absent and F2 would need to guess size or make a committed callback depend on a
  fallible reread；
- Pulsar target interface blobs differ from the lock;
- one Pulsar Entry cannot remain one L0 offset;
- a required broker path needs durable ack semantics from Future 3;
- an implementation needs multi-key atomic Oxia writes;
- a virtual ledger ID must be passed into BookKeeper code;
- BookKeeper/Nereus first-create cannot be serialized by one binding CAS, or an active class can be switched without
  an explicit migration protocol;
- terminate/delete cannot be made authoritative in L0;
- a non-known append outcome cannot carry/recover the exact retained attempt;
- an existing projection can cause `createOrGetStream` to recreate a missing current L0 head；
- delete/recreate cannot allocate a new incarnation/stream/virtual-ledger namespace；
- callback exactly-once requires blocking an event-loop;
- a non-object Nereus storage profile reaches IO;
- a non-null interceptor can reach append while the codec promises byte-for-byte persistence；
- a system/internal topic can reach Nereus before the F8 bootstrap contract；
- production bootstrap has no non-local shared ObjectStore provider；
- broker generic add-failure logic can unfence a Nereus topic before exact attempt resolution；
- a broker ack path can mutate counters、transaction state or a cursor before F2 capability validation；
- an S3 `ifAbsent` implementation depends on read-before-write, ETag-as-CRC32C, or an unbounded blocking SDK call；
- namespace storage-class policy update cannot be serialized with first topic creation；
- snapshot/count/size must be approximated for a broker correctness decision.
