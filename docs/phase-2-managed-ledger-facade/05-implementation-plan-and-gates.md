# Future 2 Implementation Plan and Gates

This plan starts after F2-M0/M0R design and the Phase 1.5 P15-M5 final gate. A milestone is complete only when its code, focused tests and listed
gate exist; documentation alone is not implementation evidence.

## 1. Build and Version Gate

Before F2-M1 production code:

1. pass `phase15FinalCheck` and the protocol-neutral F2 prerequisite fixture from
   `../phase-1.5-core-storage-foundation/05-implementation-plan-and-gates.md`；
2. add a pinned Pulsar managed-ledger compile dependency for the locked fork commit;
3. support a side-by-side Gradle composite build for development/CI, with the Pulsar checkout commit
   verified as `100d3ef0...`;
4. choose an organization-owned published artifact/version for release builds before publishing any
   Nereus facade artifact;
5. keep `pulsar-broker` off the Nereus L0 modules' compile/runtime classpaths;
6. run the API compile probe in CI.
7. lock and diff the additional read-only/Entry/Position/callback/exception/config/scan/admin/cache/MXBean blobs in
   document 06;
8. generate a `javap -public` inventory and fail if a locked method has no `I/L/N/U/D` assignment.
9. verify the broker-private call-site blobs in document 01 and require a source/admission-order re-review on drift.

A floating `master`, unverified `mavenLocal()` or an Apache snapshot that does not contain the locked
fork commit is not an acceptable dependency lock.

Expected dependency direction:

```text
nereus-managed-ledger
  -> nereus-api
  -> Pulsar managed-ledger/common artifacts (locked)

nereus-managed-ledger
  -> nereus-metadata-oxia projection metadata contract

nereus-pulsar-adapter
  -> nereus-managed-ledger

Pulsar fork broker provider
  -> pulsar-broker
  -> pinned Nereus artifacts
```

## 2. F2-M1 — Projection Foundation

Production targets:

```text
nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/projection/
  VirtualLedgerProjection.java
  StreamPositionBounds.java
  PositionProjection.java
  ProjectionValidationException.java

nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/entry/
  PulsarEntryCodec.java
  EncodedAppend.java
  NereusEntry.java
```

Consume the implemented Phase 1.5 lifecycle/recovery/generic-result surface。F2-M1 adds no protocol-neutral L0 type
and does not duplicate a target, attempt or lifecycle value in `nereus-managed-ledger`。

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

Exit:

```text
Position round trip is deterministic and restart-stable from an encoded projection.
One Pulsar Entry consumes exactly one L0 offset.
Batch index never participates in offset conversion.
Delete/recreate positions cannot alias because incarnation changes stream and virtual-ledger IDs.
```

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
  records/ProjectionIdentity.java
  records/VirtualLedgerProjectionRecord.java
  records/PositionIndexRecord.java
  ProjectionIdentityMismatchException.java
  codec/F2MetadataCodecs.java
```

Phase 1.5 already owns `MetadataRecordCodecFactory`、append replay/recovery and L0 lifecycle。F2-M2 implements only
projection metadata/open/recreate/mirror repair and consumes the implemented L0 operations。The projection adapter
and L0 adapter share one Oxia client runtime and close it once.

Tests:

- all document 03 fake/real contract cases;
- fake and Docker Oxia concurrent open/race/restart;
- existing topic projection with missing L0 head never calls create-or-get；
- concurrent deleted-topic recreation publishes one next incarnation；
- golden bytes and registry backward compatibility;
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
  integration/NereusCreationGuard.java
  integration/NereusCreationPermit.java
  config/ManagedLedgerConfigView.java
  config/ManagedLedgerOpenConfigView.java
  config/ManagedLedgerConfigValidator.java
  NereusManagedLedger.java
  NereusReadOnlyManagedLedger.java
  NereusManagedLedgerInfo.java (builder/adapter)
  ZeroCapacityEntryCacheManager.java
  ZeroCapacityEntryCache.java
  NereusManagedLedgerFactoryStats.java
  NereusManagedLedgerStats.java
  callbacks/TerminalCallback.java
  callbacks/SerialCallbackLane.java
  errors/ManagedLedgerErrorMapper.java
```

Implement:

- factory open/dedup/reopen/shutdown;
- all add overloads through one async core;
- direct read, LAC, exact count/size and Position navigation;
- properties, terminate, close and logical delete;
- every factory/ledger/read-only/Entry signature and audited default in document 06;
- one synthetic virtual `LedgerInfo` and exact Nereus stats semantics;
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
- `KNOWN_COMMITTED` recovery returns the committed Position;
- `MAY_HAVE_COMMITTED` recovery uses the same physical target/commit identity or write-fences the facade；
- background recovery that later proves `KNOWN_NOT_COMMITTED` releases the lane/capacity without a second callback,
  while permanent invariant/corruption remains fenced；
- uncertain append fences the local writer rather than inventing a result;
- read releases every facade-owned entry/buffer on success, partial failure and callback exception; a throwing test
  callback releases the entry ownership it accepted;
- close/unload does not seal/delete/trim;
- terminate and delete partial failures reconcile after restart;
- delete followed by create-if-missing opens the next incarnation; stale positions fail；
- non-null interceptor/offload/shadow/auto-skip config fails before storage IO; read-compacted cursor args fail before
  read IO.
- `getConfig()` preserves stock reference identity; `setConfig()` publishes only a fully validated replacement, and a
  concurrent direct mutation after operation-view capture cannot change that admitted operation's behavior.

Exit:

```text
Factory and ledger compile against every non-default method in the locked interfaces.
Every locked default is audited; supported methods are deterministic and unsupported methods use their exact failure channel.
No callback or buffer leak is detected.
```

## 5. F2-M4 — Cursor Boundary

Production targets:

```text
nereus-managed-ledger/.../cursor/
  AbstractNereusCursor.java
  NereusReadOnlyCursor.java
  NereusNonDurableCursor.java
  NereusManagedCursorBoundary.java
  PendingReadWaiter.java
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
  NereusStorageClassBindingStore.java
  StorageClassBindingState.java
  StorageClassBindingRecord.java
  StorageClassOpenPermit.java
  StorageClassDeletePermit.java
  NereusAdminOperation.java

distribution/server build/runtime dependency wiring
conf/broker.conf and standalone documentation
broker integration tests

nereus-pulsar-adapter/.../
  NereusRuntimeProvider.java
  DefaultNereusRuntimeProvider.java
  NereusRuntimeConfiguration.java

nereus-object-store/.../
  ObjectStoreProvider.java
  ObjectStoreConfiguration.java
  ObjectStoreSecretResolver.java
  S3CompatibleObjectStoreProvider.java
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
- require cluster-wide binding-capable broker convergence before enabling any Nereus policy; old brokers may not own
  those namespaces during rollout.

Broker tests:

- BookKeeper-only default remains unchanged;
- a Nereus policy selects only Nereus;
- BookKeeper and Nereus topics coexist in one broker;
- concurrent first-create with conflicting class selections produces one `CLAIMED/ACTIVE` binding and never two live
  durable views; crash at claim/open/activation/delete transitions resumes only the bound class；
- an `ACTIVE` binding with missing selected storage is corruption, while a `CLAIMED` binding may resume creation；
- policy selecting an absent/failed Nereus class fails load;
- Nereus topic load/unload/reload preserves Position;
- broker restart/failover preserves projection and reads;
- producer plus Reader/non-durable cursor works;
- durable subscription creation is rejected before cursor open; the direct facade boundary remains unit-tested for
  explicit mutation failure；
- virtual ledger ID never reaches mocked/real BookKeeper read/delete/offload APIs;
- default broker stats remain valid and Nereus-specific stats do not claim BookKeeper ledgers;
- namespace-level Nereus selection cannot silently pull system topics onto an unsupported path；
- reflection/TCCL construction, partial initialize cleanup and aggregated close behavior are deterministic.

Exit:

```text
Hybrid selection is explicit and restart-safe.
The stock BookKeeper path remains backward compatible.
```

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
14. fail broker-metadata/Oxia/object IO at each boundary and prove binding/callback/resource recovery.

F2 completion requires all scenarios. A green projection unit test or a broker that only starts is not
evidence for the end-to-end milestone.

## 8. Stop-the-line Conditions

Stop implementation and update design if:

- Phase 1.5 P15-M5 API/behavior differs from the locked F2 prerequisite and cannot be reconciled without changing
  Position/callback/lifecycle semantics；
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
- snapshot/count/size must be approximated for a broker correctness decision.
