# Future 2 Implementation Plan and Gates

This plan starts after F2-M0. A milestone is complete only when its code, focused tests and listed
gate exist; documentation alone is not implementation evidence.

## 1. Build and Version Gate

Before F2-M1 production code:

1. add a pinned Pulsar managed-ledger compile dependency for the locked fork commit;
2. support a side-by-side Gradle composite build for development/CI, with the Pulsar checkout commit
   verified as `100d3ef0...`;
3. choose an organization-owned published artifact/version for release builds before publishing any
   Nereus facade artifact;
4. keep `pulsar-broker` off the Nereus L0 modules' compile/runtime classpaths;
5. run the API compile probe in CI.

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
  PositionProjection.java
  ProjectionValidationException.java

nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/entry/
  PulsarEntryCodec.java
  EncodedAppend.java
  NereusEntry.java
```

Also add the protocol-neutral L0 lifecycle request types and API methods, but do not implement facade
terminate/delete until their core state machines exist.

Tests:

- every mapping/encoding case in document 02;
- golden projection record/domain-separated name vectors;
- `ByteBuf` reference-count leak tests;
- no Pulsar imports in L0 modules;
- only `OBJECT_WAL_SYNC_OBJECT` accepted.

Exit:

```text
Position round trip is deterministic and restart-stable from an encoded projection.
One Pulsar Entry consumes exactly one L0 offset.
Batch index never participates in offset conversion.
```

## 3. F2-M2 — Projection Metadata

Production targets:

```text
nereus-metadata-oxia/.../
  ManagedLedgerProjectionKeyspace.java
  ManagedLedgerProjectionMetadataStore.java
  FakeManagedLedgerProjectionMetadataStore.java (test fixture)
  OxiaJavaManagedLedgerProjectionMetadataStore.java
  records/LedgerIdAllocatorRecord.java
  records/TopicProjectionRecord.java
  records/VirtualLedgerProjectionRecord.java
  records/PositionIndexRecord.java
  codec/F2MetadataCodecs.java
```

Implement L0 seal/logical-delete stream-head CAS state machines here and in core. Logical delete does
not add ObjectStore deletion.

Tests:

- all document 03 fake/real contract cases;
- fake and Docker Oxia concurrent open/race/restart;
- golden bytes and registry backward compatibility;
- single-key CAS assertions and fault injection after every write;
- existing Phase 1 gates remain green.

Exit:

```text
One authoritative topic projection survives restart and concurrent creation.
Derived projection records repair without a multi-key transaction.
Seal/delete state is reconciled from L0 truth.
```

## 4. F2-M3 — ManagedLedger Facade

Production targets:

```text
nereus-managed-ledger/.../
  NereusManagedLedgerFactory.java
  NereusManagedLedger.java
  NereusReadOnlyManagedLedger.java
  NereusManagedLedgerInfo.java (builder/adapter)
  NereusEntryCacheManager.java
  NereusManagedLedgerFactoryStats.java
  callbacks/TerminalCallback.java
  errors/ManagedLedgerErrorMapper.java
```

Implement:

- factory open/dedup/reopen/shutdown;
- all add overloads through one async core;
- direct read, LAC, exact count/size and Position navigation;
- properties, terminate, close and logical delete;
- one synthetic virtual `LedgerInfo`;
- explicit failures for unsupported offload/migrate/truncate paths.

Focused tests:

- 100+ concurrent opens return one local facade and one durable projection;
- append callback success/failure/timeout/close races complete once;
- append success callback bytes parse identically to input;
- `KNOWN_COMMITTED` recovery returns the committed Position;
- uncertain append fences the local writer rather than inventing a result;
- read releases entries/buffers on success, partial failure and callback exception;
- close/unload does not seal/delete/trim;
- terminate and delete partial failures reconcile after restart.

Exit:

```text
Factory and ledger compile against every non-default method in the locked interfaces.
Supported methods have deterministic behavior; unsupported methods fail explicitly.
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
```

Implement read-only and non-durable cursor read/seek/skip/wait/close behavior. Basic durable cursor open
may expose identity and initial/read positions, but every durable mutation remains unsupported until
Future 3.

Tests:

- earliest/latest/trimmed initialization;
- bounded max entries/bytes/Position;
- append wakes a waiter once;
- cancel/close/append races;
- wrong-ledger and trimmed position errors;
- durable mark-delete/delete/replay/reset/property mutations never report success.

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

distribution/server build/runtime dependency wiring
conf/broker.conf and standalone documentation
broker integration tests
```

Provider rules:

- initialize and retain a stock `ManagedLedgerClientFactory` for `bookkeeper`;
- initialize one Nereus factory for `nereus`;
- return BookKeeper first/default;
- reject duplicate/missing class names;
- close Nereus and BookKeeper resources once, aggregating failures;
- never fall back across storage classes at topic runtime.

Broker tests:

- BookKeeper-only default remains unchanged;
- a Nereus policy selects only Nereus;
- BookKeeper and Nereus topics coexist in one broker;
- policy selecting an absent/failed Nereus class fails load;
- Nereus topic load/unload/reload preserves Position;
- broker restart/failover preserves projection and reads;
- producer plus Reader/non-durable cursor works;
- virtual ledger ID never reaches mocked/real BookKeeper read/delete/offload APIs;
- default broker stats remain valid and Nereus-specific stats do not claim BookKeeper ledgers.

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
  + local/S3-compatible object store fixture
```

Scenarios:

1. open, append multiple single and batched Pulsar Entries, read exact bytes;
2. record Positions, unload/restart/fail over, resolve the same Positions;
3. lose append response after L0 commit and observe one callback/one Position;
4. remove derived projection records, restart and repair them;
5. trim and prove old Positions fail without renumbering retained entries;
6. terminate and prove stable final LAC/rejected append;
7. logical delete without physical object deletion;
8. run BookKeeper and Nereus topics together;
9. instrument BookKeeper APIs and prove no virtual ledger ID is passed to them;
10. fail Oxia/object IO at each boundary and prove callback/resource cleanup.

F2 completion requires all scenarios. A green projection unit test or a broker that only starts is not
evidence for the end-to-end milestone.

## 8. Stop-the-line Conditions

Stop implementation and update design if:

- Pulsar target interface blobs differ from the lock;
- one Pulsar Entry cannot remain one L0 offset;
- a required broker path needs durable ack semantics from Future 3;
- an implementation needs multi-key atomic Oxia writes;
- a virtual ledger ID must be passed into BookKeeper code;
- terminate/delete cannot be made authoritative in L0;
- callback exactly-once requires blocking an event-loop;
- a non-object Nereus storage profile reaches IO;
- snapshot/count/size must be approximated for a broker correctness decision.
