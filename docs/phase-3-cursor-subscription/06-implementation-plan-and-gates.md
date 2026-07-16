# Future 3 Implementation Plan and Gates

## 1. Current Status

F3-M0 and F3-M0R are complete as design gates。F3-M1 is complete and final-gated：cursor metadata records、canonical
codecs、single-key store/core/Oxia adapter、F2 activation-marker preservation、cursor ack domain and immutable NCS1
snapshot codec/store all exist；unit/golden/contract tests plus real Oxia and LocalStack integrations pass。
F3-M2 is also complete and final-gated：`CursorStorage`、retention/protection/trim recovery、owner claim/fencing、
snapshot publication/hydration and bounded mutation lanes exist；deterministic state-machine/property/concurrency/
failure-injection suites plus a real Oxia + LocalStack S3 multi-runtime recovery gate pass。F3-M3 is complete and
gated：
the canonical runtime/provider now owns cursor metadata、snapshot、retention and storage resources；the exact
broker ownership supplier is represented by a checked guard，while only the named direct/no-checker path is trusted；
`openWritable` performs pre-claim/final-publication checks、fresh owner creation、stable claim/hydration and retention
validation before constructing the ledger；hydrated durable cursors are registered before visibility，and later
exact-name durable opens call `CursorStorage.open` through one local flight。The first dual-mode `ManagedCursor`
checkpoint now routes durable ack/reset/property mutations to M2，uses the same state machine locally for non-durable
cursors，keeps dispatch position broker-local，and implements retained reads、wait/replay/scan plus property staging and
terminal close behavior。Exact-name durable open/delete operations are locally single-flight；durable delete persists
the M2 tombstone and recreate generation，while non-durable delete remains local-only。Reset/clear are ordered on the
local read lane，property staging rebases concurrent deltas after destructive persistence，and cursor/ledger close uses
one shared fully asynchronous drain。The locked ManagedCursor public surface now has an exhaustive classification
test。Dedicated durable、batch-ack、read/wait/replay、reset/seek、property and callback-safety suites now pass；ledger
close also closes cursor admission and drains admitted open/delete flights without double-closing a deleting handle。
`clearBacklog` and `skipEntries` use one refreshed committed snapshot，and `scan` honors the supplied batch size。
`phase3M3Check` is green against the locked Pulsar composite。Focused writable hydration、corrupt-root
fail-closed and factory ownership-cut suites prove that callback publication follows complete hydration，one corrupt
claim fails the whole open，the checked broker ownership supplier gates pre-claim/final publication/cached return and
first durable cursor creation，and the provider preserves the exact activation guard while its F2 compatibility bridge
stays fail closed。All twelve planned M3 conformance classes pass under `phase3M3Check`。

F3-M4 is complete and gated against local Pulsar
`master@12edc9381c147ceec8bedd530acb5be7db339707`。The fork publishes independent binding/cursor protocol properties，
requires two stable all-persistent-broker snapshots before first activation，maps typed broker settings to the canonical
seven-argument runtime configuration and six-argument context，admits only the F3 topic/subscription/ack/admin matrix，
reconstructs subscriptions from hydrated durable cursors，and defers durable acknowledgement side effects until cursor
persistence succeeds。The exact eight focused broker suites and broker/broker-common spotless checks pass through
`phase3M4Check` together with the complete M1-M3 dependency chain。

F3-M5 is complete and final-gated against its historical clean Pulsar fork
`master@a2bad4cfa260cc4575ae759f8a345ce969c8ec3a`。The deterministic suites cover every CAS、snapshot、generation、
retention and delayed-owner cut；`CursorStorageScaleTest` exercises the exact 10,000-record/page/count boundary；and
the real two-broker test proves durable subscription recovery、stable MessageIds、expiry-only cursor mutations and
BookKeeper coexistence over real Oxia and pinned LocalStack S3。`phase3M5Check` and `phase3M5FinalCheck` are green。

F3-M6 is complete and final-gated against the current clean Pulsar fork
`master@1720bc00a9122b2e89d555891956f38a5f64e3d1`。It preserves exact ordinary/middle-batch MessageIds across
history、seek、unload、owner failover and broker restart；preserves internal cursor properties；closes reset、root/
snapshot limit、activation rollout、topic-incarnation and callback rejection boundaries；publishes a read-only F4
snapshot inventory；and audits every loaded、unloaded and namespace admin route。The real gate also proves that topic
delete releases the old managed-ledger factory handle before callback and that a `DELETED` projection exposes empty
pre-open properties so same-name recreation reaches a fresh F2 incarnation。`phase3M6Check`、`phase3M6FinalCheck`、
`phase3Check` and `phase3FinalCheck` are the executable completion gates。

A later milestone is complete only when：

1. every listed production target exists or an explicitly reviewed equivalent replaces it；
2. focused deterministic tests and cross-module tests pass；
3. the milestone Gradle gate exists and is green；
4. locked Pulsar source/member/call-path checks remain green；
5. authoritative docs are updated from “planned” to exact implemented behavior in the same commit。

## 2. F3-M0 Gate Record

### 2.1 Evidence performed

Against local Pulsar `master@7efae25af39a15407c1397d9e1f4ac4658d09daa`：

- exact source files and Git blobs in document 01 were inspected；
- `ManagedCursor`、`ManagedLedger`、`ManagedLedgerFactory`、`AsyncCallbacks` member surfaces were checked from source
  and existing compiled classes with `javap -public`；
- topic open/reconstruction、subscribe/open cursor、ack、dispatch/replay、reset/clear/skip/delete paths were traced；
- the executable cursor-internal property prefix and read-only analyze-backlog/duplicate-cursor path were traced；
- `AckSetState` and `ManagedCursorImpl` recovery/persistence behavior were inspected；
- Nereus F2 projection、facade、Oxia single-key CAS、ObjectStore and L0 trim contracts were inspected；
- no source or generated probe was added because the requested M0/M0R phase is design-only。

### 2.2 M0 questions

| Gate question | Decision | Implementation consequence |
| --- | --- | --- |
| Durable cursors available synchronously at topic init? | yes, through `ledger.getCursors()` | ledger open hydrates all ACTIVE roots/snapshots before callback |
| Does openCursor receive subscription type? | no | subscription type remains broker policy; not in root |
| Is normal read position durable in stock recovery? | no | F3 persists no dispatch read position |
| Ack completion boundary? | cursor callback/future | success only after authoritative CAS |
| Batch ack coordinate? | Position extension, remaining-bit `long[]` | decode Entry for batch size; AND merge; no sub-offset |
| Can existing Oxia primitive atomically update multiple keys? | not assumed | one cursor correctness root; ordered retention protocol |
| Can current ObjectStore delete an orphan snapshot? | no | record orphan; F4 owns deletion |
| Does F3 require new L0 cursor API? | no | keep cursor in L1; reuse committed read/metadata/trim |
| Can broker housekeeping invoke L0 trim in F3? | no | `trimConsumedLedgersInBackground` stays no-op；F4 must use the coordinator |
| Can admin trim silently succeed through that no-op? | no | add/reject `TRIM_TOPIC` at the loaded-topic route in F3；F4 later admits it |
| Does live `getCursors()` include a later non-durable cursor? | yes, as locked stock behavior | open callback first exposes complete durable hydration；runtime registry may then include local non-durable facades |
| Can temporary non-durable cleanup touch durable metadata? | no | registry mode dispatch closes/removes it locally and never calls `CursorStorage.delete` |
| Can an old broker operation become visible after new-owner publication? | no | existing-root CAS is observed-before per-root claim or fenced；an already-pending CREATE/RECREATE target-key race cannot stale-finalize/callback and is recovered、claimed and rescanned before publication |
| Does broker open supply an ownership check? | yes | thread the exact supplier through checked writable open；require it before claim and final publication instead of dropping it as F2 does |

**M0 result：PASS。**

## 3. F3-M0R Narrow Design Gate

The original narrow question was：

> Do the F2/F3 protocol choices allow Future 3 and Future 4 to continue without overturning the compatibility model?

### 3.1 F2-to-F3/F4 decision matrix

| Review item | Locked answer | Gate |
| --- | --- | --- |
| `streamId + offset -> MessageId` stable? | F2 virtual ledger + `entryId=offset`; F3 never adds/remaps coordinates | pass |
| virtual ledger / entry / batch forward and reverse conversion unambiguous? | one Entry per offset; batch index remains payload/Position extension | pass |
| restart/failover/seek/history preserve MessageId? | same projection identity is hydrated before cursor; local read position is discarded | pass |
| projection becomes correctness owner? | no; it interprets F1 committed truth only | pass |
| metadata location/scale controlled? | F2 formula mapping; one root per cursor; ack holes spill to bounded snapshot | pass |
| F1 commit/fencing/visibility reused? | yes; cursor CAS is independent and cannot publish append bytes/head | pass |
| hidden compaction/GC invalidation? | closed by immutable snapshot refs, generation tombstones and recoverable protection/trim pending barriers | pass |

### 3.2 F3 protocol decision matrix

| Decision | Rejected alternative | Why locked now |
| --- | --- | --- |
| one `CursorStateRecord` CAS root | state/range/property keys | prevents partially published ack truth |
| local-only dispatch read offset | durable `readPositionOffset` | prevents failover skip of unacked delivery |
| generation + permanent tombstone | delete key and reuse generation | fences stale broker after recreate |
| full immutable snapshot + bounded root delta | mutable object or range truncation | root remains visibility owner; no lost ack |
| durable `ackStateEpoch` for destructive replacement | infer reset from generic mutation sequence/current ack shape | monotonic ack retries cannot reapply across reset/clear |
| per-writable-open owner session in retention + ACTIVE cursor roots | Pulsar ownership/watch or graceful drain alone | existing-root stale CAS is fenced；already-pending target races are recovered/claimed before the new dispatcher becomes visible |
| remaining-bit batch state + AND | deleted-bit ambiguity or batch sub-offset | matches locked Pulsar API |
| strict object/header/CRC/cap | best-effort decode/fallback | corruption cannot advance cursor |
| recoverable `PROTECTION_PENDING` before every create/backward cursor CAS | momentary version bump or periodic `min(markDelete)` | freezes the whole pre-cursor interval and closes missing-create/reset vs later floor-raise/trim race |
| `TRIM_PENDING` with exact offset/attempt/composed reason | fire-and-forget trim | uncertain/crash trim is replayable without changing audit identity and cannot unlock unsafe reset |
| live two-mode cursor registry + durable-only retention | permanently durable-only `getCursors()` | preserves stock enumeration without letting temporary cursors create roots/tombstones or retention references |
| monotonic topic-projection activation marker | capability signal alone | locked F2 decoder fails before exposing empty cursor state |
| separate cursor capability | infer from storage-binding capability | upgrade converges before per-topic activation；later downgrade fails closed |
| F4 physical GC consumes refs/trim truth | F3 deletes object on cursor callback | preserves read/ref safety and module ownership |

### 3.3 Deferred scope, not unresolved protocol

These are intentionally owned by later futures and do not block F3 implementation：

- F4 policy retention、materialization、compacted read and physical GC algorithms；
- F5 Kafka group metadata；
- F6 Lakehouse table/reference domain；
- F8 Key_Shared、transaction pending ack、replicated subscription、delayed delivery；
- performance tuning of defaults within the frozen hard/correctness limits。

There is no remaining unresolved item that changes F3 durable bytes、keyspace、coordinate semantics、callback
boundary、recovery or F4 handoff。

**M0R result：PASS。Future 3 may enter M1 implementation without a wider Nereus architecture re-review。**

## 4. F3-M1 — Metadata and Snapshot Foundation

### 4.1 Production targets

```text
nereus-metadata-oxia/.../
  CursorMetadataStore.java
  OxiaJavaCursorMetadataStore.java
  CursorMetadataStoreCore.java
  CursorKeyspace.java
  CursorNames.java
  CursorIds.java
  CursorMetadataStoreConfig.java
  ManagedLedgerCursorProtocol.java
  CursorScanPage.java
  CursorScanToken.java
  VersionedCursorState.java
  VersionedCursorRetention.java
  CursorMetadataConditionFailedException.java
  records/CursorRecordLifecycle.java
  records/CursorRetentionLifecycle.java
  records/CursorProtectionKind.java
  records/CursorStateRecord.java
  records/CursorSnapshotReferenceRecord.java
  records/CursorAckRangeRecord.java
  records/CursorPartialBatchAckRecord.java
  records/CursorProtectionIntentRecord.java
  records/CursorRetentionRecord.java
  codec/F3MetadataCodecs.java
  codec/CursorStateRecordCodecV1.java
  codec/CursorRetentionRecordCodecV1.java

modify existing F2 projection compatibility path:
  ProjectionCreateRequest.java
  ManagedLedgerProjectionMetadataStore.java
  ProjectionMetadataStoreCore.java
  OxiaJavaManagedLedgerProjectionMetadataStore.java
  records/TopicProjectionRecord.java

modify explicit metadata registry dispatch:
  codec/MetadataRecordCodecFactory.java

nereus-managed-ledger/.../cursor/
  OffsetRange.java
  BatchAckState.java
  CursorAckState.java
  AckNormalizer.java
  AckWords.java
  CursorLifecycle.java
  InitialCursorPosition.java
  CursorPropertyMutation.java
  CursorLedgerIdentity.java
  CursorOwnerSession.java
  CursorIdentity.java
  CursorState.java
  CursorSnapshotCodecV1.java
  CursorSnapshotStore.java
  DefaultCursorSnapshotStore.java
  CursorSnapshotReference.java
  CursorSnapshotWriteRequest.java
  CursorSnapshotWriteAuthority.java
  CursorSnapshotPublication.java
  CursorRetentionView.java
  CursorStorageConfig.java
```

`F3MetadataCodecs` is an explicit fourth registry addition；record types are the exact Java simple names
`CursorStateRecord` / `CursorRetentionRecord`，and it must not use decoder probing or shadow F1/F1.5/F2 record
types。The snapshot store consumes the existing generic guarded replayable ObjectStore API and F4 physical
root/protection/read-pin contracts；it adds no provider-specific S3 call。The cursor CAS remains the only visibility
point, while a post-CAS hydrate/read repairs permanent protection after response loss。

### 4.2 Tests

Planned test classes：

```text
CursorKeyspaceTest
CursorStateRecordCodecTest
CursorRetentionRecordCodecTest
F3MetadataCodecsCompatibilityTest
CursorMetadataStoreContractTest
OxiaJavaCursorMetadataStoreIntegrationTest
CursorSnapshotCodecV1Test
CursorSnapshotStoreTest
```

Required cases are the complete codec/store list in document 03, plus：

- Phase 1/1.5/F2 golden bytes unchanged；
- unactivated topic projection retains exact F2 golden bytes；activated projection has a new golden and the locked F2
  decoder rejects its reserved marker；
- public property create/update/get cannot inject、remove or observe the internal activation marker；lifecycle and
  delete/recreate preserve it；
- codec/store goldens admit only the ACTIVE/no-pending owner-only retention shape when the projection remains
  unactivated，without changing the exact F2 projection golden；a cursor without marker/root and a marker-absent
  pending root fail strict validation。M2 owns the actual writable-open absent-create/claim state machine；
- fake and real Oxia return identical decoded values/condition failures；
- 32-way absent create and tombstone CAS races；
- page continuation at 0/1/255/256/max records；
- watch-disabled authoritative reads；M2 owns the two-scan open stabilization algorithm；
- snapshot ifAbsent collision；M2 owns upload-success/CAS-loss orphan observation at the root publication boundary；
- object identity/hash/generation mismatch at every header/reference field。
- `ackStateEpoch` field golden and decode invariants；M2 owns create/reset/clear/preserve/overflow transitions；
- owner-session field golden；M2 owns claim transitions that change only owner/sequence/updated time while preserving
  snapshot bytes；
- cursor and retention domain views carry the hydrated Oxia version while encoded metadataVersion remains exactly zero；
- retention ACTIVE/protection-pending/trim-pending binary goldens，including exact persisted composed-reason replay；
- codec goldens preserve negative/high-bit remaining words and the full signed position-property `long` domain；

### 4.3 Exit

```text
One key/version owns all cursor correctness fields.
Every metadata/object byte is canonical, bounded, checksummed and golden-tested.
A stable root reference hydrates exactly one full effective ack state.
At the M1 boundary no cursor state machine or Pulsar callback was implemented.
```

That statement is the historical M1 exit boundary；M2-M6 subsequently implemented and final-gated the state
machines、ManagedCursor/Pulsar callbacks、recovery and compatibility behavior。Implemented M1 gates are
`phase3M1Check` and the real Oxia/ObjectStore extension `phase3M1FinalCheck`。

### 4.4 Completion Evidence

Completed on 2026-07-14 against local Pulsar
`master@7efae25af39a15407c1397d9e1f4ac4658d09daa`：

```text
./gradlew phase3M1Check
./gradlew phase3M1FinalCheck --rerun-tasks
```

Both pass。The final gate reruns Phase 1/1.5/F2 prerequisites，the complete F3 metadata/managed-ledger/object-store
unit suites，real Oxia restart/range/watch/CAS parity，generic S3 integration and cursor-specific LocalStack
conditional-create collision plus provider-restart hydration。Real Oxia also exposed that range ordering is
hierarchical rather than flat Java string ordering；the production sentinels and fake comparator now match that
behavior。Only the typed `ObjectAlreadyExistsException` conditional-create result rotates a snapshot ID；other
non-retriable upload failures fail immediately while preserving the public `OBJECT_UPLOAD_FAILED` mapping。

## 5. F3-M2 — CursorStorage and Retention State Machines

### 5.1 Implemented production targets

```text
nereus-managed-ledger/.../cursor/
  CursorStorage.java
  DefaultCursorStorage.java
  CursorProtocolActivationGuard.java
  CursorHandle.java
  CursorOpenRequest.java
  CursorAckRequest.java
  CursorResetRequest.java
  CursorMutationResult.java
  CursorMutationOutcome.java
  CursorStateMachine.java
  CursorStatePersistencePlanner.java
  CursorStateHydrator.java
  CursorMutationLane.java
  CursorRetentionCoordinator.java
  DefaultCursorRetentionCoordinator.java
```

M2 is protocol-neutral with respect to Pulsar classes；test adapters construct offset/batch requests directly。

### 5.2 Tests

```text
CursorStateMachineTest
CursorStateMachinePropertyTest
CursorStorageOpenTest
CursorStorageAckTest
CursorStorageResetDeleteTest
CursorStorageSnapshotSpillTest
CursorStorageConcurrencyTest
CursorStoragePropertyTest
CursorMutationLaneTest
CursorRetentionCoordinatorTest
CursorRetentionFailureInjectionTest
CursorStorageOxiaS3IntegrationTest
```

Required tests：

- every pure transition and invariant in document 04；
- first-activation-only guard invocation/no-production-bypass，activated recreate skips cluster snapshot，activation
  CAS conflict/idempotence and exact preactivation owner root -> marker -> PROTECTION_PENDING -> cursor attempt proof
  -> ACTIVE ordering；
- retention-first + bounded ACTIVE-root owner claim，full rescan stabilization，partial-claim crash recovery and stale
  owner fenced at every pre/post-CAS cut；
- unactivated empty-topic competing writable opens produce one claimed owner-only root；a delayed old-session first
  create may leave the monotonic marker activated but loses before PROTECTION_PENDING/cursor mutation after the new
  owner publishes；
- an old session that entered CREATE/RECREATE PROTECTION_PENDING before takeover may win the unclaimed target-key
  race after retention claim，but cannot stale-finalize/callback；the new owner reloads、claims/proves the winner and
  rescans before publication；
- randomized model comparison for range/partial normalization；
- 100-way same-name create, delete/recreate and stale-generation mutations；
- concurrent distinct-name creates enforce the exact record cap through the single protection slot；
- disjoint/same-offset two-client same-owner ack CAS races plus different-owner rejection；
- monotonic-ack response loss after CAS and `ALREADY_APPLIED` retry；forward reset/clear uncertain completion uses
  exact destructive-result/epoch proof or fails conflict；
- unchanged-epoch monotonic ack rebase versus reset/clear `ackStateEpoch` advance and mandatory conflict；
- root size just below/at/above snapshot threshold and hard max；
- snapshot replacement race and stable missing/corrupt failure；
- every failure cut before/after protection-pending CAS、cursor attempt CAS/finalize、trim-pending CAS and L0 trim，
  including byte-identical persisted composed-reason replay；
- cursorless requestTrim versus first cursor create shares activation marker/root and never calls L0 directly；
- a floor raise started after protection acquisition is blocked until attempt finalization；
- attempt proof remains through later ack/property/delete，and recovery never resurrects a delete winner；
- no test/model field for durable normal read position。

### 5.3 Exit

```text
CursorStorage implements durable create/ack/reset/property/delete with exact CAS semantics.
Retention floor plus protection/trim pending lifecycles survive every injected crash cut conservatively.
Oversize state fails before mutation and never truncates acknowledged ranges.
```

**Result：PASS。** Implemented gates：`phase3M2Check` and the real Oxia/ObjectStore extension
`phase3M2FinalCheck`。

Completed on 2026-07-14 against local Pulsar
`master@7efae25af39a15407c1397d9e1f4ac4658d09daa`：

```text
./gradlew phase3M2Check --rerun-tasks
./gradlew phase3M2FinalCheck --rerun-tasks
```

The ordinary gate covers all M2 cursor tests，including a fixed-seed 10,000-transition
independent ack model、100-way same-name create/recreate、exact record-cap contention、same-owner CAS union、
different-owner fencing、snapshot replacement/orphans、stable missing/corrupt snapshot failure、transient snapshot
read retry classification and every injected protection/trim response-loss cut。It also proves that a running admitted
mutation drains on handle close while queued and later operations fail terminally。

The final gate reruns the complete M1 real-service prerequisite plus
`CursorStorageOxiaS3IntegrationTest`。That combined gate uses real projection/retention/cursor keys in Oxia and an
immutable NCS1 object in pinned LocalStack Community S3 `4.14.0`；it injects a lost real-Oxia cursor-CAS response，
proves the root/snapshot result idempotently，claims it from a second independent runtime，fences the delayed first
owner and hydrates the exact ack state from a third restarted runtime。The concurrent cursorless-trim/first-create
test additionally forces both callers to observe an absent marker：the activation CAS loser must reread and accept
only the exact activated projection，while the owner root serializes TRIM_PENDING against PROTECTION_PENDING and no
caller bypasses `CursorRetentionCoordinator` for L0 trim。

## 6. F3-M3 — ManagedLedger / ManagedCursor Facade

### 6.1 Production targets

Add/modify：

```text
NereusManagedCursor.java
NereusManagedCursorStats.java
NereusManagedLedger.java
NereusLedgerOpenResult.java
NereusWritableLedgerOpenResult.java
NereusManagedLedgerOwnershipGuard.java
NereusManagedLedgerOpenCoordinator.java
NereusManagedLedgerFactory.java
NereusManagedLedgerRuntime.java
NereusManagedLedgerFactoryConfig.java
ManagedLedgerModule.java
errors/ManagedLedgerErrorMapper.java

nereus-pulsar-adapter/.../
  NereusRuntimeConfiguration.java
  NereusRuntimeContext.java
  DefaultNereusRuntimeProvider.java
```

M3 maps exact locked Pulsar types/callbacks to M2 and upgrades non-durable cursor with the same in-memory ack model。

### 6.2 Method conformance tests

```text
NereusManagedCursorDurableTest
NereusManagedCursorBatchAckTest
NereusManagedCursorReadWaitReplayTest
NereusManagedCursorResetSeekTest
NereusManagedCursorPropertiesTest
NereusManagedCursorCallbackSafetyTest
NereusManagedLedgerCursorHydrationTest
NereusManagedLedgerCursorLifecycleTest
NereusManagedLedgerFactoryCursorOpenTest
ManagedCursorPublicSurfaceClassificationTest
NereusRuntimeConfigurationCursorTest
DefaultNereusRuntimeProviderCursorTest
```

The public-surface classification test inventories every locked `ManagedCursor` method and assigns one of：

```text
D = durable F3 implementation
L = broker-local/non-durable behavior
R = F1/F2 read resolver behavior
U = explicitly unsupported by owning future
```

No method may be unclassified。Tests also lock：

- exact `Position`/mark-delete/next-read roles；
- Entry/ByteBuf release on batch validation, skip, replay, cancellation and callback throw；
- callback exactly-once/executor/no-lock behavior；
- topic open fails before callback on one corrupt cursor；
- writable factory open returns only after owner claim/hydration，while get-only/read-only/storage-state inspection
  continues through the base `NereusLedgerOpenResult` and performs no cursor claim/write；
- the broker-supplied `mlOwnershipChecker` is no longer dropped：false/error/timeout before claim or final publication
  fails fenced，and first durable cursor creation checks it before and after the durable transition；trusted direct
  sync/no-checker overloads are explicit and never used by `BrokerService`；
- `NereusRuntimeContext` carries the real fail-closed cursor activation guard；the default provider wires it unchanged
  and cannot publish/activate before the fork capability coordinator has an attached broker registry；its F2
  source-compatibility constructor injects only the named fail-closed sentinel，never an allow-all fallback；
- the locked five-argument `NereusRuntimeConfiguration` source call compiles to frozen cursor defaults，while the
  canonical seven-argument constructor enforces every cursor/Oxia/ObjectStore cross-config relation；
- `getCursors()` is complete for hydrated durable roots before open callback，then includes later live non-durable
  facades like stock；`getActiveCursors()` covers both modes while slowest/retention remains durable-only；
- restart local read from first unacked, not previous dispatch position；
- clear/skip/reset/property/delete missing semantics；
- public durable open null normalization（InitialPosition -> Latest，property maps -> empty）before immutable request
  construction；
- position-property stage revision preserves a put/remove racing an in-flight ack/reset and close failure stays
  terminal without claiming persistence；
- position-property boolean compatibility：same-value put / absent-key remove still return accepted=true，but do not
  advance the internal stage revision；
- null explicit mark-delete property map reuses the visible staged map；only a non-null empty map clears it；
- null public `setCursorProperties` normalizes to empty external replacement while null keys/values still fail；
- duplicate non-durable cursor starts at mark-delete and copies whole/partial ack truth，not local dispatch position；
- analyze-backlog duplicate cleanup removes only the exact local non-durable instance；it creates no tombstone/count
  slot，and durable/non-durable same-name aliasing is rejected；
- `trimConsumedLedgersInBackground` remains no-op and `asyncTruncate` remains rejected throughout F3。

### 6.3 Exit

```text
The locked ManagedCursor surface has a code/test classification.
Durable cursor open is never ledger-local-only.
PersistentTopic can enumerate hydrated cursors without broker changes yet.
No deployable artifact combines the marker-aware decoder with the old F2 empty-cursor writable open.
```

Implementation status：the runtime/writable-open/hydration foundation and dual-mode cursor facade above are present。
The lifecycle checkpoint now includes exact-name open/delete ordering、durable tombstone delete/recreate generation、
non-durable local cleanup、reset/clear read-lane ordering、property-stage rebase、fully async close drain and exhaustive
locked API surface classification。The durable/batch/read-wait-replay/reset-seek/property/callback conformance suites
pass，and `phase3M3Check` includes `phase3M2Check`、the source lock、managed-ledger check and Pulsar-adapter
check。Writable hydration、corrupt-root fail-closed、factory ownership cuts and exact provider activation-guard wiring
are covered by their planned focused tests。All M3 exit conditions above are satisfied；`phase3M3Check` is the green
ordinary milestone gate，and F3-M3 is complete。

## 7. F3-M4 — Pulsar Broker Integration

### 7.1 Fork targets

Modify/add under `pulsar-broker/.../storage/nereus` and call sites：

```text
NereusTopicFeatureValidator.java
NereusAcknowledgeValidator.java
NereusAdminOperation.java
NereusBrokerCapabilityCoordinator.java (or focused Cursor capability coordinator)
NereusCursorProtocolCapability.java
NereusStorageBindingCapability.java (reserved-key composition/compatibility)
NereusBrokerStorageConfiguration.java
NereusManagedLedgerStorage.java
BrokerRegistryImpl.java
PersistentTopic.java
PersistentSubscription.java
Consumer.java
loaded and namespace admin call sites for the closed operation enum
```

The capability implementation may generalize the existing coordinator, but it must publish two independent reserved
properties and preserve the F2 binding protocol behavior。

### 7.2 Fork tests

```text
NereusTopicFeatureValidatorTest
NereusAcknowledgeValidatorTest
NereusCursorProtocolCapabilityTest
NereusBrokerStorageConfigurationCursorTest
NereusPersistentTopicCursorRecoveryTest
NereusPersistentSubscriptionAckTest
NereusConsumerAckOrderingTest
NereusAdminOperationTest
```

Required broker cases：

- durable/non-durable Exclusive/Failover/Shared admission；
- Key_Shared、transaction、replicated、read-compacted and delayed paths fail before mutation；
- cumulative/individual/partial batch and `requirePersistedAck` matrix；
- cursor failure leaves counters/redelivery/pending state unchanged；
- persisted success applies those effects exactly once；
- TTL/expiration/admin allowlist and F4 operations denylist；`TRIM_TOPIC` is rejected before the facade no-op，while
  periodic BrokerService housekeeping still completes normally；
- analyze-backlog validates the new closed enum member before duplication and scans the copied ack view；
- reset/clear/delete/recreate admin paths validate topic ownership and serialize dispatcher/read admission；a watch
  event alone never authorizes destructive handoff；
- retention/ACTIVE-root owner claim and cursor hydration occur before subscription recreation；
- capability spoof rejection、startup failure、two stable snapshots and downgrade failure；
- typed cursor broker-config mapping uses the seven-argument runtime configuration and the real canonical context；
  neither compatibility constructor can publish cursor capability；
- activation CAS success + retention-root/cursor-create failure/retry，and old-decoder fail-before-empty-cursor proof；

### 7.3 Exit

```text
The broker admits only the F3 subscription/ack/admin surface.
Client-visible ack success follows durable root CAS.
Every persistent broker must advertise cursor protocol 1 before first cursor creation.
BookKeeper-backed topics retain stock behavior.
```

Implemented Nereus composite gate：`phase3M4Check`；the Pulsar fork focused test selection is pinned by exact task
names in `build.gradle.kts` and the source lock requires an exact clean checkout。

### 7.4 Completion Evidence

F3-M4 completed on 2026-07-14 against Pulsar
`master@12edc9381c147ceec8bedd530acb5be7db339707`。The implementation provides：

- independent `nereus.storage-binding-protocol=1` and `nereus.cursor-protocol=1` publication with reserved-key spoof
  rejection；F2 binding readiness remains independent，while cursor readiness/first activation requires both
  capabilities on two consecutive stable persistent-broker snapshots and fails closed on downgrade；
- complete typed `ServiceConfiguration` -> `CursorMetadataStoreConfig` / `CursorStorageConfig` mapping，canonical
  seven-argument `NereusRuntimeConfiguration` construction and canonical six-argument `NereusRuntimeContext` wiring；
- durable/non-durable Exclusive、Failover and Shared admission，with Key_Shared、transactions、replication、
  read-compacted and unsupported F4 paths rejected before mutation；TTL and subscription expiration remain cursor-only；
- exact durable/non-durable cumulative/individual/batch acknowledgement validation and immutable Position inputs；
  durable client-visible completion、redelivery/pending cleanup、timestamps/counters and end-of-topic effects follow
  cursor success，while failure leaves those effects unapplied；
- hydrated durable cursor enumeration before `PersistentTopic` subscription reconstruction；
- closed admin operation validation for analyze-backlog and topic trim before duplicate/scan or facade no-op；
- unchanged stock acknowledgement behavior for BookKeeper-backed topics。

`./gradlew phase3M4Check` passed the exact source lock、M1-M3 chain、development composite publication，the eight
tests listed in section 7.2，and `:pulsar-broker:spotlessJavaCheck` /
`:pulsar-broker-common:spotlessJavaCheck`。All M4 exit conditions above are satisfied。

## 8. F3-M5 — Real Recovery and Retention Gates

M5 uses real Oxia、pinned LocalStack Community S3 `4.14.0` and two real Pulsar broker processes/services。BookKeeper
storage class remains present as a control path。

Required scenarios：

1. create durable Exclusive at Earliest/Latest, publish, cumulative ack, unload/reload；
2. Failover owner switch with delivered-but-unacked messages redelivered under identical MessageIds；
3. Shared disjoint individual ack from two consumers on one claimed owner，then broker failover/restart；exact holes remain；
4. partial batch ack, unload/failover/runtime restart, remaining indexes and MessageIdAdv coordinates stable；
5. ack CAS success + response loss, duplicate ack returns success exactly once；
6. snapshot threshold crossing, root delta, replacement snapshot and restart hydration；
7. missing/corrupt stable snapshot prevents topic open without state advancement；
8. delete/recreate increments generation; old broker/handle mutation rejected；
9. clear backlog concurrent with append leaves post-snapshot append visible；
10. backward reset/create races floor raise across every PROTECTION_PENDING cut and trim pending；no needed offset is
    trimmed，and post-barrier floor raise cannot reopen the old window；
11. crash before/after L0 trim with pending record; restart reissues/completes same offset；
12. TTL expiration and subscription expiration update/delete cursor but perform no unauthorized physical GC；
13. watch disabled: independent runtimes converge through authoritative get/poll；
14. 10,000-cursor/page-limit/count-limit behavior on an isolated scale fixture；
15. BookKeeper and Nereus topics/subscriptions coexist with no cross-class call。
16. delayed old-owner ack/forward-reset/protection CAS at unload/crash is injected before、during and after retention
    plus per-cursor claims；the new broker publishes only a fully claimed/stabilized view，and an old whole-ack cache
    cannot skip entries reopened by a destructive transition。The same gate starts from an unactivated empty topic and
    proves a pre-pending delayed old-owner first-create cannot enter PROTECTION_PENDING after the new owner-only root
    claim；when pending preceded takeover，its target-key winner is recovered/claimed and cannot stale-finalize or
    callback。Ownership-checker false/error at pre-claim、post-claim publication and first-create final callback emits no success。

Exit requires no orphan/reference leak to be mistaken for visible state；physical orphan deletion is not an F3 exit
condition and remains an observable F4 handoff。

Implemented gate：`phase3M5Check`；Docker/real-service gate：`phase3M5FinalCheck`。

### 8.1 Completion Evidence

F3-M5 completed on 2026-07-14 against Pulsar
`master@a2bad4cfa260cc4575ae759f8a345ce969c8ec3a`：

```text
./gradlew phase3M5Check
./gradlew phase3M5FinalCheck --rerun-tasks
```

The ordinary gate preserves the full M1-M4 dependency chain and executes the deterministic tests that prove scenarios
5-11、13 and 16。Those suites cover lost CAS responses and exact-result retries、snapshot threshold/replacement/
missing/corrupt handling、delete/recreate fencing、clear-backlog append races、every protection/trim pending crash cut、
watch-disabled authoritative convergence and delayed old-owner mutations。The M5 scale fixture seeds exactly 10,000
durable roots，reads them in 40 bounded pages（39 x 256 + 16），hydrates every root without call-stack growth，rejects
the 10,001st create without publishing a root，and fails closed if storage already contains 10,001 roots。This gate
found and removed recursive hydration in both `DefaultCursorStorage` and `DefaultCursorRetentionCoordinator`。

`NereusCursorMultiBrokerIntegrationTest` supplies scenarios 1-4、12 and 15 using two real broker services、real Oxia、
pinned LocalStack Community S3 `4.14.0` and a real BookKeeper control path。It proves Earliest/Latest cumulative state
through unload，delivered-unacked Failover redelivery with byte-identical `MessageIdAdv` coordinates，Shared individual
ack holes after owner loss，partial-batch remaining indexes through failover and runtime restart，cursor-only TTL and
inactive-subscription expiry with unchanged object count，and stock BookKeeper subscription behavior。Every invocation
uses fresh topic identities，and the failover phase reconnects directly to the authoritative surviving owner so the
gate measures broker recovery rather than a stale client bootstrap connection。

The same real gate requires the very first topic persistence-policy write to succeed。M5 found that lazy policy-system-
topic creation could recursively request the namespace storage-class lock while the outer policy mutation held it；
`BrokerService.updateTopicPersistence` now initializes that system topic before entering the critical section and
re-reads the proposed state under the lock。It also found that Pulsar Shared dispatch mutates the returned entry list；
Nereus cursor read/replay results now preserve the mutable `List<Entry>` contract instead of returning `List.copyOf`。

The rerun final gate executed 96 Nereus tasks and 138 Pulsar tasks，including real M1/M2 Oxia/S3 recovery prerequisites、
Pulsar spotless/checkstyle and the full two-broker acceptance scenario。No visible cursor state is inferred from an
orphan snapshot；physical orphan deletion remains the explicit F4 handoff。

## 9. F3-M6 — Compatibility and Final Gate

**Result：PASS。** All scenarios 17–26 have production code plus deterministic or real-broker evidence。

Final scenarios extend M5（numbering continues after M5）：

17. save ordinary and middle-batch MessageIds, then unload、owner failover、process restart、seek/reset/history read；
    all forward/reverse coordinates remain identical；
18. ordinary reset normalizes a trimmed/future target to current trim/end；force-reset to the exact trimmed target
    fails before F4 compacted support and cannot resurrect bytes；
19. cursor property internal-prefix behavior across two brokers and restart；
20. root/snapshot limit exhaustion fails ack without silent truncation; `isCursorDataFullyPersistable` remains true for
    admitted states；
21. rolling capability sequence: the preactivation retention owner root does not alter the F2 projection；projection
    activation then precedes the first `CursorStateRecord`；the locked F2 decoder fails open on the marker after
    durable cursor exists without exposing/corrupting it；
22. L0 append committed/unknown/fence regression suites prove cursor work did not alter F1/F2 truth；
23. retained old snapshot/current snapshot/CAS-lost orphan inventory is sufficient for an F4 dry-run scanner to
    classify without deleting bytes；both retention pending lifecycles veto a newer deletion decision；
24. topic delete/recreate gives new F2 incarnation while old cursor roots cannot alias；
25. feature/admin bypass audit covers every loaded/unloaded/namespace route；
26. async cancellation/close/executor rejection produces exactly one callback and no ref-count leak。

Implemented milestone and aggregate gates：

```text
./gradlew phase3M6Check
./gradlew phase3M6FinalCheck --rerun-tasks
./gradlew phase3Check
./gradlew phase3FinalCheck --rerun-tasks
```

`phase3M5PulsarFinalCheck` is pinned to the historical M5 method only；`phase3M6PulsarFinalCheck` independently runs
`NereusCursorMultiBrokerIntegrationTest.preservesMessageIdsPropertiesAndIncarnationAcrossCompatibilityCuts` so M6
cannot pass by accidentally reusing the M5 recovery scenario。The current exact source lock is
`1720bc00a9122b2e89d555891956f38a5f64e3d1`。

The aggregate gate runs with Gradle parallelism enabled，but every nested build that writes the same Pulsar checkout
is explicitly serialized：local Nereus compile/test and Docker gates finish first，then Phase 2 ordinary Pulsar、M4
ordinary Pulsar、Phase 2 real two-broker、M5 real two-broker and M6 real two-broker run in that order。This ordering is
part of the gate contract；a `--rerun-tasks` build must never delete shared Pulsar class outputs while another nested
build or a Nereus test is consuming them。

Code-level evidence mapping：

| Scenario | Executable evidence |
| --- | --- |
| 17, 19, 24, 25 | Pulsar `NereusCursorMultiBrokerIntegrationTest.preservesMessageIdsPropertiesAndIncarnationAcrossCompatibilityCuts`；the same method covers reader/consumer seek, owner/runtime cuts, internal properties, loaded compaction, unloaded shadow policy, namespace clear and same-name recreation |
| 18 | `NereusManagedCursorResetSeekTest.ordinaryResetNormalizesTrimmedAndFutureTargetsWhileForceCannotResurrectBytes` |
| 20 | `CursorStorageLimitExhaustionTest` plus `CursorStorageSnapshotSpillTest`；failed mutations leave the last admitted full truth intact |
| 21 | `CursorStorageOpenTest.activatesProjectionBeforeFirstCursorRootWithoutChangingTheF2PreactivationRecord` and the locked F2 `ProjectionCreateRequest` rejection |
| 22 | `NereusManagedLedgerFacadeTest` committed-response-loss/write-fence suites and the inherited Phase 1/1.5/2 dependency chain |
| 23 | `CursorSnapshotInventory`、`CursorSnapshotKeys`、`CursorSnapshotInventoryTest` and concurrent replacement-orphan coverage in `CursorStorageSnapshotSpillTest` |
| 24 storage-only isolation | `ManagedLedgerCursorProtocolTest.topicRecreationUsesANewCursorNamespaceThatCannotAliasTheOldIncarnation` |
| 25 static completeness | `scripts/check-phase3-pulsar-admin-routes.sh` and `checkPhase3PulsarAdminRoutes` |
| 26 | `NereusManagedCursorCallbackSafetyTest.cancellationCloseAndReadCompletionSurviveCallbackExecutorRejectionWithoutLeaks` and `CallbackPrimitivesTest.executorRejectionDrainsEveryAdmittedTerminalCallbackExactlyOnce` |

`checkPhase3ContractSurface` additionally locks every code-level M1-M4 production/test artifact named by this plan，
both independent real-broker test methods，owner-claim-before-publication、checked `ackStateEpoch` replacement and F4
inventory revalidation entry points。It also fails if a normal read/dispatch position appears in any durable cursor
root、retention root or snapshot codec。`checkPhase3Documentation` validates current status/source lock、the F4 term，
stale milestone language and every local Markdown link in the Phase 3 authority set。

The real scenario exposed two compatibility defects and keeps their regressions in
`NereusManagedLedgerFacadeTest.factoryLedgerAppendReadPropertiesLifecycleAndReopenUseL0Truth` and
`closeTrimReopenTerminateDeleteAndRecreatePreservePositionAndObjectContracts`：a terminal `DELETED` projection is an
empty pre-open property view，while `DELETING` remains fail-closed；successful ledger delete closes/releases the old
factory handle before its callback，so immediate same-name recreation cannot return the deleted stream。

Final gate also reruns：

- Phase 1/1.5 final gates；
- Phase 2 `phase2FinalCheck`；
- root API/dependency/golden-byte checks；
- locked Pulsar source/member/call-path drift checks；
- Pulsar fork selected unit/integration suites；
- documentation status/link/terminology audit。

## 10. Documentation Gate

Every implementation milestone updates：

- this directory's status/evidence/file/method tables；
- `docs/design/nereus-future3-cursor-subscription.md`；
- `docs/design/nereus-futures.md` and design index；
- root/design README and overall architecture status；
- terminology when a durable term changes；
- F4 design if the handoff contract changes。

Future 3 may be described as `Implemented / final-gated` only together with the passing M6 and aggregate gates above。
M1/M2 outputs remain repository/test milestones and must not be published or deployed as broker artifacts by
themselves；the marker-aware decoder、M3 writable cursor runtime and M4 admission form one release boundary。

## 11. Stop Conditions During Implementation

Implementation stops for a new narrow design review if any of these occurs：

- locked Pulsar method/call path changes semantics, not just source formatting；
- one-root record cannot fit mandatory identity/properties/reference within the hard metadata cap；
- protection intent cannot encode every admitted create or configured backward-reset target within its frozen cap；
- an ObjectStore provider cannot guarantee immutable ifAbsent + exact read/head/checksum behavior；
- L0 trim is found non-monotonic or non-idempotent at the same offset；
- broker ack counters cannot be reordered after persistence without a Pulsar-visible compatibility change；
- existing Oxia per-key CAS cannot implement retention-first + all-ACTIVE-root owner-session claim such that every
  old existing-root CAS is observed-before its per-key claim or loses，and every already-pending CREATE/RECREATE
  target race is recovered、claimed and stabilized before new-owner publication；
- F4 needs to remap logical offsets/MessageIds or make projection an append truth owner；
- any test requires silently dropping ack ranges/batch state to pass。

Performance variance、extra redelivery、conservative floor lag or orphan accumulation do not by themselves reopen the
protocol；they are implementation/operations work as long as bounds and correctness remain intact。

## 12. Phase 3 Completion Definition

Future 3 is complete only when：

```text
M1-M6 code and gates are green;
all supported durable subscription modes survive unload/failover/restart;
writable open claims retention + every ACTIVE cursor root before publication and stale owner sessions are fenced;
ack success has an authoritative one-root CAS proof;
MessageId coordinates remain stable;
no durable dispatch read position exists;
no ack truth is truncated;
destructive reset/clear advances ackStateEpoch and monotonic ack never rebases across it;
delete/recreate fences stale generations;
backward reset/create is frozen by recoverable PROTECTION_PENDING and cannot race past trim;
F4 receives explicit logical-trim offset/attempt/reason and snapshot-reference inputs;
docs describe the exact shipped implementation.
```
