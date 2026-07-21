# Current Contract and Local Source Audit

## 1. Purpose and status

This is the read-only BK-M0 audit. It answers whether the final-gated Phase 1.5/F2/F3/F4 contracts can accept a real
BookKeeper primary WAL without changing their logical compatibility model, and identifies every Object-specific seam
that must be generalized first.

Audit-time conclusion：**the compatibility model is reusable, but the audited runtime was not BookKeeper-ready**。The
tagged target、stable-head commit、generic recovery、F2 logical projection and F4 generation/task protocols point in
the right direction. At that lock, production append/read/protection/accounting paths still contained Object-specific
types and all three BookKeeper profiles had to remain rejected before IO until BK-M1/M2 closed them.

No production code is changed by BK-M0.

Current implementation note：BK-M1–M4 have since closed the provider-neutral seams、BK writer/reader/lifecycle/
retention、async F4 source path and required-higher-generation producer barrier through the module-local runtime。
The audit hashes below remain the frozen M0 input；BK-M5 production broker wiring and BK-M6 aggregate evidence remain
open。

## 2. Source locks

### 2.1 Nereus

Audit baseline：clean `main@35c58c575c3da220633c53e48a581f16756ea047`。

| Source | Git blob | Audit result |
| --- | --- | --- |
| `nereus-api/.../target/ReadTarget.java` | `37d2c3c31fe60d5ee044f19743ada68258501508` | sealed provider-neutral target union exists |
| `nereus-api/.../target/BookKeeperEntryRangeReadTarget.java` | `49df4be558caacc54e9c72231091f3e2f9192d0c` | V1 durable BK target is reserved and validated |
| `nereus-api/.../target/BookKeeperEntryMapping.java` | `bf608c0da2eec8de4580be0970789ad390ecfe38` | only one-entry-per-Nereus-entry mapping is admitted |
| `nereus-api/.../ReadBatch.java` | `b481a32070384bf3ed7e54729f1d2f4afec65add` | incorrectly requires ObjectId/object offsets and entry-index ref |
| `nereus-core/.../wal/PrimaryWalAppender.java` | `d4084fbc0bfbc7d8f327e5a5c147d673f453549c` | generic prepare/persist/revalidate SPI exists |
| `nereus-core/.../wal/PrimaryWalReader.java` | `245e6d6978e10d338d9d694c9266beeb9db1cd18` | reader marker exists |
| `nereus-core/.../wal/PrimaryWalRegistry.java` | `49de6c3b88adfd7854738a0119cdf7f653eff9d7` | registry exists and BookKeeper remains unregistered |
| `nereus-core/.../append/AppendCoordinator.java` | `eecfd8eadfa1acb0cf2999a6092e60828099b0b1` | production coordinator is still Object-WAL concrete |
| `nereus-core/.../read/ReadCoordinator.java` | `b1fd30d6c5ba4114046eca8013e77ce640045f78` | accounting converts every target to `ResolvedObjectRange` |
| `nereus-metadata-oxia/.../PreparedStableAppend.java` | `77c28e5c3f407c8650fcff08cf8ab4c0ced476d6` | stable append metadata preparation forces Object target/hash |
| `nereus-materialization/.../SourceGeneration.java` | `bde37ed08d225a8820dcdf4c7b4f7eff3e155ac0` | already carries encoded generic target + identity digest |
| `nereus-materialization/.../MaterializationTask.java` | `6e509e7d8194497c904f6ba23545799d2003fa47` | reusable exact-source task; no BK-specific task is needed |

All shortened paths are below `src/main/java/com/nereusstream/...`。

### 2.2 Local Pulsar and BookKeeper

Local checkout：`/Users/liusinan/apps/ideaproject/nereusstream/pulsar`，clean
`master@a8eef5eb3906b6005006627506b3516ff2349fa7`。The original BK-M0 audit input was
`eaf7b9a704890a9265c21f30d9f351e02d00c600`；the current commits add the audited borrowed-client handoff、typed BK
configuration、exact profile first-create capability barrier and independent live BK deletion-readiness snapshot
without changing the pinned BookKeeper version。The checkout pins
`org.apache.bookkeeper:*` to **4.18.0** in `gradle/libs.versions.toml`; it is local master, not an `M1-SNAPSHOT`.

| Source | Git blob | Audit result |
| --- | --- | --- |
| `pulsar-broker/.../ManagedLedgerClientFactory.java` | `3dece00e89a7f0d2f72bff71eabe9d2dff519d37` | owns stock BK client lifecycle |
| `pulsar-broker/.../storage/BookkeeperManagedLedgerStorageClass.java` | `1f05cde72a5b52c2e868abcd38a8e3cabf09a403` | exposes concrete borrowed `BookKeeper` client |
| `pulsar-broker/.../storage/nereus/NereusManagedLedgerStorage.java` | `0e4b37de74eb49e65be9b2e1157194502b3a9393` | fail-closes on a non-BK/null stock class，passes the exact borrowed client plus live BK readiness provider and installs the verified BK capability sink |
| `pulsar-broker/.../storage/nereus/NereusBookKeeperPrimaryWalCapability.java` | `3642d44786341be191c58c80abcb64700ac49882` | freezes reserved config/namespace/stable-publication/sync properties and profile requirements |
| `pulsar-broker/.../storage/nereus/NereusBrokerCapabilityCoordinator.java` | `4dacbfd0dd1ffc83941d80fdd7d39ec259a16f67` | computes independent two-snapshot generation and BK deletion readiness domains and invalidates both on broker-registry drift |
| `pulsar-broker/.../storage/nereus/NereusBookKeeperPrimaryWalCapabilityTest.java` | `127709efc15ee9bb076a76cde0d945b2700dcf90` | freezes stable publication binding、strongest-profile deletion readiness and broker/property drift rejection |
| `pulsar-broker/.../nereus/NereusManagedLedgerStorageBookKeeperClientTest.java` | `c88aaf3848b20b00a2b6694260d0d1f293c098b6` | freezes same-instance handoff and both fail-closed cases |

BookKeeper 4.18.0 public client API locally verified from the pinned jar：

- `org.apache.bookkeeper.client.api.BookKeeper.newCreateLedgerOp()`；
- `CreateBuilder` quorum/password/digest/write-flags/custom-metadata setters；
- `CreateAdvBuilder.withLedgerId(long)` and `execute()` returning `WriteAdvHandle`；
- `WriteAdvHandle.writeAsync(long, ByteBuf)` and `ForceableHandle.force()`；
- `OpenBuilder.withLedgerId(long).withRecovery(boolean)...execute()`；
- `ReadHandle.readAsync`、`readUnconfirmedAsync` and LAC APIs；
- `DeleteBuilder.withLedgerId(long)` only (no expected metadata/version condition)、`getLedgerMetadata(long)` and
  bounded/listing APIs；
- `LedgerMetadata` closed/last-entry/length/quorum/digest/custom-metadata facts；
- `WriteFlag.DEFERRED_SYNC` and typed `BKException` return codes。

The design uses only those public APIs. It does not depend on internal `LedgerHandle` fields or stock ManagedLedger
implementation classes.

## 3. Current executable paths

### 3.1 Append is nominally generic but concretely Object-only

Current production flow：

```text
StreamStorage.append
  -> AppendCoordinator(ObjectWalAppenderAdapter field)
  -> prepare and cast ObjectPreparedPrimaryAppend
  -> guarded immutable Object upload / manifest
  -> PreparedStableAppend(ObjectSliceReadTarget + ObjectKeyHash)
  -> reachable-object protection
  -> commit intent put/reuse
  -> stream-head CAS                         logical visibility
  -> generation-zero Object index
```

The generic `PrimaryWalAppender<P>` and registry are real Phase 1.5 foundations, but `AppendCoordinator` does not
select an appender from the execution plan and cannot construct a generic stable append. BK-M1 must separate provider
durability from common intent/protection/head/gen0 stages; adding only a BK appender to the registry would leave the
production path unreachable.

### 3.2 Stable metadata and protection still encode Object identities

The following classes reject or require exact Object target facts：

- `PreparedStableAppend`；
- `DefaultGenerationZeroPhysicalReferencePublisher`；
- `ProtectedStableAppend` and `ProtectedGenerationZero`；
- append recovery/reference registration helpers；
- physical-root/protection owners used before head publication。

BK cannot create fake `ObjectId`、`ObjectKeyHash` or Object physical root to pass those checks. BK-M1 introduces a
tagged `PrimaryPhysicalIdentity`/protection owner path while keeping the head/commit protocol common.

### 3.3 Generic read dispatch returns an Object-shaped result

Current flow：

```text
ReadResolver -> List<ResolvedRange(ReadTarget)>
  -> ReadTargetDispatcher / PrimaryWalRegistry
  -> ReadTargetReader.readWithStats(...)
  -> WalReadResult                         package: nereus-object-store
       ReadBatch(sourceObjectId, sourceObjectOffset, sourceObjectLength,
                 EntryIndexRef, ...)
       WalSliceReadStats(objectId, objectOffset, ...)
  -> ReadCoordinator.validateReadAccounting
       every ResolvedRange -> ResolvedObjectRange
```

This is a hard blocker. A correct BK reader has a ledger/range/checksum identity and no ObjectId or object footer.
BK-M1 must move provider-neutral read result/accounting values to API/core and compare the encoded `ReadTarget`
identity, not provider-specific fields.

`EntryIndexRef` also describes inline/Object footer/index-object bytes. It is input metadata for Object readers and
belongs inside Object targets/format handling; it is not a property every returned logical batch must expose. Because
F2 consumes dense logical `ReadBatch` payload/range/schema/projection and does not use the footer identity, the frozen
BK-M1 direction is：

```text
ReadBatch
  = range + payloadFormat + exact payload + schemaRefs + projectionRef
    + ReadSourceRef

ReadSourceRef
  = resolved logical range + generation + commitVersion
    + encoded ReadTarget + SHA-256 target identity

PhysicalReadStats
  = same target identity + provider-neutral payload/index/physical/returned byte counts
```

Object readers retain `EntryIndexRef` internally through `ObjectSliceReadTarget`/
`ParquetCompactedObjectReadTarget`; compatibility constructors may bridge during BK-M1, but BK never synthesizes an
Object entry index.

### 3.4 F4 exact source identity is already reusable

`SourceGeneration` already persists：

- logical range、generation and source commit version；
- generation-index key/version/hash；
- canonical encoded `ReadTarget` and SHA-256 target identity；
- policy digest and source logical facts。

Therefore a committed BK generation can be a normal F4 source. Task identity/reconstruction is based on these durable
facts, never an open ledger handle. `DefaultExactSourceRangeReader` currently contains an Object-only guard and must
dispatch its stored target through the generic reader registry; no BK-specific task record or queue is introduced.

### 3.5 F4 protection, pin and retirement are Object-only in several branches

The generation reader、failure handler、exact-source reader、task-protection reconciler、worker、committer、terminal
retirer and GC reference domains frequently assume an Object physical root. BK-M3 must install exact BK range/ledger
reference adapters and a BookKeeper retention gate; it must not translate a BK range into an Object root.

The reusable truths are：

- generation lifecycle and highest-admitted read selection；
- `MaterializationTask`/claim/recovery/checkpoint；
- worker bounds and deterministic policy digest；
- authoritative lag records/bytes/oldest age and throttle/reject；
- Object output publication、verification、protection and GC；
- source retirement proof once a higher exact generation or logical trim covers the source。

### 3.6 Current durability enum cannot express BK sync completion

Current meanings are：

```text
WAL_DURABLE
  = primary bytes durable + intent/recovery facts + reachable head commit

WAL_DURABLE_AND_INDEX_COMMITTED
  = WAL_DURABLE + generation-zero index confirmed
```

Phase 1.5 froze generation zero as the primary target. For BookKeeper, generation zero is
`BookKeeperEntryRangeReadTarget`, not Object output. `StorageProfile.defaultDurabilityLevel()` currently maps the sync
profile to index committed, and `StorageExecutionPlan` carries only durability + Object publication mode. BK-M1/M4
must add a separate internal `AppendAckBoundary` with `REQUIRED_OBJECT_GENERATION`; renaming generation zero or
overloading the existing enum is forbidden.

### 3.7 Pulsar integration can borrow the stock client

`NereusManagedLedgerStorage` already creates one local `ManagedLedgerClientFactory`, obtains its stock
`bookkeeperClass`, and owns its close. The stock class is a `BookkeeperManagedLedgerStorageClass` and exposes
`getBookKeeperClient()`.

The required wiring is：

```text
ManagedLedgerClientFactory (owner)
  -> BookkeeperManagedLedgerStorageClass.getBookKeeperClient()
  -> NereusRuntimeContext.borrowedBookKeeperClient
  -> DefaultNereusRuntimeProvider
  -> BookKeeperWalRuntime
```

`NereusRuntimeContext` explicitly says its values are borrowed and never closed. The new field follows that rule；
`NereusManagedLedgerStorage.close()` continues to close the stock owner exactly once.

## 4. V1 BookKeeper semantic choices

### 4.1 Ledger ownership

One active writable ledger belongs to exactly one `(streamId, writerSessionEpoch, segmentSequence)`。A ledger never
interleaves two Nereus streams. This is deliberately less space-efficient than shared ledgers but makes fencing、
range inventory、rollover and whole-ledger deletion provable in the first delivery.

### 4.2 Entry mapping

For a resolved generation-zero range：

```text
target.entryCount == resolved.offsetRange.recordCount
entryId(offset) = target.firstEntryId + (offset - resolved.offsetRange.startOffset)
offset(entryId) = resolved.offsetRange.startOffset + (entryId - target.firstEntryId)
```

Both directions use checked arithmetic and reject values outside their exact half-open/inclusive bounds. Mapping is
stable across rollover、restart、ownership transfer and higher-generation selection because F2 MessageId derives from
logical offset, not entry id.

### 4.3 Range checksum

The V1 `rangeChecksum` is SHA-256 over a canonical framed sequence, not concatenation without lengths：

```text
SHA-256(
  "NBKR1" ||
  u32(entryCount) ||
  repeated { u64(entryId) || u32(entryLength) || exact entry bytes }
)
```

Integers are unsigned big-endian fixed width. The writer computes it over retained exact input bytes before/while
writing；the reader recomputes it over the complete target range before returning data. A clipped caller read still
verifies the full committed range, unless a future target version introduces independently checksummed subranges.

### 4.4 Durability and stale writers

V1 uses normal BookKeeper quorum durability and prohibits `DEFERRED_SYNC`。A valid current append session is checked
before physical write and again before head CAS. BookKeeper recovery open fences the previous ledger during ownership
transfer.

No cross-system protocol can prove that an already transmitted stale write never leaves orphan BK entries. The
correct guarantee is narrower and sufficient：a stale session cannot make those bytes reachable through the current
head or receive success. Orphan/tainted entries are reclaimed only after the ledger is sealed and all reachable
references are proven absent.

### 4.5 Exact-id namespace and delete ABA

BookKeeper `DeleteBuilder` deletes by ledger id and offers no conditional “delete only if this custom-metadata/version
still matches” primitive. A metadata read followed by delete is therefore unsafe if an unrelated client may replace
the ledger in between. F1-BK requires an explicit positive-63-bit advanced-ledger-id prefix reserved to one Nereus
deployment and included in broker readiness/config/activation. Stock clients may continue using normal allocation but
must not issue `CreateAdv` inside that prefix. Without this deployment invariant, all BK profiles remain unsupported；
a configuration boolean alone is not deletion authority.

The reservation identity is `(providerScopeSha256, prefixBits, prefixValue)`, not `(clusterAlias, prefix)`。The alias
is a durable lookup name；the scope digest binds the canonical BookKeeper metadata-service/ledger-root identity so two
physical clusters cannot accidentally share roots or deletion authority under the same alias.

Likewise, a transmitted create whose result is unknown has no public provider proof that it can never appear later.
Its exact Oxia root/id and one fixed allocation slot stay consumed and scan-visible. Matching metadata may recover
owned bytes, but it does not prove an older create cannot execute after a later delete；therefore the monotonic
`lateCreateHazard` permanently vetoes automatic deletion in BK-M0–M6. Elapsed grace and repeated absence are audit
signals, not a correctness proof of non-creation.

## 5. Gap-to-milestone ownership

| Blocking gap | Owner/milestone |
| --- | --- |
| Object-concrete append prepare/persist/metadata path | core + metadata BK-M1 |
| Object-shaped `ReadBatch`/stats/accounting | API/core/object-store BK-M1 |
| no BookKeeper module/client adapter | new `nereus-bookkeeper` BK-M1 |
| no ledger allocation/root/lifecycle/protection metadata | metadata + bookkeeper BK-M1/M2 |
| no exact BK read/write/fencing/restart recovery | bookkeeper BK-M1/M2 |
| no whole-ledger retention proof | metadata/bookkeeper/materialization BK-M2 |
| Object-only exact-source/protection/retirement branches | materialization/core BK-M3 |
| no required-higher-generation producer barrier | closed by core/materialization BK-M4；publication activation control plane closed by BK-M5 checkpoint D，deletion proof production/scheduling remains M5 |
| no broker client/capability/profile admission wiring | pulsar-adapter + local Pulsar BK-M5 |
| no real multi-broker/scale/chaos evidence | BK-M5/M6 |

## 6. Audit answer

F1-BK does **not** require changing the F2/F3 compatibility model or F4 generation model. It does require a real
provider-neutral L0 append/read/protection seam before any profile is enabled. Once that seam、ledger lifecycle and
retention proof are implemented as specified here, Future 5/6 and advanced Pulsar work can consume the same stable
logical offsets/MessageIds without knowing which primary WAL stored generation zero.
