# Domain API, Module, and Target Contract

## 1. Module graph

Add one independent production module：

```text
nereus-api
  <- nereus-core
  <- nereus-metadata-oxia
  <- nereus-bookkeeper
       BookKeeper client API 4.18.0
  <- nereus-materialization
  <- nereus-managed-ledger
  <- nereus-pulsar-adapter
```

Exact dependency rules：

```text
nereus-bookkeeper
  implementation(project(":nereus-api"))
  implementation(project(":nereus-core"))
  implementation(project(":nereus-metadata-oxia"))
  implementation(bookkeeper-client-api/server client artifact needed by public implementation)

nereus-core          -X-> nereus-bookkeeper
nereus-api           -X-> BookKeeper classes
nereus-managed-ledger-X-> org.apache.bookkeeper.mledger implementation classes
nereus-bookkeeper    -X-> org.apache.bookkeeper.mledger.*
```

Only `nereus-pulsar-adapter` composes the concrete module. A module-boundary check must scan imports and Gradle
dependencies before BK-M1 is accepted.

## 2. Package and class ownership

Target package：`com.nereusstream.bookkeeper`。

| Type | Responsibility | Must not own |
| --- | --- | --- |
| `BookKeeperWalConfiguration` | validated alias、quorums、digest/password reference、rollover/limits/timeouts | mutable policy lookup、secrets in logs/metadata |
| `BookKeeperLedgerGcConfiguration` | deletion concurrency、clock/drain/audit windows and safe local switches | reference eligibility、activation authority |
| `BookKeeperLedgerIdNamespace` | validate/probe reserved prefix and generate exact positive-63-bit candidates | self-authorizing namespace reservation |
| `BookKeeperLedgerIdNamespaceReservationVerifier` | read exact provisioned reservation/version/digest and fail closed on revoke/drift | creating or renewing operator authority |
| `BookKeeperWalRuntime` | appender/reader/allocator/recovery/retention lifecycle assembly | borrowed client close |
| `BookKeeperPrimaryWalAppender` | prepare exact entries、persist one contiguous range、post-write validation | head CAS、offset allocation、MessageId |
| `BookKeeperPrimaryWalReader` | exact non-recovery open/range read/checksum/provider-neutral result | generation choice、projection truth |
| `BookKeeperLedgerAllocator` | allocation intent/durable slot、reserved exact-id create/reconcile、activate segment | random untracked create、listing correctness |
| `BookKeeperLedgerRecovery` | fence/recovery-open old active ledger、seal/reconcile | ordinary reads |
| `BookKeeperLedgerRetentionManager` | scan sealed candidates、whole-ledger proof/delete convergence | logical trim decision、Object output publication |
| `BookKeeperLedgerHandleCache` | bounded non-authoritative read handles and close | durable ownership/fencing truth |
| `BookKeeperWalReferenceManager` | range protections and reader leases | duplicate task/cursor truth |

No public class named durable `BookKeeperWalLocation` is added. If an internal convenience value exists, it must be
constructed solely from `BookKeeperEntryRangeReadTarget` and may never be serialized.

## 3. Provider-neutral append SPI

### 3.1 Existing values retained

Keep and complete the existing Phase 1.5 interfaces：

```java
interface PrimaryWalAppender<P extends PreparedPrimaryAppend> {
    ReadTargetType targetType();
    P prepare(PrimaryAppendRequest request);
    CompletableFuture<DurablePrimaryAppend> persist(P prepared, Duration timeout);
    CompletableFuture<Void> validateBeforeHeadCommit(
        DurablePrimaryAppend append, AppendSession session, Duration timeout);
}
```

`PrimaryAppendRequest` remains logical/provider-neutral and carries exact entry bytes、assigned logical range、current
session and attempt/deadline facts. Provider implementations may validate stricter bounds but cannot allocate offsets.

### 3.2 New durable append shape

`DurablePrimaryAppend` must carry everything common commit code requires without Object casts：

```java
public record DurablePrimaryAppend(
    ReadTarget target,
    PrimaryPhysicalIdentity physicalIdentity,
    Checksum payloadChecksum,
    int entryCount,
    long logicalBytes,
    ProviderAppendToken providerToken) { }

public sealed interface PrimaryPhysicalIdentity
    permits ObjectPrimaryPhysicalIdentity, BookKeeperPrimaryPhysicalIdentity {
    ReadTargetType targetType();
    byte[] canonicalIdentity();
}

public record BookKeeperPrimaryPhysicalIdentity(
    String clusterAlias,
    long ledgerId,
    long ledgerRootEpoch,
    long firstEntryId,
    int entryCount,
    Checksum rangeChecksum) implements PrimaryPhysicalIdentity { }
```

`ProviderAppendToken` is process-local and opaque. Common commit/recovery code must not serialize or require it；BK
post-write validation can use it when live and reload exact target/root/session facts when absent after restart.

### 3.3 Common coordinator split

Refactor `AppendCoordinator` into explicit provider-neutral cuts：

```text
resolve/admit StorageExecutionPlan
validate AppendSession and assign logical range
registry.requireAppender(plan.primaryTargetType).prepare
appender.persist                                      primary durability
provider reference manager.acquireReachableProtection
metadata.prepareOrReuseStableAppend(ReadTarget, identity, ...)
appender.validateBeforeHeadCommit
session revalidation
stream-head CAS                                      logical visibility
build stable AppendResult
generation-zero publication/repair according to plan
optional required-object-generation barrier
ack
```

Provider code never performs the head CAS. Common code never switches on Object/BK fields except through registered
physical-reference adapters. A failed/timeout provider future is classified before any head attempt；after a reachable
head is observed, recovery returns `KNOWN_COMMITTED` semantics regardless of later completion-barrier failure.

## 4. Physical-reference registry

Object and BK need different protection/root rules. Add a typed registry keyed by exact target identity：

```java
public interface PrimaryPhysicalReferenceAdapter<T extends ReadTarget> {
    ReadTargetReaderKey key();
    Class<T> targetClass();

    CompletionStage<ReachablePrimaryProtection> protectBeforeHead(
        StreamId streamId, OffsetRange range, long commitVersion,
        T target, AppendSession session, Duration timeout);

    CompletionStage<VisibleGenerationProtection> protectGeneration(
        StreamId streamId, GenerationKey generationKey,
        T target, Duration timeout);

    CompletionStage<Void> verifyProtectionOwner(
        ReadTarget target, ProtectionOwner owner, Duration timeout);

    CompletionStage<Void> retireProtection(
        ReadTarget target, ProtectionOwner owner, Duration timeout);
}
```

Names may be adjusted during implementation, but these operations and their ownership boundaries are mandatory.
`DefaultGenerationZeroPhysicalReferencePublisher` becomes a registry dispatcher; it cannot reject a target merely
because it is not `ObjectSliceReadTarget`.

## 5. Provider-neutral read contract

### 5.1 API values

Move generic read results out of `nereus-object-store`：

```java
public record ReadSourceRef(
    OffsetRange resolvedRange,
    long generation,
    long commitVersion,
    ReadTarget target,
    Checksum targetIdentity) { }

public record ReadBatch(
    OffsetRange range,
    PayloadFormat payloadFormat,
    byte[] payload,
    List<SchemaRef> schemaRefs,
    Optional<ProjectionRef> projectionRef,
    ReadSourceRef source) { }

public record PhysicalReadStats(
    Checksum targetIdentity,
    long resolvedPayloadBytes,
    long resolvedAuxiliaryBytes,
    long physicalPayloadBytesRead,
    long physicalAuxiliaryBytesRead,
    long returnedPayloadBytes) { }

public record PhysicalReadResult(
    List<ReadBatch> batches,
    List<PhysicalReadStats> rangeStats) { }
```

Exact names may change only if the same semantics remain. `targetIdentity` is SHA-256 of canonical encoded
`ReadTarget` including codec tag/version. It is not just the target's content checksum.

The current `WalReadResult`/`WalSliceReadStats` become Object adapter compatibility types or are migrated to these
values. `ReadTargetReader` returns the provider-neutral result. Tests must prove an Object read produces identical
logical batches/metrics after migration before BK code is registered.

### 5.2 Why `EntryIndexRef` leaves `ReadBatch`

`EntryIndexRef` describes how an Object reader locates framing/footer bytes. It remains in Object target/format APIs
and durable Object metadata. The generic reader contract promises dense logical batches, so a consumer need not know
how the provider found their boundaries.

BK V1 has implicit boundaries：one ledger entry equals one Nereus entry. It returns one `ReadBatch` per requested
offset. It must not create a zero-length inline index or fake footer merely to satisfy an Object-era constructor.

### 5.3 Read accounting validation

Replace `ResolvedObjectRange::from` validation with：

```text
expected = multiset/map of SHA256(encode(resolved.readTarget)) for resolved ranges
observed stats = exactly one record per complete resolved source range
each batch.source.targetIdentity exists in observed stats
each batch range is a subset of source.resolvedRange
sum(stats.returnedPayloadBytes) == sum(batch.payload.length)
logical batches are dense from requested start and within maxRecords/maxBytes
all arithmetic checked
```

Readers may merge physical IO internally but must still report one exact accounting identity per resolved range.
Compressed Object generations may read fewer physical bytes than returned logical bytes；BK typically reads exact
entry bytes. Neither shape is treated as corruption by sign alone.

### 5.4 Error classification

Core read failure handling becomes provider-neutral：

```java
enum PhysicalReadFailureKind {
    NOT_FOUND,
    CHECKSUM_MISMATCH,
    TRANSIENT_IO,
    FENCED_OR_CLOSED,
    AUTHENTICATION,
    METADATA_INVARIANT,
    UNSUPPORTED_TARGET
}
```

Object/BK adapters map provider exceptions to Nereus error codes/kinds. Generation fallback/quarantine policy uses
the kind and target generation, not a method named `isObjectReadFailure`.

## 6. BookKeeper target contract

The existing API is retained unchanged for V1：

```java
record BookKeeperEntryRangeReadTarget(
    int version,                 // exactly 1
    String clusterAlias,         // durable logical client/config binding
    long ledgerId,               // non-negative exact ledger
    long firstEntryId,           // non-negative
    int entryCount,              // positive
    BookKeeperEntryMapping entryMapping,
    Checksum rangeChecksum)      // SHA-256/NBKR1
```

Additional invariants checked where the logical range is available：

```text
entryCount == OffsetRange.recordCount (and fits int)
lastEntryIdInclusive = firstEntryId + entryCount - 1 without overflow
clusterAlias resolves to the same immutable BookKeeperWalConfiguration binding digest
ledger root streamId == resolved streamId
ledger root lifecycle is readable (ACTIVE/SEALING/SEALED/MARKED; not terminally absent)
target range is within sealed lastEntryId when root is sealed
range checksum matches full exact entry sequence
```

The target codec remains `BookKeeperEntryRangeReadTargetCodecV1`. Golden bytes, max encoded size, malformed UTF-8/
enum/checksum/overflow rejection and decode-reencode equality are mandatory BK-M1 gates.

### 6.1 Entry, ledger, and batch metadata placement

V1 does not create a second per-entry Oxia index：

| Fact | Durable location | Bound |
| --- | --- | --- |
| logical offset range -> physical range | existing generation-zero offset index containing one `BookKeeperEntryRangeReadTarget` per committed append range | at most `maxAppendRangesPerLedger` source ranges per ledger |
| entry boundary/order | BookKeeper entry ids inside the target's consecutive `[firstEntryId, entryCount]` range | at most `maxEntriesPerLedger`；checked arithmetic |
| ledger quorum/closed/LAC/length | BookKeeper `LedgerMetadata` | one provider record per ledger |
| Nereus ledger owner/lifecycle/config | `BookKeeperLedgerRootRecord` + writer/allocation state | one root per exact id, one writer per stream |
| source/reference inventory | fixed `(ledgerRangeSlot, protectionSlot)` rows under the ledger shard | at most checked `maxAppendRangesPerLedger * protectionSlotsPerRange`；invalid/out-of-range rows veto GC |
| reader pins | fixed reader-slot rows under the ledger shard | at most `maxReaderLeasesPerLedger` process occupants；claim before provider IO |
| Pulsar batch metadata/index | inside the exact opaque serialized Pulsar Entry bytes | batch index does not consume an L0 offset and is never copied into BK lifecycle metadata |

F2 decodes batch information only when projecting the returned Entry. Physical BK entry id、ledger rollover and
protection row identity never enter `MessageIdAdv` or become a second compatibility truth.

## 7. BookKeeper appender API behavior

### 7.1 Prepared value

```java
record BookKeeperPreparedPrimaryAppend(
    PrimaryAppendRequest request,
    List<ByteBuf> retainedEntries,
    Checksum rangeChecksum,
    int entryCount,
    long logicalBytes,
    long physicalBytes) implements PreparedPrimaryAppend, AutoCloseable { }
```

Implementation may use another reference-count-safe holder. Requirements：

- preserve exact Pulsar Entry bytes and ordering；
- compute/check exact payload physical bytes independently from logical/cumulative size before BookKeeper IO；
- release each retained buffer exactly once on success/failure/cancel/close；
- never log payload、password or digest secret；
- one provider call owns the monotonic remaining deadline across allocation/write/validation。

### 7.2 Persist behavior

```text
require current active segment or allocate one
if batch would violate entry/byte/age rollover threshold:
    seal current segment
    allocate next segment
reserve contiguous explicit entry ids in writer state CAS
reserve exact ledgerRangeSlot and mandatory protection slots 0..2
writeAsync(firstEntryId + i, entry[i]) sequentially or with bounded pipeline
await every write under one deadline
on full success: return target + exact physical identity
on partial/uncertain result: mark segment tainted; recovery seals/fences it; never reuse tail
```

The exact write pipeline bound is configuration, not metadata truth. V1 defaults to ordered one-at-a-time writes;
bounded parallel explicit ids may be enabled only after a deterministic partial-failure fixture proves identical
taint/recovery behavior.

### 7.3 Post-write validation

Before head CAS, the appender/reference adapter reloads：

- current append session owner/epoch；
- writer-state active segment identity/root epoch；
- exact root ownership/config binding；
- reserved entry range and completed write outcome；
- reachable protection owner identity。

Any drift prevents head publication. Bytes may remain orphaned but are never visible.

## 8. BookKeeper reader API behavior

```text
validate target + logical range + configuration binding
claim durable fixed whole-ledger reader slot
open ledger withRecovery(false)
readUnconfirmedAsync(firstEntryId, lastEntryIdInclusive)
require exactly entryCount entries with exact consecutive ids
recompute NBKR1 range checksum over the complete range
clip to caller logical start/maxRecords/maxBytes
emit one dense ReadBatch per selected entry
emit one PhysicalReadStats for the complete resolved target
close LedgerEntries and release ByteBufs/lease on every path
```

An open handle cache is bounded by count/bytes/idle time and keyed by `(clusterAlias, ledgerId, rootEpoch)`。A cache
hit is an optimization only；restart must reproduce the same read from target + root + configuration.

Normal reads never set `withRecovery(true)` because recovery-open can fence a live writer. `NoSuchLedger` after a
nonterminal protection/root is a metadata invariant/physical-loss error, not an empty range.

## 9. Configuration contract

`BookKeeperWalConfiguration` is immutable and validated before runtime creation：

```java
record BookKeeperWalConfiguration(
    String clusterAlias,
    String providerScopeSha256,
    int ledgerIdPrefixBits,
    long ledgerIdPrefixValue,
    String ledgerIdNamespaceReservationId,
    int ensembleSize,
    int writeQuorumSize,
    int ackQuorumSize,
    DigestType digestType,
    SecretRef passwordRef,
    long maxEntriesPerLedger,
    long maxBytesPerLedger,
    int maxAppendRangesPerLedger,
    int protectionSlotsPerRange,
    int maxReaderLeasesPerLedger,
    int maxUncertainAllocations,
    Duration maxLedgerAge,
    int maxWritesInFlight,
    int maxReadsInFlight,
    long maxReadBytesInFlight,
    Duration operationTimeout,
    Duration allocationTimeout,
    Duration sealTimeout,
    Duration deleteTimeout,
    Duration readerLeaseTtl,
    Duration readerLeaseRenewInterval,
    Duration retentionScanInterval,
    int retentionPageSize) { }

record BookKeeperLedgerGcConfiguration(
    int maxConcurrentDeletes,
    Duration maxClockSkew,
    Duration drainGrace,
    Duration lateCreateAuditGrace,
    boolean enabled,
    boolean dryRun) { }
```

Validation includes `ensemble >= writeQuorum >= ackQuorum > 0`、all bounds positive/non-overflowing、timeouts below
the broker close budget, and no `DEFERRED_SYNC` flag. Prefix bits/value must encode a nonzero subset of the positive
63-bit domain：`ledgerIdPrefixBits` is `[8,24]`、the prefix's highest bit must be one、and the random suffix is therefore
at least 39 bits；every candidate must round-trip the namespace predicate. The
nonblank reservation id names an externally enforced deployment allocation, not a trust-me toggle. The durable
**configuration binding digest** includes alias、ledger-id namespace、quorums、digest type、range/size bounds and a
non-secret password identity/version, never password bytes. A binding change cannot open an existing ledger under a
silently different namespace/digest/password.

`providerScopeSha256` is the lowercase SHA-256 of the canonical, non-secret BookKeeper metadata-service/ledger-root
scope supplied and verified by the Pulsar composition boundary. `clusterAlias` is a durable lookup name, not enough
to distinguish two physical provider scopes；one alias/config binding must resolve to exactly one scope digest.

`maxUncertainAllocations` is `[1,65536]` and defines that many fixed durable allocation-slot identities across 16 shards.
Every provider create must hold one；there is no scan-then-increment counter race.

`protectionSlotsPerRange` is `[4,64]`。Slots `0..2` are reserved for append-owned
`REACHABLE_APPEND`、`VISIBLE_GENERATION` and `APPEND_RECOVERY` transitions；slots
`[3, protectionSlotsPerRange)` are fixed, race-free dynamic owner slots for materialization、repair and replacement
contenders. Checked validation requires
`maxAppendRangesPerLedger * protectionSlotsPerRange <= 65536`。The derived maximum, rather than a scan-then-increment
counter, bounds every ledger protection scan；capacity for an admitted append cannot be stolen before its post-head
generation-zero transition completes.

`readerLeaseRenewInterval < readerLeaseTtl` and `drainGrace >= readerLeaseTtl + configuredClockSkew`。The WAL
configuration digest binds every non-secret semantic/tuning field so all brokers open/recover with one exact plan.
GC `enabled/dryRun` remain local rollout switches and are not authority；the activation/scope digest binds every other
GC field plus WAL page/limit facts, and every delete additionally requires `enabled && !dryRun`.

## 10. Resource, deadline, and close rules

- All public async operations use one caller deadline and pass monotonic remaining time downward；nested full timeout
  resets are forbidden。
- Global/per-stream in-flight entry/byte permits are acquired before retaining payload and are released exactly once。
- Cancellation is best-effort for provider IO but still releases local buffers/permits; an uncertain ledger is tainted。
- Runtime close stops admission, drains/taints active writers, closes Nereus-owned handles/scanners/executors, and
  **does not close** the borrowed BookKeeper client/event loop/OpenTelemetry。
- Close completion waits only to its configured bound. Durable intents/roots/protections make later recovery possible。
- Metrics callbacks and cache eviction cannot alter correctness futures。

## 11. BK-M1 acceptance boundary

BK-M1 is complete only when：

1. Object append/read regression uses the provider-neutral interfaces without semantic change；
2. no common API/core read result requires ObjectId or Object footer identity；
3. generic stable append/protection/gen0 paths accept a typed BK target without casts；
4. the new module compiles against the local Pulsar-pinned BookKeeper 4.18.0 API；
5. BK codec/config/entry mapping/checksum/resource/deadline tests pass；
6. runtime registration remains opt-in and every BookKeeper profile is still pre-IO rejected until BK-M2/M3/M4
   activates its exact capability。
