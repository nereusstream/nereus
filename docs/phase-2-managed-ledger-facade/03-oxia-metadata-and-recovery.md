# Oxia Projection Metadata and Recovery

F2 metadata follows the Phase 1 rule: one key can be conditionally updated; a sequence touching several
keys is a recoverable protocol, not a transaction.

## 1. Metadata Interface

Add a separate interface in `nereus-metadata-oxia`. It receives an already-created L0 snapshot; it does not call
`StreamStorage` or pretend it can validate L0 state by itself:

```java
public record ProjectionMetadataStoreConfig(
        Duration operationTimeout,
        int maxPendingOperations,
        int maxValueBytes) {
}

public interface ManagedLedgerProjectionMetadataStore extends AutoCloseable {
    CompletableFuture<Optional<TopicProjectionRecord>> getProjection(
            String cluster,
            String managedLedgerName);

    CompletableFuture<TopicProjectionRecord> createFirstProjection(
            String cluster,
            ProjectionCreateRequest request);

    CompletableFuture<TopicProjectionRecord> recreateDeletedProjection(
            String cluster,
            ManagedLedgerProjectionIdentity expectedDeletedIdentity,
            long expectedTopicMetadataVersion,
            ProjectionCreateRequest request);

    CompletableFuture<TopicProjectionRecord> updateProperties(
            String cluster,
            String managedLedgerName,
            ManagedLedgerProjectionIdentity expectedIdentity,
            long expectedVersion,
            Map<String, String> properties);

    CompletableFuture<TopicProjectionRecord> mirrorFacadeState(
            String cluster,
            String managedLedgerName,
            ManagedLedgerProjectionIdentity expectedIdentity,
            long expectedVersion,
            ManagedLedgerFacadeState state);

    CompletableFuture<ProjectionRepairResult> repairProjectionIndexes(
            String cluster,
            TopicProjectionRecord authoritative);
}

public record ProjectionCreateRequest(
        String managedLedgerName,
        long storageClassBindingGeneration,
        long incarnation,
        StreamMetadata emptyStream,
        Map<String, String> initialProperties) {
}

public record ManagedLedgerProjectionIdentity(
        long storageClassBindingGeneration,
        long incarnation,
        String streamId,
        long virtualLedgerId) {
}

public enum ManagedLedgerFacadeState {
    OPEN,
    SEALED,
    DELETING,
    DELETED
}

public enum ProjectionRepairStatus {
    ALREADY_VALID,
    CREATED
}

public record ProjectionRepairResult(
        ProjectionRepairStatus virtualLedger,
        ProjectionRepairStatus positionIndex) {
}
```

Both `FakeManagedLedgerProjectionMetadataStore` and the Oxia Java implementation implement this contract. No Pulsar
class appears in this interface or its records. `ProjectionCreateRequest` validates that the supplied snapshot has the
expected deterministic stream name/ID, canonical object profile, `ACTIVE` state, zero committed/trim offsets and zero
cumulative size before the store allocates a ledger ID. The request's positive binding generation comes from the
acquired Nereus creation permit and is never allocated by the Oxia projection store. It also requires the immutable
L0 attribute map to equal exactly `{nereus.payloadMapping=PULSAR_ENTRY_V1}`; callers set that map in
`StreamCreateOptions` on first create/recreate. Unknown extra attributes block adoption rather than allowing another
protocol's empty stream to be silently reclassified.

`TopicProjectionRecord.createdAtMillis` is copied from `emptyStream.createdAtMillis()`; the caller cannot inject a
second creation clock.

`ManagedLedgerProjectionMetadataStore.close()` closes only store-local watches/caches and is idempotent; it never
closes the shared Oxia client or executor runtime.

`ManagedLedgerProjectionIdentity` is deliberately not named `ProjectionIdentity`：Phase 1.5 already owns
`com.nereusstream.metadata.oxia.ProjectionIdentity` for L0 `ProjectionRef` decoding。Reintroducing that simple name
would be a same-package compile failure。All identity fields are immutable and validated as positive/nonblank；the
record has no Oxia version because identity and conditional-update version are separate concepts。

`ProjectionMetadataStoreConfig` requires a positive timeout、positive pending-operation bound and
`maxValueBytes == 64 * 1024` in F2。Each public call creates one monotonic deadline；every get/CAS retry and repair
sub-step consumes the same deadline rather than resetting it。Operation admission is bounded before submitting to the
adapter executor；rejection is retriable `BACKPRESSURE_REJECTED` and performs no Oxia call。

## 2. Keyspace

All raw components use the shared Phase 1 `KeyComponentCodec` or deterministic SHA-256/base32 helper.

```text
/nereus/clusters/{cluster}/facade/managed-ledger/ledger-id-allocator
/nereus/clusters/{cluster}/facade/managed-ledger/topics/{managedLedgerNameHash}
/nereus/clusters/{cluster}/streams/{streamId}/facade/managed-ledger/virtual-ledger
/nereus/clusters/{cluster}/streams/{streamId}/facade/managed-ledger/position-index
```

The topic hash is calculated from the exact managed-ledger name bytes with a domain separator:

```text
sha256-base32("pulsar-managed-ledger-name-v1\0" + managedLedgerName)
```

The L0 stream name is independently domain-separated and includes the decimal incarnation:

```text
"pulsar-ml-v1\0" + managedLedgerName + "\0" + Long.toString(incarnation)
```

Hash keys never replace collision validation: every decoded topic record contains the original exact
name, which must match the lookup request.

Target key builder API is fixed：

```java
public final class ManagedLedgerProjectionKeyspace {
    public ManagedLedgerProjectionKeyspace(String cluster);

    public String ledgerIdAllocatorKey();
    public PartitionKey ledgerIdAllocatorPartitionKey();
    public String topicProjectionKey(String managedLedgerName);
    public PartitionKey topicProjectionPartitionKey(String managedLedgerName);
    public String virtualLedgerProjectionKey(StreamId streamId);
    public String positionIndexKey(StreamId streamId);
    public PartitionKey streamPartitionKey(StreamId streamId);
    public static String managedLedgerNameHash(String managedLedgerName);
}
```

It delegates name/hash validation to the same-module `com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames`
and cluster/component encoding to the existing Nereus helpers。The managed-ledger module imports this helper；the
metadata module never imports `nereus-managed-ledger`。No caller assembles a path or partition key by string
concatenation。

The allocator uses its own stable partition key. The stable topic key always uses
`PartitionKey("managed-ledger-topic/" + managedLedgerNameHash)` so same-name recreation never changes the partition
key for an existing Oxia key. Virtual-ledger and position-index keys use the corresponding stream partition key.
Co-location is useful but does not grant multi-key atomicity.

## 3. Records

### 3.1 LedgerIdAllocatorRecord

```java
public record LedgerIdAllocatorRecord(
        long nextLedgerId,
        long allocations,
        long metadataVersion) {
}
```

Initial `nextLedgerId` is `1L << 62`. Allocation returns the current value and CAS-advances it by
one. `Long.MAX_VALUE` is reserved and exhaustion is a terminal configuration/capacity error. CAS
conflicts retry with bounded exponential backoff and the operation deadline.

IDs may be leaked by crashes or create races. They are never reused. A gap is not corruption.

### 3.2 TopicProjectionRecord — authority

```java
public record TopicProjectionRecord(
        String managedLedgerName,
        String managedLedgerNameHash,
        long storageClassBindingGeneration,
        long incarnation,
        String streamName,
        String streamId,
        String storageClass,
        String storageProfile,
        long virtualLedgerId,
        int positionMappingVersion,
        String payloadMapping,
        String facadeState,
        Map<String, String> properties,
        long createdAtMillis,
        long stateVersion,
        long metadataVersion) {
}
```

`facadeState` is stored as the exact enum name so the existing deterministic record codec needs no enum extension；
the adapter converts with `ManagedLedgerFacadeState.valueOf` and rejects unknown strings。Allowed values in F2:

- `storageClass == "nereus"`;
- `storageProfile == "OBJECT_WAL_SYNC_OBJECT"`;
- `storageClassBindingGeneration >= 1` and equals the acquired fork binding permit；
- `incarnation >= 1` and `streamName` equals the exact domain-separated value for that incarnation；
- `positionMappingVersion == 1`;
- `payloadMapping.equals("PULSAR_ENTRY_V1")` and matches the immutable L0 stream attribute；
- allowed transitions are `OPEN -> SEALED`, `OPEN -> DELETING`, `SEALED -> DELETING` and
  `DELETING -> DELETED`, plus idempotent same-state updates. Direct delete does not fabricate a `SEALED` state.

This record is authoritative for the current managed-ledger name -> incarnation/stream/virtual-ledger mapping. Within
one incarnation, binding generation, stream ID/name, ledger ID, storage class/profile, position/payload mapping and
creation time are immutable.
Properties use single-key versioned CAS.
Initial and replacement maps are normalized with
`MetadataCanonicalizer.canonicalStringMap(..., ApiLimits.MAX_STREAM_ATTRIBUTES_ENCODED_BYTES,
"managedLedgerProperties")`; null is normalized to the immutable empty map before the metadata call. The encoded
size limit is checked before allocator/topic writes, so an oversized property update cannot partially mutate state.
Keys beginning with `nereus.` and the exact Pulsar key `PULSAR.SHADOW_SOURCE` are reserved and rejected on initial
create and every update；otherwise a property mutation could enable a shadow/protocol behavior after open without
passing F2 admission。

Every mutable-topic CAS compares `ManagedLedgerProjectionIdentity` as well as the Oxia version. A stale facade receives
`ManagedLedgerProjectionIdentityMismatchException` containing expected/actual binding generation, incarnation, stream ID and
virtual ledger ID, and must close/fence locally; it never reloads a newer incarnation and
retries its old property or lifecycle mutation against that new topic lifetime.

The `state` field is a monotonic lifecycle mirror, not a second lifecycle authority. L0 stream state wins whenever the
two disagree. Same-name recreation is the only operation that replaces the projection identity: after the old record
and L0 stream are `DELETED`, one CAS publishes `incarnation + 1`, a new stream and a new ledger ID with state `OPEN`.
`stateVersion` resets to zero for the new incarnation and otherwise increments on mirror transitions.

### 3.3 VirtualLedgerProjectionRecord — derived

```java
public record VirtualLedgerProjectionRecord(
        String managedLedgerName,
        String managedLedgerNameHash,
        ManagedLedgerProjectionIdentity identity,
        long startOffset,
        int positionMappingVersion,
        long metadataVersion) {
}
```

For v1, `startOffset=0`. There is no persisted end offset or entry count; those values come from the
current L0 stream head. A stale mutable end in projection metadata would create a second visibility
truth and is forbidden.

### 3.4 PositionIndexRecord — derived formula marker

```java
public record PositionIndexRecord(
        String managedLedgerName,
        String managedLedgerNameHash,
        ManagedLedgerProjectionIdentity identity,
        int positionMappingVersion,
        String formula,
        long metadataVersion) {
}
```

For v1, `formula == "ENTRY_ID_EQUALS_STREAM_OFFSET"`. This is one record per stream, not one record
per Position. Entry boundaries remain in the Object WAL entry index and L0 offset index.

Both derived records carry the exact name plus its domain hash and the complete immutable identity。A cryptographic
hash or stream-key location never substitutes for exact-name/binding/incarnation validation。Their identity fields
must exactly equal the authoritative topic record；property/state metadata-version changes do not invalidate them。

## 4. Open, First Create and Recreation Protocols

Open always reads the topic projection before deciding whether L0 creation is legal.

### 4.1 Existing projection

1. Read the topic key by exact name hash; validate the stored exact name and binding generation against the permit
   acquired for this open.
2. For every state including `DELETED`, call `StreamStorage.getStreamMetadata(record.streamId)`; never call
   `createOrGetStream` for that published incarnation.
3. Missing current L0 head is `METADATA_INVARIANT_VIOLATION`; it must never be recreated as an empty stream.
4. Validate stream name/ID/profile/payload mapping/incarnation and reconcile the lifecycle mirror forward from L0.
5. If L0 is `ACTIVE`, repair/validate the derived keys and return a writable facade. If it is `SEALED`, return a
   terminated/readable facade. If it is `DELETING`, resume `StreamStorage.delete` to `DELETED` before continuing.
6. For `DELETED`, return not-found when `createIfMissing=false`; otherwise enter the recreation protocol in 4.3.
   A deleted incarnation is never returned as a live facade.

### 4.2 First create

When the topic key is missing and `ManagedLedgerConfig.createIfMissing` is true:

1. Acquire the fork-supplied Nereus creation permit for the exact persistence name. It captures a positive
   `storageClassBindingGeneration` in `CLAIMED`/repairable Nereus state and proves no live BookKeeper binding/data;
   then set Nereus projection `incarnation=1` and compute the domain-separated stream identity.
2. Call L0 `createOrGetStream`. If the returned snapshot is not the canonical empty `ACTIVE` object-profile
   candidate, re-read the topic key before classifying it: restart normal existing-projection open when a concurrent
   winner has published that stream or a later incarnation; fail invariant only when the topic is still missing.
   This check never adopts committed or terminal candidate state without matching topic authority.
3. Read both derived keys for that stream. If either is present, re-read the topic key: restart from a newly published
   authority, or fail invariant only when the topic is still missing. A stale initial read must not misclassify a
   concurrent creator's valid derived records.
4. Allocate one ledger ID by bounded allocator CAS.
5. Call `permit.validateBeforeProjectionPublish()`, build the complete topic candidate with the permit generation and
   `putIfAbsent(topicKey,candidate)`. A guard
   rejection publishes nothing and leaves only an empty candidate stream/leaked allocation for audit.
6. If another creator wins, restart open from the topic read. If the current record is still incarnation 1, validate
   every identity field; if it has already advanced through delete/recreate, process that newer record normally.
   The losing ID is a gap and the loser never overwrites the winner.
7. Using only the authoritative record selected after step 5/6, put-if-absent and compare its virtual-ledger derived
   record; never write a losing candidate's stream-derived key.
8. Put-if-absent and compare that same authority's formula record.
9. Return only after both derived records are present and valid.

A crash after L0 create or allocator CAS but before the topic put leaves an empty deterministic stream and/or leaked
ID. The next first-create attempt reuses the empty stream and may allocate another ID. A crash after the topic put
leaves an authoritative projection whose derived keys are repairable.

### 4.3 Same-name recreate

When the current record and L0 stream are `DELETED` and create-if-missing is true:

1. Read and retain the deleted topic record's complete `ManagedLedgerProjectionIdentity` and Oxia
   `metadataVersion`；both are passed to `recreateDeletedProjection`。
2. Acquire a fresh Nereus creation permit, require its binding generation to be greater than the deleted record's,
   then compute `nextIncarnation = Math.addExact(old.incarnation, 1)` and its new deterministic stream name/ID.
3. Create/get the new L0 stream. If it is no longer the canonical empty `ACTIVE` candidate, re-read the topic key:
   restart normal open when a concurrent winner published this or a later incarnation; fail invariant only when the
   same deleted topic version is still authoritative. Never publish over committed/terminal candidate state merely
   because its deterministic ID was expected.
4. Allocate a new virtual ledger ID.
5. Call `permit.validateBeforeProjectionPublish()`, then CAS-replace the topic key with its binding generation only if
   the deleted record version still matches.
6. On a lost race, restart open from the topic read. Validate an equal-incarnation winner exactly; process a valid
   later incarnation normally; never publish the losing ID or overwrite a later lifecycle.
7. Repair only the authoritative winner's stream-derived keys. Old and losing-candidate stream-derived keys remain
   absent or unchanged for later audit/GC and are never published as current authority.

Crash before the topic CAS can leave an empty orphan incarnation stream. Crash after CAS leaves the new mapping
authoritative and repairable. Old MessageIds carry a different virtual ledger ID and cannot address the new stream.

Derived records reference the complete immutable `identity`, not the topic key's Oxia `metadataVersion`。Property/state
mirror CAS updates therefore do not make a valid position projection look stale。

## 5. Open and Repair

The facade-level open orchestrator performs:

```text
acquire Nereus storage-class binding permit
  -> get TopicProjectionRecord
  -> existing: get exact L0 stream only
  -> missing: create first empty L0 stream if allowed
  -> deleted: create next-incarnation empty L0 stream if allowed
  -> create/CAS authoritative TopicProjectionRecord when needed
  -> validate L0 StreamId/profile/name
  -> repair/validate derived projection records
  -> read one L0 StreamMetadata snapshot
  -> construct local facade
```

Missing derived records are recreated from the topic authority. A conflicting derived record is
`METADATA_INVARIANT_VIOLATION`; it is never overwritten. Watch notifications only invalidate caches.
Every cache miss/read can reconstruct from authority.

The repair API returns whether each record was already valid or created. It is bounded to these two
known keys and has no unbounded range scan.

## 6. Append Interaction

Append does not write F2 projection metadata. After open repair has completed, mapping v1 is stable and
`entryId` is the L0 allocated start offset. This avoids a dual-commit protocol.

Append success still requires the Phase 1 L0 success boundary. Projection metadata is not written. If L0 completes
with `KNOWN_COMMITTED` or `MAY_HAVE_COMMITTED`, the exception must carry `AppendAttemptId` and the facade calls
`StreamStorage.recoverAppend` for that same physical attempt. Successful recovery supplies the exact result/Position.
Normal and recovered P15-M6 results also carry cumulative logical size at that commit，so facade snapshot advancement
never requires a projection write or a post-commit metadata reread。
Recovery that proves `KNOWN_NOT_COMMITTED` fails the pending callback, releases the exact attempt and may reopen the
local write lane without inventing a Position. Retryable uncertainty fails the callback once and write-fences the
facade while background recovery continues; a permanent invariant keeps that facade fenced.

## 7. Properties and State CAS

Topic properties live in the authoritative topic record. Updates:

1. read the current record;
2. validate it exactly equals the caller's `ManagedLedgerProjectionIdentity`;
3. apply a bounded canonical map update;
4. CAS the same key by `metadataVersion`;
5. retry conflicts until the operation deadline only while `ManagedLedgerProjectionIdentity` is unchanged; a newer incarnation fails
   immediately.

Facade state mirroring is similar, but L0 lifecycle is authoritative:

- terminate: L0 `ACTIVE -> SEALED`, then topic `OPEN -> SEALED`;
- delete: L0 `ACTIVE/SEALED -> DELETING -> DELETED`, then advance the topic mirror from `OPEN` or `SEALED`
  through `DELETING` to `DELETED` with bounded single-key CAS operations.

The facade never writes a mirror transition before observing the corresponding or later L0 state. If L0 is already
`DELETED` while the topic mirror is `OPEN`, reconciliation first CASes `OPEN -> DELETING` and then
`DELETING -> DELETED`; it does not invent `SEALED` and does not require a multi-key transaction.

If L0 succeeds and the topic state write is lost, the facade lifecycle operation still succeeds and schedules bounded
mirror repair. Open/exists/delete always re-read L0 and reconcile forward; they never reopen a sealed/deleted stream
because the topic mirror lagged. Reporting failure after the irreversible L0 transition would incorrectly invite a
caller to assume rollback.

Physical object deletion remains outside F2. Logical delete makes the current incarnation's open/append/read behavior
terminal while retaining bytes for later production GC; an authorized create-if-missing can publish a new incarnation.

## 8. Codec Contract

F2 records use the same envelope/version/CRC conventions as Phase 1. Each record has:

- a distinct stable record type ID;
- format major/minor version;
- deterministic field order and canonical map ordering;
- unknown-major rejection;
- trailing-byte rejection;
- metadata version hydrated from Oxia, not trusted from encoded bytes;
- durable golden bytes in tests.

The exact F2 registry is：

```text
LedgerIdAllocatorRecord             recordType=LedgerIdAllocatorRecord
TopicProjectionRecord               recordType=TopicProjectionRecord
VirtualLedgerProjectionRecord       recordType=VirtualLedgerProjectionRecord
PositionIndexRecord                 recordType=PositionIndexRecord
schemaVersion=1
minReaderSchemaVersion=1
payloadEncoding=binary-v1
envelopeMagic=NRM1
```

Record component order shown in section 3 is the durable field order；maps use the existing strict-UTF-8 key sort。
Every value, including envelope overhead, must be `<= ProjectionMetadataStoreConfig.maxValueBytes` before an Oxia
write。All write candidates carry `metadataVersion=0` into the encoder；every decode discards that encoded zero and
hydrates the Oxia `VersionedValue.version()`。A nonzero encoded metadata version, unknown `facadeState`, invalid nested
identity, duplicate map key, malformed UTF-8, CRC failure or trailing byte is corruption。

The Phase 1 `Phase1MetadataCodecs` registry and generic record codec are private/static。Phase 1.5 P15-M2 introduces
the package-level `MetadataRecordCodecFactory` while preserving every Phase 1 method/type/schema/golden byte。F2-M2
adds `F2MetadataCodecs` as the third explicit dispatch family after Phase 1 and L0-target registries；it must not probe
decoders, perform a second envelope refactor or change an L0 type。`Phase1MetadataCodecs.recordCodec` is reused for
these primitive/map/nested-record shapes；`facadeState` remains a String specifically because that codec has no enum
wire type。The real adapter adds explicit hydration branches for all four records and the fake uses the same codec
factory, not object serialization。

The current Phase 1 `OxiaJavaClientMetadataStore` owns its `SyncOxiaClient`, `PartitionedOxiaClient` and executors, so
M2 must perform an explicit ownership-preserving refactor:

Target surface below omits method bodies:

```text
public final class SharedOxiaClientRuntime implements AutoCloseable {
    public static SharedOxiaClientRuntime connect(OxiaClientConfiguration config, Clock clock);
    PartitionedOxiaClient client();
    Executor clientExecutor();
    Executor watchExecutor();
    void close();
}

public final class OxiaJavaClientMetadataStore implements OxiaMetadataStore {
    public static OxiaJavaClientMetadataStore connect(
            OxiaClientConfiguration config, Clock clock); // creates/owns a shared runtime

    public static OxiaJavaClientMetadataStore usingSharedRuntime(
            OxiaClientConfiguration config,
            SharedOxiaClientRuntime runtime,
            Clock clock); // does not own runtime
}

public final class OxiaJavaManagedLedgerProjectionMetadataStore
        implements ManagedLedgerProjectionMetadataStore {
    public static OxiaJavaManagedLedgerProjectionMetadataStore usingSharedRuntime(
            OxiaClientConfiguration clientConfig,
            SharedOxiaClientRuntime runtime,
            ProjectionMetadataStoreConfig storeConfig,
            Clock clock);
}
```

`ManagedLedgerProjectionMetadataStore` has an equivalent public `usingSharedRuntime` factory. The shared
runtime owns one Oxia client plus client/watch executors. Each metadata adapter retains its own bounded operation
executor and watch-registration list so projection work cannot consume L0's operation queue. Adapter close stops its
own admission/executor/watches; it closes the shared runtime only when created through the legacy owning `connect`
path. F2 constructs both adapters with `usingSharedRuntime`; `NereusManagedLedgerRuntime` is the sole owner and closes
adapters first and the shared runtime once. Existing Phase 1 public construction/close behavior and tests remain
unchanged.

## 9. Required Contract Tests

Run every case against fake and real Oxia:

- concurrent first open chooses one authoritative ledger ID;
- an existing BookKeeper ledger or a guard failure immediately before topic put prevents Nereus publication；
- an existing projection whose binding generation differs from the acquired permit is rejected before L0 IO；
- a concurrent first-open winner may publish and append before the loser validates the candidate; the loser re-reads
  topic authority and opens the winner instead of reporting corruption；
- existing projection with a missing L0 head fails without calling create-or-get；
- `createIfMissing=false` performs no writes；
- concurrent same-name recreation chooses one next incarnation/new stream/new ledger ID；
- recreation after another storage-class lifetime uses a newer binding generation while keeping the Nereus
  incarnation monotonic from its own deleted projection；
- a recreation winner may publish and append before a losing creator checks candidate emptiness; the loser follows
  the new authority and never overwrites or rejects it；
- `DELETING` open resumes the L0 terminal transition and never returns a live old-incarnation facade；
- stale old-incarnation Position cannot resolve against the winner；
- stale old-incarnation property/state CAS cannot mutate the recreated topic；
- losing allocations become gaps and are never reused;
- crash after L0 create, allocator CAS, topic put/CAS and each derived put;
- missing derived records repair idempotently;
- missing topic authority with derived records or committed data fails without allocating a new ID;
- conflicting topic hash/name, stream ID, profile, payload mapping, ledger ID or formula fails invariant;
- property/state CAS does not require rewriting derived projection records;
- property CAS conflict preserves both retry semantics and canonical limits;
- terminate/delete mirror failure returns the L0 terminal result and reconciles forward；
- watch loss/reconnect does not affect correctness;
- codec golden bytes and corruption/unknown-version rejection;
- no operation requires multi-key conditional commit;
- no append performs a projection metadata write.
- the four F2 record types dispatch without changing any Phase 1/1.5 golden bytes；encoded metadata version is zero
  and decoded version is hydrated from the backend；
- exact-name/hash collision checks cover both derived records, and a same stream/ledger tuple with another binding
  generation or name is corruption rather than repair；
- every CAS retry shares one deadline and pending-operation rejection performs zero backend calls；
- the existing Phase 1.5 `ProjectionIdentity` class remains loadable and unmodified while
  `ManagedLedgerProjectionIdentity` compiles in the same package。
