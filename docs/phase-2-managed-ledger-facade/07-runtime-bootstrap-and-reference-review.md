# Runtime Bootstrap, Broker Admission and Reference Review

本文补齐 F2 从 facade 类到真实 broker runtime 之间的构造、配置、feature admission 和资源所有权。
它同时记录对 `/Users/liusinan/apps/ideaproject/GITHUB/pulsar-storage` 的源码评审结论。参考项目用于验证
Pulsar 扩展点和 broker call path，不是 Nereus durability/position/cursor 合同的来源。

## 1. Reference Project Findings

Review baseline: tracked source at `pulsar-storage@029f9df1fa7f636b990732d3598fde8aacc5905e`. The checkout also
contained the untracked files `AGENTS.md` and `src/main/java/io/bewaremypower/github/repomix-output.xml`; the former's
repository instructions were followed, while the latter was not treated as an independent source of truth. No
tracked source change in that checkout was part of this review.

The useful evidence from `pulsar-storage` is:

- a custom `ManagedLedgerStorage` can boot a real `PulsarService` without BookKeeper data ledgers；
- `ManagedLedgerFactory -> ManagedLedger -> ManagedCursor` is sufficient to reach producer/consumer paths；
- `PersistentTopic.addComplete` needs the actual persisted `ByteBuf`；
- broker stats and housekeeping touch far more methods than the append/read happy path；
- an end-to-end producer/consumer test is necessary because interface compilation alone misses runtime assumptions。

This review covered `README.md`, `InMemoryStorage`, `InMemoryStorageClass`, `InMemoryManagedLedgerFactory`,
`InMemoryManagedLedger`, `InMemoryCursor`, `PositionImpl`, `InMemorySchemaFactory`, all three dummy MXBeans and
`EndToEndTest` under the reference root.

The following prototype choices are explicitly not adopted:

| Prototype behavior | Nereus decision |
| --- | --- |
| `entryId` advances by `numberOfMessages` | F2 entry ID advances by one persisted Pulsar Entry; batch count is in-entry metadata. |
| `getFirstPosition()` points at the first entry | F2 follows stock one-ledger semantics and returns the position before the first retained entry. |
| in-memory `AtomicLong` ledger IDs | Oxia allocator plus durable projection incarnation. |
| `getManagedLedgerStorageClass` always returns the custom class | Null/`bookkeeper` selects stock; `nereus` selects Nereus; unknown is empty. |
| no durable guard against changing an existing topic's storage implementation | Single-key broker binding claims one class per topic lifetime; live switching is rejected. |
| `getManagedLedgerFactory()` constructs a new factory on every call | One initialized factory instance per storage class. |
| callbacks run inline | Designated ordered/callback executors; no callback under a lock. |
| many TODO methods return zero, empty, sentinel or success | Exhaustive `I/L/N/U/D` method contract in `06`. |
| custom in-memory schema factory | Not adopted; F2 keeps schema-registry/system storage on BookKeeper while ordinary user messages may carry schema references. |
| cursor mark-delete reports success without durable state | Durable mutations fail until F3; local non-durable mutations are labeled local. |
| close/delete/offload are mostly no-op | Local close, L0 seal/delete and explicit offload rejection have separate contracts. |
| retained input buffers live in a process map | Append eagerly copies; Object WAL/Oxia supply durability and restart recovery. |

The prototype's `EndToEndTest` remains a useful smoke-test shape, but its consumer does not prove acknowledgement
recovery, unload/failover, trim, callback races or buffer ownership.

## 2. Fork-owned Hybrid Provider

Target fork classes:

```text
pulsar-broker/src/main/java/org/apache/pulsar/broker/storage/nereus/
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
```

### 2.1 NereusManagedLedgerStorage

The class has a public no-arg constructor because `ManagedLedgerStorage.create(...)` uses reflection. Its fields are
assigned only during `initialize`:

```java
public final class NereusManagedLedgerStorage implements ManagedLedgerStorage {
    private ManagedLedgerClientFactory bookkeeperStorage;
    private NereusManagedLedgerFactory nereusFactory;
    private NereusStorageClassBindingStore bindingStore;
    private ManagedLedgerStorageClass bookkeeperClass;
    private ManagedLedgerStorageClass nereusClass;
    private final AtomicBoolean initialized = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
}
```

Initialization sequence:

1. reject double initialize；
2. validate every Nereus broker setting without opening a topic；
3. construct a stock `ManagedLedgerClientFactory` and call its exact locked `initialize(...)`；
4. capture its one `bookkeeper` storage class, verify it is the default, and construct a binding store over the
   broker-supplied `MetadataStoreExtended` without taking ownership of that shared store；
5. create the Nereus ObjectStore provider, shared Oxia runtime, L0 `StreamStorage`, projection store and facade
   runtime in local variables；
6. construct one `NereusManagedLedgerFactory(runtime, ownsRuntime=true)` and transfer runtime ownership to it, then
   construct one stable `NereusManagedLedgerStorageClass`；
7. publish the immutable `[bookkeeperClass, nereusClass]` list only after every step succeeds。

Partial initialization closes already-created resources in reverse order and rethrows. Nereus initialization failure
is a broker startup failure; the provider never starts in a hidden BookKeeper-only fallback mode.

Lookup contract:

| Input | Result |
| --- | --- |
| null | stock BookKeeper class |
| `bookkeeper` | stock BookKeeper class |
| `nereus` | Nereus class |
| any other string | empty |

`getDefaultStorageClass()` always returns the stock BookKeeper class in F2. `getStorageClasses()` returns an immutable
ordered collection. `NereusManagedLedgerStorageClass.getManagedLedgerFactory()` always returns the same initialized
factory.

Close sequence is Nereus factory first and stock `ManagedLedgerClientFactory` second. Factory shutdown closes its
owned Nereus runtime; the hybrid provider never closes that runtime separately. Before ownership transfer during a
failed initialize, the provider closes the local runtime directly. All close attempts run; the first `IOException`
retains later failures as suppressed exceptions. `InterruptedException` from factory shutdown restores the thread's
interrupt flag and is wrapped as `IOException`; `ManagedLedgerException` is also wrapped. Stock close still runs
after either failure. Repeated close is a no-op.

`NereusStorageClassBindingStore` borrows the broker `MetadataStoreExtended`; it owns no connection and never closes
that store. Its in-memory codec/cache state is discarded only after both factories stop admitting opens/deletes.

## 3. Product-owned Runtime Provider

The Pulsar fork must not know how to implement Object WAL or Oxia metadata algorithms. The product-owned adapter
exposes one construction boundary:

```java
public interface NereusRuntimeProvider {
    NereusManagedLedgerRuntime create(
            NereusRuntimeConfiguration configuration,
            NereusRuntimeContext context) throws Exception;
}

public record NereusRuntimeConfiguration(
        OxiaClientConfiguration oxia,
        ObjectStoreConfiguration objectStore,
        StreamStorageConfig streamStorage,
        NereusManagedLedgerFactoryConfig managedLedger) {
}

public record NereusRuntimeContext(
        String brokerId,
        String processRunId,
        EventLoopGroup eventLoopGroup,
        OpenTelemetry openTelemetry,
        NereusCreationGuard creationGuard,
        ObjectStoreSecretResolver secretResolver,
        ClassLoader pluginClassLoader) {
}
```

`NereusRuntimeContext` belongs in the Pulsar adapter module; if importing `EventLoopGroup`/OpenTelemetry makes that
module broker-version-sensitive, the fork supplies executors/telemetry through a smaller product interface instead.
No broker-private class enters L0 modules.

`DefaultNereusRuntimeProvider` is the production implementation. The F2 writer ID is
`brokerId + "/" + processRunId`; `processRunId` contains at least 128 bits from `SecureRandom` and is regenerated at
every broker process start. `NereusRuntimeConfiguration.streamStorage().writerId()` must equal that value and its
cluster must equal the frozen broker cluster identity. Durable append authority is still the Oxia epoch/token, not
this human-readable ID.

Configured runtime/ObjectStore provider classes are loaded once with `pluginClassLoader`, require a public no-arg
constructor and are type-checked before any client is created. Constructor/type failure is a startup failure; there is
no fallback to another provider class.

The event loop, telemetry, classloader, creation guard and secret resolver in `NereusRuntimeContext` are borrowed
broker resources; neither the runtime nor providers close them. Executors created by the runtime itself remain
runtime-owned as specified in `06`.

### 3.1 ObjectStore provider

Phase 1 has an ObjectStore interface and a test-only local implementation, but no deployable provider. F2 broker
bootstrap therefore also requires:

```java
public interface ObjectStoreProvider extends AutoCloseable {
    ObjectStore create(
            ObjectStoreConfiguration configuration,
            ObjectStoreSecretResolver secretResolver) throws Exception;
}

public record ObjectStoreConfiguration(
        String providerClassName,
        URI endpoint,
        String region,
        String bucket,
        String prefix,
        boolean pathStyleAccess,
        Duration requestTimeout,
        int maxConnections,
        Optional<String> accessKeySecretRef,
        Optional<String> secretKeySecretRef,
        Optional<String> sessionTokenSecretRef) {
}

@FunctionalInterface
public interface ObjectStoreSecretResolver {
    Optional<char[]> resolve(String secretReference);
}
```

The provider class is configured explicitly and loaded with the broker/plugin classloader. F2 final integration uses
an S3-compatible provider against MinIO or an equivalent fixture. `LocalFileObjectStore` remains test-only and cannot
be selected by production broker configuration. Missing provider, credentials, bucket/prefix or unsupported checksum
behavior fails broker startup.

Provider ownership is exact: `NereusManagedLedgerRuntime` closes the created `ObjectStore` and then the provider
instance exactly once; the hybrid provider does not close either a second time. If `create` fails, the construction
owner closes the provider before propagating startup failure.

Secret references, not credential values, are stored in configuration. Access-key and secret-key references are both
present or both absent; a session-token reference requires both. The resolver returns a new character array per call.
The S3 provider copies it into the SDK credential object and zeroes the returned array in `finally`; it never writes
credentials into an exception, metric, thread name, projection record or `toString`. Empty references and unresolved
explicit references fail startup. When all three are absent, the provider uses its documented workload/default
credential chain.

## 4. Broker Configuration Schema

The fork adds typed, documented fields rather than reading arbitrary system properties:

```text
nereusEnabled
nereusRuntimeProviderClassName
nereusOxiaServiceAddress
nereusOxiaNamespace
nereusOxiaSessionTimeoutSeconds
nereusMaxOxiaPendingOperations
nereusObjectStoreProviderClassName
nereusObjectStoreEndpoint
nereusObjectStoreRegion
nereusObjectStoreBucket
nereusObjectStorePrefix
nereusObjectStorePathStyleAccess
nereusObjectStoreRequestTimeoutSeconds
nereusObjectStoreMaxConnections
nereusObjectStoreAccessKeySecretRef
nereusObjectStoreSecretKeySecretRef
nereusObjectStoreSessionTokenSecretRef
nereusMetadataTimeoutSeconds
nereusAppendTimeoutSeconds
nereusAppendRecoveryTimeoutSeconds
nereusReadTimeoutSeconds
nereusCloseTimeoutSeconds
nereusTailPollIntervalMillis
nereusAppendSessionTtlSeconds
nereusAppendSessionRenewBeforeSeconds
nereusAppendSessionMinCommitRemainingSeconds
nereusMaxEntryBytes
nereusMaxReadEntries
nereusMaxOpenLedgers
nereusMaxPendingCallbacks
nereusMaxRetainedAppendAttempts
nereusMaxScanEntries
nereusMaxResolveRanges
nereusMaxCommitChainScan
nereusMaxDerivedIndexRepairCommitsPerCall
nereusMaxCachedStreams
nereusMaxInFlightAppends
nereusMaxBufferedBytes
nereusMaxConcurrentObjectReads
nereusMaxReadBufferBytes
nereusMaxObjectBytes
nereusOffsetIndexCacheTtlSeconds
nereusEnableMetadataWatch
nereusEnableOffsetIndexCache
```

Credential values use the broker's secret/config provider and are not copied into projection metadata, logs or
metrics. Cluster identity defaults to `ServiceConfiguration.clusterName` but is frozen into the constructed runtime;
changing it dynamically requires restart and explicit migration review.

Validation before client construction:

- `nereusEnabled=true` when the hybrid provider is configured；
- Oxia service/namespace and object-store bucket/prefix are nonblank；
- all timeouts and limits are positive, and close timeout covers append plus append-recovery budgets；
- `nereusMaxEntryBytes` is at least Pulsar's maximum persisted entry size and no larger than L0 object/read buffers；
- ObjectStore and Oxia namespaces are scoped to the same Nereus cluster identity；
- the runtime provider and ObjectStore provider implement the exact expected interfaces；
- local filesystem/test providers are rejected outside the integration-test profile。

Dynamic updates to these fields are not supported in F2. Ordinary topic policies can select the storage class, but
changing connectivity, identity or durability configuration requires broker restart.

The conversion is constructor-by-constructor, with no untyped property bag:

| Target config | Exact broker mapping / fixed F2 value |
| --- | --- |
| `OxiaClientConfiguration` | service address, namespace, metadata request timeout, Oxia session timeout, `maxCommitChainScan`, max Oxia pending operations |
| `ObjectStoreConfiguration` | provider, parsed absolute endpoint URI, region/bucket/prefix, path-style flag, object request timeout, connection limit and secret references |
| `StreamStorageConfig` identity/session | cluster name, `brokerId/processRunId`, TTL, renew-before, minimum commit remaining |
| `StreamStorageConfig` deadlines | append/read from the matching Nereus fields; shutdown grace equals close timeout |
| `StreamStorageConfig` resource limits | resolve ranges, commit scan, derived repair page, cached streams, in-flight appends, buffered bytes, concurrent object reads, read buffer and object bytes |
| `StreamStorageConfig` fixed F2 fields | `maxAppendBatchRecords=1`, `autoAcquireAppendSession=true`; metadata watch and offset cache use their explicit booleans/TTL |
| `NereusManagedLedgerFactoryConfig` | metadata/append/recovery/read/close/poll deadlines and entry/read/open/callback/retained-attempt/scan limits |

The two `maxCommitChainScan` constructor arguments must be identical. `maxCachedStreams >= maxOpenLedgers`；
`maxInFlightAppends <= maxPendingCallbacks`；the WAL-v1 checked single-entry upper-bound function, including frame,
index and fixed F2 entry attributes, must fit `maxObjectBytes <= maxBufferedBytes` and
`maxObjectBytes <= maxReadBufferBytes`。Renew-before and minimum-commit-remaining are each positive and less than the
session TTL. The ObjectStore request timeout is no greater than append/read deadlines, and close timeout covers the
initial append plus callback recovery budgets. All byte relationships use checked `long` arithmetic and the exact
maximum-payload boundary has a construction test; comparing payload bytes directly to object bytes is insufficient.

## 5. Feature Admission

Selecting `managedLedgerStorageClassName=nereus` must not allow an unsupported broker feature to fail after it has
mutated subscription/producer state. Not every feature is known at topic open, so the fork uses operation-specific
gates rather than one impossible all-purpose check:

```java
CompletableFuture<StorageClassOpenPermit> prepareStorageClassOpen(
        TopicName topic, String persistenceName, String selectedClass, boolean createIfMissing);
CompletableFuture<Void> completeStorageClassOpen(StorageClassOpenPermit permit);
CompletableFuture<StorageClassDeletePermit> prepareStorageClassDelete(
        TopicName topic, String persistenceName, String selectedClass);
CompletableFuture<Void> completeStorageClassDelete(StorageClassDeletePermit permit);
void validateTopicOpen(TopicName topic, ManagedLedgerConfig config, NereusResolvedTopicFeatures features);
void validateProducer(TopicName topic, Producer producer, NereusResolvedTopicFeatures features);
void validatePublish(TopicName topic, ByteBuf entry, Topic.PublishContext context,
                     boolean transactional);
void validateEndTransaction(TopicName topic, int txnAction);
void validateSubscribe(TopicName topic, SubType type, boolean durable,
                       boolean readCompacted, boolean replicated, KeySharedMeta keySharedMeta);
void validateCreateSubscription(TopicName topic, boolean durable);
void validateAdminOperation(TopicName topic, NereusAdminOperation operation);
```

The raw topic policy object is not an effective view before `PersistentTopic` exists. The fork therefore resolves
broker defaults, namespace `Policies` and local/global `TopicPolicies` with the same precedence/normalization as
`HierarchyTopicPolicies` before storage selection:

```java
public record NereusResolvedTopicFeatures(
        Set<String> remoteReplicationClusters,
        boolean deduplicationEnabled,
        int messageTtlSeconds,
        int subscriptionExpirationMinutes,
        long compactionThresholdBytes,
        boolean retentionEnabled,
        boolean backlogEvictionEnabled,
        boolean pulsarOffloadEnabled,
        boolean entryFiltersEnabled,
        boolean shadowOrMigrationEnabled,
        boolean systemOrInternalTopic) {
}

public final class NereusTopicFeatureResolver {
    public static NereusResolvedTopicFeatures resolve(
            ServiceConfiguration broker,
            Policies namespacePolicies,
            Optional<TopicPolicies> localPolicies,
            Optional<TopicPolicies> globalPolicies,
            TopicName topic);
}

public record NereusTopicOpenContext(
        ManagedLedgerConfig managedLedgerConfig,
        NereusResolvedTopicFeatures features) {
}
```

The resolver is synchronous over one already-loaded input tuple and copies every relevant value; the returned view is
immutable and contains no mutable Pulsar policy/config object. Remote clusters exclude the configured local
cluster. Null/disabled TTL, expiration and compaction normalize to zero. Retention is enabled when either effective
time or size differs from zero. Backlog eviction is enabled when an effective size/time quota has a finite trigger;
Pulsar offload is enabled when an effective driver/threshold/deletion policy can schedule offload. Nonempty shadow
sources or namespace migration set the combined flag; nonempty effective entry-filter names set their own flag.
System/internal classification uses the target fork's existing
helpers, not name substring heuristics. Parameterized parity tests feed the same broker/namespace/topic combinations
into this resolver and a constructed `HierarchyTopicPolicies` and compare every normalized value.

`validateTopicOpen` requires no remote replication cluster, dedup false, TTL/expiration/compaction zero and every
boolean feature flag false. Delayed delivery is deliberately checked at publish rather than here because its broker
default is enabled; a Nereus topic remains usable for ordinary messages but every `deliverAtTime` entry is rejected.
The validated view is retained as one volatile immutable snapshot on `PersistentTopic`; an accepted policy-listener
update replaces it atomically, and producer/subscribe gates read it once per operation.

The fork factors the locked target `BrokerService.getManagedLedgerConfig` policy combination into one private
policy-input loader. Existing `getManagedLedgerConfig(TopicName)` preserves its signature and maps the shared result
to the config. Topic load calls `getNereusTopicOpenContext(TopicName)`, which builds config and effective features
from the **same** local/global/namespace snapshot; it does not issue a second policy read whose storage-class and
feature values can disagree.

Storage-class selection has one fork-owned durable binding in the broker `MetadataStoreExtended`:

```text
/managed-ledger-storage-bindings/v1/{sha256-base32("pulsar-storage-binding-v1\0" + exactPersistenceNameUtf8)}
```

```java
public enum StorageClassBindingState {
    CLAIMED,
    ACTIVE,
    DELETING,
    DELETED
}

public record StorageClassBindingRecord(
        int formatVersion,
        String persistenceName,
        String canonicalTopicName,
        String storageClass,
        long bindingGeneration,
        StorageClassBindingState state,
        long createdAtMillis,
        long stateVersion,
        long metadataVersion) {
}

public record StorageClassOpenPermit(
        String persistenceName,
        String storageClass,
        long bindingGeneration,
        long expectedMetadataVersion,
        boolean activationRequired) {
}

public record StorageClassDeletePermit(
        String persistenceName,
        String storageClass,
        long bindingGeneration,
        long expectedMetadataVersion) {
}
```

The key hash never replaces collision validation: the decoded exact persistence name and canonical `TopicName` must
match the request. `formatVersion=1`; generation starts at one and increments only on `DELETED -> CLAIMED` for a new
topic lifetime; that CAS resets `stateVersion=0` and `createdAtMillis` to the new claim time, while every transition
within the generation increments `stateVersion`. Class is immutable within a generation. `metadataVersion` comes from
the metadata-store stat, not serialized bytes. The record uses a versioned deterministic codec with golden bytes;
unknown major/state, invalid transition, mismatched name/hash or trailing data is corruption.

`prepareStorageClassOpen` runs before choosing either factory. On a missing binding it performs bounded get-only
checks against the Nereus topic projection/L0 state and stock
`bookkeeperFactory.asyncExists(persistenceName)`; lookup timeout/failure never means absent. Both live is an invariant;
only the non-selected class live is `NEREUS_STORAGE_CLASS_MIGRATION_REQUIRED`. If the selected class is already live,
the method CAS-adopts an `ACTIVE` binding. If neither is live and `createIfMissing=true`, it put-if-absent claims
`CLAIMED`; false returns not-found and writes nothing. A CAS loser rereads and applies the same rules.
Null/default selection is normalized to `bookkeeper` before this call; only exact `bookkeeper`/`nereus` values can be
bound. Non-persistent topics never enter this store.

For an existing binding:

| Binding state | Open behavior |
| --- | --- |
| `CLAIMED` | only the same class/generation may resume creation; the other class is rejected |
| `ACTIVE` | only the same class may open, and its durable state must exist; missing selected state is corruption, never empty recreation |
| `DELETING` | no live open is returned; resume the bound class's delete protocol |
| `DELETED` | with create-if-missing, verify old selected state is terminal and the other class absent, then CAS to the next generation's `CLAIMED`; otherwise not-found |

After the selected factory opens successfully, `completeStorageClassOpen` CASes the exact permit from `CLAIMED` to
`ACTIVE`; an adopted/already-active permit is a validated no-op. A crash before storage creation leaves `CLAIMED`, so
only the same class can safely retry. A crash after storage creation but before activation is repaired by observing
that same class's state and completing the CAS. `ACTIVE` is never used as a pre-create placeholder, which distinguishes
an interrupted claim from loss of previously active storage.

Delete obtains a permit by CASing the same generation `ACTIVE -> DELETING` before calling the bound factory. It then
resumes that class's logical delete and `completeStorageClassDelete` CASes `DELETING -> DELETED`. Failure after the
storage's irreversible delete schedules binding repair and does not claim rollback; new lifetimes remain blocked until
the mirror reaches `DELETED`. The binding owns only class/lifetime selection. BookKeeper metadata or the Nereus topic
projection/L0 stream remains the storage-specific data and lifecycle authority.

F2 never removes the binding key. `DELETED` is a small tombstone that is CAS-replaced by the next generation, so a
delete/create race cannot observe key absence and independently claim both classes.
The implementation uses only the locked `MetadataStore` primitives: `sync(key)` then `get(key)`, create with
`put(key,bytes,Optional.of(-1L))`, and transition with
`put(key,bytes,Optional.of(getResult.getStat().getVersion()))`. Bytes are copied
from `GetResult.getValue()`; the expected/metadata version is `GetResult.getStat().getVersion()`. Even an already-`ACTIVE` open permit is revalidated by
`sync/get` in `completeStorageClassOpen`; “activationRequired=false” means no state transition, not an unchecked no-op.
Every get/put/CAS is bounded by `nereusMetadataTimeoutSeconds`; version conflicts reread with capped exponential
backoff, while timeout/unavailability fails the broker operation and never falls back to an unbound factory call.
Class mismatch maps to stable `NotAllowedException(NEREUS_STORAGE_CLASS_MIGRATION_REQUIRED)`; both-live, active-missing
or corrupt binding maps to non-retriable persistence/invariant error; backend timeout/unavailability remains retriable
service-unavailable. No branch string-matches an underlying exception message.

Every broker topic-delete path, including force/partition fan-out, wraps the selected factory delete with the exact
binding delete permit. Direct facade deletion remains unit-test/internal surface and cannot be used as the production
cross-class lifecycle coordinator.

The permit and managed-ledger config future are captured **before** the locked target `BrokerService` delete path
removes schema/topic-policy records. Factory selection then uses `deletePermit.storageClass()`, and `asyncDelete`
receives that pre-delete config future rather than recomputing policy after a topic-level override disappeared. Only
after bound storage deletion and `completeStorageClassDelete` may policy cleanup report terminal success; partial
failures resume from the binding state.

The Nereus creation orchestrator receives a dedicated `NereusCreationGuard` from the fork. Immediately before
first/next-incarnation topic-projection put/CAS it verifies the permit is still `CLAIMED` for `nereus` at the same
generation and BookKeeper remains absent. It never overloads the managed-ledger ownership checker. A rejection may
leave only the documented empty unpublished L0 candidate/unused ledger ID; it cannot publish a competing identity.

```java
@FunctionalInterface
public interface NereusCreationGuard {
    CompletableFuture<NereusCreationPermit> acquire(String persistenceName);
}

public interface NereusCreationPermit {
    String persistenceName();
    long bindingGeneration();
    CompletableFuture<Void> validateBeforeProjectionPublish();
}
```

The factory acquires the permit at the start of every Nereus open, validates an existing projection's persisted
`storageClassBindingGeneration` against it, and calls its validation method immediately before a new projection
put/CAS. The production factory requires a non-null guard from `NereusRuntimeContext`; unit tests pass an explicit
fake. There is no implicit allow-all production constructor.

F2 has no cross-storage migration protocol, so policy writes close the remaining race: a topic-level storage class
may be set before its first durable ledger exists, but changing the effective class of an existing live topic is
rejected before policy mutation. A namespace-level storage-class change is accepted only while the namespace has no
existing persistent topics; otherwise operators use per-topic selection for not-yet-created topics. Same-value writes
are idempotent. Topic deletion must reach the selected storage's terminal delete before a newly created lifetime can
choose another class. These restrictions are an F2 product boundary, not a claim of BookKeeper/Nereus data migration.

The fork-owned enum is closed for F2; broker call sites do not pass free-form strings:

```java
public enum NereusAdminOperation {
    TERMINATE_TOPIC,
    DELETE_TOPIC,
    UNLOAD_TOPIC,
    DELETE_DURABLE_SUBSCRIPTION,
    CLEAR_BACKLOG,
    SKIP_MESSAGES,
    EXPIRE_MESSAGES,
    RESET_CURSOR,
    TRIGGER_COMPACTION,
    READ_COMPACTION_STATUS,
    TRIGGER_OFFLOAD,
    READ_OFFLOAD_STATUS,
    TRUNCATE_TOPIC,
    SET_SHADOW_TOPICS,
    MIGRATE_TOPIC
}
```

`validateAdminOperation` is an exhaustive switch with no `default`. `TERMINATE_TOPIC`, `DELETE_TOPIC` and
`UNLOAD_TOPIC` are allowed; delete first obtains the storage-binding delete permit, and they then reach the facade
seal/delete/local-close contracts. Every other value is rejected
before looking up or mutating a subscription and before invoking compactor, offloader, truncation or migration
code. The error is `NEREUS_UNSUPPORTED_ADMIN_OPERATION:<enum-name>`; adding an enum value without deciding its
admission is therefore a compile error. Read-only topic stats, internal stats and direct entry reads do not use this
enum and follow the exact facade contracts in `06`.

Topic/namespace policy setters may persist settings that are valid for BookKeeper topics. They do not make those
settings valid for Nereus: effective policy resolution is revalidated by `validateTopicOpen` before opening a Nereus
topic, and the live-topic policy listener runs the same validation before installing a replacement
`ManagedLedgerConfig`. A rejected live update keeps the last accepted config and unloads the topic; its next open
fails with the same stable feature error. This avoids partially enabling retention, TTL, offload, compaction,
deduplication, delayed delivery, replication, shadow or migration behavior inside an already-open Nereus topic.

Concretely, target `PersistentTopic.onUpdate` and `onPoliciesUpdate` route Nereus topic/namespace updates to a per-topic
`NereusTopicPolicyUpdateCoordinator` before copying new values into `AbstractTopic.topicPolicies`, calling
`ledger.setConfig`, `checkDeduplicationStatus`, `checkReplication`, compaction/offload scheduling or backlog/expiry
work. The notification is a hint: the coordinator serially refetches one authoritative local/global/namespace input
set, resolves/validates it, and applies that full current set on the topic ordered executor; it never applies a stale
event payload after validating a different snapshot. A null/delete notification therefore needs no guessed scope. On
rejection it sets a local admission-failed flag first, so new producer, publish and subscribe calls fail while
asynchronous unload runs. The policy record may remain valid for BookKeeper topics, but no rejected value is installed
or acted on by that live Nereus topic.

The coordinator assigns a monotonic refresh sequence. After IO completes on a non-topic thread, the topic ordered
executor drops an older sequence, validates the resolved view, clears both local/global hierarchy slots with explicit
empty-scope values, and reapplies broker -> namespace -> global -> local inputs. It derives the replacement
`ManagedLedgerConfig` from that same input set, calls `ledger.setConfig`, atomically publishes the feature view, then
runs accepted dedup/replication/rate-limit side effects (the first two remain disabled by validation). No policy IO or
unload callback runs while holding the topic lock.

For a Nereus topic, target `applyUpdatedTopicPolicies()` does not also invoke stock `checkPersistencePolicies()`;
that would perform a second policy read and replace the validated config. The coordinator-owned config application is
the single live update path. BookKeeper topics retain the stock method unchanged.

After binding activation and `PersistentTopic` construction, `BrokerService` installs the
`NereusTopicOpenContext` before calling `initialize()`. Nereus initialization makes the coordinator perform one fresh
full-policy refresh before creating persistent subscriptions or running replication/dedup checks; BookKeeper keeps the
locked stock path. This closes the window between the open-context snapshot and the target constructor/initialize
policy reads.

`validateTopicOpen` runs before `factory.asyncOpen`. `PersistentTopic.internalSubscribe` invokes
`validateSubscribe` before ownership/rate-limit accounting and before `getDurableSubscription` /
`getNonDurableSubscription`. `PersistentTopic.createSubscription` invokes `validateCreateSubscription` before
`ledger.asyncOpenCursor`. `PersistentTopic.addProducer` invokes `validateProducer` before `super.addProducer`, hence
before topic-epoch property mutation, producer-map insertion and dedup `producerAdded`. Both `publishMessage` and
`publishTxnMessage` invoke `validatePublish` before incrementing `pendingWriteOps`, dedup lookup, transaction-buffer
access or facade append. Admin entry points invoke their gate before their operation-specific mutation. Each rejection
is a stable `NotAllowedException`/admin error that names the unsupported feature and `nereus` storage class.

`validatePublish` parses/peeks one `MessageMetadata` without changing the entry's reader/writer indexes or reference
count. It rejects `transactional=true`, every marker message and every metadata value with `hasDeliverAtTime()`, even
when the timestamp is already in the past; F2 has no delayed-delivery index/dispatcher contract. Parse failure is a
publish validation failure and performs no append. Normal encrypted/chunked/batched bytes remain opaque after this
admission check. Remote/replicator producers are rejected at producer attach, so publish does not infer replication
from producer-name strings.

Global `transactionCoordinatorEnabled` does not make every Nereus topic unloadable. In both `PersistentTopic`
constructors, the fork checks `ledger instanceof NereusManagedLedger` rather than a later-mutable config string: a
Nereus ledger always receives `TransactionBufferDisable`, while BookKeeper follows the stock provider selection. The
existing initial and normal
publish `syncMaxReadPositionForNormalPublish` calls therefore remain harmless no-ops for Nereus. `publishTxnMessage`
uses the transactional publish rejection above, and `endTxn` invokes `validateEndTransaction` before commit/abort
buffer access. `checkIfTransactionBufferRecoverCompletely` completes through the disabled buffer. No Nereus topic
constructs or recovers transaction-buffer snapshot/system-topic state in F2.

| Broker feature | F2 admission |
| --- | --- |
| ordinary persistent user topic | allowed |
| existing topic switched between BookKeeper and Nereus | rejected; no F2 migration contract |
| ordinary non-transactional producer, batching/chunking, client encryption | allowed; entry bytes remain opaque |
| Reader, read-only cursor, non-durable Exclusive/Failover consumer | allowed |
| every durable subscription/consumer | rejected before `openCursor`; F3 owns durable progress |
| Shared / Key_Shared | rejected before cursor open |
| read-compacted cursor / topic compaction | rejected; F4 not implemented |
| BookKeeper offload policy/admin operation | rejected; Nereus Object WAL is not Pulsar offload |
| transaction buffer / pending ack transaction | rejected; F8 not implemented |
| delayed delivery / replicated subscription | rejected; F8 not implemented |
| geo-replication | rejected until its projection contract exists |
| broker-entry metadata interceptor / managed-ledger payload processor | rejected; this Pulsar version wraps both in `ManagedLedgerInterceptorImpl`, whose before-add, failure rollback, property recovery and cache-processor lifecycle F2 does not implement |
| broker message deduplication | rejected; F2 has no durable producer-sequence state |
| broker/topic entry filters | rejected until accept/reject/reschedule and cursor-progress effects are audited |
| TTL/backlog operation that requires durable cursor mutation | rejected or kept on BookKeeper |
| non-default message TTL, retention, backlog-eviction or subscription-expiration policy | rejected at topic open; F2 has no cursor-driven trim/GC contract |
| auto-skip non-recoverable data | rejected; corruption is never converted into a skipped virtual position |
| schema-registry/system/internal storage topic | kept on BookKeeper; selecting Nereus is an explicit load error until F8 bootstrap |
| ordinary user topic whose messages reference a schema | allowed; the separate schema storage topic remains BookKeeper |
| shadow topic / migration / truncate | rejected |

This validation occurs where the fork still has a `TopicName`, operation arguments and relevant policies. The Nereus
factory continues treating the persistence name as opaque and never reparses it to guess whether it is a system
topic or which consumer mode is in use. The factory still rejects unsafe `ManagedLedgerConfig` values as defense in
depth.

Namespace-level `nereus` policy can otherwise include system topics implicitly. The broker-side validator must catch
that case; it must not silently override the policy to BookKeeper after topic selection.

## 6. Open Call Path and Ownership Checker

```text
BrokerService.getNereusTopicOpenContext(topic) from one policy-input snapshot
  -> normalize selected storage class from context config
  -> if selected=nereus: validateTopicOpen against context features
  -> migrationGuard.prepareStorageClassOpen(topic, exact persistence name, selected class, createIfMissing)
  -> managedLedgerStorage.getManagedLedgerStorageClass(selected class)
  -> selected factory asyncOpen(exact persistence name, config, ...)
  -> migrationGuard.completeStorageClassOpen(exact permit)
```

The open callback does not construct/publish `PersistentTopic` until binding activation succeeds. Activation failure
closes the returned local ledger and fails topic load; the same-class `CLAIMED` recovery path repairs it on retry.

The `mlOwnershipChecker` can be consulted only after an open-time metadata condition conflict to decide whether the
broker should retry opening. It never becomes an append session or fencing token. F2 open does not eagerly acquire an
append session; the first append lets L0 lazily acquire/renew it. This permits a read-capable facade to open while a
previous writer session is still live and keeps broker ownership separate from durable write authority.

## 7. Dependency and Packaging Rules

```text
Pulsar server distribution
  -> fork Nereus hybrid provider
  -> com.nereusstream:nereus-pulsar-adapter
  -> com.nereusstream:nereus-managed-ledger
  -> pinned managed-ledger/common API
  -> Nereus L0 modules
```

Rules:

- one Nereus version is pinned in the Pulsar fork build and packaged in the server distribution；
- the managed-ledger API version used to compile Nereus has the locked blobs in `01`/`06`；
- `pulsar-broker` is not a transitive dependency of Nereus L0 modules；
- duplicate Pulsar/Netty/Oxia versions in the distribution are detected by a dependency gate；
- no `mavenLocal()` or floating snapshot is used by release builds；
- composite-build mode verifies both repository commits before compiling integration tests。

Rollout is two-step: every broker that can own the namespace must first run the hybrid provider/binding guard while
policies still select BookKeeper; only after that cluster-wide convergence may an operator enable `nereus` for new
topics. An older broker can bypass the binding protocol, so mixed-version ownership during Nereus policy enablement is
explicitly unsupported and blocked by the deployment gate.

## 8. Broker Integration Tests

In addition to `05`, F2-M5 requires:

1. reflection construction through `ManagedLedgerStorage.create(...)` and the thread context classloader；
2. exact `[bookkeeper,nereus]` order, null/default lookup and unknown-name behavior；
3. one stable Nereus factory returned by repeated storage-class calls；
4. partial initialize failure closes ObjectStore/Oxia/executors and prevents broker start；
5. close aggregates failures and closes every resource once；
6. existing BookKeeper-to-Nereus and Nereus-to-BookKeeper switches fail before creating state in the selected class;
   both-live state is an invariant failure, delete uses the binding class even after a topic-level override is
   removed, and delete-then-new-lifetime selection is allowed；
7. namespace storage-class change with existing topics is rejected, while an empty namespace and a not-yet-created
   topic can select Nereus；
8. effective-feature resolver parity covers broker/namespace/local/global policy precedence and normalization；an
   unsupported Nereus view fails before a storage-class binding is claimed；
9. namespace policy that unintentionally selects a system topic fails clearly instead of falling back；
10. each unsupported feature in section 5 is rejected at its exact topic/producer/publish/subscribe/admin gate before facade
   mutation or append/read IO；
11. broker-entry metadata and payload-processor configurations both produce a non-null
   `ManagedLedgerInterceptor` and are rejected before the first append；
12. a remote-broker append wakes a local read waiter with Oxia watch delivery disabled, through polling；
13. with the broker transaction coordinator enabled, a Nereus topic constructs only `TransactionBufferDisable`; txn,
    end-txn, marker and `deliverAtTime` paths fail before pending-write, dedup, transaction-buffer and ledger
    counters/mocks observe a call; publish validation preserves `ByteBuf` indexes/refcount, while a BookKeeper topic
    still uses the configured transaction provider；
14. MinIO/S3-compatible restart test proves shared Object WAL, not a process-local map, supplies bytes；
15. the useful `pulsar-storage` smoke shape is adapted to producer plus Reader/non-durable consumer; durable
    subscription creation is rejected before `ManagedLedger.openCursor`；
16. every typed broker field maps to the expected immutable constructor argument; invalid cross-field bounds fail
    before provider/client construction；
17. explicit secret references resolve once, returned arrays are zeroed, and injected sentinel credentials appear in
    no config `toString`, error, projection, log or metric；
18. BookKeeper-only and Nereus topics coexist without virtual ledger IDs appearing in BookKeeper mocks/logs。
19. a rollout gate refuses Nereus policy enablement while any namespace-owning broker lacks the binding-capable
    version；existing BookKeeper topics are adopted without data movement。

## 9. Release Claim Boundary

`phase2FinalCheck` proves ManagedLedger facade integration over real Oxia and an S3-compatible ObjectStore provider.
It does not claim F3 cursor durability, F4 compaction/offload, F8 transactions/system-topic bootstrap or transparent
migration of existing BookKeeper topics. Release notes and metrics must use the same boundary.
