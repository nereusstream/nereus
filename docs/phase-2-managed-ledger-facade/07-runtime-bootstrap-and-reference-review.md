# Runtime Bootstrap, Broker Admission and Reference Review

本文补齐 F2 从 facade 类到真实 broker runtime 之间的构造、配置、feature admission 和资源所有权。
真实目标接口与 broker call path 的唯一权威是用户提供的 clean checkout
`/Users/liusinan/apps/ideaproject/nereusstream/pulsar@100d3ef0ff7c7da36d497453b141ddff6f34a9d3`。本文也保留
对 `/Users/liusinan/apps/ideaproject/GITHUB/pulsar-storage` 原型的源码评审结论；该原型只用于提供扩展点
探索证据，不是 Nereus durability/position/cursor 合同的来源。没有使用在线源码。

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

Target-source re-review adds four implementation-critical facts，all locked in `01` and consumed below：

1. `PulsarService.newManagedLedgerStorage()` runs before final bound addresses and `brokerId` creation；runtime
   bootstrap cannot require a broker ID；
2. `PersistentTopic.addFailed` plus `decrementPendingWriteOpsAndCheck()` can call
   `readyToCreateNewLedger()` and `unfence()` after a generic add failure；Nereus exact recovery needs an explicit
   generation-safe handoff；
3. `Consumer.messageAcked` handles transaction/batch acknowledgement before delegating to
   `PersistentSubscription.acknowledgeMessageAsync`；F2 capability validation must run before those mutations；
4. target `PersistencePolicies.equals` compares storage-class strings by reference identity；the fork must replace
   that comparison with `Objects.equals` and lock the behavior with equal-content/different-instance tests。

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
```

### 2.1 NereusManagedLedgerStorage

The class has a public no-arg constructor because `ManagedLedgerStorage.create(...)` uses reflection. Its fields are
assigned only during `initialize`:

```java
public final class NereusManagedLedgerStorage implements ManagedLedgerStorage {
    private ManagedLedgerClientFactory bookkeeperStorage;
    private NereusManagedLedgerFactory nereusFactory;
    private NereusStorageClassBindingStore bindingStore;
    private NereusBrokerCapabilityCoordinator capabilityCoordinator;
    private ManagedLedgerStorageClass bookkeeperClass;
    private ManagedLedgerStorageClass nereusClass;
    private final AtomicBoolean initialized = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    public NereusBrokerCapabilityCoordinator capabilityCoordinator();
    public NereusStorageClassBindingStore bindingStore();
}

public final class NereusStorageClassBindingStore implements AutoCloseable {
    public NereusStorageClassBindingStore(
            MetadataStoreExtended metadataStore,
            ManagedLedgerFactory bookkeeperFactory,
            StorageClassBindingKeyspace keyspace,
            StorageClassBindingCodec codec,
            Duration operationTimeout,
            int maxPendingOperations);

    public NereusCreationGuard creationGuard();
    public void attachNereusFactory(NereusManagedLedgerFactory factory);
}
```

Initialization sequence:

1. reject double initialize；
2. validate every Nereus broker setting without opening a topic；
3. construct a stock `ManagedLedgerClientFactory` and call its exact locked `initialize(...)`；
4. capture its one `bookkeeper` storage class, verify it is the default, and construct a binding store over the
   broker-supplied `MetadataStoreExtended` without taking ownership of that shared store；
5. generate one cryptographically random process-run ID, create the Nereus ObjectStore provider, shared Oxia runtime,
   L0 `StreamStorage`, projection store and facade runtime in local variables；
6. construct one Nereus default `ManagedLedgerConfig`、one zero-cache compatibility
   `ManagedLedgerFactoryConfig` and then the exact `NereusManagedLedgerFactory` constructor from `06` with
   `ownsRuntime=true`，transferring runtime ownership to it；construct one stable
   `NereusManagedLedgerStorageClass`；attach that factory to the binding store exactly once；
7. mark the capability coordinator `storageInitialized` but do not publish a broker capability yet because
   `brokerId` is not available；
8. publish the immutable `[bookkeeperClass, nereusClass]` list only after every step succeeds。

The explicit attach closes the constructor cycle：the facade needs `bindingStore.creationGuard()`，while later
binding inspection needs the facade's get-only `inspectStorageState`。`creationGuard()` is usable immediately because
it reads only binding/BookKeeper truth；open/delete preparation is rejected until `attachNereusFactory` succeeds。
Null/double attach is a startup error，and no partially attached store is published。

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

Both fork-private accessors return their one instance only after successful initialization and otherwise throw a
startup-state error. `BrokerService` constructs the migration/namespace guards from `bindingStore()`；the fork's
`BrokerRegistryImpl` obtains the capability coordinator from
`pulsar.getManagedLedgerStorage() instanceof NereusManagedLedgerStorage` while constructing lookup data；when
`nereusEnabled=true`，any other provider type or missing initialized coordinator is a startup failure rather than a
capability omission.

Close sequence first stops capability/new binding admission，then closes the Nereus factory、stock
`ManagedLedgerClientFactory` and finally binding-store local state. Factory shutdown closes its owned Nereus runtime;
the hybrid provider never closes that runtime separately. Before ownership transfer during a
failed initialize, the provider closes the local runtime directly. All close attempts run; the first `IOException`
retains later failures as suppressed exceptions. `InterruptedException` from factory shutdown restores the thread's
interrupt flag and is wrapped as `IOException`; `ManagedLedgerException` is also wrapped. Stock close still runs
after either failure. Repeated close is a no-op.

`NereusStorageClassBindingStore` borrows the broker `MetadataStoreExtended`; it owns no connection and never closes
that store. Its in-memory codec/cache state is discarded only after both factories stop admitting opens/deletes.
`NamespaceStorageClassPolicyGuard` is owned by the fork's BrokerService lifecycle，closes only its
`LockManager`/outstanding resource locks, and never closes the borrowed `CoordinationService`。

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

public record NereusProcessIdentity(
        String processRunId,
        String writerId) {
    public static NereusProcessIdentity generate(SecureRandom random);
}

public record NereusRuntimeContext(
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

`DefaultNereusRuntimeProvider` is the production implementation. During hybrid-storage initialization, before
building `StreamStorageConfig`，the fork calls `NereusProcessIdentity.generate(new SecureRandom())` exactly once。
The method fills 16 random bytes, Base64-URL encodes them without padding as `processRunId`，zeros the temporary byte
array and returns `writerId="pulsar-f2/" + processRunId`。The broker configuration mapper injects both exact values
into `StreamStorageConfig` before `create` is called. The provider verifies that relationship and that the cluster
equals the frozen broker cluster identity；it neither reads nor waits for `PulsarService.getBrokerId()`。Phase 1.5
derives the durable writer-run hash and process-local `AppendAttemptId` namespace from the explicit process ID。
Durable append authority is still the Oxia epoch/token, not this human-readable ID.

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
credential chain. The SDK may internally retain immutable credential Strings until its provider/client is closed；F2
does not falsely claim those Strings are zeroable。The enforceable contract is zeroing every resolver-owned `char[]`、
short SDK-provider lifetime/close and complete redaction outside that SDK object.

### 3.2 S3-compatible ObjectStore code contract

Production targets in `nereus-object-store` are concrete，not a second ObjectStore abstraction：

```text
provider/S3CompatibleObjectStoreProvider.java
provider/S3CompatibleObjectStore.java
provider/S3ObjectKeyMapper.java
provider/S3ObjectErrorMapper.java
provider/NoopObjectStoreSecretResolver.java
```

`NoopObjectStoreSecretResolver.resolve` always returns `Optional.empty()` and has no resources。It exists so the SDK
default credential chain needs no fabricated secret source；configuring any explicit reference with it fails startup。

`S3CompatibleObjectStore` implements the existing `ObjectStore` interface with an SDK asynchronous client；none of
its public methods calls `join/get` or performs network/file IO on the caller thread. Every method null-checks and
validates before copying input, returns a future even for ordinary validation/closed-state failure, and links
cancellation/deadline of that future to cancellation of the SDK request. `options.timeout()` is the one operation
deadline；provider/default SDK retry attempts must fit inside it and may not install an independent longer timeout.

Provider `create` is the sole bootstrap exception to nonblocking caller behavior：after constructing the async client
it issues one `HeadBucket` and waits at most `ObjectStoreConfiguration.requestTimeout` on the broker initialization
thread. Not-found、auth/region mismatch or timeout cancels the request, closes the client/provider and fails broker
startup. It does not create the bucket/prefix or write a probe object；ongoing data methods remain fully asynchronous。

Key mapping is one-to-one and path-independent：

```java
public final class S3ObjectKeyMapper {
    public S3ObjectKeyMapper(String canonicalPrefix);
    public String map(ObjectKey key);
}

map(key) = canonicalPrefix + "/objects/v1/"
        + base64UrlWithoutPadding(strictUtf8(key.value()))
```

The configured prefix is already canonical and is rejected when blank、starts/ends with `/`、contains an empty、`.`
or `..` segment, a control/NUL character, or is not strict UTF-8. The mapped key must fit S3's 1,024 UTF-8-byte key
limit. The original `ObjectKey` is never normalized；therefore two Java strings cannot alias after mapping.

`putObject` copies the input's remaining bytes without changing caller indices，computes exact CRC32C and compares it
to `PutObjectOptions.expectedChecksum()` before network IO. Only CRC32C is accepted in F2. It writes two reserved S3
user-metadata values：

```text
nereus-storage-checksum-type  = CRC32C
nereus-storage-checksum-value = eight lowercase hexadecimal digits
```

Caller metadata keys are lower-cased as required by S3, must not collide with either reserved key, and are returned
without reserved keys by `headObject`；two input keys that collide after lower-casing are rejected rather than
last-write-wins. The checked UTF-8 header bytes for caller plus reserved user metadata must not exceed 2,048；overflow
is `INVALID_ARGUMENT` before SDK submission. With `ifAbsent=true` the actual PUT request carries
`If-None-Match: *`；there is no HEAD/read-before-write race. HTTP 412 is non-retriable
`OBJECT_UPLOAD_FAILED`。A successful response must contain a nonblank opaque ETag for `PutObjectResult.etag`，but ETag
is never parsed or treated as a checksum. An SDK checksum response, when present, is additionally compared to the
locally computed CRC32C；its absence does not weaken the stored reserved checksum.

The request also carries the exact `PutObjectOptions.contentType` and copied content length；it adds no compression or
content encoding. Success returns the caller's logical `ObjectKey`、exact copied length、locally computed checksum and
opaque response ETag；a response can never substitute a differently normalized key or length。

`headObject` requires nonnegative length and both valid reserved metadata fields，constructs the whole-object
`Checksum` from them and returns a defensive user-metadata map. Missing/malformed/reserved checksum metadata is
`OBJECT_CHECKSUM_MISMATCH`，not an empty checksum or ETag fallback.

For `readRange`，validate `offset >= 0`、`length >= 0`、checked `offset + length` and
`length <= Integer.MAX_VALUE` before IO. For positive length, send exactly
`Range: bytes={offset}-{offset+length-1}`，require HTTP 206、an exact `Content-Range` and exactly `length` response
bytes. HTTP 200、short/long bodies and inconsistent bounds are non-retriable `OBJECT_READ_FAILED`。For zero length,
perform asynchronous HEAD only, require `offset <= objectLength`，and return an empty read-only buffer plus
CRC32C(empty) without issuing an invalid GET range. In both cases the returned checksum covers the exact returned
range；when `RangeReadOptions.expectedChecksum` is present, require CRC32C before IO and compare it to that
independently computed CRC32C before success. SHA256/another checksum request is `INVALID_ARGUMENT` in F2 rather than
an implicit algorithm switch.

`S3ObjectErrorMapper` recursively unwraps SDK completion wrappers and applies this closed table：

| Failure | Nereus result |
| --- | --- |
| caller validation | `INVALID_ARGUMENT`, non-retriable |
| caller cancellation | `CANCELLED`, non-retriable |
| operation deadline | `TIMEOUT`, retriable |
| closed provider/store | `STORAGE_CLOSED`, non-retriable |
| HTTP 404 read/head | `OBJECT_NOT_FOUND`, retriable |
| HTTP 412 conditional PUT | `OBJECT_UPLOAD_FAILED`, non-retriable |
| HTTP 409 conditional-request conflict | `OBJECT_UPLOAD_FAILED`, retriable；retry remains conditional |
| HTTP 401/403 or other deterministic 4xx | operation-specific upload/read error, non-retriable |
| HTTP 408/429/5xx, connection reset or SDK transport timeout | operation-specific upload/read error, retriable |
| CRC32C mismatch/malformed checksum metadata | `OBJECT_CHECKSUM_MISMATCH`, non-retriable |
| unexpected response shape/body length | `OBJECT_READ_FAILED`, non-retriable |

Exception text includes only operation、bucket and
`sha256-base32("nereus-s3-key-log-v1\0" + strictUtf8(logicalKey))`；it excludes
endpoint credentials、secret references、raw key and response bodies. `close()` atomically rejects new work, cancels
or drains already admitted requests within the runtime close budget and closes the SDK client once. Production
provider construction rejects `LocalFileObjectStore` and every `file:` endpoint.

Because an SDK exception/cause can embed the request URI or raw key，the public `NereusException` does not retain that
throwable or its message. Safe diagnostics copy only SDK exception class、HTTP status、service error code/request ID
after strict character/length validation and the key hash above；logs use the same fields and never log the SDK
throwable object. Unit tests inject key/credential sentinels into every SDK failure shape and search the full returned
exception/suppressed stack plus captured logs。

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
nereusObjectStoreSecretResolverClassName
nereusMetadataTimeoutSeconds
nereusAppendTimeoutSeconds
nereusAppendRecoveryTimeoutSeconds
nereusAppendRecoveryAttemptTimeoutSeconds
nereusAppendRecoveryBackoffMinMillis
nereusAppendRecoveryBackoffMaxSeconds
nereusAppendRecoveryTerminalTtlSeconds
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
nereusMaxAppendRecoveryTerminals
nereusMaxScanEntries
nereusMaxNamespaceBindingScanEntries
nereusMaxBindingPendingOperations
nereusMaxProjectionMetadataPendingOperations
nereusProjectionMetadataMaxValueBytes
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

Exact `ServiceConfiguration` field types/defaults are frozen below；all duration fields are converted with checked
`Duration.ofSeconds/ofMillis` only after validation：

| Field(s) | Java type | Default / resolution |
| --- | --- | --- |
| `nereusEnabled` | `boolean` | `false` |
| `nereusRuntimeProviderClassName` | `String` | `com.nereusstream.pulsar.DefaultNereusRuntimeProvider` |
| `nereusOxiaServiceAddress`, `nereusOxiaNamespace` | `String` | empty；required when enabled |
| `nereusOxiaSessionTimeoutSeconds` | `long` | `30` |
| `nereusMaxOxiaPendingOperations` | `int` | `1024` |
| `nereusObjectStoreProviderClassName` | `String` | `com.nereusstream.objectstore.provider.S3CompatibleObjectStoreProvider` |
| `nereusObjectStoreEndpoint`, `nereusObjectStoreRegion`, `nereusObjectStoreBucket`, `nereusObjectStorePrefix` | `String` | empty；required when enabled |
| `nereusObjectStorePathStyleAccess` | `boolean` | `false` |
| `nereusObjectStoreRequestTimeoutSeconds` | `long` | `30` |
| `nereusObjectStoreMaxConnections` | `int` | `64` |
| three ObjectStore secret-ref fields | `String` | empty means absent |
| `nereusObjectStoreSecretResolverClassName` | `String` | `com.nereusstream.objectstore.provider.NoopObjectStoreSecretResolver`；explicit refs require replacing it with a resolving implementation |
| `nereusMetadataTimeoutSeconds`, `nereusAppendTimeoutSeconds`, `nereusAppendRecoveryTimeoutSeconds`, `nereusReadTimeoutSeconds` | `long` | `30` each |
| `nereusAppendRecoveryAttemptTimeoutSeconds` | `long` | `5` |
| `nereusAppendRecoveryBackoffMinMillis` | `long` | `100` |
| `nereusAppendRecoveryBackoffMaxSeconds` | `long` | `5` |
| `nereusAppendRecoveryTerminalTtlSeconds` | `long` | `600` |
| `nereusCloseTimeoutSeconds` | `long` | `75` |
| `nereusTailPollIntervalMillis` | `long` | `1000` |
| session TTL / renew-before / minimum-remaining seconds | `long` | `30 / 10 / 5` |
| `nereusMaxEntryBytes` | `int` | `0` sentinel resolved once with `Math.addExact(config.getMaxMessageSize(), Commands.MESSAGE_SIZE_FRAME_PADDING)`；resolved value must be positive |
| `nereusMaxReadEntries` | `int` | `100` |
| `nereusMaxOpenLedgers` | `int` | `10000` |
| `nereusMaxPendingCallbacks` | `int` | `1024` |
| `nereusMaxRetainedAppendAttempts`, `nereusMaxAppendRecoveryTerminals` | `int` | `1024 / 2048` |
| `nereusMaxScanEntries` | `int` | `10000` |
| `nereusMaxNamespaceBindingScanEntries` | `int` | `100000`；overflow fails namespace policy update closed |
| `nereusMaxBindingPendingOperations` | `int` | `1024` |
| `nereusMaxProjectionMetadataPendingOperations` | `int` | `1024` |
| `nereusProjectionMetadataMaxValueBytes` | `int` | `65536` |
| resolve ranges / commit scan / derived-repair page | `int` | `64 / 10000 / 256` |
| cached streams / in-flight appends | `int` | `10000 / 1024` |
| `nereusMaxBufferedBytes` | `long` | `67108864` |
| `nereusMaxConcurrentObjectReads` | `int` | `64` |
| `nereusMaxReadBufferBytes` | `long` | `134217728` |
| `nereusMaxObjectBytes` | `int` | `16777216` |
| `nereusOffsetIndexCacheTtlSeconds` | `long` | `5` |
| `nereusEnableMetadataWatch`, `nereusEnableOffsetIndexCache` | `boolean` | `false / true` |

The `0` sentinel is legal only on the raw broker field and never reaches a Nereus constructor. Every other numeric
field must be positive as configured. Blank optional secret refs become `Optional.empty()`；blank required strings
are never silently defaulted.

Credential values use the explicitly configured resolver and are not copied into projection metadata, logs or
metrics. Cluster identity defaults to `ServiceConfiguration.clusterName` but is frozen into the constructed runtime;
changing it dynamically requires restart and explicit migration review.

Validation before client construction:

- `nereusEnabled=true` when the hybrid provider is configured；
- persistent topics are enabled and the configured load manager exposes the locked extensible `BrokerRegistry`；
- Oxia service/namespace and object-store bucket/prefix are nonblank；
- all timeouts and limits are positive, and close timeout covers append plus append-recovery budgets；
- recovery-attempt timeout is no greater than callback recovery timeout, backoff minimum is no greater than maximum,
  terminal TTL is positive and terminal capacity is at least retained-attempt capacity；
- `nereusMaxEntryBytes` is at least Pulsar's maximum persisted entry size and no larger than L0 object/read buffers；
- projection metadata max bytes is exactly `65536` in F2 and its pending-operation limit is no greater than the
  shared Oxia client limit；
- ObjectStore and Oxia namespaces are scoped to the same Nereus cluster identity；
- the runtime provider and ObjectStore provider implement the exact expected interfaces；
- endpoint is absolute `http`/`https` with host，no user-info/query/fragment and empty-or-`/` path；prefix passes
  `S3ObjectKeyMapper` canonical validation, and secret resolver construction/type-check succeeds before
  provider/client creation；
- local filesystem/test providers are rejected outside the integration-test profile。

Dynamic updates to these fields are not supported in F2. Ordinary topic policies can select the storage class, but
changing connectivity, identity or durability configuration requires broker restart.

The conversion is constructor-by-constructor, with no untyped property bag:

| Target config | Exact broker mapping / fixed F2 value |
| --- | --- |
| `OxiaClientConfiguration` | service address, namespace, metadata request timeout, Oxia session timeout, `maxCommitChainScan`, max Oxia pending operations |
| `ObjectStoreConfiguration` | provider, parsed absolute endpoint URI, region/bucket/prefix, path-style flag, object request timeout, connection limit and secret references |
| `StreamStorageConfig` identity/session | cluster name, writer ID=`pulsar-f2/{processRunId}`, explicit cryptographically generated `processRunId`, TTL, renew-before, minimum commit remaining；no broker ID dependency |
| `StreamStorageConfig` deadlines | append/read/recovery attempt/backoff/terminal TTL from matching Nereus fields; shutdown grace equals close timeout |
| `StreamStorageConfig` resource limits | resolve ranges, commit scan, derived repair page, cached streams, in-flight appends, retained attempts/terminals, buffered/primary-append bytes, concurrent primary reads, primary-read buffer and object bytes |
| `StreamStorageConfig` fixed F2 fields | `maxAppendBatchRecords=1`, `autoAcquireAppendSession=true`; metadata watch and offset cache use their explicit booleans/TTL |
| `NereusManagedLedgerFactoryConfig` | metadata/append/recovery/read/close/poll deadlines and entry/read/open/callback/retained-attempt/scan limits |
| `ProjectionMetadataStoreConfig` | metadata timeout, projection-specific pending-operation limit and max encoded value bytes |
| `NereusStorageClassBindingStore` | metadata timeout and binding pending-operation limit；admission occurs before metadata-store calls |
| `NamespaceStorageClassPolicyGuard` | metadata timeout and namespace binding-scan cap；binding reads use a bounded async fan-out |

The two `maxCommitChainScan` constructor arguments must be identical. `maxCachedStreams >= maxOpenLedgers`；
the core/facade `maxRetainedAppendAttempts` values must be identical；core terminal capacity is at least that value；
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

The fork also changes the final target predicate in `PersistencePolicies.equals` to
`Objects.equals(managedLedgerStorageClassName, other.managedLedgerStorageClassName)`；all other field comparisons and
the existing `Objects.hash` stay unchanged. Tests use two distinct `new String("nereus")` instances, null/null and
null/non-null pairs and assert the equals/hashCode contract. Without this fix a policy refresh can be spuriously
classified as changed solely by String allocation identity.

### 5.1 Broker capability publication and rollout gate

The exact capability is a reserved `BrokerLookupData.properties` entry：

```text
key   = nereus.storage-binding-protocol
value = 1
```

`NereusBrokerCapabilityCoordinator` has this fork-private surface：

```java
public final class NereusBrokerCapabilityCoordinator {
    public static final String PROPERTY = "nereus.storage-binding-protocol";
    public static final String VERSION = "1";

    public void markStorageInitialized();
    public void attachBrokerRegistry(BrokerRegistry brokerRegistry);
    public Map<String, String> decorateLookupProperties(Map<String, String> configured);
    public CompletableFuture<Void> requireClusterReady();
}
```

Hybrid storage calls `markStorageInitialized` only after both storage classes and the Nereus runtime are fully
constructed. Later，after `brokerId` and advertised addresses exist，`BrokerRegistryImpl` obtains the coordinator,
calls `attachBrokerRegistry(this)` exactly once and then calls `decorateLookupProperties` while constructing
`BrokerLookupData`，all before `start()` publishes lookup data。It returns an immutable copy and adds the reserved pair
only when the initialized marker is present；a user-supplied reserved key, missing marker、missing/double registry
attach or later mutation is a broker startup failure. The attached registry is not called from its constructor；it is
used only by later policy/claim admission after `isStarted/isRegistered`。Thus configuration alone cannot spoof
readiness.

`requireClusterReady` uses the locked `BrokerRegistry.getAvailableBrokerLookupDataAsync()`，filters brokers with
`persistentTopicsEnabled=true`，and requires every returned value—including local—to contain exactly version `1`。
An empty set、lookup error、missing property、unknown version or broker-set change observed during the check fails
closed. The policy mutation path calls it immediately before allowing a namespace/topic policy to become effectively
`nereus`，and a first Nereus binding claim calls it again. The check shares the broker metadata deadline and never
assumes timeout means an old broker is absent. F2 requires the target extensible broker registry；another load-manager
mode is rejected when Nereus is enabled.

This gate proves convergence of the currently active set, not safety of a later binary downgrade. Release procedure
therefore installs protocol `1` on every broker before any Nereus policy and forbids a protocol-0 broker from joining
while such a policy/binding exists. Rollback first drains/deletes or migrates those policies using a future explicit
protocol；an old broker is never treated as a valid fallback owner.

### 5.2 Durable per-topic storage-class binding

Storage-class selection has one fork-owned durable binding in the broker `MetadataStoreExtended`:

```text
/managed-ledger-storage-bindings/v1/
  {sha256-base32("pulsar-storage-binding-namespace-v1\0" + canonicalNamespaceUtf8)}/
  {sha256-base32("pulsar-storage-binding-topic-v1\0" + exactPersistenceNameUtf8)}
```

```java
public final class StorageClassBindingKeyspace {
    public String namespaceRoot(NamespaceName namespace);
    public String bindingKey(
            NamespaceName namespace,
            String exactPersistenceName);
}
```

This is the only path builder；it requires canonical exact names、strict UTF-8 and the same 16-KiB persistence-name
bound as F2 projection identity. Callers never concatenate or reuse a projection hash with the binding domains.

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

`StorageClassBindingCodec` is the only byte codec and has no JSON/Jackson fallback：

```java
public final class StorageClassBindingCodec {
    public static final int MAX_ENCODED_BYTES = 64 * 1024;
    public byte[] encode(StorageClassBindingRecord record);
    public StorageClassBindingRecord decode(byte[] bytes, long backendVersion);
}
```

The deterministic `NSB1` layout is big-endian：

```text
4 bytes  magic ASCII "NSB1"
2 bytes  envelopeVersion = 1
2 bytes  flags = 0
4 bytes  payloadLength
payload:
  4 bytes formatVersion = 1
  string  persistenceName        (u32 byte length + strict UTF-8)
  string  canonicalTopicName     (u32 byte length + strict UTF-8)
  string  storageClass           (u32 byte length + strict UTF-8)
  8 bytes bindingGeneration
  string  state enum canonical name
  8 bytes createdAtMillis
  8 bytes stateVersion
  8 bytes metadataVersion = 0
4 bytes CRC32C over every preceding envelope/payload byte
```

`metadataVersion` encodes as zero and decode replaces it with the metadata-store stat supplied as
`backendVersion`。Encoded size is checked before allocation；names reuse the 16-KiB exact-name bound, storage class is
at most 64 UTF-8 bytes and exactly `bookkeeper` or `nereus`，state is one declared enum name, generation starts at one,
times/state version are nonnegative, and payload length must leave exactly one checksum with no trailing bytes。
Wrong magic/version/flags/checksum, malformed UTF-8, overflow or illegal record combination is corruption. Golden
bytes cover every state and a backend-version hydration vector.

The first key component makes namespace-empty checks listable without a cluster-wide binding scan；the second remains
one exact topic key. Neither hash replaces collision validation: decoded canonical namespace、exact persistence name
and canonical `TopicName` must match the request. `formatVersion=1`; generation starts at one and increments only on
`DELETED -> CLAIMED` for a new
topic lifetime; that CAS resets `stateVersion=0` and `createdAtMillis` to the new claim time, while every transition
within the generation increments `stateVersion`. Class is immutable within a generation. `metadataVersion` comes from
the metadata-store stat, not serialized bytes. The record uses a versioned deterministic codec with golden bytes;
unknown major/state, invalid transition, mismatched name/hash or trailing data is corruption.

Allowed transitions are exactly `CLAIMED -> ACTIVE`、`CLAIMED -> DELETING`、`ACTIVE -> DELETING`、
`DELETING -> DELETED` within one generation and `DELETED -> CLAIMED` with generation plus one. The
`CLAIMED -> DELETING` edge aborts a no-longer-valid first-create claim or deletes storage created before activation；
it never hides an active binding. Same-state calls are validated idempotent reads, not new state-version writes.

`prepareStorageClassOpen` runs before choosing either factory. On a missing binding it performs exactly two bounded
get-only probes in parallel：`nereusFactory.inspectStorageState(persistenceName)` and stock
`bookkeeperFactory.asyncExists(persistenceName)`；lookup timeout/failure never means absent and the Nereus probe never
repairs or creates. The decision table is closed：

| BookKeeper exists | Nereus durable state | Missing-binding action |
| --- | --- | --- |
| true | `MISSING` | put-if-absent an `ACTIVE/bookkeeper/generation=1` adoption；then open only if policy also selected BookKeeper |
| false | `MISSING` | if `createIfMissing`, put-if-absent `CLAIMED/{selected}/generation=1`；otherwise not-found with no write |
| true | any non-missing state | invariant failure: two durable class views or a lost binding |
| false | any non-missing state | invariant failure: Nereus projection embeds a binding generation and the binding key is never removed；it cannot be adopted |

Thus a policy selecting Nereus over an existing BookKeeper ledger first restores the actual `ACTIVE/bookkeeper`
binding and then returns `NEREUS_STORAGE_CLASS_MIGRATION_REQUIRED`；selection never changes observed durable truth。
A CAS loser rereads and applies existing-binding rules. Null/default selection is normalized to `bookkeeper` before
this call；only exact `bookkeeper`/`nereus` values can be bound. Non-persistent topics never enter this store.

For an existing binding:

| Binding state | Open behavior |
| --- | --- |
| `CLAIMED` | only the same class/generation may resume creation; the other class is rejected |
| `ACTIVE` | only the same class may open；BookKeeper must exist, while Nereus must be `ACTIVE/SEALED` with exact binding generation；missing selected state is corruption, never empty recreation |
| `DELETING` | no live open is returned; resume the bound class's delete protocol |
| `DELETED` | with create-if-missing, verify old selected state is terminal and the candidate class has no live state, then CAS to the next generation's `CLAIMED`; otherwise not-found |

For a `CLAIMED/nereus` permit，`MISSING` means resume first creation；`ACTIVE/SEALED` is activatable only when the
projection carries the same generation；and a `DELETED` projection is recreatable only when it carries exactly the
previous generation. `DELETING` is completed before any recreation. A same-generation `DELETED` state means the
claim's storage was deleted and the binding must take `CLAIMED -> DELETING -> DELETED` rather than reopen it. For
BookKeeper，exists/missing are respectively activatable/resumable. A selected-class probe failure never falls through
to creation.

Every `CLAIMED`/`ACTIVE` validation also probes the non-bound class get-only。A live non-bound state is an invariant/
migration-required failure before factory IO。A prior-generation Nereus `DELETED` projection is terminal and may
coexist with a newer BookKeeper claim；a Nereus `DELETING` state must finish before the binding can start that new
generation. BookKeeper absence is terminal for a prior deleted BookKeeper generation。

`prepareStorageClassDelete` has its own closed state table and never recomputes class from mutable policy：

| Binding observation | Delete action |
| --- | --- |
| missing + BookKeeper exists + Nereus `MISSING` | adopt `ACTIVE/bookkeeper/generation=1`，then retry preparation |
| missing + both absent | `ManagedLedgerNotFoundException`，no write |
| missing + any Nereus non-missing, or both live | invariant failure；a generation-bearing Nereus projection cannot lose its binding |
| `CLAIMED` + selected storage absent/not yet published | CAS `CLAIMED -> DELETING -> DELETED`，no factory delete |
| `CLAIMED` + selected storage published | CAS to `DELETING`，then call only the bound factory |
| `ACTIVE` | verify bound durable state, CAS to `DELETING`，then call only the bound factory；active-missing is corruption |
| `DELETING` | return the same-generation permit and resume only the bound factory delete |
| `DELETED` | idempotent terminal success；do not call either factory |

For Nereus，factory delete success requires `inspectStorageState == DELETED` with the permit generation before the
binding completes. `DELETING` resumes L0 delete；`MISSING` is corruption. For BookKeeper，post-delete
`asyncExists=false` is terminal. Timeout/unavailability in either verification leaves `DELETING` and is retriable。
No path treats a mutable policy class as delete authority.

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
`storageClassBindingGeneration` against it, and passes `permit::validateBeforeProjectionPublish` as the mandatory
metadata-store `ProjectionPublishGuard`。The projection store awaits it after allocator CAS and immediately before a
new topic put/CAS under the same operation deadline. The production factory requires a non-null guard from
`NereusRuntimeContext`; unit tests pass an explicit
fake. There is no implicit allow-all production constructor.

### 5.3 Namespace policy/first-create serialization

F2 has no cross-storage migration protocol, so a scan followed by a policy write is insufficient：another broker can
first-create a BookKeeper topic between them. `NamespaceStorageClassPolicyGuard` uses the target
`CoordinationService.getLockManager(...)` and a namespace-scoped lock path：

```text
/managed-ledger-storage-policy-locks/v1/
  {sha256-base32("pulsar-storage-policy-lock-v1\0" + exactNamespaceNameUtf8)}
```

```java
public record NamespaceStorageClassLockData(
        String brokerId,
        String operationId,
        long acquiredAtMillis) {
}

public interface NamespaceStorageClassPermit {
    NamespaceName namespace();
    String selectedStorageClass();
    long namespacePolicyVersion();
    CompletableFuture<Void> validateBeforeFactoryOpen(StorageClassOpenPermit bindingPermit);
    CompletableFuture<Void> closeAsync();
}

public final class NamespaceStorageClassPolicyGuard implements AutoCloseable {
    public CompletableFuture<NamespaceStorageClassPermit> acquireFirstCreate(
            NamespaceName namespace, TopicName topic, String selectedStorageClass);

    public CompletableFuture<Void> updateNamespaceStorageClass(
            NamespaceName namespace, String targetStorageClass, long expectedPolicyVersion);
}
```

The guard obtains one `LockManager<NamespaceStorageClassLockData>` from
`CoordinationService.getLockManager(NamespaceStorageClassLockData.class)` and borrows it for broker lifetime。Lock
data is diagnostic only：nonblank current broker ID、random operation ID and nonnegative clock time；it never selects
storage or replaces the binding/policy rechecks. `LockBusyException` retries with capped backoff inside the one
metadata deadline；session loss or deadline fails closed。

Every capable broker routes **both BookKeeper and Nereus first durable creation** through `acquireFirstCreate`。It
acquires the namespace resource lock within the metadata deadline, reloads one authoritative namespace policy,
requires its selected class/version to match the open snapshot, then lets the caller publish at most one
`CLAIMED` binding while retaining the lock. `validateBeforeFactoryOpen` reloads the policy and binding immediately
after that claim and before either factory can create storage. On mismatch it takes the no-storage claim through
`CLAIMED -> DELETING -> DELETED` and returns a retryable policy-change error. The permit is released before slow
factory IO；close is idempotent and a leaked process lock is session-released.

`updateNamespaceStorageClass` first runs `requireClusterReady` when the target is Nereus, acquires the same lock,
reloads the expected policy version, and lists existing persistent topics plus non-`DELETED` binding records for the
namespace. Any result rejects before policy mutation. Otherwise it CAS-writes the policy, reloads it, and repeats the
topic/binding check before success. A split/stale lock owner can at most publish a no-storage `CLAIMED` record：its
mandatory post-claim policy validation aborts that record before factory open, and the updater waits/retries the
post-write scan within the same deadline. Timeout leaves the actual policy as read-back truth and reports failure；it
does not pretend rollback or select a fallback class.

The namespace-hash key prefix makes the binding check local to one namespace。Child count above
`nereusMaxNamespaceBindingScanEntries` or any child decode/get failure rejects the policy update；it never assumes an
unscanned suffix is deleted. Record reads use at most `nereusMaxBindingPendingOperations` concurrency and one shared
deadline。

A topic-level storage class may be set before its first durable ledger exists, but changing the effective class of an
existing live topic is rejected before policy mutation. Namespace changes are accepted only under the empty-namespace
protocol above；otherwise operators use per-topic selection for not-yet-created topics. Same-value writes are
idempotent. Topic deletion must reach the selected storage's terminal delete before a newly created lifetime can
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

### 5.4 Consumer acknowledgement boundary

F2 does not claim general Pulsar Consumer acknowledgement. The one-argument locked overload remains a pure delegate；
`NereusAcknowledgeValidator` is invoked as the first executable line of
`Consumer.messageAcked(CommandAck, boolean)`，before `lastAckedTimestamp`、ack-property
collection、pending-ack lookup、transaction helpers、cursor mutation or ack counters：

```java
public final class NereusAcknowledgeValidator {
    public static Optional<NotAllowedException> rejection(
            Subscription subscription,
            SubType subscriptionType,
            CommandAck ack,
            boolean requirePersistedAck);
}
```

It is a no-op unless the `PersistentSubscription` cursor's managed ledger is Nereus. For Nereus it admits exactly this
shape：cursor `isDurable()==false`；subscription type `Exclusive` or `Failover`；`AckType.Cumulative`；one message ID；
neither transaction bit present；zero AckSet words；no batch index/batch size；no validation-error marker；and
`requirePersistedAck==false`。The ledger ID/entry ID are then validated by the projected cursor when local cumulative
mark-delete executes. Ack properties may be passed to that local call but remain broker-local and are never described
as recovered state.

For this admitted Nereus shape，the cumulative branch retains the returned
`subscription.acknowledgeMessageAsync(...)` future and derives `1L` only after its local completion even though
`requirePersistedAck=false`；it does not use the stock fire-and-forget/completed-future shortcut. This wait means
ack counters follow successful local cursor mutation，not that the position survived restart. BookKeeper keeps its
locked timing behavior unchanged。

Individual/multi-position ack、partial or whole batch-index ack、transaction ack、durable cursor ack、Shared/
Key_Shared ack and a caller requesting persisted-ack confirmation fail with
`NotAllowedException("NEREUS_UNSUPPORTED_ACK:<reason>")`。A present rejection is immediately converted with
`FutureUtil.failedFuture`；no branch logs-and-returns a
successful future. `messageAckRate`、`messageAckCounter`、unacked counts、pending-ack maps and cursor state are asserted
unchanged on rejection. This is why F2 acceptance says Reader plus limited non-durable consumer, not durable Pulsar
subscription semantics；F3 replaces this gate only after its cursor contract is implemented.

### 5.5 Exact-recovery fence handoff to PersistentTopic

The target generic `PersistentTopic.addFailed` fences/disconnects producers，then
`decrementPendingWriteOpsAndCheck()` calls `ledger.readyToCreateNewLedger()` and `unfence()` as soon as pending writes
reach zero. For Nereus this stock transition is legal only when no exact-recovery fence exists.

```java
public record NereusWriteFenceCompletion(
        long generation,
        NereusWriteFenceResolution resolution,
        Throwable failure) {
}

public final class NereusWriteFenceBridge implements AutoCloseable {
    public boolean deferAutoUnfenceIfNeeded(
            NereusWriteFenceView view,
            Executor topicOrderedExecutor,
            Consumer<NereusWriteFenceCompletion> completion);
    public void close();
}
```

Exactly one of completion `resolution/failure` is non-null；generation is positive. The bridge only captures/coalesces
the current snapshot, joins `awaitWriteFence(generation)` and schedules this immutable completion；it never decides
topic state itself. The boolean is false only when the view has no current fence；true means the generation was newly
attached or was already coalesced。Each Nereus `PersistentTopic` owns one bridge and closes it with the topic；
BookKeeper topics allocate none。

`decrementPendingWriteOpsAndCheck()` remains byte-for-byte stock for non-Nereus ledgers. For Nereus，inside the same
topic synchronization and before dedup reset/`readyToCreateNewLedger`，it obtains `currentWriteFence()`。Empty follows
the stock action；nonempty cancels `cancelFencedTopicMonitoringTask()`，calls the bridge with the topic ordered
executor and returns without unfencing. The bridge coalesces by generation and joins `awaitWriteFence(generation)`
without blocking a broker thread.

`PersistentTopic.onNereusWriteFenceCompletion` receives completion on its ordered executor and synchronizes on the
topic. It may reset dedup、call `readyToCreateNewLedger` and `unfence` only when the topic is still fenced/not closing,
`pendingWriteOps==0` and a fresh fence read is empty。
If a higher generation is present, it attaches that generation and the stale completion does nothing. Both
`COMMITTED` and `PROVEN_NOT_COMMITTED` authorize this check；neither emits a second producer callback. Exceptional
permanent recovery calls the topic's close/unload path and never unfences it；broker/runtime shutdown may close the bridge but
bridge close only detaches its continuation and never cancels the core recovery future。Shutdown cannot report a
safely retryable append outcome. Rejected publishes while fenced may drive pending count back to zero,
but they join the same generation instead of creating duplicate waiters.

| Broker feature | F2 admission |
| --- | --- |
| ordinary persistent user topic | allowed |
| existing topic switched between BookKeeper and Nereus | rejected; no F2 migration contract |
| ordinary non-transactional producer, batching/chunking, client encryption | allowed; entry bytes remain opaque |
| Reader, read-only cursor, non-durable Exclusive/Failover consumer | allowed |
| non-durable consumer acknowledgement | only the cumulative one-position whole-entry shape in 5.4；batched-entry ack is rejected even though producing/Reader-reading opaque batches is allowed |
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
  -> if createIfMissing: namespacePolicyGuard.acquireFirstCreate(namespace, topic, selected)
  -> migrationGuard.prepareStorageClassOpen(topic, exact persistence name, selected class, createIfMissing)
  -> namespace permit validateBeforeFactoryOpen(binding permit), then close permit in finally
  -> managedLedgerStorage.getManagedLedgerStorageClass(selected class)
  -> selected factory asyncOpen(exact persistence name, config, ...)
  -> migrationGuard.completeStorageClassOpen(exact permit)
```

`createIfMissing=false` performs no durable create and does not need the namespace lock. A create-if-missing load takes
the permit even when the binding later proves active；this keeps the code path race-free without an unlocked
"probably exists" probe. Permit acquisition/revalidation failure closes the permit and never calls either factory。
`prepareStorageClassOpen` reuses the exact storage class/version captured by the permit and cannot substitute a newer
policy snapshot mid-call.

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
- the S3 provider uses one release-pinned AWS SDK v2 BOM with `software.amazon.awssdk:s3` and
  `netty-nio-client`；no dynamic version、SDK-v1 `AmazonS3` client or CRT native transport is allowed；the selected
  numeric version is recorded in the Nereus version catalog/lockfile changed by F2-M5；
- no `mavenLocal()` or floating snapshot is used by release builds；
- composite-build mode verifies both repository commits before compiling integration tests。

Rollout is two-step: every broker that can own the namespace must first run the hybrid provider/binding guard while
policies still select BookKeeper; only after that cluster-wide convergence may an operator enable `nereus` for new
topics. An older broker can bypass the binding protocol, so mixed-version ownership during Nereus policy enablement is
explicitly unsupported and blocked by the exact `nereus.storage-binding-protocol=1` lookup-data gate. Binary downgrade
or protocol-0 rejoin is forbidden while Nereus policy/binding state exists.

## 8. Broker Integration Tests

In addition to `05`, F2-M5 requires:

1. reflection construction through `ManagedLedgerStorage.create(...)` and the thread context classloader；
2. exact `[bookkeeper,nereus]` order, null/default lookup and unknown-name behavior；
3. one stable Nereus factory returned by repeated storage-class calls；
4. partial initialize failure closes ObjectStore/Oxia/executors and prevents broker start；
   creation-guard-before-factory construction works, while missing/null/double binding-store factory attach fails；
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
18. BookKeeper-only and Nereus topics coexist without virtual ledger IDs appearing in BookKeeper mocks/logs；
19. a rollout gate refuses Nereus policy enablement while any namespace-owning broker lacks the binding-capable
    version；existing BookKeeper topics are adopted without data movement；
20. the capability property is absent before complete hybrid initialization, present with exact value `1` in
    registered `BrokerLookupData` afterward, missing/double registry attach fails, it cannot be spoofed through
    configured lookup properties, and is checked
    across every active persistent-topic broker；
21. namespace policy update races 100+ BookKeeper/Nereus first creates under multi-broker coordination；the result has
    one effective policy and no conflicting live binding/storage, including lock-owner crash and stale-owner cases；
22. missing binding plus BookKeeper live adopts generation 1，whereas any missing-binding Nereus projection is
    corruption；all `CLAIMED/ACTIVE/DELETING/DELETED` open/delete restart paths and `NSB1` golden/corrupt bytes are
    covered；
23. S3 tests assert conditional PUT sends `If-None-Match:*` and maps 412, ETag is never checksum, HEAD metadata and
    positive/zero ranges obey section 3.2, deadlines cancel SDK futures, CRC mismatch fails closed, and production
    rejects local/file providers；
24. every rejected ack shape leaves timestamp/rate/counter/pending-ack/cursor state unchanged；the one admitted local
    cumulative whole-entry ack advances only the non-durable cursor，and its counters wait for local async completion/
    remain unchanged on a close-race failure；
25. retryably uncertain add failure remains topic-fenced after pending writes drain, stale generation cannot unfence,
    exact committed/proven-not-committed terminal can unfence once, and permanent recovery closes/unloads；
26. `PersistencePolicies.equals/hashCode` treats equal-content distinct storage-class Strings as equal；
27. bootstrap completes before `brokerId` exists and two process starts produce distinct 128-bit-or-more process IDs
    and exact `pulsar-f2/{processRunId}` writer IDs。

## 9. Release Claim Boundary

`phase2FinalCheck` proves ManagedLedger facade integration over real Oxia and an S3-compatible ObjectStore provider.
It does not claim F3 cursor durability, F4 compaction/offload, F8 transactions/system-topic bootstrap or transparent
migration of existing BookKeeper topics. Release notes and metrics must use the same boundary.
