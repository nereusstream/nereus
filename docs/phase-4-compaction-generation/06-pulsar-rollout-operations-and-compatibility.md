# Pulsar Integration, Rollout, Operations, and Future Compatibility

## 1. Compatibility Goal

Phase 4 changes where committed bytes are read and when physically unreferenced bytes are reclaimed. It must not
change what Pulsar clients observe：

```text
managedLedgerName -> projection incarnation -> virtualLedgerId -> entry offset -> batch index
```

Generation、object id、task id、reader lease and recovery checkpoint never enter `Position` or `MessageId`.

The local Pulsar source remains the only compatibility oracle for this phase. Phase 4 does not adapt a released
`5.0.0-M1` binary or resolve a published M1 snapshot.

## 2. Target and Incremental Nereus Runtime Changes

### 2.1 Typed configuration

`NereusRuntimeConfiguration` becomes：

```java
public record NereusRuntimeConfiguration(
        OxiaClientConfiguration oxia,
        ObjectStoreConfiguration objectStore,
        StreamStorageConfig streamStorage,
        NereusManagedLedgerFactoryConfig managedLedger,
        ProjectionMetadataStoreConfig projectionMetadata,
        CursorMetadataStoreConfig cursorMetadata,
        CursorStorageConfig cursorStorage,
        MaterializationConfig materialization,
        PhysicalGcConfig physicalGc,
        NereusRetentionConfig retention) { }
```

The product-only retention lane config is：

```java
public record NereusRetentionConfig(
        int statsScanPageSize,
        int maxConcurrentPlans,
        int maxQueuedPlans,
        Duration operationTimeout,
        Duration closeTimeout) { }
```

Its page size is positive and `<= 512`；concurrency/queue limits and timeouts are positive. There is at most one
executing plan per stream；duplicate housekeeping triggers coalesce, while explicit admin futures retain independent
completion. Policy retention values are not copied into this config—they come from the versioned topic feature
snapshot on every attempt.

Checkpoint AG implements and validates this value type with defaults `128 / 8 / 1024 / 30s / 30s`, together with the
exact policy snapshot、candidate/token values and planner/service contracts. The planner currently enforces the page
and operation-timeout bounds. Checkpoint AH implements the shared per-stream coalescing lane and runtime composition
that enforce concurrency/queue/whole-operation-timeout/close bounds. The Pulsar product defaults are deliberately
`512 / 4 / 1024 / 60s / 120s` and are mapped from five restart-required broker fields；the product-neutral defaults
remain unchanged for standalone/test composition.

`NereusBrokerStorageConfiguration` builds `MaterializationConfig.committedPolicy` through
`MaterializationPolicyFactory.losslessCommitted(...)`. The factory fixes id `nereus-committed-default`, view/task/
format and empty topic-compaction spec, and copies the validated minimum-merge/source/range/object/row-group/
compression values. Its positive `policyVersion` is the low 63 bits of SHA-256 over the canonical operator policy
fields (zero maps to one)；the full `policySha256` still hashes the complete final record. Thus a
semantic config change deterministically creates a new task identity without a manual version knob.

Cross-validation additionally requires：

- generation/task/object metadata pending limits fit the shared Oxia limit；
- F4 metadata payload/page limits equal the store hard limits；
- materialization registry/physical-root shard counts equal fixed 64/256 protocol values, page values fit their hard
  limits and scan intervals are positive；
- object-store guarded-PUT attempts/backoffs satisfy their hard bounds, and the one request timeout contains the
  complete retry loop while fitting worker、reader lease and GC operation deadlines；
- reader lease covers read timeout + maximum clock skew；
- GC drain grace covers reader lease expiry + skew；
- pending-protection duration covers the longest checkpoint/snapshot publication operation + skew and remains below
  orphan grace；
- tombstone audit grace is overflow-safely at least metadata-audit grace plus orphan grace and strictly exceeds the
  maximum append session/recovery terminal, worker claim, reader lease, pending protection and guarded object-PUT
  deadline plus maximum clock skew；
- materialization worker reads fit core object-read concurrency/buffer limits；
- committed-policy target/hard object bytes are at least the maximum legal generation-zero slice/object contribution,
  so the whole-index planner never needs to split one L0 source merely to make forward progress；
- recovery checkpoint max chain/scan values match L0 anchor-aware repair；
- GC delete cannot be enabled unless generation capability and every reference domain are enabled；
- async profile lag thresholds fit `long` and reject > throttle；
- retention lane page/concurrency/timeout values fit the generation-store and managed-ledger close limits；
- managed-ledger close timeout covers admitted retention callbacks, while process runtime shutdown separately drains
  materialization/GC according to its own longer deadline。

### 2.2 Runtime ownership

`DefaultNereusRuntimeProvider` constructs in this order：

```text
shared Oxia runtime
L0/projection/cursor/generation/physical-object metadata stores
focused source-retirement and object-audit-retirement Oxia adapters
ObjectStore
ObjectReadPinManager + ObjectProtectionManager + ReadTargetReaderRegistry
GenerationZeroPhysicalReferencePublisher
StreamStorage with protected two-stage append + generation-aware resolver
ManagedLedgerGenerationProtocolActivationGuard + reference-domain instances
MaterializationRuntime (initially inert until capability ready)
CursorSnapshotStore with pin/protection hooks
CursorStorage/retention coordinator
NereusManagedLedgerRetentionService
NereusManagedLedgerRuntime
```

Checkpoint V implements the cursor-snapshot subset of this target composition：the provider shares the physical
metadata store、`DefaultObjectProtectionManager` and `DefaultObjectReadPinManager` with
`DefaultCursorSnapshotStore`，and `NereusManagedLedgerRuntime` owns/exposes the read-pin manager and closes the
read-pin/protection/physical triplet in dependency order。Generation activation guard、reference-domain registry、
backfill、`MaterializationRuntime` and GC service composition remain later checkpoints；this partial wiring does not
enable any deletion capability bit。

Checkpoint X adds the shared `OxiaJavaGenerationMetadataStore` and
`DefaultManagedLedgerMaterializationRegistrationCoordinator` to this production ownership graph.
`NereusManagedLedgerRuntime` exposes only the broker-safe registration coordinator and closes the generation store
before projection/L0/shared Oxia resources. It still does not construct the generation activation guard、
materialization/GC services or enable any capability bit.

Checkpoint AB constructs `ManagedLedgerGenerationProtocolActivationGuard` from the shared projection、L0、
generation and activation stores, the exact six-domain runtime set, the projection-generation reference domain and
the broker readiness provider. `NereusManagedLedgerRuntime` owns/exposes the typed guard. Construction itself does
not advance cluster activation、start materialization/GC or enable a delete bit.

Checkpoint AC constructs `DefaultManagedLedgerGenerationProtocolActivationCoordinator` from that same activation
store、exact domain set and readiness provider. `NereusManagedLedgerRuntime` exposes only the typed
`activatePublication()` boundary；the broker never imports or mutates `GenerationProtocolActivationStore`. The
coordinator owns the publication-only ACTIVE CAS and still does not start materialization/GC or enable a delete bit.

Checkpoint AF constructs and owns the coupled Object-WAL resolver、generation-aware read/recovery repair、lag reader
and bounded materialization service as `Phase4ObjectWalRuntime`, then installs its exact read/admission seams into
`DefaultStreamStorage`. It starts before the broker storage is returned and closes before generation/Oxia/ObjectStore
resources. This composition still does not install physical GC.

Checkpoint AG implements `DefaultRetentionCandidatePlanner` and `NereusManagedLedgerRetentionService` as tested
product-neutral components. Checkpoint AH adds `NereusRetentionRuntime` to this ownership graph after cursor storage/
retention construction. It owns the process-wide bounded lane and creates a policy-provider-backed service for every
writable `NereusManagedLedger`; runtime close drains it before cursor/generation/Oxia resources. The facade override
routes `trimConsumedLedgersInBackground` through this runtime and completes after F3 durable logical trim. Checkpoint
AI installs the exact effective topic `RetentionPolicySnapshot` from the same immutable policy-input tuple, activates
and revalidates registration-backed generation authority before an F4-mutating policy becomes visible, reloads the
policy tuple after activation, and admits loaded/unloaded `TRIM_TOPIC` only with generation readiness. The retention
service still repeats activation、ownership、policy and planner revalidation before delegating the only mutation to F3.

On construction failure it closes exact reverse order. Product close first rejects ledger opens, closes all loaded
ledgers/cursors, stops materialization/GC, then closes metadata/object/executors. A worker is never allowed to outlive
the ObjectStore it uses.

`DefaultStreamStorage`/`AppendCoordinator` receives the shared `GenerationZeroPhysicalReferencePublisher` as a
required constructor dependency. There is no nullable/no-op production path once the F4 binary is active；before
physical deletion activation it still registers/protects every new sync append so the later backfill proof has a
closed write frontier. Test-only no-op construction is forbidden by the runtime-composition test.

`NereusManagedLedgerRuntime` gains read-only accessors for：

```java
MaterializationService materializationService();
NereusManagedLedgerRetentionService retentionService();
GenerationProtocolActivationGuard generationProtocolActivationGuard();
ObjectReadPinManager objectReadPinManager();
```

Broker classes receive no raw metadata/object store and cannot bypass these services.

`MaterializationRuntime` starts its registered-stream scanner only after every store、reader、guard and reference
domain has been constructed. It scans independently of broker topic ownership；topic unload therefore stops facade
callbacks but not durable task recovery.

The interface type lives in `nereus-core`; `ManagedLedgerGenerationProtocolActivationGuard` lives in
`nereus-managed-ledger` and is constructed before `MaterializationRuntime` from the projection/generation stores plus
an injected broker-cluster-readiness callback. This preserves
`pulsar-adapter -> {managed-ledger, materialization} -> core` without a managed-ledger/materialization dependency
cycle.

### 2.3 Profile creation

`TopicProjectionRecord.storageProfile` admits exactly：

```text
OBJECT_WAL_SYNC_OBJECT
OBJECT_WAL_ASYNC_OBJECT
```

Phase 4 still rejects every BookKeeper profile. The selected profile is immutable for one topic incarnation. Existing
topics remain sync；`nereusDefaultStorageProfile` affects only first create. Changing the broker default is not a topic
migration and does not rewrite existing records.

## 3. Durable Topic Generation Marker

### 3.1 Marker contract

Cluster broker capability alone cannot prevent an old binary from loading a topic after higher-generation records
exist. Phase 4 therefore composes a monotonic marker into the authoritative F2 topic projection：

```java
public final class ManagedLedgerGenerationProtocol {
    public static final String PROPERTY = "nereus.generation-protocol";
    public static final String VERSION_1 = "1";

    public static boolean isActivated(TopicProjectionRecord record);
    public static Map<String, String> activate(Map<String, String> durableProperties);
}
```

`ManagedLedgerCursorProtocol.canonicalDurableProperties` is refactored into a composed
`ManagedLedgerProtocolProperties` validator that admits exactly the cursor and generation marker/version pairs and
continues rejecting every other `nereus.*` external property. External property reads hide both markers；external
replace preserves both.

The locked F3 decoder rejects unknown `nereus.generation-protocol`, so an F3-only broker fails closed before exposing
an activated topic. New code provides：

```java
ManagedLedgerProjectionMetadataStore.activateGenerationProtocol(
        cluster, managedLedgerName, expectedProjectionIdentity, expectedMetadataVersion);
```

It preserves projection identity、incarnation、virtual ledger、cursor marker and external properties, and changes only
the monotonic generation marker through one topic-projection CAS.

Checkpoint K implements this property/CAS foundation in the shared F2 metadata path. The composed validator hides and
preserves both cursor/generation markers, rejects unknown internal keys, and `activateGenerationProtocol` converges a
lost CAS response only by reloading the exact topic identity. It also adds
`getProjectionByStream(cluster, streamId)`：the store reads the per-stream binding and then the current topic selected
by that binding, returning each canonical key、Oxia version and exact stored-envelope SHA-256. That lookup is consumed
by the M4 projection reference domain and performs no topic load or repair.

This is not cluster activation. `GenerationProtocolActivationRecord`、the registration coordinator/barrier、broker
capability guard and runtime call sites below remain M5 work, so no production path sets the marker merely because the
API exists. Checkpoints S、X–AA later add those durable prerequisites；checkpoint AB finally constructs the guard,
and checkpoint AC adds proof-gated publication-only cluster activation. No checkpoint through AC installs a topic
mutation caller.

### 3.2 Resolvable projection reference and registration ordering

Background workers must be able to validate a projection without an owned/loaded topic. The managed-ledger adapter
therefore defines an opaque but reversible core `ProjectionRef`：

```java
public record ManagedLedgerGenerationProjectionRefV1(
        String managedLedgerName,
        ManagedLedgerProjectionIdentity identity) {
    public ProjectionRef toProjectionRef();
    public static ManagedLedgerGenerationProjectionRefV1 from(ProjectionRef ref);
    public Checksum projectionIdentitySha256();
}
```

`toProjectionRef()` uses `ProjectionType.VIRTUAL_LEDGER` and value
`nereus-ml-v1.` + base64url-without-padding of canonical binary bytes：

```text
4 bytes  "NPR1"
u32 + UTF-8 managedLedgerName
i64      storageClassBindingGeneration
i64      incarnation
u32 + UTF-8 streamId
i64      virtualLedgerId
u32      CRC32C of all preceding bytes
```

Decode uses strict UTF-8、exact lengths/no trailing bytes、the existing managed-ledger-name validator and complete
`ManagedLedgerProjectionIdentity` validation. The identity digest is SHA-256 of the exact NPR1 bytes including CRC；
the guard decodes the exact name, computes its topic key, linearly reads `TopicProjectionRecord` and compares every
identity field. The ref remains stable across mutable property/state-version updates but changes on topic recreation.

Checkpoint W implements the strict NPR1 codec and freezes a golden ref/digest vector. It also adds the protocol-neutral
`GenerationProjectionAuthorityReader`/snapshot contract in core and
`ManagedLedgerGenerationProjectionAuthorityReader` in the managed-ledger adapter. The reader decodes and verifies the
subject digest, linearly reads the per-stream binding plus the topic authority selected by that binding, and reports
`live=true` only for the exact identity in `OPEN` or `SEALED`. Missing、recreated、`DELETING` or `DELETED`
projections return a versioned non-live classification whose binding/topic presence or absence authorities must still
match at final revalidation. This reader is consumed by the checkpoint-W physical-root backfill. Checkpoints X–AA
later add the broker-side registration executor、cluster barrier and durable registration proof；checkpoint AB adds
the production activation guard itself，and checkpoint AC adds publication-only cluster ACTIVE orchestration after
the durable proof. Mutation call sites remain rollout work.

Checkpoint X implements the registration half of this ordering. `ProjectionIdentity.encode` is now the single
canonical length-delimited encoder used by durable materialization records and registration. The managed-ledger
coordinator captures exact current projection identity plus a full L0 snapshot, creates/verifies the NPR1
registration, monotonically refreshes the commit-version hint with version CAS, repairs a lost CAS response only from
an exact reload, and finally rereads projection/L0/registration. Mutable projection property CAS is compatible；
recreation、non-live state or profile/payload drift fails closed. The topic open/recreate path waits for this final
revalidation before returning the ledger result. Marker activation and broker readiness remain pending.

First activation is a recoverable cross-key sequence：

```text
load authoritative projection P and L0 stream S
derive ref/digest from P; require P.streamId == S.streamId and profile agreement
create-or-verify MaterializationStreamRegistrationRecord R
pass cluster first-activation barrier
CAS P -> P+generation marker
reload P and R; require same ref/digest/profile and marker=1
only now publish the loaded topic/policy snapshot or admit async append/F4 mutation
```

Registration-before-marker leaves only a harmless discovery hint if the marker CAS fails. Marker response loss is
resolved by reloading both keys. A marker without the matching registration is treated as incomplete activation：open
repairs `R` before exposing the topic, while background mutation and async admission fail closed. Property updates do
not need to rewrite `R` because its ref/digest/profile are incarnation-stable；every operation guard resolves the
current projection and captures its current metadata version in `GenerationActivationProof.subjectValidationVersion`.
The registration's commit-version hint may be refreshed opportunistically and is never a skip boundary.

Activation is required before：

- creating even a `PREPARED` higher-generation index in the ordinary namespace；
- enabling `OBJECT_WAL_ASYNC_OBJECT` success for that topic；
- completing policy/admin logical trim through the F4 path；
- creating a recovery checkpoint or retiring source/index/object bytes for any live projection that can address the
  affected stream；
- publishing a topic-compacted storage primitive。

Activation itself changes no bytes、index、trim or task state.

GC of an authoritatively deleted/unaddressable old incarnation or a proven ownerless orphan does not invent a topic
marker after deletion. It instead requires cluster deletion activation plus the
`projection-generation-v1` reference-domain proof defined in document 05. Multi-stream objects require marker or
deleted-incarnation proof for every stream slice.

### 3.3 Cold-topic registration backfill

Checkpoint X makes every Nereus topic create/open/recreate create or verify its registration even while generation
publication is disabled, closing the concurrent new-topic frontier. Checkpoint Z now covers existing unloaded topics
with the explicit idempotent broker traversal below；Checkpoint AA now consumes an eligible report through the
product-owned durable coverage-proof CAS：

```java
public record GenerationRegistrationBackfillRequest(
        String runId,
        long expectedBrokerReadinessEpoch,
        int maxConcurrency,
        Duration timeout) { }

public enum GenerationRegistrationBackfillStage {
    TENANT_LIST, NAMESPACE_LIST, TOPIC_LIST,
    BINDING_READ, PROJECTION_READ, REGISTRATION_WRITE
}

public record BackfillFailure(
        String resourceIdentitySha256,
        GenerationRegistrationBackfillStage stage,
        String errorCode) { }

public record GenerationRegistrationBackfillReport(
        String runId,
        long brokerReadinessEpoch,
        long tenantsScanned,
        long namespacesScanned,
        long persistentTopicsScanned,
        long nereusProjectionsRegistered,
        long deletedOrNonNereusSkipped,
        long failureCount,
        Checksum coverageSha256,
        List<BackfillFailure> boundedFailures) { }

public interface NereusGenerationRegistrationBackfill {
    CompletableFuture<GenerationRegistrationBackfillReport> run(
            GenerationRegistrationBackfillRequest request);
}
```

Run id is random 128-bit lowercase base32；readiness epoch/counters are non-negative；concurrency and timeout are
positive. `boundedFailures.size() == min(100, failureCount)` in canonical traversal order, identities are lowercase
SHA-256 (never raw tenant/topic names) and error codes are stable bounded machine values. Report digest hashes every
visited canonical resource identity、classification and registration result, not just the retained failures.

The implementation uses the locked local-source methods `TenantResources.listTenantsAsync()`、
`NamespaceResources.listNamespacesAsync(tenant)` and `TopicResources.listPersistentTopicsAsync(namespace)` in sorted
order. It does not load/own topics. For every non-system persistent topic/partition, it linearly reads storage binding
and the exact Nereus projection；a live Nereus incarnation is registered through the coordinator from 3.2, while
deleted/non-Nereus topics are counted and skipped. Concurrency and one operation deadline are bounded；failure details
are capped at 100 and the digest/counters cover the full canonical traversal. It processes one namespace result at a
time under the broker's existing topic-list memory limiter/size-cache pattern and incrementally hashes names；it never
holds an all-cluster topic list.

The run starts and ends under the same two-snapshot broker readiness value, which proves the new-topic registration
hook was active throughout. Checkpoint Z compares the complete value, including full broker-set SHA and broker count,
not only its 63-bit epoch. Any list/read/register failure、readiness change or nonzero failure count keeps the report
ineligible for `streamRegistrationBackfill`. Process loss simply reruns from the beginning；same-key create-or-verify
is idempotent. The broker traversal creates no topic marker and does not own the activation record.

Checkpoint AA adds `GenerationRegistrationBackfillCompletion` as the product boundary. After the final full-readiness
comparison, only a zero-failure report is submitted, within the same whole-run deadline. The product coordinator
reacquires exact readiness before CAS, compares epoch/full digest/broker count, and installs the proof through the
shared-Oxia activation store. Same-epoch completed coverage is immutable；a rerun with another run id and the same
coverage converges. A newer readiness epoch resets physical-root/cursor-snapshot proof epochs to incomplete and clears
the old object-store capability；deletion-enabled authority rejects standalone refresh. CAS response loss and
condition conflicts reload the durable value. Final cached-readiness plus durable-proof revalidation is mandatory.
An old broker joining therefore invalidates readiness immediately；the durable old-epoch proof remains harmless and
the activation guard must block F4 mutations until all required proofs are refreshed under a stable capable epoch.

The product boundary captures an immutable
`ManagedLedgerMaterializationRegistrationCandidate(managedLedgerName, storageClassBindingGeneration, projectionIdentity,
projectionIdentitySha256)` from the exact live `OPEN`/`SEALED` projection before crossing into the broker. The
candidate validates the strict NPR1 identity/digest and the broker re-reads storage binding after registration；
binding generation/class drift is a failure, while deletion after the idempotent write is a counted skip. Pulsar
attaches the traversal to `NereusManagedLedgerStorage` from `BrokerRegistryImpl`, with defaults of 16 concurrent topic
operations and a 3600-second whole-run deadline. Canonical topic batches are folded in order, all failures contribute
to the digest, only the first 100 redacted failures are retained, and the frozen two-topic fixture coverage SHA is
`2f234d6b9baa3a760460090850d22734f94cd72d51fd0f27706fda272fc01d7c`.

## 4. Broker Capability

### 4.1 Lookup property

The local fork adds：

```java
public final class NereusGenerationProtocolCapability {
    public static final String PROPERTY = "nereus.generation-protocol";
    public static final String VERSION = "1";
}
```

`NereusStorageBindingCapability.requireUnreserved` composes binding -> cursor -> generation reserved-property checks.
`NereusBrokerCapabilityCoordinator.decorateLookupProperties` publishes all three after local runtime construction.

### 4.2 Cluster barrier

```java
CompletableFuture<Void> requireGenerationClusterReady();
```

It executes the existing two-stable-snapshot algorithm over all persistent-topic brokers and requires exact versions
for binding、cursor and generation protocols. The broker set must be identical across snapshots and local registry
must remain started/registered.

`GenerationProtocolActivationGuard.requireReady(..., activateLiveProjectionIfAbsent=true)` calls this barrier and
then checks the durable cluster activation/domain-set record、registration and marker. With `false`, it decodes the
subject ref, reloads the current projection by exact name, compares the immutable digest and captures the current
projection metadata version. It is called for every task/index publication、recovery-checkpoint、live-stream
retirement、logical-trim and delete attempt；it also checks the current local runtime/domain registry, activation bits
and a broker-set capability readiness epoch invalidated by membership/property changes. `revalidate(proof)` repeats
those subject/version checks immediately before the mutation CAS. First activation requires the full two-stable-
snapshot barrier. Subsequent operations may use the coordinator's validated cached epoch, but an old/incapable broker
joining invalidates it and blocks new F4 mutations until two stable capable snapshots converge again.

Checkpoint AB implements this product half. It requires the durable cluster record to be `ACTIVE` with publication
enabled、the current readiness epoch、complete registration proof and an exact canonical six-domain set. Live
subjects are verified against strict NPR1 projection、L0 and materialization-registration truth；the first monotonic
topic marker is allowed only while `nereusGenerationProtocolEnabled=true`, and a lost marker response converges only
after exact reload. The returned proof freezes topic/cluster metadata versions、readiness epoch、domain-set digest and
capability bits；`revalidate` reloads them immediately before the caller's mutation CAS. Physical delete additionally
requires current delete bits/backfill/object-store proof and an exact `projection-generation-v1` snapshot. The switch
defaults to false and does not itself move the cluster activation record from `PREPARED` to `ACTIVE`.

Checkpoint AC implements that missing cluster transition through a separate product-owned coordinator. When the
explicit switch is true, it requires current exact readiness、the same durable readiness epoch、a completed
same-epoch registration proof and the exact six-domain set before its only write：
`PREPARED -> ACTIVE(publication=true, physicalDelete=false, cursorSnapshotDelete=false)`. Condition conflicts retry
from the reloaded PREPARED record；a concurrent success or lost response converges only from a valid durable ACTIVE
record. Cached readiness and durable authority are revalidated after CAS. A final readiness drift fails the caller,
while the old-epoch durable ACTIVE record remains harmless because the checkpoint-AB guard rejects it.

Checkpoint Y implements the broker half of this contract in the locked local fork. Lookup decoration now publishes
the reserved binding、cursor and generation properties together. `requireGenerationReadiness()` filters to
persistent-topic brokers, validates all three exact versions, and compares two full snapshots rather than only their
key sets. Its canonical SHA-256 input is domain-separated and length-prefixed；brokers are sorted by registry key and
each contributes that key、the advertised `BrokerLookupData.brokerId`、`startTimestamp` and sorted required
property/value pairs. The bounded V1 `brokerReadinessEpoch` is the non-negative first 63 digest bits；the complete
lowercase digest and broker count remain available for exact comparison.

The coordinator registers a broker-registry listener before readiness is used. Every notification increments a
process-local revision and invalidates the cached identity；a revision change during two otherwise identical
snapshots fails with `NEREUS_CLUSTER_CAPABILITY_SNAPSHOT_CHANGED`. Cache publication and reads compare the same
revision, and registry stop/unregister clears the visible cache. Focused tests freeze the identity, reject missing or
spoofed capabilities, prove input-order independence, distinguish same-id broker restart by start timestamp, and
cover notification-before-cache and notification-between-snapshot cuts. Checkpoint Y itself did not run the
cold-topic backfill. Checkpoint Z provides that bounded traversal/report, and Checkpoint AA now hands exact readiness
plus zero-failure coverage to the product-owned durable proof CAS. Neither checkpoint sets a topic marker or implements
the product activation guard.

Checkpoint AC changes only the sequencing after that proof：`NereusManagedLedgerStorage` waits for the product
publication coordinator only when the backfill report has zero failures and the explicit switch is enabled. A failed
report or disabled switch never calls activation；activation failure fails the returned completion promise. The broker
still does not own the activation metadata and does not set a topic marker.

### 4.3 Cluster activation record

```text
/nereus/clusters/{cluster}/capabilities/generation-v1/activation
```

The exact schema is：

```java
public enum GenerationProtocolActivationLifecycle {
    PREPARED(1), ACTIVE(2)
}

public record ReferenceDomainVersionRecord(
        String domainId, int protocolVersion) { }

public record GenerationBackfillProofRecord(
        String runId,
        long brokerReadinessEpoch,
        String coverageSha256,
        boolean complete,
        long completedAtMillis) { }

public record GenerationProtocolActivationRecord(
        int schemaVersion,
        int protocolVersion,
        GenerationProtocolActivationLifecycle lifecycle,
        boolean publicationEnabled,
        boolean physicalDeleteEnabled,
        boolean cursorSnapshotDeleteEnabled,
        long brokerCapabilityReadinessEpoch,
        List<ReferenceDomainVersionRecord> requiredReferenceDomains,
        GenerationBackfillProofRecord streamRegistrationBackfill,
        GenerationBackfillProofRecord physicalRootBackfill,
        GenerationBackfillProofRecord cursorSnapshotBackfill,
        String objectStoreCapabilitySha256,
        String activatingBrokerRunId,
        long preparedAtMillis,
        long activatedAtMillis,
        long updatedAtMillis,
        long metadataVersion) { }

public record VersionedGenerationProtocolActivation(
        String key,
        GenerationProtocolActivationRecord value,
        long metadataVersion,
        Checksum durableValueSha256) { }

public interface GenerationProtocolActivationStore extends AutoCloseable {
    CompletableFuture<Optional<VersionedGenerationProtocolActivation>> get(
            String cluster);
    CompletableFuture<VersionedGenerationProtocolActivation> getOrCreate(
            String cluster);
    CompletableFuture<VersionedGenerationProtocolActivation> compareAndSet(
            String cluster,
            GenerationProtocolActivationRecord replacement,
            long expectedVersion);
}
```

Checkpoint S implements this record、codec and exact-key production store. The `get` operation is deliberately
non-creating because GC/reference-domain evaluation must never manufacture rollout authority while checking it.
Concurrent PREPARED creation converges on the existing exact record, and compare-and-set recovery accepts only the
exact desired replacement reloaded at a later metadata version. Broker capability collection、the three backfill
executors and runtime activation guard remain later rollout work；the future sentinel is implemented by checkpoint T.

Checkpoint T implements the sentinel and global-scope consumer side. It accepts the 64-shard registration set as
ownerless scope only under an exact ACTIVE/deletion-ready activation wrapper with all three completed backfills and an
exact installed domain set, then rereads that wrapper after enumeration. This does not implement or simulate the
backfill executors/broker readiness barrier；production cannot manufacture those proofs from the scanner itself.
Checkpoint U consumes the resulting ownerless proof in the standalone DELETED-root tombstone coordinator, but does not
change this rollout boundary or schedule production deletion.

Schema/protocol are `1`. Domain pairs are canonical sorted/unique, non-empty and capped at 32. Run/broker ids are
random 128-bit lowercase base32；coverage/capability digests are lowercase SHA-256. An incomplete backfill has empty
run/digest、zero completion time but still records the readiness epoch being attempted；a complete proof has all
fields present and its epoch equals the record's broker epoch. No proof may name an epoch newer than the record's
broker epoch. `metadataVersion` is zero on wire and hydrated from
Oxia. The version wrapper follows document 03's exact key/version/durable-value-digest contract.

Allowed durable changes are `PREPARED -> ACTIVE`, false-to-true capability bits, replacement of a backfill proof by a
newer broker-readiness epoch and exact domain-set changes only under a separately reviewed capability transition.
No true bit returns false. A membership/property change invalidates the process-local readiness epoch, so the guard
blocks immediately even though durable bits are monotonic；the next stable capable epoch must refresh every required
proof before operations resume. `ACTIVE` requires：

- two stable capable broker snapshots；
- successful cold-topic registration backfill under that stable readiness epoch；
- F4 metadata codecs deployed；
- generation-zero physical-root/protection backfill complete or deletion bit disabled；
- cursor snapshot pin/inventory backfill complete or snapshot-delete bit disabled；
- exact domain registry match；
- object-store list/delete capability probe passed before delete bit is enabled。

Checkpoint AC is the first production owner of the ACTIVE CAS, but only for publication. It proves the first two
publication prerequisites above through exact broker readiness and the durable registration proof；the physical-root、
cursor-snapshot and object-store prerequisites remain mandatory only for later delete-bit transitions.

Publication and deletion bits are separate. Higher generation may be admitted while physical delete remains disabled.
`physicalDeleteEnabled` additionally requires stream-registration、physical-root and cursor-snapshot proofs complete
and the non-empty object-store capability digest. `cursorSnapshotDeleteEnabled` can remain false while ordinary source
deletion is also kept false；V1 does not permit partially enabling physical deletion for a subset of object kinds.

## 5. Rolling Upgrade / Downgrade

### 5.1 Upgrade sequence

```text
1. Deploy F4 binaries with generation protocol disabled.
   Existing F2/F3 behavior and sync profile remain unchanged.

2. Every persistent broker constructs F4 runtime and advertises generation=1.
   Workers remain inert; no marker/index/delete is written.

3. Run the tenant/namespace/persistent-topic backfill from 3.3. A zero-failure report first CASes the durable
   registration proof and, when the explicit switch is enabled, waits for the product publication coordinator.
   Separately run metadata/object capability probes and backfill physical roots/protections before deletion.
   Dry-run inventory reports missing registrations and all uncovered/ambiguous objects.

4. The product coordinator CASes
   PREPARED -> ACTIVE(publication=true, physicalDelete=false, cursorSnapshotDelete=false).

5. On first F4 operation per topic, revalidate its registration, pass the
   two-snapshot barrier and CAS the topic projection generation marker.
   Reload both records; old code now fails that topic closed.

6. Admit higher-generation publication and async profile on explicitly configured topics.
   Keep all old source bytes/index/commit records.

7. Build/verify recovery checkpoints and cursor snapshot pin coverage.

8. Enable deletion bit only after M4/M6 gates and another stable capable snapshot.
   GC begins with long grace/dry-run metrics.
```

### 5.2 Rollback

- before any topic marker：safe binary rollback；no F4 durable state exists except discovery registrations and
  inactive/backfill roots that old code ignores；
- after marker but before higher publish：old F3 binary rejects topic projection；rollback requires redeploying F4，
  not deleting the monotonic marker；
- after higher publish：old binary must remain barred；lower healthy generations allow F4 software rollback only to a
  generation-aware version；
- after source deletion/recovery checkpoint：there is no downgrade to genesis-only commit traversal；
- cluster activation and topic markers are monotonic. There is no “disable marker” API。

Operationally turning workers/GC off pauses work but does not change the minimum reader version.

## 6. ManagedLedger Facade Changes

### 6.1 Concrete trim implementation

`AbstractNereusManagedLedger` keeps a harmless no-op only for read-only/non-writable compatibility instances.
`NereusManagedLedger` overrides：

```java
@Override
public void trimConsumedLedgersInBackground(CompletableFuture<?> promise) {
    retentionService.trim(
        projection,
        cursorOwnerSession,
        ownershipGuard,
        currentConfigSnapshot(),
        RetentionTrigger.HOUSEKEEPING_OR_ADMIN)
      .whenComplete(callbacks.completePromiseExactlyOnce(promise));
}
```

The method：

- rejects/returns fenced or closed according to existing error mapper；
- runs on a bounded retention lane, never the broker event loop；
- uses current owner and final ownership checks；
- calls only `CursorRetentionCoordinator.requestTrim`；
- completes after logical trim, not physical GC；
- coalesces concurrent housekeeping requests per ledger but preserves one explicit admin future completion；
- emits no success when policy/capability admission is absent。

`asyncTruncate` remains unsupported. `offloadPrefix/asyncOffloadPrefix` remain unsupported.

### 6.2 Accounting

ManagedLedger/cursor backlog stays logical：

- entry count is offset count from cursor to committed end；
- backlog bytes use cumulative logical bytes, not compacted object size；
- generation replacement does not alter rates、last confirmed entry or mark-delete position；
- physical storage/lag/GC bytes are separate Nereus metrics；
- no virtual ledger rollover is synthesized from Parquet row groups。

### 6.3 Read/seek/reset

Ordinary facade reads continue calling `StreamStorage.read(COMMITTED)`. The projection sees the exact same
`PULSAR_ENTRY_V1` bytes. Seek/reset/history rules remain F3：

- a higher generation can serve retained offsets without changing MessageId；
- offset below completed logical trim remains unavailable even if an undeleted object still has bytes；
- topic-compacted view never serves ordinary history/reset；
- same-name recreation gets a new stream/virtual ledger and never resolves old generations。

## 7. Pulsar Feature Snapshot

`NereusResolvedTopicFeatures` is extended from booleans to exact immutable values：

```java
public record NereusResolvedTopicFeatures(
        Set<String> remoteReplicationClusters,
        boolean deduplicationEnabled,
        int messageTtlSeconds,
        int subscriptionExpirationMinutes,
        long compactionThresholdBytes,
        Optional<RetentionPolicies> retention,
        Map<BacklogQuotaType, BacklogQuota> backlogQuotas,
        boolean preciseTimeBasedBacklogQuotaCheck,
        boolean pulsarOffloadEnabled,
        boolean entryFiltersEnabled,
        boolean shadowOrMigrationEnabled,
        boolean systemOrInternalTopic,
        boolean generationProtocolRuntimeReady) { }
```

The resolver still applies local-topic > global-topic > namespace > broker precedence from one policy snapshot. It
normalizes/copies values and validates overflow before `PersistentTopic` stores them. Runtime readiness is supplied by
the Nereus storage context, not user policy.

Policy update validation runs before replacing the topic's effective snapshot. Rejected policy leaves the previous
snapshot active and sets the existing Nereus policy-admission failure state so operations fail consistently rather
than partially using the new policy.

## 8. Topic / Subscription Admission Matrix

### 8.1 Topic open/update

| Feature | Phase 4 decision |
| --- | --- |
| system/internal topic | reject |
| remote replication | reject |
| deduplication | reject |
| message TTL | allow as F3 cursor expiration only；does not directly delete objects |
| subscription expiration | allow as F3 cursor deletion only；snapshot GC later |
| retention time/size | allow only with generation runtime/cursor protocol ready and representable values |
| backlog size eviction | allow；stock skip persists through F3, then F4 trim/GC |
| backlog time eviction, precise mode | allow through expiry/cursor mutation |
| backlog time eviction, non-precise ledger mode | reject；virtual ledger has no stock ledger rollover semantics |
| producer hold/exception backlog policy | allow existing producer admission behavior；no delete authority |
| compaction threshold | reject until F8 broker compaction integration |
| Pulsar offload | reject |
| entry filters | reject |
| shadow/migration | reject |
| ManagedLedger interceptor / auto-skip / shadow source | reject unchanged |

When a supported retention/backlog policy first requires F4 and the marker is absent, topic create/open/update runs
the registration-before-marker sequence in 3.2 before publishing the topic or installing the new effective policy
snapshot, then re-resolves the registration/projection/policy versions. A namespace policy already present at first
create follows the same ordering. Existing activated topics verify/repair the exact registration on every open before
they become usable. If cluster readiness is absent, registration/activation CAS conflicts, or the final projection/
policy snapshot changes, open/update fails without installing a no-op policy. It never requires the caller to
pre-create either record.

### 8.2 Subscribe/publish

F3 matrix remains：

- durable/non-durable Exclusive、Failover、Shared allowed；
- Key_Shared、replicated subscription、transaction pending ack、delayed delivery rejected；
- `readCompacted=true` rejected throughout F4；
- publish markers/transactional entries/unsupported delayed metadata rejected；
- ordinary batched/compressed Entry bytes remain allowed and must round-trip through higher generations；
- `OBJECT_WAL_ASYNC_OBJECT` publish admission reloads the matching registration and activation proof before primary
  object IO；a missing/mismatched registry entry is a retriable fail-closed error, never an untracked ack。

## 9. Admin Admission Matrix

`NereusTopicFeatureValidator.validateAdminOperation` receives a typed capability view so loaded and unloaded checks
make the same decision：

| `NereusAdminOperation` | F4 |
| --- | --- |
| `TERMINATE_TOPIC` | allow existing seal behavior |
| `DELETE_TOPIC` | allow logical delete；physical bytes remain GC work |
| `UNLOAD_TOPIC` | allow；leases expire/release and new owner claims F3 roots |
| `DELETE_DURABLE_SUBSCRIPTION` | allow F3 tombstone/snapshot handoff |
| `ANALYZE_BACKLOG` | allow read-only |
| `CLEAR_BACKLOG` | allow persisted destructive cursor movement |
| `SKIP_MESSAGES` | allow persisted cursor movement |
| `EXPIRE_MESSAGES` | allow F3 cursor mutation |
| `RESET_CURSOR` | allow within F3 retained-range rules |
| `TRIM_TOPIC` | allow only generation-ready；routes to F4 retention service |
| `TRIGGER_COMPACTION` | reject until F8 |
| `READ_COMPACTION_STATUS` | reject until F8 |
| `TRIGGER_OFFLOAD` / `READ_OFFLOAD_STATUS` | reject |
| `TRUNCATE_TOPIC` | reject |
| `SET_SHADOW_TOPICS` / `MIGRATE_TOPIC` | reject |

The unloaded path in `NereusManagedLedgerStorage.validateUnloadedAdminOperation` first reads binding state and checks
cluster generation readiness. The loaded path in `PersistentTopic.validateNereusAdminOperation` checks the exact
topic feature/capability snapshot. Partitioned trim validates every child and reports aggregate failure; it cannot
return success for a Nereus child whose trim was rejected/no-op.

## 10. Local Pulsar File Changes

Target fork files：

```text
pulsar-broker/.../storage/nereus/
  NereusGenerationProtocolCapability.java               checkpoint Y reserved property/version
  NereusGenerationCapabilityReadiness.java              checkpoint Y deterministic epoch/full digest
  NereusBrokerCapabilityCoordinator.java                checkpoint Y generation property/barrier/cache invalidation
  NereusGenerationRegistrationBackfill.java             cold unloaded-topic enumeration/coverage gate
  NereusStorageBindingCapability.java                   reserve generation property
  NereusResolvedTopicFeatures.java                      exact retention/backlog values
  NereusTopicFeatureResolver.java                       exact policy normalization
  NereusTopicFeatureValidator.java                      F4 admission matrix
  NereusManagedLedgerStorage.java                       runtime readiness, proof completion and AC activation sequencing
  NereusBrokerStorageConfiguration.java                 F4 typed config mapping

pulsar-broker/.../service/persistent/PersistentTopic.java
  store/revalidate F4 feature snapshot and loaded admin decision

pulsar-broker/.../admin/impl/PersistentTopicsBase.java
  retain TRIM_TOPIC validation ordering and exact async completion

pulsar-broker/.../service/BacklogQuotaManager.java
  no semantic fork for size/precise paths; add Nereus rejection/route tests

pulsar-broker-common/.../ServiceConfiguration.java
  F4 settings and documentation
```

No change is required to the public `ManagedLedger` method signature. Any additional fork edit must be added to the
source-lock inventory and its focused test before M5 gate closes.

## 11. Broker Configuration Surface

Initial product defaults keep active mutation off：

| Property | Default | Constraint/purpose |
| --- | --- | --- |
| `nereusGenerationProtocolEnabled` | `false` | advertise/runtime may exist, but no first activation while false |
| `nereusGenerationRegistrationBackfillConcurrency` | `16` | positive bounded projection reads/registration writes |
| `nereusGenerationRegistrationBackfillTimeoutSeconds` | `3600` | positive deadline for one full cold-topic traversal |
| `nereusPhysicalGcEnabled` | `false` | independent destructive switch |
| `nereusPhysicalGcDryRun` | `true` | inventory/plan metrics only；never MARK/DELETE while true |
| `nereusDefaultStorageProfile` | `OBJECT_WAL_SYNC_OBJECT` | first-create only；async is opt-in |
| `nereusMaterializationRegistryScanPageSize` | `256` | positive and `<= 256`；64 shards are protocol-fixed |
| `nereusMaterializationRegistryScanIntervalSeconds` | `10` | positive；also drives authoritative planner reconciliation |
| `nereusMaterializationPlannerPageSize` | `512` | positive and `<= 512` |
| `nereusMaterializationTaskScanPageSize` | `256` | positive and `<= 256` |
| `nereusMaterializationMaxTasksPerPlan` | `64` | positive bounded work per stream pass |
| `nereusMaterializationMaxWorkers` | `8` | `<= object read concurrency` |
| `nereusMaterializationMaxWorkersPerStream` | `1` | positive and `<= max workers` |
| `nereusMaterializationSourceReadPageRecords` | `8192` | positive and `<= 65536` |
| `nereusMaterializationSourceReadPageBytes` | `8388608` | `[64 KiB, 64 MiB]` and within core read-buffer reservation |
| `nereusMaterializationWorkerClaimSeconds` | `120` | covers operation/renew/skew |
| `nereusMaterializationWorkerRenewSeconds` | `30` | `<= claim/3` |
| `nereusMaterializationOperationTimeoutSeconds` | `60` | positive and `< claim - skew` |
| `nereusMaterializationCloseTimeoutSeconds` | `300` | drains admitted publication/recovery work |
| `nereusMaterializationRetryMinMillis` | `1000` | positive |
| `nereusMaterializationRetryMaxMillis` | `60000` | `>= min` |
| `nereusMaterializationMaxTaskAttempts` | `20` | positive；exhaustion becomes terminal/operator-visible |
| `nereusMaterializationMinMergeSourceRanges` | `2` | `[2, maxSourceRanges]`；prevents single-generation rewrite churn |
| `nereusMaterializationMaxSourceRanges` | `128` | hard cap |
| `nereusMaterializationMaxRangeRecords` | `1048576` | resolver hard span |
| `nereusMaterializationTargetObjectBytes` | `268435456` | `<= 1 GiB` |
| `nereusMaterializationTargetRowGroupRecords` | `8192` | `<= 65536` |
| `nereusMaterializationCompression` | `ZSTD` | exactly `ZSTD` or `UNCOMPRESSED` |
| `nereusMaterializationStagingDirectory` | `${java.io.tmpdir}/nereus-materialization` | absolute owner-only local base；runtime appends processRunId；not durable truth |
| `nereusMaterializationMaxStagingBytes` | `2147483648` | global worker staging semaphore；covers both target object and recovery-checkpoint maxima |
| `nereusObjectUploadChunkBytes` | `1048576` | `[64 KiB, 8 MiB]`；backpressure-aware streaming upload |
| `nereusObjectPutMaxAttempts` | `3` | total guarded provider transmissions；`[1, 10]` |
| `nereusObjectPutRetryMinMillis` | `100` | positive first-retry full-jitter cap |
| `nereusObjectPutRetryMaxMillis` | `2000` | `>= min` and `<= object-store request timeout` |
| `nereusReaderLeaseSeconds` | `120` | covers max read + skew |
| `nereusReaderLeaseRenewSeconds` | `30` | positive, bounded |
| `nereusMaximumClockSkewSeconds` | `5` | operational clock contract |
| `nereusGcScanIntervalSeconds` | `60` | positive |
| `nereusGcMetadataScanPageSize` | `1000` | positive and `<= 1000` |
| `nereusGcObjectListPageSize` | `1000` | positive and `<= 1000` |
| `nereusGcMaxConcurrentDeletes` | `4` | positive bounded object-store delete concurrency |
| `nereusGcMaxStreamsPerCandidate` | `1024` | positive and `<= 4096`；larger manifest quarantines |
| `nereusGcMaxAuthoritiesPerDomainSnapshot` | `100000` | positive hard memory bound；overflow vetoes |
| `nereusGcMaxReferencesPerDomainSnapshot` | `100000` | positive hard memory bound；overflow vetoes |
| `nereusGcOperationTimeoutSeconds` | `60` | positive and compatible with lease/skew |
| `nereusGcCloseTimeoutSeconds` | `300` | drains admitted GC state reconciliation |
| `nereusGcDrainGraceSeconds` | `300` | lease + skew lower bound |
| `nereusPendingProtectionSeconds` | `300` | bounded checkpoint/snapshot publication intent；operation + skew lower bound |
| `nereusSourceRetirementGraceSeconds` | `3600` | fallback/checkpoint grace |
| `nereusAppendReplayGraceSeconds` | `21600` | protects live commit evidence during checkpoint retirement |
| `nereusMaterializationMetadataAuditGraceSeconds` | `86400` | terminal task/index audit retention；`>= source retirement grace` |
| `nereusOrphanGraceSeconds` | `86400` | no young orphan deletion |
| `nereusGcTombstoneAuditGraceSeconds` | `604800` | `>= metadata audit + orphan grace` and beyond every stale PUT/owner lifetime |
| `nereusRecoveryCheckpointMaxEntries` | `1000000` | NRC1 cap |
| `nereusRecoveryCheckpointMaxBytes` | `1073741824` | NRC1 cap |
| `nereusMaterializationLagThrottleRecords` | `1000000` | zero disables threshold |
| `nereusMaterializationLagRejectRecords` | `10000000` | zero disables；otherwise > throttle |
| `nereusMaterializationLagThrottleBytes` | `1073741824` | zero disables threshold |
| `nereusMaterializationLagRejectBytes` | `8589934592` | zero disables；otherwise > throttle |
| `nereusMaterializationLagRejectAgeSeconds` | `600` | zero disables；otherwise positive |
| `nereusMaterializationLagThrottleDelayMillis` | `25` | positive；one bounded delay followed by exact remeasurement |
| `nereusRetentionStatsScanPageSize` | `512` | positive and `<= 512` |
| `nereusRetentionMaxConcurrentPlans` | `4` | positive；at most one executing per stream |
| `nereusRetentionMaxQueuedPlans` | `1024` | positive bounded admission queue |
| `nereusRetentionOperationTimeoutSeconds` | `60` | positive；one deadline for plan + logical trim request |
| `nereusRetentionCloseTimeoutSeconds` | `120` | positive and within managed-ledger close budget |

Secrets and object-store credentials remain on the existing resolver path. Config logs redact secret refs/tokens and
do not log object keys at info level.

Checkpoint AF maps the Object-WAL subset of this table through
`NereusBrokerStorageConfiguration` into the production `MaterializationConfig` and installs the Phase 4 resolver、
exact lag reader/guard、generation-aware read/repair and materialization lifecycle through
`DefaultNereusRuntimeProvider`. The default remains `OBJECT_WAL_SYNC_OBJECT`; the async value is exact-name only,
first-create only and still requires the durable activation/marker proof before primary IO. Checkpoints AH–AI map the
retention values；checkpoint AO maps the physical-GC-specific properties below the materialization boundary.

Checkpoint AK composes the product-side cursor physical-GC executor、six reference domains、durable journal and
source-retirement coordinator in `Phase4PhysicalGcRuntime`, and `NereusManagedLedgerRuntime` owns/closes it before
cursor and shared stores. `NereusRuntimeConfiguration` now carries a cross-validated `PhysicalGcConfig`; its
compatibility constructors deliberately use `PhysicalGcConfig.defaults()` (`enabled=false, dryRun=true`). Checkpoint AL
makes the inventory scanner an owned runtime component；listing remains audit/discovery input and exposes no MARK/delete
path. Checkpoint AN adds the non-overlapping lifecycle service and provider startup hook: when and only when
`PhysicalGcConfig.enabled()` is true, the runtime is eligible to run complete physical-root routing/recovery, then
complete registration retirement, then object inventory, using fixed delay after each pass. Checkpoint AO now maps all 17 explicit broker
properties through `NereusBrokerStorageConfiguration` into the cross-validated runtime value and makes the provider use
that same value for pending protections、reader leases、clock skew and orphan grace. The mapped `enabled=false` default
therefore starts no lifecycle pass, while `dryRun=true` remains the second independent mutation guard. Broker startup
still cannot acquire physical-delete authority until coverage/capability proofs and monotonic activation are complete,
so configuration alone cannot bypass the rollout sequence above.

Checkpoint AP adds `DefaultObjectStoreDeleteCapabilityProbe` at the protocol-neutral object-store boundary. Before a
product coordinator may request deletion activation, it can now prove the exact configured provider/scope supports
guarded PUT、exact HEAD、complete LIST、ETag-bound exact DELETE、lost-response absence convergence and idempotent
absence. The stable digest includes provider/endpoint/region/bucket/prefix/path-style plus frozen V1 semantic labels；
it excludes credentials and tuning values. Each run uses a high-entropy isolated canary key, validates CRC32C/length/
ETag/last-modified, performs post-delete HEAD and LIST absence checks, and exact-cleans the canary on failure.
Checkpoint AP intentionally exposes no broker startup hook：it neither runs checkpoint-W coverage backfill nor writes
the digest/delete bits. Therefore an enabled non-dry-run broker configuration still lacks destructive authority until
the product activation coordinator completes the same readiness epoch's backfills, executes this probe and advances
both V1 delete bits atomically through the activation store.

Checkpoint AQ implements that product-owned bounded transaction. Checkpoint AR installs it in production without
making configuration itself authority. The provider constructs one configured-scope probe and passes the same
expected digest to the generation operation guard、AQ coordinator and `Phase4PhysicalGcStartupGate`；the runtime owns
checkpoint-W backfill and exposes activation through the managed-ledger runtime/factory. `enabled=false` starts
nothing；`enabled=true, dryRun=true` may run non-mutating audit passes；`enabled=true, dryRun=false` starts or resumes
the lifecycle only after durable dual delete bits、all proofs、the exact six-domain set and the local scope digest match.
Absent/publication-only authority defers startup, while a durable scope/domain mismatch fails before MARKED/DELETING
recovery.

Checkpoint AS removes the remaining interpretation split inside that composition. The provider creates one
`Phase4GcReferenceDomainAssembly` whose registered-stream global scope、configured reference bounds and projection
domain are passed by exact instance to both the generation activation guard and `Phase4PhysicalGcRuntime`. A real
four-shard Oxia + pinned LocalStack fixture then proves publication-only deferral、W/AP-backed atomic activation、
wrong logical S3 prefix rejection before MARKED recovery, and correct-scope ownerless recovery when inventory LIST is
empty and the first successful target DELETE response is lost. HEAD absence is the recovery fact and the durable root
reaches DELETED；the broker safe defaults remain unchanged and this is not the complete multi-broker/scale final gate.

Checkpoint AT exercises the stricter process boundary after a confirmed real target DELETE but before the old runtime
can invoke its DELETED-root CAS. The interrupted process leaves exact DELETING authority and an absent LocalStack key；
a new independently assembled runtime with the same scope is admitted by the startup gate, discovers DELETING from
the root shard, reauthenticates the sealed journal and completes DELETED from HEAD absence. Neither broker startup
state nor object listing is transferred. This closes one required restart cut without claiming two-broker rollout or
the remaining scale/failure matrix.

The Pulsar bridge records the typed `mutationsAllowed()` value during storage initialization. After a zero-failure
registration backfill it first waits for publication activation, then only under that physical switch submits the AQ
request with the same run id/concurrency/timeout and waits through lifecycle start. Failure reports、disabled generation
publication and safe/default/dry-run physical config never call physical activation；either activation failure is
propagated to the original backfill completion promise.

## 12. Operations and Status

F4 does not add a public Pulsar compaction/offload status. It exposes Nereus runtime snapshots for broker metrics and
debug logging：

```java
public record NereusMaterializationStatus(
        long registeredStreamsSeen,
        long registryLastFullScanAtMillis,
        long registrationMismatches,
        long plannedTasks,
        long runningTasks,
        long retryTasks,
        long lagOffsets,
        long lagBytes,
        long oldestLagMillis,
        long committedGenerations,
        long quarantinedGenerations) { }

public record NereusGcStatus(
        boolean enabled,
        boolean dryRun,
        long rootLastFullScanAtMillis,
        long lifecycleRecoveryPending,
        long candidates,
        long marked,
        long deleting,
        long deletedRootsAwaitingAuditRetirement,
        long tombstonesRetired,
        long quarantined,
        long bytesDeleted,
        Map<String, Long> vetoesByDomain) { }
```

Snapshots are read-only and bounded；they do not scan Oxia/object storage on the broker request thread. Admin tools may
later expose them through Nereus-specific endpoints without changing Pulsar's stock compaction contract.

Alert conditions：

- registry full-scan age exceeds the configured interval/SLA or registration mismatches are non-zero；
- physical-root full-scan age exceeds GC SLA or shard skew prevents MARKED/DELETING recovery；
- oldest materialization lag approaching primary retention budget；
- PREPARED/PUBLISHING task older than claim/recovery SLA；
- generation candidate count approaching 4,096；
- recovery live commit tail approaching scan limit；
- reader leases repeatedly outliving normal read timeout；
- MARKED/DELETING object older than GC recovery SLA；
- DELETED-root audit backlog/oldest age exceeds tombstone-retirement SLA；
- domain-set mismatch or GC globally vetoed；
- fallback/quarantine/checksum errors；
- policy trim blocked by pending F3 lifecycle for abnormal duration。

## 13. Compatibility Proof Obligations

### 13.1 Pulsar

Across generation publish、source retirement、unload、owner transfer and process restart：

- virtual ledger id and stream incarnation remain identical；
- entry id remains offset；
- exact Entry bytes/properties/batch metadata remain identical；
- ordinary and middle-batch MessageIds compare equal；
- cursor mark-delete/ack ranges/partial words do not mention generation；
- reset/seek/history returns identical retained entries；
- logical backlog count/bytes do not jump with physical compression；
- reader fallback is invisible except metrics/latency；
- topic delete/recreate cannot resolve old generation objects。

### 13.2 Future 5 KoP

F5 may add a Kafka mapping-specific lossless verifier and `TOPIC_COMPACTED` consumer, but：

- Kafka offset remains stream offset；
- it registers its group/reference domain before GC；
- it cannot reinterpret an F2 multi-message Pulsar Entry as multiple stream offsets；
- it uses view-scoped generation records and never creates another physical catalog truth。

### 13.3 Future 6 Lakehouse

F6 may reference NCP1 objects or write table objects. It must：

- publish catalog protection before catalog visibility；
- record exact stream range/view/generation/publication/object checksum lineage；
- register `catalog-v1` domain/capability before first reference；
- never let catalog success/failure enter producer acknowledgement；
- release protection only after catalog snapshot/delivery retirement。

### 13.4 Future 8 Pulsar compaction

F8 consumes the NTC1 primitive and must add：

- trigger/status ownership and idempotence；
- Pulsar key/tombstone/batch/encryption semantics；
- read-compacted cursor/dispatcher merge of compacted prefix + committed tail；
- stable MessageId behavior for sparse output；
- compaction policy update and broker admin gates。

It cannot publish NTC1 into the ordinary offset-index namespace or change F4 committed fallback.

## 14. Required Fork/Facade Tests

- lookup-property reserved composition and two-stable-snapshot generation barrier；
- NPR1 projection-ref golden bytes/CRC/name collision validation and current-projection resolution after property CAS；
- registration-before-marker response-loss cuts、missing/mismatched registration fail-closed and open repair；
- cold unloaded-topic backfill over tenant/namespace/topic resource APIs, concurrent create/delete, bounded failure and
  broker-readiness epoch invalidation；
- first topic marker CAS, concurrent activation, external property preservation and F3-old-decoder rejection；
- profile first-create sync/async immutability and BookKeeper profile rejection；
- topic open/update exact retention/backlog matrix including non-precise time rejection；
- loaded/unloaded/partitioned `TRIM_TOPIC` agreement and no no-op success；
- housekeeping trim uses current owner and callback exactly once；
- ownership loss before candidate、requestTrim and callback success；
- backlog size/precise-time eviction persists cursor movement and later logical trim；
- compaction/offload/truncate/read-compacted remain rejected on every route；
- generation replacement/fallback preserves exact ordinary/middle-batch MessageIds through unload/failover/restart；
- unloaded activated topic with no task is discovered from all-shard registry scan on a new broker process；
- same-name recreation and old-stream GC isolation；
- rollout steps with publication on/deletion off, then deletion activation；
- old broker joining after cluster activation blocks new F4 operations and old decoder fails activated topics；
- runtime construction/partial failure/reverse close with workers/leases drained before ObjectStore close。
