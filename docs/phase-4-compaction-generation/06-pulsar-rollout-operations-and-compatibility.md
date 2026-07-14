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

## 2. Planned Nereus Runtime Changes

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
GenerationProtocolActivationGuard generationActivationGuard();
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

After every broker has the F4 runtime, new Nereus topic creation always creates/verifies its registration even while
generation publication is disabled. Existing unloaded topics are covered by an explicit idempotent backfill：

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

The run starts and ends under the same two-snapshot broker readiness epoch, which proves the new-topic registration
hook was active throughout. Any list/read/register failure、epoch change or nonzero failure count prevents the cluster
activation record's `streamRegistrationBackfillComplete` bit. Process loss simply reruns from the beginning；same-key
create-or-verify is idempotent. The backfill creates no topic marker and cannot activate publication. If an old broker
later joins, readiness is invalidated and the bit must be re-proved/rerun before F4 mutations resume.

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
    CompletableFuture<VersionedGenerationProtocolActivation> getOrCreate(
            String cluster);
    CompletableFuture<VersionedGenerationProtocolActivation> compareAndSet(
            String cluster,
            GenerationProtocolActivationRecord replacement,
            long expectedVersion);
}
```

Schema/protocol are `1`. Domain pairs are canonical sorted/unique, non-empty and capped at 32. Run/broker ids are
random 128-bit lowercase base32；coverage/capability digests are lowercase SHA-256. An incomplete backfill has empty
run/digest、zero completion time but still records the readiness epoch being attempted；a complete proof has all
fields present and its epoch equals the record's broker epoch. `metadataVersion` is zero on wire and hydrated from
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

Publication and deletion bits are separate. Higher generation may be admitted while physical delete remains disabled.
`physicalDeleteEnabled` additionally requires both physical-root and cursor-snapshot proofs complete and the non-empty
object-store capability digest. `cursorSnapshotDeleteEnabled` can remain false while ordinary source deletion is also
kept false；V1 does not permit partially enabling physical deletion for a subset of object kinds.

## 5. Rolling Upgrade / Downgrade

### 5.1 Upgrade sequence

```text
1. Deploy F4 binaries with generation protocol disabled.
   Existing F2/F3 behavior and sync profile remain unchanged.

2. Every persistent broker constructs F4 runtime and advertises generation=1.
   Workers remain inert; no marker/index/delete is written.

3. Run the tenant/namespace/persistent-topic backfill from 3.3 and CAS its coverage bit;
   run metadata/object capability probes and backfill physical roots/protections.
   Dry-run inventory reports missing registrations and all uncovered/ambiguous objects.

4. CAS cluster activation PREPARED -> ACTIVE(publication=true, deletion=false).

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
  NereusGenerationProtocolCapability.java               new
  NereusBrokerCapabilityCoordinator.java                compose generation property/barrier
  NereusGenerationRegistrationBackfill.java             cold unloaded-topic enumeration/coverage gate
  NereusStorageBindingCapability.java                   reserve generation property
  NereusResolvedTopicFeatures.java                      exact retention/backlog values
  NereusTopicFeatureResolver.java                       exact policy normalization
  NereusTopicFeatureValidator.java                      F4 admission matrix
  NereusManagedLedgerStorage.java                       runtime readiness/unloaded admin
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
| `nereusMaterializationStagingDirectory` | deployment-set | absolute owner-only local directory；not durable truth |
| `nereusMaterializationMaxStagingBytes` | `2147483648` | global worker staging semaphore；`>= target object bytes` |
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
| `nereusAppendReplayGraceSeconds` | `3600` | protects live commit evidence during checkpoint retirement |
| `nereusMaterializationMetadataAuditGraceSeconds` | `86400` | terminal task/index audit retention；`>= source retirement grace` |
| `nereusOrphanGraceSeconds` | `86400` | no young orphan deletion |
| `nereusGcTombstoneAuditGraceSeconds` | `604800` | `>= metadata audit + orphan grace` and beyond every stale PUT/owner lifetime |
| `nereusRecoveryCheckpointMaxEntries` | `1000000` | NRC1 cap |
| `nereusRecoveryCheckpointMaxBytes` | `1073741824` | NRC1 cap |
| `nereusMaterializationLagThrottleRecords` | deployment-set | zero disables threshold |
| `nereusMaterializationLagRejectRecords` | deployment-set | zero disables；otherwise > throttle |
| `nereusMaterializationLagThrottleBytes` | deployment-set | zero disables threshold |
| `nereusMaterializationLagRejectBytes` | deployment-set | zero disables；otherwise > throttle |
| `nereusMaterializationLagRejectAgeSeconds` | deployment-set | zero disables；otherwise positive |
| `nereusRetentionStatsScanPageSize` | `512` | positive and `<= 512` |
| `nereusRetentionMaxConcurrentPlans` | `4` | positive；at most one executing per stream |
| `nereusRetentionMaxQueuedPlans` | `1024` | positive bounded admission queue |
| `nereusRetentionOperationTimeoutSeconds` | `60` | positive；one deadline for plan + logical trim request |
| `nereusRetentionCloseTimeoutSeconds` | `120` | positive and within managed-ledger close budget |

Secrets and object-store credentials remain on the existing resolver path. Config logs redact secret refs/tokens and
do not log object keys at info level.

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
