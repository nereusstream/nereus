# Reader Leases, Retention, Source Retirement, and Physical GC

## 1. Safety Invariant

Physical deletion is legal only when all of the following are proved in one recoverable attempt：

```text
object immutable identity is exact
AND object root blocks new leases/protections
AND every existing reader lease is released or skew-safely expired
AND every visible generation/index reference is part of the retirement plan
AND append/recovery checkpoint no longer needs the source
AND materialization/repair tasks do not protect it
AND completed logical trim and cursor pending lifecycles allow the action
AND current cursor snapshot roots do not reference it
AND every live Pulsar projection that can address an affected stream has generation protocol 1
    (or the old incarnation is authoritatively deleted/unaddressable)
AND every activated future reference domain reports no veto
AND the complete authoritative version set is unchanged after the drain wait
```

Failure to prove any clause delays deletion. Age、list absence、task completion、higher generation、cursor floor or a
local reference count alone is never sufficient.

## 2. Source Layout and Implementation State

```text
nereus-core/src/main/java/com/nereusstream/core/physical/
  PhysicalObjectIdentity.java
  ObjectReadPinManager.java
  DefaultObjectReadPinManager.java
  ObjectReadLease.java
  PhysicalObjectRecords.java
  PhysicalValueDigests.java
  ObjectProtectionManager.java
  DefaultObjectProtectionManager.java
  ObjectProtection.java
  ObjectProtectionOwner.java
  ObjectProtectionRequest.java
  GcReferenceDomain.java
  GcReferenceQuery.java
  GcReferenceQueryKind.java
  GcReferenceSnapshot.java
  GcAuthorityToken.java
  GcReference.java
  GcReferenceDomainConfig.java
  GcReferenceSnapshotBuilder.java

nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/f4/
  PhysicalObjectMetadataStore.java

nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/retirement/
  RetirementMetadataClient.java
  RetirementMetadataKey.java
  RetirementMetadataValue.java
  GenerationZeroMarkerIdentity.java
  LegacyCommittedSliceIdentity.java
  GenericCommittedAppendIdentity.java
  VersionedGenerationZeroMarker.java
  VersionedGenerationZeroCommit.java
  SourceRetirementMetadataStore.java
  OxiaJavaSourceRetirementMetadataStore.java
  VersionedObjectManifestAudit.java
  VersionedObjectReferencesAudit.java
  ObjectAuditRetirementStore.java
  OxiaJavaObjectAuditRetirementStore.java

nereus-materialization/src/main/java/com/nereusstream/materialization/gc/
  GcCandidate.java
  GcPlan.java
  GcPlannedProtectionRemoval.java
  GcPlannedMetadataRemoval.java
  PhysicalGcConfig.java
  GcReferenceDomainVersion.java
  GcReferenceCollectionStatus.java
  GcReferenceCollection.java
  GcReferenceDomainRegistry.java
  GcPlanMetadataRevalidator.java
  PhysicalGcMarkStatus.java
  PhysicalGcMarkResult.java
  PhysicalGcAdvanceStatus.java
  PhysicalGcAdvanceResult.java
  PhysicalObjectGarbageCollector.java
  PhysicalObjectRootVisitor.java
  PhysicalObjectRootScanResult.java
  PhysicalObjectRootScanner.java
  GenerationZeroIndexRetirementHandler.java
  GenerationZeroMarkerRetirementHandler.java
  GenerationZeroCommitRetirementHandler.java
  HigherGenerationIndexRetirementHandler.java
  CompletedTrimRetirementVerifier.java
  RecoveryReplacementVerifier.java
  HigherGenerationRecoveryCoverageVerifier.java
  TopicCompactedReplacementVerifier.java
  HigherGenerationRetirementEligibilityVerifier.java
  HigherGenerationPreDrainCoordinator.java
  HigherGenerationPreDrainResult.java
  HigherGenerationPreDrainStatus.java
  SourceRetirementPlanBuilder.java
  PhysicalRootTombstoneRetirementCoordinator.java
  DefaultPhysicalRootTombstoneRetirementCoordinator.java
  TombstoneRetirementResult.java
  TombstoneRetirementStatus.java
  TombstoneRetirementDigests.java
  SourceRetirementCoordinator.java
  StreamRegistrationRetirementCoordinator.java
  ObjectInventoryScanner.java

nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/retention/
  RetentionPolicySnapshot.java
  NereusRetentionConfig.java
  RetentionStatsToken.java
  RetentionCandidate.java
  RetentionPolicySnapshotProvider.java
  RetentionCandidatePlanner.java
  DefaultRetentionCandidatePlanner.java
  NereusManagedLedgerRetentionService.java
  RetentionEvidenceDigests.java
  ProjectionGenerationReferenceDomain.java
  CursorSnapshotReferenceDomain.java
  CursorSnapshotGcScanner.java

nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/cursor/
  CursorSnapshotWriteAuthority.java
  CursorSnapshotPublication.java
  CursorSnapshotStore.java
  DefaultCursorSnapshotStore.java
```

As of F4-M2 on 2026-07-15, `PhysicalObjectIdentity`、the GC reference-domain values、the reader-pin manager and the
durable protection manager are implemented with deterministic handshake/response-loss tests. The committed-generation
resolver、exact reader dispatch、pin lifetime、fallback/quarantine and publication state machine are integrated and
final-gated. The M3 storage-level compacted-format strict reader、exact core NCP1 adapter、exact-source worker、
protection/checkpoint reconciliation、bounded service lifecycle and Pulsar Entry/NCP1 byte round trip are implemented,
and M3 terminal workflow-metadata retirement is implemented/final-gated. M4 checkpoint A adds the NRC1 immutable
object codec and canonical metadata verifier. M4 checkpoint B adds permanent commit-owned `REACHABLE_APPEND` before
head visibility and exact-index-owned `VISIBLE_GENERATION` before strict success for every new/recovered
generation-zero append. Checkpoint D now publishes verified NRC1 bytes only through the recovery-root CAS and repairs
checkpoint-object/target protections after CAS or restart before releasing the bounded pending protection. M4 GC
checkpoint E adds the root-stable pinned append-replay consumer. Checkpoint F adds root-stable pinned NRC1
generation-index repair, exact raw/envelope digest separation, ACTIVE-target selection and current-root-owned target
protection before committed-index restoration. Checkpoint G adds the read-before-delete source/object-audit metadata
adapters with exact key、encoding、Oxia version and stored-envelope digest checks, including explicit committed-marker
capture and response-loss fail-closed behavior. Checkpoint H adds strict GC config、ACTIVE-root candidate and bounded
canonical restart-reconstructable plan values；the plan digest commits every complete domain fact and planned
key/protection while excluding ephemeral candidate identity/time. Checkpoint I adds the exact bounded domain registry、
mandatory planned-metadata revalidation、recoverable `ACTIVE -> MARKED -> DELETING` root fence and complete 256-shard
root scan. It stops before every source/protection/audit/object delete and is not production-composed. Concrete
reference domains arrive in checkpoint J for affected-stream generation、append recovery and materialization facts,
including query-bound stateless revalidation and exact reference/removal binding. Checkpoint K moves bounded canonical
snapshot construction into core and adds affected-stream F2 projection-generation plus F3 cursor-snapshot domains over
exact stored authority digests. Checkpoint M adds the root-authenticated DELETING recovery skeleton；checkpoint N adds
canonical generation-index routing plus generation-zero delete/higher-generation RETIRED handlers and reauthenticates
root+journal at every destructive batch. Checkpoint O adds exact-key generation-zero marker/commit handlers and
NRC1-bound source triple freezing/revalidation. Checkpoint P additionally requires a current exact COMMITTED NRC1
replacement index plus its matching ACTIVE physical root and revalidates both after source reads. Checkpoint Q adds
COMMITTED-view whole-range NRC1/count/schema proof and response-loss-safe higher-generation
`COMMITTED/QUARANTINED -> DRAINING`, then repeats that proof when a DRAINING removal is frozen. Checkpoint R adds
exact completed-trim eligibility for generation-zero and either higher view、strictly newer current
TOPIC_COMPACTED/ACTIVE same-view replacement eligibility, and a zero-read `sourceRetirementGrace` admission fence.
Checkpoint T implements future-sentinel and ownerless global domain variants. Checkpoint U implements the slow
DELETED-root/Phase 1 audit-retirement pass with a persisted first-absence checkpoint、fresh ownerless-domain scans、
late-byte cleanup、references-before-manifest ordering、root-last conditional delete and response-loss convergence.
Checkpoint V closes the new cursor-snapshot write/read frontier：guarded immutable PUT、ACTIVE physical root、
current-root-owned bounded pending protection、cursor-CAS visibility、permanent live-root protection and durable read
lease；a hydrate/read repairs permanent protection after cursor-CAS response loss. Physical-root backfill、object
inventory、registration retirement and the remaining materialization/GC runtime composition were still planned at
that checkpoint.
Checkpoint W subsequently implements physical/cursor live-reference backfill, and checkpoint AJ implements read-only
cursor snapshot candidate discovery plus post-drain full revalidation. Checkpoint AK makes that evidence
restart-reconstructable, adds exact drift rollback and composes the cursor path into the central six-domain
mark/drain/DELETING/source-retirement runtime. Checkpoint AL adds current-writer object inventory and exact old
missing-root registration. Checkpoint AM adds proof-driven stream-registration retirement and its exact managed-ledger
F3 authority. Checkpoint AN composes a non-overlapping metadata-first 256-root-shard/64-registration-shard/inventory
lifecycle and exact ACTIVE/MARKED/DELETING/DELETED routing. Broker config mapping、coverage/delete activation and the
real-service final gate remain planned，so the safe-default production bridge still schedules no pass or deletion.

`ObjectReadPinManager` is injected into both ordinary target readers and `DefaultCursorSnapshotStore`; no direct
object read remains on a physically collectible key.

The lease/protection managers and `GcReferenceDomain` SPI live in `nereus-core`, not the new materialization module.
Thus core/managed-ledger can use or implement the contracts while `nereus-materialization` depends on them for GC；
there is no reverse core -> materialization dependency.

### 2.1 `PhysicalGcConfig`

```java
public record PhysicalGcConfig(
        boolean enabled,
        boolean dryRun,
        int metadataScanPageSize,
        int objectListPageSize,
        int maxConcurrentDeletes,
        int maxStreamsPerCandidate,
        int maxAuthoritiesPerDomainSnapshot,
        int maxReferencesPerDomainSnapshot,
        Duration scanInterval,
        Duration readerLeaseDuration,
        Duration readerLeaseRenewInterval,
        Duration maximumClockSkew,
        Duration drainGrace,
        Duration pendingProtectionDuration,
        Duration orphanGrace,
        Duration tombstoneAuditGrace,
        Duration operationTimeout,
        Duration closeTimeout) { }
```

Page sizes are positive and `<= 1,000`；delete concurrency is positive；`maxStreamsPerCandidate` is in
`[1, 4,096]`；domain authority/reference limits are positive and at most 100,000. Every duration except maximum clock
skew is positive；clock skew may be zero. Renewal is at most one
third of lease duration；drain grace is at least lease duration plus maximum skew；operation timeout plus
skew is shorter than a lease. Cross-validation against materialization config requires orphan grace to exceed every
source-retirement/pending-publication grace. `tombstoneAuditGrace` is at least
`metadataAuditGrace + orphanGrace` and, after cross-validating `StreamStorageConfig` and `MaterializationConfig`,
strictly exceeds every append/session/recovery terminal lifetime, worker claim, reader lease, pending protection,
provider operation timeout and maximum clock-skew allowance that could retain a response-loss retry. `enabled=false`
and `dryRun=true` both prohibit MARKED/DELETING/object-delete/tombstone-retirement mutations, but physical-root
registration、reader leases and protections remain active whenever F4 publication is enabled. Config never overrides
the cluster deletion capability bit.
Every duration is millisecond-representable and every grace/deadline addition uses overflow-checked arithmetic；an
overflow disables the candidate instead of wrapping a timestamp.

## 3. Physical-object Metadata

### 3.1 Identity and kind

```java
public record PhysicalObjectIdentity(
        ObjectKey objectKey,
        ObjectKeyHash objectKeyHash,
        Optional<ObjectId> objectId,
        PhysicalObjectKind kind,
        long objectLength,
        Checksum storageChecksum,
        Optional<Checksum> contentSha256,
        Optional<String> etag) { }

public enum PhysicalObjectKind {
    OBJECT_WAL(1),
    COMMITTED_COMPACTED(2),
    TOPIC_COMPACTED(3),
    RECOVERY_CHECKPOINT(4),
    CURSOR_SNAPSHOT(5),
    INDEX_OBJECT(6),
    FUTURE_CATALOG_OBJECT(7)
}
```

Object key hash is always SHA-256 of the exact key. Length/checksum/etag are immutable and compared with HEAD before
registration、pin、mark and delete.

### 3.2 Root

```java
public record PhysicalObjectRootRecord(
        int schemaVersion,
        String objectKeyHash,
        String objectKey,
        String objectId,
        int objectKindId,
        long objectLength,
        String storageChecksumType,
        String storageChecksumValue,
        String contentSha256,
        String etag,
        PhysicalObjectLifecycle lifecycle,
        long lifecycleEpoch,
        long createdAtMillis,
        long orphanNotBeforeMillis,
        String gcAttemptId,
        String referenceSetSha256,
        long markedAtMillis,
        long deleteNotBeforeMillis,
        long deleteStartedAtMillis,
        long deletedAtMillis,
        long tombstoneFirstAbsentAtMillis,
        String tombstoneProofSha256,
        String stateReason,
        long metadataVersion) { }

public enum PhysicalObjectLifecycle {
    ACTIVE(1),
    MARKED(2),
    DELETING(3),
    DELETED(4),
    QUARANTINED(5)
}
```

Allowed transitions：

```text
ACTIVE      -> MARKED
MARKED      -> ACTIVE          only before DELETING, if a veto/ref appears
MARKED      -> DELETING
ACTIVE      -> QUARANTINED     identity/corruption ambiguity
MARKED      -> QUARANTINED
DELETING    -> DELETED
QUARANTINED -> MARKED          explicit audited operator/reconciler action only

DELETED logically terminal; its audit record may be conditionally retired by §9.6
```

Every transition increments `lifecycleEpoch` exactly once. Returning `MARKED -> ACTIVE` clears attempt/digest/times
and increments the epoch. Reader leases from an older epoch may only finish within their original deadline and cannot
renew/admit new reads. A durable protection's recorded epoch is acquisition audit, not an automatic invalidation rule；
if its authoritative owner still matches, it remains a deletion veto and must be explicitly reconciled/reacquired
before a new owner-side mutation. `DELETING` never returns active；recovery must finish or quarantine without exposing
the object again. Retiring a `DELETED` root is not a lifecycle transition and never permits the deleted key to be
reused；new writes require a new attempt-addressed key and a still-live durable upload owner.

All non-DELETED records and a newly entered DELETED record encode
`tombstoneFirstAbsentAtMillis == 0` and empty `tombstoneProofSha256`. The tombstone coordinator may CAS
`DELETED -> DELETED` only to install or refresh those two audit-checkpoint fields；it does not change
`lifecycleEpoch`, `deletedAtMillis` or immutable identity. The proof digest hashes the object/root identity plus the
complete canonical owner absence and per-domain authority tokens from the first pass. This same-state CAS is not a
deletion authorization；it only makes the separated absence window restart-safe and bounded.
`tombstoneFirstAbsentAtMillis` is greater than `deletedAtMillis`; its digest is exactly 64 lowercase hexadecimal
SHA-256 characters. Timestamp zero and empty digest occur together. Any other combination, or either field on a
non-DELETED root, fails strict decode.

Decode/retrieval recomputes `ObjectKeyHash` from `objectKey`, recomputes the `000..255` first-hash-byte shard and
requires the exact root key/partition. A root found under the wrong shard is quarantined metadata, never silently
moved or read through another key.

Bootstrap of an existing generation-zero/cursor snapshot object creates `ACTIVE(epoch=1)` only after exact HEAD and
an authoritative reference scan. Until backfill coverage is complete, the cluster activation record keeps physical
delete disabled.

```java
public enum PhysicalRootBackfillStage {
    REGISTRY_SCAN, PROJECTION_READ, HEAD_COMMIT_SCAN, GENERATION_ZERO_SCAN,
    CURSOR_INVENTORY_SCAN, OBJECT_HEAD, ROOT_WRITE, PROTECTION_WRITE, FINAL_REVALIDATION
}

public record PhysicalRootBackfillFailure(
        String resourceIdentitySha256,
        PhysicalRootBackfillStage stage,
        String errorCode) { }

public record PhysicalRootBackfillRequest(
        String runId,
        long expectedBrokerReadinessEpoch,
        int maxConcurrentStreams,
        Duration timeout) { }

public record PhysicalRootBackfillReport(
        String runId,
        long brokerReadinessEpoch,
        long streamsScanned,
        long dataObjectsScanned,
        long cursorObjectsScanned,
        long rootsCreatedOrVerified,
        long protectionsCreatedOrVerified,
        long failureCount,
        Checksum dataCoverageSha256,
        Checksum cursorCoverageSha256,
        List<PhysicalRootBackfillFailure> boundedFailures) { }

public interface PhysicalRootBackfillCoordinator {
    CompletableFuture<PhysicalRootBackfillReport> run(
            PhysicalRootBackfillRequest request);
}
```

Run id/epoch/concurrency/timeout and bounded-failure rules match the registration backfill contract in document 06.
Both coverage digests incrementally hash every registry/projection/head/cursor authority token and every classified
object/root/protection outcome in canonical order. Counters cover the full traversal；failure details retain only the
first 100 hashed resource identities. A zero-object traversal is valid only when the stable authority scan itself is
non-empty/complete or independently proves there are no Nereus projections.

`PhysicalRootBackfillCoordinator` runs only after cold-topic stream registration coverage. It pages all 64 registered-
stream shards, then for each still-live projection pages generation-zero indexes、reachable commits and F3 cursor
snapshot inventory. It creates/verifies exact roots plus commit-owned `REACHABLE_APPEND`、generation-zero-index-owned
`VISIBLE_GENERATION` and `CURSOR_SNAPSHOT_ROOT` protections,
re-reads registration/projection/head/cursor versions and counts a stream covered only if they are unchanged. A full
pass with zero failures and the same broker/domain readiness epoch CASes the activation record's live-reference
data/cursor backfill proofs from the two report digests. New append/snapshot publication creates root/protection before visibility once this epoch begins, so
concurrent new objects are covered by their write hook. Object-store listing is not coverage evidence；unreferenced
or deleted-incarnation bytes enter the orphan/domain-proof path separately. Any unknown live index/target format
keeps deletion disabled.

Checkpoint W implements this contract in `DefaultPhysicalRootBackfillCoordinator`. The coordinator first requires the
same-epoch completed stream-registration proof, then walks every one of the 64 registration shards in canonical page
order. Up to `maxConcurrentStreams` streams are processed concurrently, while their per-stream data/cursor coverage
digests are folded in registry order so scheduling cannot change the report. A live registration is admitted only
after its L0 state/profile/head and the exact F2 binding/topic authority agree；stale non-live registrations remain
part of the stable classification digest but do not manufacture live references.

For each admitted stream, the implementation walks the recovery-root-anchored reachable commit tail and the complete
COMMITTED index prefix, accepting only generation-zero Object-WAL targets whose optional projection is empty or
equals the live registration. Exact object HEAD precedes every bootstrap. Commit-owned `REACHABLE_APPEND` and
index-owned `VISIBLE_GENERATION` reuse the same public reference-id formulas as the append path and re-read their
exact key/version/durable SHA through the retirement/generation stores during `acquireOrTransfer` and revalidation.
Cursor inventory requires an ACTIVE same-projection retention root, scans every cursor root, verifies the canonical
NCS1 key/HEAD metadata and installs `CURSOR_SNAPSHOT_ROOT` with the exact cursor key/version/envelope SHA as owner.

Before counting a stream covered, checkpoint W reloads the registration、full L0 snapshot、F2 authority、
recovery-root/head and complete cursor-authority digest. Only a zero-failure run whose activation record still has
the same broker readiness epoch、domain set and registration proof may install the two coverage proofs. Lost
activation-CAS responses converge only when a reload contains the exact desired data/cursor proofs. The report keeps
the first 100 redacted failures while counters cover the entire attempted traversal；`dataObjectsScanned` and
`cursorObjectsScanned` count successful reference handshakes, so one shared Object-WAL root may be verified once per
distinct commit/index owner. Checkpoint W does not use object listing, enable delete bits, implement the broker-side
registration backfill, or schedule production GC.

### 3.3 Reader lease

```java
public record ObjectReaderLeaseRecord(
        int schemaVersion,
        String objectKeyHash,
        String processRunId,
        String leaseId,
        long rootLifecycleEpoch,
        long acquiredAtMillis,
        long expiresAtMillis,
        long maximumReadDeadlineMillis,
        long renewalSequence,
        long metadataVersion) { }
```

One process has at most one durable lease key per object. A local ref count multiplexes concurrent reads. Lease id is
new after process restart or after a fully released lease.
`maximumReadDeadlineMillis <= expiresAtMillis - maximumClockSkew.toMillis()`；a read cannot outlive its durable
protection.

The implemented manager serializes operations per object hash、re-scans the durable process lease before every local
reuse and records a larger `maximumReadDeadlineMillis` with CAS even when the existing expiry already covers it.
When the deadline no longer fits, it extends the same lease id and increments `renewalSequence` before the root and
selection post-check. `close()` rejects new acquisitions；each returned handle owns an idempotent asynchronous
`release()` so the actual read completion path can await durable cleanup.

### 3.4 Durable protection

```java
public enum ObjectProtectionType {
    VISIBLE_GENERATION(1),
    REACHABLE_APPEND(2),
    MATERIALIZATION_SOURCE(3),
    MATERIALIZATION_OUTPUT(4),
    RECOVERY_CHECKPOINT_OBJECT(5),
    RECOVERY_CHECKPOINT_TARGET(6),
    CURSOR_SNAPSHOT_PENDING(7),
    CURSOR_SNAPSHOT_ROOT(8),
    REPAIR(9),
    CATALOG_SNAPSHOT(10),
    CATALOG_DELIVERY(11),
    RECOVERY_CHECKPOINT_PENDING(12)
}

public record ObjectProtectionRecord(
        int schemaVersion,
        String objectKeyHash,
        int protectionTypeId,
        String referenceId,
        String ownerKey,
        long ownerMetadataVersion,
        String ownerIdentitySha256,
        long rootLifecycleEpoch,
        long createdAtMillis,
        long expiresAtMillis,
        long metadataVersion) { }

public record ObjectProtectionIdentity(
        ObjectKeyHash object,
        ObjectProtectionType type,
        String referenceId) { }

public record ObjectProtectionOwner(
        String ownerKey,
        long metadataVersion,
        Checksum identitySha256) { }

public record ObjectProtectionRequest(
        PhysicalObjectIdentity object,
        ObjectProtectionType type,
        String referenceId,
        ObjectProtectionOwner owner,
        long expiresAtMillis) {
    public ObjectProtectionIdentity identity();
    public boolean isPending();
}

public record ObjectProtection(
        PhysicalObjectIdentity object,
        ObjectProtectionIdentity identity,
        ObjectProtectionOwner owner,
        long rootLifecycleEpoch,
        long createdAtMillis,
        long expiresAtMillis,
        long metadataVersion,
        Checksum durableValueSha256) {
    public boolean isPending();
}

public interface ObjectProtectionManager extends AutoCloseable {
    interface OwnerRevalidator {
        CompletableFuture<Void> revalidate(ObjectProtectionOwner expectedOwner);
    }

    interface RemovalAuthorizer {
        CompletableFuture<Void> authorizeRemoval(ObjectProtection protection);
    }

    CompletableFuture<ObjectProtection> acquire(
            ObjectProtectionRequest request, OwnerRevalidator ownerRevalidator);
    CompletableFuture<ObjectProtection> acquireOrTransfer(
            ObjectProtectionRequest request, OwnerRevalidator ownerRevalidator);
    CompletableFuture<ObjectProtection> revalidate(
            ObjectProtection protection, OwnerRevalidator ownerRevalidator);
    CompletableFuture<ObjectProtection> transfer(
            ObjectProtection protection, ObjectProtectionOwner newOwner,
            OwnerRevalidator newOwnerRevalidator);
    CompletableFuture<Void> release(
            ObjectProtection protection, RemovalAuthorizer removalAuthorizer);
}
```

The value states which authoritative owner can re-prove the reference. Protection keys are not reference counts；
duplicates with the same deterministic reference id compare equal, and a mismatched collision is corruption.
`ObjectProtectionIdentity.referenceId` is a non-empty canonical key component capped at 256 UTF-8 bytes；the key is
always rebuilt from all three identity fields. Callers cannot pass a raw protection key to delete or transfer.
`rootLifecycleEpoch` proves the create/post-check ordering. GC never ignores a protection solely because the root was
later unmarked to a newer epoch；it first revalidates `ownerKey/version/identity` and only a stale owner permits
conditional cleanup.

Permanent protections encode `expiresAtMillis == 0`. Only `CURSOR_SNAPSHOT_PENDING` and
`RECOVERY_CHECKPOINT_PENDING` are bounded intents and require
`createdAt < expiresAt <= createdAt + pendingProtectionDuration.toMillis()`. Their owner is the exact pre-publication
cursor or recovery root version plus an attempt/output digest. Before its owner CAS, the caller revalidates owner、object
root、protection and skew-safe non-expiry. After expiry, GC re-reads the owner：if publication succeeded it repairs
the permanent protection；otherwise it conditionally removes the pending record. A pending record never makes bytes
visible.

`DefaultObjectProtectionManager` is constructed with cluster、physical metadata store、pending duration、maximum
clock skew、orphan grace and clock. It uses 256 bounded process-local serialization stripes keyed by the complete
`ObjectProtectionIdentity`；cross-process ordering still comes only from the exact metadata CAS. A pending acquire
requires `now + maximumClockSkew < expiresAt <= now + pendingProtectionDuration` and every later revalidation also
requires `expiresAt <= createdAt + pendingProtectionDuration`. `close()` rejects acquire/revalidate/transfer but
continues to admit authenticated release so shutdown cannot strand cleanup.

`acquireOrTransfer` is the restart-recovery primitive for a workflow whose exact metadata record advanced before all
of its protection-owner CASes. It accepts only the same complete protection identity, object/root epoch, expiry and
`ownerKey`, requires a non-decreasing owner metadata version, rejects a same-version/different-digest contradiction,
revalidates the requested owner before and after CAS, and resolves response loss only from the exact desired durable
record. Arbitrary owner changes still require an explicit `transfer` handle；recovery cannot use this method to seize
another logical owner's veto.

## 4. Physical-object Store API

```java
public interface PhysicalObjectMetadataStore extends AutoCloseable {
    CompletableFuture<Optional<VersionedPhysicalObjectRoot>> getRoot(
            String cluster, ObjectKeyHash object);
    CompletableFuture<VersionedPhysicalObjectRoot> createRoot(
            String cluster, PhysicalObjectRootRecord root);
    CompletableFuture<VersionedPhysicalObjectRoot> compareAndSetRoot(
            String cluster, PhysicalObjectRootRecord root, long expectedVersion);
    CompletableFuture<Void> deleteRoot(
            String cluster, ObjectKeyHash object,
            long expectedVersion, Checksum expectedRootSha256);
    CompletableFuture<PhysicalObjectRootScanPage> scanRoots(
            String cluster, int shard,
            Optional<F4ScanToken> continuation, int limit);

    CompletableFuture<VersionedReaderLease> createOrCompareReaderLease(
            String cluster, ObjectReaderLeaseRecord lease);
    CompletableFuture<VersionedReaderLease> compareAndSetReaderLease(
            String cluster, ObjectReaderLeaseRecord lease, long expectedVersion);
    CompletableFuture<Void> deleteReaderLease(
            String cluster, ObjectKeyHash object, String processRunId, long expectedVersion);
    CompletableFuture<ReaderLeaseScanPage> scanReaderLeases(
            String cluster, ObjectKeyHash object,
            Optional<F4ScanToken> continuation, int limit);

    CompletableFuture<VersionedObjectProtection> createProtection(
            String cluster, ObjectProtectionRecord protection);
    CompletableFuture<VersionedObjectProtection> compareAndSetProtection(
            String cluster, ObjectProtectionRecord protection, long expectedVersion);
    CompletableFuture<Void> deleteProtection(
            String cluster, ObjectProtectionIdentity protection, long expectedVersion);
    CompletableFuture<ObjectProtectionScanPage> scanProtections(
            String cluster, ObjectKeyHash object,
            Optional<F4ScanToken> continuation, int limit);
}
```

All scan results include continuation plus the exact key/version/value. `scanRoots` accepts only `0..255` and scans
the root-only shard prefix；lease/protection scans cannot escape one object prefix. A repeated/out-of-prefix/non-
increasing page is an invariant failure. Production Oxia ranges are slash-aware and fixed-depth：roots/readers use
one-descendant bounds while protections use two-descendant bounds. Java-string prefix successor logic is forbidden；
the logical continuation scope still records the canonical `base + "/"` prefix.
`deleteRoot` rebuilds the shard/key, re-reads and strictly decodes the exact value, compares version/value SHA and
requires `DELETED` with a non-zero valid tombstone observation. It then performs one expected-version conditional
delete. Missing-key success is decided only by the coordinator's exact response-loss proof；the store never exposes an
ACTIVE/MARKED/DELETING root delete.

## 5. Resolve / Pin / Read / Release

### 5.1 First local acquisition

```text
load/create exact physical root R1
require R1.lifecycle == ACTIVE
create or CAS process lease L with:
  new leaseId
  rootLifecycleEpoch = R1.epoch
  expiresAt covering operation deadline + clock skew
reread root R2
require R2.version/epoch/identity == R1 and ACTIVE
revalidate exact generation/index/cursor-root selection
increment local ref count and return ObjectReadLease
```

If root changes between lease write and re-read, the lease is conditionally removed and acquisition retries/re-resolves.
This is the cross-key handshake：a GC mark may win, but then the reader cannot start IO.

### 5.2 Local reuse and renewal

Every logical read acquisition re-reads the root；it never admits new IO from a stale local ACTIVE cache. Existing
local lease reuse is allowed only when：

- durable lease identity/version is current；
- remaining time covers the new operation deadline + skew；
- root is still ACTIVE at the lease epoch；
- exact selected index/root reference is revalidated。

Renewal CASes the lease first and then re-reads ACTIVE root at the same epoch. If GC marked the object, renewal fails；
already admitted IO must complete/cancel before the old expiry and no new IO starts.

### 5.3 Release/crash

Local release decrements the ref count. At zero, it conditionally deletes the durable lease; response loss reloads and
compares lease id. Process crash leaves a bounded lease. GC treats it live until
`expiresAtMillis + maximumClockSkew.toMillis()` and never trusts broker-registry disappearance alone.

The read future owns the lease in a `whenComplete`/structured cleanup path. Decode exceptions、cancellation、executor
rejection and fallback release exactly once.

## 6. Durable-protection Handshake

For every permanent non-reader reference：

```text
load root R1; require ACTIVE and exact identity
putIfAbsent protection P(owner identity, R1.epoch)
reread root R2
require R2 same version/epoch and ACTIVE
reread authoritative owner and require owner identity/version still matches
return acquired protection
```

If any post-check fails, the manager conditionally deletes P only when the successful create response proves that
this invocation created that exact version. A recovered create after response loss never claims exclusive cleanup
ownership；failure leaves the protection as a deletion veto for reconciliation. A concurrent GC that marks after the
post-check waits the mark grace and rescans protections；it sees P and returns the root to ACTIVE before any
index/object deletion. An exact duplicate with the same owner/expiry/root epoch is idempotent；a different owner or
root epoch requires explicit transfer/reconciliation and is never overwritten by acquire.

Removing a protection is also owner-checked. `release` first loads the exact handle version、calls the owner-specific
`RemovalAuthorizer`、reloads the same version and only then conditionally deletes. A lost delete response succeeds
only after the exact key is observed absent；a changed owner/version fails without deletion. For example, a
visible-generation protection is removed only after its index is `DRAINING/RETIRED/deleted` and recovery root no
longer cites it.

Owner transfer prevalidates the new owner, uses the same-key protection CAS while the old protection remains a veto,
then revalidates the unchanged ACTIVE root and new owner. It preserves reference id、creation time and expiry；only
owner fields、root epoch and metadata version may change. If either post-check fails, the new durable value is not
rolled back or deleted because it remains the safe GC veto. An idempotent retry recognizes the exact new owner and
same immutable protection fields. `acquireOrTransfer` applies the stricter same-logical-owner monotonic rule used by
materialization task recovery, while this explicit handle transfer remains the only path between different owner
keys. Generation publication
transfers `VISIBLE_GENERATION` from the exact `PUBLISHING` task to the exact `COMMITTED` index. Recovery checkpoint
and cursor snapshot publication replace a bounded pending protection with a permanent root-owned protection before
removing the pending key. Response loss always reloads and compares both owner identities.

Generation-zero append uses a deliberate two-protection handoff. Before head CAS it creates permanent
`REACHABLE_APPEND`, owned by the exact immutable generic commit-intent key/version/value SHA. After head/replay proof,
generation-zero index materialization creates `VISIBLE_GENERATION`, owned by the exact index key/version/value SHA.
The first protection is retained through index lag and until an NRC1 root replaces live commit replay；the second is
retained until the generation-zero index itself retires. An intent that never became reachable is not cleared by TTL：
the reconciler waits orphan grace and proves unchanged intent、head、recovery root、index absence and reference domains
before conditional removal. The complete ordering and deterministic reference ids are document 03 §10.

## 7. Reference-domain SPI

Explicit protections make common scans local to an object, but GC also revalidates authoritative domains to detect a
missing/stale protection during migration or response loss：

```java
public enum GcReferenceQueryKind {
    REFERENCED_OBJECT,
    CURSOR_SNAPSHOT_CANDIDATE,
    OWNERLESS_ORPHAN_CANDIDATE
}

public record GcReferenceQuery(
        GcReferenceQueryKind kind,
        PhysicalObjectIdentity object,
        List<StreamId> affectedStreams,
        Checksum candidateEvidenceSha256,
        Checksum queryIdentitySha256) { }

public interface GcReferenceDomain {
    String domainId();
    int protocolVersion();
    CompletableFuture<GcReferenceSnapshot> snapshot(GcReferenceQuery query);
    CompletableFuture<Boolean> stillMatches(
        GcReferenceQuery query,
        GcReferenceSnapshot snapshot);
}

public record GcAuthorityToken(
        String authorityKey,
        long metadataVersion,
        Checksum identitySha256) { }

public record GcReference(
        String referenceType,
        String referenceId,
        String ownerKey,
        long ownerMetadataVersion,
        Checksum ownerIdentitySha256) { }

public record GcReferenceSnapshot(
        String domainId,
        int protocolVersion,
        Checksum queryIdentitySha256,
        boolean complete,
        boolean veto,
        long authorityCount,
        long referenceCount,
        List<GcAuthorityToken> authorities,
        List<GcReference> references,
        Checksum snapshotSha256) { }
```

The materialization-owned planning values wrap that core query without widening the SPI：

```java
public record GcCandidate(
        String candidateId,
        PhysicalObjectIdentity object,
        GcReferenceQuery referenceQuery,
        Checksum discoveryEvidenceSha256,
        GcCandidateRootState rootState,
        long rootMetadataVersion,
        long rootLifecycleEpoch,
        long discoveredAtMillis,
        long notBeforeMillis) { }

public record GcPlan(
        String gcAttemptId,
        GcCandidate candidate,
        List<GcReferenceSnapshot> domainSnapshots,
        List<GcPlannedProtectionRemoval> plannedProtectionRemovals,
        List<GcPlannedMetadataRemoval> plannedMetadataRemovals,
        Checksum referenceSetSha256,
        long markedRootMetadataVersion,
        long markedRootLifecycleEpoch,
        long deleteNotBeforeMillis) { }

public record GcPlannedProtectionRemoval(
        VersionedObjectProtection protection) { }

public record GcPlannedMetadataRemoval(
        String removalType,
        String key,
        long metadataVersion,
        Checksum durableValueSha256) { }

public record GcDomainSnapshotProof(
        String domainId,
        int protocolVersion,
        Checksum queryIdentitySha256,
        Checksum snapshotSha256) { }
```

Candidate/attempt ids are random 128-bit lowercase base32 and never authorize deletion. Candidate construction
uses either the exact ACTIVE root version/epoch (`ACTIVE_DISCOVERY`) or the current exact MARKED wrapper
(`MARKED_RECOVERY`) and hashes the root、manifest/inventory evidence and affected stream
set. Plan lists are canonical sorted/unique and bounded by `PhysicalGcConfig`; every planned protection retains its
exact key、full owner/value、root epoch、Oxia version and durable-envelope SHA, while every other metadata removal
retains a canonical type/key/version/envelope SHA. `referenceSetSha256` protocol v2 hashes the query identity、the
ordered `(domainId, protocolVersion, queryIdentitySha256, snapshotSha256)` proofs and those full removal facts. Each
`snapshotSha256` already commits the complete domain authority/reference set、counts and complete/veto bits, so the
compact proof is equivalent to copying the unbounded set into the root digest while remaining restart-recoverable. A
same-key protection owner/version change or any domain snapshot change therefore changes the digest and invalidates
drain. The marked
root version/epoch are filled only from the successful `ACTIVE -> MARKED` CAS. After restart, recovery uses the
current MARKED metadata version/epoch directly and never guesses the prior ACTIVE Oxia version or assumes versions are
consecutive. Every domain reference must also match one planned metadata removal by exact owner key、Oxia version and
durable identity SHA；a reference that is merely hashed but whose owner would survive deletion invalidates the plan.
`GcPlan` remains process-local. Before MARK, however, its destructive recovery facts are copied into a sealed durable
retirement journal because source metadata can disappear one key at a time after DELETING. The journal is evidence,
not a second correctness owner：only a physical root in MARKED/DELETING with the same object、attempt and
`referenceSetSha256` authorizes its use.

The persisted binary-v1 journal schema is deliberately sharded instead of embedding an unbounded plan in one Oxia
value：

```java
public record GcRetirementManifestRecord(
        int schemaVersion,
        String objectKeyHash,
        String gcAttemptId,
        int referenceSetProtocolVersion,       // exactly 2
        String queryIdentitySha256,
        List<GcDomainSnapshotProofRecord> domainProofs, // canonical, <= 32
        int protectionCount,
        int metadataRemovalCount,              // each count <= 100_000
        String referenceSetSha256,
        long createdAtMillis,
        long metadataVersion) { }

public record GcRetirementProtectionRecord(
        int schemaVersion,
        String objectKeyHash,
        String gcAttemptId,
        String protectionKey,
        long protectionMetadataVersion,
        String protectionDurableValueSha256,
        ObjectProtectionRecord protection,
        long metadataVersion) { }

public record GcRetirementRemovalRecord(
        int schemaVersion,
        String objectKeyHash,
        String gcAttemptId,
        String removalType,
        String removalKey,
        long removalMetadataVersion,
        String removalDurableValueSha256,
        long metadataVersion) { }
```

Entry keys are deterministic hashes of the source key within the physical-object shard and attempt subtree. Writers
create/verify every entry first, scan the exact fixed-depth prefixes, recompute protocol-v2 digest and counts, and only
then create the manifest. Thus presence of a manifest means the complete fact set was sealed；loose entries from a
crashed pre-MARK attempt grant no authority and may be swept later.

Checkpoint H implements these three values and `SecureGcIdGenerator`. `GcPlan.computeReferenceSetSha256` is the
pre-MARK digest operation；`GcPlan.fromMarkedRoot` accepts only the exact MARK response/reload carrying that digest and
attempt id, an Oxia version newer than the candidate and lifecycle `epoch + 1`. Construction rejects incomplete or
vetoing snapshots、query mismatch、configured count overflow、non-canonical lists、duplicate protection identities、
protections for another object/root epoch and any exact removal-fact drift.
Checkpoint I now consumes this value/proof boundary through these implemented seams：

```java
public enum GcReferenceCollectionStatus {
    CLEAR, VETOED, INCOMPLETE, LIMIT_EXCEEDED
}

public final class GcReferenceDomainRegistry {
    public List<GcReferenceDomainVersion> requiredDomains();
    public boolean contains(String domainId, int protocolVersion);
    public CompletableFuture<GcReferenceCollection> snapshotForDeletion(GcReferenceQuery query);
    public CompletableFuture<Boolean> stillMatches(GcReferenceCollection clearCollection);
}

@FunctionalInterface
public interface GcPlanMetadataRevalidator {
    CompletableFuture<List<GcPlannedMetadataRemoval>> reload(
        GcCandidate candidate,
        List<GcPlannedMetadataRemoval> expectedRemovals);
}
```

The registry constructor rejects an empty set、more than 32 domains and duplicate domain ids, then canonical-sorts by
`(domainId, protocolVersion)`. Collection is sequential under one absolute operation deadline. Every returned
snapshot must repeat the registered id/version and the exact query digest. An authority/reference count over config
is `LIMIT_EXCEEDED` even if the domain incorrectly reports complete/non-veto；incomplete and veto are distinct blockers,
and collection stops at the first blocker without turning a prefix into permission. `stillMatches` accepts only a
`CLEAR` collection containing the complete exact registered set and short-circuits on the first `false`; `null` is an
invariant violation. `PhysicalObjectGarbageCollector` uses package-private overloads so domain calls share its one
operation deadline rather than acquiring independent timeout budgets.

`GcPlanMetadataRevalidator` is mandatory, not a permissive default. It reloads the authoritative typed metadata facts
selected by the future source/orphan/cursor planner；the collector canonicalizes and compares the entire returned list
before MARK and twice during DRAIN. Checkpoint J implements three storage domains and checkpoint K implements the two
managed-ledger domains in the table below；checkpoint T adds the future sentinel and activation-gated ownerless-global
variants. Config defaults continue to keep mutation disabled and dry-run on.

The query is a core value；materialization's `GcCandidate` wraps it with retry/plan timestamps and never appears in
the SPI. Affected streams are sorted/unique and capped at 4,096；`REFERENCED_OBJECT` and
`CURSOR_SNAPSHOT_CANDIDATE` require at least one, while `OWNERLESS_ORPHAN_CANDIDATE` requires an empty set plus a
complete domain-wide absence proof. Query identity hashes the exact object、kind、
candidate-evidence digest and stream set. Evidence is the canonical manifest/reference record when present, otherwise
the canonical root/listing classification proof. Snapshot lists use canonical `(type/key/version/identity)` order. `complete=false` or
any count above configured in-memory limits requires `veto=true`；a domain may stream-hash a larger scan for
diagnostics but cannot truncate it into permission. `stillMatches(query, snapshot)` receives the full exact query and
repeats the authoritative scan；no domain may recover scope from a process-local snapshot-to-query cache.

Checkpoint K makes that bounded behavior reusable outside materialization. `GcReferenceDomainConfig` validates
page size in `[1, 1000]` and authority/reference limits in `[1, 100000]`；`GcReferenceSnapshotBuilder` canonical-sorts
both lists, retains at most the configured values, reports the first overflow as count `max + 1`, and forces
`complete=false/veto=true`. Checkpoint K's `unsupportedOwnerless` remains a compatibility helper；checkpoint T's
default domain constructors instead inject `GcGlobalReferenceScope.unsupported()`, whose explicit incomplete snapshot
flows through the same builder. `PhysicalGcConfig.referenceDomainConfig()` supplies the same limits to all
implementations.

Checkpoint J's concrete rules are：

- `generation-v1` scans both closed read views for every sorted affected stream. Generation zero is removable while
  non-tombstoned. Higher generation contributes a removable reference only in `DRAINING`；`PREPARED`、`COMMITTED` and
  `QUARANTINED` matching records set `veto=true`, while `RETIRED/ABORTED` are non-referencing authorities；
- `append-recovery-v1` reads optional recovery root plus the full root-anchored live tail. Root absence is an explicit
  domain-separated token. The observed head and every commit version/digest are authorities；a matching current NRC1
  root reference or live commit sets `veto=true` because neither can be retired by the physical-object plan；
- `materialization-v1` scans every durable task for the affected streams. Matching source/output references from
  `PLANNED/CLAIMED/OUTPUT_READY/PUBLISHING/RETRY_WAIT` set `veto=true`; terminal tasks remain workflow authorities but
  no longer own physical correctness references.

All three stop with incomplete+veto after the configured bound. At checkpoint J they also returned incomplete+veto for
`OWNERLESS_ORPHAN_CANDIDATE` because the stream-registration registry alone is a hint. Checkpoint T supplies the
required registration/physical-root/cursor backfill epoch plus exact installed-domain authority before the same scans
may consume the full registry as scope.

Checkpoint K's managed-ledger rules are：

- `projection-generation-v1` reads the exact per-stream `VirtualLedgerProjectionRecord` and, when present, the
  current `TopicProjectionRecord` selected by its managed-ledger name. Present authorities use canonical key、Oxia
  version and stored-envelope SHA-256；missing binding/topic uses a domain-separated absence token and vetoes. For an
  equal identity, `DELETED` clears、`DELETING` vetoes、and `OPEN/SEALED` require
  `nereus.generation-protocol=1`. A different current topic proves an old stream unaddressable only when both its
  incarnation and storage-class binding generation are strictly greater than the historical binding；
- `cursor-snapshot-v1` reads the exact F3 retention record and completely pages every cursor root for each affected
  stream. Retention absence is an authority；non-`ACTIVE` retention、retention/cursor projection mismatch、or cursors
  without a retention authority veto. An `ACTIVE` snapshot root matching the candidate object emits an exact
  `cursor-snapshot-root` owner reference and vetoes. A cursor-snapshot query carrying a non-cursor physical object kind
  also vetoes.

Both domains rerun the full query for `stillMatches` and use exact F3/F2 stored-envelope digests, so state、identity、
root or version drift cannot survive DRAIN. Their default constructors still return incomplete+veto for ownerless
queries；checkpoint T's runtime-facing constructors accept the protocol-neutral core global scope and scan its exact
streams. The shared builder/scope live in core specifically so `nereus-managed-ledger` does not depend on
materialization.

F4 registers：

| Domain | Revalidated authority |
| --- | --- |
| `generation-v1` | implemented J affected / T ownerless：both-view exact indexes and DRAINING eligibility |
| `append-recovery-v1` | implemented J affected / T ownerless：head + optional stable recovery root + complete live tail |
| `materialization-v1` | implemented J affected / T ownerless：task roots/output/source identities and active-task veto |
| `projection-generation-v1` | implemented K affected / T ownerless：exact F2 binding/current-topic authority、marker and strict old-incarnation unaddressability proof |
| `cursor-snapshot-v1` | implemented K affected / T ownerless：exact F3 retention authority + complete paged cursor roots and live-root veto |
| `future-catalog-sentinel-v1` | implemented T：veto if durable required domain set differs from installed plugins or deletion stage is disabled |

The durable cluster generation-activation record stores the exact required domain id/version set. Runtime startup
fails F4 readiness if its registered set differs. Future 6 must atomically add its capability/domain before writing a
catalog reference；F4 never interprets absence of a plugin as absence of references.

Checkpoint S implements this record/codec/exact-key store and its monotonic CAS authority, including a read-only `get`
that GC scans use without bootstrapping cluster state. It also freezes three backfill-proof slots、the broker readiness
epoch and V1's all-or-nothing physical/cursor deletion rule. Checkpoint T adds exact installed-domain comparison、
sentinel and full ownerless scans. The actual backfill coordinators and broker/runtime activation are still pending, so
production deletion remains disabled.

`projection-generation-v1` makes the per-topic downgrade fence usable by protocol-neutral GC. For each affected
stream, the implemented proof requires one of：the exact current identity is `DELETED`；the exact current live identity
has `nereus.generation-protocol=1`；or the current topic identity is strictly newer in both incarnation and storage
binding generation than the historical per-stream binding. Missing binding/current topic and `DELETING` never count
as absence. A multi-stream WAL object requires a proof for every slice. A true orphan with no stream owner still
requires the later complete projection/binding-domain absence proof；object-list absence is not enough. The
materialization module consumes only the domain snapshot/tokens and never parses Pulsar metadata itself.

## 8. Logical Retention Candidate

### 8.1 Policy snapshot

The Pulsar resolver must pass exact values rather than the current booleans：

```java
public record RetentionPolicySnapshot(
        long policyVersion,
        long retentionTimeMillis,      // -1=infinite, 0=no post-consume time retention
        long retentionSizeBytes) { }    // -1=infinite, 0=no post-consume size retention

public record NereusRetentionConfig(
        int statsScanPageSize,
        int maxConcurrentPlans,
        int maxQueuedPlans,
        Duration operationTimeout,
        Duration closeTimeout) { }

public record RetentionStatsToken(
        String key,
        long metadataVersion,
        Checksum durableValueSha256) { }

public record RetentionCandidate(
        StreamId streamId,
        long currentTrimOffset,
        long committedEndOffset,
        long cursorCut,
        long timeCut,
        long sizeCut,
        long candidateTrimOffset,
        long streamHeadMetadataVersion,
        long cursorRetentionMetadataVersion,
        long policyVersion,
        List<RetentionStatsToken> statsTokens,
        Checksum evidenceSha256,
        long plannedAtMillis) { }

public interface RetentionCandidatePlanner {
    CompletableFuture<Optional<RetentionCandidate>> plan(
            StreamId streamId, RetentionPolicySnapshot policy);

    CompletableFuture<Void> revalidate(
            RetentionCandidate candidate, RetentionPolicySnapshot policy);
}
```

Overflow converting minutes/MiB fails topic admission. Negative values other than `-1` are invalid.
The locked Pulsar validator rule is also preserved exactly：one dimension may not be `0` while the other is non-zero；
use `-1` to ignore one dimension, and `(0, 0)` to disable post-consume retention.
Backlog-quota limits、types、retention policies and the precise-time switch remain in the exact
`NereusResolvedTopicFeatures` snapshot in document 06；they are not collapsed into this logical-retention value or
used by the candidate formula.
Candidate construction requires canonical ordered/unique stats tokens, bounds them by the configured page/candidate
limit and hashes the complete head/cursor/policy/stats evidence. It returns empty when the computed candidate does not
advance the current trim or any scan is incomplete. The result is ephemeral；the trim service reloads and revalidates
every captured authority before entering F3's pending protocol.

Checkpoint AG implements this product-neutral surface. `RetentionPolicySnapshot.fromMinutesAndMebibytes` uses checked
unit conversion；`NereusRetentionConfig` accepts a stats page size in `[1, 512]`、positive plan/queue bounds and
positive millisecond-representable operation/close timeouts. Its current defaults are `128 / 8 / 1024 / 30s / 30s`.
The active planner consumes `statsScanPageSize` and `operationTimeout`. Checkpoint AH implements the shared process
lane that enforces `maxConcurrentPlans`/`maxQueuedPlans` against the lifetime of the returned async operation, not
merely its launch. Same-stream triggers coalesce onto one admitted root operation while caller completions remain
independent；queue overflow、whole-operation timeout and forced close fail with typed errors.

`DefaultRetentionCandidatePlanner` captures one `Clock.millis()` per attempt, scans at most 4,096 canonical stats
tokens, verifies each token's exact source-index key/version/durable SHA and COMMITTED-view range/commit/cumulative
identity, and captures the complete candidate twice. Authority drift retries at most four stable attempts. Final
`revalidate` repeats the same two captures with the original `plannedAtMillis` and requires byte-for-byte equivalent
candidate evidence. Missing/incomplete stats or stale source identity can only reduce the eligible boundary or return
empty；they never authorize a trim.

### 8.2 Candidate formula

Inputs from one stable planning attempt：

```text
now = one Clock.millis() captured for the whole attempt
H = current StreamMetadata/head/trim
C = current F3 CursorRetentionView under writable owner
P = exact topic policy snapshot

cursorCut = min(C.protectedFloorOffset, H.committedEndOffset)
timeCut   = largest verified range boundary <= cursorCut whose
            now - maxPublishTimeMillis > P.retentionTimeMillis;
            -1 contributes H.trimOffset;
            0 contributes cursorCut
sizeCut   = largest verified range boundary <= cursorCut such that
            totalCommittedLogicalBytes - cumulativeLogicalBytesAtBoundary
            >= P.retentionSizeBytes; -1 contributes
            H.trimOffset; 0 contributes cursorCut
policyCut = max(timeCut, sizeCut)       # stock retention deletes when time OR size eligible
candidate = min(cursorCut, policyCut)
```

If both dimensions are infinite, no policy trim occurs. If retention is absent/zero in both dimensions, consumed data
may advance to `cursorCut`. For a positive size limit, the next boundary that would make remaining logical bytes
strictly smaller than the limit is not deleted；this matches the locked ManagedLedger ledger-granularity rule at
Nereus range granularity. Missing/stale publish-time stats conservatively stop `timeCut`; size uses exact cumulative
sizes from committed truth. Age arithmetic is checked/saturating and never wraps. Candidate is never
aligned by physical object identity；it is an offset.

Before requesting trim, planner re-reads policy、head and owner retention root. Any version/session/lifecycle change
restarts. `PROTECTION_PENDING` and `TRIM_PENDING` are vetoes, not temporary values to ignore.

### 8.3 Logical trim call

```text
NereusManagedLedgerRetentionService.trim(reason)
  -> ownershipGuard.requireOwned()
  -> activationGuard.requireReady(LOGICAL_TRIM, liveProjection, activateIfAbsent=true)
  -> reload exact RetentionPolicySnapshot
  -> compute stable candidate
  -> activationGuard.revalidate(proof)
  -> planner.revalidate(candidate, policy)
  -> CursorRetentionCoordinator.requestTrim(cursorOwnerSession, candidate, reason)
  -> ownershipGuard.requireOwned() before callback success
```

The returned/admin promise completes after F3 proves logical trim ACTIVE/completed. Physical deletion is scheduled
later and is not included in that promise. No Phase 4 caller invokes `StreamStorage.trim` directly.

Checkpoint AG implements that exact order in `NereusManagedLedgerRetentionService`. Only retriable
`METADATA_CONDITION_FAILED` failures before `requestTrim` are retried, at most four times；once the F3 call starts it
is never replayed by this service. A no-op still performs the final ownership check. Lost ownership after durable F3
completion fails the broker callback and suppresses the completion observer, without rolling back the durable trim.
This checkpoint does not yet install the service in `NereusManagedLedger` or map Pulsar topic policy/admin calls；it
therefore does not claim broker-visible retention rollout or physical GC.

### 8.4 Backlog eviction

Size eviction can reuse stock `BacklogQuotaManager` because it advances the slowest durable `NereusManagedCursor`
through persisted `skipEntries`; dispatcher cleanup follows the existing fork hook. After cursor floors reconcile，
normal F4 trim computes the new candidate.

Time eviction is admitted only with Pulsar's precise expiry path. The non-precise stock path assumes multiple
BookKeeper ledgers (`getLedgerInfo/getLedgersInfo`) and remains rejected for Nereus virtual ledgers. Producer hold/
exception policies remain admission/backpressure behavior and do not authorize physical deletion.

## 9. Data-generation and Source Retirement

### 9.1 Per-index eligibility

An old physical index reference is retireable when either：

1. its entire range is below completed L0 trim and the exact source、stream snapshot and recovery-root version set
   remains unchanged through the retirement proof；or
2. for a `COMMITTED` source, a healthy strictly newer `COMMITTED` generation fully covers it, the recovery checkpoint
   records replacement evidence and append-replay/index-repair no longer needs the old target；or
3. for a `TOPIC_COMPACTED` source, a healthy strictly newer current `TOPIC_COMPACTED` generation fully covers its
   offset、commit-version and cumulative-size bounds with the same payload/projection identity。

Partial overlap is insufficient. `TOPIC_COMPACTED` never covers a committed source and COMMITTED NRC1 evidence never
covers a topic-compacted source. A quarantined/draining replacement does not count.

### 9.2 Whole-object eligibility

Object WAL may contain slices for multiple streams. The physical object can be marked only when every visible slice/
index reference is retireable and every stream's head/recovery-root tokens are included. One stream's trim or higher
generation cannot delete bytes for another stream.

The current repairable `ObjectReferenceRecord` is used to discover expected slices, then every index/head/root is
read authoritatively. A missing/mismatched manifest/reference/index quarantines the candidate.

For a live projection, recovery-checkpoint publication and source/index/object retirement require the exact
generation marker in the projection-domain proof. For an authoritatively deleted/unaddressable old incarnation, the
cluster activation/domain/backfill gates suffice；there is no topic marker to create after deletion. Orphan and
cursor-snapshot deletion follow the same distinction. This avoids both unsafe old-binary reads and an impossible
requirement to activate a non-existent topic.

### 9.3 Mark/drain/delete algorithm

```text
DISCOVER
  exact HEAD/root identity
  compute candidate.notBefore from durable root eligibility and the candidate-specific grace
    (source retirement includes sourceRetirementGrace here, before MARK)
  collect index refs + protection scan + reference-domain snapshots
  reload every typed planned metadata removal from its authoritative store
  requireReady(PHYSICAL_DELETE,
      DomainValidatedDeletionSubject(exactGcReferenceQuery,
          projectionDomainSnapshotSha256), false)
  classify planned visible/recovery protections vs veto protections
  compute canonical protocol-v2 referenceSetSha256 from compact domain proofs
    plus exact protection/removal facts

PREPARE RETIREMENT JOURNAL
  under object shard + attempt, create-or-verify one exact record per protection/removal
  scan the complete fixed-depth entry prefixes and reject missing/extra/duplicate facts
  recompute query/domain-proof/count/referenceSet identities from the scanned entries
  create manifest last, then reload and verify its exact stored envelope

MARK
  require now >= candidate.notBefore
  require the exact sealed journal for object + attempt + referenceSetSha256
  revalidate activation proof
  CAS root ACTIVE -> MARKED(
      new epoch, attemptId, digest,
      markedAt, deleteNotBefore = markedAt + drainGrace)
  no new reader/protection handshake can complete

DRAIN
  wait until deleteNotBefore
  scan leases; wait through every expiresAt + maxClockSkew
  rescan protections, typed metadata facts and all domains
  immediately before intent CAS, rescan leases/protections/metadata and reload exact MARKED root
  require the exact digest/authority set or:
      if no destructive side effect yet, CAS MARKED -> ACTIVE(new epoch)
      otherwise quarantine

DELETE INTENT
  revalidate activation proof and exact projection-domain snapshot
  CAS MARKED -> DELETING preserving attempt/digest

METADATA RETIREMENT
  load the root-authenticated journal; never reconstruct a shrinking plan from remaining source keys
  conditionally delete/retire every journaled F4 index key
  through SourceRetirementMetadataStore, conditionally delete exact generation-zero
    index + committed-slice/committed-append marker + checkpoint-replaced commit node
  conditionally remove planned visible/recovery protection keys
  reread roots and require no unexpected key/reference appeared

PHYSICAL DELETE
  HEAD exact expected immutable identity
  ObjectStore.deleteObject(expected identity)
    use If-Match first; retry without it only for conditional DELETE HTTP 405/501
    keep HEAD + both DELETE attempts under one absolute deadline
  ALREADY_ABSENT is idempotent only under the same DELETING root
  CAS root DELETING -> DELETED
  emit bounded structured audit log/metrics; DELETED root is the durable outcome
```

The root stores only the canonical digest, not an unbounded reference list. Before destructive work, recovery requires
the manifest object/attempt/digest to match that root and recomputes the digest from the journal entries. It also
revalidates still-live source facts while they remain；after a conditional delete succeeds, the journal preserves the
original version/SHA needed to classify a retry as exact progress rather than treating absence as permission. Journal
cleanup occurs only after the DELETED outcome/tombstone policy permits it. `sourceRetirementGrace` and `drainGrace`
are intentionally not interchangeable：the former is
part of candidate eligibility and preserves fallback/recovery policy before MARK；the latter starts at the successful
MARK and gives pre-existing readers/protection handshakes time to drain after new admissions are fenced.

Checkpoint G implements only the metadata-retirement calls used inside `METADATA RETIREMENT`. Checkpoint H implements
the bounded candidate/plan/digest values. Checkpoint I implements registry collection plus the `MARK`、`DRAIN` and
`DELETE INTENT` portions in `PhysicalObjectGarbageCollector`：

- the constructor requires exact `projection-generation-v1@1` registration and a non-null
  `GcPlanMetadataRevalidator`；
- `mark` refuses disabled、dry-run、premature and overflowed candidates without mutation, shares one deadline across
  domain/protection/metadata/root/activation calls, and converges a lost MARK CAS only by exact root reload；
- `advanceToDeleteIntent` waits through the maximum skew-safe reader expiry, performs the domain revalidation and a
  second final lease/protection/metadata scan, then reloads root and activation proof before the intent CAS；
- protection、metadata or domain drift before a destructive side effect conditionally unmarks to a fresh ACTIVE epoch；
  lost unmark and lost DELETING responses require an exact replacement reload；
- the class has no dependency on `SourceRetirementMetadataStore`、`ObjectAuditRetirementStore`、protection deletion or
  `ObjectStore.deleteObject`, so `DELETE_INTENT` is the terminal result of this checkpoint.

Candidate discovery and typed metadata-plan construction remain owned by later source/orphan/cursor coordinators.
Checkpoint T implements the future-catalog sentinel and ownerless-global variants, but production runtime composition
remains pending；the five affected/ownerless storage/projection/cursor domains are not production-composed.
Checkpoint M supplies the first DELETING-recovery coordinator and checkpoint N reaches the checkpoint-G
generation-zero-index delete primitive plus higher-index CAS. Marker/commit-node typed actions and their planner are
still pending. A missing key or lost metadata-delete response is resolved only under the unchanged DELETING
root/journal, never accepted from absence alone.

Checkpoint L's protocol foundation changes the canonical digest to protocol v2 and adds the validated manifest、domain
proof、protection/removal records plus explicit binary-v1 codecs and frozen vectors. Its durable-store slice is also
implemented：`F4Keyspace` derives fixed-depth object/attempt/typed-entry keys；production and fake metadata stores
provide exact create-or-reload plus bounded token-scoped scans；`DefaultGcRetirementJournal.prepare` writes all
entries, scans exactly the expected counts, recomputes the v2 digest and creates the manifest last. `load` accepts only
a present manifest and reconstructs a count- and digest-verified snapshot. Entry/manifest response loss converges by
exact reload, while a missing、extra、conflicting or cross-attempt entry fails closed.

The collector now requires this service. Before activation acquisition、ACTIVE-root reload or `markCas`, it seals and
field-by-field authenticates the journal against the candidate facts and computed digest；a PREPARE error leaves the
root ACTIVE. `advanceToDeleteIntent` authenticates the sealed snapshot once before drain work and again after the
final exact MARKED-root reload, immediately before activation revalidation and the intent CAS. A missing or mismatched
journal therefore leaves the root MARKED. `phase4M4RetirementJournalCheck` freezes these orderings and the two-load
contract in addition to store/writer restart and response-loss behavior.

Checkpoint M adds the initial root-authenticated destructive-recovery implementation. `SourceRetirementCoordinator`
accepts only `DELETING/DELETED` roots, reloads the exact sealed journal, rejects every unregistered metadata-removal
type before mutation, dispatches journaled metadata keys through a closed type-owned handler registry, conditionally
removes only journaled protections, then performs exact immutable HEAD/delete and CASes the same attempt to `DELETED`.
An already absent object is accepted only after the matching root and journal have authenticated the attempt；a later
restart from the exact `DELETED` record is idempotent. Focused tests freeze successful deletion、already-absent
convergence and missing-journal failure before object access. Production handlers for generation-zero/higher index
families, response-loss crash cuts for every metadata/protection/root CAS, runtime composition and the final M4 gate
remain pending, so this checkpoint does not enable production deletion.

Checkpoint N supplies the first production metadata handlers reached by that registry. A sealed removal of type
`generation-zero-index` is routed by strict canonical key decoding, reloaded through `GenerationMetadataStore`, and
conditionally deleted through `SourceRetirementMetadataStore` only when key、version and stored-envelope SHA still
match. A `generation-index` removal must name the exact `DRAINING` higher record；the handler CASes it to `RETIRED`
while preserving every immutable publication field and binds the terminal reason to attempt + reference-set digest.
Lost delete/CAS responses reload the exact key under the same DELETING root/journal and accept only absent or that
deterministic replacement. The plan validator additionally requires removal type to equal the reference type.

`SourceRetirementCoordinator` now reloads the exact DELETING wrapper and the identical sealed journal before every
metadata/protection batch, before physical object access, after uncertain protection/object responses, and before the
final DELETED CAS. The DELETING wrapper comparison includes Oxia version and durable-envelope SHA；journal comparison
includes manifest and every entry wrapper. If either disappears or changes, later batches/object HEAD are not issued.
Checkpoint O adds the remaining two generation-zero source action types. `generation-zero-marker` and
`generation-zero-commit` route only through the focused adapter's exact-key APIs. The adapter strictly decodes the
current cluster/stream/family, reads the value and reconstructs the canonical legacy/generic identity；this is
necessary because a legacy marker key contains a one-way slice hash. Key/value alias、wrong family、non-canonical
component or foreign cluster fails before delete. Both handlers require the sealed key/version/envelope SHA, and an
uncertain delete converges only after exact-key absence under the same authenticated root/journal.

`SourceRetirementPlanBuilder` now scans the complete bounded generation namespace and, for each matching
generation-zero index, requires one unchanged recovery root reference that covers the same range/commit version. It
strictly opens that NRC1, finds the covering commit, then compares NRC1 canonical commit SHA、exact source commit、
index fields/read target and committed marker before returning three separate removals. It reloads the recovery root
after all source reads and implements `GcPlanMetadataRevalidator` as closed type-owned exact-key reloads. Tests freeze
legacy/generic inverse routing、response-loss cuts、root drift、a different canonical NRC1 commit and rejection of an
otherwise-existing source removal that cannot be reconstructed from a candidate-owned generation index.

Checkpoint P closes the generation-zero healthy-replacement half of §9.1. The planner resolves each bounded NRC1
publication reference independently, strictly decodes and hashes its canonical `GenerationIndexRecord`, then accepts
it only when the current exact Oxia index is still `COMMITTED` with the same canonical durable envelope. The decoded
target must name another physical object whose exact root is `ACTIVE` and matches object key/hash/id/kind plus slice
bounds. Missing、quarantined/draining/drifted current values make that publication unavailable；malformed or
non-canonical checkpoint evidence is an invariant failure. At least one referenced publication must remain healthy.

After reading the exact source commit and marker, the planner reloads the selected current index/root wrappers and
then the recovery root. Its `GcPlanMetadataRevalidator` reconstructs this complete candidate-bound proof again rather
than accepting unchanged removal keys. Focused tests freeze success and reject a QUARANTINED replacement index、a
MARKED replacement root、index/root drift during freeze、root drift and unbound source removals.

Checkpoint Q completes the replacement-backed COMMITTED-view half of higher-generation §9.1 eligibility. The
pre-drain verifier walks the source's whole NRC1 range and predecessor chain, reproduces its count/size/schema facts,
requires a strictly newer current COMMITTED/ACTIVE replacement for every entry, and reloads all replacement facts、
recovery root and source. Only then may the coordinator CAS `COMMITTED/QUARANTINED -> DRAINING` under an unchanged
ACTIVE candidate-root fence. A DRAINING source must pass the same proof again when the removal plan freezes it, so a
later replacement/index/root degradation blocks MARK/DELETE planning.

Checkpoint R completes the remaining §9.1 source-eligibility branches. `CompletedTrimRetirementVerifier` requires the
whole source range below the current L0 trim, freezes the exact source、full `StreamMetadataSnapshot` and optional
recovery-root wrapper, then rereads all three. Generation-zero uses that proof as the alternative to checkpoint P's
healthy NRC1 replacement；higher COMMITTED and TOPIC_COMPACTED sources use it before consulting view-specific
replacement facts.

For an untrimmed TOPIC_COMPACTED source, `TopicCompactedReplacementVerifier` performs a bounded same-view scan and
selects a strictly newer current COMMITTED index whose range、commit-version、cumulative-size、payload and projection
cover the source. Its target must be another object with the exact topic-compacted physical format and an
`ACTIVE/TOPIC_COMPACTED` root. The selected index/root and source are reread before the proof returns；a DRAINING
removal-plan reproof repeats the same checks. This closes source eligibility only；the global-domain/runtime/final-GC
gates below remain mandatory, so production deletion stays disabled.

If a process crashes after `DELETING`, another process resumes; the object never becomes readable again. If it crashes
after physical delete before root CAS, HEAD/`ALREADY_ABSENT` plus exact root identity completes `DELETED`.

### 9.4 Higher-generation lifecycle

For an F4 index selected for retirement, the coordinator first CASes `COMMITTED/QUARANTINED -> DRAINING`, so new
resolvers ignore it even before object mark. It becomes `RETIRED` after physical delete or after another active object
reference keeps shared bytes but this generation ref is removed. Generation-zero frozen records have no lifecycle；
the physical root mark and exact pin post-check provide the same new-reader fence.

Checkpoint Q implements the first transition for replacement-backed COMMITTED-view sources. Checkpoint R extends the
same transition to completed-trim sources in either view and replacement-backed TOPIC_COMPACTED sources. Before any
L0、generation、checkpoint or root read, the coordinator returns `NOT_ELIGIBLE_YET` while candidate
`notBeforeMillis` has not passed. It writes `physical-gc-pre-drain:{candidateId}` only after the selected exact
eligibility proof and an unchanged candidate-root fence；CAS response loss accepts only the exact replacement、the
unchanged original or the same immutable publication already in DRAINING. Checkpoint N implements the second
`DRAINING -> RETIRED` transition. Its restart marker is
`physical-gc:{gcAttemptId}:{referenceSetSha256}` with `stateChangedAtMillis == deleteStartedAtMillis`; any other
RETIRED value is drift, not idempotent success.

### 9.5 Fallback grace

After a new generation publishes, source retirement waits `sourceRetirementGrace` plus successful full validation and
checkpoint publication. During grace, checksum failure can fall back. After the last lower generation is physically
retired, the new object may be the sole durable copy；later corruption is a data-integrity incident, not a reason to
resurrect deleted source metadata.

### 9.6 Deleted-root and object-audit retirement

Keeping every `DELETED` root and the Phase 1 `ObjectManifestRecord` / `ObjectReferenceRecord` forever would make
metadata cardinality `O(all historical physical objects)`. F4 therefore has a separate, deliberately slow retirement
pass. It does not change the `DELETED` lifecycle outcome and it is never part of the data-delete critical path.

```java
public enum TombstoneRetirementStatus {
    DISABLED, DRY_RUN,
    RETIRED, NOT_OLD_ENOUGH, OWNER_PRESENT, HANDLE_PRESENT,
    DOMAIN_VETO, OBJECT_PRESENT, VERSION_CHANGED, QUARANTINED
}

public record TombstoneRetirementResult(
        ObjectKeyHash object,
        long rootMetadataVersion,
        TombstoneRetirementStatus status,
        boolean referencesRetired,
        boolean manifestRetired,
        boolean rootRetired) { }

public interface PhysicalRootTombstoneRetirementCoordinator {
    CompletableFuture<TombstoneRetirementResult> retire(
            VersionedPhysicalObjectRoot deletedRoot);
}
```

The coordinator accepts only an exact `DELETED` wrapper returned by the 256-shard root scan. It captures one deadline
and performs this order:

```text
capture one now from the injected Clock for this pass
load exact DELETED root D1;
  require deletedAt + tombstoneAuditGrace + maximumClockSkew < now
HEAD exact key; page leases/protections and every generation/task/checkpoint/cursor/catalog/append owner;
  query every activated domain and require object absent plus complete no-reference proofs
if D1 has no first observation:
  CAS DELETED -> DELETED with firstAbsentAt=now and canonical proof digest; stop this pass
require firstAbsentAt + orphanGrace + maximumClockSkew < now; no in-memory sleep/timer is retained
reload D2 and require D2 key/version/value SHA/lifecycle epoch exactly equal D1
repeat HEAD, every owner/handle/domain scan and require the same canonical proof digest
if objectId exists, load exact optional ObjectReferenceRecord and ObjectManifestRecord audit wrappers
HEAD exact key again and require absent
conditionally delete ObjectReferenceRecord by captured key/version/value SHA
conditionally delete ObjectManifestRecord by captured key/version/value SHA
reload both optional audit keys and require absent
repeat owner/domain scan and HEAD absence; reload exact unchanged D2
deleteRoot(objectKeyHash, D2.version, D2.durableValueSha256) as the final metadata action
```

Checkpoint U implements this contract in
`DefaultPhysicalRootTombstoneRetirementCoordinator`. One public call captures one non-negative clock value and shares
one monotonic operation deadline across all metadata and object-store cuts. The first clear pass computes
`nereus-deleted-root-tombstone-proof-v1` from the immutable/deletion-attempt root facts、the ownerless query identity、
explicit reader/protection absence markers and every canonical domain snapshot digest, then uses the permitted
`DELETED -> DELETED` CAS to persist `tombstoneFirstAbsentAtMillis` and the proof. No scheduler or in-memory wait retains
the candidate. A later scanner pass must observe both strict grace boundaries and the same proof before it can read or
delete an audit key.

The implemented coordinator treats any persisted reader lease or protection as `HANDLE_PRESENT`, any emitted domain
reference as `OWNER_PRESENT`, and an incomplete/vetoing reference-domain pass as `DOMAIN_VETO`. Each blocker clears the
persisted absence observation by exact root CAS. An exact reappearing object is deleted only after the root is reloaded,
handles remain absent and the previously captured ownerless collection passes `stillMatches`; delete-response loss is
accepted only after exact HEAD absence. A mismatched HEAD returns `QUARANTINED` without provider deletion. Optional
Phase 1 references and manifest are captured against the root identity, reauthenticated before each mutation, deleted
in that order and reloaded absent. The final root delete uses both Oxia version and durable-value SHA；a lost response
converges only when that exact key is absent.

Both separated HEAD absence observations are direct, non-cached requests. Root-shard scans resume the durable first
observation after restart and keep only one candidate/page in memory. If its authority digest changes, if the object
is present or if any owner appears, the coordinator conditionally clears/replaces the observation only after the same
DELETED root is reloaded and starts a new window；it never carries an older proof forward. A reappearing object is
unreadable under DELETED. If HEAD matches the immutable root identity, the coordinator reruns the full ownerless-domain
proof, deletes the bytes under that exact DELETED root, clears the observation and starts a new window. A mismatched
HEAD identity returns audit `QUARANTINED`, emits an alert and retains the root for operator resolution；it never
deletes unknown bytes.
A response-loss delete of either L0 audit record is
accepted only after the same `DELETED` root/version is reloaded and the exact audit key is absent. A missing audit
record is otherwise normal for F4 compacted/checkpoint/cursor objects. Any unknown object kind、reserved owner prefix、
decode failure、domain overflow or backend that cannot provide exact HEAD absence returns `QUARANTINED` and retains
the root.

No single snapshot version is invented across authorities：the second pass validates every domain's own authority
tokens and the canonical domain-set digest. `ObjectReferenceRecord` is removed before its manifest, and the root is
always last, so a crash leaves a rediscoverable `DELETED` coordinator root. Once the root is gone, a stale object-store
listing or externally reappearing byte is a missing-root orphan and must pass the full ownerless-domain proof before
deletion；list age alone is still insufficient.
If the final root-delete response is lost, absence of that exact root key completes the same attempt；any reappearing
value is strictly decoded and a different version/identity is never treated as success.

Every writer enforces the other side of this protocol. Before the first PUT and before every provider retry it reloads
its exact durable append/session, task-claim, checkpoint or cursor owner and requires the physical root to be absent or the
same `ACTIVE` identity. An absent root is legal only for that current owner. `MARKED/DELETING/DELETED/QUARANTINED`,
owner absence/version drift or a lost claim aborts the PUT. A later attempt allocates a fresh 128-bit attempt id (and
for Object WAL a never-reused process-run/sequence identity), hence a fresh object key. This rule, plus the long
tombstone grace and final owner scan, prevents a stopped process from resurrecting bytes after the audit tombstone is
retired.

## 10. Cursor Snapshot GC

### 10.1 Rollout/backfill

Before snapshot deletion is enabled：

- list every known F3 cursor-snapshot prefix page；
- create exact physical roots for discovered keys older than orphan grace；
- create `CURSOR_SNAPSHOT_ROOT` protections for every ACTIVE root reference；
- register F4 pinning in `DefaultCursorSnapshotStore.read`（implemented in checkpoint V）；
- prove a complete paginated cursor/root inventory at the configured 10,000-root limit；
- set the cursor-snapshot GC coverage bit in generation activation metadata。

Until the bit is set, scanner reports candidates but delete is globally disabled.

### 10.2 New snapshot publication

F4 wraps the existing F3 sequence without changing its cursor visibility point：

```text
capture exact current cursor root/session/version and a fresh snapshot attempt id
guarded upload of immutable attempt-addressed snapshot;
  before every provider transmission, reload that cursor owner and require the optional
  physical root absent or the same ACTIVE identity
register physical root + bounded CURSOR_SNAPSHOT_PENDING protection
  owned by exact current cursor root/session/version + attempt/output digest
revalidate pending protection/root/owner and skew-safe expiry
CAS CursorStateRecord to new snapshot ref       F3 visibility remains here
create CURSOR_SNAPSHOT_ROOT protection for exact root version/ref
remove pending protection after re-proving permanent root protection
old snapshot remains candidate only after old root version disappears
```

If the cursor-root CAS loses, pending protection is retained only through its bounded expiry/orphan grace and then
reconciled. If root CAS wins but its response or permanent-protection completion is lost, every later hydrate/read
loads the exact ACTIVE cursor root, creates/transfers and revalidates `CURSOR_SNAPSHOT_ROOT`, and only then admits a
durable reader lease；the original pending protection may remain until bounded expiry. The authoritative cursor
reference domain therefore vetoes deletion even while the permanent key is temporarily absent.

Checkpoint V implements this with `CursorSnapshotWriteAuthority` and `CursorSnapshotPublication`.
`DefaultCursorStorage` visibly orders `prepareWrite -> compareAndSetCursor -> completeWrite`；strict APPLIED success
comes only after permanent protection and pending release. `DefaultCursorSnapshotStore.prepareWrite` uses the
replayable guarded PUT hook so every provider attempt reloads the exact captured cursor root and rejects owner/root
drift or any non-`ACTIVE` conflicting physical root. `read` performs an identity-only HEAD, converges permanent
protection from the live root, acquires `ObjectReadPinManager`, then performs the second HEAD/range read/decode and
releases the lease on both success and failure. The initial HEAD never admits byte consumption and a concurrent
MARK blocks protection/pin acquisition before range IO.

### 10.3 Candidate deletion

For each discovered unreferenced snapshot candidate：

```text
build CursorSnapshotInventory from:
  exact versioned retention root
  complete versioned cursor-root scan
  complete discovered-object set
require !inventory.deletionVetoed()
require candidate in unreferencedCandidates and older than grace
mark physical root
drain reader leases
repeat full list/root scan and inventory classification
require inventory.stillMatches(latest retention/roots)
require candidate still unreferenced and authority owner/session/version unchanged
CAS DELETING and delete exact object
```

An owner change、cursor create/recreate/reset/delete、snapshot replacement or either pending retention lifecycle
invalidates the attempt. A same-name topic recreation uses another stream/projection prefix and cannot authorize old
incarnation deletion without old-stream lifecycle/reference proof.

Checkpoint AJ implements the read-only discovery and post-drain revalidation portion of this sequence.
`CursorSnapshotGcScanner` first reads the exact retention root, completely pages cursor roots and the canonical F3
object prefix, and refuses any pass that exceeds the configured hard maximum of 10,000 roots、objects or protections.
Prefix membership is insufficient：`CursorSnapshotKeys.parse` proves the exact
`cursorNameHash/cursorGeneration/snapshotId.ncs` grammar. An eligible candidate additionally requires a listing
`lastModified` older than `orphanGrace + maximumClockSkew`、an exact ACTIVE `CURSOR_SNAPSHOT` physical root whose
length/ETag match the listing、and a complete protection set containing only same-root-epoch cursor protections.
Permanent protections are removable only against the complete current cursor-root inventory；pending protections are
removable only strictly after `expiresAt + maximumClockSkew`. Unknown age、missing root、non-ACTIVE root、identity
mismatch、foreign protection and malformed keys are counted but never emitted.

The scanner binds retention/live-reference facts、the exact listing、the normalized ACTIVE owner lifecycle epoch and
every planned protection version/digest into `CURSOR_SNAPSHOT_CANDIDATE` evidence, then invokes one asynchronous
visitor at a time under one whole-pass deadline. Discovery time、root metadata version and the transient ACTIVE wrapper
digest are deliberately excluded because an exact `ACTIVE -> MARKED` transition must remain reconstructable after a
fresh process starts. After the central collector has marked and drained the root, `revalidate(candidate)` repeats the
complete list/retention/root/protection cut, accepts only the exact ACTIVE root or its one-epoch MARKED successor, and
returns false for ordinary owner/root/list/reference drift. The new
`PhysicalObjectGarbageCollector.FinalCandidateRevalidator` runs this callback after its second reader/protection/
metadata drain and immediately before the final root/journal/activation fence；false rolls MARKED back to ACTIVE,
while a failed/incomplete revalidation leaves MARKED for retry. This preserves one deletion correctness owner.

Checkpoint AK implements `recoverMarked` and the exact conditional `unmarkDrifted` rollback. Its
`CursorSnapshotGcExecutor` constructs the canonical candidate/plan, invokes the central mark/drain/final-revalidation
fence, then resumes journal-authenticated protection and exact-object retirement from DELETING. A restart can recover
MARKED without in-memory discovery state or resume DELETING without object-store listing. Changed complete evidence
cannot reuse the old digest and is unmarked without deleting bytes；an uncertain scan remains MARKED.

`Phase4PhysicalGcRuntime` composes the executor with all six domains and is owned by the production provider/runtime.
Checkpoint AN extends it with the lifecycle service、root/registration/inventory scanners、registration retirement、
the complete source-metadata retirement registry and root-state router. The existing broker mapping continues to
construct `PhysicalGcConfig.defaults()` (`enabled=false, dryRun=true`), and the runtime calls the lifecycle service's
`start()` only when `enabled` is true. The cursor-snapshot coverage bit、broker GC knob mapping and physical-delete
activation remain required before production cursor snapshot deletion is available.

## 11. Orphan Discovery

`PhysicalObjectRootScanner` is the authoritative lifecycle work scanner. A complete pass visits shards `000..255` in
order and pages `scanRoots`; end-of-shard clears the continuation and a later pass restarts from every prefix. Runtime
composition must route `ACTIVE` roots to candidate evaluation、`MARKED/DELETING` roots to exact recovery even when
object-store listing is empty, and `DELETED` roots to the bounded tombstone-retirement coordinator rather than an
in-memory delay queue. Watch events may only accelerate a complete pass. This metadata-first design lets a later
coordinator converge delete-response loss from Oxia truth after bytes are already absent.

Checkpoint I implements the complete-pass scanner primitive. One `scan(visitor)` call traverses all 256 shards in
ascending order with `metadataScanPageSize`, validates that the first key of every continuation page is strictly after
the prior page, invokes only one visitor future at a time, and returns exact ACTIVE/MARKED/DELETING/DELETED/QUARANTINED
counts. Per-page and per-visitor calls each receive a bounded operation deadline. An overlapping scan fails, visitor or
metadata failure releases the admission flag for a later pass, and `close()` rejects new scans without closing the
borrowed metadata store/scheduler. Checkpoint AN supplies lifecycle-specific routing、fixed-delay scheduling and
recovery composition；the scanner itself never consults object-store listing.

Checkpoint AL implements `ObjectInventoryScanner` for the exact object-key families written by the current runtime：
Object-WAL v1、COMMITTED compacted v1、TOPIC_COMPACTED v1、NRC1 recovery checkpoints and NCS1 cursor snapshots.
Each family owns a canonical directory prefix and strict forward/inverse key grammar；prefix membership alone is not
an identity. Candidate classes include：

- output uploaded but no task/output/index reference；
- PREPARED/ABORTED publication output after retry grace；
- CAS-lost cursor snapshot；
- checkpoint upload whose root CAS lost；
- legacy manifest with no reachable slices after migration coverage；
- physical object whose metadata root is missing。

One complete pass visits the canonical non-overlapping family prefixes in order and completely pages each prefix.
For every listed key it first applies the strict inverse and checks for an existing root. A missing root is eligible
for registration only when listing `lastModified + orphanGrace + maximumClockSkew <= passStart`、an exact HEAD still
has the same key/length and, when listing supplied it, ETag、the HEAD length is positive and its storage checksum is
CRC32C, and a second root read is still absent. Disabled or dry-run mode records `WOULD_REGISTER` without mutation.
Enabled non-dry-run mode creates an exact ACTIVE root whose `createdAtMillis` is listing `lastModified` and whose
`orphanNotBeforeMillis` is `passStart + orphanGrace + maximumClockSkew`, thereby imposing a second full grace before
root-authoritative GC can even evaluate it. A lost create response converges only when reload returns that complete
desired root record；a concurrent different root becomes `ROOT_CONFLICT`.

Listing is eventually consistent audit/discovery input used mainly for bytes uploaded before any root CAS. It never
proves absence, never produces a GC plan and the scanner contains no MARK、protection-retirement or object-delete
operation. Missing `lastModified`、young/future timestamps、malformed keys、stale listing 404、HEAD/list mismatch and
root conflict each receive an exact report outcome and no mutation. Unknown prefixes are never enumerated. Checkpoint
AN invokes it only after the complete root and registration passes of an enabled lifecycle；a single empty listing is
never proof of orphanhood, and the current safe-default broker mapping does not start that lifecycle.

## 12. Stream-registration Retirement

A stream registration is one record per stream incarnation, so deleted incarnations must eventually leave the 64
active scan shards. Its removal is the final metadata-retirement step, never an input that authorizes another delete.
Checkpoint AM implements `StreamRegistrationRetirementCoordinator` and the protocol-neutral
`StreamRetirementReferenceAuthorityReader`/exact snapshot contract. The managed-ledger implementation scans the
stream's F3 retention record plus every cursor page, retains the exact key/version/value-digest authorities and is
reference-free only for a complete bounded snapshot with stable ACTIVE retention and only DELETED cursor records.
Transitional retention, a non-terminal cursor, NPR1 mismatch, pagination overflow or authority drift fails closed.

The coordinator may call `deleteStreamRegistration(..., expectedVersion)`（implemented by
conditional `deleteIfVersion`）only after one complete proof：

```text
load registration R1 + version
decode ref and obtain projection-generation-v1 deleted/unaddressable proof
require authoritative L0 StreamMetadata is DELETED at captured version
page every task; retire only terminal/audit-grace-expired tasks and require prefix empty
page both view index namespaces; conditionally retire audit-grace-expired ABORTED/RETIRED
  records and require no generation-zero or higher-generation key remains
require live commit prefix empty/retired and every checkpoint protection/reference
  retired through append-recovery-v1; conditionally delete recovery root and require absent
require generation-v1/materialization-v1 snapshots contain no live reference for stream
conditionally remove every materialization checkpoint、retention-stats and generation-sequence key;
  rescan each prefix and require empty
reload projection proof, L0 tombstone and R2
require every captured version/digest unchanged and R2.version == R1.version
deleteIfVersion(registrationKey, R1.version, registryShardPartitionKey)
```

Recovery-root removal itself is conditional and occurs only after the `DELETED` stream has no live commit/index/task
state and all referenced checkpoint objects/protections are retired. Terminal task/index deletion obeys the metadata
audit grace in document 04；therefore the registration remains discoverable throughout that grace. Any page/limit
failure、unknown lifecycle、domain
veto or changed version retains the registration. Response loss is resolved by re-reading the exact key；absence is
success only for the same completed proof attempt. A cached scanner entry remains harmless because it reloads the L0
tombstone/projection before mutation. Same-name topic recreation has a different `StreamId` and registration key.

Owner-backed protections are removed before their terminal index/task/recovery-root metadata owner, never after it.
If the process stops in that interval, the still-present owner/root remains visible to its reference domain and vetoes
physical deletion. NRC1 root publications and source/target identities reconstruct the permanent protection scope;
pending root protections that have no durable owner object may remain as conservative stale vetoes and are not used as
retirement success evidence. After every metadata prefix is empty, the coordinator re-captures the exact registration,
L0, projection and external authority snapshots before the registration-last CAS. Current focused tests cover the
empty deleted-stream path, final registration-delete response loss, final external-authority drift, non-DELETED L0,
F3 cursor/retention terminality and authority limits. They also exercise a real published workflow with two terminal
higher indexes and three index/task-owned protections, plus a real NRC1 non-empty recovery root whose checkpoint and
target protections retire before the root; protection/index/task/root/registration delete-response loss converges
from exact absence while both physical roots remain present. `phase4M4RegistrationRetirementCheck` freezes this
ordering and the fail-closed blocker matrix. Checkpoint AN runtime-composes the coordinator after every complete root
pass and before object inventory. The real-service final gate、broker configuration and delete activation remain
pending; the current safe-default bridge keeps the composed lifecycle disabled.

### 12.1 Checkpoint AN lifecycle composition

`StreamRegistrationRetirementScanner.scan()` completely traverses registry shards `00..3f` in ascending order with
`metadataScanPageSize`. Every page key must be the exact inverse of its decoded registration value and belong to the
current shard; continuation must advance strictly. It invokes one coordinator future at a time, counts every exhaustive
`StreamRegistrationRetirementStatus`, rejects overlap and closes without closing its borrowed store or scheduler.

`PhysicalGcLifecyclePass` is a strict asynchronous chain rather than three independent timers:

```text
complete 256-shard physical-root scan and lifecycle routing
  -> complete 64-shard registration-retirement scan
    -> complete known-prefix object inventory
```

Failure or cancellation at one arrow prevents the next stage from starting. This preserves metadata-first recovery:
MARKED、DELETING and DELETED roots converge even when listing is empty; registration removal remains a final metadata
cleanup rather than deletion authority; listing can only recover an exact old object that has no root after both
metadata scans completed.

For one complete root pass, `Phase4PhysicalRootLifecycleRouter` creates fresh deduplication state and applies the
following total routing table:

| root lifecycle | route |
|---|---|
| `ACTIVE`, canonical NCS1 cursor key with stable historical binding | one `CursorSnapshotGcScanner` evaluation per stream |
| `ACTIVE`, every other exact identity or unresolved cursor binding | `OwnerlessObjectGcExecutor.executeActive` |
| `MARKED`, recoverable cursor evidence | `CursorSnapshotGcExecutor.recoverMarked` |
| `MARKED`, every other exact identity | `OwnerlessObjectGcExecutor.recoverMarked` |
| `DELETING` | `SourceRetirementCoordinator.resume` |
| `DELETED` | `DefaultPhysicalRootTombstoneRetirementCoordinator.retire` |
| `QUARANTINED` | no mutation |

The generic ownerless executor derives candidate evidence from the immutable physical identity SHA, constructs an
`OWNERLESS_ORPHAN_CANDIDATE` query and delegates every destructive transition to the central collector. A durable
protection precheck may conservatively skip work, but absence from that precheck never authorizes deletion: the full
reader/protection/metadata/domain proof and activation fence still run inside the collector. Restart from MARKED
reconstructs the exact plan; complete drift unmarks, while incomplete/limit failure leaves MARKED.

`DefaultPhysicalGcLifecycleService` starts immediately only after explicit `start()`, permits at most one active pass,
coalesces any number of hints during a pass into one immediate follow-up and otherwise schedules the next attempt at
`completion + scanInterval` (including after failure). `closeAsync()` rejects future starts/hints, cancels the pending
timer and waits for the active source and target futures; at the close deadline it cancels both without shutting down
borrowed executors. `Phase4PhysicalGcRuntime.close()` drains that service before closing its scanners and transferred
source/audit retirement adapters. `DefaultNereusRuntimeProvider` invokes `start()` only for `config.enabled()`；with
the currently mapped `enabled=false, dryRun=true` defaults, broker startup schedules no lifecycle pass.

V1 has no TTL-only or “stale hint” deletion. Operationally, the active registry cardinality is bounded by live plus
not-yet-fully-retired stream incarnations；metrics expose both populations and shard skew.

## 13. Future Reference-domain Handoff

F5 group offsets and F6 catalogs may protect logical ranges/physical objects. Before either writes an authoritative
reference it must：

1. define a durable domain id/version and authority tokens；
2. ship the `GcReferenceDomain` plugin on all F4 workers；
3. negotiate the cluster capability/domain-set update；
4. acquire object protections before publishing its owner record；
5. remove them only after owner-record retirement and root revalidation。

F4's `future-catalog-sentinel-v1` vetoes all deletion when it observes a later capability but lacks the registered
domain. This makes extension fail closed rather than relying on F4 knowing every future key prefix.

## 14. GC Metrics

```text
nereus_object_reader_lease_count{kind}
nereus_object_reader_lease_renew_total{outcome}
nereus_object_protection_count{type}
nereus_gc_candidate_total{kind,classification}
nereus_gc_root_scan_total{outcome,shard}
nereus_gc_root_scan_age_seconds
nereus_gc_mark_total{outcome}
nereus_gc_veto_total{domain,reason}
nereus_gc_drain_seconds{kind}
nereus_gc_delete_total{kind,outcome}
nereus_gc_bytes_deleted_total{kind}
nereus_gc_quarantined_total{reason}
nereus_gc_tombstone_retire_total{kind,outcome}
nereus_gc_deleted_root_count{kind,age_bucket}
nereus_recovery_checkpoint_coverage_offsets
nereus_recovery_live_commit_nodes
nereus_retention_candidate_offset{reason}
nereus_retention_logical_trim_total{outcome}
nereus_cursor_snapshot_gc_total{classification,outcome}
nereus_materialization_registration_count{state,shard}
nereus_materialization_registration_retire_total{outcome}
```

Object keys、cursor names、tenant/topic names and task ids are not metric labels. Audit logs may include hashed
identities under configured privacy policy.

## 15. Failure Matrix

| Race/failure | Safe outcome |
| --- | --- |
| reader lease write races mark | root post-check or mark rescan orders them；reader starts only if protected |
| lease expires during slow IO | read deadline/renewal rule cancels before unprotected interval |
| task protection write races mark | post-check fails or GC rescan vetoes/unmarks |
| append intent/head races mark | pre-head protection handshake fails or durable `REACHABLE_APPEND` vetoes deletion；head is never acknowledged unprotected |
| generation-zero index write response is lost | reachable protection remains；exact index reload adds/repairs `VISIBLE_GENERATION` before strict success |
| generation becomes committed while GC scans output | visible protection/domain version changes；attempt invalid |
| cursor root starts referencing candidate after inventory | final root/version inventory differs；unmark/retry |
| cursor retention enters pending after mark | domain veto at final scan；unmark before deletion |
| object has another stream slice | whole-object eligibility fails |
| recovery root advances while source plan waits | digest/version changes；recompute, possibly safer |
| higher generation quarantined during source retirement | coverage veto；no delete |
| metadata index delete partially succeeds | DELETING root blocks reads；recovery conditionally finishes |
| object delete succeeds, response lost | root-shard scan rediscovers exact DELETING root；HEAD absent completes idempotently |
| stopped writer retries PUT while tombstone retires | owner/root pre-PUT validation or final owner scan loses；the old key is never recreated |
| audit-record delete succeeds, response lost | exact unchanged DELETED root plus missing audit key completes that step；root remains until final CAS |
| object reappears after root retirement | missing-root inventory classifies it as ownerless candidate；full domain proof is still required |
| unknown future domain activates | sentinel vetoes globally |
| stale S3 listing omits a live object | registered root scan still runs；only missing-root orphan discovery is delayed |
| stale listing contains deleted object | HEAD/root resolves status；no second unrelated delete |
| registration retirement races task/reference creation | final stream/domain/version proof changes；conditional delete is not attempted |
| registration delete response lost | exact key/version reread proves absent or retry；never deletes another incarnation |

## 16. Required Tests

- model-check lease/protection/mark orderings including renewal and clock-skew boundaries；
- kill process at every MARKED/DELETING/index-delete/object-delete/root-CAS cut；
- all 256 physical-root shards、multi-page continuation/restart/watch loss and empty-list DELETING recovery；
- 10,000 DELETED roots age through two HEAD-absence windows, bounded audit cleanup and final root CAS without
  unbounded in-memory accumulation；
- late first/retried PUT at every owner/audit/root-retirement cut, including response loss and externally reappearing
  bytes；
- multi-stream WAL object with one protected slice never deletes；
- source retirement before/after recovery checkpoint and higher-generation quarantine；
- concurrent append/recovery-root advance during GC plan；
- append prepare/root/protection/head/gen-0-index ordering and abandoned-intent cleanup at every crash cut；
- F3 `PROTECTION_PENDING`/`TRIM_PENDING` at every inventory/mark/drain boundary；
- 10,000 cursor roots plus live/old/CAS-lost snapshot objects with paginated list/scan；
- same-name topic delete/recreate and old-incarnation snapshot/object isolation；
- registration retirement requires full stream/task/index/root/domain drain and survives every response-loss cut；
- missing protection migration case caught by domain revalidation；
- future-domain sentinel blocks deletion；
- LocalStack exact HEAD/list/delete, response loss and `ALREADY_ABSENT` recovery；
- policy time/size OR formula、zero/infinite values、stale stats、policy/owner change and backlog eviction；
- no direct `StreamStorage.trim` call outside F3 coordinator, enforced by source audit。
