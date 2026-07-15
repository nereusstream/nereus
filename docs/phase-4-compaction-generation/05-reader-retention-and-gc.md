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

nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/f4/
  PhysicalObjectMetadataStore.java

nereus-materialization/src/main/java/com/nereusstream/materialization/gc/
  GcCandidate.java
  GcPlan.java
  PhysicalGcConfig.java
  PhysicalObjectGarbageCollector.java
  PhysicalObjectRootScanner.java
  PhysicalRootTombstoneRetirementCoordinator.java
  TombstoneRetirementResult.java
  TombstoneRetirementStatus.java
  SourceRetirementCoordinator.java
  StreamRegistrationRetirementCoordinator.java
  ObjectInventoryScanner.java

nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/retention/
  RetentionCandidatePlanner.java
  RetentionPolicySnapshot.java
  NereusManagedLedgerRetentionService.java
  ProjectionGenerationReferenceDomain.java
  CursorSnapshotReferenceDomain.java
  CursorSnapshotGcScanner.java
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
protection before committed-index restoration. Runtime composition、source/index retirement、GC coordinators、cursor
integration and retirement remain planned；therefore no object deletion is enabled by these checkpoints.

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
    CompletableFuture<Boolean> stillMatches(GcReferenceSnapshot snapshot);
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
        long rootMetadataVersion,
        long rootLifecycleEpoch,
        long discoveredAtMillis,
        long notBeforeMillis) { }

public record GcPlan(
        String gcAttemptId,
        GcCandidate candidate,
        List<GcReferenceSnapshot> domainSnapshots,
        List<ObjectProtectionIdentity> plannedProtectionRemovals,
        List<String> plannedMetadataKeys,
        Checksum referenceSetSha256,
        long markedRootMetadataVersion,
        long markedRootLifecycleEpoch,
        long deleteNotBeforeMillis) { }
```

Candidate/attempt ids are random 128-bit lowercase base32 and never authorize deletion. Candidate construction
requires the exact ACTIVE root version/epoch and hashes the root、manifest/inventory evidence and affected stream
set. Plan lists are canonical sorted/unique and bounded by `PhysicalGcConfig`; `referenceSetSha256` hashes the full
domain authority/reference sets plus every planned key/protection, not merely the retained Java lists. The marked
root version/epoch are filled only from the successful `ACTIVE -> MARKED` CAS. A plan is process-local and is always
reconstructed from the root digest after restart；serializing it would create a second correctness owner.

The query is a core value；materialization's `GcCandidate` wraps it with retry/plan timestamps and never appears in
the SPI. Affected streams are sorted/unique and capped at 4,096；`REFERENCED_OBJECT` and
`CURSOR_SNAPSHOT_CANDIDATE` require at least one, while `OWNERLESS_ORPHAN_CANDIDATE` requires an empty set plus a
complete domain-wide absence proof. Query identity hashes the exact object、kind、
candidate-evidence digest and stream set. Evidence is the canonical manifest/reference record when present, otherwise
the canonical root/listing classification proof. Snapshot lists use canonical `(type/key/version/identity)` order. `complete=false` or
any count above configured in-memory limits requires `veto=true`；a domain may stream-hash a larger scan for
diagnostics but cannot truncate it into permission. `stillMatches` re-reads the authority tokens and exact query
identity rather than trusting list equality alone.

F4 registers：

| Domain | Revalidated authority |
| --- | --- |
| `generation-v1` | exact committed/draining index records and source coverage |
| `append-recovery-v1` | head + stable recovery root/checkpoint refs |
| `materialization-v1` | task roots/output/source identities |
| `projection-generation-v1` | F2 topic projection/incarnation identity、generation marker and old-stream deletion/unaddressability proof |
| `cursor-snapshot-v1` | F3 retention root + every cursor root + inventory classification |
| `future-catalog-sentinel-v1` | veto if any later catalog capability is active without its plugin |

The durable cluster generation-activation record stores the exact required domain id/version set. Runtime startup
fails F4 readiness if its registered set differs. Future 6 must atomically add its capability/domain before writing a
catalog reference；F4 never interprets absence of a plugin as absence of references.

`projection-generation-v1` makes the per-topic downgrade fence usable by protocol-neutral GC. For each affected
stream it returns one of two proofs：the exact live `TopicProjectionRecord`/incarnation has
`nereus.generation-protocol=1`，or the old incarnation is sealed/logically deleted and no current binding/projection
can address that stream. A multi-stream WAL object requires a proof for every slice. A true orphan with no stream
owner still requires the complete projection/binding-domain absence proof；object-list absence is not enough. The
materialization module consumes only the domain snapshot/tokens and never parses Pulsar metadata itself.

## 8. Logical Retention Candidate

### 8.1 Policy snapshot

The Pulsar resolver must pass exact values rather than the current booleans：

```java
public record RetentionPolicySnapshot(
        long policyVersion,
        long retentionTimeMillis,      // -1=infinite, 0=no post-consume time retention
        long retentionSizeBytes) { }    // -1=infinite, 0=no post-consume size retention

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
  -> compute stable candidate
  -> activationGuard.revalidate(proof)
  -> CursorRetentionCoordinator.requestTrim(cursorOwnerSession, candidate, reason)
  -> ownershipGuard.requireOwned() before callback success
```

The returned/admin promise completes after F3 proves logical trim ACTIVE/completed. Physical deletion is scheduled
later and is not included in that promise. No Phase 4 caller invokes `StreamStorage.trim` directly.

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

1. its entire range is below completed L0 trim and the stable recovery root no longer needs its primary target；or
2. a healthy higher `COMMITTED` generation fully covers it, the recovery checkpoint records replacement evidence and
   append-replay/index-repair no longer needs the old target。

Partial overlap is insufficient. `TOPIC_COMPACTED` never covers a committed source. A quarantined/draining replacement
does not count.

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
  collect index refs + protection scan + reference-domain snapshots
  requireReady(PHYSICAL_DELETE,
      DomainValidatedDeletionSubject(exactGcReferenceQuery,
          projectionDomainSnapshotSha256), false)
  classify planned visible/recovery protections vs veto protections
  compute canonical referenceSetSha256 from keys/versions/owner tokens

MARK
  revalidate activation proof
  CAS root ACTIVE -> MARKED(
      new epoch, attemptId, digest,
      markedAt, deleteNotBefore = now + sourceRetirementGrace)
  no new reader/protection handshake can complete

DRAIN
  wait until deleteNotBefore
  scan leases; wait through every expiresAt + maxClockSkew
  rescan protections and all domains
  require the exact digest/authority set or:
      if no destructive side effect yet, CAS MARKED -> ACTIVE(new epoch)
      otherwise quarantine

DELETE INTENT
  revalidate activation proof and exact projection-domain snapshot
  CAS MARKED -> DELETING preserving attempt/digest

METADATA RETIREMENT
  conditionally delete/retire every planned F4 index key
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

The root stores only the canonical digest, not an unbounded reference list. Recovery recomputes the set from
authoritative metadata and requires the same digest before continuing. Conditional metadata deletion makes partial
progress discoverable.

If a process crashes after `DELETING`, another process resumes; the object never becomes readable again. If it crashes
after physical delete before root CAS, HEAD/`ALREADY_ABSENT` plus exact root identity completes `DELETED`.

### 9.4 Higher-generation lifecycle

For an F4 index selected for retirement, the coordinator first CASes `COMMITTED/QUARANTINED -> DRAINING`, so new
resolvers ignore it even before object mark. It becomes `RETIRED` after physical delete or after another active object
reference keeps shared bytes but this generation ref is removed. Generation-zero frozen records have no lifecycle；
the physical root mark and exact pin post-check provide the same new-reader fence.

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
- register F4 pinning in `DefaultCursorSnapshotStore.read`；
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
reconciled. If root
CAS wins but live protection creation is delayed, the authoritative cursor domain revalidation vetoes deletion even
when the protection key is temporarily absent.

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

## 11. Orphan Discovery

`PhysicalObjectRootScanner` is the authoritative lifecycle work scanner. It visits shards `000..255` round-robin with
one in-memory continuation per shard and pages `scanRoots`. `ACTIVE` roots are evaluated as candidates；`MARKED` and
`DELETING` roots are always handed to exact recovery even when object-store listing is empty；`DELETED` roots are
handed to the bounded tombstone-retirement coordinator and never held in an in-memory delay queue. End-of-shard clears the
continuation, a full pass restarts at every prefix, and watch events only accelerate a pass. Thus delete-response loss
after bytes are already absent still converges to `DELETED` from Oxia truth.

`ObjectInventoryScanner` pages only known product prefixes and registers exact HEAD identity. Candidate classes：

- output uploaded but no task/output/index reference；
- PREPARED/ABORTED publication output after retry grace；
- CAS-lost cursor snapshot；
- checkpoint upload whose root CAS lost；
- legacy manifest with no reachable slices after migration coverage；
- physical object whose metadata root is missing。

Listing is eventually consistent/audit input used mainly for bytes uploaded before any root CAS. The scanner waits
`orphanNotBefore`, registers/revalidates every domain，
marks the physical root, waits a second inventory/grace cycle and only then follows the same GC algorithm. A missing
manifest or a single empty listing is never proof of orphanhood.

Unknown prefixes、malformed keys、identity mismatch or objects newer than grace are reported/quarantined, not deleted.

## 12. Stream-registration Retirement

A stream registration is one record per stream incarnation, so deleted incarnations must eventually leave the 64
active scan shards. Its removal is the final metadata-retirement step, never an input that authorizes another delete.
`StreamRegistrationRetirementCoordinator` may call `deleteStreamRegistration(..., expectedVersion)`（implemented by
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
