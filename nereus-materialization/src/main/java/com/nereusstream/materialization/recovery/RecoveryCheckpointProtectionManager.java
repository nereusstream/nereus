/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.recovery;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.keys.DeterministicIds;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.physical.ObjectProtection;
import com.nereusstream.core.physical.ObjectProtectionManager;
import com.nereusstream.core.physical.ObjectProtectionOwner;
import com.nereusstream.core.physical.ObjectProtectionRequest;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.metadata.oxia.GenerationIndexIdentity;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.ObjectProtectionIdentity;
import com.nereusstream.metadata.oxia.ObjectProtectionScanPage;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedObjectProtection;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.VersionedRecoveryCheckpointRoot;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointReferenceRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/** Exact owner/root handshakes for pending, object, and target checkpoint protections. */
public final class RecoveryCheckpointProtectionManager {
    private final String cluster;
    private final GenerationMetadataStore generationStore;
    private final PhysicalObjectMetadataStore physicalStore;
    private final ObjectProtectionManager protections;

    public RecoveryCheckpointProtectionManager(
            String cluster,
            GenerationMetadataStore generationStore,
            PhysicalObjectMetadataStore physicalStore,
            ObjectProtectionManager protections) {
        this.cluster = requireText(cluster, "cluster");
        this.generationStore = Objects.requireNonNull(generationStore, "generationStore");
        this.physicalStore = Objects.requireNonNull(physicalStore, "physicalStore");
        this.protections = Objects.requireNonNull(protections, "protections");
    }

    public CompletableFuture<ObjectProtection> acquirePending(
            RecoveryCheckpointPlan plan,
            PhysicalObjectIdentity checkpointObject,
            long expiresAtMillis) {
        Objects.requireNonNull(plan, "plan");
        Objects.requireNonNull(checkpointObject, "checkpointObject");
        ObjectProtectionOwner owner = rootOwner(plan.baseRoot());
        ObjectProtectionRequest request = new ObjectProtectionRequest(
                checkpointObject,
                ObjectProtectionType.RECOVERY_CHECKPOINT_PENDING,
                pendingReferenceId(plan, checkpointObject),
                owner,
                expiresAtMillis);
        return protections.acquireOrTransfer(
                request,
                actual -> requireExactRoot(plan.baseRoot(), owner, actual));
    }

    public CompletableFuture<ObjectProtection> revalidatePending(
            RecoveryCheckpointPlan plan,
            ObjectProtection pending) {
        Objects.requireNonNull(plan, "plan");
        Objects.requireNonNull(pending, "pending");
        ObjectProtectionOwner owner = rootOwner(plan.baseRoot());
        return protections.revalidate(
                pending,
                actual -> requireExactRoot(plan.baseRoot(), owner, actual));
    }

    public CompletableFuture<RecoveryCheckpointProtections> acquirePermanent(
            RecoveryCheckpointPlan plan,
            VersionedRecoveryCheckpointRoot publishedRoot,
            PhysicalObjectIdentity checkpointObject) {
        Objects.requireNonNull(plan, "plan");
        Objects.requireNonNull(publishedRoot, "publishedRoot");
        Objects.requireNonNull(checkpointObject, "checkpointObject");
        ObjectProtectionOwner owner = rootOwner(publishedRoot);
        ObjectProtectionRequest checkpointRequest = new ObjectProtectionRequest(
                checkpointObject,
                ObjectProtectionType.RECOVERY_CHECKPOINT_OBJECT,
                checkpointObjectReferenceId(publishedRoot, checkpointObject),
                owner,
                0);
        return protections.acquireOrTransfer(
                        checkpointRequest,
                        actual -> requireExactRoot(publishedRoot, owner, actual))
                .thenCompose(checkpointProtection -> acquireTargets(
                                plan,
                                publishedRoot,
                                owner,
                                0,
                                new ArrayList<>())
                        .thenApply(targets -> new RecoveryCheckpointProtections(
                                checkpointProtection, targets)));
    }

    public CompletableFuture<ObjectProtection> acquireCheckpointObject(
            VersionedRecoveryCheckpointRoot publishedRoot,
            PhysicalObjectIdentity checkpointObject) {
        Objects.requireNonNull(publishedRoot, "publishedRoot");
        Objects.requireNonNull(checkpointObject, "checkpointObject");
        ObjectProtectionOwner owner = rootOwner(publishedRoot);
        ObjectProtectionRequest request = new ObjectProtectionRequest(
                checkpointObject,
                ObjectProtectionType.RECOVERY_CHECKPOINT_OBJECT,
                checkpointObjectReferenceId(publishedRoot, checkpointObject),
                owner,
                0);
        return protections.acquireOrTransfer(
                request,
                actual -> requireExactRoot(publishedRoot, owner, actual));
    }

    public CompletableFuture<ObjectProtection> acquireCheckpointTarget(
            VersionedRecoveryCheckpointRoot publishedRoot,
            VersionedGenerationIndex target) {
        Objects.requireNonNull(publishedRoot, "publishedRoot");
        Objects.requireNonNull(target, "target");
        ObjectProtectionOwner owner = rootOwner(publishedRoot);
        return loadExactTargetIdentity(target).thenCompose(identity -> {
            ObjectProtectionRequest request = new ObjectProtectionRequest(
                    identity,
                    ObjectProtectionType.RECOVERY_CHECKPOINT_TARGET,
                    checkpointTargetReferenceId(publishedRoot, target, identity),
                    owner,
                    0);
            return revalidateIndex(target)
                    .thenCompose(ignored -> protections.acquireOrTransfer(
                            request,
                            actual -> requireExactRoot(publishedRoot, owner, actual)))
                    .thenCompose(protection -> revalidateIndex(target)
                            .thenApply(ignored -> protection));
        });
    }

    public CompletableFuture<ObjectProtection> revalidateCheckpointObject(
            VersionedRecoveryCheckpointRoot publishedRoot,
            ObjectProtection protection) {
        Objects.requireNonNull(publishedRoot, "publishedRoot");
        Objects.requireNonNull(protection, "protection");
        ObjectProtectionOwner owner = rootOwner(publishedRoot);
        return protections.revalidate(
                protection,
                actual -> requireExactRoot(publishedRoot, owner, actual));
    }

    public CompletableFuture<Void> releasePublishedPending(
            RecoveryCheckpointReferenceRecord reference,
            VersionedRecoveryCheckpointRoot publishedRoot,
            PhysicalObjectIdentity checkpointObject,
            ObjectProtection checkpointProtection,
            Supplier<CompletableFuture<Void>> targetRevalidator) {
        Objects.requireNonNull(reference, "reference");
        Objects.requireNonNull(publishedRoot, "publishedRoot");
        Objects.requireNonNull(checkpointObject, "checkpointObject");
        Objects.requireNonNull(checkpointProtection, "checkpointProtection");
        Objects.requireNonNull(targetRevalidator, "targetRevalidator");
        ObjectProtectionIdentity pendingIdentity = new ObjectProtectionIdentity(
                checkpointObject.objectKeyHash(),
                ObjectProtectionType.RECOVERY_CHECKPOINT_PENDING,
                pendingReferenceId(
                        publishedRoot.value().streamId(), reference, checkpointObject));
        return findProtection(pendingIdentity, Optional.empty()).thenCompose(optional -> {
            Supplier<CompletableFuture<Void>> authorization = () ->
                    revalidateCheckpointObject(publishedRoot, checkpointProtection)
                            .thenCompose(ignored -> invoke(targetRevalidator));
            if (optional.isEmpty()) {
                return invoke(authorization);
            }
            ObjectProtection pending = protection(
                    checkpointObject, pendingIdentity, optional.orElseThrow());
            return protections.release(pending, actual -> invoke(authorization));
        });
    }

    public CompletableFuture<Void> releasePending(
            ObjectProtection pending,
            RecoveryCheckpointPlan plan,
            VersionedRecoveryCheckpointRoot publishedRoot,
            RecoveryCheckpointProtections permanent) {
        Objects.requireNonNull(pending, "pending");
        Objects.requireNonNull(plan, "plan");
        Objects.requireNonNull(publishedRoot, "publishedRoot");
        Objects.requireNonNull(permanent, "permanent");
        ObjectProtectionOwner owner = rootOwner(publishedRoot);
        return revalidatePermanent(plan, publishedRoot, owner, permanent)
                .thenCompose(ignored -> protections.release(
                        pending,
                        actual -> revalidatePermanent(plan, publishedRoot, owner, permanent)));
    }

    private CompletableFuture<List<ObjectProtection>> acquireTargets(
            RecoveryCheckpointPlan plan,
            VersionedRecoveryCheckpointRoot publishedRoot,
            ObjectProtectionOwner owner,
            int index,
            List<ObjectProtection> accumulator) {
        if (index == plan.targets().size()) {
            return CompletableFuture.completedFuture(List.copyOf(accumulator));
        }
        RecoveryCheckpointTarget target = plan.targets().get(index);
        return loadExactTargetIdentity(target.index())
                .thenCompose(identity -> {
                    ObjectProtectionRequest request = new ObjectProtectionRequest(
                            identity,
                            ObjectProtectionType.RECOVERY_CHECKPOINT_TARGET,
                            checkpointTargetReferenceId(
                                    publishedRoot, target.index(), identity),
                            owner,
                            0);
                    return revalidateIndex(target.index())
                            .thenCompose(ignored -> protections.acquireOrTransfer(
                                    request,
                                    actual -> requireExactRoot(publishedRoot, owner, actual)))
                            .thenCompose(protection -> revalidateIndex(target.index())
                                    .thenApply(ignored -> protection));
                })
                .thenCompose(protection -> {
                    accumulator.add(protection);
                    return acquireTargets(
                            plan, publishedRoot, owner, index + 1, accumulator);
                });
    }

    private CompletableFuture<Void> revalidatePermanent(
            RecoveryCheckpointPlan plan,
            VersionedRecoveryCheckpointRoot publishedRoot,
            ObjectProtectionOwner owner,
            RecoveryCheckpointProtections permanent) {
        return protections.revalidate(
                        permanent.checkpointObject(),
                        actual -> requireExactRoot(publishedRoot, owner, actual))
                .thenCompose(ignored -> revalidateTargets(
                        plan,
                        publishedRoot,
                        owner,
                        permanent.checkpointTargets(),
                        0));
    }

    private CompletableFuture<Void> revalidateTargets(
            RecoveryCheckpointPlan plan,
            VersionedRecoveryCheckpointRoot publishedRoot,
            ObjectProtectionOwner owner,
            List<ObjectProtection> targetProtections,
            int index) {
        if (index == plan.targets().size()) {
            if (targetProtections.size() != index) {
                return CompletableFuture.failedFuture(invariant(
                        "checkpoint target protection count changed"));
            }
            return CompletableFuture.completedFuture(null);
        }
        if (index >= targetProtections.size()) {
            return CompletableFuture.failedFuture(invariant(
                    "checkpoint target protection is absent"));
        }
        VersionedGenerationIndex target = plan.targets().get(index).index();
        return revalidateIndex(target)
                .thenCompose(ignored -> protections.revalidate(
                        targetProtections.get(index),
                        actual -> requireExactRoot(publishedRoot, owner, actual)))
                .thenCompose(ignored -> revalidateTargets(
                        plan, publishedRoot, owner, targetProtections, index + 1));
    }

    private CompletableFuture<PhysicalObjectIdentity> loadExactTargetIdentity(
            VersionedGenerationIndex index) {
        Object target = ReadTargetCodecRegistry.phase15().decode(index.value().readTarget());
        if (!(target instanceof ObjectSliceReadTarget objectTarget)) {
            return CompletableFuture.failedFuture(invariant(
                    "recovery checkpoint target is not an object slice"));
        }
        ObjectKeyHash hash = ObjectKeyHash.from(objectTarget.objectKey());
        return physicalStore.getRoot(cluster, hash).thenApply(optional -> {
            VersionedPhysicalObjectRoot root = optional.orElseThrow(() -> condition(
                    "recovery checkpoint target physical root is absent"));
            PhysicalObjectIdentity identity = PhysicalObjectIdentity.from(root.value());
            PhysicalObjectKind expectedKind = switch (objectTarget.objectType()) {
                case MULTI_STREAM_WAL_OBJECT -> PhysicalObjectKind.OBJECT_WAL;
                case STREAM_COMPACTED_OBJECT -> PhysicalObjectKind.COMMITTED_COMPACTED;
                default -> throw invariant(
                        "recovery checkpoint target object type is unsupported");
            };
            long requiredEnd;
            try {
                requiredEnd = Math.addExact(
                        objectTarget.objectOffset(), objectTarget.objectLength());
            } catch (ArithmeticException overflow) {
                throw invariant("recovery checkpoint target range overflows");
            }
            if (root.value().lifecycle() != PhysicalObjectLifecycle.ACTIVE
                    || !identity.objectKey().equals(objectTarget.objectKey())
                    || identity.objectId().isEmpty()
                    || !identity.objectId().orElseThrow().equals(objectTarget.objectId())
                    || identity.kind() != expectedKind
                    || requiredEnd > identity.objectLength()) {
                throw condition("recovery checkpoint target physical root is not exact and ACTIVE");
            }
            return identity;
        });
    }

    private CompletableFuture<Void> revalidateIndex(VersionedGenerationIndex expected) {
        var value = expected.value();
        GenerationIndexIdentity identity = new GenerationIndexIdentity(
                new StreamId(value.streamId()),
                ReadView.fromWireId(value.readViewId()),
                value.offsetEnd(),
                value.generation());
        return generationStore.getIndex(cluster, identity).thenApply(actual -> {
            if (!actual.equals(Optional.of(expected))) {
                throw condition("recovery checkpoint target index changed");
            }
            return null;
        });
    }

    private CompletableFuture<Optional<VersionedObjectProtection>> findProtection(
            ObjectProtectionIdentity identity,
            Optional<F4ScanToken> continuation) {
        return physicalStore.scanProtections(
                        cluster,
                        identity.object(),
                        continuation,
                        1_000)
                .thenCompose(page -> findProtection(identity, page));
    }

    private CompletableFuture<Optional<VersionedObjectProtection>> findProtection(
            ObjectProtectionIdentity identity,
            ObjectProtectionScanPage page) {
        Optional<VersionedObjectProtection> found = page.values().stream()
                .filter(value -> protectionIdentity(value).equals(identity))
                .findFirst();
        if (found.isPresent() || page.continuation().isEmpty()) {
            return CompletableFuture.completedFuture(found);
        }
        return findProtection(identity, page.continuation());
    }

    private static ObjectProtectionIdentity protectionIdentity(
            VersionedObjectProtection protection) {
        var value = protection.value();
        return new ObjectProtectionIdentity(
                new ObjectKeyHash(value.objectKeyHash()),
                ObjectProtectionType.fromWireId(value.protectionTypeId()),
                value.referenceId());
    }

    private static ObjectProtection protection(
            PhysicalObjectIdentity object,
            ObjectProtectionIdentity identity,
            VersionedObjectProtection versioned) {
        var value = versioned.value();
        return new ObjectProtection(
                object,
                identity,
                new ObjectProtectionOwner(
                        value.ownerKey(),
                        value.ownerMetadataVersion(),
                        new Checksum(ChecksumType.SHA256, value.ownerIdentitySha256())),
                value.rootLifecycleEpoch(),
                value.createdAtMillis(),
                value.expiresAtMillis(),
                versioned.metadataVersion(),
                versioned.durableValueSha256());
    }

    private CompletableFuture<Void> requireExactRoot(
            VersionedRecoveryCheckpointRoot expectedRoot,
            ObjectProtectionOwner expectedOwner,
            ObjectProtectionOwner actualOwner) {
        if (!expectedOwner.equals(actualOwner)) {
            return CompletableFuture.failedFuture(invariant(
                    "recovery checkpoint protection owner changed"));
        }
        return generationStore.getRecoveryRoot(
                        cluster, new StreamId(expectedRoot.value().streamId()))
                .thenApply(actual -> {
                    if (!actual.equals(Optional.of(expectedRoot))) {
                        throw condition("recovery checkpoint root changed during protection handshake");
                    }
                    return null;
                });
    }

    private static ObjectProtectionOwner rootOwner(
            VersionedRecoveryCheckpointRoot root) {
        return new ObjectProtectionOwner(
                root.key(), root.metadataVersion(), root.durableValueSha256());
    }

    private static String pendingReferenceId(
            RecoveryCheckpointPlan plan,
            PhysicalObjectIdentity object) {
        return "rcp1-" + stable(plan.writeRequest().streamId().value()
                + '\0' + plan.writeRequest().checkpointSequence()
                + '\0' + plan.writeRequest().checkpointAttemptId()
                + '\0' + object.objectKeyHash().value());
    }

    private static String pendingReferenceId(
            String streamId,
            RecoveryCheckpointReferenceRecord reference,
            PhysicalObjectIdentity object) {
        return "rcp1-" + stable(streamId
                + '\0' + reference.checkpointSequence()
                + '\0' + reference.checkpointAttemptId()
                + '\0' + object.objectKeyHash().value());
    }

    private static String checkpointObjectReferenceId(
            VersionedRecoveryCheckpointRoot root,
            PhysicalObjectIdentity object) {
        return "rco1-" + stable(root.value().streamId()
                + '\0' + root.value().checkpointSequence()
                + '\0' + object.objectKeyHash().value());
    }

    private static String checkpointTargetReferenceId(
            VersionedRecoveryCheckpointRoot root,
            VersionedGenerationIndex target,
            PhysicalObjectIdentity object) {
        return "rct1-" + stable(root.value().streamId()
                + '\0' + root.value().checkpointSequence()
                + '\0' + target.key()
                + '\0' + target.durableValueSha256().value()
                + '\0' + object.objectKeyHash().value());
    }

    private static String stable(String value) {
        return DeterministicIds.stableHashComponent(value);
    }

    private static <T> CompletableFuture<T> invoke(
            Supplier<CompletableFuture<T>> operation) {
        try {
            return Objects.requireNonNull(operation.get(), "operation future");
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static NereusException condition(String message) {
        return new NereusException(ErrorCode.METADATA_CONDITION_FAILED, true, message);
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }
}
