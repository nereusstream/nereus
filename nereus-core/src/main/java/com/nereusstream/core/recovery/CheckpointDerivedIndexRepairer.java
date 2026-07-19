/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.recovery;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.capability.GenerationActivationProof;
import com.nereusstream.core.capability.GenerationOperation;
import com.nereusstream.core.capability.GenerationProtocolActivationGuard;
import com.nereusstream.core.capability.LiveProjectionSubject;
import com.nereusstream.core.physical.ObjectProtection;
import com.nereusstream.core.physical.ObjectProtectionManager;
import com.nereusstream.core.physical.ObjectProtectionOwner;
import com.nereusstream.core.physical.ObjectProtectionRequest;
import com.nereusstream.core.physical.ObjectReadLease;
import com.nereusstream.core.physical.ObjectReadPinManager;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.core.read.GenerationIndexRepairResult;
import com.nereusstream.core.read.GenerationIndexRepairer;
import com.nereusstream.core.read.MetadataGenerationIndexRepairer;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.GenerationIndexDigests;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.ProjectionIdentity;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedMaterializationStreamRegistration;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.VersionedRecoveryCheckpointRoot;
import com.nereusstream.metadata.oxia.codec.GenerationIndexRecordCodecV1;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointReferenceRecord;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointCodecV1;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointEntry;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointObject;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointPublication;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;

/** Restores the highest healthy committed generation index from a root-stable NRC1 prefix. */
public final class CheckpointDerivedIndexRepairer implements GenerationIndexRepairer {
    private static final int MAX_ROOT_RESTARTS = 8;

    private final String cluster;
    private final OxiaMetadataStore l0Store;
    private final GenerationMetadataStore generationStore;
    private final PhysicalObjectMetadataStore physicalStore;
    private final AnchorAwareCommitWalker walker;
    private final RecoveryCheckpointCodecV1 codec;
    private final ObjectReadPinManager pinManager;
    private final ObjectProtectionManager protections;
    private final GenerationProtocolActivationGuard activationGuard;
    private final GenerationIndexRepairer liveRepairer;
    private final int maxLiveCommits;
    private final int livePageSize;
    private final Clock clock;
    private final GenerationIndexRecordCodecV1 generationCodec =
            new GenerationIndexRecordCodecV1();

    public CheckpointDerivedIndexRepairer(
            String cluster,
            OxiaMetadataStore l0Store,
            GenerationMetadataStore generationStore,
            PhysicalObjectMetadataStore physicalStore,
            AnchorAwareCommitWalker walker,
            RecoveryCheckpointCodecV1 codec,
            ObjectReadPinManager pinManager,
            ObjectProtectionManager protections,
            GenerationProtocolActivationGuard activationGuard,
            int maxLiveCommits,
            int livePageSize,
            Clock clock) {
        this(
                cluster,
                l0Store,
                generationStore,
                physicalStore,
                walker,
                codec,
                pinManager,
                protections,
                activationGuard,
                new MetadataGenerationIndexRepairer(
                        cluster, l0Store, maxLiveCommits),
                maxLiveCommits,
                livePageSize,
                clock);
    }

    public CheckpointDerivedIndexRepairer(
            String cluster,
            OxiaMetadataStore l0Store,
            GenerationMetadataStore generationStore,
            PhysicalObjectMetadataStore physicalStore,
            AnchorAwareCommitWalker walker,
            RecoveryCheckpointCodecV1 codec,
            ObjectReadPinManager pinManager,
            ObjectProtectionManager protections,
            GenerationProtocolActivationGuard activationGuard,
            GenerationIndexRepairer liveRepairer,
            int maxLiveCommits,
            int livePageSize,
            Clock clock) {
        this.cluster = requireText(cluster, "cluster");
        this.l0Store = Objects.requireNonNull(l0Store, "l0Store");
        this.generationStore = Objects.requireNonNull(
                generationStore, "generationStore");
        this.physicalStore = Objects.requireNonNull(
                physicalStore, "physicalStore");
        this.walker = Objects.requireNonNull(walker, "walker");
        this.codec = Objects.requireNonNull(codec, "codec");
        this.pinManager = Objects.requireNonNull(pinManager, "pinManager");
        this.protections = Objects.requireNonNull(protections, "protections");
        this.activationGuard = Objects.requireNonNull(
                activationGuard, "activationGuard");
        if (maxLiveCommits <= 0
                || livePageSize <= 0
                || livePageSize > maxLiveCommits
                || livePageSize > 1_000) {
            throw new IllegalArgumentException(
                    "live repair bounds are invalid");
        }
        this.maxLiveCommits = maxLiveCommits;
        this.livePageSize = livePageSize;
        this.liveRepairer = Objects.requireNonNull(
                liveRepairer, "liveRepairer");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public CompletableFuture<GenerationIndexRepairResult> repair(
            StreamId streamId, long targetOffset, Duration timeout) {
        Objects.requireNonNull(streamId, "streamId");
        if (targetOffset < 0) {
            throw new IllegalArgumentException("targetOffset must be non-negative");
        }
        RepairDeadline deadline = new RepairDeadline(timeout, clock);
        return repairAttempt(streamId, targetOffset, deadline, 0)
                .whenComplete((ignored, failure) -> deadline.close());
    }

    private CompletableFuture<GenerationIndexRepairResult> repairAttempt(
            StreamId streamId,
            long targetOffset,
            RepairDeadline deadline,
            int rootRestarts) {
        CompletableFuture<GenerationIndexRepairResult> attempt = deadline.bound(
                        () -> l0Store.getStreamSnapshot(cluster, streamId),
                        "load stream snapshot for generation repair")
                .thenCompose(snapshot -> admitTarget(
                        streamId, targetOffset, snapshot))
                .thenCompose(initial -> initial.isPresent()
                        ? CompletableFuture.completedFuture(
                                initial.orElseThrow())
                        : deadline.bound(
                                        () -> walker.walk(
                                                streamId,
                                                maxLiveCommits,
                                                livePageSize),
                                        "walk root-stable live tail for generation repair")
                                .thenCompose(walk -> route(
                                        streamId,
                                        targetOffset,
                                        walk,
                                        deadline)));
        return attempt.handle((value, failure) -> {
            if (failure == null) {
                return CompletableFuture.completedFuture(value);
            }
            Throwable exact = unwrap(failure);
            if (exact instanceof TargetTrimmedException) {
                return CompletableFuture.completedFuture(
                        GenerationIndexRepairResult.trimmed(
                                streamId, targetOffset));
            }
            if (exact instanceof RecoveryRootChangedException
                    && rootRestarts + 1 < MAX_ROOT_RESTARTS) {
                return repairAttempt(
                        streamId,
                        targetOffset,
                        deadline,
                        rootRestarts + 1);
            }
            if (exact instanceof RecoveryRootChangedException) {
                return CompletableFuture.<GenerationIndexRepairResult>failedFuture(
                        condition("recovery root changed throughout index repair"));
            }
            return CompletableFuture.<GenerationIndexRepairResult>failedFuture(exact);
        }).thenCompose(Function.identity());
    }

    private CompletableFuture<Optional<GenerationIndexRepairResult>> admitTarget(
            StreamId streamId,
            long targetOffset,
            StreamMetadataSnapshot snapshot) {
        requireSnapshotStream(streamId, snapshot);
        if (targetOffset < snapshot.trim().trimOffset()) {
            return CompletableFuture.completedFuture(Optional.of(
                    GenerationIndexRepairResult.trimmed(
                            streamId, targetOffset)));
        }
        if (targetOffset >= snapshot.committedEnd().committedEndOffset()) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.READ_RESOLUTION_FAILED,
                    true,
                    "generation repair target is not a committed offset"));
        }
        return CompletableFuture.completedFuture(Optional.empty());
    }

    private CompletableFuture<GenerationIndexRepairResult> route(
            StreamId streamId,
            long targetOffset,
            AnchorAwareCommitWalk walk,
            RepairDeadline deadline) {
        if (!walk.anchorReached()) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.READ_RESOLUTION_FAILED,
                    true,
                    "generation repair live tail exceeds its bounded scan"));
        }
        if (!walk.observedHead().streamId().equals(streamId)
                || targetOffset >= walk.observedHead().offsetEnd()) {
            return CompletableFuture.failedFuture(invariant(
                    "generation repair walk does not contain the committed target", null));
        }
        Optional<VersionedRecoveryCheckpointRoot> optionalRoot =
                walk.recoveryRoot();
        if (optionalRoot.isEmpty()
                || optionalRoot.orElseThrow().value().checkpoints().isEmpty()) {
            requireLiveCoverage(targetOffset, walk);
            return repairLive(
                    streamId,
                    targetOffset,
                    optionalRoot,
                    deadline);
        }
        VersionedRecoveryCheckpointRoot root = optionalRoot.orElseThrow();
        if (targetOffset >= root.value().coveredEndOffset()) {
            requireLiveCoverage(targetOffset, walk);
            return repairLive(
                    streamId,
                    targetOffset,
                    optionalRoot,
                    deadline);
        }
        if (targetOffset < root.value().coveredStartOffset()) {
            return deadline.bound(
                            () -> l0Store.getStreamSnapshot(cluster, streamId),
                            "revalidate trim before checkpoint repair")
                    .thenCompose(snapshot -> {
                        requireSnapshotStream(streamId, snapshot);
                        if (targetOffset < snapshot.trim().trimOffset()) {
                            return CompletableFuture.completedFuture(
                                    GenerationIndexRepairResult.trimmed(
                                            streamId, targetOffset));
                        }
                        return CompletableFuture.failedFuture(new NereusException(
                                ErrorCode.METADATA_UNAVAILABLE,
                                false,
                                "untrimmed offset precedes retained recovery evidence"));
                    });
        }
        RecoveryCheckpointReferenceRecord reference =
                referenceCovering(root, targetOffset);
        return repairCheckpoint(
                streamId,
                targetOffset,
                walk.commitsNewestFirst().size(),
                root,
                reference,
                deadline);
    }

    private CompletableFuture<GenerationIndexRepairResult> repairLive(
            StreamId streamId,
            long targetOffset,
            Optional<VersionedRecoveryCheckpointRoot> expectedRoot,
            RepairDeadline deadline) {
        return deadline.bound(
                        () -> liveRepairer.repair(
                                streamId,
                                targetOffset,
                                deadline.remaining(
                                        "repair index from live commit evidence")),
                        "repair index from live commit evidence")
                .thenCompose(result -> requireExactRoot(streamId, expectedRoot)
                        .thenCompose(ignored -> requireUntrimmed(
                                        streamId, targetOffset, deadline)
                                .thenApply(unused -> result)));
    }

    private CompletableFuture<GenerationIndexRepairResult> repairCheckpoint(
            StreamId streamId,
            long targetOffset,
            int scannedLiveRecords,
            VersionedRecoveryCheckpointRoot root,
            RecoveryCheckpointReferenceRecord reference,
            RepairDeadline deadline) {
        PhysicalObjectIdentity checkpointObject = checkpointIdentity(reference);
        CompletableFuture<ObjectReadLease> acquired = deadline.bound(
                () -> pinManager.acquire(
                        checkpointObject,
                        deadline.maximumReadDeadlineMillis(),
                        () -> requireExactRoot(root)),
                "acquire recovery checkpoint read pin for index repair");
        return withLease(acquired, ignored -> openAndRepair(
                        streamId,
                        targetOffset,
                        scannedLiveRecords,
                        root,
                        reference,
                        deadline))
                .thenCompose(result -> requireExactRoot(root)
                        .thenApply(unused -> result));
    }

    private CompletableFuture<GenerationIndexRepairResult> openAndRepair(
            StreamId streamId,
            long targetOffset,
            int scannedLiveRecords,
            VersionedRecoveryCheckpointRoot root,
            RecoveryCheckpointReferenceRecord reference,
            RepairDeadline deadline) {
        return deadline.bound(
                        () -> codec.openAndVerify(
                                new ObjectKey(reference.objectKey()),
                                reference.objectLength(),
                                new Checksum(
                                        ChecksumType.SHA256,
                                        reference.contentSha256()),
                                deadline.remaining(
                                        "open NRC1 for index repair")),
                        "open NRC1 for index repair")
                .thenCompose(checkpoint -> {
                    requireExactReference(root, reference, checkpoint);
                    return deadline.bound(
                                    () -> codec.findCommitCoveringOffset(
                                            checkpoint,
                                            targetOffset,
                                            deadline.remaining(
                                                    "find NRC1 commit for index repair")),
                                    "find NRC1 commit for index repair")
                            .thenCompose(optional -> loadReferencedPublications(
                                    checkpoint,
                                    optional.orElseThrow(() -> invariant(
                                            "NRC1 has no commit for a covered repair offset",
                                            null)),
                                    deadline));
                })
                .thenCompose(publications -> prepareCandidates(
                        streamId, targetOffset, publications))
                .thenCompose(candidates -> loadActivation(
                                streamId, reference, deadline)
                        .thenCompose(activation -> selectHealthyCandidate(
                                streamId,
                                targetOffset,
                                root,
                                candidates,
                                0,
                                deadline)
                                .thenCompose(selected -> restoreSelected(
                                        streamId,
                                        targetOffset,
                                        scannedLiveRecords,
                                        root,
                                        activation,
                                        selected,
                                        deadline))));
    }

    private CompletableFuture<List<RecoveryCheckpointPublication>>
            loadReferencedPublications(
                    RecoveryCheckpointObject checkpoint,
                    RecoveryCheckpointEntry entry,
                    RepairDeadline deadline) {
        return loadReferencedPublication(
                checkpoint,
                entry.coveringPublicationIndexes(),
                0,
                new ArrayList<>(),
                deadline);
    }

    private CompletableFuture<List<RecoveryCheckpointPublication>>
            loadReferencedPublication(
                    RecoveryCheckpointObject checkpoint,
                    List<Integer> indexes,
                    int cursor,
                    List<RecoveryCheckpointPublication> accumulated,
                    RepairDeadline deadline) {
        if (cursor == indexes.size()) {
            return CompletableFuture.completedFuture(List.copyOf(accumulated));
        }
        int publicationIndex = indexes.get(cursor);
        return deadline.bound(
                        () -> codec.scanPublications(
                                checkpoint,
                                OptionalInt.of(publicationIndex),
                                1,
                                deadline.remaining(
                                        "load NRC1 publication for index repair")),
                        "load NRC1 publication for index repair")
                .thenCompose(page -> {
                    if (page.values().size() != 1) {
                        return CompletableFuture.failedFuture(invariant(
                                "NRC1 publication index did not resolve exactly one row",
                                null));
                    }
                    accumulated.add(page.values().get(0));
                    return loadReferencedPublication(
                            checkpoint,
                            indexes,
                            cursor + 1,
                            accumulated,
                            deadline);
                });
    }

    private CompletableFuture<List<IndexCandidate>> prepareCandidates(
            StreamId streamId,
            long targetOffset,
            List<RecoveryCheckpointPublication> publications) {
        List<IndexCandidate> result = publications.stream()
                .map(publication -> decodeCandidate(
                        streamId, targetOffset, publication))
                .sorted(Comparator.comparingLong(
                                (IndexCandidate value) ->
                                        value.record().generation())
                        .reversed()
                        .thenComparing(value ->
                                value.record().publicationId()))
                .toList();
        for (int index = 1; index < result.size(); index++) {
            if (result.get(index - 1).record().generation()
                    == result.get(index).record().generation()) {
                return CompletableFuture.failedFuture(invariant(
                        "NRC1 repair candidates reuse one generation number",
                        null));
            }
        }
        return CompletableFuture.completedFuture(result);
    }

    private IndexCandidate decodeCandidate(
            StreamId streamId,
            long targetOffset,
            RecoveryCheckpointPublication publication) {
        byte[] canonical = bytes(
                publication.canonicalGenerationIndexRecord());
        GenerationIndexRecord record;
        try {
            record = generationCodec.decode(canonical);
        } catch (RuntimeException failure) {
            throw invariant(
                    "cannot decode NRC1 generation index during repair", failure);
        }
        if (!Arrays.equals(canonical, generationCodec.encode(record))
                || !GenerationIndexDigests.canonicalRecordSha256(record)
                        .equals(publication.generationIndexRecordSha256())
                || record.metadataVersion() != 0
                || record.lifecycle() != GenerationLifecycle.COMMITTED
                || record.readViewId() != ReadView.COMMITTED.wireId()
                || !record.streamId().equals(streamId.value())
                || record.generation() != publication.generation()
                || !record.publicationId().equals(
                        publication.publicationId().value())
                || record.offsetStart()
                        != publication.coverage().startOffset()
                || record.offsetEnd() != publication.coverage().endOffset()
                || record.offsetStart() > targetOffset
                || targetOffset >= record.offsetEnd()
                || !record.targetIdentitySha256().equals(
                        record.readTarget().identityChecksumValue())) {
            throw invariant(
                    "NRC1 generation index is non-canonical or does not cover the repair target",
                    null);
        }
        Checksum durableDigest = GenerationIndexDigests.durableValueSha256(record);
        String indexKey = new F4Keyspace(cluster).generationIndexKey(
                streamId,
                ReadView.COMMITTED,
                record.offsetEnd(),
                record.generation());
        return new IndexCandidate(
                publication,
                record,
                publication.generationIndexRecordSha256(),
                durableDigest,
                indexKey);
    }

    private CompletableFuture<Activation> loadActivation(
            StreamId streamId,
            RecoveryCheckpointReferenceRecord reference,
            RepairDeadline deadline) {
        return deadline.bound(
                        () -> generationStore.getStreamRegistration(
                                cluster, streamId),
                        "load stream registration for checkpoint index repair")
                .thenCompose(optional -> {
                    VersionedMaterializationStreamRegistration registration =
                            optional.orElseThrow(() -> condition(
                                    "materialization stream registration is absent"));
                    LiveProjectionSubject subject = subject(
                            streamId, reference, registration);
                    return deadline.bound(
                                    () -> activationGuard.requireReady(
                                            GenerationOperation.GENERATION_PUBLISH,
                                            subject,
                                            false),
                                    "admit checkpoint-derived index repair")
                            .thenApply(proof -> new Activation(subject, proof));
                });
    }

    private LiveProjectionSubject subject(
            StreamId streamId,
            RecoveryCheckpointReferenceRecord reference,
            VersionedMaterializationStreamRegistration registration) {
        MaterializationStreamRegistrationRecord value = registration.value();
        String expectedKey = new F4Keyspace(cluster)
                .materializationRegistryKey(streamId);
        ProjectionRef projection;
        StorageProfile profile;
        try {
            projection = ProjectionIdentity.decode(value.projectionRef())
                    .orElseThrow(() -> invariant(
                            "checkpoint repair registration has no projection identity",
                            null));
            profile = StorageProfile.valueOf(value.storageProfile()).canonical();
        } catch (NereusException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw invariant(
                    "checkpoint repair registration is malformed", failure);
        }
        if (!registration.key().equals(expectedKey)
                || !value.streamId().equals(streamId.value())
                || !value.projectionIdentitySha256().equals(
                        reference.projectionIdentitySha256())
                || !profile.objectMaterializationEnabled()) {
            throw invariant(
                    "checkpoint repair registration differs from NRC1 projection identity",
                    null);
        }
        return new LiveProjectionSubject(
                streamId,
                projection,
                new Checksum(
                        ChecksumType.SHA256,
                        value.projectionIdentitySha256()));
    }

    private CompletableFuture<SelectedCandidate> selectHealthyCandidate(
            StreamId streamId,
            long targetOffset,
            VersionedRecoveryCheckpointRoot root,
            List<IndexCandidate> candidates,
            int index,
            RepairDeadline deadline) {
        if (index == candidates.size()) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.READ_RESOLUTION_FAILED,
                    true,
                    "no healthy NRC1 generation target covers the committed offset"));
        }
        IndexCandidate candidate = candidates.get(index);
        return loadTargetIdentity(candidate.record(), deadline)
                .handle((identity, failure) -> {
                    if (failure == null) {
                        return CompletableFuture.completedFuture(
                                new SelectedCandidate(candidate, identity));
                    }
                    Throwable exact = unwrap(failure);
                    if (exact instanceof CandidateUnavailableException) {
                        return selectHealthyCandidate(
                                streamId,
                                targetOffset,
                                root,
                                candidates,
                                index + 1,
                                deadline);
                    }
                    return CompletableFuture.<SelectedCandidate>failedFuture(
                            exact);
                })
                .thenCompose(Function.identity());
    }

    private CompletableFuture<PhysicalObjectIdentity> loadTargetIdentity(
            GenerationIndexRecord record,
            RepairDeadline deadline) {
        Object decoded;
        try {
            decoded = ReadTargetCodecRegistry.phase15().decode(
                    record.readTarget());
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(invariant(
                    "NRC1 repair target cannot be decoded", failure));
        }
        if (!(decoded instanceof ObjectSliceReadTarget target)) {
            return CompletableFuture.failedFuture(invariant(
                    "NRC1 repair target is not an object slice", null));
        }
        ObjectKeyHash hash = ObjectKeyHash.from(target.objectKey());
        return deadline.bound(
                        () -> physicalStore.getRoot(cluster, hash),
                        "load checkpoint target physical root")
                .thenApply(optional -> {
                    VersionedPhysicalObjectRoot root = optional.orElseThrow(
                            CandidateUnavailableException::new);
                    PhysicalObjectIdentity identity = PhysicalObjectIdentity.from(
                            root.value());
                    PhysicalObjectKind expectedKind = switch (target.objectType()) {
                        case MULTI_STREAM_WAL_OBJECT ->
                                PhysicalObjectKind.OBJECT_WAL;
                        case STREAM_COMPACTED_OBJECT ->
                                PhysicalObjectKind.COMMITTED_COMPACTED;
                        default -> throw invariant(
                                "NRC1 repair target object type is unsupported",
                                null);
                    };
                    long requiredEnd;
                    try {
                        requiredEnd = Math.addExact(
                                target.objectOffset(), target.objectLength());
                    } catch (ArithmeticException overflow) {
                        throw invariant(
                                "NRC1 repair target range overflows", overflow);
                    }
                    if (root.value().lifecycle()
                                    != PhysicalObjectLifecycle.ACTIVE
                            || !identity.objectKey().equals(target.objectKey())
                            || identity.objectId().isEmpty()
                            || !identity.objectId().orElseThrow()
                                    .equals(target.objectId())
                            || identity.kind() != expectedKind
                            || requiredEnd > identity.objectLength()) {
                        throw new CandidateUnavailableException();
                    }
                    return identity;
                });
    }

    private CompletableFuture<GenerationIndexRepairResult> restoreSelected(
            StreamId streamId,
            long targetOffset,
            int scannedLiveRecords,
            VersionedRecoveryCheckpointRoot root,
            Activation activation,
            SelectedCandidate selected,
            RepairDeadline deadline) {
        IndexCandidate candidate = selected.candidate();
        PhysicalObjectIdentity object = selected.object();
        ObjectProtectionOwner owner =
                RecoveryCheckpointProtectionIdentities.rootOwner(root);
        ObjectProtectionRequest request = new ObjectProtectionRequest(
                object,
                ObjectProtectionType.RECOVERY_CHECKPOINT_TARGET,
                RecoveryCheckpointProtectionIdentities
                        .checkpointTargetReferenceId(
                                root,
                                candidate.indexKey(),
                                candidate.durableDigest(),
                                object),
                owner,
                0);
        ObjectProtectionManager.OwnerRevalidator revalidator = actualOwner -> {
            if (!actualOwner.equals(owner)) {
                return CompletableFuture.failedFuture(invariant(
                        "checkpoint target protection owner changed", null));
            }
            return requireExactRoot(root)
                    .thenCompose(ignored -> deadline.bound(
                            () -> activationGuard.revalidate(
                                    activation.proof()),
                            "revalidate activation during checkpoint index repair"))
                    .thenCompose(ignored -> requireUntrimmed(
                            streamId, targetOffset, deadline));
        };
        return deadline.bound(
                        () -> activationGuard.revalidate(activation.proof()),
                        "revalidate activation before checkpoint target protection")
                .thenCompose(ignored -> requireExactRoot(root))
                .thenCompose(ignored -> requireUntrimmed(
                        streamId, targetOffset, deadline))
                .thenCompose(ignored -> deadline.bound(
                        () -> protections.acquireOrTransfer(
                                request, revalidator),
                        "acquire checkpoint target protection before index restore"))
                .thenCompose(protection -> revalidateSelected(
                                streamId,
                                targetOffset,
                                root,
                                activation,
                                selected,
                                protection,
                                revalidator,
                                deadline)
                        .thenCompose(unused -> deadline.bound(
                                () -> generationStore
                                        .restoreCommittedFromCheckpoint(
                                                cluster,
                                                candidate.record(),
                                                candidate.rawDigest()),
                                "restore committed generation index from NRC1"))
                        .thenCompose(restored -> revalidateSelected(
                                        streamId,
                                        targetOffset,
                                        root,
                                        activation,
                                        selected,
                                        protection,
                                        revalidator,
                                        deadline)
                                .thenApply(unused ->
                                        GenerationIndexRepairResult.checkpoint(
                                                streamId,
                                                targetOffset,
                                                scannedLiveRecords,
                                                restored))));
    }

    private CompletableFuture<Void> revalidateSelected(
            StreamId streamId,
            long targetOffset,
            VersionedRecoveryCheckpointRoot root,
            Activation activation,
            SelectedCandidate selected,
            ObjectProtection protection,
            ObjectProtectionManager.OwnerRevalidator ownerRevalidator,
            RepairDeadline deadline) {
        return requireExactRoot(root)
                .thenCompose(ignored -> deadline.bound(
                        () -> activationGuard.revalidate(
                                activation.proof()),
                        "revalidate activation around checkpoint index restore"))
                .thenCompose(ignored -> requireUntrimmed(
                        streamId, targetOffset, deadline))
                .thenCompose(ignored -> loadTargetIdentity(
                        selected.candidate().record(), deadline))
                .thenCompose(identity -> {
                    if (!identity.equals(selected.object())) {
                        return CompletableFuture.failedFuture(condition(
                                "checkpoint target physical root changed"));
                    }
                    return deadline.bound(
                                    () -> protections.revalidate(
                                            protection,
                                            ownerRevalidator),
                                    "revalidate checkpoint target protection")
                            .thenApply(unused -> null);
                });
    }

    private CompletableFuture<Void> requireUntrimmed(
            StreamId streamId,
            long targetOffset,
            RepairDeadline deadline) {
        return deadline.bound(
                        () -> l0Store.getStreamSnapshot(cluster, streamId),
                        "revalidate trim during checkpoint index repair")
                .thenAccept(snapshot -> {
                    requireSnapshotStream(streamId, snapshot);
                    if (targetOffset < snapshot.trim().trimOffset()) {
                        throw new TargetTrimmedException();
                    }
                    if (targetOffset
                            >= snapshot.committedEnd().committedEndOffset()) {
                        throw invariant(
                                "checkpoint repair target left committed head truth",
                                null);
                    }
                });
    }

    private CompletableFuture<Void> requireExactRoot(
            VersionedRecoveryCheckpointRoot expected) {
        return requireExactRoot(
                new StreamId(expected.value().streamId()),
                Optional.of(expected));
    }

    private CompletableFuture<Void> requireExactRoot(
            StreamId streamId,
            Optional<VersionedRecoveryCheckpointRoot> expected) {
        return generationStore.getRecoveryRoot(cluster, streamId)
                .thenAccept(actual -> {
                    if (!actual.equals(expected)) {
                        throw new RecoveryRootChangedException();
                    }
                });
    }

    private static void requireLiveCoverage(
            long targetOffset, AnchorAwareCommitWalk walk) {
        boolean covered = walk.commitsNewestFirst().stream()
                .map(value -> value.canonicalCommit())
                .anyMatch(commit -> commit.offsetStart() <= targetOffset
                        && targetOffset < commit.offsetEnd());
        if (!covered) {
            throw invariant(
                    "root-stable live tail does not cover its repair target",
                    null);
        }
    }

    private static RecoveryCheckpointReferenceRecord referenceCovering(
            VersionedRecoveryCheckpointRoot root, long offset) {
        return root.value().checkpoints().stream()
                .filter(reference -> reference.coveredStartOffset() <= offset
                        && offset < reference.coveredEndOffset())
                .findFirst()
                .orElseThrow(() -> invariant(
                        "recovery root has a gap at the repair target", null));
    }

    private static PhysicalObjectIdentity checkpointIdentity(
            RecoveryCheckpointReferenceRecord reference) {
        PhysicalObjectIdentity identity = PhysicalObjectIdentity.create(
                new ObjectKey(reference.objectKey()),
                Optional.of(new ObjectId(reference.objectId())),
                PhysicalObjectKind.RECOVERY_CHECKPOINT,
                reference.objectLength(),
                new Checksum(
                        ChecksumType.CRC32C,
                        reference.storageCrc32c()),
                Optional.of(new Checksum(
                        ChecksumType.SHA256,
                        reference.contentSha256())),
                Optional.empty());
        if (!identity.objectKeyHash().equals(
                new ObjectKeyHash(reference.objectKeyHash()))) {
            throw invariant(
                    "recovery checkpoint reference has a wrong object-key hash",
                    null);
        }
        return identity;
    }

    private void requireExactReference(
            VersionedRecoveryCheckpointRoot root,
            RecoveryCheckpointReferenceRecord reference,
            RecoveryCheckpointObject object) {
        var header = object.header();
        if (!header.cluster().equals(cluster)
                || !header.streamId().value().equals(root.value().streamId())
                || header.checkpointSequence()
                        != reference.checkpointSequence()
                || !header.checkpointAttemptId().equals(
                        reference.checkpointAttemptId())
                || header.coverage().startOffset()
                        != reference.coveredStartOffset()
                || header.coverage().endOffset()
                        != reference.coveredEndOffset()
                || header.firstCommitVersion()
                        != reference.firstCommitVersion()
                || header.lastCommitVersion()
                        != reference.lastCommitVersion()
                || header.cumulativeSizeAtStart()
                        != reference.cumulativeSizeAtStart()
                || header.cumulativeSizeAtEnd()
                        != reference.cumulativeSizeAtEnd()
                || !header.firstCommitId().equals(reference.firstCommitId())
                || !header.lastCommitId().equals(reference.lastCommitId())
                || !header.sourceHeadCommitId().equals(
                        reference.sourceHeadCommitId())
                || header.sourceHeadCommitVersion()
                        != reference.sourceHeadCommitVersion()
                || !header.projectionIdentitySha256().value().equals(
                        reference.projectionIdentitySha256())
                || header.expectedEntryCount()
                        != reference.commitEntryCount()
                || header.expectedPublicationCount()
                        != reference.publicationCount()
                || !object.objectId().value().equals(reference.objectId())
                || !object.objectKey().value().equals(reference.objectKey())
                || object.objectLength() != reference.objectLength()
                || !object.contentSha256().value().equals(
                        reference.contentSha256())) {
            throw invariant(
                    "verified NRC1 object differs from recovery-root reference",
                    null);
        }
    }

    private static void requireSnapshotStream(
            StreamId streamId, StreamMetadataSnapshot snapshot) {
        if (!snapshot.metadata().streamId().equals(streamId.value())
                || !snapshot.committedEnd().streamId().equals(streamId.value())
                || !snapshot.trim().streamId().equals(streamId.value())) {
            throw invariant(
                    "generation repair snapshot belongs to another stream",
                    null);
        }
    }

    private static <T> CompletableFuture<T> withLease(
            CompletableFuture<ObjectReadLease> acquired,
            Function<ObjectReadLease, CompletableFuture<T>> operation) {
        return acquired.thenCompose(lease -> {
            CompletableFuture<T> source;
            try {
                source = Objects.requireNonNull(
                        operation.apply(lease), "checkpoint repair operation");
            } catch (Throwable failure) {
                source = CompletableFuture.failedFuture(failure);
            }
            CompletableFuture<T> result = new CompletableFuture<>();
            source.whenComplete((value, failure) -> lease.release()
                    .whenComplete((ignored, releaseFailure) -> {
                        if (failure == null && releaseFailure == null) {
                            result.complete(value);
                            return;
                        }
                        Throwable exact = failure == null
                                ? unwrap(releaseFailure)
                                : unwrap(failure);
                        if (failure != null && releaseFailure != null) {
                            exact.addSuppressed(unwrap(releaseFailure));
                        }
                        result.completeExceptionally(exact);
                    }));
            return result;
        });
    }

    private static byte[] bytes(ByteBuffer value) {
        ByteBuffer copy = value.asReadOnlyBuffer();
        byte[] result = new byte[copy.remaining()];
        copy.get(result);
        return result;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = Objects.requireNonNull(failure, "failure");
        while ((current instanceof CompletionException
                        || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static NereusException condition(String message) {
        return new NereusException(
                ErrorCode.METADATA_CONDITION_FAILED, true, message);
    }

    private static NereusException invariant(
            String message, Throwable cause) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION,
                false,
                message,
                cause);
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }

    private record IndexCandidate(
            RecoveryCheckpointPublication publication,
            GenerationIndexRecord record,
            Checksum rawDigest,
            Checksum durableDigest,
            String indexKey) {
    }

    private record SelectedCandidate(
            IndexCandidate candidate, PhysicalObjectIdentity object) {
    }

    private record Activation(
            LiveProjectionSubject subject, GenerationActivationProof proof) {
    }

    private static final class RecoveryRootChangedException
            extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    private static final class TargetTrimmedException
            extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    private static final class CandidateUnavailableException
            extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    private static final class RepairDeadline implements AutoCloseable {
        private final long expiresAtNanos;
        private final Clock clock;

        private RepairDeadline(Duration timeout, Clock clock) {
            Objects.requireNonNull(timeout, "timeout");
            if (timeout.isZero() || timeout.isNegative()) {
                throw new IllegalArgumentException("timeout must be positive");
            }
            this.clock = Objects.requireNonNull(clock, "clock");
            long timeoutNanos;
            try {
                timeoutNanos = timeout.toNanos();
            } catch (ArithmeticException overflow) {
                timeoutNanos = Long.MAX_VALUE;
            }
            long now = System.nanoTime();
            expiresAtNanos = timeoutNanos >= Long.MAX_VALUE - now
                    ? Long.MAX_VALUE
                    : now + timeoutNanos;
        }

        private Duration remaining(String action) {
            long remaining = expiresAtNanos - System.nanoTime();
            if (remaining <= 0) {
                throw timeout(action);
            }
            return Duration.ofNanos(remaining);
        }

        private <T> CompletableFuture<T> bound(
                Supplier<CompletableFuture<T>> operation, String action) {
            long remaining = remaining(action).toNanos();
            CompletableFuture<T> source;
            try {
                source = Objects.requireNonNull(
                        operation.get(), "operation future");
            } catch (Throwable failure) {
                return CompletableFuture.failedFuture(failure);
            }
            return source.orTimeout(remaining, TimeUnit.NANOSECONDS)
                    .handle((value, failure) -> {
                        if (failure == null) {
                            return value;
                        }
                        Throwable exact = unwrap(failure);
                        if (exact instanceof TimeoutException) {
                            throw timeout(action);
                        }
                        if (exact instanceof RuntimeException runtime) {
                            throw runtime;
                        }
                        throw new CompletionException(exact);
                    });
        }

        private long maximumReadDeadlineMillis() {
            long millis = Math.max(
                    1L,
                    remaining("acquire recovery checkpoint read pin")
                            .toMillis());
            try {
                return Math.addExact(clock.millis(), millis);
            } catch (ArithmeticException overflow) {
                return Long.MAX_VALUE;
            }
        }

        private static NereusException timeout(String action) {
            return new NereusException(
                    ErrorCode.TIMEOUT,
                    true,
                    action + " exceeded the generation repair deadline");
        }

        @Override
        public void close() {
        }
    }
}
