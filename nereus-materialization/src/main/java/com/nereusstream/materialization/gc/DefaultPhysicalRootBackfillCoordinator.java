/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamState;
import com.nereusstream.api.keys.KeyComponentCodec;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.api.target.ReadTarget;
import com.nereusstream.core.append.GenerationZeroProtectionIdentities;
import com.nereusstream.core.capability.GenerationProjectionAuthorityReader;
import com.nereusstream.core.capability.GenerationProjectionAuthoritySnapshot;
import com.nereusstream.core.capability.LiveProjectionSubject;
import com.nereusstream.core.physical.GcAuthorityToken;
import com.nereusstream.core.physical.ObjectProtection;
import com.nereusstream.core.physical.ObjectProtectionManager;
import com.nereusstream.core.physical.ObjectProtectionOwner;
import com.nereusstream.core.physical.ObjectProtectionRequest;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.core.read.MetadataPhysicalObjectIdentityResolver;
import com.nereusstream.core.read.PhysicalObjectIdentityResolver;
import com.nereusstream.metadata.oxia.AppendRecoveryAnchor;
import com.nereusstream.metadata.oxia.AppendRecoveryCommit;
import com.nereusstream.metadata.oxia.AppendRecoveryHead;
import com.nereusstream.metadata.oxia.AppendRecoveryTailCursor;
import com.nereusstream.metadata.oxia.AppendRecoveryTailPage;
import com.nereusstream.metadata.oxia.CursorKeyspace;
import com.nereusstream.metadata.oxia.CursorMetadataDigests;
import com.nereusstream.metadata.oxia.CursorMetadataStore;
import com.nereusstream.metadata.oxia.CursorScanPage;
import com.nereusstream.metadata.oxia.CursorScanToken;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationProtocolActivationStore;
import com.nereusstream.metadata.oxia.GenerationScanPage;
import com.nereusstream.metadata.oxia.OxiaKeyspace;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.ProjectionIdentity;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.StreamRegistrationScanPage;
import com.nereusstream.metadata.oxia.VersionedCursorRetention;
import com.nereusstream.metadata.oxia.VersionedCursorState;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedGenerationProtocolActivation;
import com.nereusstream.metadata.oxia.VersionedGenerationZeroIndex;
import com.nereusstream.metadata.oxia.VersionedMaterializationStreamRegistration;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.VersionedRecoveryCheckpointRoot;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.CursorRecordLifecycle;
import com.nereusstream.metadata.oxia.records.CursorRetentionLifecycle;
import com.nereusstream.metadata.oxia.records.CursorSnapshotReferenceRecord;
import com.nereusstream.metadata.oxia.records.CursorStateRecord;
import com.nereusstream.metadata.oxia.records.GenerationBackfillProofRecord;
import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationRecord;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.ReferenceDomainVersionRecord;
import com.nereusstream.metadata.oxia.retirement.SourceRetirementMetadataStore;
import com.nereusstream.metadata.oxia.retirement.VersionedGenerationZeroCommit;
import com.nereusstream.objectstore.HeadObjectOptions;
import com.nereusstream.objectstore.HeadObjectResult;
import com.nereusstream.objectstore.ObjectStore;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Full 64-shard live-reference backfill with exact owner/root handshakes and
 * activation-proof publication only after a stable zero-failure pass.
 */
public final class DefaultPhysicalRootBackfillCoordinator
        implements PhysicalRootBackfillCoordinator {
    private static final ReadTargetCodecRegistry TARGET_CODECS =
            ReadTargetCodecRegistry.phase15();
    private static final String CURSOR_FORMAT = "NCS1";
    private static final String CURSOR_OBJECT_TYPE =
            "CURSOR_SNAPSHOT_OBJECT";

    private final String cluster;
    private final OxiaMetadataStore l0Metadata;
    private final GenerationMetadataStore generations;
    private final SourceRetirementMetadataStore sourceRetirement;
    private final CursorMetadataStore cursors;
    private final GenerationProtocolActivationStore activations;
    private final PhysicalObjectMetadataStore physicalMetadata;
    private final ObjectProtectionManager protections;
    private final ObjectStore objectStore;
    private final GenerationProjectionAuthorityReader projectionAuthorities;
    private final PhysicalObjectIdentityResolver dataIdentityResolver;
    private final int metadataPageSize;
    private final int cursorPageSize;
    private final Duration objectStoreRequestTimeout;
    private final Clock clock;
    private final LongSupplier nanoTime;
    private final F4Keyspace f4Keys;
    private final OxiaKeyspace l0Keys;
    private final CursorKeyspace cursorKeys;

    public DefaultPhysicalRootBackfillCoordinator(
            String cluster,
            OxiaMetadataStore l0Metadata,
            GenerationMetadataStore generations,
            SourceRetirementMetadataStore sourceRetirement,
            CursorMetadataStore cursors,
            GenerationProtocolActivationStore activations,
            PhysicalObjectMetadataStore physicalMetadata,
            ObjectProtectionManager protections,
            ObjectStore objectStore,
            GenerationProjectionAuthorityReader projectionAuthorities,
            int metadataPageSize,
            Duration objectStoreRequestTimeout,
            Clock clock) {
        this(
                cluster,
                l0Metadata,
                generations,
                sourceRetirement,
                cursors,
                activations,
                physicalMetadata,
                protections,
                objectStore,
                projectionAuthorities,
                metadataPageSize,
                metadataPageSize,
                objectStoreRequestTimeout,
                clock);
    }

    public DefaultPhysicalRootBackfillCoordinator(
            String cluster,
            OxiaMetadataStore l0Metadata,
            GenerationMetadataStore generations,
            SourceRetirementMetadataStore sourceRetirement,
            CursorMetadataStore cursors,
            GenerationProtocolActivationStore activations,
            PhysicalObjectMetadataStore physicalMetadata,
            ObjectProtectionManager protections,
            ObjectStore objectStore,
            GenerationProjectionAuthorityReader projectionAuthorities,
            int metadataPageSize,
            int cursorPageSize,
            Duration objectStoreRequestTimeout,
            Clock clock) {
        this(
                cluster,
                l0Metadata,
                generations,
                sourceRetirement,
                cursors,
                activations,
                physicalMetadata,
                protections,
                objectStore,
                projectionAuthorities,
                metadataPageSize,
                cursorPageSize,
                objectStoreRequestTimeout,
                clock,
                System::nanoTime);
    }

    DefaultPhysicalRootBackfillCoordinator(
            String cluster,
            OxiaMetadataStore l0Metadata,
            GenerationMetadataStore generations,
            SourceRetirementMetadataStore sourceRetirement,
            CursorMetadataStore cursors,
            GenerationProtocolActivationStore activations,
            PhysicalObjectMetadataStore physicalMetadata,
            ObjectProtectionManager protections,
            ObjectStore objectStore,
            GenerationProjectionAuthorityReader projectionAuthorities,
            int metadataPageSize,
            int cursorPageSize,
            Duration objectStoreRequestTimeout,
            Clock clock,
            LongSupplier nanoTime) {
        this.cluster = requireText(cluster, "cluster");
        this.l0Metadata = Objects.requireNonNull(
                l0Metadata, "l0Metadata");
        this.generations = Objects.requireNonNull(
                generations, "generations");
        this.sourceRetirement = Objects.requireNonNull(
                sourceRetirement, "sourceRetirement");
        this.cursors = Objects.requireNonNull(cursors, "cursors");
        this.activations = Objects.requireNonNull(
                activations, "activations");
        this.physicalMetadata = Objects.requireNonNull(
                physicalMetadata, "physicalMetadata");
        this.protections = Objects.requireNonNull(
                protections, "protections");
        this.objectStore = Objects.requireNonNull(
                objectStore, "objectStore");
        this.projectionAuthorities = Objects.requireNonNull(
                projectionAuthorities, "projectionAuthorities");
        if (metadataPageSize <= 0 || metadataPageSize > 4_096) {
            throw new IllegalArgumentException(
                    "metadataPageSize must be in [1, 4096]");
        }
        this.metadataPageSize = metadataPageSize;
        if (cursorPageSize <= 0 || cursorPageSize > 4_096) {
            throw new IllegalArgumentException(
                    "cursorPageSize must be in [1, 4096]");
        }
        this.cursorPageSize = cursorPageSize;
        this.objectStoreRequestTimeout = requirePositive(
                objectStoreRequestTimeout, "objectStoreRequestTimeout");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        this.dataIdentityResolver =
                new MetadataPhysicalObjectIdentityResolver(
                        this.cluster, this.l0Metadata, this.physicalMetadata);
        this.f4Keys = new F4Keyspace(this.cluster);
        this.l0Keys = new OxiaKeyspace(this.cluster);
        this.cursorKeys = new CursorKeyspace(this.cluster);
    }

    @Override
    public CompletableFuture<PhysicalRootBackfillReport> run(
            PhysicalRootBackfillRequest request) {
        PhysicalRootBackfillRequest exact =
                Objects.requireNonNull(request, "request");
        Deadline deadline = Deadline.start(exact.timeout(), nanoTime);
        RunAccumulator accumulator = new RunAccumulator(exact);
        return deadline.call(() -> activations.get(cluster))
                .handle((optional, failure) -> {
                    if (failure != null) {
                        accumulator.globalFailure(
                                CoverageSide.BOTH,
                                PhysicalRootBackfillStage.FINAL_REVALIDATION,
                                f4Keys.generationProtocolActivationKey(),
                                errorCode(
                                        PhysicalRootBackfillStage
                                                .FINAL_REVALIDATION,
                                        failure));
                        return Optional
                                .<VersionedGenerationProtocolActivation>empty();
                    }
                    return optional;
                })
                .thenCompose(optional -> start(
                        optional, exact, accumulator, deadline))
                .exceptionally(failure -> {
                    accumulator.globalFailure(
                            CoverageSide.BOTH,
                            PhysicalRootBackfillStage.FINAL_REVALIDATION,
                            f4Keys.generationProtocolActivationKey(),
                            errorCode(
                                    PhysicalRootBackfillStage
                                            .FINAL_REVALIDATION,
                                    failure));
                    return accumulator.report();
                });
    }

    @Override
    public CompletableFuture<PhysicalRootBackfillReport> runRollover(
            PhysicalRootBackfillRequest request,
            VersionedGenerationProtocolActivation expectedCurrent) {
        final PhysicalRootBackfillRequest exact;
        final VersionedGenerationProtocolActivation expected;
        final Deadline deadline;
        try {
            exact = Objects.requireNonNull(request, "request");
            expected = Objects.requireNonNull(
                    expectedCurrent, "expectedCurrent");
            deadline = Deadline.start(exact.timeout(), nanoTime);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        RunAccumulator accumulator = new RunAccumulator(exact);
        return deadline.call(() -> activations.get(cluster))
                .handle((optional, failure) -> {
                    if (failure != null) {
                        accumulator.globalFailure(
                                CoverageSide.BOTH,
                                PhysicalRootBackfillStage.FINAL_REVALIDATION,
                                f4Keys.generationProtocolActivationKey(),
                                errorCode(
                                        PhysicalRootBackfillStage
                                                .FINAL_REVALIDATION,
                                        failure));
                        return Optional
                                .<VersionedGenerationProtocolActivation>empty();
                    }
                    return optional;
                })
                .thenCompose(optional -> startRollover(
                        optional,
                        expected,
                        exact,
                        accumulator,
                        deadline))
                .exceptionally(failure -> {
                    accumulator.globalFailure(
                            CoverageSide.BOTH,
                            PhysicalRootBackfillStage.FINAL_REVALIDATION,
                            f4Keys.generationProtocolActivationKey(),
                            errorCode(
                                    PhysicalRootBackfillStage
                                            .FINAL_REVALIDATION,
                                    failure));
                    return accumulator.report();
                });
    }

    private CompletableFuture<PhysicalRootBackfillReport> start(
            Optional<VersionedGenerationProtocolActivation> optional,
            PhysicalRootBackfillRequest request,
            RunAccumulator accumulator,
            Deadline deadline) {
        if (optional.isEmpty()) {
            accumulator.globalFailure(
                    CoverageSide.BOTH,
                    PhysicalRootBackfillStage.FINAL_REVALIDATION,
                    f4Keys.generationProtocolActivationKey(),
                    "ACTIVATION_ABSENT");
            return CompletableFuture.completedFuture(
                    accumulator.report());
        }
        VersionedGenerationProtocolActivation activation =
                optional.orElseThrow();
        String precondition = activationPrecondition(
                activation.value(),
                request.expectedBrokerReadinessEpoch());
        if (precondition != null) {
            accumulator.globalFailure(
                    CoverageSide.BOTH,
                    PhysicalRootBackfillStage.FINAL_REVALIDATION,
                    activation.key(),
                    precondition);
            return CompletableFuture.completedFuture(
                    accumulator.report());
        }
        ActivationBasis basis = new ActivationBasis(
                activation.value().requiredReferenceDomains(),
                activation.value().streamRegistrationBackfill());
        return scanShard(
                        0, request, accumulator, deadline)
                .thenCompose(ignored -> publishProofs(
                        basis, request, accumulator, deadline))
                .thenApply(ignored -> accumulator.report());
    }

    private CompletableFuture<PhysicalRootBackfillReport> startRollover(
            Optional<VersionedGenerationProtocolActivation> optional,
            VersionedGenerationProtocolActivation expected,
            PhysicalRootBackfillRequest request,
            RunAccumulator accumulator,
            Deadline deadline) {
        if (optional.isEmpty()) {
            accumulator.globalFailure(
                    CoverageSide.BOTH,
                    PhysicalRootBackfillStage.FINAL_REVALIDATION,
                    f4Keys.generationProtocolActivationKey(),
                    "ACTIVATION_ABSENT");
            return CompletableFuture.completedFuture(
                    accumulator.report());
        }
        VersionedGenerationProtocolActivation current =
                optional.orElseThrow();
        String precondition = rolloverPrecondition(
                current, expected, request.expectedBrokerReadinessEpoch());
        if (precondition != null) {
            accumulator.globalFailure(
                    CoverageSide.BOTH,
                    PhysicalRootBackfillStage.FINAL_REVALIDATION,
                    current.key(),
                    precondition);
            return CompletableFuture.completedFuture(
                    accumulator.report());
        }
        return scanShard(0, request, accumulator, deadline)
                .thenCompose(ignored -> revalidateRolloverBasis(
                        expected, accumulator, deadline))
                .thenApply(ignored -> accumulator.report());
    }

    private CompletableFuture<Void> revalidateRolloverBasis(
            VersionedGenerationProtocolActivation expected,
            RunAccumulator accumulator,
            Deadline deadline) {
        if (accumulator.failureCount != 0) {
            return CompletableFuture.completedFuture(null);
        }
        return deadline.call(() -> activations.get(cluster))
                .thenAccept(optional -> {
                    if (optional.isEmpty()
                            || !optional.orElseThrow().equals(expected)) {
                        accumulator.globalFailure(
                                CoverageSide.BOTH,
                                PhysicalRootBackfillStage
                                        .FINAL_REVALIDATION,
                                expected.key(),
                                "ACTIVATION_AUTHORITY_CHANGED");
                    }
                });
    }

    private CompletableFuture<Void> scanShard(
            int shard,
            PhysicalRootBackfillRequest request,
            RunAccumulator accumulator,
            Deadline deadline) {
        if (shard == F4Keyspace.MATERIALIZATION_REGISTRY_SHARDS) {
            return CompletableFuture.completedFuture(null);
        }
        accumulator.shardStarted(shard);
        return scanRegistryPage(
                        shard,
                        Optional.empty(),
                        null,
                        request,
                        accumulator,
                        deadline)
                .thenCompose(ignored -> {
                    accumulator.shardCompleted(shard);
                    return scanShard(
                            shard + 1,
                            request,
                            accumulator,
                            deadline);
                });
    }

    private CompletableFuture<Void> scanRegistryPage(
            int shard,
            Optional<F4ScanToken> continuation,
            String previousKey,
            PhysicalRootBackfillRequest request,
            RunAccumulator accumulator,
            Deadline deadline) {
        return deadline.call(() -> generations.scanStreamRegistrations(
                        cluster,
                        shard,
                        continuation,
                        metadataPageSize))
                .handle((page, failure) -> {
                    if (failure != null) {
                        accumulator.globalFailure(
                                CoverageSide.BOTH,
                                PhysicalRootBackfillStage.REGISTRY_SCAN,
                                f4Keys.materializationRegistryPrefix(
                                        shard),
                                errorCode(
                                        PhysicalRootBackfillStage
                                                .REGISTRY_SCAN,
                                        failure));
                        return null;
                    }
                    return page;
                })
                .thenCompose(page -> {
                    if (page == null) {
                        return CompletableFuture.completedFuture(null);
                    }
                    try {
                        requireRegistryProgress(page, previousKey);
                    } catch (RuntimeException failure) {
                        accumulator.globalFailure(
                                CoverageSide.BOTH,
                                PhysicalRootBackfillStage.REGISTRY_SCAN,
                                f4Keys.materializationRegistryPrefix(
                                        shard),
                                "REGISTRY_SCAN_DID_NOT_ADVANCE");
                        return CompletableFuture.completedFuture(null);
                    }
                    return processRegistrationPage(
                                    page.values(),
                                    0,
                                    request,
                                    accumulator,
                                    deadline)
                            .thenCompose(ignored -> {
                                if (page.continuation().isEmpty()) {
                                    return CompletableFuture
                                            .completedFuture(null);
                                }
                                String lastKey = page.values()
                                        .get(page.values().size() - 1)
                                        .key();
                                return scanRegistryPage(
                                        shard,
                                        page.continuation(),
                                        lastKey,
                                        request,
                                        accumulator,
                                        deadline);
                            });
                });
    }

    private CompletableFuture<Void> processRegistrationPage(
            List<VersionedMaterializationStreamRegistration> registrations,
            int start,
            PhysicalRootBackfillRequest request,
            RunAccumulator accumulator,
            Deadline deadline) {
        if (start == registrations.size()) {
            return CompletableFuture.completedFuture(null);
        }
        int end = Math.min(
                registrations.size(),
                Math.addExact(
                        start, request.maxConcurrentStreams()));
        ArrayList<CompletableFuture<StreamBackfillResult>> futures =
                new ArrayList<>(end - start);
        for (int index = start; index < end; index++) {
            VersionedMaterializationStreamRegistration registration =
                    registrations.get(index);
            StreamId stream =
                    new StreamId(registration.value().streamId());
            if (!accumulator.admitStream(stream)) {
                accumulator.globalFailure(
                        CoverageSide.BOTH,
                        PhysicalRootBackfillStage.REGISTRY_SCAN,
                        registration.key(),
                        "DUPLICATE_REGISTERED_STREAM");
                continue;
            }
            futures.add(processRegistration(
                    registration, deadline));
        }
        return CompletableFuture.allOf(
                        futures.toArray(CompletableFuture[]::new))
                .thenCompose(ignored -> {
                    for (CompletableFuture<StreamBackfillResult> future :
                            futures) {
                        accumulator.fold(future.join());
                    }
                    return processRegistrationPage(
                            registrations,
                            end,
                            request,
                            accumulator,
                            deadline);
                });
    }

    private CompletableFuture<StreamBackfillResult> processRegistration(
            VersionedMaterializationStreamRegistration registration,
            Deadline deadline) {
        StreamAccumulator accumulator =
                new StreamAccumulator(registration);
        StreamId streamId =
                new StreamId(registration.value().streamId());
        try {
            if (!registration.key().equals(
                    f4Keys.materializationRegistryKey(streamId))) {
                accumulator.failure(
                        CoverageSide.BOTH,
                        PhysicalRootBackfillStage.REGISTRY_SCAN,
                        registration.key(),
                        "REGISTRATION_KEY_IDENTITY_MISMATCH");
                return CompletableFuture.completedFuture(
                        accumulator.result(false));
            }
            accumulator.registration();
        } catch (RuntimeException failure) {
            accumulator.failure(
                    CoverageSide.BOTH,
                    PhysicalRootBackfillStage.REGISTRY_SCAN,
                    registration.key(),
                    "REGISTRATION_IDENTITY_INVALID");
            return CompletableFuture.completedFuture(
                    accumulator.result(false));
        }
        return deadline.call(() -> l0Metadata.getStreamSnapshot(
                        cluster, streamId))
                .thenCompose(snapshot -> captureProjection(
                        registration,
                        snapshot,
                        accumulator,
                        deadline))
                .thenCompose(capture -> {
                    accumulator.capture = capture;
                    if (!capture.live()) {
                        return finalRevalidate(
                                accumulator, deadline);
                    }
                    accumulator.live = true;
                    return scanRecovery(
                                    accumulator, deadline)
                            .thenCompose(ignored ->
                                    scanGenerationZero(
                                            accumulator, deadline))
                            .thenCompose(ignored ->
                                    scanCursorInventory(
                                            accumulator, deadline))
                            .thenCompose(ignored ->
                                    finalRevalidate(
                                            accumulator, deadline));
                })
                .exceptionally(failure -> {
                    Throwable exact = unwrap(failure);
                    PhysicalRootBackfillStage stage =
                            stage(exact,
                                    PhysicalRootBackfillStage
                                            .PROJECTION_READ);
                    accumulator.failure(
                            CoverageSide.BOTH,
                            stage,
                            registration.key(),
                            errorCode(stage, exact));
                    return accumulator.result(false);
                });
    }

    private CompletableFuture<RegistrationCapture> captureProjection(
            VersionedMaterializationStreamRegistration registration,
            StreamMetadataSnapshot snapshot,
            StreamAccumulator accumulator,
            Deadline deadline) {
        MaterializationStreamRegistrationRecord value =
                registration.value();
        StreamId streamId = new StreamId(value.streamId());
        if (!snapshot.metadata().streamId().equals(streamId.value())) {
            return failedStep(
                    PhysicalRootBackfillStage.PROJECTION_READ,
                    invariant(
                            "registered stream snapshot belongs to another stream"));
        }
        final StreamState state;
        final StorageProfile registeredProfile;
        final StorageProfile actualProfile;
        final ProjectionRef projectionRef;
        try {
            state = StreamState.valueOf(snapshot.metadata().state());
            registeredProfile =
                    StorageProfile.valueOf(value.storageProfile())
                            .canonical();
            actualProfile =
                    StorageProfile.valueOf(snapshot.metadata().profile())
                            .canonical();
            projectionRef = ProjectionIdentity.decode(
                            value.projectionRef())
                    .orElseThrow(() -> invariant(
                            "registered stream has no projection reference"));
        } catch (RuntimeException failure) {
            return failedStep(
                    PhysicalRootBackfillStage.PROJECTION_READ,
                    invariant(
                            "registered stream carries an unsupported state/profile/projection",
                            failure));
        }
        LiveProjectionSubject subject = new LiveProjectionSubject(
                streamId,
                projectionRef,
                new Checksum(
                        ChecksumType.SHA256,
                        value.projectionIdentitySha256()));
        accumulator.streamSnapshot(snapshot);
        return deadline.call(() ->
                        projectionAuthorities.capture(subject))
                .thenApply(projection -> {
                    accumulator.projection(projection);
                    boolean streamLive =
                            (state == StreamState.ACTIVE
                                            || state
                                                    == StreamState.SEALED)
                                    && registeredProfile
                                            == actualProfile
                                    && actualProfile
                                            .objectMaterializationEnabled()
                                    && value.lastHintCommitVersion()
                                            <= snapshot.committedEnd()
                                                    .commitVersion();
                    if (projection.live() && !streamLive) {
                        throw invariant(
                                "live projection conflicts with its registered L0 stream authority");
                    }
                    return new RegistrationCapture(
                            registration,
                            snapshot,
                            projection,
                            streamLive && projection.live());
                })
                .exceptionallyCompose(failure -> failedStep(
                        PhysicalRootBackfillStage.PROJECTION_READ,
                        unwrap(failure)));
    }

    private CompletableFuture<Void> scanRecovery(
            StreamAccumulator accumulator,
            Deadline deadline) {
        StreamId stream = accumulator.streamId();
        return deadline.call(() -> generations.getRecoveryRoot(
                        cluster, stream))
                .handle((root, failure) -> {
                    if (failure != null) {
                        accumulator.failure(
                                CoverageSide.DATA,
                                PhysicalRootBackfillStage
                                        .HEAD_COMMIT_SCAN,
                                f4Keys.recoveryRootKey(stream),
                                errorCode(
                                        PhysicalRootBackfillStage
                                                .HEAD_COMMIT_SCAN,
                                        failure));
                        return null;
                    }
                    return root;
                })
                .thenCompose(root -> {
                    if (root == null) {
                        return CompletableFuture.completedFuture(null);
                    }
                    accumulator.recoveryRoot = root;
                    accumulator.recoveryRoot(
                            f4Keys.recoveryRootKey(stream),
                            root);
                    AppendRecoveryAnchor anchor = root
                            .map(value -> anchor(stream, value))
                            .orElseGet(() ->
                                    AppendRecoveryAnchor.genesis(
                                            stream));
                    accumulator.recoveryAnchor = anchor;
                    return scanRecoveryPage(
                            accumulator,
                            anchor,
                            Optional.empty(),
                            null,
                            deadline);
                });
    }

    private CompletableFuture<Void> scanRecoveryPage(
            StreamAccumulator accumulator,
            AppendRecoveryAnchor anchor,
            Optional<AppendRecoveryTailCursor> continuation,
            AppendRecoveryHead expectedHead,
            Deadline deadline) {
        StreamId stream = accumulator.streamId();
        return deadline.call(() -> l0Metadata.readAppendRecoveryTail(
                        cluster,
                        stream,
                        anchor,
                        continuation,
                        metadataPageSize))
                .handle((page, failure) -> {
                    if (failure != null) {
                        accumulator.failure(
                                CoverageSide.DATA,
                                PhysicalRootBackfillStage
                                        .HEAD_COMMIT_SCAN,
                                l0Keys.streamHeadKey(stream),
                                errorCode(
                                        PhysicalRootBackfillStage
                                                .HEAD_COMMIT_SCAN,
                                        failure));
                        return null;
                    }
                    return page;
                })
                .thenCompose(page -> {
                    if (page == null) {
                        return CompletableFuture.completedFuture(null);
                    }
                    AppendRecoveryHead observed = page.observedHead();
                    if (expectedHead != null
                            && !expectedHead.equals(observed)) {
                        accumulator.failure(
                                CoverageSide.DATA,
                                PhysicalRootBackfillStage
                                        .HEAD_COMMIT_SCAN,
                                l0Keys.streamHeadKey(stream),
                                "HEAD_CHANGED_DURING_SCAN");
                        return CompletableFuture.completedFuture(null);
                    }
                    if (continuation.isEmpty()) {
                        accumulator.recoveryHead = observed;
                        accumulator.recoveryHead(observed);
                    }
                    return processRecoveryCommits(
                                    accumulator,
                                    page.commitsNewestFirst(),
                                    0,
                                    deadline)
                            .thenCompose(ignored -> {
                                if (page.continuation().isEmpty()) {
                                    return CompletableFuture
                                            .completedFuture(null);
                                }
                                return scanRecoveryPage(
                                        accumulator,
                                        anchor,
                                        page.continuation(),
                                        observed,
                                        deadline);
                            });
                });
    }

    private CompletableFuture<Void> processRecoveryCommits(
            StreamAccumulator accumulator,
            List<AppendRecoveryCommit> commits,
            int index,
            Deadline deadline) {
        if (index == commits.size()) {
            return CompletableFuture.completedFuture(null);
        }
        AppendRecoveryCommit commit = commits.get(index);
        accumulator.commitAuthority(commit);
        final ObjectSliceReadTarget target;
        try {
            ReadTarget decoded =
                    TARGET_CODECS.decode(
                            commit.canonicalCommit().readTarget());
            if (!(decoded instanceof ObjectSliceReadTarget object)
                    || object.objectType()
                            != ObjectType.MULTI_STREAM_WAL_OBJECT) {
                throw invariant(
                        "reachable commit has an unsupported live target");
            }
            target = object;
        } catch (RuntimeException failure) {
            accumulator.failure(
                    CoverageSide.DATA,
                    PhysicalRootBackfillStage.HEAD_COMMIT_SCAN,
                    commit.key(),
                    "UNSUPPORTED_LIVE_COMMIT_TARGET");
            return processRecoveryCommits(
                    accumulator, commits, index + 1, deadline);
        }
        try {
            requireCompatibleProjection(
                    ProjectionIdentity.decode(
                            commit.canonicalCommit()
                                    .projectionRef()),
                    accumulator.capture.projection()
                            .subject()
                            .projectionRef());
        } catch (RuntimeException failure) {
            accumulator.failure(
                    CoverageSide.DATA,
                    PhysicalRootBackfillStage.HEAD_COMMIT_SCAN,
                    commit.key(),
                    "REACHABLE_COMMIT_PROJECTION_MISMATCH");
            return processRecoveryCommits(
                    accumulator, commits, index + 1, deadline);
        }
        ObjectProtectionOwner owner =
                new ObjectProtectionOwner(
                        commit.key(),
                        commit.sourceMetadataVersion(),
                        commit.sourceRecordSha256());
        ObjectKeyHash objectHash =
                ObjectKeyHash.from(target.objectKey());
        String referenceId =
                GenerationZeroProtectionIdentities
                        .reachableAppendReferenceId(
                                accumulator.streamId(),
                                commit.canonicalCommit().commitId(),
                                objectHash);
        ObjectProtectionManager.OwnerRevalidator revalidator =
                expected -> revalidateCommit(commit, expected, deadline);
        return protectDataReference(
                        accumulator,
                        commit.key(),
                        target,
                        ObjectProtectionType.REACHABLE_APPEND,
                        referenceId,
                        owner,
                        revalidator,
                        deadline)
                .thenCompose(ignored -> processRecoveryCommits(
                        accumulator, commits, index + 1, deadline));
    }

    private CompletableFuture<Void> scanGenerationZero(
            StreamAccumulator accumulator,
            Deadline deadline) {
        return scanGenerationZeroPage(
                accumulator, Optional.empty(), null, deadline);
    }

    private CompletableFuture<Void> scanGenerationZeroPage(
            StreamAccumulator accumulator,
            Optional<F4ScanToken> continuation,
            String previousKey,
            Deadline deadline) {
        StreamId stream = accumulator.streamId();
        return deadline.call(() -> generations.scanIndex(
                        cluster,
                        stream,
                        ReadView.COMMITTED,
                        0,
                        Long.MAX_VALUE,
                        continuation,
                        metadataPageSize))
                .handle((page, failure) -> {
                    if (failure != null) {
                        accumulator.failure(
                                CoverageSide.DATA,
                                PhysicalRootBackfillStage
                                        .GENERATION_ZERO_SCAN,
                                f4Keys.generationIndexPrefix(
                                        stream, ReadView.COMMITTED),
                                errorCode(
                                        PhysicalRootBackfillStage
                                                .GENERATION_ZERO_SCAN,
                                        failure));
                        return null;
                    }
                    return page;
                })
                .thenCompose(page -> {
                    if (page == null) {
                        return CompletableFuture.completedFuture(null);
                    }
                    if (previousKey != null
                            && !page.values().isEmpty()
                            && page.values()
                                            .get(0)
                                            .key()
                                            .compareTo(previousKey)
                                    <= 0) {
                        accumulator.failure(
                                CoverageSide.DATA,
                                PhysicalRootBackfillStage
                                        .GENERATION_ZERO_SCAN,
                                previousKey,
                                "GENERATION_SCAN_DID_NOT_ADVANCE");
                        return CompletableFuture.completedFuture(null);
                    }
                    return processGenerationCandidates(
                                    accumulator,
                                    page.values(),
                                    0,
                                    deadline)
                            .thenCompose(ignored -> {
                                if (page.continuation().isEmpty()) {
                                    return CompletableFuture
                                            .completedFuture(null);
                                }
                                return scanGenerationZeroPage(
                                        accumulator,
                                        page.continuation(),
                                        page.values()
                                                .get(page.values().size() - 1)
                                                .key(),
                                        deadline);
                            });
                });
    }

    private CompletableFuture<Void> processGenerationCandidates(
            StreamAccumulator accumulator,
            List<VersionedGenerationCandidate> candidates,
            int index,
            Deadline deadline) {
        if (index == candidates.size()) {
            return CompletableFuture.completedFuture(null);
        }
        VersionedGenerationCandidate candidate =
                candidates.get(index);
        if (candidate instanceof VersionedGenerationIndex) {
            return processGenerationCandidates(
                    accumulator,
                    candidates,
                    index + 1,
                    deadline);
        }
        if (!(candidate instanceof VersionedGenerationZeroIndex zero)) {
            accumulator.failure(
                    CoverageSide.DATA,
                    PhysicalRootBackfillStage.GENERATION_ZERO_SCAN,
                    candidate.key(),
                    "UNKNOWN_GENERATION_CANDIDATE");
            return processGenerationCandidates(
                    accumulator,
                    candidates,
                    index + 1,
                    deadline);
        }
        accumulator.generationZeroAuthority(zero);
        if (zero.value().tombstoned()) {
            accumulator.generationZeroTombstone(zero);
            return processGenerationCandidates(
                    accumulator,
                    candidates,
                    index + 1,
                    deadline);
        }
        if (zero.value().offsetEnd()
                        > accumulator.capture
                                .streamSnapshot()
                                .committedEnd()
                                .committedEndOffset()
                || zero.value().commitVersion()
                        > accumulator.capture
                                .streamSnapshot()
                                .committedEnd()
                                .commitVersion()) {
            accumulator.failure(
                    CoverageSide.DATA,
                    PhysicalRootBackfillStage.GENERATION_ZERO_SCAN,
                    zero.key(),
                    "GENERATION_ZERO_AHEAD_OF_HEAD");
            return processGenerationCandidates(
                    accumulator,
                    candidates,
                    index + 1,
                    deadline);
        }
        if (!(zero.value().readTarget()
                        instanceof ObjectSliceReadTarget target)
                || target.objectType()
                        != ObjectType.MULTI_STREAM_WAL_OBJECT) {
            accumulator.failure(
                    CoverageSide.DATA,
                    PhysicalRootBackfillStage.GENERATION_ZERO_SCAN,
                    zero.key(),
                    "UNSUPPORTED_LIVE_INDEX_TARGET");
            return processGenerationCandidates(
                    accumulator,
                    candidates,
                    index + 1,
                    deadline);
        }
        try {
            requireCompatibleProjection(
                    zero.value().projectionRef(),
                    accumulator.capture.projection()
                            .subject()
                            .projectionRef());
        } catch (RuntimeException failure) {
            accumulator.failure(
                    CoverageSide.DATA,
                    PhysicalRootBackfillStage.GENERATION_ZERO_SCAN,
                    zero.key(),
                    "GENERATION_ZERO_PROJECTION_MISMATCH");
            return processGenerationCandidates(
                    accumulator,
                    candidates,
                    index + 1,
                    deadline);
        }
        ObjectProtectionOwner owner =
                new ObjectProtectionOwner(
                        zero.key(),
                        zero.metadataVersion(),
                        zero.durableValueSha256());
        String referenceId =
                GenerationZeroProtectionIdentities
                        .visibleGenerationReferenceId(
                                accumulator.streamId(),
                                zero.key(),
                                zero.durableValueSha256());
        ObjectProtectionManager.OwnerRevalidator revalidator =
                expected -> revalidateIndex(zero, expected, deadline);
        return protectDataReference(
                        accumulator,
                        zero.key(),
                        target,
                        ObjectProtectionType.VISIBLE_GENERATION,
                        referenceId,
                        owner,
                        revalidator,
                        deadline)
                .thenCompose(ignored -> processGenerationCandidates(
                        accumulator,
                        candidates,
                        index + 1,
                        deadline));
    }

    private CompletableFuture<Void> scanCursorInventory(
            StreamAccumulator accumulator,
            Deadline deadline) {
        return captureCursorInventory(
                        accumulator,
                        accumulator.capture.projection()
                                .managedLedgerIdentity()
                                .orElseThrow(),
                        true,
                        deadline)
                .handle((capture, failure) -> {
                    if (failure != null) {
                        accumulator.failure(
                                CoverageSide.CURSOR,
                                stage(
                                        unwrap(failure),
                                        PhysicalRootBackfillStage
                                                .CURSOR_INVENTORY_SCAN),
                                cursorKeys.retentionKey(
                                        accumulator.streamId()),
                                errorCode(
                                        PhysicalRootBackfillStage
                                                .CURSOR_INVENTORY_SCAN,
                                        failure));
                    } else {
                        accumulator.cursorInventory = capture;
                        accumulator.cursorInventory(capture);
                    }
                    return null;
                });
    }

    private CompletableFuture<CursorInventoryCapture> captureCursorInventory(
            StreamAccumulator accumulator,
            com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity
                    expectedProjection,
            boolean protectObjects,
            Deadline deadline) {
        StreamId stream = accumulator.streamId();
        return deadline.call(() -> cursors.getRetention(
                        cluster, stream))
                .thenCompose(retention -> {
                    CursorInventoryBuilder builder =
                            new CursorInventoryBuilder(
                                    stream, retention);
                    if (retention.isPresent()) {
                        VersionedCursorRetention exact =
                                retention.orElseThrow();
                        if (!exact.value().projection()
                                        .equals(expectedProjection)
                                || exact.value().lifecycle()
                                        != CursorRetentionLifecycle.ACTIVE) {
                            return failedStep(
                                    PhysicalRootBackfillStage
                                            .CURSOR_INVENTORY_SCAN,
                                    invariant(
                                            "cursor retention is not ACTIVE for the live projection"));
                        }
                    }
                    return scanCursorPage(
                                    accumulator,
                                    expectedProjection,
                                    protectObjects,
                                    builder,
                                    Optional.empty(),
                                    null,
                                    deadline)
                            .thenApply(ignored -> {
                                if (builder.retention.isEmpty()
                                        && builder.cursorCount > 0) {
                                    throw invariant(
                                            "cursor roots exist without a retention authority");
                                }
                                return builder.finish();
                            });
                })
                .exceptionallyCompose(failure -> failedStep(
                        PhysicalRootBackfillStage
                                .CURSOR_INVENTORY_SCAN,
                        unwrap(failure)));
    }

    private CompletableFuture<Void> scanCursorPage(
            StreamAccumulator accumulator,
            com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity
                    expectedProjection,
            boolean protectObjects,
            CursorInventoryBuilder builder,
            Optional<CursorScanToken> continuation,
            String previousKey,
            Deadline deadline) {
        StreamId stream = accumulator.streamId();
        return deadline.call(() -> cursors.scanCursors(
                        cluster,
                        stream,
                        continuation,
                        cursorPageSize))
                .thenCompose(page -> {
                    if (previousKey != null
                            && !page.records().isEmpty()
                            && cursorKey(
                                                    stream,
                                                    page.records()
                                                            .get(0)
                                                            .value())
                                            .compareTo(previousKey)
                                    <= 0) {
                        return failedStep(
                                PhysicalRootBackfillStage
                                        .CURSOR_INVENTORY_SCAN,
                                invariant(
                                        "cursor inventory scan did not advance"));
                    }
                    return processCursorRoots(
                                    accumulator,
                                    expectedProjection,
                                    protectObjects,
                                    builder,
                                    page.records(),
                                    0,
                                    deadline)
                            .thenCompose(ignored -> {
                                if (page.continuation().isEmpty()) {
                                    return CompletableFuture
                                            .completedFuture(null);
                                }
                                String lastKey = cursorKey(
                                        stream,
                                        page.records()
                                                .get(
                                                        page.records()
                                                                        .size()
                                                                - 1)
                                                .value());
                                return scanCursorPage(
                                        accumulator,
                                        expectedProjection,
                                        protectObjects,
                                        builder,
                                        page.continuation(),
                                        lastKey,
                                        deadline);
                            });
                });
    }

    private CompletableFuture<Void> processCursorRoots(
            StreamAccumulator accumulator,
            com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity
                    expectedProjection,
            boolean protectObjects,
            CursorInventoryBuilder builder,
            List<VersionedCursorState> roots,
            int index,
            Deadline deadline) {
        if (index == roots.size()) {
            return CompletableFuture.completedFuture(null);
        }
        VersionedCursorState versioned = roots.get(index);
        CursorStateRecord cursor = versioned.value();
        if (!cursor.projection().equals(expectedProjection)) {
            return failedStep(
                    PhysicalRootBackfillStage.CURSOR_INVENTORY_SCAN,
                    invariant(
                            "cursor root belongs to another projection"));
        }
        builder.add(versioned);
        if (!protectObjects
                || cursor.lifecycle()
                        != CursorRecordLifecycle.ACTIVE
                || cursor.snapshotReference().isEmpty()) {
            return processCursorRoots(
                    accumulator,
                    expectedProjection,
                    protectObjects,
                    builder,
                    roots,
                    index + 1,
                    deadline);
        }
        CursorSnapshotReferenceRecord reference =
                cursor.snapshotReference().orElseThrow();
        final ObjectKey key;
        try {
            key = canonicalCursorObjectKey(
                    accumulator.streamId(),
                    cursor,
                    reference);
            if (!key.value().equals(reference.objectKey())) {
                throw invariant(
                        "cursor snapshot reference uses a non-canonical object key");
            }
        } catch (RuntimeException failure) {
            accumulator.failure(
                    CoverageSide.CURSOR,
                    PhysicalRootBackfillStage
                            .CURSOR_INVENTORY_SCAN,
                    cursorKey(accumulator.streamId(), cursor),
                    "CURSOR_SNAPSHOT_KEY_INVALID");
            return processCursorRoots(
                    accumulator,
                    expectedProjection,
                    protectObjects,
                    builder,
                    roots,
                    index + 1,
                    deadline);
        }
        return protectCursorReference(
                        accumulator,
                        versioned,
                        reference,
                        key,
                        deadline)
                .thenCompose(ignored -> processCursorRoots(
                        accumulator,
                        expectedProjection,
                        protectObjects,
                        builder,
                        roots,
                        index + 1,
                        deadline));
    }

    private CompletableFuture<Void> protectDataReference(
            StreamAccumulator accumulator,
            String resource,
            ObjectSliceReadTarget target,
            ObjectProtectionType type,
            String referenceId,
            ObjectProtectionOwner owner,
            ObjectProtectionManager.OwnerRevalidator revalidator,
            Deadline deadline) {
        return step(
                        PhysicalRootBackfillStage.OBJECT_HEAD,
                        deadline,
                        () -> dataIdentityResolver.resolve(
                                target, ReadView.COMMITTED))
                .thenCompose(object -> step(
                        PhysicalRootBackfillStage.OBJECT_HEAD,
                        deadline,
                        () -> objectStore.headObject(
                                object.objectKey(),
                                new HeadObjectOptions(
                                        deadline.callTimeout(
                                                objectStoreRequestTimeout))))
                        .thenApply(head -> {
                            requireHeadMatches(
                                    head, object, Optional.empty());
                            return object;
                        }))
                .thenCompose(object -> protect(
                        object,
                        type,
                        referenceId,
                        owner,
                        revalidator,
                        deadline))
                .handle((outcome, failure) -> {
                    if (failure != null) {
                        Throwable exact = unwrap(failure);
                        PhysicalRootBackfillStage stage =
                                stage(
                                        exact,
                                        PhysicalRootBackfillStage
                                                .PROTECTION_WRITE);
                        accumulator.failure(
                                CoverageSide.DATA,
                                stage,
                                resource,
                                errorCode(stage, exact));
                    } else {
                        accumulator.dataOutcome(
                                resource, outcome);
                    }
                    return null;
                });
    }

    private CompletableFuture<Void> protectCursorReference(
            StreamAccumulator accumulator,
            VersionedCursorState cursor,
            CursorSnapshotReferenceRecord reference,
            ObjectKey key,
            Deadline deadline) {
        String resource = cursorKey(
                accumulator.streamId(), cursor.value());
        Checksum checksum = new Checksum(
                ChecksumType.valueOf(
                        reference.storageChecksumType()),
                reference.storageChecksumValue());
        Map<String, String> metadata = Map.of(
                "nereus-format",
                CURSOR_FORMAT,
                "nereus-object-type",
                CURSOR_OBJECT_TYPE,
                "nereus-snapshot-id",
                reference.snapshotId());
        return step(
                        PhysicalRootBackfillStage.OBJECT_HEAD,
                        deadline,
                        () -> objectStore.headObject(
                                key,
                                new HeadObjectOptions(
                                        deadline.callTimeout(
                                                objectStoreRequestTimeout))))
                .thenApply(head -> {
                    PhysicalObjectIdentity object =
                            PhysicalObjectIdentity.create(
                                    key,
                                    Optional.empty(),
                                    PhysicalObjectKind.CURSOR_SNAPSHOT,
                                    reference.objectLength(),
                                    checksum,
                                    Optional.empty(),
                                    head.etag());
                    requireHeadMatches(
                            head, object, Optional.of(metadata));
                    return object;
                })
                .thenCompose(object -> {
                    ObjectProtectionOwner owner =
                            new ObjectProtectionOwner(
                                    resource,
                                    cursor.metadataVersion(),
                                    CursorMetadataDigests
                                            .durableValueSha256(
                                                    cursor.value()));
                    ObjectProtectionManager.OwnerRevalidator revalidator =
                            expected -> revalidateCursor(
                                    cursor,
                                    expected,
                                    deadline);
                    return protect(
                            object,
                            ObjectProtectionType
                                    .CURSOR_SNAPSHOT_ROOT,
                            reference.snapshotId(),
                            owner,
                            revalidator,
                            deadline);
                })
                .handle((outcome, failure) -> {
                    if (failure != null) {
                        Throwable exact = unwrap(failure);
                        PhysicalRootBackfillStage stage =
                                stage(
                                        exact,
                                        PhysicalRootBackfillStage
                                                .PROTECTION_WRITE);
                        accumulator.failure(
                                CoverageSide.CURSOR,
                                stage,
                                resource,
                                errorCode(stage, exact));
                    } else {
                        accumulator.cursorOutcome(
                                resource, outcome);
                    }
                    return null;
                });
    }

    private CompletableFuture<ProtectionOutcome> protect(
            PhysicalObjectIdentity object,
            ObjectProtectionType type,
            String referenceId,
            ObjectProtectionOwner owner,
            ObjectProtectionManager.OwnerRevalidator revalidator,
            Deadline deadline) {
        return step(
                        PhysicalRootBackfillStage.ROOT_WRITE,
                        deadline,
                        () -> physicalMetadata.getRoot(
                                cluster, object.objectKeyHash()))
                .thenCompose(initial -> {
                    if (initial.isPresent()) {
                        requireExactActiveRoot(
                                object, initial.orElseThrow());
                    }
                    PhysicalRootBackfillStage acquisitionStage =
                            initial.isEmpty()
                                    ? PhysicalRootBackfillStage.ROOT_WRITE
                                    : PhysicalRootBackfillStage
                                            .PROTECTION_WRITE;
                    ObjectProtectionRequest request =
                            new ObjectProtectionRequest(
                                    object,
                                    type,
                                    referenceId,
                                    owner,
                                    0);
                    return step(
                                    acquisitionStage,
                                    deadline,
                                    () -> protections.acquireOrTransfer(
                                            request,
                                            revalidator))
                            .thenCompose(protection -> step(
                                    PhysicalRootBackfillStage
                                            .PROTECTION_WRITE,
                                    deadline,
                                    () -> protections.revalidate(
                                            protection,
                                            revalidator)));
                })
                .thenCompose(protection -> step(
                        PhysicalRootBackfillStage.ROOT_WRITE,
                        deadline,
                        () -> physicalMetadata.getRoot(
                                cluster, object.objectKeyHash()))
                        .thenApply(optional -> {
                            VersionedPhysicalObjectRoot root =
                                    optional.orElseThrow(() ->
                                            invariant(
                                                    "physical root is absent after protection"));
                            requireExactActiveRoot(object, root);
                            if (root.value().lifecycleEpoch()
                                    != protection
                                            .rootLifecycleEpoch()) {
                                throw invariant(
                                        "protection/root lifecycle epoch changed");
                            }
                            return new ProtectionOutcome(
                                    object, root, protection);
                        }));
    }

    private CompletableFuture<Void> revalidateCommit(
            AppendRecoveryCommit expected,
            ObjectProtectionOwner actualOwner,
            Deadline deadline) {
        ObjectProtectionOwner canonical =
                new ObjectProtectionOwner(
                        expected.key(),
                        expected.sourceMetadataVersion(),
                        expected.sourceRecordSha256());
        if (!canonical.equals(actualOwner)) {
            return CompletableFuture.failedFuture(
                    invariant(
                            "reachable append protection owner changed"));
        }
        return deadline.call(() ->
                        sourceRetirement.getCommitNodeByKey(
                                cluster, expected.key()))
                .thenAccept(current -> {
                    VersionedGenerationZeroCommit exact =
                            current.orElseThrow(() ->
                                    invariant(
                                            "reachable append owner disappeared"));
                    if (!exact.key().equals(expected.key())
                            || exact.metadataVersion()
                                    != expected
                                            .sourceMetadataVersion()
                            || !exact.durableValueSha256()
                                    .equals(
                                            expected
                                                    .sourceRecordSha256())
                            || !exact.commitId()
                                    .equals(
                                            expected
                                                    .canonicalCommit()
                                                    .commitId())) {
                        throw invariant(
                                "reachable append owner changed");
                    }
                });
    }

    private CompletableFuture<Void> revalidateIndex(
            VersionedGenerationZeroIndex expected,
            ObjectProtectionOwner actualOwner,
            Deadline deadline) {
        ObjectProtectionOwner canonical =
                new ObjectProtectionOwner(
                        expected.key(),
                        expected.metadataVersion(),
                        expected.durableValueSha256());
        if (!canonical.equals(actualOwner)) {
            return CompletableFuture.failedFuture(
                    invariant(
                            "visible generation protection owner changed"));
        }
        return deadline.call(() -> generations.getCandidateByKey(
                        cluster,
                        expected.value().streamId(),
                        ReadView.COMMITTED,
                        expected.key()))
                .thenAccept(current -> {
                    if (current.isEmpty()
                            || !current.orElseThrow()
                                    .equals(expected)) {
                        throw invariant(
                                "visible generation owner changed");
                    }
                });
    }

    private CompletableFuture<Void> revalidateCursor(
            VersionedCursorState expected,
            ObjectProtectionOwner actualOwner,
            Deadline deadline) {
        StreamId stream =
                new StreamId(expected.value().projection().streamId());
        ObjectProtectionOwner canonical =
                new ObjectProtectionOwner(
                        cursorKey(stream, expected.value()),
                        expected.metadataVersion(),
                        CursorMetadataDigests.durableValueSha256(
                                expected.value()));
        if (!canonical.equals(actualOwner)) {
            return CompletableFuture.failedFuture(
                    invariant(
                            "cursor snapshot protection owner changed"));
        }
        return deadline.call(() -> cursors.getCursor(
                        cluster,
                        stream,
                        expected.value().cursorName()))
                .thenAccept(current -> {
                    if (current.isEmpty()
                            || !current.orElseThrow()
                                    .equals(expected)) {
                        throw invariant(
                                "cursor snapshot owner changed");
                    }
                });
    }

    private CompletableFuture<StreamBackfillResult> finalRevalidate(
            StreamAccumulator accumulator,
            Deadline deadline) {
        RegistrationCapture capture = accumulator.capture;
        StreamId stream = accumulator.streamId();
        return deadline.call(() -> generations.getStreamRegistration(
                        cluster, stream))
                .thenAccept(current -> {
                    if (current.isEmpty()
                            || !current.orElseThrow()
                                    .equals(capture.registration())) {
                        throw invariant(
                                "registration changed during backfill");
                    }
                })
                .thenCompose(ignored -> deadline.call(() ->
                        l0Metadata.getStreamSnapshot(cluster, stream)))
                .thenAccept(current -> {
                    if (!sameStreamAuthority(
                            current, capture.streamSnapshot())) {
                        throw invariant(
                                "stream head/profile changed during backfill");
                    }
                })
                .thenCompose(ignored -> deadline.call(() ->
                        projectionAuthorities.capture(
                                capture.projection().subject())))
                .thenAccept(current -> {
                    if (!current.equals(capture.projection())) {
                        throw invariant(
                                "projection authority changed during backfill");
                    }
                })
                .thenCompose(ignored -> {
                    if (!capture.live()) {
                        return CompletableFuture.completedFuture(null);
                    }
                    return revalidateLiveAuthorities(
                            accumulator, deadline);
                })
                .handle((ignored, failure) -> {
                    if (failure != null) {
                        accumulator.failure(
                                CoverageSide.BOTH,
                                PhysicalRootBackfillStage
                                        .FINAL_REVALIDATION,
                                accumulator.registration.key(),
                                finalRevalidationErrorCode(failure));
                        return accumulator.result(false);
                    }
                    accumulator.finalRevalidation();
                    return accumulator.result(
                            !accumulator.failed);
                });
    }

    private CompletableFuture<Void> revalidateLiveAuthorities(
            StreamAccumulator accumulator,
            Deadline deadline) {
        StreamId stream = accumulator.streamId();
        CompletableFuture<Void> recovery;
        if (accumulator.recoveryRoot == null
                || accumulator.recoveryAnchor == null
                || accumulator.recoveryHead == null) {
            recovery = CompletableFuture.completedFuture(null);
        } else {
            recovery = deadline.call(() -> generations.getRecoveryRoot(
                            cluster, stream))
                    .thenAccept(current -> {
                        if (!current.equals(
                                accumulator.recoveryRoot)) {
                            throw invariant(
                                    "recovery root changed during backfill");
                        }
                    })
                    .thenCompose(ignored -> deadline.call(() ->
                            l0Metadata.readAppendRecoveryTail(
                                    cluster,
                                    stream,
                                    accumulator.recoveryAnchor,
                                    Optional.empty(),
                                    1)))
                    .thenAccept(page -> {
                        if (!sameAppendRecoveryAuthority(
                                page.observedHead(),
                                accumulator.recoveryHead)) {
                            throw invariant(
                                    "append head changed during backfill");
                        }
                    });
        }
        return recovery.thenCompose(ignored -> {
            if (accumulator.cursorInventory == null) {
                return CompletableFuture.completedFuture(null);
            }
            return captureCursorInventory(
                            accumulator,
                            accumulator.capture.projection()
                                    .managedLedgerIdentity()
                                    .orElseThrow(),
                            false,
                            deadline)
                    .thenAccept(current -> {
                        if (!current.equals(
                                accumulator.cursorInventory)) {
                            throw invariant(
                                    "cursor inventory changed during backfill");
                        }
                    });
        });
    }

    private CompletableFuture<Void> publishProofs(
            ActivationBasis basis,
            PhysicalRootBackfillRequest request,
            RunAccumulator accumulator,
            Deadline deadline) {
        if (accumulator.failureCount != 0) {
            return CompletableFuture.completedFuture(null);
        }
        Checksum dataCoverage = accumulator.dataCoverage();
        Checksum cursorCoverage = accumulator.cursorCoverage();
        return deadline.call(() -> activations.get(cluster))
                .thenCompose(optional -> {
                    if (optional.isEmpty()) {
                        accumulator.globalFailure(
                                CoverageSide.BOTH,
                                PhysicalRootBackfillStage
                                        .FINAL_REVALIDATION,
                                f4Keys.generationProtocolActivationKey(),
                                "ACTIVATION_ABSENT");
                        return CompletableFuture.completedFuture(null);
                    }
                    VersionedGenerationProtocolActivation current =
                            optional.orElseThrow();
                    String precondition = activationPrecondition(
                            current.value(),
                            request.expectedBrokerReadinessEpoch());
                    if (precondition != null
                            || !current.value()
                                            .requiredReferenceDomains()
                                    .equals(basis.requiredDomains())
                            || !current.value()
                                            .streamRegistrationBackfill()
                                    .equals(
                                            basis
                                                    .registrationBackfill())) {
                        accumulator.globalFailure(
                                CoverageSide.BOTH,
                                PhysicalRootBackfillStage
                                        .FINAL_REVALIDATION,
                                current.key(),
                                precondition == null
                                        ? "ACTIVATION_AUTHORITY_CHANGED"
                                        : precondition);
                        return CompletableFuture.completedFuture(null);
                    }
                    final GenerationBackfillProofRecord dataProof;
                    final GenerationBackfillProofRecord cursorProof;
                    try {
                        long completedAt = positiveNow();
                        dataProof = proof(
                                current.value()
                                        .physicalRootBackfill(),
                                request,
                                dataCoverage,
                                completedAt);
                        cursorProof = proof(
                                current.value()
                                        .cursorSnapshotBackfill(),
                                request,
                                cursorCoverage,
                                completedAt);
                    } catch (ProofConflictException conflict) {
                        accumulator.globalFailure(
                                CoverageSide.BOTH,
                                PhysicalRootBackfillStage
                                        .FINAL_REVALIDATION,
                                current.key(),
                                "ACTIVATION_PROOF_CONFLICT");
                        return CompletableFuture.completedFuture(null);
                    }
                    GenerationProtocolActivationRecord replacement =
                            withBackfillProofs(
                                    current.value(),
                                    dataProof,
                                    cursorProof,
                                    Math.max(
                                            current.value()
                                                    .updatedAtMillis(),
                                            Math.max(
                                                    dataProof
                                                            .completedAtMillis(),
                                                    cursorProof
                                                            .completedAtMillis())));
                    if (current.value()
                                    .physicalRootBackfill()
                                    .equals(dataProof)
                            && current.value()
                                    .cursorSnapshotBackfill()
                                    .equals(cursorProof)) {
                        return CompletableFuture
                                .<Void>completedFuture(null);
                    }
                    return deadline.call(() -> activations.compareAndSet(
                                    cluster,
                                    replacement,
                                    current.metadataVersion()))
                            .handle((updated, failure) -> {
                                if (failure == null) {
                                    return CompletableFuture
                                            .<Void>completedFuture(null);
                                }
                                return deadline.call(() ->
                                                activations.get(cluster))
                                        .thenAccept(reloaded -> {
                                            if (reloaded.isEmpty()
                                                    || !activationHasProofs(
                                                            reloaded
                                                                    .orElseThrow()
                                                                    .value(),
                                                            request,
                                                            basis,
                                                            dataProof,
                                                            cursorProof)) {
                                                throw new CompletionException(
                                                        unwrap(failure));
                                            }
                                        });
                            })
                            .thenCompose(value -> value)
                            .exceptionally(failure -> {
                                accumulator.globalFailure(
                                        CoverageSide.BOTH,
                                        PhysicalRootBackfillStage
                                                .FINAL_REVALIDATION,
                                        current.key(),
                                        "ACTIVATION_CAS_FAILED");
                                return null;
                            });
                })
                .thenApply(ignored -> (Void) null)
                .exceptionally(failure -> {
                    accumulator.globalFailure(
                            CoverageSide.BOTH,
                            PhysicalRootBackfillStage
                                    .FINAL_REVALIDATION,
                            f4Keys.generationProtocolActivationKey(),
                            errorCode(
                                    PhysicalRootBackfillStage
                                            .FINAL_REVALIDATION,
                                    failure));
                    return null;
                });
    }

    private GenerationBackfillProofRecord proof(
            GenerationBackfillProofRecord current,
            PhysicalRootBackfillRequest request,
            Checksum coverage,
            long completedAt) {
        if (current.complete()) {
            if (current.brokerReadinessEpoch()
                            != request
                                    .expectedBrokerReadinessEpoch()
                    || !current.coverageSha256()
                            .equals(coverage.value())) {
                throw new ProofConflictException();
            }
            return current;
        }
        return new GenerationBackfillProofRecord(
                request.runId(),
                request.expectedBrokerReadinessEpoch(),
                coverage.value(),
                true,
                completedAt);
    }

    private static GenerationProtocolActivationRecord withBackfillProofs(
            GenerationProtocolActivationRecord current,
            GenerationBackfillProofRecord dataProof,
            GenerationBackfillProofRecord cursorProof,
            long updatedAtMillis) {
        return new GenerationProtocolActivationRecord(
                current.schemaVersion(),
                current.protocolVersion(),
                current.lifecycle(),
                current.publicationEnabled(),
                current.physicalDeleteEnabled(),
                current.cursorSnapshotDeleteEnabled(),
                current.brokerCapabilityReadinessEpoch(),
                current.requiredReferenceDomains(),
                current.streamRegistrationBackfill(),
                dataProof,
                cursorProof,
                current.objectStoreCapabilitySha256(),
                current.activatingBrokerRunId(),
                current.preparedAtMillis(),
                current.activatedAtMillis(),
                updatedAtMillis,
                0);
    }

    private static boolean activationHasProofs(
            GenerationProtocolActivationRecord activation,
            PhysicalRootBackfillRequest request,
            ActivationBasis basis,
            GenerationBackfillProofRecord dataProof,
            GenerationBackfillProofRecord cursorProof) {
        return activation.brokerCapabilityReadinessEpoch()
                        == request.expectedBrokerReadinessEpoch()
                && activation.requiredReferenceDomains()
                        .equals(basis.requiredDomains())
                && activation.streamRegistrationBackfill()
                        .equals(basis.registrationBackfill())
                && activation.physicalRootBackfill()
                        .equals(dataProof)
                && activation.cursorSnapshotBackfill()
                        .equals(cursorProof);
    }

    private long positiveNow() {
        long value = clock.millis();
        if (value <= 0) {
            throw new IllegalStateException(
                    "backfill completion clock must be positive");
        }
        return value;
    }

    private static String activationPrecondition(
            GenerationProtocolActivationRecord activation,
            long expectedEpoch) {
        if (activation.brokerCapabilityReadinessEpoch()
                != expectedEpoch) {
            return "READINESS_EPOCH_MISMATCH";
        }
        if (!activation.streamRegistrationBackfill().complete()
                || activation.streamRegistrationBackfill()
                                .brokerReadinessEpoch()
                        != expectedEpoch) {
            return "REGISTRATION_BACKFILL_INCOMPLETE";
        }
        return null;
    }

    private static String rolloverPrecondition(
            VersionedGenerationProtocolActivation current,
            VersionedGenerationProtocolActivation expected,
            long nextEpoch) {
        if (!current.equals(expected)) {
            return "ACTIVATION_AUTHORITY_CHANGED";
        }
        GenerationProtocolActivationRecord value = current.value();
        if (!value.physicalDeleteEnabled()
                || !value.cursorSnapshotDeleteEnabled()) {
            return "DELETION_AUTHORITY_INACTIVE";
        }
        if (nextEpoch == value.brokerCapabilityReadinessEpoch()) {
            return "READINESS_EPOCH_UNCHANGED";
        }
        return null;
    }

    private static void requireHeadMatches(
            HeadObjectResult head,
            PhysicalObjectIdentity object,
            Optional<Map<String, String>> expectedMetadata) {
        if (!head.key().equals(object.objectKey())
                || head.objectLength() != object.objectLength()
                || !head.checksum()
                        .equals(object.storageChecksum())
                || (object.etag().isPresent()
                        && !head.etag().equals(object.etag()))
                || (expectedMetadata.isPresent()
                        && !head.metadata()
                                .equals(
                                        expectedMetadata
                                                .orElseThrow()))) {
            throw new NereusException(
                    ErrorCode.OBJECT_CHECKSUM_MISMATCH,
                    false,
                    "physical-root backfill HEAD identity mismatch");
        }
    }

    private static void requireExactActiveRoot(
            PhysicalObjectIdentity object,
            VersionedPhysicalObjectRoot root) {
        if (root.value().lifecycle()
                        != PhysicalObjectLifecycle.ACTIVE
                || !PhysicalObjectIdentity.from(root.value())
                        .equals(object)) {
            throw invariant(
                    "physical root is not the exact ACTIVE identity");
        }
    }

    private ObjectKey canonicalCursorObjectKey(
            StreamId stream,
            CursorStateRecord cursor,
            CursorSnapshotReferenceRecord reference) {
        String value = KeyComponentCodec.encodeComponent(cluster)
                + "/cursor-snapshots/v1/"
                + KeyComponentCodec.encodeComponent(stream.value())
                + "/"
                + cursor.cursorNameHash()
                + "/"
                + KeyComponentCodec.encodeNonNegativeLong(
                        cursor.cursorGeneration())
                + "/"
                + reference.snapshotId()
                + ".ncs";
        return new ObjectKey(value);
    }

    private String cursorKey(
            StreamId stream, CursorStateRecord cursor) {
        return cursorKeys.cursorStateKey(
                stream, cursor.cursorName());
    }

    private static AppendRecoveryAnchor anchor(
            StreamId stream,
            VersionedRecoveryCheckpointRoot root) {
        if (root.value().checkpoints().isEmpty()) {
            return AppendRecoveryAnchor.genesis(stream);
        }
        return new AppendRecoveryAnchor(
                stream,
                root.value().lastCommitId(),
                root.value().coveredEndOffset(),
                root.value().cumulativeSizeAtEnd(),
                root.value().lastCommitVersion());
    }

    private static void requireRegistryProgress(
            StreamRegistrationScanPage page, String previousKey) {
        if (previousKey != null
                && !page.values().isEmpty()
                && page.values()
                                .get(0)
                                .key()
                                .compareTo(previousKey)
                        <= 0) {
            throw invariant(
                    "registered-stream scan did not advance");
        }
    }

    private static void requireCompatibleProjection(
            Optional<ProjectionRef> stored,
            ProjectionRef expected) {
        Objects.requireNonNull(stored, "stored");
        Objects.requireNonNull(expected, "expected");
        if (stored.isPresent()
                && !stored.orElseThrow().equals(expected)) {
            throw invariant(
                    "generation-zero projection reference conflicts with the live registration");
        }
    }

    private static <T> CompletableFuture<T> step(
            PhysicalRootBackfillStage stage,
            Deadline deadline,
            Supplier<CompletableFuture<T>> operation) {
        return deadline.call(operation)
                .exceptionallyCompose(failure -> failedStep(
                        stage, unwrap(failure)));
    }

    private static <T> CompletableFuture<T> failedStep(
            PhysicalRootBackfillStage stage,
            Throwable failure) {
        return CompletableFuture.failedFuture(
                new BackfillStepException(stage, failure));
    }

    private static PhysicalRootBackfillStage stage(
            Throwable failure,
            PhysicalRootBackfillStage fallback) {
        Throwable exact = unwrap(failure);
        return exact instanceof BackfillStepException step
                ? step.stage
                : fallback;
    }

    private static String errorCode(
            PhysicalRootBackfillStage stage,
            Throwable failure) {
        Throwable exact = unwrap(failure);
        while (exact instanceof BackfillStepException step) {
            exact = unwrap(step.getCause());
        }
        if (exact instanceof TimeoutException) {
            return "BACKFILL_TIMEOUT";
        }
        return switch (stage) {
            case REGISTRY_SCAN -> "REGISTRY_SCAN_FAILED";
            case PROJECTION_READ -> "PROJECTION_READ_FAILED";
            case HEAD_COMMIT_SCAN -> "HEAD_COMMIT_SCAN_FAILED";
            case GENERATION_ZERO_SCAN ->
                    "GENERATION_ZERO_SCAN_FAILED";
            case CURSOR_INVENTORY_SCAN ->
                    cursorInventoryErrorCode(exact);
            case OBJECT_HEAD -> "OBJECT_HEAD_FAILED";
            case ROOT_WRITE -> "ROOT_WRITE_FAILED";
            case PROTECTION_WRITE -> "PROTECTION_WRITE_FAILED";
            case FINAL_REVALIDATION ->
                    "FINAL_REVALIDATION_FAILED";
        };
    }

    private static String cursorInventoryErrorCode(Throwable failure) {
        String message = failure.getMessage();
        if ("cursor retention is not ACTIVE for the live projection".equals(message)) {
            return "CURSOR_RETENTION_NOT_ACTIVE";
        }
        if ("cursor roots exist without a retention authority".equals(message)) {
            return "CURSOR_ROOTS_WITHOUT_RETENTION";
        }
        if ("cursor root belongs to another projection".equals(message)) {
            return "CURSOR_PROJECTION_MISMATCH";
        }
        if ("cursor inventory scan did not advance".equals(message)) {
            return "CURSOR_SCAN_DID_NOT_ADVANCE";
        }
        if (failure instanceof NereusException nereus) {
            return "CURSOR_INVENTORY_" + nereus.code().name();
        }
        return "CURSOR_INVENTORY_SCAN_FAILED";
    }

    private static String finalRevalidationErrorCode(Throwable failure) {
        Throwable exact = unwrap(failure);
        while (exact instanceof BackfillStepException step) {
            exact = unwrap(step.getCause());
        }
        String message = exact.getMessage();
        if ("registration changed during backfill".equals(message)) {
            return "REGISTRATION_AUTHORITY_CHANGED";
        }
        if ("stream head/profile changed during backfill".equals(message)) {
            return "STREAM_AUTHORITY_CHANGED";
        }
        if ("projection authority changed during backfill".equals(message)) {
            return "PROJECTION_AUTHORITY_CHANGED";
        }
        if ("recovery root changed during backfill".equals(message)) {
            return "RECOVERY_ROOT_AUTHORITY_CHANGED";
        }
        if ("append head changed during backfill".equals(message)) {
            return "APPEND_HEAD_AUTHORITY_CHANGED";
        }
        if ("cursor inventory changed during backfill".equals(message)) {
            return "CURSOR_INVENTORY_AUTHORITY_CHANGED";
        }
        if (exact instanceof NereusException nereus) {
            return "FINAL_REVALIDATION_" + nereus.code().name();
        }
        return "FINAL_REVALIDATION_FAILED";
    }

    static boolean sameStreamAuthority(
            StreamMetadataSnapshot first,
            StreamMetadataSnapshot second) {
        Objects.requireNonNull(first, "first");
        Objects.requireNonNull(second, "second");
        return first.metadata().streamId().equals(
                        second.metadata().streamId())
                && first.metadata().streamName().equals(
                        second.metadata().streamName())
                && first.metadata().streamNameHash().equals(
                        second.metadata().streamNameHash())
                && first.metadata().state().equals(
                        second.metadata().state())
                && first.metadata().profile().equals(
                        second.metadata().profile())
                && first.metadata().attributes().equals(
                        second.metadata().attributes())
                && first.metadata().createdAtMillis()
                        == second.metadata().createdAtMillis()
                && first.metadata().policyVersion()
                        == second.metadata().policyVersion()
                && first.committedEnd().committedEndOffset()
                        == second.committedEnd().committedEndOffset()
                && first.committedEnd().cumulativeSize()
                        == second.committedEnd().cumulativeSize()
                && first.committedEnd().commitVersion()
                        == second.committedEnd().commitVersion()
                && first.trim().trimOffset()
                        == second.trim().trimOffset();
    }

    static boolean sameAppendRecoveryAuthority(
            AppendRecoveryHead first,
            AppendRecoveryHead second) {
        Objects.requireNonNull(first, "first");
        Objects.requireNonNull(second, "second");
        return first.streamId().equals(second.streamId())
                && first.lastCommitId().equals(
                        second.lastCommitId())
                && first.offsetEnd() == second.offsetEnd()
                && first.cumulativeSize()
                        == second.cumulativeSize()
                && first.commitVersion()
                        == second.commitVersion();
    }

    private static Throwable unwrap(Throwable supplied) {
        Throwable current = supplied;
        while ((current instanceof CompletionException
                        || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static NereusException invariant(String message) {
        return invariant(message, null);
    }

    private static NereusException invariant(
            String message, Throwable cause) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION,
                false,
                message,
                cause);
    }

    private static String requireText(
            String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(
                    field + " cannot be blank");
        }
        return value;
    }

    private static Duration requirePositive(
            Duration value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(
                    field + " must be positive");
        }
        return value;
    }

    private enum CoverageSide {
        DATA,
        CURSOR,
        BOTH
    }

    private record RegistrationCapture(
            VersionedMaterializationStreamRegistration registration,
            StreamMetadataSnapshot streamSnapshot,
            GenerationProjectionAuthoritySnapshot projection,
            boolean live) {
    }

    private record ProtectionOutcome(
            PhysicalObjectIdentity object,
            VersionedPhysicalObjectRoot root,
            ObjectProtection protection) {
    }

    private record CursorInventoryCapture(
            Optional<VersionedCursorRetention> retention,
            long cursorCount,
            Checksum authoritySha256) {
    }

    private record ActivationBasis(
            List<ReferenceDomainVersionRecord> requiredDomains,
            GenerationBackfillProofRecord registrationBackfill) {
        private ActivationBasis {
            requiredDomains = List.copyOf(requiredDomains);
        }
    }

    private record StreamBackfillResult(
            long streamsScanned,
            long dataObjectsScanned,
            long cursorObjectsScanned,
            long rootsCreatedOrVerified,
            long protectionsCreatedOrVerified,
            long failureCount,
            List<PhysicalRootBackfillFailure> boundedFailures,
            Checksum dataCoverage,
            Checksum cursorCoverage) {
    }

    private static final class StreamAccumulator {
        private final VersionedMaterializationStreamRegistration
                registration;
        private final PhysicalRootBackfillDigest data =
                new PhysicalRootBackfillDigest(
                        "physical-root-backfill-stream-data-v1");
        private final PhysicalRootBackfillDigest cursor =
                new PhysicalRootBackfillDigest(
                        "physical-root-backfill-stream-cursor-v1");
        private final ArrayList<PhysicalRootBackfillFailure>
                failures = new ArrayList<>();
        private RegistrationCapture capture;
        private Optional<VersionedRecoveryCheckpointRoot>
                recoveryRoot;
        private AppendRecoveryAnchor recoveryAnchor;
        private AppendRecoveryHead recoveryHead;
        private CursorInventoryCapture cursorInventory;
        private long dataObjectsScanned;
        private long cursorObjectsScanned;
        private long rootsCreatedOrVerified;
        private long protectionsCreatedOrVerified;
        private long failureCount;
        private boolean live;
        private boolean failed;

        private StreamAccumulator(
                VersionedMaterializationStreamRegistration
                        registration) {
            this.registration =
                    Objects.requireNonNull(
                            registration, "registration");
            data.text(registration.value().streamId());
            cursor.text(registration.value().streamId());
        }

        private StreamId streamId() {
            return new StreamId(
                    registration.value().streamId());
        }

        private void registration() {
            GcAuthorityToken authority =
                    new GcAuthorityToken(
                            registration.key(),
                            registration.metadataVersion(),
                            registration.durableValueSha256());
            data.authority(authority);
            cursor.authority(authority);
        }

        private void streamSnapshot(
                StreamMetadataSnapshot snapshot) {
            writeStreamAuthority(data, snapshot);
            writeStreamAuthority(cursor, snapshot);
        }

        private void projection(
                GenerationProjectionAuthoritySnapshot projection) {
            data.text("projection");
            data.bool(projection.live());
            cursor.text("projection");
            cursor.bool(projection.live());
            for (GcAuthorityToken authority :
                    projection.authorities()) {
                data.authority(authority);
                cursor.authority(authority);
            }
        }

        private void recoveryRoot(
                String rootKey,
                Optional<VersionedRecoveryCheckpointRoot> root) {
            if (root.isEmpty()) {
                data.authority(new GcAuthorityToken(
                        rootKey,
                        0,
                        absence(
                                "recovery-root",
                                rootKey)));
                return;
            }
            VersionedRecoveryCheckpointRoot exact =
                    root.orElseThrow();
            data.authority(new GcAuthorityToken(
                    exact.key(),
                    exact.metadataVersion(),
                    exact.durableValueSha256()));
        }

        private void recoveryHead(
                AppendRecoveryHead head) {
            data.text("append-head");
            data.text(head.streamId().value());
            data.text(head.lastCommitId());
            data.int64(head.offsetEnd());
            data.int64(head.cumulativeSize());
            data.int64(head.commitVersion());
        }

        private void commitAuthority(
                AppendRecoveryCommit commit) {
            data.authority(new GcAuthorityToken(
                    commit.key(),
                    commit.sourceMetadataVersion(),
                    commit.sourceRecordSha256()));
        }

        private void generationZeroAuthority(
                VersionedGenerationZeroIndex zero) {
            data.authority(new GcAuthorityToken(
                    zero.key(),
                    zero.metadataVersion(),
                    zero.durableValueSha256()));
            data.text(zero.encoding().name());
        }

        private void generationZeroTombstone(
                VersionedGenerationZeroIndex zero) {
            data.text("generation-zero-tombstone");
            data.text(zero.key());
        }

        private void dataOutcome(
                String resource,
                ProtectionOutcome outcome) {
            dataObjectsScanned = Math.addExact(
                    dataObjectsScanned, 1);
            rootsCreatedOrVerified = Math.addExact(
                    rootsCreatedOrVerified, 1);
            protectionsCreatedOrVerified = Math.addExact(
                    protectionsCreatedOrVerified, 1);
            data.text("data-protected");
            data.text(resource);
            data.object(outcome.object());
            data.root(outcome.root());
            data.protection(outcome.protection());
        }

        private void cursorOutcome(
                String resource,
                ProtectionOutcome outcome) {
            cursorObjectsScanned = Math.addExact(
                    cursorObjectsScanned, 1);
            rootsCreatedOrVerified = Math.addExact(
                    rootsCreatedOrVerified, 1);
            protectionsCreatedOrVerified = Math.addExact(
                    protectionsCreatedOrVerified, 1);
            cursor.text("cursor-protected");
            cursor.text(resource);
            cursor.object(outcome.object());
            cursor.root(outcome.root());
            cursor.protection(outcome.protection());
        }

        private void finalRevalidation() {
            data.text("final-revalidation");
            data.bool(true);
            cursor.text("final-revalidation");
            cursor.bool(true);
        }

        private void cursorInventory(
                CursorInventoryCapture inventory) {
            cursor.text("cursor-inventory");
            cursor.int64(inventory.cursorCount());
            cursor.checksum(inventory.authoritySha256());
        }

        private void failure(
                CoverageSide side,
                PhysicalRootBackfillStage stage,
                String resource,
                String errorCode) {
            failed = true;
            failureCount = Math.addExact(
                    failureCount, 1);
            PhysicalRootBackfillFailure failure =
                    new PhysicalRootBackfillFailure(
                            PhysicalRootBackfillDigest
                                    .resourceIdentity(
                                            stage.name(),
                                            resource),
                            stage,
                            errorCode);
            if (failures.size()
                    < PhysicalRootBackfillReport
                            .MAX_FAILURE_DETAILS) {
                failures.add(failure);
            }
            if (side == CoverageSide.DATA
                    || side == CoverageSide.BOTH) {
                writeFailure(data, failure);
            }
            if (side == CoverageSide.CURSOR
                    || side == CoverageSide.BOTH) {
                writeFailure(cursor, failure);
            }
        }

        private StreamBackfillResult result(
                boolean covered) {
            data.text("classification");
            data.bool(live);
            data.bool(covered);
            cursor.text("classification");
            cursor.bool(live);
            cursor.bool(covered);
            return new StreamBackfillResult(
                    covered ? 1 : 0,
                    dataObjectsScanned,
                    cursorObjectsScanned,
                    rootsCreatedOrVerified,
                    protectionsCreatedOrVerified,
                    failureCount,
                    List.copyOf(failures),
                    data.finish(),
                    cursor.finish());
        }

        private static void writeStreamAuthority(
                PhysicalRootBackfillDigest writer,
                StreamMetadataSnapshot snapshot) {
            writer.text("stream-authority-v1");
            writer.text(snapshot.metadata().streamId());
            writer.text(snapshot.metadata().streamName());
            writer.text(snapshot.metadata().streamNameHash());
            writer.text(snapshot.metadata().state());
            writer.text(snapshot.metadata().profile());
            writer.int64(snapshot.metadata().createdAtMillis());
            writer.int64(snapshot.metadata().policyVersion());
            writer.int64(
                    snapshot.committedEnd()
                            .committedEndOffset());
            writer.int64(
                    snapshot.committedEnd()
                            .cumulativeSize());
            writer.int64(
                    snapshot.committedEnd()
                            .commitVersion());
            writer.int64(snapshot.trim().trimOffset());
            writer.int32(
                    snapshot.metadata()
                            .attributes()
                            .size());
            snapshot.metadata()
                    .attributes()
                    .forEach((key, value) -> {
                        writer.text(key);
                        writer.text(value);
                    });
        }
    }

    private final class CursorInventoryBuilder {
        private final StreamId stream;
        private final Optional<VersionedCursorRetention>
                retention;
        private final PhysicalRootBackfillDigest digest =
                new PhysicalRootBackfillDigest(
                        "physical-root-backfill-cursor-inventory-v1");
        private long cursorCount;

        private CursorInventoryBuilder(
                StreamId stream,
                Optional<VersionedCursorRetention>
                        retention) {
            this.stream = stream;
            this.retention = retention;
            String key = cursorKeys.retentionKey(stream);
            if (retention.isEmpty()) {
                digest.authority(new GcAuthorityToken(
                        key,
                        0,
                        absence(
                                "cursor-retention",
                                key)));
            } else {
                VersionedCursorRetention exact =
                        retention.orElseThrow();
                digest.authority(new GcAuthorityToken(
                        key,
                        exact.metadataVersion(),
                        CursorMetadataDigests
                                .durableValueSha256(
                                        exact.value())));
            }
        }

        private void add(VersionedCursorState cursor) {
            cursorCount = Math.addExact(
                    cursorCount, 1);
            digest.authority(new GcAuthorityToken(
                    cursorKey(stream, cursor.value()),
                    cursor.metadataVersion(),
                    CursorMetadataDigests
                            .durableValueSha256(
                                    cursor.value())));
        }

        private CursorInventoryCapture finish() {
            digest.int64(cursorCount);
            return new CursorInventoryCapture(
                    retention,
                    cursorCount,
                    digest.finish());
        }
    }

    private static final class RunAccumulator {
        private final PhysicalRootBackfillRequest request;
        private final ArrayList<Checksum> dataPieces =
                new ArrayList<>();
        private final ArrayList<Checksum> cursorPieces =
                new ArrayList<>();
        private final ArrayList<PhysicalRootBackfillFailure>
                failures = new ArrayList<>();
        private final HashSet<String> streamIds =
                new HashSet<>();
        private long streamsScanned;
        private long dataObjectsScanned;
        private long cursorObjectsScanned;
        private long rootsCreatedOrVerified;
        private long protectionsCreatedOrVerified;
        private long failureCount;

        private RunAccumulator(
                PhysicalRootBackfillRequest request) {
            this.request = request;
        }

        private boolean admitStream(StreamId stream) {
            return streamIds.add(stream.value());
        }

        private void shardStarted(int shard) {
            addShardPiece("start", shard);
        }

        private void shardCompleted(int shard) {
            addShardPiece("complete", shard);
        }

        private void addShardPiece(
                String state, int shard) {
            PhysicalRootBackfillDigest piece =
                    new PhysicalRootBackfillDigest(
                            "physical-root-backfill-registry-shard-v1");
            piece.text(state);
            piece.int32(shard);
            Checksum checksum = piece.finish();
            dataPieces.add(checksum);
            cursorPieces.add(checksum);
        }

        private void fold(
                StreamBackfillResult result) {
            streamsScanned = Math.addExact(
                    streamsScanned,
                    result.streamsScanned());
            dataObjectsScanned = Math.addExact(
                    dataObjectsScanned,
                    result.dataObjectsScanned());
            cursorObjectsScanned = Math.addExact(
                    cursorObjectsScanned,
                    result.cursorObjectsScanned());
            rootsCreatedOrVerified = Math.addExact(
                    rootsCreatedOrVerified,
                    result.rootsCreatedOrVerified());
            protectionsCreatedOrVerified = Math.addExact(
                    protectionsCreatedOrVerified,
                    result.protectionsCreatedOrVerified());
            long oldFailureCount = failureCount;
            failureCount = Math.addExact(
                    failureCount, result.failureCount());
            int remaining = (int) Math.max(
                    0,
                    PhysicalRootBackfillReport
                                    .MAX_FAILURE_DETAILS
                            - failures.size());
            if (remaining > 0) {
                failures.addAll(
                        result.boundedFailures()
                                .subList(
                                        0,
                                        Math.min(
                                                remaining,
                                                result
                                                        .boundedFailures()
                                                        .size())));
            }
            if (failureCount < oldFailureCount) {
                throw new ArithmeticException(
                        "backfill failure counter overflow");
            }
            dataPieces.add(result.dataCoverage());
            cursorPieces.add(result.cursorCoverage());
        }

        private void globalFailure(
                CoverageSide side,
                PhysicalRootBackfillStage stage,
                String resource,
                String errorCode) {
            failureCount = Math.addExact(
                    failureCount, 1);
            PhysicalRootBackfillFailure failure =
                    new PhysicalRootBackfillFailure(
                            PhysicalRootBackfillDigest
                                    .resourceIdentity(
                                            stage.name(),
                                            resource),
                            stage,
                            errorCode);
            if (failures.size()
                    < PhysicalRootBackfillReport
                            .MAX_FAILURE_DETAILS) {
                failures.add(failure);
            }
            PhysicalRootBackfillDigest piece =
                    new PhysicalRootBackfillDigest(
                            "physical-root-backfill-global-failure-v1");
            writeFailure(piece, failure);
            Checksum checksum = piece.finish();
            if (side == CoverageSide.DATA
                    || side == CoverageSide.BOTH) {
                dataPieces.add(checksum);
            }
            if (side == CoverageSide.CURSOR
                    || side == CoverageSide.BOTH) {
                cursorPieces.add(checksum);
            }
        }

        private Checksum dataCoverage() {
            return coverage(
                    "physical-root-backfill-data-coverage-v1",
                    dataPieces);
        }

        private Checksum cursorCoverage() {
            return coverage(
                    "physical-root-backfill-cursor-coverage-v1",
                    cursorPieces);
        }

        private PhysicalRootBackfillReport report() {
            return new PhysicalRootBackfillReport(
                    request.runId(),
                    request.expectedBrokerReadinessEpoch(),
                    streamsScanned,
                    dataObjectsScanned,
                    cursorObjectsScanned,
                    rootsCreatedOrVerified,
                    protectionsCreatedOrVerified,
                    failureCount,
                    dataCoverage(),
                    cursorCoverage(),
                    List.copyOf(failures));
        }

        private static Checksum coverage(
                String domain, List<Checksum> pieces) {
            PhysicalRootBackfillDigest writer =
                    new PhysicalRootBackfillDigest(domain);
            writer.int64(pieces.size());
            for (Checksum piece : pieces) {
                writer.checksum(piece);
            }
            return writer.finish();
        }
    }

    private static void writeFailure(
            PhysicalRootBackfillDigest writer,
            PhysicalRootBackfillFailure failure) {
        writer.text("failure");
        writer.text(
                failure.resourceIdentitySha256());
        writer.text(failure.stage().name());
        writer.text(failure.errorCode());
    }

    private static Checksum absence(
            String domain, String key) {
        PhysicalRootBackfillDigest writer =
                new PhysicalRootBackfillDigest(
                        "physical-root-backfill-absence-v1");
        writer.text(domain);
        writer.text(key);
        return writer.finish();
    }

    private static final class BackfillStepException
            extends RuntimeException {
        private final PhysicalRootBackfillStage stage;

        private BackfillStepException(
                PhysicalRootBackfillStage stage,
                Throwable cause) {
            super(cause);
            this.stage = stage;
        }
    }

    private static final class ProofConflictException
            extends RuntimeException {
    }

    private static final class Deadline {
        private final long deadlineNanos;
        private final LongSupplier nanoTime;

        private Deadline(
                long deadlineNanos,
                LongSupplier nanoTime) {
            this.deadlineNanos = deadlineNanos;
            this.nanoTime = nanoTime;
        }

        private static Deadline start(
                Duration timeout,
                LongSupplier nanoTime) {
            long now = nanoTime.getAsLong();
            long duration;
            try {
                duration = timeout.toNanos();
            } catch (ArithmeticException ignored) {
                duration = Long.MAX_VALUE;
            }
            long deadline;
            try {
                deadline = Math.addExact(now, duration);
            } catch (ArithmeticException ignored) {
                deadline = Long.MAX_VALUE;
            }
            return new Deadline(deadline, nanoTime);
        }

        private <T> CompletableFuture<T> call(
                Supplier<CompletableFuture<T>> operation) {
            long remaining = deadlineNanos
                    - nanoTime.getAsLong();
            if (remaining <= 0) {
                return CompletableFuture.failedFuture(
                        new TimeoutException(
                                "physical-root backfill deadline expired"));
            }
            final CompletableFuture<T> future;
            try {
                future = Objects.requireNonNull(
                        operation.get(),
                        "backfill operation returned null");
            } catch (Throwable failure) {
                return CompletableFuture.failedFuture(failure);
            }
            return future.orTimeout(
                    remaining, TimeUnit.NANOSECONDS);
        }

        private Duration callTimeout(Duration maximum) {
            long remaining = deadlineNanos
                    - nanoTime.getAsLong();
            if (remaining <= 0) {
                throw new CompletionException(
                        new TimeoutException(
                                "physical-root backfill deadline expired"));
            }
            long maximumNanos;
            try {
                maximumNanos = maximum.toNanos();
            } catch (ArithmeticException ignored) {
                maximumNanos = Long.MAX_VALUE;
            }
            return Duration.ofNanos(
                    Math.min(remaining, maximumNanos));
        }
    }
}
