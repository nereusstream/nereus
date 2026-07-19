/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.recovery;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.capability.GenerationActivationProof;
import com.nereusstream.core.capability.GenerationOperation;
import com.nereusstream.core.capability.GenerationProtocolActivationGuard;
import com.nereusstream.core.capability.LiveProjectionSubject;
import com.nereusstream.core.physical.ObjectProtection;
import com.nereusstream.core.physical.ObjectReadPinManager;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.materialization.MaterializationConfig;
import com.nereusstream.materialization.MaterializationDeadline;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.ProjectionIdentity;
import com.nereusstream.metadata.oxia.RecoveryCheckpointRootDigests;
import com.nereusstream.metadata.oxia.VersionedMaterializationStreamRegistration;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.VersionedRecoveryCheckpointRoot;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointReferenceRecord;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointRootRecord;
import com.nereusstream.objectstore.HeadObjectOptions;
import com.nereusstream.objectstore.HeadObjectResult;
import com.nereusstream.objectstore.ObjectAlreadyExistsException;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointCodecV1;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointFormatV1;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointMergeResult;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointObject;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointWriteRequest;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointWriteResult;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

/** Publishes one bounded NRC1 prefix and converges every root-owned protection. */
public final class RecoveryCheckpointCoordinator implements RecoveryCheckpointPublisher {
    private final String cluster;
    private final GenerationMetadataStore generationStore;
    private final PhysicalObjectMetadataStore physicalStore;
    private final ObjectStore objectStore;
    private final RecoveryCheckpointCodecV1 codec;
    private final RecoveryCheckpointBuilder builder;
    private final RecoveryCheckpointMerger merger;
    private final RecoveryCheckpointProtectionManager protections;
    private final RecoveryCheckpointRootReconciler rootReconciler;
    private final GenerationProtocolActivationGuard activationGuard;
    private final RecoveryCheckpointAttemptIdGenerator attemptIds;
    private final MaterializationConfig config;
    private final long pendingProtectionDurationMillis;
    private final ScheduledExecutorService scheduler;
    private final Clock clock;

    public RecoveryCheckpointCoordinator(
            String cluster,
            GenerationMetadataStore generationStore,
            PhysicalObjectMetadataStore physicalStore,
            ObjectStore objectStore,
            RecoveryCheckpointCodecV1 codec,
            RecoveryCheckpointBuilder builder,
            RecoveryCheckpointProtectionManager protections,
            ObjectReadPinManager readPins,
            GenerationProtocolActivationGuard activationGuard,
            MaterializationConfig config,
            Duration pendingProtectionDuration,
            ScheduledExecutorService scheduler,
            Clock clock) {
        this(
                cluster,
                generationStore,
                physicalStore,
                objectStore,
                codec,
                builder,
                protections,
                readPins,
                activationGuard,
                new SecureRecoveryCheckpointAttemptIdGenerator(),
                config,
                pendingProtectionDuration,
                scheduler,
                clock);
    }

    RecoveryCheckpointCoordinator(
            String cluster,
            GenerationMetadataStore generationStore,
            PhysicalObjectMetadataStore physicalStore,
            ObjectStore objectStore,
            RecoveryCheckpointCodecV1 codec,
            RecoveryCheckpointBuilder builder,
            RecoveryCheckpointProtectionManager protections,
            ObjectReadPinManager readPins,
            GenerationProtocolActivationGuard activationGuard,
            RecoveryCheckpointAttemptIdGenerator attemptIds,
            MaterializationConfig config,
            Duration pendingProtectionDuration,
            ScheduledExecutorService scheduler,
            Clock clock) {
        this.cluster = requireText(cluster, "cluster");
        this.generationStore = Objects.requireNonNull(generationStore, "generationStore");
        this.physicalStore = Objects.requireNonNull(physicalStore, "physicalStore");
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
        this.codec = Objects.requireNonNull(codec, "codec");
        this.builder = Objects.requireNonNull(builder, "builder");
        this.protections = Objects.requireNonNull(protections, "protections");
        this.activationGuard = Objects.requireNonNull(activationGuard, "activationGuard");
        this.attemptIds = Objects.requireNonNull(attemptIds, "attemptIds");
        this.config = Objects.requireNonNull(config, "config");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.merger = new RecoveryCheckpointMerger(
                this.cluster,
                this.generationStore,
                this.objectStore,
                this.codec,
                Objects.requireNonNull(readPins, "readPins"),
                this.clock);
        this.pendingProtectionDurationMillis = requirePendingDuration(
                pendingProtectionDuration,
                config.operationTimeout(),
                config.maximumClockSkew());
        this.rootReconciler = new RecoveryCheckpointRootReconciler(
                this.cluster,
                this.generationStore,
                this.objectStore,
                this.codec,
                this.protections,
                this.config.operationTimeout(),
                this.scheduler);
    }

    @Override
    public CompletableFuture<RecoveryCheckpointRunResult> checkpoint(StreamId streamId) {
        try {
            return new Operation(Objects.requireNonNull(streamId, "streamId")).run();
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private final class Operation {
        private final StreamId streamId;
        private final MaterializationDeadline deadline;
        private RecoveryCheckpointWriteResult written;

        private Operation(StreamId streamId) {
            this.streamId = streamId;
            this.deadline = new MaterializationDeadline(config.operationTimeout(), scheduler);
        }

        private CompletableFuture<RecoveryCheckpointRunResult> run() {
            CompletableFuture<VersionedMaterializationStreamRegistration> registration =
                    deadline.bound(
                            () -> generationStore.getStreamRegistration(cluster, streamId)
                                    .thenApply(optional -> optional.orElseThrow(() -> condition(
                                            "materialization stream registration is absent"))),
                            "load recovery checkpoint stream registration");
            CompletableFuture<VersionedRecoveryCheckpointRoot> root = deadline.bound(
                    () -> generationStore.getOrCreateRecoveryRoot(cluster, streamId),
                    "load recovery checkpoint root");
            CompletableFuture<RecoveryCheckpointRunResult> result = registration
                    .thenCombine(root, AdmissionInputs::new)
                    .thenCompose(this::admit)
                    .thenCompose(admission -> rootReconciler
                            .reconcile(admission.root(), deadline)
                            .thenApply(ignored -> admission))
                    .thenCompose(this::planAndPublish)
                    .whenComplete((ignored, failure) -> close());
            return result;
        }

        private CompletableFuture<RecoveryCheckpointRunResult> planAndPublish(
                Admission admission) {
            String attemptId = requireText(
                    attemptIds.next(), "checkpointAttemptId");
            if (admission.root().value().checkpoints().size()
                    == RecoveryCheckpointMerger.MERGE_REFERENCE_COUNT) {
                return deadline.bound(
                                () -> merger.prepare(
                                        admission.root(),
                                        admission.registration(),
                                        attemptId,
                                        deadline),
                                "prepare recovery checkpoint merge")
                        .thenCompose(plan -> publish(
                                admission, new MergePublicationPlan(plan)));
            }
            return deadline.bound(
                            () -> builder.build(
                                    streamId,
                                    admission.root(),
                                    admission.registration(),
                                    attemptId),
                            "build recovery checkpoint plan")
                    .thenCompose(build -> build.plan().isEmpty()
                            ? CompletableFuture.completedFuture(
                                    RecoveryCheckpointRunResult.skipped(
                                            build.status()))
                            : publish(
                                    admission,
                                    new BuiltPublicationPlan(
                                            build.plan().orElseThrow())));
        }

        private CompletableFuture<Admission> admit(AdmissionInputs inputs) {
            LiveProjectionSubject subject = subject(inputs.registration(), streamId);
            return deadline.bound(
                            () -> activationGuard.requireReady(
                                    GenerationOperation.RECOVERY_CHECKPOINT,
                                    subject,
                                    true),
                            "admit recovery checkpoint publication")
                    .thenApply(proof -> new Admission(
                            inputs.root(), inputs.registration(), proof));
        }

        private CompletableFuture<RecoveryCheckpointRunResult> publish(
                Admission admission,
                PublicationPlan plan) {
            CompletableFuture<RecoveryCheckpointRunResult> publication;
            if (!plan.baseRoot().equals(admission.root())
                    || !plan.registration().equals(admission.registration())) {
                publication = CompletableFuture.failedFuture(invariant(
                        "recovery checkpoint publication plan differs from admission"));
            } else {
                publication = deadline.bound(
                            () -> plan.write(deadline.remaining()),
                            "write staged NRC1 recovery checkpoint")
                    .thenCompose(staged -> {
                        written = staged.object();
                        if (staged.object().objectLength()
                                > plan.maximumObjectBytes()) {
                            throw invariant("NRC1 object exceeds configured checkpoint byte limit");
                        }
                        return upload(admission, plan, staged);
                    })
                    .thenCompose(uploaded -> verifyUploaded(uploaded.staged())
                            .thenApply(opened -> new VerifiedUpload(
                                    uploaded.head(),
                                    opened,
                                    checkpointIdentity(
                                            uploaded.staged().object(),
                                            uploaded.head()))))
                    .thenCompose(upload -> acquirePending(
                            admission, plan, upload))
                    .thenCompose(state -> publishRoot(
                            admission, plan, state))
                    .thenCompose(published -> rootReconciler
                            .reconcile(published.root(), deadline)
                            .thenApply(ignored -> RecoveryCheckpointRunResult.published(published)));
            }
            return releasePlan(plan, publication);
        }

        private CompletableFuture<UploadedCheckpoint> upload(
                Admission admission,
                PublicationPlan plan,
                StagedCheckpoint staged) {
            RecoveryCheckpointWriteRequest request = staged.request();
            RecoveryCheckpointWriteResult result = staged.object();
            PutObjectOptions options = new PutObjectOptions(
                    RecoveryCheckpointFormatV1.CONTENT_TYPE,
                    result.storageCrc32c(),
                    true,
                    Map.of(
                            "nereus-format", "NRC1",
                            "nereus-content-sha256", result.contentSha256().value(),
                            "nereus-stream-id", streamId.value(),
                            "nereus-checkpoint-sequence", Long.toString(
                                    request.checkpointSequence()),
                            "nereus-checkpoint-attempt-id",
                                    request.checkpointAttemptId()),
                    deadline.remaining());
            return deadline.bound(
                            () -> objectStore.putObject(
                                    result.objectKey(),
                                    result.stagingFile(),
                                    options,
                                    (key, attempt) -> deadline.bound(
                                            () -> authorizeUpload(
                                                    admission,
                                                    plan,
                                                    staged,
                                                    key,
                                                    attempt),
                                            "authorize recovery checkpoint provider attempt")),
                            "upload NRC1 recovery checkpoint")
                    .handle((ignored, failure) -> {
                        if (failure == null
                                || unwrap(failure) instanceof ObjectAlreadyExistsException) {
                            return head(staged);
                        }
                        return CompletableFuture.<HeadObjectResult>failedFuture(unwrap(failure));
                    })
                    .thenCompose(value -> value)
                    .thenApply(head -> new UploadedCheckpoint(staged, head));
        }

        private CompletableFuture<Void> authorizeUpload(
                Admission admission,
                PublicationPlan plan,
                StagedCheckpoint staged,
                ObjectKey key,
                int providerAttempt) {
            RecoveryCheckpointWriteResult result = staged.object();
            if (!key.equals(result.objectKey()) || providerAttempt <= 0) {
                return CompletableFuture.failedFuture(invariant(
                        "guarded recovery checkpoint upload identity is invalid"));
            }
            return activationGuard.revalidate(admission.proof())
                    .thenCompose(ignored -> plan.revalidate())
                    .thenCompose(ignored -> physicalStore.getRoot(
                            cluster, result.objectKeyHash()))
                    .thenAccept(optional -> optional.ifPresent(root ->
                            requireReusableCheckpointRoot(result, root)));
        }

        private CompletableFuture<HeadObjectResult> head(
                StagedCheckpoint staged) {
            RecoveryCheckpointWriteResult result = staged.object();
            return deadline.bound(
                            () -> objectStore.headObject(
                                    result.objectKey(),
                                    new HeadObjectOptions(deadline.remaining())),
                            "HEAD NRC1 recovery checkpoint")
                    .thenApply(head -> {
                        if (!head.key().equals(result.objectKey())
                                || head.objectLength() != result.objectLength()
                                || !head.checksum().equals(result.storageCrc32c())
                                || !headMetadataMatches(
                                        staged.request(),
                                        result,
                                        head,
                                        streamId.value())) {
                            throw invariant(
                                    "uploaded recovery checkpoint HEAD differs from staged bytes");
                        }
                        return head;
                    });
        }

        private CompletableFuture<RecoveryCheckpointObject> verifyUploaded(
                StagedCheckpoint staged) {
            RecoveryCheckpointWriteResult result = staged.object();
            return deadline.bound(
                            () -> codec.openAndVerify(
                                    result.objectKey(),
                                    result.objectLength(),
                                    result.contentSha256(),
                                    deadline.remaining()),
                            "strictly verify uploaded NRC1 recovery checkpoint")
                    .thenApply(object -> {
                        if (!object.header().equals(staged.request())
                                || !object.objectId().equals(result.objectId())
                                || !object.objectKey().equals(result.objectKey())
                                || object.objectLength() != result.objectLength()
                                || !object.bodySha256().equals(result.bodySha256())
                                || !object.contentSha256().equals(result.contentSha256())
                                || !object.directory().equals(result.directory())) {
                            throw invariant(
                                    "verified NRC1 object differs from staged checkpoint plan");
                        }
                        return object;
                    });
        }

        private CompletableFuture<PendingPublication> acquirePending(
                Admission admission,
                PublicationPlan plan,
                VerifiedUpload upload) {
            long expiresAt = Math.addExact(
                    clock.millis(), pendingProtectionDurationMillis);
            return deadline.bound(
                            plan::revalidate,
                            "revalidate checkpoint plan before pending protection")
                    .thenCompose(ignored -> deadline.bound(
                            () -> activationGuard.revalidate(admission.proof()),
                            "revalidate activation before pending protection"))
                    .thenCompose(ignored -> deadline.bound(
                            () -> protections.acquirePending(
                                    plan.baseRoot(),
                                    upload.checkpoint().header(),
                                    upload.object(),
                                    expiresAt),
                            "acquire bounded recovery checkpoint pending protection"))
                    .thenApply(pending -> new PendingPublication(upload, pending));
        }

        private CompletableFuture<PublishedRecoveryCheckpoint> publishRoot(
                Admission admission,
                PublicationPlan plan,
                PendingPublication state) {
            RecoveryCheckpointWriteRequest request =
                    state.upload().checkpoint().header();
            RecoveryCheckpointReferenceRecord reference = reference(
                    request, written);
            RecoveryCheckpointRootRecord replacement = root(
                    plan, request, reference);
            return deadline.bound(
                            plan::revalidate,
                            "revalidate checkpoint plan before root CAS")
                    .thenCompose(ignored -> deadline.bound(
                            () -> activationGuard.revalidate(admission.proof()),
                            "revalidate activation before recovery-root CAS"))
                    .thenCompose(ignored -> deadline.bound(
                            () -> protections.revalidatePending(
                                    plan.baseRoot(), state.pending()),
                            "revalidate pending protection before recovery-root CAS"))
                    .thenCompose(ignored -> compareAndSetRoot(plan, replacement))
                    .thenApply(published -> new PublishedRecoveryCheckpoint(
                            published, reference, state.upload().object()));
        }

        private CompletableFuture<VersionedRecoveryCheckpointRoot> compareAndSetRoot(
                PublicationPlan plan,
                RecoveryCheckpointRootRecord replacement) {
            return deadline.bound(
                            () -> generationStore.compareAndSetRecoveryRoot(
                                    cluster,
                                    replacement,
                                    plan.baseRoot().metadataVersion()),
                            "publish recovery checkpoint root")
                    .handle((published, failure) -> {
                        if (failure == null) {
                            return CompletableFuture.completedFuture(published);
                        }
                        Throwable exact = unwrap(failure);
                        return deadline.bound(
                                        () -> generationStore.getRecoveryRoot(cluster, streamId),
                                        "reload recovery root after CAS failure")
                                .thenCompose(optional -> {
                                    if (optional.isPresent()
                                            && exactReplacement(
                                                    replacement, optional.orElseThrow())) {
                                        return CompletableFuture.completedFuture(
                                                optional.orElseThrow());
                                    }
                                    return CompletableFuture.failedFuture(exact);
                                });
                    })
                    .thenCompose(value -> value);
        }

        private <T> CompletableFuture<T> releasePlan(
                PublicationPlan plan,
                CompletableFuture<T> source) {
            CompletableFuture<T> result = new CompletableFuture<>();
            source.whenComplete((value, failure) -> {
                CompletableFuture<Void> release;
                try {
                    release = Objects.requireNonNull(
                            plan.release(), "checkpoint plan release future");
                } catch (Throwable releaseFailure) {
                    release = CompletableFuture.failedFuture(releaseFailure);
                }
                release.whenComplete((ignored, releaseFailure) -> {
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
                });
            });
            result.whenComplete((ignored, failure) -> {
                if (result.isCancelled()) {
                    source.cancel(true);
                }
            });
            return result;
        }

        private void close() {
            try {
                RecoveryCheckpointWriteResult result = written;
                if (result != null) {
                    result.close();
                }
            } finally {
                deadline.close();
            }
        }
    }

    private LiveProjectionSubject subject(
            VersionedMaterializationStreamRegistration registration,
            StreamId expectedStream) {
        MaterializationStreamRegistrationRecord value = registration.value();
        String expectedKey = new F4Keyspace(cluster).materializationRegistryKey(
                expectedStream);
        if (!registration.key().equals(expectedKey)
                || !value.streamId().equals(expectedStream.value())) {
            throw invariant("recovery checkpoint registration key is non-canonical");
        }
        StorageProfile profile;
        ProjectionRef projection;
        try {
            profile = StorageProfile.valueOf(value.storageProfile()).canonical();
            projection = ProjectionIdentity.decode(value.projectionRef())
                    .orElseThrow(() -> invariant(
                            "recovery checkpoint registration has no projection identity"));
        } catch (NereusException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw invariant("recovery checkpoint registration is malformed", failure);
        }
        if (!profile.objectMaterializationEnabled()) {
            throw new NereusException(
                    ErrorCode.UNSUPPORTED_STORAGE_PROFILE,
                    false,
                    "recovery checkpoint requires an object-materialization profile");
        }
        return new LiveProjectionSubject(
                new StreamId(value.streamId()),
                projection,
                new Checksum(
                        ChecksumType.SHA256, value.projectionIdentitySha256()));
    }

    private static PhysicalObjectIdentity checkpointIdentity(
            RecoveryCheckpointWriteResult result,
            HeadObjectResult head) {
        return PhysicalObjectIdentity.create(
                result.objectKey(),
                Optional.of(result.objectId()),
                PhysicalObjectKind.RECOVERY_CHECKPOINT,
                result.objectLength(),
                result.storageCrc32c(),
                Optional.of(result.contentSha256()),
                head.etag());
    }

    private static void requireReusableCheckpointRoot(
            RecoveryCheckpointWriteResult result,
            VersionedPhysicalObjectRoot root) {
        var value = root.value();
        if (value.lifecycle() != PhysicalObjectLifecycle.ACTIVE
                || !value.objectKeyHash().equals(result.objectKeyHash().value())
                || !value.objectKey().equals(result.objectKey().value())
                || !value.objectId().equals(result.objectId().value())
                || value.objectKindId() != PhysicalObjectKind.RECOVERY_CHECKPOINT.wireId()
                || value.objectLength() != result.objectLength()
                || !value.storageChecksumType().equals(ChecksumType.CRC32C.name())
                || !value.storageChecksumValue().equals(result.storageCrc32c().value())
                || !value.contentSha256().equals(result.contentSha256().value())) {
            throw condition(
                    "checkpoint object key already has a conflicting physical root");
        }
    }

    private static boolean headMetadataMatches(
            RecoveryCheckpointWriteRequest request,
            RecoveryCheckpointWriteResult result,
            HeadObjectResult head,
            String streamId) {
        if (head.metadata().isEmpty()) {
            return true;
        }
        return "NRC1".equals(head.metadata().get("nereus-format"))
                && result.contentSha256().value().equals(
                        head.metadata().get("nereus-content-sha256"))
                && streamId.equals(
                        head.metadata().get("nereus-stream-id"))
                && Long.toString(request.checkpointSequence()).equals(
                        head.metadata().get("nereus-checkpoint-sequence"))
                && request.checkpointAttemptId().equals(
                        head.metadata().get("nereus-checkpoint-attempt-id"));
    }

    private RecoveryCheckpointReferenceRecord reference(
            RecoveryCheckpointWriteRequest request,
            RecoveryCheckpointWriteResult result) {
        return new RecoveryCheckpointReferenceRecord(
                request.checkpointSequence(),
                request.checkpointAttemptId(),
                request.coverage().startOffset(),
                request.coverage().endOffset(),
                request.firstCommitVersion(),
                request.lastCommitVersion(),
                request.cumulativeSizeAtStart(),
                request.cumulativeSizeAtEnd(),
                request.firstCommitId(),
                request.lastCommitId(),
                request.sourceHeadCommitId(),
                request.sourceHeadCommitVersion(),
                request.projectionIdentitySha256().value(),
                result.objectId().value(),
                result.objectKey().value(),
                result.objectKeyHash().value(),
                result.objectLength(),
                result.storageCrc32c().value(),
                result.contentSha256().value(),
                request.expectedEntryCount(),
                request.expectedPublicationCount());
    }

    private RecoveryCheckpointRootRecord root(
            PublicationPlan plan,
            RecoveryCheckpointWriteRequest request,
            RecoveryCheckpointReferenceRecord reference) {
        List<RecoveryCheckpointReferenceRecord> references = new ArrayList<>(
                plan.retainedReferences());
        references.add(reference);
        List<RecoveryCheckpointReferenceRecord> exact = List.copyOf(references);
        RecoveryCheckpointReferenceRecord first = exact.get(0);
        RecoveryCheckpointReferenceRecord last = exact.get(exact.size() - 1);
        return new RecoveryCheckpointRootRecord(
                1,
                request.streamId().value(),
                request.checkpointSequence(),
                first.coveredStartOffset(),
                last.coveredEndOffset(),
                first.firstCommitVersion(),
                last.lastCommitVersion(),
                first.cumulativeSizeAtStart(),
                last.cumulativeSizeAtEnd(),
                first.firstCommitId(),
                last.lastCommitId(),
                exact,
                RecoveryCheckpointRootDigests.checkpointSetSha256(exact).value(),
                reference.sourceHeadCommitId(),
                reference.sourceHeadCommitVersion(),
                Math.max(clock.millis(), plan.baseRoot().value().publishedAtMillis()),
                0);
    }

    private static boolean exactReplacement(
            RecoveryCheckpointRootRecord expected,
            VersionedRecoveryCheckpointRoot actual) {
        return expected.withMetadataVersion(actual.metadataVersion())
                .equals(actual.value());
    }

    private static long requirePendingDuration(
            Duration pending,
            Duration operationTimeout,
            Duration maximumClockSkew) {
        Objects.requireNonNull(pending, "pendingProtectionDuration");
        if (pending.isZero() || pending.isNegative()) {
            throw new IllegalArgumentException(
                    "pendingProtectionDuration must be positive");
        }
        Duration minimum;
        try {
            minimum = operationTimeout.plus(maximumClockSkew);
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException(
                    "operation timeout plus clock skew overflows", failure);
        }
        if (pending.compareTo(minimum) <= 0) {
            throw new IllegalArgumentException(
                    "pendingProtectionDuration must exceed operation timeout plus clock skew");
        }
        try {
            return pending.toMillis();
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException(
                    "pendingProtectionDuration is not millisecond-representable", failure);
        }
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }

    private static NereusException condition(String message) {
        return new NereusException(
                ErrorCode.METADATA_CONDITION_FAILED, true, message);
    }

    private static NereusException invariant(String message) {
        return invariant(message, null);
    }

    private static NereusException invariant(String message, Throwable cause) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION, false, message, cause);
    }

    private interface PublicationPlan {
        VersionedRecoveryCheckpointRoot baseRoot();

        VersionedMaterializationStreamRegistration registration();

        List<RecoveryCheckpointReferenceRecord> retainedReferences();

        long maximumObjectBytes();

        CompletableFuture<StagedCheckpoint> write(Duration timeout);

        CompletableFuture<Void> revalidate();

        CompletableFuture<Void> release();
    }

    private final class BuiltPublicationPlan implements PublicationPlan {
        private final RecoveryCheckpointPlan plan;

        private BuiltPublicationPlan(RecoveryCheckpointPlan plan) {
            this.plan = Objects.requireNonNull(plan, "plan");
        }

        @Override
        public VersionedRecoveryCheckpointRoot baseRoot() {
            return plan.baseRoot();
        }

        @Override
        public VersionedMaterializationStreamRegistration registration() {
            return plan.registration();
        }

        @Override
        public List<RecoveryCheckpointReferenceRecord> retainedReferences() {
            return plan.retainedReferences();
        }

        @Override
        public long maximumObjectBytes() {
            return plan.maximumObjectBytes();
        }

        @Override
        public CompletableFuture<StagedCheckpoint> write(Duration timeout) {
            Objects.requireNonNull(timeout, "timeout");
            return codec.write(
                            plan.writeRequest(),
                            new RecoveryCheckpointListPublisher<>(
                                    plan.publications()),
                            new RecoveryCheckpointListPublisher<>(
                                    plan.entries()))
                    .thenApply(object -> new StagedCheckpoint(
                            plan.writeRequest(), object));
        }

        @Override
        public CompletableFuture<Void> revalidate() {
            return builder.revalidate(plan);
        }

        @Override
        public CompletableFuture<Void> release() {
            return CompletableFuture.completedFuture(null);
        }
    }

    private final class MergePublicationPlan implements PublicationPlan {
        private final RecoveryCheckpointMerger.PreparedMerge plan;

        private MergePublicationPlan(
                RecoveryCheckpointMerger.PreparedMerge plan) {
            this.plan = Objects.requireNonNull(plan, "plan");
            if (plan.sourceCount()
                    != RecoveryCheckpointMerger.MERGE_REFERENCE_COUNT) {
                throw invariant("prepared checkpoint merge source count changed");
            }
        }

        @Override
        public VersionedRecoveryCheckpointRoot baseRoot() {
            return plan.baseRoot();
        }

        @Override
        public VersionedMaterializationStreamRegistration registration() {
            return plan.registration();
        }

        @Override
        public List<RecoveryCheckpointReferenceRecord> retainedReferences() {
            return List.of();
        }

        @Override
        public long maximumObjectBytes() {
            return config.recoveryCheckpointMaxBytes();
        }

        @Override
        public CompletableFuture<StagedCheckpoint> write(Duration timeout) {
            return plan.write(timeout).thenApply(this::staged);
        }

        private StagedCheckpoint staged(
                RecoveryCheckpointMergeResult result) {
            try {
                if (result.sourceCount()
                                != RecoveryCheckpointMerger.MERGE_REFERENCE_COUNT
                        || result.request().checkpointSequence()
                                != plan.checkpointSequence()
                        || !result.request().checkpointAttemptId().equals(
                                plan.checkpointAttemptId())) {
                    throw invariant("NRC1 merge result differs from its prepared plan");
                }
                return new StagedCheckpoint(
                        result.request(), result.object());
            } catch (Throwable failure) {
                result.close();
                throw failure;
            }
        }

        @Override
        public CompletableFuture<Void> revalidate() {
            return plan.revalidate();
        }

        @Override
        public CompletableFuture<Void> release() {
            return plan.release();
        }
    }

    private record StagedCheckpoint(
            RecoveryCheckpointWriteRequest request,
            RecoveryCheckpointWriteResult object) {
        private StagedCheckpoint {
            Objects.requireNonNull(request, "request");
            Objects.requireNonNull(object, "object");
            if (!object.objectKey().equals(
                    RecoveryCheckpointFormatV1.objectKey(
                            request, object.contentSha256()))) {
                throw new IllegalArgumentException(
                        "staged checkpoint request/object identity is inconsistent");
            }
        }
    }

    private record UploadedCheckpoint(
            StagedCheckpoint staged,
            HeadObjectResult head) {
        private UploadedCheckpoint {
            Objects.requireNonNull(staged, "staged");
            Objects.requireNonNull(head, "head");
        }
    }

    private record AdmissionInputs(
            VersionedMaterializationStreamRegistration registration,
            VersionedRecoveryCheckpointRoot root) {
    }

    private record Admission(
            VersionedRecoveryCheckpointRoot root,
            VersionedMaterializationStreamRegistration registration,
            GenerationActivationProof proof) {
    }

    private record VerifiedUpload(
            HeadObjectResult head,
            RecoveryCheckpointObject checkpoint,
            PhysicalObjectIdentity object) {
    }

    private record PendingPublication(
            VerifiedUpload upload,
            ObjectProtection pending) {
    }
}
