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
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointObject;
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
                    .thenCompose(admission -> deadline.bound(
                                    () -> builder.build(
                                            streamId,
                                            admission.root(),
                                            admission.registration(),
                                            requireText(attemptIds.next(), "checkpointAttemptId")),
                                    "build recovery checkpoint plan")
                            .thenCompose(build -> build.plan().isEmpty()
                                    ? CompletableFuture.completedFuture(
                                            RecoveryCheckpointRunResult.skipped(build.status()))
                                    : publish(admission, build.plan().orElseThrow())))
                    .whenComplete((ignored, failure) -> close());
            return result;
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
                RecoveryCheckpointPlan plan) {
            return deadline.bound(
                            () -> codec.write(
                                    plan.writeRequest(),
                                    new RecoveryCheckpointListPublisher<>(plan.publications()),
                                    new RecoveryCheckpointListPublisher<>(plan.entries())),
                            "write staged NRC1 recovery checkpoint")
                    .thenCompose(result -> {
                        written = result;
                        if (result.objectLength() > plan.maximumObjectBytes()) {
                            throw invariant("NRC1 object exceeds configured checkpoint byte limit");
                        }
                        return upload(admission, plan, result);
                    })
                    .thenCompose(head -> verifyUploaded(plan, written)
                            .thenApply(opened -> new VerifiedUpload(
                                    head,
                                    opened,
                                    checkpointIdentity(written, head))))
                    .thenCompose(upload -> acquirePending(admission, plan, upload))
                    .thenCompose(state -> publishRoot(admission, plan, state))
                    .thenCompose(published -> rootReconciler
                            .reconcile(published.root(), deadline)
                            .thenApply(ignored -> RecoveryCheckpointRunResult.published(published)));
        }

        private CompletableFuture<HeadObjectResult> upload(
                Admission admission,
                RecoveryCheckpointPlan plan,
                RecoveryCheckpointWriteResult result) {
            PutObjectOptions options = new PutObjectOptions(
                    RecoveryCheckpointFormatV1.CONTENT_TYPE,
                    result.storageCrc32c(),
                    true,
                    Map.of(
                            "nereus-format", "NRC1",
                            "nereus-content-sha256", result.contentSha256().value(),
                            "nereus-stream-id", streamId.value(),
                            "nereus-checkpoint-sequence", Long.toString(
                                    plan.writeRequest().checkpointSequence()),
                            "nereus-checkpoint-attempt-id",
                                    plan.writeRequest().checkpointAttemptId()),
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
                                                    result,
                                                    key,
                                                    attempt),
                                            "authorize recovery checkpoint provider attempt")),
                            "upload NRC1 recovery checkpoint")
                    .handle((ignored, failure) -> {
                        if (failure == null
                                || unwrap(failure) instanceof ObjectAlreadyExistsException) {
                            return head(plan, result);
                        }
                        return CompletableFuture.<HeadObjectResult>failedFuture(unwrap(failure));
                    })
                    .thenCompose(value -> value);
        }

        private CompletableFuture<Void> authorizeUpload(
                Admission admission,
                RecoveryCheckpointPlan plan,
                RecoveryCheckpointWriteResult result,
                ObjectKey key,
                int providerAttempt) {
            if (!key.equals(result.objectKey()) || providerAttempt <= 0) {
                return CompletableFuture.failedFuture(invariant(
                        "guarded recovery checkpoint upload identity is invalid"));
            }
            return activationGuard.revalidate(admission.proof())
                    .thenCompose(ignored -> builder.revalidate(plan))
                    .thenCompose(ignored -> physicalStore.getRoot(
                            cluster, result.objectKeyHash()))
                    .thenAccept(optional -> optional.ifPresent(root ->
                            requireReusableCheckpointRoot(result, root)));
        }

        private CompletableFuture<HeadObjectResult> head(
                RecoveryCheckpointPlan plan,
                RecoveryCheckpointWriteResult result) {
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
                                        plan, result, head, streamId.value())) {
                            throw invariant(
                                    "uploaded recovery checkpoint HEAD differs from staged bytes");
                        }
                        return head;
                    });
        }

        private CompletableFuture<RecoveryCheckpointObject> verifyUploaded(
                RecoveryCheckpointPlan plan,
                RecoveryCheckpointWriteResult result) {
            return deadline.bound(
                            () -> codec.openAndVerify(
                                    result.objectKey(),
                                    result.objectLength(),
                                    result.contentSha256(),
                                    deadline.remaining()),
                            "strictly verify uploaded NRC1 recovery checkpoint")
                    .thenApply(object -> {
                        if (!object.header().equals(plan.writeRequest())
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
                RecoveryCheckpointPlan plan,
                VerifiedUpload upload) {
            long expiresAt = Math.addExact(
                    clock.millis(), pendingProtectionDurationMillis);
            return deadline.bound(
                            () -> builder.revalidate(plan),
                            "revalidate checkpoint plan before pending protection")
                    .thenCompose(ignored -> deadline.bound(
                            () -> activationGuard.revalidate(admission.proof()),
                            "revalidate activation before pending protection"))
                    .thenCompose(ignored -> deadline.bound(
                            () -> protections.acquirePending(
                                    plan, upload.object(), expiresAt),
                            "acquire bounded recovery checkpoint pending protection"))
                    .thenApply(pending -> new PendingPublication(upload, pending));
        }

        private CompletableFuture<PublishedRecoveryCheckpoint> publishRoot(
                Admission admission,
                RecoveryCheckpointPlan plan,
                PendingPublication state) {
            RecoveryCheckpointReferenceRecord reference = reference(plan, written);
            RecoveryCheckpointRootRecord replacement = root(plan, reference);
            return deadline.bound(
                            () -> builder.revalidate(plan),
                            "revalidate checkpoint plan before root CAS")
                    .thenCompose(ignored -> deadline.bound(
                            () -> activationGuard.revalidate(admission.proof()),
                            "revalidate activation before recovery-root CAS"))
                    .thenCompose(ignored -> deadline.bound(
                            () -> protections.revalidatePending(plan, state.pending()),
                            "revalidate pending protection before recovery-root CAS"))
                    .thenCompose(ignored -> compareAndSetRoot(plan, replacement))
                    .thenApply(published -> new PublishedRecoveryCheckpoint(
                            published, reference, state.upload().object()));
        }

        private CompletableFuture<VersionedRecoveryCheckpointRoot> compareAndSetRoot(
                RecoveryCheckpointPlan plan,
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
        if (profile != StorageProfile.OBJECT_WAL_SYNC_OBJECT
                && profile != StorageProfile.OBJECT_WAL_ASYNC_OBJECT) {
            throw new NereusException(
                    ErrorCode.UNSUPPORTED_STORAGE_PROFILE,
                    false,
                    "recovery checkpoint requires an Object-WAL profile");
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
            RecoveryCheckpointPlan plan,
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
                && Long.toString(plan.writeRequest().checkpointSequence()).equals(
                        head.metadata().get("nereus-checkpoint-sequence"))
                && plan.writeRequest().checkpointAttemptId().equals(
                        head.metadata().get("nereus-checkpoint-attempt-id"));
    }

    private RecoveryCheckpointReferenceRecord reference(
            RecoveryCheckpointPlan plan,
            RecoveryCheckpointWriteResult result) {
        var request = plan.writeRequest();
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
            RecoveryCheckpointPlan plan,
            RecoveryCheckpointReferenceRecord reference) {
        List<RecoveryCheckpointReferenceRecord> references = new ArrayList<>(
                plan.retainedReferences());
        references.add(reference);
        List<RecoveryCheckpointReferenceRecord> exact = List.copyOf(references);
        RecoveryCheckpointReferenceRecord first = exact.get(0);
        RecoveryCheckpointReferenceRecord last = exact.get(exact.size() - 1);
        return new RecoveryCheckpointRootRecord(
                1,
                plan.writeRequest().streamId().value(),
                plan.writeRequest().checkpointSequence(),
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
