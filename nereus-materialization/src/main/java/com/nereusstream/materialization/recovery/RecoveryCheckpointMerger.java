/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.recovery;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.PublicationId;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.physical.ObjectReadLease;
import com.nereusstream.core.physical.ObjectReadPinManager;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.materialization.MaterializationDeadline;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.VersionedMaterializationStreamRegistration;
import com.nereusstream.metadata.oxia.VersionedRecoveryCheckpointRoot;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointReferenceRecord;
import com.nereusstream.objectstore.HeadObjectOptions;
import com.nereusstream.objectstore.HeadObjectResult;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointCodecV1;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointMergeResult;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointObject;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/** Pins and strictly opens the current 32-object NRC1 chain before one root-replacement merge. */
public final class RecoveryCheckpointMerger {
    public static final int MERGE_REFERENCE_COUNT = 32;

    private final String cluster;
    private final GenerationMetadataStore generationStore;
    private final ObjectStore objectStore;
    private final RecoveryCheckpointCodecV1 codec;
    private final ObjectReadPinManager readPins;
    private final Clock clock;

    public RecoveryCheckpointMerger(
            String cluster,
            GenerationMetadataStore generationStore,
            ObjectStore objectStore,
            RecoveryCheckpointCodecV1 codec,
            ObjectReadPinManager readPins,
            Clock clock) {
        this.cluster = requireText(cluster, "cluster");
        this.generationStore = Objects.requireNonNull(generationStore, "generationStore");
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
        this.codec = Objects.requireNonNull(codec, "codec");
        this.readPins = Objects.requireNonNull(readPins, "readPins");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public CompletableFuture<PreparedMerge> prepare(
            VersionedRecoveryCheckpointRoot root,
            VersionedMaterializationStreamRegistration registration,
            String checkpointAttemptId,
            MaterializationDeadline deadline) {
        final String exactAttempt;
        final long checkpointSequence;
        final long maximumReadDeadlineMillis;
        try {
            validateInputs(root, registration);
            exactAttempt = new PublicationId(requireText(
                            checkpointAttemptId, "checkpointAttemptId"))
                    .value();
            checkpointSequence = Math.addExact(
                    root.value().checkpointSequence(), 1);
            maximumReadDeadlineMillis = maximumReadDeadlineMillis(deadline);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }

        List<RecoveryCheckpointObject> sources = new ArrayList<>(
                MERGE_REFERENCE_COUNT);
        List<ObjectReadLease> leases = new ArrayList<>(MERGE_REFERENCE_COUNT);
        AtomicBoolean cancelled = new AtomicBoolean();
        CompletableFuture<PreparedMerge> prepared = loadSource(
                        root,
                        registration,
                        deadline,
                        maximumReadDeadlineMillis,
                        sources,
                        leases,
                        cancelled,
                        0)
                .thenCompose(ignored -> requireExactSelection(root, registration))
                .thenApply(ignored -> new PreparedMerge(
                        root,
                        registration,
                        List.copyOf(sources),
                        List.copyOf(leases),
                        checkpointSequence,
                        exactAttempt));
        CompletableFuture<PreparedMerge> result = new CompletableFuture<>();
        prepared.whenComplete((plan, failure) -> {
            if (failure == null) {
                if (!result.complete(plan)) {
                    plan.release();
                }
                return;
            }
            Throwable exact = unwrap(failure);
            releaseAll(leases).whenComplete((ignored, releaseFailure) -> {
                if (releaseFailure != null) {
                    exact.addSuppressed(unwrap(releaseFailure));
                }
                result.completeExceptionally(exact);
            });
        });
        result.whenComplete((ignored, failure) -> {
            if (result.isCancelled()) {
                cancelled.set(true);
            }
        });
        return result;
    }

    private CompletableFuture<Void> loadSource(
            VersionedRecoveryCheckpointRoot root,
            VersionedMaterializationStreamRegistration registration,
            MaterializationDeadline deadline,
            long maximumReadDeadlineMillis,
            List<RecoveryCheckpointObject> sources,
            List<ObjectReadLease> leases,
            AtomicBoolean cancelled,
            int index) {
        if (cancelled.get()) {
            return CompletableFuture.failedFuture(cancelled());
        }
        if (index == root.value().checkpoints().size()) {
            return CompletableFuture.completedFuture(null);
        }
        RecoveryCheckpointReferenceRecord reference =
                root.value().checkpoints().get(index);
        ObjectKey key = new ObjectKey(reference.objectKey());
        CompletableFuture<HeadObjectResult> headed = deadline.bound(
                () -> objectStore.headObject(
                        key, new HeadObjectOptions(deadline.remaining())),
                "HEAD recovery checkpoint merge source");
        return headed.thenApply(head -> checkpointIdentity(root, reference, head))
                .thenCompose(identity -> cancelled.get()
                        ? CompletableFuture.failedFuture(cancelled())
                        : deadline.bound(
                                () -> readPins.acquire(
                                        identity,
                                        maximumReadDeadlineMillis,
                                        () -> requireExactSelection(
                                                root, registration)),
                                "pin recovery checkpoint merge source"))
                .thenCompose(lease -> {
                    if (cancelled.get()) {
                        return releaseCancelled(lease);
                    }
                    leases.add(lease);
                    return deadline.bound(
                                    () -> codec.openAndVerify(
                                            key,
                                            reference.objectLength(),
                                            new Checksum(
                                                    ChecksumType.SHA256,
                                                    reference.contentSha256()),
                                            deadline.remaining()),
                                    "open recovery checkpoint merge source")
                            .thenApply(object -> {
                                requireExactReference(root, registration, reference, object);
                                sources.add(object);
                                return null;
                            });
                })
                .thenCompose(ignored -> loadSource(
                        root,
                        registration,
                        deadline,
                        maximumReadDeadlineMillis,
                        sources,
                        leases,
                        cancelled,
                        index + 1));
    }

    private static CompletableFuture<Void> releaseCancelled(
            ObjectReadLease lease) {
        CancellationException cancelled = cancelled();
        CompletableFuture<Void> release;
        try {
            release = Objects.requireNonNull(
                    lease.release(), "cancelled reader lease release future");
        } catch (Throwable failure) {
            cancelled.addSuppressed(unwrap(failure));
            return CompletableFuture.failedFuture(cancelled);
        }
        return release.handle((ignored, failure) -> {
                    if (failure != null) {
                        cancelled.addSuppressed(unwrap(failure));
                    }
                    return CompletableFuture.<Void>failedFuture(cancelled);
                })
                .thenCompose(value -> value);
    }

    private CompletableFuture<Void> requireExactSelection(
            VersionedRecoveryCheckpointRoot root,
            VersionedMaterializationStreamRegistration registration) {
        StreamId streamId = new StreamId(root.value().streamId());
        CompletableFuture<Optional<VersionedRecoveryCheckpointRoot>> currentRoot =
                generationStore.getRecoveryRoot(cluster, streamId);
        CompletableFuture<Optional<VersionedMaterializationStreamRegistration>>
                currentRegistration = generationStore.getStreamRegistration(
                        cluster, streamId);
        return currentRoot.thenCombine(currentRegistration, (actualRoot, actualRegistration) -> {
            if (!actualRoot.equals(Optional.of(root))) {
                throw condition("recovery checkpoint root changed during merge pinning");
            }
            if (!actualRegistration.equals(Optional.of(registration))) {
                throw condition("materialization stream registration changed during checkpoint merge");
            }
            return null;
        });
    }

    private void validateInputs(
            VersionedRecoveryCheckpointRoot root,
            VersionedMaterializationStreamRegistration registration) {
        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(registration, "registration");
        StreamId streamId = new StreamId(root.value().streamId());
        F4Keyspace keys = new F4Keyspace(cluster);
        if (!root.key().equals(keys.recoveryRootKey(streamId))
                || !registration.key().equals(
                        keys.materializationRegistryKey(streamId))
                || !registration.value().streamId().equals(streamId.value())) {
            throw invariant("recovery checkpoint merge root/registration identity is non-canonical");
        }
        List<RecoveryCheckpointReferenceRecord> references =
                root.value().checkpoints();
        if (references.size() != MERGE_REFERENCE_COUNT) {
            throw new IllegalArgumentException(
                    "recovery checkpoint merge requires exactly 32 current references");
        }
        RecoveryCheckpointReferenceRecord previous = null;
        for (RecoveryCheckpointReferenceRecord reference : references) {
            if (!reference.projectionIdentitySha256().equals(
                    registration.value().projectionIdentitySha256())) {
                throw invariant("recovery checkpoint reference projection differs from registration");
            }
            if (reference.checkpointSequence()
                    > root.value().checkpointSequence()) {
                throw invariant("recovery checkpoint reference sequence exceeds root sequence");
            }
            if (previous != null
                    && reference.checkpointSequence()
                            <= previous.checkpointSequence()) {
                throw invariant("recovery checkpoint reference sequences are not strictly ordered");
            }
            previous = reference;
        }
        RecoveryCheckpointReferenceRecord last = Objects.requireNonNull(previous);
        if (!last.sourceHeadCommitId().equals(root.value().sourceHeadCommitId())
                || last.sourceHeadCommitVersion()
                        != root.value().sourceHeadCommitVersion()) {
            throw invariant("recovery checkpoint root source-head summary differs from its last reference");
        }
    }

    private PhysicalObjectIdentity checkpointIdentity(
            VersionedRecoveryCheckpointRoot root,
            RecoveryCheckpointReferenceRecord reference,
            HeadObjectResult head) {
        ObjectKey key = new ObjectKey(reference.objectKey());
        Checksum storage = new Checksum(
                ChecksumType.CRC32C, reference.storageCrc32c());
        Checksum content = new Checksum(
                ChecksumType.SHA256, reference.contentSha256());
        if (!head.key().equals(key)
                || head.objectLength() != reference.objectLength()
                || !head.checksum().equals(storage)
                || !headMetadataMatches(root, reference, head)) {
            throw invariant("recovery checkpoint merge HEAD differs from root reference");
        }
        PhysicalObjectIdentity identity = PhysicalObjectIdentity.create(
                key,
                Optional.of(new ObjectId(reference.objectId())),
                PhysicalObjectKind.RECOVERY_CHECKPOINT,
                reference.objectLength(),
                storage,
                Optional.of(content),
                head.etag());
        if (!identity.objectKeyHash().equals(
                new ObjectKeyHash(reference.objectKeyHash()))) {
            throw invariant("recovery checkpoint reference has a wrong object-key hash");
        }
        return identity;
    }

    private static boolean headMetadataMatches(
            VersionedRecoveryCheckpointRoot root,
            RecoveryCheckpointReferenceRecord reference,
            HeadObjectResult head) {
        if (head.metadata().isEmpty()) {
            return true;
        }
        return "NRC1".equals(head.metadata().get("nereus-format"))
                && reference.contentSha256().equals(
                        head.metadata().get("nereus-content-sha256"))
                && root.value().streamId().equals(
                        head.metadata().get("nereus-stream-id"))
                && Long.toString(reference.checkpointSequence()).equals(
                        head.metadata().get("nereus-checkpoint-sequence"))
                && reference.checkpointAttemptId().equals(
                        head.metadata().get("nereus-checkpoint-attempt-id"));
    }

    private void requireExactReference(
            VersionedRecoveryCheckpointRoot root,
            VersionedMaterializationStreamRegistration registration,
            RecoveryCheckpointReferenceRecord reference,
            RecoveryCheckpointObject object) {
        var header = object.header();
        if (!header.cluster().equals(cluster)
                || !header.streamId().value().equals(root.value().streamId())
                || !header.streamId().value().equals(
                        registration.value().streamId())
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
                        registration.value().projectionIdentitySha256())
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
            throw invariant("verified NRC1 object differs from recovery-root merge reference");
        }
    }

    private long maximumReadDeadlineMillis(MaterializationDeadline deadline) {
        Objects.requireNonNull(deadline, "deadline");
        Duration remaining = deadline.remaining();
        long remainingMillis = remaining.toMillis();
        if (!remaining.isZero() && remainingMillis == 0) {
            remainingMillis = 1;
        }
        try {
            return Math.addExact(clock.millis(), remainingMillis);
        } catch (ArithmeticException failure) {
            throw invariant("recovery checkpoint merge read deadline overflows", failure);
        }
    }

    private static CompletableFuture<Void> releaseAll(
            List<ObjectReadLease> leases) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        releaseAt(List.copyOf(leases), 0, null, result);
        return result;
    }

    private static void releaseAt(
            List<ObjectReadLease> leases,
            int index,
            Throwable firstFailure,
            CompletableFuture<Void> result) {
        if (index == leases.size()) {
            if (firstFailure == null) {
                result.complete(null);
            } else {
                result.completeExceptionally(firstFailure);
            }
            return;
        }
        CompletableFuture<Void> release;
        try {
            release = Objects.requireNonNull(
                    leases.get(index).release(), "reader lease release future");
        } catch (Throwable failure) {
            Throwable exact = unwrap(failure);
            if (firstFailure != null) {
                firstFailure.addSuppressed(exact);
                exact = firstFailure;
            }
            releaseAt(leases, index + 1, exact, result);
            return;
        }
        Throwable retained = firstFailure;
        release.whenComplete((ignored, failure) -> {
            Throwable next = retained;
            if (failure != null) {
                Throwable exact = unwrap(failure);
                if (next == null) {
                    next = exact;
                } else {
                    next.addSuppressed(exact);
                }
            }
            releaseAt(leases, index + 1, next, result);
        });
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

    private static CancellationException cancelled() {
        return new CancellationException(
                "recovery checkpoint merge preparation was cancelled");
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

    /** Close-owned source leases remain live until root publication and protection reconciliation finish. */
    public final class PreparedMerge {
        private final VersionedRecoveryCheckpointRoot baseRoot;
        private final VersionedMaterializationStreamRegistration registration;
        private final List<RecoveryCheckpointObject> sources;
        private final List<ObjectReadLease> leases;
        private final long checkpointSequence;
        private final String checkpointAttemptId;
        private final AtomicBoolean writeStarted = new AtomicBoolean();
        private CompletableFuture<Void> release;

        private PreparedMerge(
                VersionedRecoveryCheckpointRoot baseRoot,
                VersionedMaterializationStreamRegistration registration,
                List<RecoveryCheckpointObject> sources,
                List<ObjectReadLease> leases,
                long checkpointSequence,
                String checkpointAttemptId) {
            this.baseRoot = baseRoot;
            this.registration = registration;
            this.sources = sources;
            this.leases = leases;
            this.checkpointSequence = checkpointSequence;
            this.checkpointAttemptId = checkpointAttemptId;
        }

        public VersionedRecoveryCheckpointRoot baseRoot() {
            return baseRoot;
        }

        public VersionedMaterializationStreamRegistration registration() {
            return registration;
        }

        public int sourceCount() {
            return sources.size();
        }

        public long checkpointSequence() {
            return checkpointSequence;
        }

        public String checkpointAttemptId() {
            return checkpointAttemptId;
        }

        public CompletableFuture<RecoveryCheckpointMergeResult> write(
                Duration timeout) {
            if (!writeStarted.compareAndSet(false, true)) {
                return CompletableFuture.failedFuture(invariant(
                        "recovery checkpoint merge plan was written more than once"));
            }
            synchronized (this) {
                if (release != null) {
                    return CompletableFuture.failedFuture(invariant(
                            "recovery checkpoint merge plan was released before write"));
                }
            }
            return codec.merge(
                    sources,
                    checkpointSequence,
                    checkpointAttemptId,
                    timeout);
        }

        public CompletableFuture<Void> revalidate() {
            return requireExactSelection(baseRoot, registration);
        }

        public synchronized CompletableFuture<Void> release() {
            if (release == null) {
                release = releaseAll(leases);
            }
            return release;
        }
    }
}
