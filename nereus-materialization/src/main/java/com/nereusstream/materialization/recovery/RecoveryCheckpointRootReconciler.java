/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.recovery;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.physical.ObjectProtection;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.materialization.MaterializationDeadline;
import com.nereusstream.metadata.oxia.GenerationIndexIdentity;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedRecoveryCheckpointRoot;
import com.nereusstream.metadata.oxia.codec.GenerationIndexRecordCodecV1;
import com.nereusstream.metadata.oxia.codec.MetadataRecordCodecFactory;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointReferenceRecord;
import com.nereusstream.objectstore.HeadObjectOptions;
import com.nereusstream.objectstore.HeadObjectResult;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointCodecV1;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointFormatV1;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointObject;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointPublication;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/** Rebuilds every current-root permanent protection from immutable NRC1 bytes. */
public final class RecoveryCheckpointRootReconciler {
    private static final int PUBLICATION_PAGE_SIZE =
            RecoveryCheckpointFormatV1.MAX_PUBLICATION_SCAN_PAGE_SIZE;

    private final String cluster;
    private final GenerationMetadataStore generationStore;
    private final ObjectStore objectStore;
    private final RecoveryCheckpointCodecV1 codec;
    private final RecoveryCheckpointProtectionManager protections;
    private final Duration operationTimeout;
    private final ScheduledExecutorService scheduler;
    private final GenerationIndexRecordCodecV1 generationCodec =
            new GenerationIndexRecordCodecV1();

    public RecoveryCheckpointRootReconciler(
            String cluster,
            GenerationMetadataStore generationStore,
            ObjectStore objectStore,
            RecoveryCheckpointCodecV1 codec,
            RecoveryCheckpointProtectionManager protections,
            Duration operationTimeout,
            ScheduledExecutorService scheduler) {
        this.cluster = requireText(cluster, "cluster");
        this.generationStore = Objects.requireNonNull(generationStore, "generationStore");
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
        this.codec = Objects.requireNonNull(codec, "codec");
        this.protections = Objects.requireNonNull(protections, "protections");
        this.operationTimeout = requirePositive(operationTimeout, "operationTimeout");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    }

    public CompletableFuture<Void> reconcile(VersionedRecoveryCheckpointRoot root) {
        MaterializationDeadline deadline = new MaterializationDeadline(
                operationTimeout, scheduler);
        CompletableFuture<Void> result;
        try {
            result = reconcile(root, deadline);
        } catch (Throwable failure) {
            deadline.close();
            return CompletableFuture.failedFuture(failure);
        }
        return result.whenComplete((ignored, failure) -> deadline.close());
    }

    CompletableFuture<Void> reconcile(
            VersionedRecoveryCheckpointRoot root,
            MaterializationDeadline deadline) {
        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(deadline, "deadline");
        if (root.value().checkpoints().isEmpty()) {
            return requireExactRoot(root, deadline);
        }
        return reconcileReference(root, deadline, 0)
                .thenCompose(ignored -> requireExactRoot(root, deadline));
    }

    private CompletableFuture<Void> reconcileReference(
            VersionedRecoveryCheckpointRoot root,
            MaterializationDeadline deadline,
            int index) {
        if (index == root.value().checkpoints().size()) {
            return CompletableFuture.completedFuture(null);
        }
        RecoveryCheckpointReferenceRecord reference =
                root.value().checkpoints().get(index);
        ObjectKey key = new ObjectKey(reference.objectKey());
        CompletableFuture<HeadObjectResult> head = deadline.bound(
                () -> objectStore.headObject(
                        key, new HeadObjectOptions(deadline.remaining())),
                "HEAD recovery checkpoint during root reconciliation");
        return head.thenCompose(value -> {
                    PhysicalObjectIdentity object = checkpointIdentity(root, reference, value);
                    return open(root, reference, deadline)
                            .thenCompose(opened -> deadline.bound(
                                            () -> protections.acquireCheckpointObject(root, object),
                                            "acquire root-owned recovery checkpoint protection")
                                    .thenCompose(checkpointProtection -> scanAndProtectTargets(
                                                    root,
                                                    opened,
                                                    deadline,
                                                    OptionalInt.empty())
                                            .thenCompose(ignored -> open(root, reference, deadline))
                                            .thenCompose(reopened -> deadline.bound(
                                                    () -> protections.releasePublishedPending(
                                                            reference,
                                                            root,
                                                            object,
                                                            checkpointProtection,
                                                            () -> scanAndProtectTargets(
                                                                    root,
                                                                    reopened,
                                                                    deadline,
                                                                    OptionalInt.empty())),
                                                    "replace recovery checkpoint pending protection"))));
                })
                .thenCompose(ignored -> reconcileReference(root, deadline, index + 1));
    }

    private CompletableFuture<RecoveryCheckpointObject> open(
            VersionedRecoveryCheckpointRoot root,
            RecoveryCheckpointReferenceRecord reference,
            MaterializationDeadline deadline) {
        ObjectKey key = new ObjectKey(reference.objectKey());
        Checksum content = new Checksum(ChecksumType.SHA256, reference.contentSha256());
        return deadline.bound(
                        () -> codec.openAndVerify(
                                key,
                                reference.objectLength(),
                                content,
                                deadline.remaining()),
                        "open and verify recovery checkpoint during root reconciliation")
                .thenApply(object -> {
                    requireExactReference(root, reference, object);
                    return object;
                });
    }

    private CompletableFuture<Void> scanAndProtectTargets(
            VersionedRecoveryCheckpointRoot root,
            RecoveryCheckpointObject object,
            MaterializationDeadline deadline,
            OptionalInt continuation) {
        return deadline.bound(
                        () -> codec.scanPublications(
                                object,
                                continuation,
                                PUBLICATION_PAGE_SIZE,
                                deadline.remaining()),
                        "scan recovery checkpoint publication table")
                .thenCompose(page -> protectPageTargets(
                                root, page.values(), deadline, 0)
                        .thenCompose(ignored -> page.continuation().isPresent()
                                ? scanAndProtectTargets(
                                        root,
                                        object,
                                        deadline,
                                        page.continuation())
                                : CompletableFuture.completedFuture(null)));
    }

    private CompletableFuture<Void> protectPageTargets(
            VersionedRecoveryCheckpointRoot root,
            java.util.List<RecoveryCheckpointPublication> publications,
            MaterializationDeadline deadline,
            int index) {
        if (index == publications.size()) {
            return CompletableFuture.completedFuture(null);
        }
        RecoveryCheckpointPublication publication = publications.get(index);
        return loadExactIndex(root, publication, deadline)
                .thenCompose(target -> deadline.bound(
                        () -> protections.acquireCheckpointTarget(root, target),
                        "acquire root-owned recovery checkpoint target protection"))
                .thenCompose(ignored -> protectPageTargets(
                        root, publications, deadline, index + 1));
    }

    private CompletableFuture<VersionedGenerationIndex> loadExactIndex(
            VersionedRecoveryCheckpointRoot root,
            RecoveryCheckpointPublication publication,
            MaterializationDeadline deadline) {
        GenerationIndexRecord embedded;
        byte[] canonical = bytes(publication.canonicalGenerationIndexRecord());
        try {
            embedded = generationCodec.decode(canonical);
            if (!Arrays.equals(canonical, generationCodec.encode(embedded))
                    || embedded.metadataVersion() != 0
                    || !embedded.streamId().equals(root.value().streamId())) {
                throw invariant("NRC1 publication contains a non-canonical generation index");
            }
        } catch (NereusException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw invariant("cannot decode NRC1 generation index", failure);
        }
        GenerationIndexIdentity identity = new GenerationIndexIdentity(
                new StreamId(embedded.streamId()),
                ReadView.fromWireId(embedded.readViewId()),
                embedded.offsetEnd(),
                embedded.generation());
        return deadline.bound(
                        () -> generationStore.getIndex(cluster, identity),
                        "load exact recovery checkpoint target index")
                .thenApply(optional -> {
                    VersionedGenerationIndex actual = optional.orElseThrow(() -> condition(
                            "recovery checkpoint target index is absent"));
                    if (!embedded.withMetadataVersion(actual.metadataVersion())
                                    .equals(actual.value())
                            || !actual.durableValueSha256().equals(sha256(
                                    MetadataRecordCodecFactory.encodeEnvelope(
                                            embedded, GenerationIndexRecord.class)))) {
                        throw condition("recovery checkpoint target index changed");
                    }
                    return actual;
                });
    }

    private CompletableFuture<Void> requireExactRoot(
            VersionedRecoveryCheckpointRoot root,
            MaterializationDeadline deadline) {
        StreamId streamId = new StreamId(root.value().streamId());
        return deadline.bound(
                        () -> generationStore.getRecoveryRoot(cluster, streamId),
                        "revalidate recovery checkpoint root")
                .thenAccept(actual -> {
                    if (!actual.equals(Optional.of(root))) {
                        throw condition("recovery checkpoint root changed during reconciliation");
                    }
                });
    }

    private PhysicalObjectIdentity checkpointIdentity(
            VersionedRecoveryCheckpointRoot root,
            RecoveryCheckpointReferenceRecord reference,
            HeadObjectResult head) {
        ObjectKey key = new ObjectKey(reference.objectKey());
        Checksum storage = new Checksum(ChecksumType.CRC32C, reference.storageCrc32c());
        Checksum content = new Checksum(ChecksumType.SHA256, reference.contentSha256());
        if (!head.key().equals(key)
                || head.objectLength() != reference.objectLength()
                || !head.checksum().equals(storage)
                || !headMetadataMatches(root, reference, head)) {
            throw invariant("recovery checkpoint HEAD differs from root reference");
        }
        PhysicalObjectIdentity object = PhysicalObjectIdentity.create(
                key,
                Optional.of(new ObjectId(reference.objectId())),
                PhysicalObjectKind.RECOVERY_CHECKPOINT,
                reference.objectLength(),
                storage,
                Optional.of(content),
                head.etag());
        if (!object.objectKeyHash().equals(new ObjectKeyHash(reference.objectKeyHash()))) {
            throw invariant("recovery checkpoint root reference has a wrong object-key hash");
        }
        return object;
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
            RecoveryCheckpointReferenceRecord reference,
            RecoveryCheckpointObject object) {
        var header = object.header();
        if (!header.cluster().equals(cluster)
                || !header.streamId().value().equals(root.value().streamId())
                || header.checkpointSequence() != reference.checkpointSequence()
                || !header.checkpointAttemptId().equals(reference.checkpointAttemptId())
                || header.coverage().startOffset() != reference.coveredStartOffset()
                || header.coverage().endOffset() != reference.coveredEndOffset()
                || header.firstCommitVersion() != reference.firstCommitVersion()
                || header.lastCommitVersion() != reference.lastCommitVersion()
                || header.cumulativeSizeAtStart() != reference.cumulativeSizeAtStart()
                || header.cumulativeSizeAtEnd() != reference.cumulativeSizeAtEnd()
                || !header.firstCommitId().equals(reference.firstCommitId())
                || !header.lastCommitId().equals(reference.lastCommitId())
                || !header.sourceHeadCommitId().equals(reference.sourceHeadCommitId())
                || header.sourceHeadCommitVersion() != reference.sourceHeadCommitVersion()
                || !header.projectionIdentitySha256().value().equals(
                        reference.projectionIdentitySha256())
                || header.expectedEntryCount() != reference.commitEntryCount()
                || header.expectedPublicationCount() != reference.publicationCount()
                || !object.objectId().value().equals(reference.objectId())
                || !object.objectKey().value().equals(reference.objectKey())
                || object.objectLength() != reference.objectLength()
                || !object.contentSha256().value().equals(reference.contentSha256())) {
            throw invariant("verified NRC1 object differs from recovery-root reference");
        }
    }

    private static byte[] bytes(ByteBuffer value) {
        ByteBuffer copy = value.asReadOnlyBuffer();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }

    private static Checksum sha256(byte[] value) {
        try {
            return new Checksum(
                    ChecksumType.SHA256,
                    java.util.HexFormat.of().formatHex(
                            java.security.MessageDigest.getInstance("SHA-256")
                                    .digest(value)));
        } catch (java.security.NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    private static Duration requirePositive(Duration value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(field + " must be positive");
        }
        return value;
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }

    private static NereusException condition(String message) {
        return new NereusException(ErrorCode.METADATA_CONDITION_FAILED, true, message);
    }

    private static NereusException invariant(String message) {
        return invariant(message, null);
    }

    private static NereusException invariant(String message, Throwable cause) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION, false, message, cause);
    }
}
