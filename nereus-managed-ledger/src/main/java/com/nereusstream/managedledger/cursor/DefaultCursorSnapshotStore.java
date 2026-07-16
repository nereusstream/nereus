/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.physical.ObjectProtection;
import com.nereusstream.core.physical.ObjectProtectionManager;
import com.nereusstream.core.physical.ObjectProtectionOwner;
import com.nereusstream.core.physical.ObjectProtectionRequest;
import com.nereusstream.core.physical.ObjectReadLease;
import com.nereusstream.core.physical.ObjectReadPinManager;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.metadata.oxia.CursorIds;
import com.nereusstream.metadata.oxia.CursorKeyspace;
import com.nereusstream.metadata.oxia.CursorMetadataDigests;
import com.nereusstream.metadata.oxia.CursorMetadataStore;
import com.nereusstream.metadata.oxia.OxiaKeyspace;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.VersionedCursorState;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.records.CursorRecordLifecycle;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.objectstore.ByteBufferObjectUpload;
import com.nereusstream.objectstore.HeadObjectOptions;
import com.nereusstream.objectstore.HeadObjectResult;
import com.nereusstream.objectstore.ObjectAlreadyExistsException;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.objectstore.PutObjectResult;
import com.nereusstream.objectstore.RangeReadOptions;
import com.nereusstream.objectstore.RangeReadResult;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * F4-protected ObjectStore-backed immutable NCS1 cursor snapshot persistence.
 *
 * <p>Upload is guarded by the exact current cursor root. Preparation creates an ACTIVE physical root plus a bounded
 * current-root-owned pending protection. Completion creates/revalidates the published-root-owned permanent protection
 * before releasing the pending protection. Every byte-bearing read holds a durable physical-object lease through its
 * post-admission HEAD and range IO; the initial HEAD only constructs the immutable identity needed for admission.
 */
public final class DefaultCursorSnapshotStore implements CursorSnapshotStore {
    private static final String CONTENT_TYPE = "application/vnd.nereus.cursor-snapshot-v1";
    private static final Map<String, String> METADATA_BASE = Map.of(
            "nereus-format", "NCS1",
            "nereus-object-type", "CURSOR_SNAPSHOT_OBJECT");

    private final String cluster;
    private final ObjectStore objectStore;
    private final CursorMetadataStore cursorMetadataStore;
    private final PhysicalObjectMetadataStore physicalMetadataStore;
    private final ObjectProtectionManager protections;
    private final ObjectReadPinManager readPins;
    private final CursorStorageConfig cursorConfig;
    private final Duration objectStoreRequestTimeout;
    private final long pendingProtectionDurationMillis;
    private final Clock clock;
    private final Supplier<String> snapshotIdSupplier;
    private final LongSupplier nanoTime;
    private final CursorKeyspace cursorKeys;
    private final AtomicBoolean closed = new AtomicBoolean();

    public DefaultCursorSnapshotStore(
            String cluster,
            ObjectStore objectStore,
            CursorMetadataStore cursorMetadataStore,
            PhysicalObjectMetadataStore physicalMetadataStore,
            ObjectProtectionManager protections,
            ObjectReadPinManager readPins,
            CursorStorageConfig cursorConfig,
            Duration objectStoreRequestTimeout,
            Duration pendingProtectionDuration,
            Clock clock) {
        this(
                cluster,
                objectStore,
                cursorMetadataStore,
                physicalMetadataStore,
                protections,
                readPins,
                cursorConfig,
                objectStoreRequestTimeout,
                pendingProtectionDuration,
                clock,
                secureRandomIdSupplier(),
                System::nanoTime);
    }

    DefaultCursorSnapshotStore(
            String cluster,
            ObjectStore objectStore,
            CursorMetadataStore cursorMetadataStore,
            PhysicalObjectMetadataStore physicalMetadataStore,
            ObjectProtectionManager protections,
            ObjectReadPinManager readPins,
            CursorStorageConfig cursorConfig,
            Duration objectStoreRequestTimeout,
            Duration pendingProtectionDuration,
            Clock clock,
            Supplier<String> snapshotIdSupplier,
            LongSupplier nanoTime) {
        this.cluster = new OxiaKeyspace(cluster).cluster();
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
        this.cursorMetadataStore = Objects.requireNonNull(
                cursorMetadataStore, "cursorMetadataStore");
        this.physicalMetadataStore = Objects.requireNonNull(
                physicalMetadataStore, "physicalMetadataStore");
        this.protections = Objects.requireNonNull(protections, "protections");
        this.readPins = Objects.requireNonNull(readPins, "readPins");
        this.cursorConfig = Objects.requireNonNull(cursorConfig, "cursorConfig");
        this.objectStoreRequestTimeout = requirePositive(
                objectStoreRequestTimeout, "objectStoreRequestTimeout");
        this.pendingProtectionDurationMillis = requirePositive(
                        pendingProtectionDuration, "pendingProtectionDuration")
                .toMillis();
        if (pendingProtectionDuration.compareTo(
                        cursorConfig.cursorSnapshotOperationTimeout())
                <= 0) {
            throw new IllegalArgumentException(
                    "pendingProtectionDuration must exceed cursor snapshot operation timeout");
        }
        this.clock = Objects.requireNonNull(clock, "clock");
        this.snapshotIdSupplier = Objects.requireNonNull(
                snapshotIdSupplier, "snapshotIdSupplier");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        this.cursorKeys = new CursorKeyspace(this.cluster);
    }

    @Override
    public CompletableFuture<CursorSnapshotPublication> prepareWrite(
            CursorSnapshotWriteRequest request,
            CursorSnapshotWriteAuthority authority) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(authority, "authority");
        if (closed.get()) {
            return closedFuture();
        }
        try {
            authority.requireMatches(request);
            Deadline deadline = Deadline.start(
                    cursorConfig.cursorSnapshotOperationTimeout(), nanoTime);
            return writeAttempt(request, authority, deadline, 0);
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
    }

    @Override
    public CompletableFuture<Void> completeWrite(
            CursorSnapshotPublication publication,
            VersionedCursorState publishedRoot) {
        Objects.requireNonNull(publication, "publication");
        Objects.requireNonNull(publishedRoot, "publishedRoot");
        if (closed.get()) {
            return closedFuture();
        }
        try {
            requirePublishedRoot(publication, publishedRoot);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        ObjectProtectionOwner owner = owner(publishedRoot);
        ObjectProtectionRequest request = new ObjectProtectionRequest(
                publication.object(),
                ObjectProtectionType.CURSOR_SNAPSHOT_ROOT,
                publication.reference().snapshotId(),
                owner,
                0);
        ObjectProtectionManager.OwnerRevalidator revalidator = expected ->
                revalidatePublishedRoot(publication, publishedRoot, expected);
        return protections.acquireOrTransfer(request, revalidator)
                .thenCompose(permanent -> protections.revalidate(
                        permanent, revalidator))
                .thenCompose(permanent -> protections.release(
                        publication.pendingProtection(),
                        ignored -> protections.revalidate(permanent, revalidator)
                                .thenCompose(revalidated -> revalidatePublishedRoot(
                                        publication,
                                        publishedRoot,
                                        revalidated.owner()))));
    }

    @Override
    public CompletableFuture<CursorAckState> read(
            CursorSnapshotReference reference,
            CursorIdentity expectedIdentity) {
        Objects.requireNonNull(reference, "reference");
        Objects.requireNonNull(expectedIdentity, "expectedIdentity");
        if (closed.get()) {
            return closedFuture();
        }
        final Deadline deadline;
        try {
            validateReferenceKey(reference, expectedIdentity);
            deadline = Deadline.start(
                    cursorConfig.cursorSnapshotOperationTimeout(), nanoTime);
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
        return head(reference, deadline)
                .thenCompose(initialHead -> {
                    PhysicalObjectIdentity object = physicalObject(
                            reference, initialHead);
                    long maximumReadDeadline = maximumReadDeadline();
                    return protectLiveReference(
                                    reference, expectedIdentity, object)
                            .thenCompose(ignored -> readPins.acquire(
                                    object,
                                    maximumReadDeadline,
                                    () -> revalidateLiveReference(
                                            reference, expectedIdentity)))
                            .thenCompose(lease -> readUnderLease(
                                    reference,
                                    expectedIdentity,
                                    object,
                                    lease,
                                    deadline));
                });
    }

    private CompletableFuture<ObjectProtection> protectLiveReference(
            CursorSnapshotReference reference,
            CursorIdentity expectedIdentity,
            PhysicalObjectIdentity object) {
        return loadLiveReferenceRoot(reference, expectedIdentity)
                .thenCompose(root -> {
                    ObjectProtectionRequest request =
                            new ObjectProtectionRequest(
                                    object,
                                    ObjectProtectionType.CURSOR_SNAPSHOT_ROOT,
                                    reference.snapshotId(),
                                    owner(root),
                                    0);
                    ObjectProtectionManager.OwnerRevalidator revalidator =
                            expectedOwner -> revalidateLiveReferenceOwner(
                                    reference,
                                    expectedIdentity,
                                    expectedOwner);
                    return protections.acquireOrTransfer(request, revalidator)
                            .thenCompose(protection ->
                                    protections.revalidate(
                                            protection, revalidator));
                });
    }

    @Override
    public void close() {
        closed.set(true);
    }

    private CompletableFuture<CursorSnapshotPublication> writeAttempt(
            CursorSnapshotWriteRequest request,
            CursorSnapshotWriteAuthority authority,
            Deadline deadline,
            int attempt) {
        if (closed.get()) {
            return closedFuture();
        }
        final String snapshotId;
        final ObjectKey objectKey;
        final CursorSnapshotCodecV1.EncodedSnapshot encoded;
        final Map<String, String> metadata;
        try {
            if (attempt >= cursorConfig.cursorSnapshotIdMaxAttempts()) {
                throw new NereusException(
                        ErrorCode.OBJECT_UPLOAD_FAILED,
                        false,
                        "cursor snapshot ID collision retry bound is exhausted");
            }
            snapshotId = CursorIds.requireRandomId(
                    snapshotIdSupplier.get(), "snapshotId");
            objectKey = CursorSnapshotKeys.objectKey(
                    cluster, request.identity(), snapshotId);
            encoded = CursorSnapshotCodecV1.encode(
                    request, snapshotId, cursorConfig);
            metadata = metadata(snapshotId);
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }

        ByteBufferObjectUpload source = new ByteBufferObjectUpload(encoded.payload());
        CompletableFuture<CursorSnapshotPublication> attemptFuture;
        try {
            PutObjectOptions options = new PutObjectOptions(
                    CONTENT_TYPE,
                    encoded.storageChecksum(),
                    true,
                    metadata,
                    deadline.callTimeout(objectStoreRequestTimeout));
            attemptFuture = objectStore.putObject(
                            objectKey,
                            source,
                            options,
                            (key, providerAttempt) -> authorizeUpload(
                                    request,
                                    authority,
                                    key,
                                    encoded))
                    .thenCompose(result -> {
                        verifyPut(
                                result,
                                objectKey,
                                encoded.objectLength(),
                                encoded.storageChecksum());
                        return objectStore.headObject(
                                objectKey,
                                new HeadObjectOptions(
                                        deadline.callTimeout(
                                                objectStoreRequestTimeout)));
                    })
                    .thenCompose(head -> {
                        verifyHead(
                                head,
                                objectKey,
                                encoded.objectLength(),
                                encoded.storageChecksum(),
                                metadata);
                        CursorSnapshotReference reference =
                                new CursorSnapshotReference(
                                        objectKey,
                                        snapshotId,
                                        request.identity().cursorGeneration(),
                                        request.sourceMutationSequence(),
                                        request.fullState().markDeleteOffset(),
                                        encoded.objectLength(),
                                        encoded.storageChecksum(),
                                        encoded.formatCrc32c(),
                                        1,
                                        request.createdAtMillis());
                        PhysicalObjectIdentity object = physicalObject(
                                reference, head);
                        return acquirePending(
                                request,
                                authority,
                                reference,
                                object);
                    });
        } catch (Throwable error) {
            attemptFuture = CompletableFuture.failedFuture(error);
        }
        attemptFuture.whenComplete((ignored, failure) -> source.close());

        return attemptFuture.handle((result, error) -> {
            if (error == null) {
                return CompletableFuture.completedFuture(result);
            }
            Throwable cause = unwrap(error);
            if (isIfAbsentCollision(cause)
                    && attempt + 1
                            < cursorConfig.cursorSnapshotIdMaxAttempts()) {
                return writeAttempt(
                        request, authority, deadline, attempt + 1);
            }
            return CompletableFuture
                    .<CursorSnapshotPublication>failedFuture(cause);
        }).thenCompose(Function.identity());
    }

    private CompletableFuture<CursorSnapshotPublication> acquirePending(
            CursorSnapshotWriteRequest request,
            CursorSnapshotWriteAuthority authority,
            CursorSnapshotReference reference,
            PhysicalObjectIdentity object) {
        ObjectProtectionOwner owner = owner(authority.currentRoot());
        long expiresAt;
        try {
            expiresAt = Math.addExact(
                    nonNegativeNow(), pendingProtectionDurationMillis);
        } catch (ArithmeticException overflow) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.METADATA_LIMIT_EXCEEDED,
                    false,
                    "cursor snapshot pending-protection deadline overflows"));
        }
        ObjectProtectionRequest protectionRequest = new ObjectProtectionRequest(
                object,
                ObjectProtectionType.CURSOR_SNAPSHOT_PENDING,
                reference.snapshotId(),
                owner,
                expiresAt);
        ObjectProtectionManager.OwnerRevalidator revalidator = expected ->
                revalidateWriteAuthority(request, authority, expected);
        return protections.acquire(protectionRequest, revalidator)
                .thenCompose(pending -> protections.revalidate(
                        pending, revalidator))
                .thenCompose(pending -> loadExactActiveRoot(object)
                        .thenCompose(root -> revalidateWriteAuthority(
                                        request,
                                        authority,
                                        pending.owner())
                                .thenApply(ignored -> {
                                    if (root.value().lifecycleEpoch()
                                            != pending.rootLifecycleEpoch()) {
                                        throw invariant(
                                                "cursor pending protection root epoch changed");
                                    }
                                    return new CursorSnapshotPublication(
                                            request,
                                            authority,
                                            reference,
                                            object,
                                            pending);
                                })));
    }

    private CompletableFuture<Void> authorizeUpload(
            CursorSnapshotWriteRequest request,
            CursorSnapshotWriteAuthority authority,
            ObjectKey key,
            CursorSnapshotCodecV1.EncodedSnapshot encoded) {
        ObjectKey expected = CursorSnapshotKeys.objectKey(
                cluster,
                request.identity(),
                keySnapshotId(key));
        if (!expected.equals(key)) {
            return CompletableFuture.failedFuture(invariant(
                    "cursor snapshot guarded PUT received a different key"));
        }
        return revalidateWriteAuthority(
                        request, authority, owner(authority.currentRoot()))
                .thenCompose(ignored -> physicalMetadataStore.getRoot(
                        cluster, ObjectKeyHash.from(key)))
                .thenAccept(root -> root.ifPresent(value ->
                        requireCompatiblePreUploadRoot(
                                key, encoded, value)));
    }

    private CompletableFuture<Void> revalidateWriteAuthority(
            CursorSnapshotWriteRequest request,
            CursorSnapshotWriteAuthority authority,
            ObjectProtectionOwner expectedOwner) {
        ObjectProtectionOwner canonical = owner(authority.currentRoot());
        if (!canonical.equals(expectedOwner)) {
            return CompletableFuture.failedFuture(invariant(
                    "cursor snapshot pending owner fields changed"));
        }
        return cursorMetadataStore.getCursor(
                        cluster,
                        streamId(request.identity()),
                        request.identity().cursorName())
                .thenAccept(current -> {
                    if (current.isEmpty()
                            || !current.orElseThrow().equals(
                                    authority.currentRoot())) {
                        throw condition(
                                "cursor snapshot write authority changed");
                    }
                    authority.requireMatches(request);
                });
    }

    private CompletableFuture<Void> revalidatePublishedRoot(
            CursorSnapshotPublication publication,
            VersionedCursorState publishedRoot,
            ObjectProtectionOwner expectedOwner) {
        ObjectProtectionOwner canonical = owner(publishedRoot);
        if (!canonical.equals(expectedOwner)) {
            return CompletableFuture.failedFuture(invariant(
                    "cursor snapshot permanent owner fields changed"));
        }
        return cursorMetadataStore.getCursor(
                        cluster,
                        streamId(publication.request().identity()),
                        publication.request().identity().cursorName())
                .thenAccept(current -> {
                    if (current.isEmpty()
                            || !current.orElseThrow().equals(publishedRoot)) {
                        throw condition(
                                "published cursor snapshot root changed");
                    }
                    requirePublishedRoot(publication, publishedRoot);
                });
    }

    private CompletableFuture<Void> revalidateLiveReference(
            CursorSnapshotReference reference,
            CursorIdentity expectedIdentity) {
        return loadLiveReferenceRoot(reference, expectedIdentity)
                .thenApply(ignored -> null);
    }

    private CompletableFuture<Void> revalidateLiveReferenceOwner(
            CursorSnapshotReference reference,
            CursorIdentity expectedIdentity,
            ObjectProtectionOwner expectedOwner) {
        return loadLiveReferenceRoot(reference, expectedIdentity)
                .thenAccept(root -> {
                    if (!owner(root).equals(expectedOwner)) {
                        throw condition(
                                "cursor snapshot read owner changed");
                    }
                });
    }

    private CompletableFuture<VersionedCursorState> loadLiveReferenceRoot(
            CursorSnapshotReference reference,
            CursorIdentity expectedIdentity) {
        return cursorMetadataStore.getCursor(
                        cluster,
                        streamId(expectedIdentity),
                        expectedIdentity.cursorName())
                .thenApply(current -> {
                    if (current.isEmpty()) {
                        throw condition(
                                "cursor snapshot read owner is absent");
                    }
                    VersionedCursorState versioned = current.orElseThrow();
                    var root = versioned.value();
                    if (root.lifecycle() != CursorRecordLifecycle.ACTIVE
                            || !root.projection().equals(
                                    expectedIdentity.ledger().projection())
                            || root.cursorGeneration()
                                    != expectedIdentity.cursorGeneration()
                            || root.snapshotReference().isEmpty()
                            || !root.snapshotReference()
                                    .orElseThrow()
                                    .equals(reference.toMetadataRecord())) {
                        throw condition(
                                "cursor snapshot read selection changed");
                    }
                    return versioned;
                });
    }

    private CompletableFuture<CursorAckState> readUnderLease(
            CursorSnapshotReference reference,
            CursorIdentity expectedIdentity,
            PhysicalObjectIdentity object,
            ObjectReadLease lease,
            Deadline deadline) {
        CompletableFuture<CursorAckState> operation = objectStore.headObject(
                        reference.objectKey(),
                        new HeadObjectOptions(
                                deadline.callTimeout(
                                        objectStoreRequestTimeout)))
                .thenCompose(head -> {
                    verifyHead(
                            head,
                            reference.objectKey(),
                            reference.objectLength(),
                            reference.storageChecksum(),
                            metadata(reference.snapshotId()));
                    if (!physicalObject(reference, head).equals(object)) {
                        return CompletableFuture.failedFuture(invariant(
                                "cursor snapshot HEAD identity changed after read pin"));
                    }
                    return objectStore.readRange(
                            reference.objectKey(),
                            0,
                            reference.objectLength(),
                            new RangeReadOptions(
                                    Optional.of(reference.storageChecksum()),
                                    deadline.callTimeout(
                                            objectStoreRequestTimeout)));
                })
                .thenApply(result -> {
                    verifyRead(result, reference);
                    return CursorSnapshotCodecV1.decode(
                            result.payload(), reference, expectedIdentity, cursorConfig);
                });
        CompletableFuture<CursorAckState> result = new CompletableFuture<>();
        operation.whenComplete((value, failure) -> lease.release()
                .whenComplete((ignored, releaseFailure) -> {
                    if (failure == null && releaseFailure == null) {
                        result.complete(value);
                        return;
                    }
                    Throwable exact = failure == null
                            ? unwrap(releaseFailure)
                            : unwrap(failure);
                    if (releaseFailure != null && failure != null) {
                        exact.addSuppressed(unwrap(releaseFailure));
                    }
                    result.completeExceptionally(exact);
                }));
        return result;
    }

    private CompletableFuture<HeadObjectResult> head(
            CursorSnapshotReference reference,
            Deadline deadline) {
        return objectStore.headObject(
                        reference.objectKey(),
                        new HeadObjectOptions(
                                deadline.callTimeout(
                                        objectStoreRequestTimeout)))
                .thenApply(result -> {
                    verifyHead(
                            result,
                            reference.objectKey(),
                            reference.objectLength(),
                            reference.storageChecksum(),
                            metadata(reference.snapshotId()));
                    return result;
                });
    }

    private CompletableFuture<VersionedPhysicalObjectRoot> loadExactActiveRoot(
            PhysicalObjectIdentity object) {
        return physicalMetadataStore.getRoot(
                        cluster, object.objectKeyHash())
                .thenApply(optional -> {
                    VersionedPhysicalObjectRoot root = optional.orElseThrow(() ->
                            invariant(
                                    "cursor snapshot physical root is absent after protection"));
                    if (root.value().lifecycle()
                                    != PhysicalObjectLifecycle.ACTIVE
                            || !PhysicalObjectIdentity.from(root.value())
                                    .equals(object)) {
                        throw invariant(
                                "cursor snapshot physical root changed after protection");
                    }
                    return root;
                });
    }

    private void validateReferenceKey(
            CursorSnapshotReference reference,
            CursorIdentity expectedIdentity) {
        ObjectKey expected = CursorSnapshotKeys.objectKey(
                cluster, expectedIdentity, reference.snapshotId());
        if (!expected.equals(reference.objectKey())
                || reference.cursorGeneration()
                        != expectedIdentity.cursorGeneration()) {
            throw new CursorSnapshotCodecV1.CursorSnapshotCorruptionException(
                    "cursor snapshot reference key or generation does not match the expected identity");
        }
        if (reference.objectLength()
                > cursorConfig.cursorSnapshotMaxBytes()) {
            throw new CursorSnapshotCodecV1.CursorSnapshotCorruptionException(
                    "cursor snapshot reference exceeds the configured object bound");
        }
    }

    private static void requirePublishedRoot(
            CursorSnapshotPublication publication,
            VersionedCursorState publishedRoot) {
        var request = publication.request();
        var value = publishedRoot.value();
        if (value.lifecycle() != CursorRecordLifecycle.ACTIVE
                || !value.projection().equals(
                        request.identity().ledger().projection())
                || !value.cursorName().equals(
                        request.identity().cursorName())
                || !value.cursorNameHash().equals(
                        request.identity().cursorNameHash())
                || value.cursorGeneration()
                        != request.identity().cursorGeneration()
                || value.mutationSequence()
                        < request.sourceMutationSequence()
                || value.snapshotReference().isEmpty()
                || !value.snapshotReference()
                        .orElseThrow()
                        .equals(publication.reference().toMetadataRecord())) {
            throw new IllegalArgumentException(
                    "published cursor root does not match the prepared snapshot");
        }
    }

    private static void requireCompatiblePreUploadRoot(
            ObjectKey key,
            CursorSnapshotCodecV1.EncodedSnapshot encoded,
            VersionedPhysicalObjectRoot root) {
        var value = root.value();
        if (value.lifecycle() != PhysicalObjectLifecycle.ACTIVE
                || !value.objectKey().equals(key.value())
                || !value.objectKeyHash().equals(
                        ObjectKeyHash.from(key).value())
                || !value.objectId().isEmpty()
                || value.objectKindId()
                        != PhysicalObjectKind.CURSOR_SNAPSHOT.wireId()
                || value.objectLength() != encoded.objectLength()
                || !value.storageChecksumType().equals(
                        encoded.storageChecksum().type().name())
                || !value.storageChecksumValue().equals(
                        encoded.storageChecksum().value())
                || !value.contentSha256().isEmpty()) {
            throw new ObjectAlreadyExistsException(
                    "cursor snapshot ID already belongs to a conflicting physical root");
        }
    }

    private PhysicalObjectIdentity physicalObject(
            CursorSnapshotReference reference,
            HeadObjectResult head) {
        return PhysicalObjectIdentity.create(
                reference.objectKey(),
                Optional.empty(),
                PhysicalObjectKind.CURSOR_SNAPSHOT,
                reference.objectLength(),
                reference.storageChecksum(),
                Optional.empty(),
                head.etag());
    }

    private ObjectProtectionOwner owner(VersionedCursorState root) {
        StreamId stream = new StreamId(root.value().projection().streamId());
        return new ObjectProtectionOwner(
                cursorKeys.cursorStateKey(
                        stream, root.value().cursorName()),
                root.metadataVersion(),
                CursorMetadataDigests.durableValueSha256(root.value()));
    }

    private static StreamId streamId(CursorIdentity identity) {
        return new StreamId(identity.ledger().projection().streamId());
    }

    private long maximumReadDeadline() {
        try {
            return Math.addExact(
                    nonNegativeNow(),
                    cursorConfig.cursorSnapshotOperationTimeout().toMillis());
        } catch (ArithmeticException overflow) {
            throw new NereusException(
                    ErrorCode.METADATA_LIMIT_EXCEEDED,
                    false,
                    "cursor snapshot read deadline overflows");
        }
    }

    private long nonNegativeNow() {
        long value = clock.millis();
        if (value < 0) {
            throw new IllegalStateException(
                    "cursor snapshot clock returned a negative epoch millisecond");
        }
        return value;
    }

    private static String keySnapshotId(ObjectKey key) {
        String value = key.value();
        int slash = value.lastIndexOf('/');
        if (slash < 0 || !value.endsWith(".ncs")) {
            throw invariant("cursor snapshot object key is not canonical");
        }
        return value.substring(slash + 1, value.length() - 4);
    }

    private static void verifyPut(
            PutObjectResult result,
            ObjectKey key,
            long length,
            com.nereusstream.api.Checksum checksum) {
        if (!result.key().equals(key)
                || result.objectLength() != length
                || !result.checksum().equals(checksum)) {
            throw objectFailure(
                    ErrorCode.OBJECT_UPLOAD_FAILED,
                    "cursor snapshot PUT result mismatch");
        }
    }

    private static void verifyHead(
            HeadObjectResult result,
            ObjectKey key,
            long length,
            com.nereusstream.api.Checksum checksum,
            Map<String, String> metadata) {
        if (!result.key().equals(key)
                || result.objectLength() != length
                || !result.checksum().equals(checksum)
                || !result.metadata().equals(metadata)) {
            throw objectFailure(
                    ErrorCode.OBJECT_CHECKSUM_MISMATCH,
                    "cursor snapshot HEAD result mismatch");
        }
    }

    private static void verifyRead(
            RangeReadResult result,
            CursorSnapshotReference reference) {
        if (!result.key().equals(reference.objectKey())
                || result.offset() != 0
                || result.length() != reference.objectLength()
                || result.payload().remaining()
                        != reference.objectLength()
                || result.checksum().isEmpty()
                || !result.checksum()
                        .orElseThrow()
                        .equals(reference.storageChecksum())) {
            throw objectFailure(
                    ErrorCode.OBJECT_CHECKSUM_MISMATCH,
                    "cursor snapshot range result mismatch");
        }
    }

    private static Map<String, String> metadata(String snapshotId) {
        return Map.of(
                "nereus-format",
                METADATA_BASE.get("nereus-format"),
                "nereus-object-type",
                METADATA_BASE.get("nereus-object-type"),
                "nereus-snapshot-id",
                snapshotId);
    }

    private static boolean isIfAbsentCollision(Throwable error) {
        return error instanceof ObjectAlreadyExistsException;
    }

    private static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while ((current instanceof CompletionException
                        || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static NereusException objectFailure(
            ErrorCode code,
            String message) {
        return new NereusException(code, false, message);
    }

    private static NereusException invariant(String message) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static NereusException condition(String message) {
        return new NereusException(
                ErrorCode.METADATA_CONDITION_FAILED, true, message);
    }

    private static <T> CompletableFuture<T> closedFuture() {
        return NereusException.failedFuture(
                ErrorCode.STORAGE_CLOSED,
                false,
                "cursor snapshot store is closed");
    }

    private static Duration requirePositive(
            Duration value,
            String fieldName) {
        Objects.requireNonNull(value, fieldName);
        if (value.isZero()
                || value.isNegative()
                || value.toMillis() <= 0
                || !value.equals(Duration.ofMillis(value.toMillis()))) {
            throw new IllegalArgumentException(
                    fieldName
                            + " must be positive and exactly millisecond-representable");
        }
        return value;
    }

    private static Supplier<String> secureRandomIdSupplier() {
        SecureRandom random = new SecureRandom();
        return () -> {
            byte[] bytes = new byte[16];
            random.nextBytes(bytes);
            return HexFormat.of().formatHex(bytes);
        };
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

        static Deadline start(
                Duration timeout,
                LongSupplier nanoTime) {
            long now = nanoTime.getAsLong();
            long timeoutNanos;
            try {
                timeoutNanos = timeout.toNanos();
            } catch (ArithmeticException ignored) {
                timeoutNanos = Long.MAX_VALUE;
            }
            long deadline;
            try {
                deadline = Math.addExact(now, timeoutNanos);
            } catch (ArithmeticException ignored) {
                deadline = Long.MAX_VALUE;
            }
            return new Deadline(deadline, nanoTime);
        }

        Duration callTimeout(Duration perCallTimeout) {
            long remaining = deadlineNanos - nanoTime.getAsLong();
            if (remaining <= 0) {
                throw new NereusException(
                        ErrorCode.TIMEOUT,
                        true,
                        "cursor snapshot operation deadline expired",
                        new TimeoutException());
            }
            long perCall;
            try {
                perCall = perCallTimeout.toNanos();
            } catch (ArithmeticException ignored) {
                perCall = Long.MAX_VALUE;
            }
            return Duration.ofNanos(Math.min(remaining, perCall));
        }
    }
}
