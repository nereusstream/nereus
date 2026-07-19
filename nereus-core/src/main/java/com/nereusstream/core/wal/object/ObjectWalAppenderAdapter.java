/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.wal.object;
import com.nereusstream.api.AppendSession;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.api.target.ReadTargetType;
import com.nereusstream.core.wal.DurablePrimaryAppend;
import com.nereusstream.core.wal.PrimaryAppendRequest;
import com.nereusstream.core.wal.PrimaryWalAppender;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.objectstore.PutObjectAttemptGuard;
import com.nereusstream.objectstore.wal.CompressionType;
import com.nereusstream.objectstore.wal.PreparedWalObject;
import com.nereusstream.objectstore.wal.WalObjectWriter;
import com.nereusstream.objectstore.wal.WalStreamSliceInput;
import com.nereusstream.objectstore.wal.WalWriteOptions;
import com.nereusstream.objectstore.wal.WalWriteRequest;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Adapter preserving Object WAL v1 bytes while exposing the provider-neutral append seam. */
public final class ObjectWalAppenderAdapter implements PrimaryWalAppender<ObjectPreparedPrimaryAppend> {
    private final String cluster;
    private final String writerId;
    private final String writerRunIdHash;
    private final int maxObjectBytes;
    private final WalObjectWriter writer;
    private final ObjectWalUploadAuthorizer uploadAuthorizer;
    private final ObjectWalManifestPublisher manifestPublisher;
    public ObjectWalAppenderAdapter(String cluster, String writerId, String writerRunIdHash,
            int maxObjectBytes, WalObjectWriter writer) {
        this(cluster, writerId, writerRunIdHash, maxObjectBytes, writer,
                ObjectWalUploadAuthorizer.noOp(), ObjectWalManifestPublisher.noOp());
    }
    public ObjectWalAppenderAdapter(String cluster, String writerId, String writerRunIdHash,
            int maxObjectBytes, WalObjectWriter writer,
            ObjectWalUploadAuthorizer uploadAuthorizer,
            ObjectWalManifestPublisher manifestPublisher) {
        this.cluster = Objects.requireNonNull(cluster); this.writerId = Objects.requireNonNull(writerId);
        this.writerRunIdHash = Objects.requireNonNull(writerRunIdHash); this.maxObjectBytes = maxObjectBytes;
        this.writer = Objects.requireNonNull(writer);
        this.uploadAuthorizer = Objects.requireNonNull(uploadAuthorizer);
        this.manifestPublisher = Objects.requireNonNull(manifestPublisher);
    }
    @Override public ReadTargetType targetType() { return ReadTargetType.OBJECT_SLICE; }
    @Override public Class<ObjectPreparedPrimaryAppend> preparedClass() { return ObjectPreparedPrimaryAppend.class; }
    @Override public ObjectPreparedPrimaryAppend prepare(PrimaryAppendRequest request) {
        PreparedWalObject prepared = writer.prepare(new WalWriteRequest(cluster, writerId, writerRunIdHash,
                request.session().epoch(), List.of(new WalStreamSliceInput(request.streamId(), request.batch())),
                new WalWriteOptions(CompressionType.NONE, maxObjectBytes, maxObjectBytes, request.timeout(), true)));
        var slice = prepared.result().slices().get(0);
        return new ObjectPreparedPrimaryAppend(request.streamId(), slice.recordCount(), slice.entryCount(),
                slice.logicalBytes(), prepared.objectLength(), prepared, request);
    }
    @Override public CompletableFuture<DurablePrimaryAppend> persist(
            ObjectPreparedPrimaryAppend prepared, Duration timeout) {
        return persist(
                prepared,
                timeout,
                (key, attempt) -> uploadAuthorizer.authorize(
                        prepared,
                        physicalObject(prepared.preparedObject().result()),
                        key,
                        attempt,
                        timeout));
    }
    @Override public CompletableFuture<Void> publishDurableMetadata(
            DurablePrimaryAppend append, AppendSession session, Duration timeout) {
        if (!(append.providerToken() instanceof ObjectWalCommitEvidence evidence))
            return CompletableFuture.failedFuture(new IllegalArgumentException("wrong provider evidence"));
        return manifestPublisher.publish(evidence.writeResult(), session, timeout);
    }
    public CompletableFuture<DurablePrimaryAppend> persist(
            ObjectPreparedPrimaryAppend prepared,
            Duration timeout,
            PutObjectAttemptGuard attemptGuard) {
        Objects.requireNonNull(timeout, "timeout");
        return writer.upload(prepared.preparedObject(), attemptGuard).thenApply(result -> {
            var slice = result.slices().get(0);
            ObjectSliceReadTarget target = new ObjectSliceReadTarget(1, result.objectId(), result.objectKey(),
                    ObjectType.MULTI_STREAM_WAL_OBJECT, "WAL_OBJECT_V1", "OPAQUE_SLICE", slice.sliceId(),
                    slice.objectOffset(), slice.objectLength(), slice.sliceChecksum(), slice.entryIndexRef());
            return new DurablePrimaryAppend(prepared.streamId(), target,
                    ObjectPrimaryPhysicalIdentity.from(target), slice.sliceChecksum(),
                    slice.payloadFormat(), slice.recordCount(),
                    slice.entryCount(), slice.logicalBytes(), slice.schemaRefs(), slice.minEventTimeMillis(),
                    slice.maxEventTimeMillis(), new ObjectWalCommitEvidence(result));
        });
    }
    @Override public CompletableFuture<Void> validateBeforeHeadCommit(
            DurablePrimaryAppend append, AppendSession session, Duration timeout) {
        if (!(append.providerToken() instanceof ObjectWalCommitEvidence))
            return CompletableFuture.failedFuture(new IllegalArgumentException("wrong provider evidence"));
        return CompletableFuture.completedFuture(null);
    }
    private static PhysicalObjectIdentity physicalObject(com.nereusstream.objectstore.wal.WalWriteResult result) {
        return PhysicalObjectIdentity.create(
                result.objectKey(), Optional.of(result.objectId()), PhysicalObjectKind.OBJECT_WAL,
                result.objectLength(), result.storageChecksum(), Optional.empty(), Optional.empty());
    }
}
