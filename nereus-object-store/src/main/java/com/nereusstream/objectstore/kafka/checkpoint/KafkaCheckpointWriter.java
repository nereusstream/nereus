/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.kafka.checkpoint;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.objectstore.PutObjectResult;
import com.nereusstream.objectstore.staging.StagingFileManager;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/** Private-staging immutable NKC1 writer with exact response-loss reconciliation. */
public final class KafkaCheckpointWriter {
    @FunctionalInterface
    public interface PreUploadGuard {
        CompletableFuture<Void> authorize(KafkaCheckpointUploadIdentity object);
    }

    private final ObjectStore objectStore;
    private final StagingFileManager stagingFiles;
    private final Executor codecExecutor;
    private final KafkaCheckpointCodecV1 codec;
    private final KafkaCheckpointReader reader;
    private final KafkaCheckpointVerifier verifier;

    public KafkaCheckpointWriter(
            ObjectStore objectStore,
            StagingFileManager stagingFiles,
            Executor codecExecutor,
            KafkaCheckpointCodecV1 codec,
            KafkaCheckpointReader reader,
            KafkaCheckpointVerifier verifier) {
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
        this.stagingFiles = Objects.requireNonNull(stagingFiles, "stagingFiles");
        this.codecExecutor = Objects.requireNonNull(codecExecutor, "codecExecutor");
        this.codec = Objects.requireNonNull(codec, "codec");
        this.reader = Objects.requireNonNull(reader, "reader");
        this.verifier = Objects.requireNonNull(verifier, "verifier");
    }

    public CompletableFuture<KafkaCheckpointObject> write(KafkaCheckpointWriteRequest request) {
        return write(request, ignored -> CompletableFuture.completedFuture(null));
    }

    public CompletableFuture<KafkaCheckpointObject> write(
            KafkaCheckpointWriteRequest request, PreUploadGuard preUploadGuard) {
        if (request == null) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.INVALID_ARGUMENT, false, "Kafka checkpoint write request is required"));
        }
        Objects.requireNonNull(preUploadGuard, "preUploadGuard");
        return CompletableFuture.supplyAsync(
                        () -> codec.encodeToStaging(stagingFiles, request.header(), request.sections()),
                        codecExecutor)
                .thenCompose(encoded -> uploadAndVerify(request, encoded, preUploadGuard)
                        .whenComplete((ignored, failure) -> encoded.close()))
                .handle((value, failure) -> {
                    if (failure == null) return value;
                    Throwable cause = unwrap(failure);
                    if (cause instanceof NereusException nereus) throw new CompletionException(nereus);
                    throw new CompletionException(new NereusException(
                            ErrorCode.OBJECT_UPLOAD_FAILED, true, "failed to publish NKC1 object", cause));
                });
    }

    private CompletableFuture<KafkaCheckpointObject> uploadAndVerify(
            KafkaCheckpointWriteRequest request,
            EncodedKafkaCheckpoint encoded,
            PreUploadGuard preUploadGuard) {
        ObjectKey key = KafkaCheckpointFormatV1.objectKey(
                request.nereusCluster(), request.header(), request.contentPolicySha256());
        String attempt = KafkaCheckpointFormatV1.attemptId(request.header(), request.contentPolicySha256());
        PutObjectOptions options = new PutObjectOptions(
                KafkaCheckpointFormatV1.CONTENT_TYPE,
                encoded.storageCrc32c(),
                true,
                Map.of(
                        "nereus.format", "NKC1",
                        "nereus.object.sha256", encoded.objectSha256().value(),
                        "nereus.checkpoint.attempt", attempt),
                request.timeout());
        KafkaCheckpointUploadIdentity physical = new KafkaCheckpointUploadIdentity(
                KafkaCheckpointFormatV1.objectId(key), key,
                encoded.objectLength(),
                encoded.storageCrc32c(),
                encoded.objectSha256());
        CompletableFuture<KafkaCheckpointObject> primary = preUploadGuard.authorize(physical)
                .thenCompose(ignored -> objectStore.putObject(key, encoded.stagingFile(), options))
                .thenCompose(result -> {
                    validatePut(result, key, encoded);
                    return openExpected(request, encoded, key);
                });
        return primary.exceptionallyCompose(original -> openExpected(request, encoded, key)
                .handle((reconciled, recoveryFailure) -> {
                    if (recoveryFailure == null) return reconciled;
                    throw new CompletionException(unwrap(original));
                }));
    }

    private CompletableFuture<KafkaCheckpointObject> openExpected(
            KafkaCheckpointWriteRequest request,
            EncodedKafkaCheckpoint encoded,
            ObjectKey key) {
        return reader.openAndVerify(
                        key, encoded.objectLength(), encoded.storageCrc32c(),
                        encoded.objectSha256(), request.timeout())
                .thenApply(object -> {
                    verifier.verifyExpected(
                            object, request.nereusCluster(), request.header(), request.contentPolicySha256());
                    return object;
                });
    }

    private static void validatePut(
            PutObjectResult result, ObjectKey key, EncodedKafkaCheckpoint encoded) {
        if (!result.key().equals(key)
                || result.objectLength() != encoded.objectLength()
                || !result.checksum().equals(encoded.storageCrc32c())) {
            throw new NereusException(
                    ErrorCode.OBJECT_UPLOAD_FAILED, false, "NKC1 PUT result identity mismatch");
        }
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) current = current.getCause();
        return current;
    }
}
