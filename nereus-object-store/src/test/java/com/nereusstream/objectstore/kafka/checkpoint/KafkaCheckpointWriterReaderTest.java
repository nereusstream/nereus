/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.kafka.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.objectstore.HeadObjectOptions;
import com.nereusstream.objectstore.HeadObjectResult;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.objectstore.PutObjectResult;
import com.nereusstream.objectstore.RangeReadOptions;
import com.nereusstream.objectstore.RangeReadResult;
import com.nereusstream.objectstore.ReplayableObjectUpload;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import com.nereusstream.objectstore.staging.StagingFileManager;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KafkaCheckpointWriterReaderTest {
    @TempDir Path temporaryDirectory;

    @Test
    void immutablePutAndAlreadyExistsResponseConvergeToExactObject() throws Exception {
        Path objects = Files.createDirectory(temporaryDirectory.resolve("objects"));
        Path stagingDirectory = Files.createDirectory(temporaryDirectory.resolve("staging"));
        Files.setPosixFilePermissions(stagingDirectory, PosixFilePermissions.fromString("rwx------"));
        try (LocalFileObjectStore store = new LocalFileObjectStore(objects);
             StagingFileManager staging = new StagingFileManager(
                     stagingDirectory, 32L << 20, StagingFileManager.MIN_UPLOAD_CHUNK_BYTES,
                     Duration.ofHours(1), Runnable::run)) {
            KafkaCheckpointCodecV1 codec = new KafkaCheckpointCodecV1();
            KafkaCheckpointReader reader = new KafkaCheckpointReader(store, codec);
            KafkaCheckpointVerifier verifier = new KafkaCheckpointVerifier();
            KafkaCheckpointWriter writer = new KafkaCheckpointWriter(
                    store, staging, Runnable::run, codec, reader, verifier);
            KafkaCheckpointWriteRequest request = new KafkaCheckpointWriteRequest(
                    "nereus", KafkaCheckpointCodecV1Test.header(0),
                    KafkaCheckpointCodecV1Test.sections(), KafkaCheckpointCodecV1Test.sha256('b'),
                    Duration.ofSeconds(5));

            KafkaCheckpointObject first = writer.write(request).join();
            KafkaCheckpointObject replayed = writer.write(request).join();

            assertThat(replayed.objectKey()).isEqualTo(first.objectKey());
            assertThat(replayed.objectSha256()).isEqualTo(first.objectSha256());
            assertThat(replayed.sections()).isEqualTo(request.sections());
            verifier.verifyExpected(replayed, "nereus", request.header(), request.contentPolicySha256());
            verifier.verifyRecoveryWindow(
                    replayed, "kraft-cluster", request.header().topicId(), 3, 1, "stream-1", 1, 5, 42);
        }
    }

    @Test
    void reconcilesResponseLossAfterImmutablePut() throws Exception {
        Path objects = Files.createDirectory(temporaryDirectory.resolve("response-loss-objects"));
        try (LocalFileObjectStore durable = new LocalFileObjectStore(objects);
             ResponseLossObjectStore store = new ResponseLossObjectStore(durable);
             StagingFileManager staging = staging("response-loss-staging")) {
            KafkaCheckpointCodecV1 codec = new KafkaCheckpointCodecV1();
            KafkaCheckpointReader reader = new KafkaCheckpointReader(store, codec);
            KafkaCheckpointWriter writer = new KafkaCheckpointWriter(
                    store, staging, Runnable::run, codec, reader, new KafkaCheckpointVerifier());

            KafkaCheckpointObject object = writer.write(request()).join();

            assertThat(store.lostResponse()).isTrue();
            assertThat(object.sections()).isEqualTo(request().sections());
        }
    }

    @Test
    void neverReconcilesPastAFailedPreUploadGuard() throws Exception {
        Path objects = Files.createDirectory(temporaryDirectory.resolve("guard-objects"));
        try (LocalFileObjectStore store = new LocalFileObjectStore(objects);
             StagingFileManager staging = staging("guard-staging")) {
            KafkaCheckpointCodecV1 codec = new KafkaCheckpointCodecV1();
            KafkaCheckpointReader reader = new KafkaCheckpointReader(store, codec);
            KafkaCheckpointWriter writer = new KafkaCheckpointWriter(
                    store, staging, Runnable::run, codec, reader, new KafkaCheckpointVerifier());
            writer.write(request()).join();

            assertThatThrownBy(() -> writer.write(request(), ignored -> CompletableFuture.failedFuture(
                            new NereusException(ErrorCode.METADATA_CONDITION_FAILED, true, "guard failed")))
                    .join())
                    .satisfies(failure -> assertThat(unwrap(failure))
                            .isInstanceOfSatisfying(NereusException.class, nereus ->
                                    assertThat(nereus.code()).isEqualTo(ErrorCode.METADATA_CONDITION_FAILED)));
        }
    }

    private KafkaCheckpointWriteRequest request() {
        return new KafkaCheckpointWriteRequest(
                "nereus", KafkaCheckpointCodecV1Test.header(0),
                KafkaCheckpointCodecV1Test.sections(), KafkaCheckpointCodecV1Test.sha256('b'),
                Duration.ofSeconds(5));
    }

    private StagingFileManager staging(String name) throws Exception {
        Path stagingDirectory = Files.createDirectory(temporaryDirectory.resolve(name));
        Files.setPosixFilePermissions(stagingDirectory, PosixFilePermissions.fromString("rwx------"));
        return new StagingFileManager(
                stagingDirectory, 32L << 20, StagingFileManager.MIN_UPLOAD_CHUNK_BYTES,
                Duration.ofHours(1), Runnable::run);
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current.getCause() != null && current instanceof java.util.concurrent.CompletionException) {
            current = current.getCause();
        }
        return current;
    }

    private static final class ResponseLossObjectStore implements ObjectStore {
        private final ObjectStore delegate;
        private boolean lostResponse;

        private ResponseLossObjectStore(ObjectStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletableFuture<PutObjectResult> putObject(
                ObjectKey key, ReplayableObjectUpload source, PutObjectOptions options) {
            return delegate.putObject(key, source, options).thenCompose(ignored -> {
                lostResponse = true;
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.OBJECT_UPLOAD_FAILED, true, "simulated response loss"));
            });
        }

        @Override
        public CompletableFuture<RangeReadResult> readRange(
                ObjectKey key, long offset, long length, RangeReadOptions options) {
            return delegate.readRange(key, offset, length, options);
        }

        @Override
        public CompletableFuture<HeadObjectResult> headObject(ObjectKey key, HeadObjectOptions options) {
            return delegate.headObject(key, options);
        }

        boolean lostResponse() {
            return lostResponse;
        }

        @Override
        public void close() { }
    }
}
