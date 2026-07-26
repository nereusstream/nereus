/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.admin;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.ObjectKey;
import com.nereusstream.objectstore.DeleteObjectOptions;
import com.nereusstream.objectstore.DeleteObjectResult;
import com.nereusstream.objectstore.HeadObjectOptions;
import com.nereusstream.objectstore.HeadObjectResult;
import com.nereusstream.objectstore.ListObjectsOptions;
import com.nereusstream.objectstore.ListObjectsResult;
import com.nereusstream.objectstore.ObjectKeyPrefix;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.objectstore.PutObjectResult;
import com.nereusstream.objectstore.RangeReadOptions;
import com.nereusstream.objectstore.RangeReadResult;
import com.nereusstream.objectstore.ReplayableObjectUpload;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ObjectStoreContractCommandTest {

    @Test
    void exercisesTheCompleteContractAndCleansItsPrefix(
            @TempDir Path tempDir) throws Exception {
        Path output = tempDir.resolve("contract.json");
        try (ObjectStore store = new MetadataPreservingObjectStore(
                new LocalFileObjectStore(tempDir.resolve("objects")))) {
            AdminExitCode result = ObjectStoreContractCommand.contract(
                    store,
                    "run-1",
                    Duration.ofSeconds(30),
                    Optional.of(output));

            assertThat(result).isEqualTo(AdminExitCode.SUCCESS);
            String evidence = Files.readString(output);
            assertThat(evidence.getBytes(StandardCharsets.UTF_8).length)
                    .isLessThan(4096);
            assertThat(evidence)
                    .contains("\"headBucketSuccess\": true")
                    .contains("\"conditionalCreateSingleWinner\": true")
                    .contains("\"concurrentConditionalCreateSingleWinner\": true")
                    .contains("\"listPaginationObserved\": true")
                    .contains("\"wrongEtagDeletePreservesObject\": true")
                    .contains("\"postDeleteHeadNotFound\": true")
                    .contains("\"contractPrefixCleanedUp\": true")
                    .contains("\"overallSuccess\": true");
        }
    }

    @Test
    void preservesAndVerifiesExactObjectAcrossProviderRestart(
            @TempDir Path tempDir) throws Exception {
        String runId = "abcdefghijklmnopqrstuvwxyz";
        Path objectRoot = tempDir.resolve("objects");
        Path createEvidence = tempDir.resolve("create.json");
        Map<ObjectKey, Map<String, String>> durableMetadata =
                new ConcurrentHashMap<>();
        try (ObjectStore store = new MetadataPreservingObjectStore(
                new LocalFileObjectStore(objectRoot), durableMetadata)) {
            assertThat(ObjectStoreContractCommand.persistenceCreate(
                    store,
                    runId,
                    Duration.ofSeconds(30),
                    Optional.of(createEvidence)))
                    .isEqualTo(AdminExitCode.SUCCESS);
        }

        Path verifyEvidence = tempDir.resolve("verify.json");
        Path cleanupEvidence = tempDir.resolve("cleanup.json");
        try (ObjectStore store = new MetadataPreservingObjectStore(
                new LocalFileObjectStore(objectRoot), durableMetadata)) {
            assertThat(ObjectStoreContractCommand.persistenceVerify(
                    store,
                    runId,
                    Duration.ofSeconds(30),
                    Optional.of(verifyEvidence)))
                    .isEqualTo(AdminExitCode.SUCCESS);
            assertThat(ObjectStoreContractCommand.persistenceCleanup(
                    store,
                    runId,
                    Duration.ofSeconds(30),
                    Optional.of(cleanupEvidence)))
                    .isEqualTo(AdminExitCode.SUCCESS);
        }

        assertThat(Files.readString(createEvidence))
                .contains("\"conditionalCreateSucceeded\": true")
                .contains("\"createdIdentityExact\": true")
                .contains("\"overallSuccess\": true");
        assertThat(Files.readString(verifyEvidence))
                .contains("\"restartPersistenceVerified\": true")
                .contains("\"overallSuccess\": true");
        assertThat(Files.readString(cleanupEvidence))
                .contains("\"conditionalDeleteSucceeded\": true")
                .contains("\"postDeleteHeadNotFound\": true")
                .contains("\"overallSuccess\": true");
    }

    @Test
    void returnsStableTimeoutExitCodeForObjectOperations(
            @TempDir Path tempDir) throws Exception {
        Path contractEvidence = tempDir.resolve("timeout-contract.json");
        try (ObjectStore store = new TimeoutObjectStore()) {
            assertThat(ObjectStoreContractCommand.persistenceCreate(
                    store,
                    "abcdefghijklmnopqrstuvwxyz",
                    Duration.ofSeconds(30),
                    Optional.empty()))
                    .isEqualTo(AdminExitCode.TIMEOUT);
            assertThat(ObjectStoreContractCommand.contract(
                    store,
                    "timeout-run",
                    Duration.ofSeconds(30),
                    Optional.of(contractEvidence)))
                    .isEqualTo(AdminExitCode.TIMEOUT);
        }

        assertThat(Files.readString(contractEvidence))
                .contains("\"timeoutObserved\": true")
                .contains("\"overallSuccess\": false");
    }

    @Test
    void stopsContractAfterFirstOperationTimeout() {
        long started = System.nanoTime();
        try (ObjectStore store = new NeverCompletingPutObjectStore()) {
            assertThat(ObjectStoreContractCommand.contract(
                    store,
                    "timeout-run",
                    Duration.ofSeconds(1),
                    Optional.empty()))
                    .isEqualTo(AdminExitCode.TIMEOUT);
        }

        assertThat(Duration.ofNanos(System.nanoTime() - started))
                .isLessThan(Duration.ofSeconds(5));
    }

    private static class TimeoutObjectStore implements ObjectStore {

        private static <T> CompletableFuture<T> timeout() {
            return CompletableFuture.failedFuture(
                    new TimeoutException("forced timeout"));
        }

        @Override
        public CompletableFuture<PutObjectResult> putObject(
                ObjectKey key,
                ReplayableObjectUpload source,
                PutObjectOptions options) {
            return timeout();
        }

        @Override
        public CompletableFuture<RangeReadResult> readRange(
                ObjectKey key,
                long offset,
                long length,
                RangeReadOptions options) {
            return timeout();
        }

        @Override
        public CompletableFuture<HeadObjectResult> headObject(
                ObjectKey key,
                HeadObjectOptions options) {
            return timeout();
        }

        @Override
        public CompletableFuture<ListObjectsResult> listObjects(
                ObjectKeyPrefix prefix,
                Optional<String> continuationToken,
                ListObjectsOptions options) {
            return timeout();
        }

        @Override
        public CompletableFuture<DeleteObjectResult> deleteObject(
                ObjectKey key,
                DeleteObjectOptions options) {
            return timeout();
        }

        @Override
        public void close() {
        }
    }

    private static final class NeverCompletingPutObjectStore
            extends TimeoutObjectStore {

        @Override
        public CompletableFuture<PutObjectResult> putObject(
                ObjectKey key,
                ReplayableObjectUpload source,
                PutObjectOptions options) {
            return new CompletableFuture<>();
        }
    }

    private static final class MetadataPreservingObjectStore
            implements ObjectStore {
        private final ObjectStore delegate;
        private final Map<ObjectKey, Map<String, String>> metadata;

        private MetadataPreservingObjectStore(ObjectStore delegate) {
            this(delegate, new ConcurrentHashMap<>());
        }

        private MetadataPreservingObjectStore(
                ObjectStore delegate,
                Map<ObjectKey, Map<String, String>> metadata) {
            this.delegate = delegate;
            this.metadata = metadata;
        }

        @Override
        public CompletableFuture<PutObjectResult> putObject(
                ObjectKey key,
                ReplayableObjectUpload source,
                PutObjectOptions options) {
            return delegate.putObject(key, source, options)
                    .thenApply(result -> {
                        metadata.put(key, options.metadata());
                        return result;
                    });
        }

        @Override
        public CompletableFuture<RangeReadResult> readRange(
                ObjectKey key,
                long offset,
                long length,
                RangeReadOptions options) {
            return delegate.readRange(key, offset, length, options);
        }

        @Override
        public CompletableFuture<HeadObjectResult> headObject(
                ObjectKey key,
                HeadObjectOptions options) {
            return delegate.headObject(key, options)
                    .thenApply(head -> new HeadObjectResult(
                            head.key(),
                            head.objectLength(),
                            head.checksum(),
                            head.etag(),
                            metadata.getOrDefault(key, Map.of())));
        }

        @Override
        public CompletableFuture<ListObjectsResult> listObjects(
                ObjectKeyPrefix prefix,
                Optional<String> continuationToken,
                ListObjectsOptions options) {
            return delegate.listObjects(prefix, continuationToken, options);
        }

        @Override
        public CompletableFuture<DeleteObjectResult> deleteObject(
                ObjectKey key,
                DeleteObjectOptions options) {
            return delegate.deleteObject(key, options)
                    .thenApply(result -> {
                        if (result.status() == DeleteObjectResult.Status.DELETED
                                || result.status()
                                        == DeleteObjectResult.Status.ALREADY_ABSENT) {
                            metadata.remove(key);
                        }
                        return result;
                    });
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
