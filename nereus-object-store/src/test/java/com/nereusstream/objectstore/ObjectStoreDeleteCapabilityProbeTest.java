/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ObjectStoreDeleteCapabilityProbeTest {
    private static final String RUN_A = "aaaaaaaaaaaaaaaaaaaaaaaaaa";
    private static final String RUN_B = "bbbbbbbbbbbbbbbbbbbbbbbbbb";
    private static final Duration TIMEOUT = Duration.ofSeconds(2);

    @TempDir
    Path temporary;

    @Test
    void provesExactLifecycleAndProducesStableScopeIdentity() {
        ObjectStoreConfiguration configuration = configuration("bucket-a", "cluster-a");
        try (LocalFileObjectStore store = new LocalFileObjectStore(temporary.resolve("objects"))) {
            var probe = new DefaultObjectStoreDeleteCapabilityProbe(
                    store, configuration, Clock.systemUTC());

            ObjectStoreDeleteCapabilityProof first = probe.probe(
                    new ObjectStoreDeleteCapabilityRequest(RUN_A, TIMEOUT)).join();
            ObjectStoreDeleteCapabilityProof second = probe.probe(
                    new ObjectStoreDeleteCapabilityRequest(RUN_B, TIMEOUT)).join();

            assertThat(first.protocolVersion()).isEqualTo(1);
            assertThat(first.capabilitySha256())
                    .isEqualTo(probe.expectedCapabilitySha256())
                    .isEqualTo(second.capabilitySha256());
            assertThat(first.probeObjectKeySha256())
                    .hasSize(64)
                    .isNotEqualTo(second.probeObjectKeySha256());
            assertThat(first.completedAtMillis()).isPositive();
            assertThat(store.listObjects(
                            new ObjectKeyPrefix("__nereus_capability__/delete-v1/"),
                            Optional.empty(),
                            new ListObjectsOptions(10, TIMEOUT))
                    .join()
                    .objects())
                    .isEmpty();
        }
    }

    @Test
    void capabilityIdentityChangesWithThePhysicalObjectScope() {
        try (LocalFileObjectStore firstStore = new LocalFileObjectStore(temporary.resolve("first"));
                LocalFileObjectStore secondStore = new LocalFileObjectStore(temporary.resolve("second"))) {
            var first = new DefaultObjectStoreDeleteCapabilityProbe(
                    firstStore, configuration("bucket-a", "cluster-a"), Clock.systemUTC());
            var same = new DefaultObjectStoreDeleteCapabilityProbe(
                    secondStore, configuration("bucket-a", "cluster-a"), Clock.systemUTC());
            var changed = new DefaultObjectStoreDeleteCapabilityProbe(
                    secondStore, configuration("bucket-b", "cluster-a"), Clock.systemUTC());

            assertThat(first.expectedCapabilitySha256())
                    .isEqualTo(same.expectedCapabilitySha256())
                    .isNotEqualTo(changed.expectedCapabilitySha256());
        }
    }

    @Test
    void recoversLostPutAndDeleteResponsesThroughExactFacts() {
        try (LocalFileObjectStore delegate = new LocalFileObjectStore(temporary.resolve("loss"))) {
            LostResponseStore store = new LostResponseStore(delegate);
            var probe = new DefaultObjectStoreDeleteCapabilityProbe(
                    store, configuration("bucket-a", "cluster-a"), Clock.systemUTC());

            assertThat(probe.probe(
                            new ObjectStoreDeleteCapabilityRequest(RUN_A, TIMEOUT))
                    .join()
                    .capabilitySha256())
                    .isEqualTo(probe.expectedCapabilitySha256());
            assertThat(store.lostPut.get()).isTrue();
            assertThat(store.lostDelete.get()).isTrue();
        }
    }

    @Test
    void incompleteListingFailsClosedAndCleansTheExactCanary() {
        try (LocalFileObjectStore delegate = new LocalFileObjectStore(temporary.resolve("missing-list"))) {
            ObjectStore store = new DelegatingObjectStore(delegate) {
                @Override
                public CompletableFuture<ListObjectsResult> listObjects(
                        ObjectKeyPrefix prefix,
                        Optional<String> continuationToken,
                        ListObjectsOptions options) {
                    return CompletableFuture.completedFuture(
                            new ListObjectsResult(prefix, java.util.List.of(), Optional.empty()));
                }
            };
            var probe = new DefaultObjectStoreDeleteCapabilityProbe(
                    store, configuration("bucket-a", "cluster-a"), Clock.systemUTC());

            assertThatThrownBy(() -> probe.probe(
                            new ObjectStoreDeleteCapabilityRequest(RUN_A, TIMEOUT))
                    .join())
                    .satisfies(error -> assertThat(unwrap(error).code())
                            .isEqualTo(ErrorCode.OBJECT_CHECKSUM_MISMATCH));
            assertThatThrownBy(() -> delegate.headObject(
                            new ObjectKey("__nereus_capability__/delete-v1/" + RUN_A + "/probe"),
                            new HeadObjectOptions(TIMEOUT))
                    .join())
                    .satisfies(error -> assertThat(unwrap(error).code())
                            .isEqualTo(ErrorCode.OBJECT_NOT_FOUND));
        }
    }

    @Test
    void rejectsAmbiguousRequestsAndProofs() {
        assertThatThrownBy(() -> new ObjectStoreDeleteCapabilityRequest("not-base32", TIMEOUT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ObjectStoreDeleteCapabilityRequest(RUN_A, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ObjectStoreDeleteCapabilityProof(1, "0".repeat(64), "x", 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static ObjectStoreConfiguration configuration(String bucket, String prefix) {
        return new ObjectStoreConfiguration(
                "example.objectstore.Provider",
                URI.create("http://127.0.0.1:4566"),
                "us-east-1",
                bucket,
                prefix,
                true,
                Duration.ofSeconds(5),
                8,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    private static NereusException unwrap(Throwable supplied) {
        Throwable failure = supplied;
        while ((failure instanceof java.util.concurrent.CompletionException
                        || failure instanceof java.util.concurrent.ExecutionException)
                && failure.getCause() != null) {
            failure = failure.getCause();
        }
        assertThat(failure).isInstanceOf(NereusException.class);
        return (NereusException) failure;
    }

    private static class DelegatingObjectStore implements ObjectStore {
        private final ObjectStore delegate;

        private DelegatingObjectStore(ObjectStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletableFuture<PutObjectResult> putObject(
                ObjectKey key, ReplayableObjectUpload source, PutObjectOptions options) {
            return delegate.putObject(key, source, options);
        }

        @Override
        public CompletableFuture<RangeReadResult> readRange(
                ObjectKey key, long offset, long length, RangeReadOptions options) {
            return delegate.readRange(key, offset, length, options);
        }

        @Override
        public CompletableFuture<HeadObjectResult> headObject(
                ObjectKey key, HeadObjectOptions options) {
            return delegate.headObject(key, options);
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
                ObjectKey key, DeleteObjectOptions options) {
            return delegate.deleteObject(key, options);
        }

        @Override
        public void close() {
            // The test owns the delegate.
        }
    }

    private static final class LostResponseStore extends DelegatingObjectStore {
        private final AtomicBoolean lostPut = new AtomicBoolean();
        private final AtomicBoolean lostDelete = new AtomicBoolean();

        private LostResponseStore(ObjectStore delegate) {
            super(delegate);
        }

        @Override
        public CompletableFuture<PutObjectResult> putObject(
                ObjectKey key, ReplayableObjectUpload source, PutObjectOptions options) {
            return super.putObject(key, source, options).thenCompose(result -> {
                if (lostPut.compareAndSet(false, true)) {
                    return CompletableFuture.failedFuture(
                            new NereusException(
                                    ErrorCode.OBJECT_UPLOAD_FAILED,
                                    true,
                                    "lost probe PUT response"));
                }
                return CompletableFuture.completedFuture(result);
            });
        }

        @Override
        public CompletableFuture<DeleteObjectResult> deleteObject(
                ObjectKey key, DeleteObjectOptions options) {
            return super.deleteObject(key, options).thenCompose(result -> {
                if (lostDelete.compareAndSet(false, true)) {
                    return CompletableFuture.failedFuture(
                            new NereusException(
                                    ErrorCode.OBJECT_UPLOAD_FAILED,
                                    true,
                                    "lost probe DELETE response"));
                }
                return CompletableFuture.completedFuture(result);
            });
        }
    }
}
