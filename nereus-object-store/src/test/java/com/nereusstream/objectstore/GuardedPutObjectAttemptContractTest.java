/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.objectstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

class GuardedPutObjectAttemptContractTest {
    private static final ObjectKey KEY = new ObjectKey("compacted/output");
    private static final byte[] PAYLOAD = "replay-me".getBytes(StandardCharsets.UTF_8);

    private ScheduledExecutorService scheduler;
    private S3CompatibleObjectStore store;

    @AfterEach
    void close() {
        if (store != null) {
            store.close();
        } else if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    @Test
    void guardRunsImmediatelyBeforeEveryOwnedProviderRetry() {
        StubClient stub = new StubClient();
        AtomicInteger providerAttempts = new AtomicInteger();
        stub.put = (request, body) -> providerAttempts.incrementAndGet() == 1
                ? CompletableFuture.failedFuture(service(503))
                : CompletableFuture.completedFuture(PutObjectResponse.builder().eTag("etag").build());
        store = store(stub);
        CountingUpload upload = new CountingUpload(PAYLOAD);
        List<Integer> guarded = new ArrayList<>();

        PutObjectResult result = store.putObject(
                KEY,
                upload,
                options(),
                (key, attempt) -> {
                    assertThat(key).isEqualTo(KEY);
                    guarded.add(attempt);
                    return CompletableFuture.completedFuture(null);
                }).join();

        assertThat(result.checksum()).isEqualTo(Crc32cChecksums.checksum(PAYLOAD));
        assertThat(guarded).containsExactly(1, 2);
        assertThat(providerAttempts.get()).isEqualTo(2);
        assertThat(upload.openCount()).isEqualTo(2);
    }

    @Test
    void failedRetryGuardSendsNoSecondAttemptAndPreservesFenceFailure() {
        StubClient stub = new StubClient();
        AtomicInteger providerAttempts = new AtomicInteger();
        stub.put = (request, body) -> {
            providerAttempts.incrementAndGet();
            return CompletableFuture.failedFuture(service(503));
        };
        store = store(stub);
        CountingUpload upload = new CountingUpload(PAYLOAD);
        List<Integer> guarded = new ArrayList<>();
        NereusException fenced = new NereusException(
                ErrorCode.FENCED_APPEND, false, "durable output owner changed");

        assertThatThrownBy(() -> store.putObject(
                        KEY,
                        upload,
                        options(),
                        (key, attempt) -> {
                            guarded.add(attempt);
                            return attempt == 1
                                    ? CompletableFuture.completedFuture(null)
                                    : CompletableFuture.failedFuture(fenced);
                        }).join())
                .satisfies(error -> assertThat(unwrap(error)).isSameAs(fenced));

        assertThat(guarded).containsExactly(1, 2);
        assertThat(providerAttempts.get()).isEqualTo(1);
        assertThat(upload.openCount()).isEqualTo(1);
    }

    @Test
    void failedInitialGuardNeverOpensUploadOrCallsProvider() {
        StubClient stub = new StubClient();
        AtomicInteger providerAttempts = new AtomicInteger();
        stub.put = (request, body) -> {
            providerAttempts.incrementAndGet();
            return CompletableFuture.completedFuture(PutObjectResponse.builder().eTag("etag").build());
        };
        store = store(stub);
        CountingUpload upload = new CountingUpload(PAYLOAD);

        assertThatThrownBy(() -> store.putObject(
                        KEY,
                        upload,
                        options(),
                        (key, attempt) -> CompletableFuture.failedFuture(new NereusException(
                                ErrorCode.METADATA_CONDITION_FAILED, false, "root is no longer active"))).join())
                .satisfies(error -> assertThat(unwrap(error))
                        .isInstanceOfSatisfying(NereusException.class, nereus ->
                                assertThat(nereus.code()).isEqualTo(ErrorCode.METADATA_CONDITION_FAILED)));
        assertThat(providerAttempts.get()).isZero();
        assertThat(upload.openCount()).isZero();
    }

    @Test
    void callerCancellationAndStoreCloseTerminateAWaitingGuard() {
        StubClient stub = new StubClient();
        stub.put = (request, body) -> CompletableFuture.completedFuture(
                PutObjectResponse.builder().eTag("etag").build());
        store = store(stub);
        CompletableFuture<Void> waitingGuard = new CompletableFuture<>();
        CompletableFuture<PutObjectResult> cancelled = store.putObject(
                KEY, new CountingUpload(PAYLOAD), options(), (key, attempt) -> waitingGuard);

        assertThat(cancelled.cancel(true)).isTrue();
        assertThatThrownBy(cancelled::join)
                .satisfies(error -> assertThat(unwrap(error))
                        .isInstanceOfSatisfying(NereusException.class, nereus ->
                                assertThat(nereus.code()).isEqualTo(ErrorCode.CANCELLED)));
        assertThat(waitingGuard.isCancelled()).isTrue();

        CompletableFuture<Void> closeGuard = new CompletableFuture<>();
        CompletableFuture<PutObjectResult> closed = store.putObject(
                KEY, new CountingUpload(PAYLOAD), options(), (key, attempt) -> closeGuard);
        store.close();
        assertThatThrownBy(closed::join)
                .satisfies(error -> assertThat(unwrap(error))
                        .isInstanceOfSatisfying(NereusException.class, nereus ->
                                assertThat(nereus.code()).isEqualTo(ErrorCode.STORAGE_CLOSED)));
        assertThat(closeGuard.isCancelled()).isTrue();
    }

    private S3CompatibleObjectStore store(StubClient stub) {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        return new S3CompatibleObjectStore(
                stub.proxy(),
                scheduler,
                "bucket",
                "prefix",
                Duration.ofSeconds(2),
                new ObjectPutRetryPolicy(3, Duration.ofMillis(1), Duration.ofMillis(1)),
                ignored -> 0);
    }

    private static PutObjectOptions options() {
        return new PutObjectOptions(
                "application/octet-stream",
                Crc32cChecksums.checksum(PAYLOAD),
                true,
                Map.of(),
                Duration.ofSeconds(2));
    }

    private static S3Exception service(int status) {
        return (S3Exception) S3Exception.builder().statusCode(status).message("provider detail").build();
    }

    private static Throwable unwrap(Throwable supplied) {
        Throwable failure = supplied;
        while (failure instanceof CompletionException && failure.getCause() != null) {
            failure = failure.getCause();
        }
        return failure;
    }

    private static final class CountingUpload implements ReplayableObjectUpload {
        private final byte[] bytes;
        private final AtomicInteger opens = new AtomicInteger();
        private final AtomicBoolean closed = new AtomicBoolean();

        private CountingUpload(byte[] bytes) {
            this.bytes = bytes.clone();
        }

        @Override
        public long contentLength() {
            return bytes.length;
        }

        @Override
        public Flow.Publisher<ByteBuffer> openPublisher() {
            if (closed.get()) {
                throw new IllegalStateException("closed");
            }
            opens.incrementAndGet();
            return new ByteBufferObjectUpload(ByteBuffer.wrap(bytes)).openPublisher();
        }

        @Override
        public void close() {
            closed.set(true);
        }

        private int openCount() {
            return opens.get();
        }
    }

    private static final class StubClient implements InvocationHandler {
        private BiFunction<PutObjectRequest, AsyncRequestBody, CompletableFuture<PutObjectResponse>> put;

        private S3AsyncClient proxy() {
            return (S3AsyncClient) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[] {S3AsyncClient.class}, this);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] arguments) {
            return switch (method.getName()) {
                case "putObject" -> consume((AsyncRequestBody) arguments[1]).thenCompose(ignored ->
                        put.apply((PutObjectRequest) arguments[0], (AsyncRequestBody) arguments[1]));
                case "serviceName" -> "S3";
                case "close" -> null;
                case "toString" -> "GuardedPutStubS3AsyncClient";
                default -> throw new UnsupportedOperationException(method.toString());
            };
        }

        private static CompletableFuture<Void> consume(AsyncRequestBody body) {
            CompletableFuture<Void> result = new CompletableFuture<>();
            body.subscribe(new Subscriber<>() {
                @Override
                public void onSubscribe(Subscription subscription) {
                    subscription.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(ByteBuffer ignored) {
                }

                @Override
                public void onError(Throwable failure) {
                    result.completeExceptionally(failure);
                }

                @Override
                public void onComplete() {
                    result.complete(null);
                }
            });
            return result;
        }
    }
}
