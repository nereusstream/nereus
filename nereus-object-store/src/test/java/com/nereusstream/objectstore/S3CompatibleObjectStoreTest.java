/* Licensed under the Apache License, Version 2.0 */
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
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

class S3CompatibleObjectStoreTest {
    private ScheduledExecutorService scheduler;
    private S3CompatibleObjectStore store;

    @AfterEach
    void close() {
        if (store != null) {
            store.close();
        } else {
            if (scheduler != null) {
                scheduler.shutdownNow();
            }
        }
    }

    @Test
    void putUsesConditionalHeaderReservedMetadataAndDoesNotMoveCallerBuffer() {
        AtomicReference<PutObjectRequest> captured = new AtomicReference<>();
        StubClient stub = new StubClient();
        stub.put = request -> {
            captured.set(request);
            return CompletableFuture.completedFuture(PutObjectResponse.builder().eTag("opaque-etag").build());
        };
        store = store(stub);
        ByteBuffer payload = ByteBuffer.wrap("x0123y".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        payload.position(1).limit(5);
        var checksum = Crc32cChecksums.checksum("0123".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        PutObjectResult result = store.putObject(new ObjectKey("wal/raw/key"), payload,
                new PutObjectOptions("application/octet-stream", checksum, true,
                        Map.of("Owner", "nereus"), Duration.ofSeconds(1))).join();

        assertThat(payload.position()).isEqualTo(1);
        assertThat(payload.limit()).isEqualTo(5);
        assertThat(result.etag()).isEqualTo("opaque-etag");
        PutObjectRequest request = captured.get();
        assertThat(request.key()).isEqualTo("prefix/objects/v1/d2FsL3Jhdy9rZXk");
        assertThat(request.overrideConfiguration().orElseThrow().headers().get("If-None-Match"))
                .containsExactly("*");
        assertThat(request.metadata())
                .containsEntry("owner", "nereus")
                .containsEntry(S3CompatibleObjectStore.CHECKSUM_TYPE_METADATA, "CRC32C")
                .containsEntry(S3CompatibleObjectStore.CHECKSUM_VALUE_METADATA, checksum.value());
    }

    @Test
    void requiresExactPartialContentRangeAndChecksum() {
        StubClient stub = new StubClient();
        stub.get = request -> {
            GetObjectResponse.Builder response = GetObjectResponse.builder();
            response.sdkHttpResponse(SdkHttpResponse.builder()
                    .statusCode(206)
                    .putHeader("Content-Range", "bytes 2-5/10")
                    .build());
            return CompletableFuture.completedFuture(ResponseBytes.fromByteArray(
                    response.build(), "2345".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        };
        store = store(stub);
        byte[] expected = "2345".getBytes(java.nio.charset.StandardCharsets.UTF_8);

        RangeReadResult result = store.readRange(new ObjectKey("key"), 2, 4,
                new RangeReadOptions(Optional.of(Crc32cChecksums.checksum(expected)), Duration.ofSeconds(1))).join();

        assertThat(result.payload()).isEqualByComparingTo(ByteBuffer.wrap(expected));
    }

    @Test
    void timeoutAndCallerCancellationCancelSdkRequestWithStableCodes() throws Exception {
        StubClient timeoutStub = new StubClient();
        CompletableFuture<HeadObjectResponse> timeoutSdk = new CompletableFuture<>();
        timeoutStub.head = ignored -> timeoutSdk;
        store = store(timeoutStub);

        CompletableFuture<HeadObjectResult> timed = store.headObject(
                new ObjectKey("timeout-key"), new HeadObjectOptions(Duration.ofMillis(10)));
        assertCode(() -> timed.join(), ErrorCode.TIMEOUT);
        assertThat(timeoutSdk.isCancelled()).isTrue();
        store.close();

        StubClient cancelStub = new StubClient();
        CompletableFuture<HeadObjectResponse> cancelSdk = new CompletableFuture<>();
        cancelStub.head = ignored -> cancelSdk;
        store = store(cancelStub);
        CompletableFuture<HeadObjectResult> cancelled = store.headObject(
                new ObjectKey("cancel-key"), new HeadObjectOptions(Duration.ofSeconds(1)));
        assertThat(cancelled.cancel(true)).isTrue();
        assertCode(() -> cancelled.join(), ErrorCode.CANCELLED);
        assertThat(cancelSdk.isCancelled()).isTrue();
    }

    @Test
    void sdkFailureIsRedactedAndDoesNotRetainSdkThrowable() {
        String rawKey = "raw-key-sentinel";
        String secret = "credential-sentinel";
        StubClient stub = new StubClient();
        stub.head = ignored -> CompletableFuture.failedFuture(S3Exception.builder()
                .statusCode(403)
                .message(rawKey + " " + secret)
                .build());
        store = store(stub);

        assertThatThrownBy(() -> store.headObject(
                        new ObjectKey(rawKey), new HeadObjectOptions(Duration.ofSeconds(1))).join())
                .satisfies(error -> {
                    NereusException nereus = unwrap(error);
                    assertThat(nereus.code()).isEqualTo(ErrorCode.OBJECT_READ_FAILED);
                    assertThat(nereus.retriable()).isFalse();
                    assertThat(nereus.getMessage()).doesNotContain(rawKey, secret);
                    assertThat(nereus.getCause()).isNull();
                });
    }

    private S3CompatibleObjectStore store(StubClient stub) {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        return new S3CompatibleObjectStore(stub.proxy(), scheduler, "bucket", "prefix");
    }

    private static void assertCode(Runnable operation, ErrorCode code) {
        assertThatThrownBy(operation::run).satisfies(error ->
                assertThat(unwrap(error).code()).isEqualTo(code));
    }

    private static NereusException unwrap(Throwable error) {
        Throwable current = error;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        assertThat(current).isInstanceOf(NereusException.class);
        return (NereusException) current;
    }

    private static final class StubClient implements InvocationHandler {
        private java.util.function.Function<PutObjectRequest, CompletableFuture<PutObjectResponse>> put;
        private java.util.function.Function<GetObjectRequest, CompletableFuture<ResponseBytes<GetObjectResponse>>> get;
        private java.util.function.Function<Object, CompletableFuture<HeadObjectResponse>> head;

        private S3AsyncClient proxy() {
            return (S3AsyncClient) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[] {S3AsyncClient.class}, this);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] arguments) {
            return switch (method.getName()) {
                case "putObject" -> put.apply((PutObjectRequest) arguments[0]);
                case "getObject" -> get.apply((GetObjectRequest) arguments[0]);
                case "headObject" -> head.apply(arguments[0]);
                case "serviceName" -> "S3";
                case "close" -> null;
                case "toString" -> "StubS3AsyncClient";
                default -> throw new UnsupportedOperationException(method.toString());
            };
        }
    }
}
