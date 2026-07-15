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

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

class S3ObjectListDeleteErrorMapperTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(2);

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
    void listUsesExactPrefixExpansionAndOpaqueCrossVariantContinuation() {
        S3ObjectKeyMapper mapper = new S3ObjectKeyMapper("prefix");
        List<S3Object> providerObjects = List.of(
                object(mapper.map(new ObjectKey("a/one")), 1),
                object(mapper.map(new ObjectKey("a/two")), 2),
                object(mapper.map(new ObjectKey("b/outside")), 3));
        StubClient stub = new StubClient();
        AtomicInteger calls = new AtomicInteger();
        stub.list = request -> {
            calls.incrementAndGet();
            List<S3Object> matching = providerObjects.stream()
                    .filter(value -> value.key().startsWith(request.prefix()))
                    .sorted(Comparator.comparing(S3Object::key))
                    .toList();
            int start = request.continuationToken() == null
                    ? 0 : Integer.parseInt(request.continuationToken());
            int end = Math.min(matching.size(), start + request.maxKeys());
            boolean truncated = end < matching.size();
            return CompletableFuture.completedFuture(ListObjectsV2Response.builder()
                    .prefix(request.prefix())
                    .contents(matching.subList(start, end))
                    .keyCount(end - start)
                    .isTruncated(truncated)
                    .nextContinuationToken(truncated ? Integer.toString(end) : null)
                    .build());
        };
        store = store(stub);

        List<String> keys = new ArrayList<>();
        Optional<String> continuation = Optional.empty();
        do {
            ListObjectsResult page = store.listObjects(
                    new ObjectKeyPrefix("a"),
                    continuation,
                    new ListObjectsOptions(1, TIMEOUT)).join();
            keys.addAll(page.objects().stream().map(value -> value.key().value()).toList());
            Optional<String> previous = continuation;
            continuation = page.continuationToken();
            assertThat(continuation).isNotEqualTo(previous);
        } while (continuation.isPresent());

        assertThat(keys).containsExactlyInAnyOrder("a/one", "a/two");
        assertThat(calls.get()).isGreaterThan(2);
    }

    @Test
    void malformedListPageFailsClosedAndDoesNotLeakProviderDetails() {
        StubClient stub = new StubClient();
        stub.list = request -> CompletableFuture.completedFuture(ListObjectsV2Response.builder()
                .prefix("wrong-prefix")
                .contents(List.of())
                .keyCount(0)
                .isTruncated(false)
                .build());
        store = store(stub);

        assertThatThrownBy(() -> store.listObjects(
                        new ObjectKeyPrefix("a"), Optional.empty(), new ListObjectsOptions(10, TIMEOUT)).join())
                .satisfies(error -> assertThat(unwrap(error).code()).isEqualTo(ErrorCode.OBJECT_READ_FAILED));
    }

    @Test
    void deleteHeadsExactIdentityUsesIfMatchAndTreatsPriorAbsenceAsSuccess() {
        ObjectKey key = new ObjectKey("gc/object");
        Checksum checksum = new Checksum(ChecksumType.CRC32C, "01020304");
        StubClient stub = new StubClient();
        stub.head = request -> CompletableFuture.completedFuture(HeadObjectResponse.builder()
                .contentLength(11L)
                .eTag("etag-1")
                .metadata(Map.of(
                        S3CompatibleObjectStore.CHECKSUM_TYPE_METADATA, "CRC32C",
                        S3CompatibleObjectStore.CHECKSUM_VALUE_METADATA, checksum.value()))
                .build());
        AtomicReference<DeleteObjectRequest> deleted = new AtomicReference<>();
        stub.delete = request -> {
            deleted.set(request);
            return CompletableFuture.completedFuture(DeleteObjectResponse.builder().build());
        };
        store = store(stub);

        DeleteObjectResult result = store.deleteObject(
                key, new DeleteObjectOptions(11, checksum, Optional.of("etag-1"), TIMEOUT)).join();
        assertThat(result.status()).isEqualTo(DeleteObjectResult.Status.DELETED);
        assertThat(deleted.get().ifMatch()).isEqualTo("etag-1");
        store.close();

        StubClient absent = new StubClient();
        absent.head = request -> CompletableFuture.failedFuture(service(404));
        absent.delete = request -> CompletableFuture.failedFuture(new AssertionError("delete must not run"));
        store = store(absent);
        assertThat(store.deleteObject(
                        key, new DeleteObjectOptions(11, checksum, Optional.of("etag-1"), TIMEOUT)).join().status())
                .isEqualTo(DeleteObjectResult.Status.ALREADY_ABSENT);
    }

    @Test
    void listAndDeleteServiceFailuresUseClosedRedactedMappings() {
        NereusException list = S3ObjectErrorMapper.list(service(503), "bucket");
        NereusException delete = S3ObjectErrorMapper.delete(
                service(403), "bucket", new ObjectKey("secret/key"));

        assertThat(list.code()).isEqualTo(ErrorCode.OBJECT_READ_FAILED);
        assertThat(list.retriable()).isTrue();
        assertThat(delete.code()).isEqualTo(ErrorCode.OBJECT_UPLOAD_FAILED);
        assertThat(delete.retriable()).isFalse();
        assertThat(delete.getMessage()).doesNotContain("secret/key", "credential-sentinel");
        assertThat(delete.getCause()).isNull();
    }

    private S3CompatibleObjectStore store(StubClient stub) {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        return new S3CompatibleObjectStore(stub.proxy(), scheduler, "bucket", "prefix");
    }

    private static S3Object object(String key, long size) {
        return S3Object.builder()
                .key(key)
                .size(size)
                .eTag("etag-" + size)
                .lastModified(Instant.EPOCH.plusSeconds(size))
                .build();
    }

    private static S3Exception service(int status) {
        return (S3Exception) S3Exception.builder()
                .statusCode(status)
                .message("credential-sentinel")
                .build();
    }

    private static NereusException unwrap(Throwable supplied) {
        Throwable failure = supplied;
        while (failure instanceof CompletionException && failure.getCause() != null) {
            failure = failure.getCause();
        }
        assertThat(failure).isInstanceOf(NereusException.class);
        return (NereusException) failure;
    }

    private static final class StubClient implements InvocationHandler {
        private Function<ListObjectsV2Request, CompletableFuture<ListObjectsV2Response>> list;
        private Function<HeadObjectRequest, CompletableFuture<HeadObjectResponse>> head;
        private Function<DeleteObjectRequest, CompletableFuture<DeleteObjectResponse>> delete;

        private S3AsyncClient proxy() {
            return (S3AsyncClient) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[] {S3AsyncClient.class}, this);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] arguments) {
            return switch (method.getName()) {
                case "listObjectsV2" -> list.apply((ListObjectsV2Request) arguments[0]);
                case "headObject" -> head.apply((HeadObjectRequest) arguments[0]);
                case "deleteObject" -> delete.apply((DeleteObjectRequest) arguments[0]);
                case "serviceName" -> "S3";
                case "close" -> null;
                case "toString" -> "ListDeleteStubS3AsyncClient";
                default -> throw new UnsupportedOperationException(method.toString());
            };
        }
    }
}
