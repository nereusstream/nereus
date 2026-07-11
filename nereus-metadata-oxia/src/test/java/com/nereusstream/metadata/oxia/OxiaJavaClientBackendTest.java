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

package com.nereusstream.metadata.oxia;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamName;
import com.nereusstream.api.StreamState;
import com.nereusstream.api.keys.DeterministicIds;
import com.nereusstream.metadata.oxia.codec.Phase1MetadataCodecs;
import com.nereusstream.metadata.oxia.records.AppendSessionSnapshotRecord;
import com.nereusstream.metadata.oxia.records.StreamHeadRecord;
import io.oxia.client.api.CloseableIterable;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.Notification;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.Version;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class OxiaJavaClientBackendTest {
    private static final PartitionKey PARTITION_KEY = new PartitionKey("stream-1");

    @Test
    void rangeScanStopsConsumingAndClosesTheOxiaIteratorAtLimit() {
        AtomicInteger consumed = new AtomicInteger();
        AtomicBoolean closed = new AtomicBoolean();
        AtomicReference<Consumer<Notification>> notifications = new AtomicReference<>();
        List<GetResult> source = List.of(result("/range/1", 1), result("/range/2", 2), result("/range/3", 3));
        SyncOxiaClient client = client(notifications, () -> iterable(source, consumed, closed));
        Executor direct = Runnable::run;
        OxiaJavaClientBackend backend = new OxiaJavaClientBackend(client, direct, direct);

        List<PartitionedOxiaClient.VersionedValue> values = backend
                .rangeScan("/range/0", "/range/z", 2, PARTITION_KEY)
                .join();

        assertThat(values).extracting(PartitionedOxiaClient.VersionedValue::key)
                .containsExactly("/range/1", "/range/2");
        assertThat(consumed).hasValue(2);
        assertThat(closed).isTrue();
    }

    @Test
    void notificationCallbackCanSynchronouslyReadWithoutStarvingRequestExecutor() throws Exception {
        AtomicReference<Consumer<Notification>> notifications = new AtomicReference<>();
        SyncOxiaClient client = client(notifications, () -> iterable(List.of(), new AtomicInteger(), new AtomicBoolean()));
        ExecutorService requests = Executors.newSingleThreadExecutor(runnable -> new Thread(runnable, "oxia-request"));
        ExecutorService callbacks = Executors.newSingleThreadExecutor(runnable -> new Thread(runnable, "oxia-notification"));
        try {
            OxiaJavaClientBackend backend = new OxiaJavaClientBackend(client, requests, callbacks);
            CountDownLatch delivered = new CountDownLatch(1);
            AtomicReference<String> callbackThread = new AtomicReference<>();
            backend.watchPrefix("/watch", PARTITION_KEY, () -> {
                assertThat(backend.get("/value", PARTITION_KEY).join()).isPresent();
                callbackThread.set(Thread.currentThread().getName());
                delivered.countDown();
            });

            notifications.get().accept(new Notification.KeyModified("/watch/key", 1));

            assertThat(delivered.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(callbackThread.get()).startsWith("oxia-notification");
        } finally {
            requests.shutdownNow();
            callbacks.shutdownNow();
        }
    }

    @Test
    void productionAdapterRejectsWorkWhenBoundedOperationQueueIsFull() throws Exception {
        CountDownLatch started = new CountDownLatch(4);
        CountDownLatch release = new CountDownLatch(1);
        SyncOxiaClient client = blockingClient(started, release);
        OxiaJavaClientMetadataStore store = new OxiaJavaClientMetadataStore(
                new OxiaClientConfiguration(
                        "unused:6648",
                        "default",
                        Duration.ofSeconds(5),
                        Duration.ofSeconds(5),
                        10,
                        1),
                client,
                Clock.systemUTC());
        List<CompletableFuture<?>> admitted = new ArrayList<>();
        try {
            for (int index = 0; index < 4; index++) {
                admitted.add(store.getStream("cluster", new StreamId("stream-" + index)));
            }
            assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();
            admitted.add(store.getStream("cluster", new StreamId("queued")));

            CompletableFuture<?> rejected = store.getStream("cluster", new StreamId("rejected"));

            assertThatThrownBy(rejected::join)
                    .isInstanceOfSatisfying(CompletionException.class, error -> {
                        NereusException failure = (NereusException) error.getCause();
                        assertThat(failure.code()).isEqualTo(ErrorCode.BACKPRESSURE_REJECTED);
                        assertThat(failure.retriable()).isTrue();
                    });
        } finally {
            release.countDown();
            admitted.forEach(future -> future.handle((value, error) -> null).join());
            store.close();
        }
    }

    @Test
    void productionAdapterRejectsStreamHeadStoredUnderAnotherStreamKey() {
        StreamName wrongName = new StreamName("wrong-stream");
        StreamId wrongId = DeterministicIds.streamIdFor(wrongName);
        StreamHeadRecord wrongHead = new StreamHeadRecord(
                wrongId.value(),
                wrongName.value(),
                DeterministicIds.streamNameHash(wrongName),
                StreamState.ACTIVE.name(),
                StorageProfile.OBJECT_WAL_SYNC_OBJECT.name(),
                Map.of(),
                1,
                0,
                0,
                0,
                0,
                0,
                "",
                AppendSessionSnapshotRecord.EMPTY,
                0);
        byte[] encoded = Phase1MetadataCodecs.encodeEnvelope(wrongHead, StreamHeadRecord.class);
        OxiaJavaClientMetadataStore store = new OxiaJavaClientMetadataStore(
                OxiaClientConfiguration.defaults("unused:6648"),
                valueClient(encoded),
                Clock.systemUTC());
        try {
            CompletableFuture<?> result = store.getStream("cluster", new StreamId("expected-stream"));

            assertThatThrownBy(result::join)
                    .isInstanceOfSatisfying(CompletionException.class, error ->
                            assertThat((NereusException) error.getCause())
                                    .extracting(NereusException::code)
                                    .isEqualTo(ErrorCode.METADATA_INVARIANT_VIOLATION));
        } finally {
            store.close();
        }
    }

    private static SyncOxiaClient client(
            AtomicReference<Consumer<Notification>> notifications,
            java.util.function.Supplier<CloseableIterable<GetResult>> rangeScan) {
        return (SyncOxiaClient) Proxy.newProxyInstance(
                SyncOxiaClient.class.getClassLoader(),
                new Class<?>[] {SyncOxiaClient.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "notifications" -> {
                        @SuppressWarnings("unchecked")
                        Consumer<Notification> consumer = (Consumer<Notification>) arguments[0];
                        notifications.set(consumer);
                        yield null;
                    }
                    case "rangeScan" -> rangeScan.get();
                    case "get" -> result((String) arguments[0], 7);
                    case "close" -> null;
                    case "toString" -> "OxiaJavaClientBackendTestClient";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static SyncOxiaClient blockingClient(
            CountDownLatch started,
            CountDownLatch release) {
        return (SyncOxiaClient) Proxy.newProxyInstance(
                SyncOxiaClient.class.getClassLoader(),
                new Class<?>[] {SyncOxiaClient.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "notifications", "close" -> null;
                    case "get" -> {
                        started.countDown();
                        try {
                            release.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException("blocking client interrupted", e);
                        }
                        yield result((String) arguments[0], 1);
                    }
                    case "toString" -> "OxiaJavaClientBackendBlockingTestClient";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static SyncOxiaClient valueClient(byte[] value) {
        return (SyncOxiaClient) Proxy.newProxyInstance(
                SyncOxiaClient.class.getClassLoader(),
                new Class<?>[] {SyncOxiaClient.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "notifications", "close" -> null;
                    case "get" -> new GetResult((String) arguments[0], value, version(1));
                    case "toString" -> "OxiaJavaClientBackendValueTestClient";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static CloseableIterable<GetResult> iterable(
            List<GetResult> source,
            AtomicInteger consumed,
            AtomicBoolean closed) {
        return new CloseableIterable<>() {
            @Override
            public Iterator<GetResult> iterator() {
                Iterator<GetResult> delegate = source.iterator();
                return new Iterator<>() {
                    @Override
                    public boolean hasNext() {
                        return delegate.hasNext();
                    }

                    @Override
                    public GetResult next() {
                        consumed.incrementAndGet();
                        return delegate.next();
                    }
                };
            }

            @Override
            public void close() {
                closed.set(true);
            }
        };
    }

    private static GetResult result(String key, long versionId) {
        return new GetResult(
                key,
                new byte[] {(byte) versionId},
                version(versionId));
    }

    private static Version version(long versionId) {
        return new Version(versionId, 0, 0, 1, Optional.empty(), Optional.empty());
    }
}
