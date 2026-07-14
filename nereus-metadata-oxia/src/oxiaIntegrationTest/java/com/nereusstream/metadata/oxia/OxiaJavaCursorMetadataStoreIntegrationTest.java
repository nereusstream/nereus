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

import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.testing.CursorMetadataStoreContractScenario;
import io.oxia.testcontainers.OxiaContainer;
import java.time.Clock;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class OxiaJavaCursorMetadataStoreIntegrationTest {
    private static final String IMAGE = "oxia/oxia:0.16.3";

    @Container
    private static final OxiaContainer OXIA =
            new OxiaContainer(DockerImageName.parse(IMAGE)).withShards(4);

    @Test
    void fakeAndRealRunTheSameContractAndRealBytesSurviveRuntimeRestart() {
        String cluster = "f3/cursor-contract/" + UUID.randomUUID();
        String managedLedgerName = "tenant/ns/persistent/" + UUID.randomUUID();
        CursorMetadataStoreContractScenario.Result fake;
        try (FakeCursorMetadataStore store = new FakeCursorMetadataStore()) {
            fake = CursorMetadataStoreContractScenario.run(store, cluster, managedLedgerName);
        }

        CursorMetadataStoreContractScenario.Result real;
        var projection = CursorMetadataStoreContractScenario.projection(managedLedgerName);
        StreamId streamId = new StreamId(projection.streamId());
        OxiaClientConfiguration configuration = configuration();
        try (SharedOxiaClientRuntime runtime = SharedOxiaClientRuntime.connect(
                        configuration, Clock.systemUTC());
                OxiaJavaCursorMetadataStore store = OxiaJavaCursorMetadataStore.usingSharedRuntime(
                        configuration, runtime, CursorMetadataStoreConfig.defaults())) {
            real = CursorMetadataStoreContractScenario.run(store, cluster, managedLedgerName);
            CursorKeyspace keyspace = new CursorKeyspace(cluster);
            assertThat(runtime.client().list(
                    keyspace.cursorStateScanFrom(streamId),
                    keyspace.cursorStateScanToExclusive(streamId),
                    keyspace.streamPartitionKey(streamId)).join())
                    .as("real Oxia cursor key range")
                    .hasSize(2);
            assertThat(runtime.client().rangeScan(
                    keyspace.cursorStateScanFrom(streamId),
                    keyspace.cursorStateScanToExclusive(streamId),
                    256,
                    keyspace.streamPartitionKey(streamId)).join())
                    .as("real Oxia cursor value range")
                    .hasSize(2);
        }
        assertThat(real).isEqualTo(fake);

        try (SharedOxiaClientRuntime runtime = SharedOxiaClientRuntime.connect(
                        configuration, Clock.systemUTC());
                OxiaJavaCursorMetadataStore restarted = OxiaJavaCursorMetadataStore.usingSharedRuntime(
                        configuration, runtime, CursorMetadataStoreConfig.defaults())) {
            assertThat(restarted.getCursor(cluster, streamId, "sub-a").join().orElseThrow().value())
                    .isEqualTo(real.updatedCursor());
            assertThat(restarted.getRetention(cluster, streamId).join().orElseThrow().value())
                    .isEqualTo(real.updatedRetention());
            assertThat(restarted.scanCursors(cluster, streamId, java.util.Optional.empty(), 256)
                    .join().records().stream().map(VersionedCursorState::value).toList())
                    .containsExactlyElementsOf(real.scannedCursors());
        }
    }

    @Test
    void realWatchInvalidatesAndThirtyTwoWayAbsentCreateHasOneWinner() throws Exception {
        String cluster = "f3/cursor-contention/" + UUID.randomUUID();
        String managedLedgerName = "tenant/ns/persistent/" + UUID.randomUUID();
        var projection = CursorMetadataStoreContractScenario.projection(managedLedgerName);
        StreamId streamId = new StreamId(projection.streamId());
        OxiaClientConfiguration configuration = configuration();
        try (SharedOxiaClientRuntime runtime = SharedOxiaClientRuntime.connect(
                        configuration, Clock.systemUTC());
                OxiaJavaCursorMetadataStore store = OxiaJavaCursorMetadataStore.usingSharedRuntime(
                        configuration, runtime, CursorMetadataStoreConfig.defaults())) {
            CountDownLatch invalidated = new CountDownLatch(1);
            try (WatchRegistration ignored = store.watchStreamCursors(
                    cluster, streamId, invalidated::countDown)) {
                var candidate = CursorMetadataStoreContractScenario.cursor(projection, "contended", 1);
                var attempts = java.util.stream.IntStream.range(0, 32)
                        .mapToObj(index -> store.createCursor(cluster, candidate))
                        .toList();
                CompletableFuture.allOf(attempts.stream()
                        .map(future -> future.handle((value, error) -> null))
                        .toArray(CompletableFuture[]::new))
                        .join();
                assertThat(attempts.stream().filter(future -> !future.isCompletedExceptionally()))
                        .hasSize(1);
                assertThat(invalidated.await(10, TimeUnit.SECONDS)).isTrue();
                assertThat(store.getCursor(cluster, streamId, "contended").join()).isPresent();
            }
        }
    }

    private static OxiaClientConfiguration configuration() {
        return new OxiaClientConfiguration(
                OXIA.getServiceAddress(),
                "default",
                Duration.ofSeconds(10),
                Duration.ofSeconds(30),
                100,
                1_024);
    }
}
