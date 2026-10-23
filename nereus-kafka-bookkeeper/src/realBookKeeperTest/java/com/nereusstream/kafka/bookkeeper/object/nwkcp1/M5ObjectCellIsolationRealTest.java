/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.kafka.bookkeeper.object.nwkcp1;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.KafkaCellId;
import com.nereusstream.domain.protocol.KafkaProtocolCellIdentity;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaNwg1ObjectPipelineV1;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.object.control.WalRunRootRecord;
import com.nereusstream.storage.object.kms.KmsCellSession;
import io.oxia.client.api.OxiaClientBuilder;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/** Real M2 Object boundary: shared SDK clients, fixed lane semantics and a constant six-Binding corpus. */
class M5ObjectCellIsolationRealTest {
    private record Binding(
            String role,
            KafkaObjectAuthorizationRealTest.Fixture fixture,
            KafkaObjectAuthorizationRealTest.Partition partition,
            int lane) {}

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    @Timeout(180)
    void nativePutReplyHeldThenUnknownPreservesBindingsAcrossLanesCellsAndShards(int shards) throws Exception {
        var http = new KafkaObjectAuthorizationRealTest.NwgHttpCounts();
        String cohort = "m5-cell-" + UUID.randomUUID().toString().replace("-", "");
        String bucketA = cohort + "-a";
        String bucketB = cohort + "-b";
        var scopeA = scope(bucketA);
        var scopeB = scope(bucketB);
        try (var objects = S3Client.builder()
                        .endpointOverride(URI.create(System.getProperty("nereus.nsip1.object.endpoint")))
                        .credentialsProvider(StaticCredentialsProvider.create(
                                AwsBasicCredentials.create("nereus-m3-evidence", "nereus-m3-evidence-secret")))
                        .region(Region.US_EAST_1)
                        .forcePathStyle(true)
                        .overrideConfiguration(value -> value.addExecutionInterceptor(http))
                        .build();
                var metadata = OxiaClientBuilder.create(System.getProperty("nereus.nsip1.object.oxia"))
                        .namespace("default")
                        .requestTimeout(Duration.ofSeconds(10))
                        .asyncClient()
                        .get();
                var kmsA = new KmsCellSession(
                        new StorageObjectNwkcp1BackendV1Test.FakeKmsTransport(),
                        scopeA,
                        "kms/cell-a",
                        4,
                        new SecureRandom());
                var kmsB = new KmsCellSession(
                        new StorageObjectNwkcp1BackendV1Test.FakeKmsTransport(),
                        scopeB,
                        "kms/cell-a",
                        4,
                        new SecureRandom());
                var owned = new Fixtures()) {
            objects.createBucket(value -> value.bucket(bucketA));
            objects.createBucket(value -> value.bucket(bucketB));
            var services = new KafkaObjectAuthorizationRealTest.Fixture.SharedServices(
                    objects, metadata, bucketA, "/" + cohort + "/a");
            var otherServices = new KafkaObjectAuthorizationRealTest.Fixture.SharedServices(
                    objects, metadata, bucketB, "/" + cohort + "/b");
            var first = owned.add(new KafkaObjectAuthorizationRealTest.Fixture(8, root(1, 7, scopeA), kmsA, services));
            var last = shards == 1
                    ? first
                    : owned.add(new KafkaObjectAuthorizationRealTest.Fixture(8, root(1, 8, scopeA), kmsA, services));
            var other = owned.add(
                    new KafkaObjectAuthorizationRealTest.Fixture(8, root(2, 17, scopeB), kmsB, otherServices));
            if (shards == 2) {
                assertThat(first.root.providerConfiguration().exclusiveNamespacePrefix())
                        .isNotEqualTo(last.root.providerConfiguration().exclusiveNamespacePrefix());
                assertThat(first.root.providerScopeId()).isEqualTo(last.root.providerScopeId());
            }
            assertThat(first.root.protocolCellIdentity()).isNotEqualTo(other.root.protocolCellIdentity());
            assertThat(first.root.providerScopeId()).isNotEqualTo(other.root.providerScopeId());
            var a = binding("HELD", first, 0, 0);
            var b = binding("SAME_LANE_1", first, 1, 0);
            var c = binding("SAME_LANE_2", first, 2, 0);
            var d = binding("OTHER_LANE", first, 3, 1);
            var e = binding("SHARD_DISTRIBUTED", last, 4, shards == 1 ? 2 : 0);
            var f = binding("OTHER_CELL", other, 5, 0);
            var rows = List.of(a, b, c, d, e, f);
            var firstCandidate = a.partition().prepare(first, body());
            var sharedCredit = first.pipeline.reservePhysicalBeforePosition(4096);
            var grouped = new ArrayList<>(List.of(
                    b.partition().prepare(first, body(), sharedCredit),
                    c.partition().prepare(first, body(), sharedCredit)));
            grouped.sort(Comparator.comparing(
                    value -> value.commit().expectedFence().bindingId().digest().toHex()));
            long assemblyStarted = System.nanoTime();
            var groupedPlan = first.plan(grouped, 0);
            long assemblyNanos = System.nanoTime() - assemblyStarted;
            var members = grouped.stream()
                    .map(value -> new KafkaNwg1ObjectPipelineV1.SharedMember(
                            value.ticket(), value.commit(), value.nativeState(), value.partition().tracker))
                    .toList();
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            first.provider.afterNwgPut = identity -> {
                first.provider.losePutKey = identity.key();
                entered.countDown();
                await(release);
            };
            var executor = Executors.newFixedThreadPool(2);
            CompletableFuture<Void> accepted = null;
            long started = System.nanoTime();
            try {
                accepted = CompletableFuture.runAsync(
                        () -> {
                            first.pipeline.writeResolveAndInstallShared(
                                    first.plan(List.of(firstCandidate), 0),
                                    List.of(new KafkaNwg1ObjectPipelineV1.SharedMember(
                                            firstCandidate.ticket(),
                                            firstCandidate.commit(),
                                            firstCandidate.nativeState(),
                                            a.partition().tracker)),
                                    2);
                            assertThat(a.partition().bridge.publishNext(() -> {}))
                                    .isPresent();
                        },
                        executor);
                assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
                assertThat(http.puts).isPositive();
                assertThat(first.admittedProvider.acceptedOperations()).isOne();
                var observer = accepted.copy();
                assertThat(observer.cancel(true)).isTrue();
                assertThat(accepted).isNotDone();
                assertThat(first.admittedProvider.acceptedOperations()).isOne();
                assertThatThrownBy(() -> first.pipeline.writeResolveAndInstallShared(groupedPlan, members, 2))
                        .hasRootCauseMessage("sealed plan differs from the unresolved lane candidate");
                assertThat(b.partition().bridge.publishNext(() -> {})).isEmpty();
                assertThat(c.partition().bridge.publishNext(() -> {})).isEmpty();
                sample(shards, a, "REPLY_HELD", System.nanoTime() - started, assemblyNanos, 0, 0, 0);
                for (var healthy : List.of(d, e, f)) {
                    long callStarted = System.nanoTime();
                    CompletableFuture.runAsync(() -> write(healthy), executor).get(10, TimeUnit.SECONDS);
                    assertThat(accepted).isNotDone();
                    sample(shards, healthy, "WHILE_HELD", System.nanoTime() - callStarted, 0, 0, 0, 0);
                }
                long released = System.nanoTime();
                release.countDown();
                accepted.get(30, TimeUnit.SECONDS);
                assertThat(observer).isCancelled();
                assertThat(first.admittedProvider.acceptedOperations()).isZero();
                assertThat(first.admittedProvider.unknownObjectCount()).isZero();
                sample(shards, a, "RECONCILED", System.nanoTime() - released, 0, 0, 0, 0);
                first.pipeline.writeResolveAndInstallShared(groupedPlan, members, 3);
                assertThat(b.partition().bridge.publishNext(() -> {})).isPresent();
                assertThat(c.partition().bridge.publishNext(() -> {})).isPresent();
                for (var delayed : List.of(b, c)) {
                    sample(shards, delayed, "SAME_LANE_CONVERGED", System.nanoTime() - started, assemblyNanos, 0, 0, 0);
                }
                for (var fixture : owned.values) {
                    fixture.publisher.flush();
                }
                KafkaObjectAuthorizationRealTest.Partition otherRecovered = null;
                for (var row : rows) {
                    assertThat(row.partition().snapshot().root().frontiers().highWatermark())
                            .isOne();
                    assertThat(row.partition().tracker.pendingUnits()).isZero();
                    var fixture = row.fixture();
                    int gets = fixture.provider.fullGets + fixture.provider.ranges;
                    long bytes = fixture.provider.getBytes;
                    long recoveryStarted = System.nanoTime();
                    var closed = fixture.authority.close(row.partition().fence);
                    var restored = fixture.takeover(closed, 7, 8);
                    assertThat(fixture.readAtLso(restored, 1).ranges())
                            .singleElement()
                            .satisfies(value ->
                                    assertThat(value.kafkaRecordBatchBytes()).isEqualTo(body()));
                    assertThat(fixture.provider.fullGets + fixture.provider.ranges - gets)
                            .isPositive();
                    assertThat(fixture.provider.getBytes - bytes).isPositive();
                    sample(
                            shards,
                            row,
                            "RECOVERED",
                            0,
                            0,
                            System.nanoTime() - recoveryStarted,
                            fixture.provider.fullGets + fixture.provider.ranges - gets,
                            fixture.provider.getBytes - bytes);
                    if (row == f) {
                        otherRecovered = restored;
                    }
                }
                assertThat(otherRecovered).isNotNull();
                first.close();
                owned.values.remove(first);
                otherRecovered.append(other, KafkaObjectAuthorizationRealTest.data(1, 8, 1, false));
                assertThat(otherRecovered.snapshot().root().frontiers().highWatermark())
                        .isEqualTo(2);
                System.out.println("M5_CELL_PUT_COHORT shards=" + shards + " bindings=6 recordBodySha256="
                        + Sha256Digest.hash(body()).toHex() + " sharedS3Client=true sharedOxiaClient=true"
                        + " closedCellDoesNotCloseSharedClients=true productionAuthority=false wholeM5Authority=false");
            } finally {
                release.countDown();
                if (accepted != null) {
                    accepted.handle((value, failure) -> null).get(30, TimeUnit.SECONDS);
                }
                executor.shutdownNow();
                assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    private static CanonicalBytes body() {
        return KafkaObjectAuthorizationRealTest.data(0, 7, 0, false);
    }

    private static Binding binding(
            String role, KafkaObjectAuthorizationRealTest.Fixture fixture, int partition, int lane) {
        return new Binding(role, fixture, fixture.partition(partition, 6, 7), lane);
    }

    private static void write(Binding row) {
        var fixture = row.fixture();
        var candidate = row.partition().prepare(fixture, body());
        fixture.pipeline.writeResolveAndInstallShared(
                fixture.plan(List.of(candidate), row.lane()),
                List.of(new KafkaNwg1ObjectPipelineV1.SharedMember(
                        candidate.ticket(), candidate.commit(), candidate.nativeState(), row.partition().tracker)),
                2);
        assertThat(row.partition().bridge.publishNext(() -> {})).isPresent();
    }

    private static CellProviderScopeId scope(String bucket) {
        return new CellProviderScopeId(
                Sha256Digest.hash(CanonicalBytes.copyOf(bucket.getBytes(StandardCharsets.UTF_8))));
    }

    private static WalRunRootRecord root(int cell, int shard, CellProviderScopeId scope) {
        var old = StorageObjectNwkcp1BackendV1Test.root();
        return new WalRunRootRecord(
                shard,
                1,
                new Id128(9300 + cell, shard),
                old.openedAtMillis(),
                new KafkaProtocolCellIdentity(
                        new DeploymentId(new Id128(1, 2)), new KafkaCellId(new Id128(9300, cell))),
                scope,
                old.formatContract(),
                old.nwg1AdmissionCaps(),
                old.bounds(),
                old.checkpointPolicy(),
                old.providerConfiguration(),
                old.recoveryEnvelope(),
                old.wrappedRunKey(),
                Optional.empty());
    }

    private static void sample(
            int shards,
            Binding row,
            String phase,
            long latency,
            long assembly,
            long recoveryNanos,
            long recoveryGets,
            long recoveryBytes) {
        var fixture = row.fixture();
        System.out.println("M5_CELL_PUT_SAMPLE {\"schema\":\"NEREUS_V2_M5_CELL_PUT_SAMPLE_V1\",\"shards\":" + shards
                + ",\"role\":\"" + row.role() + "\",\"phase\":\"" + phase + "\",\"shard\":" + fixture.root.shardId()
                + ",\"bindingId\":\""
                + row.partition().fence.bindingId().digest().toHex() + "\",\"lane\":" + row.lane()
                + ",\"latencyNanos\":" + latency + ",\"assemblyNanos\":" + assembly
                + ",\"pendingUnits\":" + row.partition().tracker.pendingUnits()
                + ",\"reservedLocatorBytes\":" + row.partition().tracker.reservedLocatorBytes()
                + ",\"uncoveredBodyBytes\":" + fixture.publisher.uncoveredBodyBytes()
                + ",\"providerPutCalls\":" + fixture.provider.puts + ",\"providerFullGets\":"
                + fixture.provider.fullGets
                + ",\"providerPutBytes\":" + fixture.provider.putBytes + ",\"providerGetBytes\":"
                + fixture.provider.getBytes
                + ",\"recoveryNanos\":" + recoveryNanos + ",\"recoveryGets\":" + recoveryGets
                + ",\"recoveryBytes\":" + recoveryBytes + "}");
    }

    private static void await(CountDownLatch release) {
        try {
            if (!release.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("native PUT reply hold expired");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(failure);
        }
    }

    private static final class Fixtures implements AutoCloseable {
        private final List<KafkaObjectAuthorizationRealTest.Fixture> values = new ArrayList<>();

        KafkaObjectAuthorizationRealTest.Fixture add(KafkaObjectAuthorizationRealTest.Fixture value) {
            values.add(value);
            return value;
        }

        @Override
        public void close() throws Exception {
            Exception first = null;
            for (int index = values.size() - 1; index >= 0; index--) {
                try {
                    values.get(index).close();
                } catch (Exception failure) {
                    if (first == null) {
                        first = failure;
                    } else {
                        first.addSuppressed(failure);
                    }
                }
            }
            if (first != null) {
                throw first;
            }
        }
    }
}
