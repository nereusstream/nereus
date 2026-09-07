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

package com.nereusstream.metadata.oxia.v2.retention;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.oxia.v2.mutation.AsyncOxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.mutation.AuthorityRecord;
import com.nereusstream.metadata.oxia.v2.mutation.OxiaConditionalClient;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.object.gc.M5PermanentDoneCacheV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterTicketV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteDoneV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteWriterGuardV1;
import com.nereusstream.storage.object.gc.SyntheticDeleteAuthorityFixturesV2;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Actual source-locked Oxia state and CAS. Deletion eligibility, owner and absence proofs remain synthetic. */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class M5PermanentDoneOxiaIntegrationTest {
    @Test
    void continuousNativeTerminalsOutliveResidentCapAndFreshClientsRejectRediscovery() throws Exception {
        String root = Fixture.root();
        try (var fixture = new Fixture(root)) {
            for (int id = 0; id < 258; id++) {
                var terminal = fixture.complete(id);
                assertThat(terminal.encode().length()).isLessThan(1024);
                assertThat(fixture.cache.snapshot().entries()).isLessThanOrEqualTo(8);
                assertThat(fixture.cache.snapshot().encodedBytes()).isLessThanOrEqualTo(8192);
                if (id % 64 == 0) {
                    System.out.printf(
                            java.util.Locale.ROOT,
                            "M5_NATIVE_DONE completed=%d resident=%d bytes=%d evictions=%d%n",
                            id + 1,
                            fixture.cache.snapshot().entries(),
                            fixture.cache.snapshot().encodedBytes(),
                            fixture.cache.snapshot().evictions());
                }
            }
            assertThat(fixture.cache.snapshot().evictions()).isGreaterThan(240);
            long reads = fixture.cache.snapshot().authoritativeReads();
            assertThat(fixture.done(0).resource()).isEqualTo(SyntheticDeleteAuthorityFixturesV2.resource(0));
            assertThat(fixture.done(257).resource()).isEqualTo(SyntheticDeleteAuthorityFixturesV2.resource(257));
            assertThat(fixture.cache.snapshot().authoritativeReads()).isGreaterThan(reads);
        }
        try (var reopened = new Fixture(root)) {
            for (int id : new int[] {0, 128, 257}) {
                var done = reopened.done(id);
                assertThat(done.resource()).isEqualTo(SyntheticDeleteAuthorityFixturesV2.resource(id));
                reopened.rejectRediscovery(id);
            }
            reopened.complete(258);
            assertThat(reopened.done(258)).isNotNull();
        }
    }

    @Test
    void lostAppliedCompactionAndUnavailableReadReconcileWithoutReopening() throws Exception {
        try (var fixture = new Fixture(Fixture.root())) {
            var full = fixture.full(1);
            fixture.faults.loseCas = true;
            var result = Fixture.await(fixture.coordinator.compactDone(full));
            assertThat(result.exactCandidateIsAuthoritative()).isTrue();
            assertThat(Fixture.await(fixture.coordinator.compactDone(full)).exactCandidateIsAuthoritative())
                    .isTrue();
            var expected = fixture.done(1);
            var fresh = new M5TargetDeleteAuthorityCoordinatorV1(new M5PermanentDoneCacheV2(fixture.route, 1, 4096));
            fixture.faults.loseRead = true;
            assertThatThrownBy(() -> Fixture.await(fresh.inspect(expected.authorityKey())))
                    .hasRootCauseMessage("injected native read delivery loss");
            assertThat(Fixture.await(fresh.inspect(expected.authorityKey()))
                            .orElseThrow()
                            .compactDone())
                    .contains(expected);
            fixture.rejectRediscovery(1);
        }
    }

    @Test
    void fixedCompletionRetryAfterAnotherClientCompactsNativeDoneRemainsExactlyTerminal() throws Exception {
        String root = Fixture.root();
        try (var first = new Fixture(root);
                var other = new Fixture(root)) {
            var phases = SyntheticDeleteAuthorityFixturesV2.phases(SyntheticDeleteAuthorityFixturesV2.resource(1));
            var current = Optional.<VersionedValue>empty();
            for (var phase : phases.subList(0, 3)) {
                current = Optional.of(first.persist(current, phase));
            }
            var intent = current.orElseThrow();
            var full = first.persist(current, phases.get(3));
            assertThat(Fixture.await(other.coordinator.compactDone(full)).exactTerminalIsAuthoritative())
                    .isTrue();
            var done = phases.get(3).deleteDone().orElseThrow();
            var retry = Fixture.await(first.coordinator.completeDelete(
                    intent,
                    done.terminalOutcome(),
                    done.absenceInventoryRootSha256(),
                    done.completionProofDigestSha256()));
            assertThat(retry.outcome()).isEqualTo(M5TargetDeleteAuthorityCoordinatorV1.Outcome.EXISTING_TERMINAL);
            assertThat(retry.exactTerminalIsAuthoritative()).isTrue();
            assertThat(retry.exactCandidateIsAuthoritative()).isFalse();
            assertThat(Fixture.await(first.coordinator.completeDelete(
                                    intent,
                                    done.terminalOutcome(),
                                    done.absenceInventoryRootSha256(),
                                    digest("wrong completion")))
                            .exactTerminalIsAuthoritative())
                    .isFalse();
            first.rejectRediscovery(1);
        }
    }

    @Test
    void heldTicketCasCannotDispatchAfterExactNativeTerminalCompaction() throws Exception {
        String root = Fixture.root();
        try (var first = new Fixture(root);
                var other = new Fixture(root)) {
            var phases = SyntheticDeleteAuthorityFixturesV2.phases(SyntheticDeleteAuthorityFixturesV2.resource(1));
            var open = first.persist(Optional.empty(), phases.get(0));
            first.faults.holdCas = true;
            var ticket = new ProofBoundWriterTicketV1(
                    ProofBoundWriterClassV1.REPLICA_TOPOLOGY_V1,
                    digest("operation"),
                    digest("owner"),
                    digest("proof"),
                    digest("successor"),
                    1);
            var external = new AtomicInteger();
            var pending = new M5TargetDeleteWriterGuardV1(first.coordinator)
                    .execute(open, ticket, ignored -> {
                        external.incrementAndGet();
                        return CompletableFuture.completedFuture(
                                M5TargetDeleteWriterGuardV1.ExternalMutationResultV1.applied(digest("result")));
                    })
                    .toCompletableFuture();
            try {
                first.faults.held.get(20, TimeUnit.SECONDS);
                var current = open;
                for (var phase : phases.subList(1, phases.size())) {
                    current = other.persist(Optional.of(current), phase);
                }
                assertThat(Fixture.await(other.coordinator.compactDone(current)).exactCandidateIsAuthoritative())
                        .isTrue();
                other.complete(2);
                assertThat(pending).isNotDone();
            } finally {
                var release = first.faults.release.getAndSet(null);
                if (release != null) {
                    release.run();
                }
            }
            assertThat(pending.get(20, TimeUnit.SECONDS).externalMutationInvoked())
                    .isFalse();
            assertThat(external).hasValue(0);
            assertThat(first.done(1)).isEqualTo(other.done(1));
        }
    }

    static final class Fixture implements AutoCloseable {
        final String root;
        final AsyncOxiaClient client;
        final Faults faults;
        final OxiaTargetDeleteAuthorityStoreV2 route;
        final M5PermanentDoneCacheV2 cache;
        final M5TargetDeleteAuthorityCoordinatorV1 coordinator;

        Fixture(String root) throws Exception {
            this.root = root;
            String address = System.getProperty("nereus.m5.retention.oxia.serviceAddress");
            if (address == null || address.isBlank() || address.equals("UNSET")) {
                throw new IllegalStateException("missing source-locked permanent-done Oxia address");
            }
            var builder = OxiaClientBuilder.create(address);
            var jar = Path.of(builder.getClass()
                    .getProtectionDomain()
                    .getCodeSource()
                    .getLocation()
                    .toURI());
            assertThat(Sha256Digest.hash(CanonicalBytes.copyOf(Files.readAllBytes(jar)))
                            .toHex())
                    .isEqualTo("0ca719e6d11bd2ee2c2e7e94b42c6843e60f776bea12f7b5814cff9928e2e4c5");
            client = builder.namespace("default")
                    .requestTimeout(Duration.ofSeconds(10))
                    .asyncClient()
                    .get(30, TimeUnit.SECONDS);
            faults = new Faults(new AsyncOxiaConditionalClient(client));
            route = new OxiaTargetDeleteAuthorityStoreV2(
                    faults,
                    root,
                    SyntheticDeleteAuthorityFixturesV2.resource(0).namespace(),
                    new Oxia09ExactMetadataTransactionStoreV1(faults));
            cache = new M5PermanentDoneCacheV2(route, 8, 8192);
            coordinator = new M5TargetDeleteAuthorityCoordinatorV1(cache);
        }

        VersionedValue persist(Optional<VersionedValue> current, TargetDeleteAuthorityV1 next) throws Exception {
            assertThat(await(route.compareAndSet(
                            current, next.authorityKey(), M5TargetDeleteAuthorityCodecV1.encodeAuthority(next))))
                    .isEqualTo(ExactMetadataTransactionStoreV1.MutationOutcome.APPLIED_EXACT);
            return await(route.read(next.authorityKey())).orElseThrow();
        }

        VersionedValue full(int id) throws Exception {
            Optional<VersionedValue> current = Optional.empty();
            for (var phase :
                    SyntheticDeleteAuthorityFixturesV2.phases(SyntheticDeleteAuthorityFixturesV2.resource(id))) {
                current = Optional.of(persist(current, phase));
            }
            return current.orElseThrow();
        }

        M5TargetDeleteDoneV2 complete(int id) throws Exception {
            assertThat(await(coordinator.compactDone(full(id))).exactCandidateIsAuthoritative())
                    .isTrue();
            return done(id);
        }

        M5TargetDeleteDoneV2 done(int id) throws Exception {
            return await(coordinator.inspect(
                            SyntheticDeleteAuthorityFixturesV2.resource(id).authorityKey()))
                    .orElseThrow()
                    .compactDone()
                    .orElseThrow();
        }

        void rejectRediscovery(int id) throws Exception {
            var open = SyntheticDeleteAuthorityFixturesV2.phases(SyntheticDeleteAuthorityFixturesV2.resource(id))
                    .get(0);
            var before = await(route.read(open.authorityKey()));
            assertThat(await(route.compareAndSet(
                            Optional.empty(),
                            open.authorityKey(),
                            M5TargetDeleteAuthorityCodecV1.encodeAuthority(open))))
                    .isEqualTo(ExactMetadataTransactionStoreV1.MutationOutcome.DEFINITIVE_CONFLICT);
            assertThat(await(route.read(open.authorityKey()))).isEqualTo(before);
        }

        static String root() {
            return "/nereus/m5-done-test/" + UUID.randomUUID();
        }

        static <T> T await(CompletionStage<T> stage) throws Exception {
            return stage.toCompletableFuture().get(30, TimeUnit.SECONDS);
        }

        public void close() throws Exception {
            client.close();
        }
    }

    static final class Faults implements OxiaConditionalClient {
        final OxiaConditionalClient nativeClient;
        volatile boolean loseCas;
        volatile boolean loseRead;
        volatile boolean holdCas;
        final CompletableFuture<Void> held = new CompletableFuture<>();
        final AtomicReference<Runnable> release = new AtomicReference<>();

        Faults(OxiaConditionalClient nativeClient) {
            this.nativeClient = nativeClient;
        }

        public CompletionStage<Optional<AuthorityRecord>> read(String key) {
            var result = nativeClient.read(key);
            if (loseRead) {
                loseRead = false;
                return result.thenCompose(ignored -> CompletableFuture.failedFuture(
                        new IllegalStateException("injected native read delivery loss")));
            }
            return result;
        }

        public CompletionStage<Void> createIfAbsent(String key, CanonicalBytes value) {
            return nativeClient.createIfAbsent(key, value);
        }

        public CompletionStage<Void> compareAndSet(String key, CanonicalBytes value, long version) {
            if (holdCas) {
                holdCas = false;
                var result = new CompletableFuture<Void>();
                release.set(
                        () -> nativeClient.compareAndSet(key, value, version).whenComplete((ignored, failure) -> {
                            if (failure == null) {
                                result.complete(null);
                            } else {
                                result.completeExceptionally(failure);
                            }
                        }));
                held.complete(null);
                return result;
            }
            var result = nativeClient.compareAndSet(key, value, version);
            if (loseCas) {
                loseCas = false;
                return result.thenCompose(ignored ->
                        CompletableFuture.failedFuture(new IllegalStateException("injected native CAS delivery loss")));
            }
            return result;
        }
    }

    private static Sha256Digest digest(String value) {
        return SyntheticDeleteAuthorityFixturesV2.digest(value);
    }
}
