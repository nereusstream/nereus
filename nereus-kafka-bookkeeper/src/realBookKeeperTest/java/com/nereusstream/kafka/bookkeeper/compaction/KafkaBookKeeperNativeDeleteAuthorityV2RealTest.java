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

package com.nereusstream.kafka.bookkeeper.compaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperOxiaControlV2RealTest.NativeContext;
import com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.oxia.v2.retention.OxiaTargetDeleteAuthorityStoreV2;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateClientV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateSpecV2;
import com.nereusstream.storage.object.gc.DeleteObservationContextV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityStateMachineV1;
import com.nereusstream.storage.object.gc.SyntheticDeleteAuthorityFixturesV2;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Actual native GC ownership/intent and Oxia CAS; protocol eligibility, M4 and dispatch admission remain synthetic. */
@Timeout(value = 3, unit = TimeUnit.MINUTES)
class KafkaBookKeeperNativeDeleteAuthorityV2RealTest {
    @Test
    void actualGcEpochFactsTakeOverIntentAndBindNativeDeleteBeforeExactAbsenceCompletion() throws Exception {
        try (var fixture = new Fixture(1001);
                var second = connect(fixture.input, fixture.spec)) {
            var first = fixture.authority(fixture.client, UUID.randomUUID());
            var epoch = await(first.claim(Optional.empty()));
            var observation = await(first.observe(1, Optional.empty()));
            var nativeFacts = first.readOnlyFacts(fixture.facts);
            assertThat(await(nativeFacts.read(first.factKey())).orElseThrow().canonicalStoredBytes())
                    .isEqualTo(epoch.encode());
            // A same-named Oxia value cannot shadow the actual native fact family.
            await(fixture.facts.compareAndSet(Optional.empty(), first.factKey(), bytes("forged shadow")));
            assertThat(await(nativeFacts.read(first.factKey())).orElseThrow().canonicalStoredBytes())
                    .isEqualTo(epoch.encode());
            assertThatThrownBy(
                            () -> await(nativeFacts.compareAndSet(Optional.empty(), first.factKey(), bytes("forged"))))
                    .hasRootCauseMessage("native GC fact route is read-only");
            var route = fixture.route(first);
            var coordinator = fixture.coordinator(first, route);
            var intent = fixture.intent(coordinator, observation);
            var nativeIntent = await(first.bindIntent(route, intent));
            assertThat(nativeIntent.intentAuthoritySha256()).isEqualTo(intent.canonicalStoredSha256());
            assertThat(nativeIntent.dispatchTokenSha256())
                    .isEqualTo(M5TargetDeleteAuthorityCodecV1.decodeAuthority(intent.canonicalStoredBytes())
                            .deleteIntent()
                            .orElseThrow()
                            .dispatchTokenSha256());
            assertThat(await(first.bindIntent(route, intent))).isEqualTo(nativeIntent);

            var successor = fixture.authority(second, UUID.randomUUID());
            await(successor.claim(Optional.of(epoch)));
            var nextObservation = await(successor.observe(2, Optional.of(observation)));
            await(successor.requirePredecessorFenced(fixture.resource, observation, nextObservation));
            assertThatThrownBy(() -> await(first.bindIntent(route, intent)))
                    .hasRootCauseMessage("native GC owner differs from this coordinator");
            var nextRoute = fixture.route(successor);
            var nextCoordinator = fixture.coordinator(successor, nextRoute);
            var refreshed = await(nextCoordinator.refreshDispatch(
                            intent,
                            nextObservation,
                            SyntheticDeleteAuthorityFixturesV2.replacement(fixture.resource, 4, fixture.fact)))
                    .observed()
                    .orElseThrow();
            var nextNativeIntent = await(successor.bindIntent(nextRoute, refreshed));
            assertThat(nextNativeIntent.nativeVersion()).isEqualTo(nativeIntent.nativeVersion() + 1);
            assertThat(nextNativeIntent.encode().length())
                    .isEqualTo(nativeIntent.encode().length());
            var target = await(second.captureExactTarget(fixture.handle))
                    .exactTarget()
                    .orElseThrow();
            assertThatThrownBy(() ->
                            await(fixture.client.deleteAuthority(fixture.handle).deleteExact(nativeIntent, target)))
                    .hasRootCauseMessage("native delete owner is fenced");
            assertThatThrownBy(() -> await(nextCoordinator.completeAbsent(refreshed)))
                    .hasRootCauseMessage("native target absence was not established");
            // Explicit low-level fixture deletion: full grace/Cell/protocol admission is not supplied by this test.
            assertThat(await(second.deleteAuthority(fixture.handle).deleteExact(nextNativeIntent, target))
                            .outcome())
                    .isEqualTo(M5BookKeeperDeleteAdapterV1.DeleteOutcome.AUTHORITATIVELY_ABSENT);
            var done =
                    await(nextCoordinator.completeAbsent(refreshed)).observed().orElseThrow();
            assertThat(await(nextCoordinator.compactDone(done)).exactTerminalIsAuthoritative())
                    .isTrue();
            assertThat(await(nextCoordinator.completeAbsent(refreshed)).exactTerminalIsAuthoritative())
                    .isTrue();
            assertThatThrownBy(() -> await(coordinator.completeAbsent(intent)))
                    .hasRootCauseMessage("native GC owner differs from this coordinator");
            assertThat(await(second.deleteAuthority(fixture.handle).readIntent()))
                    .contains(nextNativeIntent);
            assertThat(fixture.context.onOwner(fixture.context.m4::readSelector))
                    .isEqualTo(fixture.selectedBefore);
        }
    }

    @Test
    void wrongOwnerForgedFactAndUnadvancedEpochAreRejectedWhileSameOwnerNativeAdvanceRefreshesReadFence()
            throws Exception {
        try (var fixture = new Fixture(1002)) {
            UUID owner = UUID.randomUUID();
            var authority = fixture.authority(fixture.client, owner);
            var epoch = await(authority.claim(Optional.empty()));
            var observation = await(authority.observe(1, Optional.empty()));
            var otherOwner = fixture.authority(fixture.client, UUID.randomUUID());
            assertThatThrownBy(() -> await(otherOwner.requireCurrent(fixture.resource, observation)))
                    .hasRootCauseMessage("native GC owner differs from this coordinator");
            var fact = observation.coordinatorOwner();
            var forged = new AuthorityFactV1(
                    fact.key(), fact.metadataVersion(), SyntheticDeleteAuthorityFixturesV2.digest("forged"));
            assertThatThrownBy(() -> await(authority.requireCurrent(
                            fixture.resource, new DeleteObservationContextV2(1, forged, forged, Optional.empty()))))
                    .hasRootCauseMessage("native GC observation does not match actual epoch bytes/version");
            assertThatThrownBy(() -> await(authority.requirePredecessorFenced(
                            fixture.resource,
                            observation,
                            new DeleteObservationContextV2(2, fact, fact, Optional.of(fact)))))
                    .hasRootCauseMessage("native predecessor epoch was not advanced");
            var route = fixture.route(authority);
            var coordinator = fixture.coordinator(authority, route);
            var fenced = fixture.fenced(coordinator, observation);
            var intermediate = await(authority.claim(Optional.of(epoch)));
            await(authority.claim(Optional.of(intermediate)));
            var successor = await(authority.observe(2, Optional.of(observation)));
            // Native epoch can advance while a previous Oxia refresh loses; observation epoch still advances once.
            assertThat(successor.observationEpoch()).isEqualTo(2);
            assertThat(successor.coordinatorOwner()).isNotEqualTo(observation.coordinatorOwner());
            var refreshed = await(coordinator.refreshIdentityRead(
                            fenced,
                            successor,
                            SyntheticDeleteAuthorityFixturesV2.replacement(fixture.resource, 3, fixture.fact)))
                    .observed()
                    .orElseThrow();
            assertThat(M5TargetDeleteAuthorityCodecV1.decodeAuthority(refreshed.canonicalStoredBytes())
                            .readFence()
                            .orElseThrow()
                            .observationContext())
                    .isEqualTo(successor);
            assertThatThrownBy(() -> await(
                            authority.readOnlyFacts(fixture.facts).read("v2/bk-native-delete-epoch/foreign/fact-v1")))
                    .hasRootCauseMessage("native GC fact route differs");
        }
    }

    @Test
    void intentChangedAfterNativeBindingRetainsOldBindingAndRequiresEpochRefreshBeforeNewToken() throws Exception {
        try (var fixture = new Fixture(1003)) {
            var authority = fixture.authority(fixture.client, UUID.randomUUID());
            var epoch = await(authority.claim(Optional.empty()));
            var observation = await(authority.observe(1, Optional.empty()));
            var route = fixture.route(authority);
            var coordinator = fixture.coordinator(authority, route);
            var intent = fixture.intent(coordinator, observation);
            var sameNativeEpoch = await(authority.observe(2, Optional.of(observation)));
            var reads = new AtomicInteger();
            var replacement = new CompletableFuture<VersionedValue>();
            var delayedMetadata = new ExactMetadataTransactionStoreV1() {
                public CompletionStage<Optional<VersionedValue>> read(String key) {
                    if (reads.incrementAndGet() == 2) {
                        return coordinator
                                .refreshDispatch(
                                        intent,
                                        sameNativeEpoch,
                                        SyntheticDeleteAuthorityFixturesV2.replacement(
                                                fixture.resource, 4, fixture.fact))
                                .thenCompose(result -> {
                                    replacement.complete(result.observed().orElseThrow());
                                    return route.read(key);
                                });
                    }
                    return route.read(key);
                }

                public CompletionStage<MutationOutcome> compareAndSet(
                        Optional<VersionedValue> previous, String key, CanonicalBytes candidate) {
                    return CompletableFuture.failedFuture(new UnsupportedOperationException());
                }

                public CompletionStage<TransactionOutcome> conditionalTransaction(ExactTransaction transaction) {
                    return CompletableFuture.completedFuture(TransactionOutcome.UNSUPPORTED);
                }

                public boolean supportsAtomicMultiKeyTransactions() {
                    return false;
                }
            };
            assertThatThrownBy(() -> await(authority.bindIntent(delayedMetadata, intent)))
                    .hasRootCauseMessage("M5 intent is no longer the exact stored authority");
            var changed = await(replacement);
            assertThat(await(fixture.client.deleteAuthority(fixture.handle).readIntent())
                            .orElseThrow()
                            .intentAuthoritySha256())
                    .isEqualTo(intent.canonicalStoredSha256());
            assertThatThrownBy(() -> await(authority.bindIntent(route, changed)))
                    .hasRootCauseMessage("changed native intent requires a newer GC epoch");
            await(authority.claim(Optional.of(epoch)));
            var next = await(authority.observe(3, Optional.of(sameNativeEpoch)));
            var recovered = await(coordinator.refreshDispatch(
                            changed,
                            next,
                            SyntheticDeleteAuthorityFixturesV2.replacement(fixture.resource, 5, fixture.fact)))
                    .observed()
                    .orElseThrow();
            assertThat(await(authority.bindIntent(route, recovered)).intentAuthoritySha256())
                    .isEqualTo(recovered.canonicalStoredSha256());
            assertThat(await(fixture.client.captureExactTarget(fixture.handle)).exactTarget())
                    .isPresent();
        }
    }

    private static final class Fixture implements AutoCloseable {
        final KafkaBookKeeperCompactionTestSupportV2.Input input;
        final M5BookKeeperNativeCreateSpecV2 spec;
        final M5BookKeeperNativeCreateClientV2 client;
        final NativeContext context;
        final RunLedgerHandleV1 handle;
        final PhysicalResourceIdV2.BookKeeperLedger resource;
        final Oxia09ExactMetadataTransactionStoreV1 facts;
        final BiFunction<String, CanonicalBytes, AuthorityFactV1> fact;
        final Object selectedBefore;

        Fixture(long attempt) throws Exception {
            input = KafkaBookKeeperNativeCreateV2RealTest.nativeInput(false, attempt);
            spec = KafkaBookKeeperNativeCreateV2RealTest.spec(input);
            client = connect(input, spec);
            context = new NativeContext(input.layout().task(), NativeContext.root(), client.newSession());
            var descriptor = context.write(input);
            selectedBefore = context.onOwner(context.m4::readSelector);
            handle = descriptor.sealedParts().get(0).handle();
            resource = new PhysicalResourceIdV2.BookKeeperLedger(
                    spec.namespace(), handle.ledgerIdentity().ledgerId());
            await(client.fenceCreates());
            facts = new Oxia09ExactMetadataTransactionStoreV1(context.faults);
            fact = (suffix, bytes) -> {
                String key = context.root + "/synthetic-delete-protocol" + suffix;
                var prior = await(facts.read(key));
                if (prior.isEmpty()) {
                    await(facts.compareAndSet(Optional.empty(), key, bytes));
                }
                var stored = await(facts.read(key)).orElseThrow();
                assertThat(stored.canonicalStoredBytes()).isEqualTo(bytes);
                return new AuthorityFactV1(key, stored.metadataVersion(), stored.canonicalStoredSha256());
            };
        }

        KafkaBookKeeperDeleteObservationAuthorityV2 authority(M5BookKeeperNativeCreateClientV2 connection, UUID owner) {
            return new KafkaBookKeeperDeleteObservationAuthorityV2(connection, handle, owner);
        }

        OxiaTargetDeleteAuthorityStoreV2 route(KafkaBookKeeperDeleteObservationAuthorityV2 authority) {
            return new OxiaTargetDeleteAuthorityStoreV2(
                    context.faults, context.root + "/delete", spec.namespace(), authority.readOnlyFacts(facts));
        }

        M5TargetDeleteAuthorityCoordinatorV1 coordinator(
                KafkaBookKeeperDeleteObservationAuthorityV2 authority, ExactMetadataTransactionStoreV1 route) {
            return new M5TargetDeleteAuthorityCoordinatorV1(
                    route, authority, new KafkaBookKeeperDeleteIdentityReaderV2(client, handle));
        }

        VersionedValue fenced(
                M5TargetDeleteAuthorityCoordinatorV1 coordinator, DeleteObservationContextV2 observation) {
            var template = SyntheticDeleteAuthorityFixturesV2.phases(resource).get(0);
            var open = await(coordinator.create(M5TargetDeleteAuthorityStateMachineV1.open(
                            template.target(),
                            template.writerEnrollment(),
                            SyntheticDeleteAuthorityFixturesV2.replacement(resource, 1, fact))))
                    .observed()
                    .orElseThrow();
            return await(coordinator.prepareIdentityRead(
                            open, SyntheticDeleteAuthorityFixturesV2.digest("native-read"), observation))
                    .observed()
                    .orElseThrow();
        }

        VersionedValue intent(
                M5TargetDeleteAuthorityCoordinatorV1 coordinator, DeleteObservationContextV2 observation) {
            var fenced = fenced(coordinator, observation);
            var reader = new KafkaBookKeeperDeleteIdentityReaderV2(client, handle);
            var external = await(
                    reader.capture(M5TargetDeleteAuthorityCodecV1.decodeAuthority(fenced.canonicalStoredBytes())));
            return await(coordinator.bindDeleteIntent(
                            fenced, external, SyntheticDeleteAuthorityFixturesV2.digest("native-delete")))
                    .observed()
                    .orElseThrow();
        }

        public void close() throws Exception {
            try {
                context.close();
            } finally {
                client.close();
            }
        }
    }

    private static M5BookKeeperNativeCreateClientV2 connect(
            KafkaBookKeeperCompactionTestSupportV2.Input input, M5BookKeeperNativeCreateSpecV2 spec) throws Exception {
        return M5BookKeeperNativeCreateClientV2.connect(
                System.getProperty("nereus.bookkeeper.metadataServiceUri"),
                input.layout().task().capability(),
                spec);
    }

    private static CanonicalBytes bytes(String value) {
        return CanonicalUtf8.fromString(value).bytes();
    }

    private static <T> T await(CompletionStage<T> stage) {
        try {
            return stage.toCompletableFuture().get(30, TimeUnit.SECONDS);
        } catch (Exception failure) {
            throw new IllegalStateException("native GC test operation failed", failure);
        }
    }
}
