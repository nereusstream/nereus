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
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperRunRootsV2RealTest.Fixture;
import com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMetadataNamespaceV2;
import com.nereusstream.metadata.oxia.v2.retention.OxiaQuotaTargetDeleteStoreV2;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity;
import com.nereusstream.storage.api.bookkeeper.RunLedgerAppendRequestV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.bookkeeper.ImmutableRetainedStoragePayload;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1;
import com.nereusstream.storage.object.gc.DeleteObservationContextV2;
import com.nereusstream.storage.object.gc.M5GcQuotaCoordinatorV2;
import com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityStateMachineV1;
import com.nereusstream.storage.object.gc.SyntheticDeleteAuthorityFixturesV2;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Actual bound route/quota/GC restart; protocol eligibility, grace and dispatch capacity are not certified. */
@Timeout(value = 4, unit = TimeUnit.MINUTES)
class KafkaBookKeeperBoundDeleteV2RealTest {
    @Test
    void boundNativeClientRequiresActiveQuotaAuthorityAndRejectsRawRoutesBeforeEpochOrIntentMutation()
            throws Exception {
        try (var f = new Fixture(8801, "gc-bound-route", null)) {
            var handle = sealed(f);
            var resource = resource(f, handle);
            assertThat(await(f.source.requireNamespaceBinding())).isEqualTo(f.binding);
            var gc = new KafkaBookKeeperDeleteObservationAuthorityV2(f.source, handle, UUID.randomUUID());
            var facts = facts(f, resource);
            var route = route(f, gc);
            var raw = new OxiaQuotaTargetDeleteStoreV2(
                    f.oxia,
                    new M5GcQuotaRecordsV2.Layout(f.binding.authorityRoot(), f.binding.physicalNamespace()),
                    gc.readOnlyFacts(new Oxia09ExactMetadataTransactionStoreV1(f.oxia)));
            assertThatThrownBy(() -> await(gc.claim(raw, Optional.empty())))
                    .hasRootCauseMessage("native physical namespace route admission is not installed");
            assertThatThrownBy(() -> await(gc.claim(route, Optional.empty())))
                    .hasRootCauseMessage("authority lacks a permanent quota reservation");
            assertThat(await(route.quota().reserve(resource))).isEqualTo(M5GcQuotaCoordinatorV2.Result.GRANTED);
            assertThatThrownBy(() -> await(gc.claim(route, Optional.empty())))
                    .hasRootCauseMessage("reserved native resource has no active authority");
            assertThat(await(f.source.deleteAuthority(handle).read())).isEmpty();
            var coordinator = coordinator(f, handle, gc, route);
            var open = open(coordinator, resource, facts);
            var epoch = await(gc.claim(route, Optional.empty()));
            var observation = await(gc.observe(1, Optional.empty()));
            var intent = intent(f, handle, coordinator, open, observation);
            assertThatThrownBy(() -> await(gc.bindIntent(raw, intent)))
                    .hasRootCauseMessage("native physical namespace route admission is not installed");
            assertThat(await(f.source.deleteAuthority(handle).readIntent())).isEmpty();
            var nativeIntent = await(gc.bindIntent(route, intent));
            assertThat(nativeIntent.epoch()).isEqualTo(epoch);
            assertThat(nativeIntent.intentAuthoritySha256()).isEqualTo(intent.canonicalStoredSha256());
            assertThat(await(route.requireActiveResource(resource))).isEqualTo(f.binding);
            assertThat(await(f.source.captureExactTarget(handle)).exactTarget()).isPresent();
        }
    }

    @Test
    void writeBeforeServerRestart() throws Exception {
        try (var f = new Fixture(8802, "gc-bound-restart", null)) {
            var handle = sealed(f);
            var resource = resource(f, handle);
            var gc = new KafkaBookKeeperDeleteObservationAuthorityV2(f.source, handle, UUID.randomUUID());
            var route = route(f, gc);
            var coordinator = coordinator(f, handle, gc, route);
            var open = open(coordinator, resource, facts(f, resource));
            var epoch = await(gc.claim(route, Optional.empty()));
            var observation = await(gc.observe(1, Optional.empty()));
            var intent = intent(f, handle, coordinator, open, observation);
            var nativeIntent = await(gc.bindIntent(route, intent));
            Files.write(
                    checkpoint(),
                    List.of(
                            f.source.spec().encode().toHex(),
                            Long.toString(handle.ledgerIdentity().ledgerId()),
                            intent.key(),
                            intent.canonicalStoredSha256().toHex(),
                            intent.metadataVersion().value().toHex(),
                            Sha256Digest.hash(epoch.encode()).toHex(),
                            Sha256Digest.hash(nativeIntent.encode()).toHex()));
        }
    }

    @Test
    void readAfterServerRestart() throws Exception {
        var lines = Files.readAllLines(checkpoint());
        assertThat(lines).hasSize(7);
        try (var f = new Fixture(8802, "gc-bound-restart", List.of(lines.get(0)))) {
            var config = f.source.spec().configurations().get(0);
            var handle = new RunLedgerHandleV1(
                    config.providerScopeId(),
                    config.runId(),
                    new BookKeeperLedgerIdentity(Long.parseLong(lines.get(1))),
                    config.configurationDigest());
            var resource = resource(f, handle);
            var nativeAuthority = f.source.deleteAuthority(handle);
            var previous = await(nativeAuthority.read()).orElseThrow();
            var previousIntent = await(nativeAuthority.readIntent()).orElseThrow();
            assertThat(Sha256Digest.hash(previous.encode()).toHex()).isEqualTo(lines.get(5));
            assertThat(Sha256Digest.hash(previousIntent.encode()).toHex()).isEqualTo(lines.get(6));
            var gc = new KafkaBookKeeperDeleteObservationAuthorityV2(f.source, handle, UUID.randomUUID());
            var route = route(f, gc);
            var stored = await(route.read(lines.get(2))).orElseThrow();
            assertThat(stored.canonicalStoredSha256().toHex()).isEqualTo(lines.get(3));
            assertThat(stored.metadataVersion().value().toHex()).isEqualTo(lines.get(4));
            assertThat(previousIntent.intentAuthoritySha256()).isEqualTo(stored.canonicalStoredSha256());
            assertThat(await(route.requireActiveResource(resource))).isEqualTo(f.binding);
            var priorValue = M5TargetDeleteAuthorityCodecV1.decodeAuthority(stored.canonicalStoredBytes());
            var priorObservation = M5TargetDeleteAuthorityStateMachineV1.dispatchContext(priorValue);
            assertThat(priorObservation.coordinatorOwner().valueSha256())
                    .isEqualTo(Sha256Digest.hash(previous.encode()));
            // All three durable records have been read and matched before any GC/M5 mutation.
            var current = await(gc.claim(route, Optional.of(previous)));
            var observation = await(gc.observe(priorObservation.observationEpoch() + 1, Optional.of(priorObservation)));
            var coordinator = coordinator(f, handle, gc, route);
            var refreshed = await(coordinator.refreshDispatch(
                            stored,
                            observation,
                            SyntheticDeleteAuthorityFixturesV2.replacement(
                                    resource, priorValue.authorityRevision() + 1, facts(f, resource))))
                    .observed()
                    .orElseThrow();
            var bound = await(gc.bindIntent(route, refreshed));
            var target =
                    await(f.source.captureExactTarget(handle)).exactTarget().orElseThrow();
            assertThatThrownBy(() -> await(nativeAuthority.deleteExact(previousIntent, target)))
                    .hasRootCauseMessage("native delete owner is fenced");
            // Low-level deletion of this test-owned fixture. Full protocol/grace/Cell admission is still separate.
            assertThat(await(nativeAuthority.deleteExact(bound, target)).outcome())
                    .isEqualTo(M5BookKeeperDeleteAdapterV1.DeleteOutcome.AUTHORITATIVELY_ABSENT);
            var done = await(coordinator.completeAbsent(refreshed)).observed().orElseThrow();
            await(coordinator.compactDone(done));
            assertThat(await(route.quota().settle(resource))).isEqualTo(M5GcQuotaCoordinatorV2.Result.SETTLED);
            assertThatThrownBy(() -> await(gc.claim(route, Optional.of(current))))
                    .hasRootCauseMessage("native resource quota is settled");
            assertThat(await(nativeAuthority.read())).contains(current);
            assertThat(await(coordinator.completeAbsent(refreshed)).exactTerminalIsAuthoritative())
                    .isTrue();
        }
    }

    private static RunLedgerHandleV1 sealed(Fixture f) throws Exception {
        var id = await(f.session.reserveLedgerIdentity()).exactProof().orElseThrow();
        var handle = await(f.session.createReservedRunLedger(
                        f.source.spec().configurations().get(0), id))
                .exactProof()
                .orElseThrow();
        var payload = ImmutableRetainedStoragePayload.copyOf(new byte[] {4, 5, 6});
        try {
            assertThat(await(f.session.appendExplicitEntry(new RunLedgerAppendRequestV1(handle, 0, payload)))
                            .exactProof())
                    .isPresent();
        } finally {
            payload.release();
        }
        assertThat(await(f.session.closeRunLedger(handle)).exactProof()).isPresent();
        await(f.source.fenceCreates());
        return handle;
    }

    private static PhysicalResourceIdV2.BookKeeperLedger resource(Fixture f, RunLedgerHandleV1 handle) {
        return new PhysicalResourceIdV2.BookKeeperLedger(
                f.binding.physicalNamespace(), handle.ledgerIdentity().ledgerId());
    }

    private static OxiaQuotaTargetDeleteStoreV2 route(Fixture f, KafkaBookKeeperDeleteObservationAuthorityV2 gc) {
        var namespace = await(OxiaPhysicalMetadataNamespaceV2.connect(f.oxia, f.binding.metadataNamespace()));
        return await(namespace.openAuthorityRoute(
                f.backend, gc.readOnlyFacts(new Oxia09ExactMetadataTransactionStoreV1(f.oxia))));
    }

    private static BiFunction<String, CanonicalBytes, AuthorityFactV1> facts(Fixture f, PhysicalResourceIdV2 resource) {
        var facts = new Oxia09ExactMetadataTransactionStoreV1(f.oxia);
        return (suffix, bytes) -> {
            String key = "/native-gc-bound-facts/" + resource.sha256().toHex() + suffix;
            if (await(facts.read(key)).isEmpty()) {
                await(facts.compareAndSet(Optional.empty(), key, bytes));
            }
            var actual = await(facts.read(key)).orElseThrow();
            assertThat(actual.canonicalStoredBytes()).isEqualTo(bytes);
            return new AuthorityFactV1(key, actual.metadataVersion(), actual.canonicalStoredSha256());
        };
    }

    private static M5TargetDeleteAuthorityCoordinatorV1 coordinator(
            Fixture f,
            RunLedgerHandleV1 handle,
            KafkaBookKeeperDeleteObservationAuthorityV2 gc,
            OxiaQuotaTargetDeleteStoreV2 route) {
        return new M5TargetDeleteAuthorityCoordinatorV1(
                route, gc, new KafkaBookKeeperDeleteIdentityReaderV2(f.source, handle));
    }

    private static VersionedValue open(
            M5TargetDeleteAuthorityCoordinatorV1 coordinator,
            PhysicalResourceIdV2 resource,
            BiFunction<String, CanonicalBytes, AuthorityFactV1> facts) {
        var template = SyntheticDeleteAuthorityFixturesV2.phases(resource).get(0);
        return await(coordinator.create(M5TargetDeleteAuthorityStateMachineV1.open(
                        template.target(),
                        template.writerEnrollment(),
                        SyntheticDeleteAuthorityFixturesV2.replacement(resource, 1, facts))))
                .observed()
                .orElseThrow();
    }

    private static VersionedValue intent(
            Fixture f,
            RunLedgerHandleV1 handle,
            M5TargetDeleteAuthorityCoordinatorV1 coordinator,
            VersionedValue open,
            DeleteObservationContextV2 observation) {
        var fenced = await(coordinator.prepareIdentityRead(
                        open, SyntheticDeleteAuthorityFixturesV2.digest("bound-read"), observation))
                .observed()
                .orElseThrow();
        var reader = new KafkaBookKeeperDeleteIdentityReaderV2(f.source, handle);
        var identity =
                await(reader.capture(M5TargetDeleteAuthorityCodecV1.decodeAuthority(fenced.canonicalStoredBytes())));
        return await(coordinator.bindDeleteIntent(
                        fenced, identity, SyntheticDeleteAuthorityFixturesV2.digest("bound-delete")))
                .observed()
                .orElseThrow();
    }

    private static Path checkpoint() {
        return Path.of(System.getProperty("nereus.m5.boundDelete.restartCheckpoint"));
    }

    private static <T> T await(CompletionStage<T> stage) {
        try {
            return stage.toCompletableFuture().get(30, TimeUnit.SECONDS);
        } catch (Exception failure) {
            throw new IllegalStateException("bound GC test operation failed", failure);
        }
    }
}
