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

package com.nereusstream.storage.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMetadataNamespaceV2;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerAppendRequestV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerConfigurationV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1;
import com.nereusstream.storage.object.gc.SyntheticDeleteAuthorityFixturesV2;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.BookKeeper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Actual namespace assignment and delayed native creates; OPEN eligibility is synthetic and no deletion is issued. */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class M5PhysicalNamespaceOxiaBookKeeperRealTest {
    private static final BookKeeperCapabilitySnapshotV1 CAPABILITY = M5BookKeeperNativeCreateV2RealTest.capability();
    private static final byte[] PAYLOAD = new byte[] {11, 22, 33, 44};

    @Test
    void bindAndWriteBeforeServerRestart() throws Exception {
        verifyBookKeeperJar();
        try (var oxia = openOxia(false, false);
                var aliasOxia = openOxia(false, true);
                var foreignOxia = openOxia(true, false);
                var backend = M5BookKeeperNamespaceAuthorityV2.connect(uri(), CAPABILITY);
                var aliasBackend = M5BookKeeperNamespaceAuthorityV2.connect(aliasUri(), CAPABILITY)) {
            assertThat(aliasBackend.namespace()).isEqualTo(backend.namespace());
            assertThat(await(backend.readBinding())).isEmpty();
            var metadata = await(OxiaPhysicalMetadataNamespaceV2.provision(oxia));
            var aliasMetadata = await(OxiaPhysicalMetadataNamespaceV2.connect(aliasOxia, metadata.identity()));
            var foreignMetadata = await(OxiaPhysicalMetadataNamespaceV2.provision(foreignOxia));
            assertThat(foreignMetadata.identity()).isNotEqualTo(metadata.identity());
            var spec = spec(M5BookKeeperNativeCreateClientV2.discoverInstanceId(uri(), CAPABILITY));
            var binding = new PhysicalNamespaceAuthorityBindingV2(backend.namespace(), metadata.identity());
            long ledgerId;
            try (var fault = new M5BookKeeperNativeCreateV2RealTest.NativeFaults(uri(), CAPABILITY, spec);
                    var legacy = M5BookKeeperNativeCreateClientV2.connect(uri(), CAPABILITY, spec)) {
                ledgerId = fault.allocate();
                fault.guard.reserve(ledgerId).get(10, TimeUnit.SECONDS);
                fault.zk.holdNextMulti = true;
                var delayed = fault.manager.createLedgerMetadata(ledgerId, fault.metadata(ledgerId));
                fault.zk.held.get(10, TimeUnit.SECONDS);
                var gate = new M5BookKeeperNamespaceGateV2(
                        fault.zk,
                        java.net.URI.create(uri()).getPath(),
                        spec.nativeInstanceId(),
                        org.apache.bookkeeper.util.ZkUtils.getACLs(
                                RealBookKeeperClientConfigurationV1.from(uri(), CAPABILITY)));
                fault.zk.loseNextSet = true;
                assertThat(await(gate.bind(binding))).isEqualTo(binding);
                assertThat(fault.zk.lost.get()).isEqualTo(1);
                assertThat(await(metadata.bind(backend))).isEqualTo(binding);
                assertThat(await(aliasBackend.readBinding())).contains(binding);
                fault.zk.release.get().run();
                assertThatThrownBy(() -> await(delayed))
                        .hasRootCauseInstanceOf(org.apache.zookeeper.KeeperException.BadVersionException.class);
                assertThatThrownBy(() -> await(fault.manager.readLedgerMetadata(ledgerId)))
                        .hasRootCauseInstanceOf(
                                org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsOnMetadataServerException
                                        .class);
                var legacySession = legacy.newSession();
                assertThat(await(legacySession.reserveLedgerIdentity()).exactProof())
                        .isEmpty();
                assertThat(await(legacySession.createReservedRunLedger(
                                        spec.configurations().get(0), new BookKeeperLedgerIdentity(ledgerId)))
                                .exactProof())
                        .isEmpty();
                await(legacySession.closeAsync());
                assertThatThrownBy(() -> M5BookKeeperNativeCreateClientV2.connect(aliasUri(), CAPABILITY, spec))
                        .hasRootCauseMessage("native M5 create client lacks the exact bound metadata namespace");
            }
            assertThatThrownBy(() -> await(foreignMetadata.bind(aliasBackend)))
                    .hasRootCauseMessage("physical namespace is bound to another metadata namespace");
            assertThatThrownBy(() -> await(foreignMetadata.openAuthorityRoute(
                            backend, new Oxia09ExactMetadataTransactionStoreV1(foreignOxia))))
                    .hasRootCauseMessage("physical backend metadata namespace assignment differs");
            assertThatThrownBy(() -> await(OxiaPhysicalMetadataNamespaceV2.connect(foreignOxia, metadata.identity())))
                    .hasRootCauseMessage("actual metadata backend namespace differs from the physical binding");
            assertThatThrownBy(() -> M5BookKeeperNativeCreateClientV2.connect(
                            uri(),
                            CAPABILITY,
                            spec,
                            new PhysicalNamespaceAuthorityBindingV2(backend.namespace(), foreignMetadata.identity())))
                    .hasRootCauseMessage("native M5 create client lacks the exact bound metadata namespace");

            var handle = handle(spec, ledgerId);
            try (var bound = M5BookKeeperNativeCreateClientV2.connect(aliasUri(), CAPABILITY, spec, binding)) {
                var session = bound.newSession();
                assertThat(await(session.createReservedRunLedger(
                                        spec.configurations().get(0), new BookKeeperLedgerIdentity(ledgerId)))
                                .exactProof())
                        .contains(handle);
                var payload = ImmutableRetainedStoragePayload.copyOf(PAYLOAD);
                assertThat(await(session.appendExplicitEntry(new RunLedgerAppendRequestV1(handle, 0, payload)))
                                .outcome())
                        .isEqualTo(ProviderMutationOutcomeV1.APPLIED_EXACT);
                assertThat(payload.release()).isTrue();
                assertThat(await(session.closeRunLedger(handle)).outcome())
                        .isEqualTo(ProviderMutationOutcomeV1.APPLIED_EXACT);
                await(session.closeAsync());
            }
            var first = await(metadata.openAuthorityRoute(backend, new Oxia09ExactMetadataTransactionStoreV1(oxia)));
            var second = await(aliasMetadata.openAuthorityRoute(
                    aliasBackend, new Oxia09ExactMetadataTransactionStoreV1(aliasOxia)));
            await(first.initialize(2_200_000));
            var resource = new PhysicalResourceIdV2.BookKeeperLedger(backend.namespace(), ledgerId);
            var open = SyntheticDeleteAuthorityFixturesV2.phases(resource).get(0);
            assertThat(await(first.compareAndSet(
                            Optional.empty(),
                            resource.authorityKey(),
                            M5TargetDeleteAuthorityCodecV1.encodeAuthority(open))))
                    .isEqualTo(ExactMetadataTransactionStoreV1.MutationOutcome.APPLIED_EXACT);
            var exact = await(first.read(resource.authorityKey())).orElseThrow();
            assertThat(await(second.read(resource.authorityKey()))).contains(exact);
            assertThat(await(second.compareAndSet(
                            Optional.empty(),
                            resource.authorityKey(),
                            M5TargetDeleteAuthorityCodecV1.encodeAuthority(open))))
                    .isEqualTo(ExactMetadataTransactionStoreV1.MutationOutcome.DEFINITIVE_CONFLICT);
            assertThat(await(first.quota().snapshot()).head().reservedResources())
                    .isEqualTo(1);
            // Marker rewrite is a test-only backend fault on the independent foreign store. It never changes BK
            // assignment.
            await(foreignOxia.put(
                    OxiaPhysicalMetadataNamespaceV2.MARKER_KEY,
                    foreignMetadata.identity().markerBytes().toByteArray()));
            assertThatThrownBy(() -> await(foreignMetadata.requireCurrent()))
                    .hasRootCauseMessage("native metadata namespace identity disappeared or changed");
            assertThat(await(backend.readBinding())).contains(binding);
            Files.write(
                    checkpoint(),
                    List.of(
                            binding.encode().toHex(),
                            spec.encode().toHex(),
                            Long.toString(ledgerId),
                            exact.canonicalStoredSha256().toHex(),
                            exact.metadataVersion().value().toHex(),
                            Sha256Digest.hash((await(first.quota().snapshot())
                                            .head()
                                            .encode()))
                                    .toHex()));
        }
    }

    @Test
    void readAfterServerRestart() throws Exception {
        verifyBookKeeperJar();
        var lines = Files.readAllLines(checkpoint());
        assertThat(lines).hasSize(6);
        var binding = PhysicalNamespaceAuthorityBindingV2.decode(bytes(lines.get(0)));
        var spec = M5BookKeeperNativeCreateSpecV2.decode(bytes(lines.get(1)));
        long ledgerId = Long.parseLong(lines.get(2));
        try (var oxia = openOxia(false, true);
                var foreign = openOxia(true, false);
                var backend = M5BookKeeperNamespaceAuthorityV2.connect(aliasUri(), CAPABILITY)) {
            assertThat(await(backend.readBinding())).contains(binding);
            var metadata = await(OxiaPhysicalMetadataNamespaceV2.connect(oxia, binding.metadataNamespace()));
            assertThatThrownBy(
                            () -> await(OxiaPhysicalMetadataNamespaceV2.connect(foreign, binding.metadataNamespace())))
                    .hasRootCauseMessage("actual metadata backend namespace differs from the physical binding");
            var route = await(metadata.openAuthorityRoute(backend, new Oxia09ExactMetadataTransactionStoreV1(oxia)));
            var resource = new PhysicalResourceIdV2.BookKeeperLedger(backend.namespace(), ledgerId);
            var authority = await(route.read(resource.authorityKey())).orElseThrow();
            assertThat(authority.canonicalStoredSha256().toHex()).isEqualTo(lines.get(3));
            assertThat(authority.metadataVersion().value().toHex()).isEqualTo(lines.get(4));
            assertThat(Sha256Digest.hash(await(route.quota().snapshot()).head().encode())
                            .toHex())
                    .isEqualTo(lines.get(5));
            assertThatThrownBy(() -> M5BookKeeperNativeCreateClientV2.connect(uri(), CAPABILITY, spec))
                    .hasRootCauseMessage("native M5 create client lacks the exact bound metadata namespace");
            try (var bound = M5BookKeeperNativeCreateClientV2.connect(uri(), CAPABILITY, spec, binding)) {
                var session = bound.newSession();
                var handle = handle(spec, ledgerId);
                assertThat(await(session.openRunLedger(handle)).exactHandle()).contains(handle);
                assertThat(await(session.readExactEntry(handle, 0))
                                .exactEntry()
                                .orElseThrow()
                                .payload()
                                .toByteArray())
                        .containsExactly(PAYLOAD);
                assertThat(await(bound.captureExactTarget(handle)).exactTarget())
                        .isPresent();
                await(session.closeAsync());
            }
            assertThat(await(route.quota().snapshot()).head().reservedResources())
                    .isEqualTo(1);
        }
    }

    private static RunLedgerHandleV1 handle(M5BookKeeperNativeCreateSpecV2 spec, long id) {
        var run = spec.configurations().get(0);
        return new RunLedgerHandleV1(
                run.providerScopeId(), run.runId(), new BookKeeperLedgerIdentity(id), run.configurationDigest());
    }

    private static M5BookKeeperNativeCreateSpecV2 spec(String instance) {
        var id = UUID.randomUUID();
        var task = Sha256Digest.hash(
                CanonicalBytes.copyOf(id.toString().getBytes(java.nio.charset.StandardCharsets.US_ASCII)));
        return M5BookKeeperNativeCreateSpecV2.of(
                instance,
                task,
                List.of(RunLedgerConfigurationV1.from(
                        CAPABILITY,
                        new StorageRunId(new Id128(id.getMostSignificantBits(), id.getLeastSignificantBits())))));
    }

    private static AsyncOxiaClient openOxia(boolean foreign, boolean alias) throws Exception {
        String address = System.getProperty(
                foreign
                        ? "nereus.m5.namespace.foreignOxiaAddress"
                        : (alias ? "nereus.m5.namespace.oxiaAliasAddress" : "nereus.m5.namespace.oxiaAddress"));
        var builder = OxiaClientBuilder.create(address);
        var jar = Path.of(builder.getClass()
                .getProtectionDomain()
                .getCodeSource()
                .getLocation()
                .toURI());
        assertThat(Sha256Digest.hash(CanonicalBytes.copyOf(Files.readAllBytes(jar)))
                        .toHex())
                .isEqualTo("0ca719e6d11bd2ee2c2e7e94b42c6843e60f776bea12f7b5814cff9928e2e4c5");
        return builder.namespace("default")
                .requestTimeout(Duration.ofSeconds(10))
                .asyncClient()
                .get(30, TimeUnit.SECONDS);
    }

    private static void verifyBookKeeperJar() throws Exception {
        var jar = Path.of(BookKeeper.class
                .getProtectionDomain()
                .getCodeSource()
                .getLocation()
                .toURI());
        assertThat(Sha256Digest.hash(CanonicalBytes.copyOf(Files.readAllBytes(jar))))
                .isEqualTo(CAPABILITY.clientArtifactSha256());
    }

    private static CanonicalBytes bytes(String hex) {
        return CanonicalBytes.copyOf(java.util.HexFormat.of().parseHex(hex));
    }

    private static String uri() {
        return System.getProperty("nereus.bookkeeper.metadataServiceUri");
    }

    private static String aliasUri() {
        return uri().replace("127.0.0.1", "127.1");
    }

    private static Path checkpoint() {
        return Path.of(System.getProperty("nereus.m5.namespace.restartCheckpoint"));
    }

    private static <T> T await(CompletionStage<T> stage) throws Exception {
        return stage.toCompletableFuture().get(30, TimeUnit.SECONDS);
    }
}
