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

package com.nereusstream.storage.object.vault;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.KafkaCellId;
import com.nereusstream.domain.protocol.KafkaProtocolCellIdentity;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.control.LaneSequenceVector;
import com.nereusstream.storage.object.control.Nwg1RootAdmissionCaps;
import com.nereusstream.storage.object.control.ObjectProviderAccessProfile;
import com.nereusstream.storage.object.control.ObjectProviderRootConfiguration;
import com.nereusstream.storage.object.control.ProviderProofMode;
import com.nereusstream.storage.object.control.WalCheckpointHeadV1;
import com.nereusstream.storage.object.control.WalCheckpointPolicy;
import com.nereusstream.storage.object.control.WalRunBounds;
import com.nereusstream.storage.object.control.WalRunControlCodec;
import com.nereusstream.storage.object.control.WalRunControlKeys;
import com.nereusstream.storage.object.control.WalRunFormatContractV1;
import com.nereusstream.storage.object.control.WalRunLifecycleManager;
import com.nereusstream.storage.object.control.WalRunObjectSession;
import com.nereusstream.storage.object.control.WalRunRootRecord;
import com.nereusstream.storage.object.control.WalRunRuntime;
import com.nereusstream.storage.object.control.WalRunSealRecord;
import com.nereusstream.storage.object.control.WalRunTerminalClosureProofV1;
import com.nereusstream.storage.object.kms.KmsCellSession;
import com.nereusstream.storage.object.kms.KmsTransport;
import com.nereusstream.storage.object.kms.WrappedRunKeyEnvelope;
import com.nereusstream.storage.object.provider.C1ObjectProviderSession;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.provider.ObjectProviderCapabilities;
import com.nereusstream.storage.object.provider.ObjectProviderTransport;
import com.nereusstream.storage.object.recovery.RecoveryEnvelopeLimits;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class VaultTransitRealKmsEvidenceTest {
    private static final String IMAGE =
            "hashicorp/vault@sha256:268bb80aa9c6d13d65fcfa05c0c268caca068952240a8087291a6ce0b66e3a10";
    private static final String PRODUCT_VERSION = "1.20.4";
    private static final String TOKEN = "m3-local-evidence-token";
    private static final String KEY_NAME = "nereus-m3-run-key";

    @Test
    void provesRealTransitWrapUnwrapVersionRotationAndLifecycle() throws Exception {
        boolean formalEvidence = formalEvidenceMode();
        ContainerIdentity containerIdentity;
        try (DockerCliEvidenceContainer vault = DockerCliEvidenceContainer.start(
                "m3-real-kms",
                IMAGE,
                8200,
                List.of("VAULT_DEV_ROOT_TOKEN_ID=" + TOKEN, "VAULT_DEV_LISTEN_ADDRESS=0.0.0.0:8200"),
                List.of("server", "-dev"))) {
            URI endpoint = vault.endpoint();
            HttpClient client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(10))
                    .build();
            awaitHealthy(client, endpoint.resolve("/v1/sys/health"));
            enableTransitAndCreateKey(client, endpoint);

            VaultTransitRunKeyKms kms = new VaultTransitRunKeyKms(
                    client, endpoint, "transit", KEY_NAME, () -> TOKEN, Duration.ofSeconds(30), new SecureRandom());
            assertThat(kms).isInstanceOf(KmsTransport.class);
            byte[] spiPlaintext = new byte[32];
            Arrays.fill(spiPlaintext, (byte) 0x5a);
            WrappedRunKeyEnvelope spiEnvelope = kms.wrap(kms.keyIdentity(), spiPlaintext);
            byte[] spiUnwrapped = kms.unwrap(spiEnvelope);
            try {
                assertThat(MessageDigest.isEqual(spiPlaintext, spiUnwrapped)).isTrue();
            } finally {
                Arrays.fill(spiPlaintext, (byte) 0);
                Arrays.fill(spiUnwrapped, (byte) 0);
            }
            byte[] firstPlaintext = null;
            try (var generated = kms.generateAndWrap()) {
                WrappedRunKeyEnvelope versionOne = generated.envelope();
                assertThat(versionOne.providerId()).isEqualTo(VaultTransitRunKeyKms.PROVIDER_ID);
                assertThat(versionOne.wrappingAlgorithmId()).isEqualTo(VaultTransitRunKeyKms.WRAPPING_ALGORITHM_ID);
                assertThat(versionOne.wrappingKeyId()).isEqualTo(kms.keyIdentity());
                assertThat(versionOne.wrappingKeyVersion()).isEqualTo("1");
                firstPlaintext = generated.plaintextKey().use(bytes -> bytes.clone());

                try (ZeroizableRunKey unwrapped = kms.unwrapZeroizable(versionOne)) {
                    byte[] expected = firstPlaintext;
                    boolean equal = unwrapped.use(bytes -> MessageDigest.isEqual(bytes, expected));
                    assertThat(equal).isTrue();
                }

                WalRunTerminalClosureProofV1 closureProof = publishTerminalClosure(versionOne, kms);
                int rotated = kms.rotateAfterRunSeal(closureProof);
                assertThat(rotated).isEqualTo(2);

                try (var successor = kms.generateAndWrap()) {
                    assertThat(successor.envelope().wrappingKeyVersion()).isEqualTo("2");
                }
                try (ZeroizableRunKey oldVersionStillReadable = kms.unwrapZeroizable(versionOne)) {
                    byte[] expected = firstPlaintext;
                    boolean equal = oldVersionStillReadable.use(bytes -> MessageDigest.isEqual(bytes, expected));
                    assertThat(equal).isTrue();
                }

                WrappedRunKeyEnvelope mismatchedVersion = new WrappedRunKeyEnvelope(
                        versionOne.providerId(),
                        versionOne.wrappingAlgorithmId(),
                        versionOne.wrappingKeyId(),
                        "2",
                        versionOne.wrappedKey());
                assertThatThrownBy(() -> kms.unwrapZeroizable(mismatchedVersion))
                        .isInstanceOf(VaultTransitException.class)
                        .satisfies(failure -> assertThat(((VaultTransitException) failure).kind())
                                .isEqualTo(VaultTransitException.Kind.VERSION_MISMATCH));
            } finally {
                if (firstPlaintext != null) {
                    Arrays.fill(firstPlaintext, (byte) 0);
                }
            }

            kms.close();
            assertThatThrownBy(kms::generateAndWrap)
                    .isInstanceOf(VaultTransitException.class)
                    .satisfies(failure -> assertThat(((VaultTransitException) failure).kind())
                            .isEqualTo(VaultTransitException.Kind.CLOSED));
            containerIdentity = new ContainerIdentity(vault.containerId(), vault.imageConfigDigest());
        }
        if (formalEvidence) {
            // Write only after the KMS adapter and exact-target auto-remove container have both closed.
            writeEvidence(containerIdentity.containerId(), containerIdentity.imageConfigDigest());
        }
    }

    private static void awaitHealthy(HttpClient client, URI uri) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            try {
                HttpResponse<Void> response = client.send(
                        HttpRequest.newBuilder(uri)
                                .timeout(Duration.ofSeconds(2))
                                .GET()
                                .build(),
                        HttpResponse.BodyHandlers.discarding());
                if (response.statusCode() == 200) {
                    return;
                }
            } catch (java.io.IOException ignored) {
                // The exact-digest product is still starting; no credential or container log is emitted.
            }
            Thread.sleep(100);
        }
        throw new IllegalStateException("Vault did not become healthy within the bounded startup deadline");
    }

    private static void enableTransitAndCreateKey(HttpClient client, URI endpoint) throws Exception {
        send(client, endpoint.resolve("/v1/sys/mounts/transit"), "{\"type\":\"transit\"}");
        send(
                client,
                endpoint.resolve("/v1/transit/keys/" + KEY_NAME),
                "{\"type\":\"aes256-gcm96\",\"derived\":false,\"exportable\":false,"
                        + "\"allow_plaintext_backup\":false}");
    }

    private static void send(HttpClient client, URI uri, String body) throws Exception {
        HttpResponse<Void> response = client.send(
                HttpRequest.newBuilder(uri)
                        .timeout(Duration.ofSeconds(30))
                        .header("X-Vault-Token", TOKEN)
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                        .build(),
                HttpResponse.BodyHandlers.discarding());
        assertThat(response.statusCode()).isBetween(200, 299);
    }

    private static void writeEvidence(String containerId, String imageConfigDigest) throws Exception {
        Path output = Path.of(System.getProperty("nereus.m3.kmsEvidenceOutput"));
        Files.createDirectories(output.toAbsolutePath().getParent());
        String testedCommit = exactTestedCommit();
        String contract = "provider=hashicorp-vault-transit\n"
                + "productVersion=" + PRODUCT_VERSION + "\n"
                + "keyType=aes256-gcm96\n"
                + "derivedKeyContext=false\n"
                + "kmsContextAuthority=NONE\n"
                + "runKeyBytes=32\n"
                + "envelopeProviderId=" + VaultTransitRunKeyKms.PROVIDER_ID + "\n"
                + "envelopeAlgorithmId=" + VaultTransitRunKeyKms.WRAPPING_ALGORITHM_ID + "\n"
                + "envelopeIdentity=vault-transit://transit/keys/" + KEY_NAME + "\n"
                + "rotationBoundary=WALRUN_TERMINAL_CLOSURE_PROOF_V1\n";
        String json = "{\n"
                + "  \"schema\": \"NEREUS_V2_M3_REAL_KMS_EVIDENCE_V1\",\n"
                + "  \"result\": \"PASS_REAL_VAULT_TRANSIT_KMS_ONLY\",\n"
                + "  \"promotionEligible\": false,\n"
                + "  \"realKms\": true,\n"
                + "  \"product\": \"HashiCorp Vault Transit\",\n"
                + "  \"productVersion\": \"" + PRODUCT_VERSION + "\",\n"
                + "  \"imageReference\": \"" + IMAGE + "\",\n"
                + "  \"imageConfigDigest\": \"" + imageConfigDigest + "\",\n"
                + "  \"containerId\": \"" + containerId.substring(0, 12) + "\",\n"
                + "  \"networkBinding\": \"127.0.0.1:RANDOM\",\n"
                + "  \"containerAutoRemove\": true,\n"
                + "  \"devMode\": true,\n"
                + "  \"productionDeploymentProven\": false,\n"
                + "  \"adapterVersion\": \"" + VaultTransitRunKeyKms.ADAPTER_VERSION + "\",\n"
                + "  \"nereusCommit\": \"" + testedCommit + "\",\n"
                + "  \"testTask\": \"realKmsTest\",\n"
                + "  \"testClass\": \"com.nereusstream.storage.object.vault.VaultTransitRealKmsEvidenceTest\",\n"
                + "  \"testMethod\": \"provesRealTransitWrapUnwrapVersionRotationAndLifecycle()\",\n"
                + "  \"runKeyBytes\": 32,\n"
                + "  \"coreKmsTransportSpi\": true,\n"
                + "  \"derivedKeyContext\": false,\n"
                + "  \"kmsContextAuthority\": \"NONE\",\n"
                + "  \"envelopeProviderId\": \"" + VaultTransitRunKeyKms.PROVIDER_ID + "\",\n"
                + "  \"envelopeAlgorithmId\": \"" + VaultTransitRunKeyKms.WRAPPING_ALGORITHM_ID + "\",\n"
                + "  \"wrappingKeyIdentity\": \"vault-transit://transit/keys/" + KEY_NAME + "\",\n"
                + "  \"versionsProven\": [1, 2],\n"
                + "  \"oldVersionDecryptAfterRotation\": true,\n"
                + "  \"walRunTerminalClosureProof\": true,\n"
                + "  \"applicationOwnedPlaintextKeyArraysZeroized\": true,\n"
                + "  \"tokenStringZeroizationProven\": false,\n"
                + "  \"jdkHttpTlsInternalBufferZeroizationProven\": false,\n"
                + "  \"contractSha256\": \"" + sha256(contract) + "\",\n"
                + "  \"tests\": 1,\n"
                + "  \"failures\": 0,\n"
                + "  \"errors\": 0,\n"
                + "  \"skipped\": 0\n"
                + "}\n";
        Files.writeString(output, json, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
    }

    private static boolean formalEvidenceMode() {
        return switch (System.getProperty("nereus.m3.evidenceMode", "UNSET")) {
            case "FORMAL" -> true;
            case "DIAGNOSTIC" -> false;
            default -> throw new IllegalStateException("real KMS execution mode must be FORMAL or DIAGNOSTIC");
        };
    }

    private static WalRunTerminalClosureProofV1 publishTerminalClosure(
            WrappedRunKeyEnvelope envelope, VaultTransitRunKeyKms kms) {
        InMemoryControlStore store = new InMemoryControlStore();
        WalRunLifecycleManager lifecycle = new WalRunLifecycleManager(store);
        WalRunRootRecord root = root(envelope);
        var publication = lifecycle.createRootAndInitializePointer(WalRunControlKeys.rootKey(7, 1), root);
        var reference = publication.reference();
        String headKey = WalRunControlKeys.checkpointHeadKey(7, 1);
        CanonicalBytes headBytes = WalRunControlCodec.encodeCheckpointHead(
                WalCheckpointHeadV1.empty(reference.rootSha256(), reference.shardRunEpoch(), 1));
        assertThat(store.putIfAbsent(headKey, headBytes)).isEqualTo(ControlMutationOutcome.APPLIED);
        WalRunSealRecord seal = new WalRunSealRecord(
                reference, LaneSequenceVector.empty(), headKey, Sha256Digest.hash(headBytes), 0, 0);
        C1ObjectProviderSession provider = new C1ObjectProviderSession(
                new NoIoProviderTransport(),
                root.providerScopeId(),
                root.providerConfiguration().exclusiveNamespacePrefix(),
                root.providerConfiguration().maxObjectBodyBytes(),
                root.nwg1AdmissionCaps().maxDirectoryPrefixBytes());
        KmsCellSession kmsCell = new KmsCellSession(
                kms,
                root.providerScopeId(),
                kms.keyIdentity(),
                root.recoveryEnvelope().maxLiveRoots(),
                new SecureRandom());
        try (WalRunObjectSession session =
                WalRunObjectSession.openNew(publication.ownerAuthority().orElseThrow(), provider, kmsCell, () -> 0L)) {
            session.stopAdmission(WalRunRuntime.StopReason.OWNER_REQUEST);
            assertThat(session.sealRuntime()).isEqualTo(LaneSequenceVector.empty());
            session.drain();
            return lifecycle.publishSeal(WalRunControlKeys.sealKey(7, 1), seal, session);
        } finally {
            kmsCell.close();
        }
    }

    private static WalRunRootRecord root(WrappedRunKeyEnvelope envelope) {
        return new WalRunRootRecord(
                7,
                1,
                new Id128(2, 3),
                0,
                new KafkaProtocolCellIdentity(new DeploymentId(new Id128(1, 2)), new KafkaCellId(new Id128(3, 4))),
                new CellProviderScopeId(digest(1)),
                WalRunFormatContractV1.frozen(),
                new Nwg1RootAdmissionCaps(1024 * 1024, 4096, 3824, 16, 100, 100, 4080, 4096, 1024 * 1024, 1024 * 1024),
                new WalRunBounds(100, 1024 * 1024, 60_000, 4),
                new WalCheckpointPolicy(0, 16, 1024 * 1024, 5_000, 16, 8192),
                new ObjectProviderRootConfiguration(
                        ObjectProviderAccessProfile.C1_SINGLE_PUT_SINGLE_RANGE_STRONG_LIST,
                        "test-adapter-v1",
                        "canonical-key-v1",
                        "cell-a/wal/run-1",
                        ProviderProofMode.NONE,
                        0,
                        1024 * 1024,
                        1024 * 1024,
                        4096,
                        1,
                        10_100,
                        digest(2)),
                new RecoveryEnvelopeLimits(
                        5,
                        4,
                        10,
                        102,
                        100_000,
                        0,
                        10_100,
                        10,
                        32L * 1024 * 1024,
                        10_000,
                        10_000,
                        10_000,
                        1024 * 1024,
                        4,
                        10,
                        60_000_000_000L),
                envelope,
                Optional.empty());
    }

    private static Sha256Digest digest(int seed) {
        byte[] value = new byte[Sha256Digest.LENGTH];
        for (int index = 0; index < value.length; index++) {
            value[index] = (byte) (seed + index);
        }
        return Sha256Digest.copyOf(value);
    }

    private static String exactTestedCommit() {
        String value = System.getProperty("nereus.m3.testedCommit", "UNSET");
        if (!value.matches("[0-9a-f]{40}")) {
            throw new IllegalStateException("real KMS evidence requires exact tested Nereus commit");
        }
        return value;
    }

    private static String sha256(String value) throws Exception {
        return HexFormat.of()
                .formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
    }

    private record ContainerIdentity(String containerId, String imageConfigDigest) {}

    private static final class NoIoProviderTransport implements ObjectProviderTransport {
        private static final ObjectProviderCapabilities CAPABILITIES = new ObjectProviderCapabilities(
                "vault-terminal-closure-no-io", true, true, true, true, true, 1024 * 1024, 4096, 10_100);

        @Override
        public ObjectProviderCapabilities capabilities() {
            return CAPABILITIES;
        }

        @Override
        public ConditionalCreateResult putIfAbsent(ObjectIdentity identity, InputStream body) {
            throw unexpectedIo();
        }

        @Override
        public StreamingObject get(String key, Optional<CanonicalBytes> exactVersionToken) {
            throw unexpectedIo();
        }

        @Override
        public StreamingObject getRange(
                String key, long inclusiveStart, long exclusiveEnd, Optional<CanonicalBytes> versionToken) {
            throw unexpectedIo();
        }

        @Override
        public ListPage list(String prefix, Optional<CanonicalBytes> continuationToken, int maximumKeys) {
            throw unexpectedIo();
        }

        private static AssertionError unexpectedIo() {
            return new AssertionError("terminal closure without extents must not issue Provider I/O");
        }
    }

    private static final class InMemoryControlStore implements CanonicalControlMetadataStore {
        private final Map<String, CanonicalBytes> values = new LinkedHashMap<>();

        @Override
        public synchronized Optional<CanonicalBytes> get(String key) {
            return Optional.ofNullable(values.get(key));
        }

        @Override
        public synchronized ControlMutationOutcome putIfAbsent(String key, CanonicalBytes exactValue) {
            CanonicalBytes current = values.putIfAbsent(key, exactValue);
            return current == null ? ControlMutationOutcome.APPLIED : ControlMutationOutcome.DEFINITIVE_CONFLICT;
        }

        @Override
        public synchronized ControlMutationOutcome compareAndSet(
                String key, Optional<CanonicalBytes> exactExpected, CanonicalBytes exactCandidate) {
            CanonicalBytes current = values.get(key);
            if (!Objects.equals(current, exactExpected.orElse(null))) {
                return ControlMutationOutcome.DEFINITIVE_CONFLICT;
            }
            values.put(key, exactCandidate);
            return ControlMutationOutcome.APPLIED;
        }
    }
}
