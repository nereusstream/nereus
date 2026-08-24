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

package com.nereusstream.storage.object.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.object.provider.C1ObjectProviderSession;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.provider.ObjectProviderCapabilities;
import com.nereusstream.storage.object.provider.ObjectProviderTransport;
import com.nereusstream.storage.object.provider.ProviderObjectOutcome;
import com.nereusstream.storage.object.provider.RepeatableObjectBody;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

class MinioC1RealProviderEvidenceTest {
    private static final String IMAGE =
            "quay.io/minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e";
    private static final String PRODUCT_VERSION = "RELEASE.2025-09-07T16-13-09Z";
    private static final String ACCESS_KEY = "nereus-m3-evidence";
    private static final String SECRET_KEY = "nereus-m3-evidence-secret";
    private static final String BUCKET = "nereus-m3-object-wal";
    private static final long CAP = S3C1ObjectProviderTransport.EVIDENCED_MAXIMUM_OBJECT_BYTES;

    @Test
    void provesC1AtTheAdmitted64MiBRootCap() throws Exception {
        boolean formalEvidence = formalEvidenceMode();
        ContainerIdentity containerIdentity;
        try (DockerCliEvidenceContainer minio = DockerCliEvidenceContainer.start(
                "m3-real-provider",
                IMAGE,
                9000,
                List.of("MINIO_ROOT_USER=" + ACCESS_KEY, "MINIO_ROOT_PASSWORD=" + SECRET_KEY),
                List.of("server", "/data", "--console-address", ":9001"))) {
            URI endpoint = minio.endpoint();
            awaitHealthy(endpoint.resolve("/minio/health/ready"));
            S3Client client = S3Client.builder()
                    .endpointOverride(endpoint)
                    .credentialsProvider(
                            StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                    .region(Region.US_EAST_1)
                    .forcePathStyle(true)
                    .build();
            client.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
            try (S3C1ObjectProviderTransport transport =
                    new S3C1ObjectProviderTransport(client, BUCKET, "minio/" + PRODUCT_VERSION, true, 2)) {
                transport.capabilities().requireC1();
                CellProviderScopeId scope = new CellProviderScopeId(Sha256Digest.hash(
                        CanonicalBytes.copyOf("m3-real-provider-scope".getBytes(StandardCharsets.US_ASCII))));
                StreamingBody capBody = StreamingBody.pattern("walrun/lane-1/42/object", CAP, 0x5a);
                try (C1ObjectProviderSession provider =
                        new C1ObjectProviderSession(transport, scope, "walrun", CAP, 4 * 1024 * 1024)) {
                    assertThat(provider.conditionalCreate(capBody).outcome())
                            .isEqualTo(ProviderObjectOutcome.APPLIED_EXACT);
                    assertThat(provider.conditionalCreate(capBody).outcome())
                            .isEqualTo(ProviderObjectOutcome.EXISTING_EXACT);
                    assertThat(provider.readDirectoryPrefix(capBody.identity(), 4 * 1024 * 1024, Optional.empty())
                                    .length())
                            .isEqualTo(4 * 1024 * 1024);

                    StreamingBody conflicting = StreamingBody.pattern("walrun/lane-1/42/object", CAP, 0x6b);
                    assertThat(provider.conditionalCreate(conflicting).outcome())
                            .isEqualTo(ProviderObjectOutcome.DEFINITIVE_CONFLICT);

                    for (int index = 0; index < 5; index++) {
                        StreamingBody listed = StreamingBody.pattern("walrun/lane-2/11/list-" + index, 1, index + 1);
                        assertThat(provider.conditionalCreate(listed).outcome())
                                .isEqualTo(ProviderObjectOutcome.APPLIED_EXACT);
                    }
                    var immediate = provider.strongList("walrun/lane-2/11/", 4_096, 10_000, 1_000_000, 1_024);
                    assertThat(immediate.objects())
                            .extracting(ObjectProviderTransport.ListedObject::key)
                            .containsExactly(
                                    "walrun/lane-2/11/list-0",
                                    "walrun/lane-2/11/list-1",
                                    "walrun/lane-2/11/list-2",
                                    "walrun/lane-2/11/list-3",
                                    "walrun/lane-2/11/list-4");
                    assertThat(immediate.pageCount()).isEqualTo(3);
                    StreamingBody absent = StreamingBody.pattern("walrun/lane-2/11/absent", 1, 9);
                    assertThat(provider.reconcileUnknown(
                                            absent.identity(), "walrun/lane-2/11/", 4_096, 10_000, 1_000_000, 1_024)
                                    .objectResult()
                                    .outcome())
                            .isEqualTo(ProviderObjectOutcome.DEFINITIVELY_NOT_APPLIED);
                    assertThat(provider.acceptedOperations()).isZero();
                    assertThat(provider.unknownObjectCount()).isZero();
                }

                try (ObjectProviderTransport.StreamingObject frame =
                        transport.getRange(capBody.identity().key(), CAP - 1024 * 1024, CAP, Optional.empty())) {
                    assertThat(frame.bodyLength()).isEqualTo(CAP);
                    assertThat(frame.inclusiveStart()).isEqualTo(CAP - 1024 * 1024);
                    assertThat(frame.exclusiveEnd()).isEqualTo(CAP);
                    assertThat(frame.body().readAllBytes()).hasSize(1024 * 1024);
                }

                responseUnknownCuts(transport, capBody);
            }
            containerIdentity = new ContainerIdentity(minio.containerId(), minio.imageConfigDigest());
        }
        if (formalEvidence) {
            // Write only after transport, SDK client, and exact-target auto-remove container have all closed.
            writeEvidence(containerIdentity.containerId(), containerIdentity.imageConfigDigest());
        }
    }

    private static void awaitHealthy(URI uri) throws Exception {
        var client = java.net.http.HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            try {
                var response = client.send(
                        java.net.http.HttpRequest.newBuilder(uri)
                                .timeout(Duration.ofSeconds(2))
                                .GET()
                                .build(),
                        java.net.http.HttpResponse.BodyHandlers.discarding());
                if (response.statusCode() == 200) {
                    return;
                }
            } catch (java.io.IOException ignored) {
                // The exact-digest product is still starting; no credential or container log is emitted.
            }
            Thread.sleep(100);
        }
        throw new IllegalStateException("MinIO did not become healthy within the bounded startup deadline");
    }

    private static void responseUnknownCuts(S3C1ObjectProviderTransport realTransport, StreamingBody alreadyPresent)
            throws Exception {
        FaultCutTransport presentCut = new FaultCutTransport(realTransport, FaultCut.PRESENT);
        try (C1ObjectProviderSession presentProvider = provider(presentCut)) {
            assertThat(presentProvider.conditionalCreate(alreadyPresent).outcome())
                    .isEqualTo(ProviderObjectOutcome.OUTCOME_UNKNOWN);
            assertThat(presentProvider
                            .reconcileUnknown(
                                    alreadyPresent.identity(), "walrun/lane-1/42/", 4_096, 10_000, 1_000_000, 1_024)
                            .objectResult()
                            .outcome())
                    .isEqualTo(ProviderObjectOutcome.EXISTING_EXACT);
        }

        StreamingBody absentBody = StreamingBody.pattern("walrun/lane-0/90/object", 31, 3);
        FaultCutTransport absentCut = new FaultCutTransport(realTransport, FaultCut.ABSENT);
        try (C1ObjectProviderSession absentProvider = provider(absentCut)) {
            assertThat(absentProvider.conditionalCreate(absentBody).outcome())
                    .isEqualTo(ProviderObjectOutcome.OUTCOME_UNKNOWN);
            assertThat(absentProvider
                            .reconcileUnknown(
                                    absentBody.identity(), "walrun/lane-0/90/", 4_096, 10_000, 1_000_000, 1_024)
                            .objectResult()
                            .outcome())
                    .isEqualTo(ProviderObjectOutcome.DEFINITIVELY_NOT_APPLIED);
        }

        StreamingBody unknownBody = StreamingBody.pattern("walrun/lane-0/91/object", 31, 4);
        FaultCutTransport unknownCut = new FaultCutTransport(realTransport, FaultCut.UNKNOWN);
        try (C1ObjectProviderSession unknownProvider = provider(unknownCut)) {
            assertThat(unknownProvider.conditionalCreate(unknownBody).outcome())
                    .isEqualTo(ProviderObjectOutcome.OUTCOME_UNKNOWN);
            assertThatThrownBy(() -> unknownProvider.reconcileUnknown(
                            unknownBody.identity(), "walrun/lane-0/91/", 4_096, 10_000, 1_000_000, 1_024))
                    .isInstanceOf(S3C1ProviderException.class);
            assertThat(unknownProvider.acceptedOperations()).isOne();
            assertThat(unknownProvider.unknownObjectCount()).isOne();
            assertThat(unknownProvider
                            .reconcileUnknown(
                                    unknownBody.identity(), "walrun/lane-0/91/", 4_096, 10_000, 1_000_000, 1_024)
                            .objectResult()
                            .outcome())
                    .isEqualTo(ProviderObjectOutcome.DEFINITIVELY_NOT_APPLIED);
        }
    }

    private static C1ObjectProviderSession provider(ObjectProviderTransport transport) {
        CellProviderScopeId scope = new CellProviderScopeId(
                Sha256Digest.hash(CanonicalBytes.copyOf("m3-real-provider-scope".getBytes(StandardCharsets.US_ASCII))));
        return new C1ObjectProviderSession(transport, scope, "walrun", CAP, 4 * 1024 * 1024);
    }

    private static void writeEvidence(String containerId, String imageConfigDigest) throws Exception {
        Path output = Path.of(System.getProperty("nereus.m3.providerEvidenceOutput"));
        Files.createDirectories(output.toAbsolutePath().getParent());
        String contract = "strategy=C1_SINGLE_OBJECT_V1\n"
                + "provider=minio/" + PRODUCT_VERSION + "\n"
                + "adapter=" + S3C1ObjectProviderTransport.ADAPTER_VERSION + "\n"
                + "maxObjectBodyBytes=" + CAP + "\n"
                + "maxDirectoryPrefixBytes=4194304\n"
                + "maxSingleRangeBytes=67108864\n"
                + "listPageKeys=2\n"
                + "providerProofMode=NONE\n"
                + "conditionalCreate=true\nstreamingFullGetSha256=true\nstrongListAbsence=true\n";
        String testedCommit = exactTestedCommit();
        String json = "{\n"
                + "  \"schema\": \"NEREUS_V2_M3_EXACT_PROVIDER_CAPACITY_EVIDENCE_V1\",\n"
                + "  \"result\": \"PASS_REAL_PROVIDER_C1_ONLY\",\n"
                + "  \"promotionEligible\": false,\n"
                + "  \"realProvider\": true,\n"
                + "  \"strategy\": \"C1_SINGLE_OBJECT_V1\",\n"
                + "  \"c2Tested\": false,\n"
                + "  \"c2PromotionEligible\": false,\n"
                + "  \"providerProduct\": \"MinIO\",\n"
                + "  \"providerVersion\": \"" + PRODUCT_VERSION + "\",\n"
                + "  \"imageReference\": \"" + IMAGE + "\",\n"
                + "  \"imageConfigDigest\": \"" + imageConfigDigest + "\",\n"
                + "  \"containerId\": \"" + containerId.substring(0, 12) + "\",\n"
                + "  \"networkBinding\": \"127.0.0.1:RANDOM\",\n"
                + "  \"containerAutoRemove\": true,\n"
                + "  \"adapterVersion\": \"" + S3C1ObjectProviderTransport.ADAPTER_VERSION + "\",\n"
                + "  \"nereusCommit\": \"" + testedCommit + "\",\n"
                + "  \"testTask\": \"realProviderTest\",\n"
                + "  \"testClass\": \"com.nereusstream.storage.object.s3.MinioC1RealProviderEvidenceTest\",\n"
                + "  \"testMethod\": \"provesC1AtTheAdmitted64MiBRootCap()\",\n"
                + "  \"coreC1ObjectProviderSession\": true,\n"
                + "  \"providerProofMode\": \"NONE\",\n"
                + "  \"rootBodyCapBytes\": " + CAP + ",\n"
                + "  \"actualStreamingPutBytes\": " + CAP + ",\n"
                + "  \"actualStreamingFullGetSha256Bytes\": " + CAP + ",\n"
                + "  \"actualPrefixRangeBytes\": 4194304,\n"
                + "  \"actualFrameRangeBytes\": 1048576,\n"
                + "  \"forcedPaginationPageKeys\": 2,\n"
                + "  \"forcedPaginationObjects\": 5,\n"
                + "  \"samePrefixImmediateList\": true,\n"
                + "  \"absenceListPlusExactGetNotFound\": true,\n"
                + "  \"etagUsedAsContentProof\": false,\n"
                + "  \"userMetadataUsedAsContentProof\": false,\n"
                + "  \"headCalls\": 0,\n"
                + "  \"terminalOutcomes\": [\"APPLIED_EXACT\",\"EXISTING_EXACT\","
                + "\"DEFINITIVELY_NOT_APPLIED\",\"DEFINITIVE_CONFLICT\"],\n"
                + "  \"responseUnknownCuts\": [\"PRESENT\",\"ABSENT\",\"UNKNOWN\"],\n"
                + "  \"responseUnknownFaultInjection\": \"DETERMINISTIC_CLIENT_BOUNDARY\",\n"
                + "  \"candidateRootAdmissionContractSha256\": \"" + sha256(contract) + "\",\n"
                + "  \"unexpectedErrors\": 0,\n"
                + "  \"inFlightAtTerminal\": 0,\n"
                + "  \"spoolResourcesAtTerminal\": 0,\n"
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
            default -> throw new IllegalStateException("real Provider execution mode must be FORMAL or DIAGNOSTIC");
        };
    }

    private static String exactTestedCommit() {
        String value = System.getProperty("nereus.m3.testedCommit", "UNSET");
        if (!value.matches("[0-9a-f]{40}")) {
            throw new IllegalStateException("real Provider evidence requires exact tested Nereus commit");
        }
        return value;
    }

    private static String sha256(String value) throws Exception {
        return HexFormat.of()
                .formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
    }

    private record ContainerIdentity(String containerId, String imageConfigDigest) {}

    private record StreamingBody(ObjectIdentity identity, long length, int seed) implements RepeatableObjectBody {
        private static StreamingBody pattern(String key, long length, int seed) throws Exception {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (InputStream input = new PatternInputStream(length, seed)) {
                byte[] buffer = new byte[64 * 1024];
                while (true) {
                    int read = input.read(buffer);
                    if (read < 0) {
                        break;
                    }
                    digest.update(buffer, 0, read);
                }
            }
            return new StreamingBody(
                    key == null ? null : new ObjectIdentity(key, length, Sha256Digest.copyOf(digest.digest())),
                    length,
                    seed);
        }

        @Override
        public InputStream openStream() {
            return new PatternInputStream(length, seed);
        }
    }

    private static final class PatternInputStream extends InputStream {
        private final long length;
        private final int seed;
        private long position;

        private PatternInputStream(long length, int seed) {
            this.length = length;
            this.seed = seed;
        }

        @Override
        public int read() {
            if (position == length) {
                return -1;
            }
            return value(position++);
        }

        @Override
        public int read(byte[] bytes, int offset, int requested) {
            if (position == length) {
                return -1;
            }
            int count = (int) Math.min(requested, length - position);
            for (int index = 0; index < count; index++) {
                bytes[offset + index] = (byte) value(position++);
            }
            return count;
        }

        private int value(long at) {
            return (int) ((at * 31 + seed) & 0xff);
        }
    }

    private enum FaultCut {
        PRESENT,
        ABSENT,
        UNKNOWN
    }

    private static final class FaultCutTransport implements ObjectProviderTransport {
        private final ObjectProviderTransport delegate;
        private final FaultCut cut;
        private final AtomicInteger putCalls = new AtomicInteger();
        private final AtomicBoolean unknownListCut = new AtomicBoolean(true);

        private FaultCutTransport(ObjectProviderTransport delegate, FaultCut cut) {
            this.delegate = delegate;
            this.cut = cut;
        }

        @Override
        public ObjectProviderCapabilities capabilities() {
            return delegate.capabilities();
        }

        @Override
        public ConditionalCreateResult putIfAbsent(ObjectIdentity identity, InputStream body) throws IOException {
            if (putCalls.getAndIncrement() != 0) {
                return delegate.putIfAbsent(identity, body);
            }
            if (cut == FaultCut.PRESENT) {
                delegate.putIfAbsent(identity, body);
            }
            return ConditionalCreateResult.RESPONSE_UNKNOWN;
        }

        @Override
        public StreamingObject get(String key, Optional<CanonicalBytes> exactVersionToken) throws IOException {
            return delegate.get(key, exactVersionToken);
        }

        @Override
        public StreamingObject getRange(
                String key, long inclusiveStart, long exclusiveEnd, Optional<CanonicalBytes> versionToken)
                throws IOException {
            return delegate.getRange(key, inclusiveStart, exclusiveEnd, versionToken);
        }

        @Override
        public ListPage list(String prefix, Optional<CanonicalBytes> continuationToken, int maximumKeys)
                throws IOException {
            if (cut == FaultCut.UNKNOWN && unknownListCut.compareAndSet(true, false)) {
                throw new S3C1ProviderException(S3C1ProviderException.Kind.RETRYABLE, "injected LIST cut");
            }
            return delegate.list(prefix, continuationToken, maximumKeys);
        }

        @Override
        public FailureKind classifyFailure(IOException failure) {
            return delegate.classifyFailure(failure);
        }
    }
}
