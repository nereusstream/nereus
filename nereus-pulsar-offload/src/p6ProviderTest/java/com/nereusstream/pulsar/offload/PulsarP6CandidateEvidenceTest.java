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

package com.nereusstream.pulsar.offload;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;
import com.nereusstream.pulsar.offload.PulsarOffloadObjectStoreV1.Body;
import com.nereusstream.pulsar.offload.PulsarOffloadObjectStoreV1.Capabilities;
import com.nereusstream.pulsar.offload.PulsarOffloadObjectStoreV1.ImmutableObject;
import com.nereusstream.pulsar.offload.PulsarSealedLedgerAttemptV1.DeleteState;
import com.nereusstream.pulsar.offload.PulsarSealedLedgerAttemptV1.RetentionClass;
import com.nereusstream.pulsar.offload.PulsarSealedLedgerPublisherV1.PreparedAttempt;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.CompressionPolicy;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.DataObject;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.EntryPayload;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.StreamingEncoder;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.CustomMetadataValue;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.DigestType;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.EnsembleSegment;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.SealedLedgerSection;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

@Testcontainers
class PulsarP6CandidateEvidenceTest {
    private static final String BUCKET = "nereus-m2-p6-evidence";
    private static final int SMALL_ENTRY_COUNT = 50_000;
    private static final int SMALL_ENTRY_BYTES = 100;
    private static final int SCAN_ENTRY_COUNT = 4_000;
    private static final int SCAN_ENTRY_BYTES = 5_000;
    private static final int STOCK_ENTRY_BYTES = 5 * PulsarOffloadLimitCandidateV1.MIB;
    private static final int NEAR_HARD_CAP_BYTES = 64 * PulsarOffloadLimitCandidateV1.MIB - 1_024;
    private static final PulsarOffloadLimitCandidateV1 LIMITS =
            PulsarOffloadLimitCandidateV1.adr0056EvidenceCandidate();
    private static final SecretKey KEY = new SecretKeySpec(new byte[32], "AES");

    @Container
    private static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:4.14.0")).withServices(S3);

    private static ExecutorService executor;
    private static S3PulsarOffloadObjectStoreV1 provider;

    @TempDir
    Path temporaryDirectory;

    @BeforeAll
    static void setUp() {
        executor = Executors.newFixedThreadPool(8);
        S3Client client = S3Client.builder()
                .endpointOverride(LOCALSTACK.getEndpointOverride(S3))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
                .region(Region.of(LOCALSTACK.getRegion()))
                .forcePathStyle(true)
                .build();
        client.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
        provider = new S3PulsarOffloadObjectStoreV1(client, BUCKET, executor, true);
    }

    @AfterAll
    static void tearDown() {
        if (provider != null) {
            provider.close().toCompletableFuture().join();
        }
    }

    @Test
    void writesCompleteSourceQualifiedCandidateMatrix() throws Exception {
        String testedSource = requiredProperty("nereus.p6.testedSourceCommit");
        String pulsarSource = requiredProperty("nereus.p6.pulsarSourceCommit");
        Path output = Path.of(requiredProperty("nereus.p6.evidenceOutput"));
        List<Workload> workloads = List.of(
                new Workload("max-entries-100b", smallEntries(), SMALL_ENTRY_BYTES),
                new Workload("scan-20mb", scanEntries(), SCAN_ENTRY_BYTES));
        List<CaseResult> matrix = new ArrayList<>();
        PeakMemory peakMemory = new PeakMemory();

        for (Workload workload : workloads) {
            for (int target : LIMITS.blockTargetBytes()) {
                for (CompressionPolicy policy :
                        List.of(CompressionPolicy.FIXED_NONE, CompressionPolicy.ZSTD_IF_SMALLER)) {
                    matrix.add(runCase(target, policy, workload, peakMemory));
                }
            }
        }
        BoundaryResult stock =
                runBoundary("stock-5mib", STOCK_ENTRY_BYTES, CompressionPolicy.ZSTD_IF_SMALLER, peakMemory);
        BoundaryResult near =
                runBoundary("near-hard-cap", NEAR_HARD_CAP_BYTES, CompressionPolicy.ZSTD_IF_SMALLER, peakMemory);

        assertThat(matrix).hasSize(16);
        assertThat(matrix).allSatisfy(result -> {
            assertThat(result.randomReadSamplesMicros()).hasSize(20);
            assertThat(result.providerRequests()).isPositive();
            assertThat(result.providerTransferredBytes()).isPositive();
        });
        assertThat(stock.decodedBytes()).isEqualTo(STOCK_ENTRY_BYTES);
        assertThat(near.decodedBytes()).isEqualTo(NEAR_HARD_CAP_BYTES);

        Files.createDirectories(output.toAbsolutePath().getParent());
        Files.writeString(
                output, json(testedSource, pulsarSource, matrix, stock, near, peakMemory), StandardCharsets.UTF_8);
    }

    private CaseResult runCase(
            int targetBytes, CompressionPolicy compressionPolicy, Workload workload, PeakMemory peakMemory) {
        String caseId = workload.name() + "-" + targetBytes + "-"
                + compressionPolicy.name().toLowerCase(Locale.ROOT);
        UUID attemptUuid = UUID.nameUUIDFromBytes(caseId.getBytes(StandardCharsets.UTF_8));
        long cpuBefore = processCpuTime();
        long wallBefore = System.nanoTime();
        DataObject data = encode(caseId, targetBytes, compressionPolicy, attemptUuid, workload.entries());
        CountingStore store = new CountingStore(provider);
        PreparedAttempt prepared = prepared(attemptUuid, data, targetBytes);
        PulsarPublishedAttemptVerifierV1 verifier = new PulsarPublishedAttemptVerifierV1(store, LIMITS, attempt -> KEY);
        new PulsarSealedLedgerPublisherV1(store, LIMITS, verifier, Runnable::run)
                .publish(prepared)
                .toCompletableFuture()
                .join();
        PulsarObjectReadHandleV1 handle = PulsarObjectReadHandleV1.open(store, LIMITS, prepared.attempt(), KEY)
                .toCompletableFuture()
                .join();

        List<Long> randomSamplesMicros = new ArrayList<>();
        for (int sample = 0; sample < 20; sample++) {
            long entryId = Math.floorMod(sample * 7_919L, workload.entries().size());
            long start = System.nanoTime();
            assertThat(handle.read(entryId, entryId).toCompletableFuture().join())
                    .singleElement()
                    .extracting(EntryPayload::entryId)
                    .isEqualTo(entryId);
            randomSamplesMicros.add(microsSince(start));
            peakMemory.sample();
        }
        long sequentialStart = System.nanoTime();
        int sequentialEntries = Math.min(1_000, workload.entries().size());
        assertThat(handle.read(0, sequentialEntries - 1L).toCompletableFuture().join())
                .hasSize(sequentialEntries);
        long sequentialMicros = microsSince(sequentialStart);

        long concurrencyStart = System.nanoTime();
        CompletableFuture<?>[] concurrent = new CompletableFuture<?>[4];
        for (int index = 0; index < concurrent.length; index++) {
            long entryId = (long) index * (workload.entries().size() / concurrent.length);
            concurrent[index] = handle.read(entryId, entryId).toCompletableFuture();
        }
        CompletableFuture.allOf(concurrent).join();
        long concurrencyMicros = microsSince(concurrencyStart);
        handle.close().toCompletableFuture().join();
        peakMemory.sample();

        return new CaseResult(
                workload.name(),
                targetBytes,
                compressionPolicy.name(),
                data.blocks().size(),
                data.bytes(),
                workload.decodedBytes(),
                elapsedMicros(wallBefore),
                Math.max(0, (processCpuTime() - cpuBefore) / 1_000),
                percentile(randomSamplesMicros, 50),
                percentile(randomSamplesMicros, 99),
                sequentialMicros,
                concurrencyMicros,
                4,
                store.requests.get(),
                store.transferredBytes.get(),
                percentile(store.latenciesMicros, 50),
                percentile(store.latenciesMicros, 99),
                List.copyOf(randomSamplesMicros));
    }

    private BoundaryResult runBoundary(
            String name, int payloadBytes, CompressionPolicy compressionPolicy, PeakMemory peakMemory) {
        UUID attemptUuid = UUID.nameUUIDFromBytes(name.getBytes(StandardCharsets.UTF_8));
        byte[] payload = new byte[payloadBytes];
        new Random(name.hashCode()).nextBytes(payload);
        DataObject data = encode(
                name,
                PulsarOffloadLimitCandidateV1.MIB,
                compressionPolicy,
                attemptUuid,
                List.of(new EntryPayload(0, payload)));
        CountingStore store = new CountingStore(provider);
        PreparedAttempt prepared = prepared(attemptUuid, data, PulsarOffloadLimitCandidateV1.MIB);
        PulsarPublishedAttemptVerifierV1 verifier = new PulsarPublishedAttemptVerifierV1(store, LIMITS, attempt -> KEY);
        new PulsarSealedLedgerPublisherV1(store, LIMITS, verifier, Runnable::run)
                .publish(prepared)
                .toCompletableFuture()
                .join();
        peakMemory.sample();
        return new BoundaryResult(
                name,
                payloadBytes,
                data.bytes(),
                data.blocks().get(0).compressionFamily().name(),
                store.requests.get(),
                store.transferredBytes.get());
    }

    private DataObject encode(
            String caseId,
            int targetBytes,
            CompressionPolicy compressionPolicy,
            UUID attemptUuid,
            List<EntryPayload> entries) {
        try (StreamingEncoder encoder = Npd1CodecV1.openStreaming(
                temporaryDirectory.resolve(caseId + ".npd1"),
                targetBytes,
                compressionPolicy,
                KEY,
                attemptUuid,
                LIMITS)) {
            entries.forEach(encoder::append);
            return encoder.finish();
        }
    }

    private static PreparedAttempt prepared(UUID attemptUuid, DataObject data, int targetBytes) {
        long entryCount = data.lastEntryId() + 1;
        long logicalLength = data.blocks().stream()
                .mapToLong(Npd1CodecV1.SparseBlock::decodedBlockBytes)
                .sum();
        PulsarSealedLedgerAttemptV1 attempt = new PulsarSealedLedgerAttemptV1(
                Math.floorMod(attemptUuid.getLeastSignificantBits(), Long.MAX_VALUE),
                attemptUuid,
                data.lastEntryId(),
                entryCount,
                logicalLength,
                100,
                7,
                "evidence/p6/cells/pulsar-a",
                RetentionClass.DELETE_AFTER_VERIFIED,
                DeleteState.BK_DELETE_NONE,
                false);
        SealedLedgerSection sealed = new SealedLedgerSection(
                data.lastEntryId(),
                entryCount,
                logicalLength,
                100,
                7,
                3,
                3,
                2,
                DigestType.CRC32C,
                Map.of("evidence", new CustomMetadataValue(new byte[] {1})),
                List.of(new EnsembleSegment(0, List.of("bookie-1", "bookie-2", "bookie-3"))));
        return new PreparedAttempt(attempt, sealed, data, targetBytes);
    }

    private static List<EntryPayload> smallEntries() {
        List<EntryPayload> result = new ArrayList<>(SMALL_ENTRY_COUNT);
        for (int entryId = 0; entryId < SMALL_ENTRY_COUNT; entryId++) {
            byte[] payload = new byte[SMALL_ENTRY_BYTES];
            if ((entryId & 1) != 0) {
                new Random(entryId).nextBytes(payload);
            }
            result.add(new EntryPayload(entryId, payload));
        }
        return List.copyOf(result);
    }

    private static List<EntryPayload> scanEntries() {
        List<EntryPayload> result = new ArrayList<>(SCAN_ENTRY_COUNT);
        for (int entryId = 0; entryId < SCAN_ENTRY_COUNT; entryId++) {
            byte[] payload = new byte[SCAN_ENTRY_BYTES];
            if ((entryId & 1) != 0) {
                new Random(entryId * 31L).nextBytes(payload);
            }
            result.add(new EntryPayload(entryId, payload));
        }
        return List.copyOf(result);
    }

    private static String json(
            String testedSource,
            String pulsarSource,
            List<CaseResult> matrix,
            BoundaryResult stock,
            BoundaryResult near,
            PeakMemory peakMemory) {
        StringBuilder output = new StringBuilder();
        output.append("{\n")
                .append("  \"schema\": \"NEREUS_V2_M2_PULSAR_P6_CANDIDATE_V1\",\n")
                .append("  \"generatedAt\": \"")
                .append(Instant.now())
                .append("\",\n")
                .append("  \"testedSourceCommit\": \"")
                .append(testedSource)
                .append("\",\n")
                .append("  \"pulsarSourceCommit\": \"")
                .append(pulsarSource)
                .append("\",\n")
                .append("  \"provider\": {\"kind\": \"S3_COMPATIBLE_LOCALSTACK\", \"image\": ")
                .append("\"localstack/localstack:4.14.0\", \"awsSdkV2\": \"2.47.5\"},\n")
                .append("  \"sourceDefaults\": {\"maxMessageSize\": 5242880, ")
                .append("\"managedLedgerMaxEntriesPerLedger\": 50000, ")
                .append("\"managedLedgerMaxSizePerLedgerMbytes\": 2048, ")
                .append("\"nativeReadBufferBytes\": 1048576},\n")
                .append("  \"selectedHardLimits\": {\"maxDataObjectBytes\": 4294967296, ")
                .append("\"maxMultipartParts\": 1024, \"maxEntryBytes\": 67108864, ")
                .append("\"maxDecodedBlockBytes\": 67108864, \"maxEntriesPerBlock\": 65536},\n")
                .append("  \"observedMemory\": {\"heapPeakBytes\": ")
                .append(peakMemory.heapPeak.get())
                .append(", \"directPeakBytes\": ")
                .append(peakMemory.directPeak.get())
                .append("},\n")
                .append("  \"matrix\": [\n");
        for (int index = 0; index < matrix.size(); index++) {
            if (index > 0) {
                output.append(",\n");
            }
            output.append("    ").append(matrix.get(index).json());
        }
        output.append("\n  ],\n")
                .append("  \"boundaryCoverage\": [")
                .append(stock.json())
                .append(", ")
                .append(near.json())
                .append("],\n")
                .append("  \"selection\": {\"classes\": [\"latency-1mib\", \"balanced-4mib\", ")
                .append("\"scan-8mib\"], \"deploymentDefault\": \"balanced-4mib\", ")
                .append("\"excludedCandidateBytes\": 16777216},\n")
                .append(
                        "  \"claimBoundary\": \"LocalStack proves S3-compatible adapter behavior; it is not Amazon S3 performance or service endorsement\"\n")
                .append("}\n");
        return output.toString();
    }

    private static long percentile(List<Long> values, int percentile) {
        if (values.isEmpty()) {
            return 0;
        }
        List<Long> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        int index = Math.min(sorted.size() - 1, Math.max(0, (int) Math.ceil(percentile * sorted.size() / 100.0) - 1));
        return sorted.get(index);
    }

    private static long processCpuTime() {
        java.lang.management.OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean();
        return bean instanceof com.sun.management.OperatingSystemMXBean operatingSystem
                ? operatingSystem.getProcessCpuTime()
                : 0;
    }

    private static long elapsedMicros(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000;
    }

    private static long microsSince(long startNanos) {
        return Math.max(1, elapsedMicros(startNanos));
    }

    private static String requiredProperty(String name) {
        String value = System.getProperty(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("missing required evidence property " + name);
        }
        return value;
    }

    private record CaseResult(
            String workload,
            int targetBytes,
            String compressionPolicy,
            int blockCount,
            long encodedBytes,
            long decodedBytes,
            long wallMicros,
            long processCpuMicros,
            long randomP50Micros,
            long randomP99Micros,
            long sequentialMicros,
            long concurrencyMicros,
            int concurrency,
            long providerRequests,
            long providerTransferredBytes,
            long providerP50Micros,
            long providerP99Micros,
            List<Long> randomReadSamplesMicros) {
        private String json() {
            return String.format(
                    Locale.ROOT,
                    "{\"workload\": \"%s\", \"targetBytes\": %d, \"compressionPolicy\": \"%s\", "
                            + "\"blockCount\": %d, "
                            + "\"encodedBytes\": %d, \"decodedBytes\": %d, \"compressionRatio\": %.6f, "
                            + "\"wallMicros\": %d, \"processCpuMicros\": %d, \"randomP50Micros\": %d, "
                            + "\"randomP99Micros\": %d, \"sequential1000Micros\": %d, "
                            + "\"concurrency4Micros\": %d, \"concurrency\": %d, \"providerRequests\": %d, "
                            + "\"providerTransferredBytes\": %d, \"providerP50Micros\": %d, "
                            + "\"providerP99Micros\": %d}",
                    workload,
                    targetBytes,
                    compressionPolicy,
                    blockCount,
                    encodedBytes,
                    decodedBytes,
                    (double) encodedBytes / decodedBytes,
                    wallMicros,
                    processCpuMicros,
                    randomP50Micros,
                    randomP99Micros,
                    sequentialMicros,
                    concurrencyMicros,
                    concurrency,
                    providerRequests,
                    providerTransferredBytes,
                    providerP50Micros,
                    providerP99Micros);
        }
    }

    private record Workload(String name, List<EntryPayload> entries, int entryBytes) {
        private Workload {
            entries = List.copyOf(entries);
        }

        private long decodedBytes() {
            return Math.multiplyExact((long) entries.size(), entryBytes);
        }
    }

    private record BoundaryResult(
            String name,
            long decodedBytes,
            long encodedBytes,
            String actualCompression,
            long providerRequests,
            long providerTransferredBytes) {
        private String json() {
            return String.format(
                    Locale.ROOT,
                    "{\"name\": \"%s\", \"decodedBytes\": %d, \"encodedBytes\": %d, "
                            + "\"actualCompression\": \"%s\", \"providerRequests\": %d, "
                            + "\"providerTransferredBytes\": %d}",
                    name,
                    decodedBytes,
                    encodedBytes,
                    actualCompression,
                    providerRequests,
                    providerTransferredBytes);
        }
    }

    private static final class PeakMemory {
        private final AtomicLong heapPeak = new AtomicLong();
        private final AtomicLong directPeak = new AtomicLong();

        private void sample() {
            heapPeak.accumulateAndGet(
                    ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed(), Math::max);
            long direct = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class).stream()
                    .filter(pool -> "direct".equals(pool.getName()))
                    .mapToLong(BufferPoolMXBean::getMemoryUsed)
                    .sum();
            directPeak.accumulateAndGet(direct, Math::max);
        }
    }

    private static final class CountingStore implements PulsarOffloadObjectStoreV1 {
        private final PulsarOffloadObjectStoreV1 delegate;
        private final AtomicLong requests = new AtomicLong();
        private final AtomicLong transferredBytes = new AtomicLong();
        private final List<Long> latenciesMicros = Collections.synchronizedList(new ArrayList<>());

        private CountingStore(PulsarOffloadObjectStoreV1 delegate) {
            this.delegate = delegate;
        }

        @Override
        public Capabilities capabilities() {
            return delegate.capabilities();
        }

        @Override
        public CompletionStage<ImmutableObject> createImmutable(String key, Body body) {
            return measured(body.bytes(), () -> delegate.createImmutable(key, body));
        }

        @Override
        public CompletionStage<ImmutableObject> head(String key) {
            return measured(0, () -> delegate.head(key));
        }

        @Override
        public CompletionStage<byte[]> readRange(String key, long offset, int length) {
            return measured(length, () -> delegate.readRange(key, offset, length));
        }

        @Override
        public CompletionStage<Void> deleteAndProveAbsent(String key) {
            return measured(0, () -> delegate.deleteAndProveAbsent(key));
        }

        @Override
        public CompletionStage<Void> cleanupAttemptMultipartResidue(String attemptPrefix) {
            return measured(0, () -> delegate.cleanupAttemptMultipartResidue(attemptPrefix));
        }

        @Override
        public CompletionStage<Void> close() {
            return CompletableFuture.completedFuture(null);
        }

        private <T> CompletionStage<T> measured(long bytes, Supplier<CompletionStage<T>> operation) {
            long start = System.nanoTime();
            requests.incrementAndGet();
            transferredBytes.addAndGet(bytes);
            CompletionStage<T> stage = operation.get();
            return stage.whenComplete((ignored, failure) -> latenciesMicros.add(microsSince(start)));
        }
    }
}
