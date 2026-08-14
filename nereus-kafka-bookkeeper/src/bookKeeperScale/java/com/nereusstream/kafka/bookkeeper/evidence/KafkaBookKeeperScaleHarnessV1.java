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

package com.nereusstream.kafka.bookkeeper.evidence;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperDigestTypeV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperProtocolModeV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperTimeoutClassV1;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationResultV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerAppendRequestV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerConfigurationV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import com.nereusstream.storage.bookkeeper.BookKeeperV3Crc32cAddPayloadLimitV1;
import com.nereusstream.storage.bookkeeper.ImmutableRetainedStoragePayload;
import com.nereusstream.storage.bookkeeper.RealBookKeeperCellSessionV1;
import com.nereusstream.storage.bookkeeper.RealBookKeeperClientConfigurationV1;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.client.api.BookKeeper;

/** Exact-image K9 scale harness. Its thresholds are read before the run from the committed plan. */
public final class KafkaBookKeeperScaleHarnessV1 {
    private static final String PLAN_SCHEMA = "NEREUS_V2_M2_KAFKA_K9_SCALE_PLAN_V1";
    private static final String CONFIG_SCHEMA = "NEREUS_V2_M2_KAFKA_BOOKKEEPER_CONFORMANCE_CONFIG_V1";
    private static final byte[] PASSWORD = new byte[0];
    private static final long OPERATION_TIMEOUT_SECONDS = 45;

    private KafkaBookKeeperScaleHarnessV1() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            throw new IllegalArgumentException(
                    "usage: <scale-plan> <conformance-config> <tier> <output> <tested-source-commit>");
        }
        Path planPath = Path.of(args[0]);
        Path conformancePath = Path.of(args[1]);
        Plan plan = Plan.load(planPath, Integer.parseInt(args[2]));
        Conformance conformance = Conformance.load(conformancePath, plan);
        Path output = Path.of(args[3]);
        String testedSourceCommit = requireCommit(args[4], "tested source commit");
        Files.createDirectories(output.toAbsolutePath().getParent());

        ResourceSampler sampler = new ResourceSampler();
        sampler.start();
        long started = System.nanoTime();
        RunResult result;
        try {
            result = execute(plan, conformance, sampler);
        } finally {
            sampler.stop();
        }
        long elapsed = System.nanoTime() - started;
        result = result.withElapsed(elapsed, sampler.snapshot());
        result.validate(plan);
        Files.writeString(
                output,
                result.toJson(plan, conformance, testedSourceCommit, sha256(planPath), sha256(conformancePath)),
                StandardCharsets.UTF_8);
        System.out.println("K9 scale tier " + plan.partitions + " PASS output=" + output);
    }

    private static RunResult execute(Plan plan, Conformance conformance, ResourceSampler sampler) throws Exception {
        BookKeeperCapabilitySnapshotV1 capability = conformance.capability(plan);
        Latencies create = new Latencies();
        Latencies append = new Latencies();
        Latencies close = new Latencies();
        Latencies read = new Latencies();
        Latencies recovery = new Latencies();
        List<RunLedgerHandleV1> hot = new ArrayList<>(plan.hotLedgerAdmission);
        MessageDigest handleDigest = messageDigest();
        AtomicLong metadataMutations = new AtomicLong();
        AtomicLong appendedEntries = new AtomicLong();
        AtomicLong appendedBytes = new AtomicLong();
        long maximumOwnedHandles = 0;

        try (BookKeeper client = BookKeeper.newBuilder(
                        RealBookKeeperClientConfigurationV1.from(conformance.metadataServiceUri, capability))
                .build()) {
            if (!client.isDriverMetadataServiceAvailable().get(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("BookKeeper metadata service is unavailable");
            }
            RealBookKeeperCellSessionV1 session = new RealBookKeeperCellSessionV1(client, capability, PASSWORD);
            try {
                for (int base = 0; base < plan.partitions; base += plan.ioConcurrency) {
                    int end = Math.min(plan.partitions, base + plan.ioConcurrency);
                    List<CompletableFuture<CreatedLedger>> batch = new ArrayList<>(end - base);
                    for (int partition = base; partition < end; partition++) {
                        boolean retain = partition >= plan.partitions - plan.hotLedgerAdmission;
                        batch.add(createPartition(
                                session,
                                capability,
                                plan,
                                partition,
                                retain,
                                create,
                                append,
                                close,
                                metadataMutations,
                                appendedEntries,
                                appendedBytes));
                    }
                    CompletableFuture.allOf(batch.toArray(CompletableFuture[]::new))
                            .get(OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    for (CompletableFuture<CreatedLedger> future : batch) {
                        CreatedLedger ledger = future.join();
                        digestHandle(handleDigest, ledger.partition, ledger.handle);
                        if (ledger.retained) {
                            hot.add(ledger.handle);
                        }
                    }
                    maximumOwnedHandles = Math.max(maximumOwnedHandles, hot.size());
                    if (end % 1_000 == 0 || end == plan.partitions) {
                        System.out.println("K9 scale progress partitions=" + end + "/" + plan.partitions);
                    }
                    sampler.sample();
                }

                appendRecoveryTails(session, plan, hot, append, appendedEntries, appendedBytes);
                readSamples(session, plan, hot, read);
                List<RunLedgerHandleV1> recovered = recoverSamples(session, plan, hot, recovery);
                rolloverSamples(
                        session,
                        capability,
                        plan,
                        recovered,
                        create,
                        append,
                        close,
                        metadataMutations,
                        appendedEntries,
                        appendedBytes);
                session.closeAsync().toCompletableFuture().get(OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } finally {
                session.closeAsync().toCompletableFuture().get(OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
        }

        return new RunResult(
                plan.partitions,
                plan.partitions + plan.rolloverSamples,
                appendedEntries.get(),
                appendedBytes.get(),
                metadataMutations.get(),
                maximumOwnedHandles,
                HexFormat.of().formatHex(handleDigest.digest()),
                create.snapshot(),
                append.snapshot(),
                close.snapshot(),
                read.snapshot(),
                recovery.snapshot(),
                0,
                ResourceSnapshot.empty());
    }

    private static CompletableFuture<CreatedLedger> createPartition(
            RealBookKeeperCellSessionV1 session,
            BookKeeperCapabilitySnapshotV1 capability,
            Plan plan,
            int partition,
            boolean retain,
            Latencies create,
            Latencies append,
            Latencies close,
            AtomicLong metadataMutations,
            AtomicLong appendedEntries,
            AtomicLong appendedBytes) {
        RunLedgerConfigurationV1 configuration = RunLedgerConfigurationV1.from(
                capability, new StorageRunId(new Id128(plan.partitions, (long) partition + 1)));
        long createStarted = System.nanoTime();
        return session.createRunLedger(configuration)
                .thenCompose(result -> {
                    create.add(System.nanoTime() - createStarted);
                    RunLedgerHandleV1 handle = exact(result, "create partition " + partition);
                    metadataMutations.incrementAndGet();
                    return append(
                                    session,
                                    handle,
                                    0,
                                    payload(plan.payloadBytes, plan.partitions, partition, 0),
                                    append,
                                    appendedEntries,
                                    appendedBytes)
                            .thenCompose(ignored -> {
                                if (retain) {
                                    return CompletableFuture.completedFuture(
                                            new CreatedLedger(partition, handle, true));
                                }
                                long closeStarted = System.nanoTime();
                                return session.closeRunLedger(handle).thenApply(closeResult -> {
                                    close.add(System.nanoTime() - closeStarted);
                                    exact(closeResult, "close partition " + partition);
                                    metadataMutations.incrementAndGet();
                                    return new CreatedLedger(partition, handle, false);
                                });
                            });
                })
                .toCompletableFuture();
    }

    private static CompletionStage<Void> append(
            RealBookKeeperCellSessionV1 session,
            RunLedgerHandleV1 handle,
            long entryId,
            byte[] bytes,
            Latencies latencies,
            AtomicLong appendedEntries,
            AtomicLong appendedBytes) {
        ImmutableRetainedStoragePayload payload = ImmutableRetainedStoragePayload.copyOf(bytes);
        long started = System.nanoTime();
        CompletionStage<Void> stage = session.appendExplicitEntry(
                        new RunLedgerAppendRequestV1(handle, entryId, payload))
                .thenApply(result -> {
                    latencies.add(System.nanoTime() - started);
                    exact(result, "append ledger " + handle.ledgerIdentity().ledgerId() + " entry " + entryId);
                    appendedEntries.incrementAndGet();
                    appendedBytes.addAndGet(bytes.length);
                    return null;
                });
        return stage.whenComplete((ignored, failure) -> payload.release());
    }

    private static void appendRecoveryTails(
            RealBookKeeperCellSessionV1 session,
            Plan plan,
            List<RunLedgerHandleV1> hot,
            Latencies append,
            AtomicLong appendedEntries,
            AtomicLong appendedBytes)
            throws Exception {
        List<RunLedgerHandleV1> sample = hot.subList(0, plan.recoverySamples);
        for (long entryId = 1; entryId < plan.tailEntries; entryId++) {
            for (int base = 0; base < sample.size(); base += plan.ioConcurrency) {
                int end = Math.min(sample.size(), base + plan.ioConcurrency);
                List<CompletableFuture<Void>> batch = new ArrayList<>(end - base);
                for (int index = base; index < end; index++) {
                    batch.add(append(
                                    session,
                                    sample.get(index),
                                    entryId,
                                    payload(plan.payloadBytes, plan.partitions, index, entryId),
                                    append,
                                    appendedEntries,
                                    appendedBytes)
                            .toCompletableFuture());
                }
                CompletableFuture.allOf(batch.toArray(CompletableFuture[]::new))
                        .get(OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
        }
    }

    private static void readSamples(
            RealBookKeeperCellSessionV1 session, Plan plan, List<RunLedgerHandleV1> hot, Latencies read)
            throws Exception {
        for (int base = 0; base < plan.readSamples; base += plan.ioConcurrency) {
            int end = Math.min(plan.readSamples, base + plan.ioConcurrency);
            List<CompletableFuture<Void>> batch = new ArrayList<>(end - base);
            for (int index = base; index < end; index++) {
                RunLedgerHandleV1 handle = hot.get(index);
                long entryId = index < plan.recoverySamples ? plan.tailEntries - 1L : 0;
                long started = System.nanoTime();
                batch.add(session.readExactEntry(handle, entryId)
                        .thenAccept(result -> {
                            read.add(System.nanoTime() - started);
                            if (result.exactEntry().isEmpty()
                                    || result.exactEntry().orElseThrow().entryId() != entryId) {
                                throw new IllegalStateException("targeted read did not return the exact entry");
                            }
                        })
                        .toCompletableFuture());
            }
            CompletableFuture.allOf(batch.toArray(CompletableFuture[]::new))
                    .get(OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }

    private static List<RunLedgerHandleV1> recoverSamples(
            RealBookKeeperCellSessionV1 session, Plan plan, List<RunLedgerHandleV1> hot, Latencies recovery)
            throws Exception {
        List<RunLedgerHandleV1> recovered = new ArrayList<>(plan.recoverySamples);
        for (int base = 0; base < plan.recoverySamples; base += plan.ioConcurrency) {
            int end = Math.min(plan.recoverySamples, base + plan.ioConcurrency);
            List<CompletableFuture<RunLedgerHandleV1>> batch = new ArrayList<>(end - base);
            for (int index = base; index < end; index++) {
                RunLedgerHandleV1 handle = hot.get(index);
                long started = System.nanoTime();
                batch.add(session.fenceAndRecoverRunLedger(handle)
                        .thenApply(result -> {
                            recovery.add(System.nanoTime() - started);
                            var proof = exact(
                                    result,
                                    "recover ledger " + handle.ledgerIdentity().ledgerId());
                            if (proof.lastAddConfirmed() != plan.tailEntries - 1L || !proof.fenced()) {
                                throw new IllegalStateException("recovery did not prove the exact bounded tail");
                            }
                            return handle;
                        })
                        .toCompletableFuture());
            }
            CompletableFuture.allOf(batch.toArray(CompletableFuture[]::new))
                    .get(OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            for (CompletableFuture<RunLedgerHandleV1> future : batch) {
                recovered.add(future.join());
            }
        }
        return recovered;
    }

    private static void rolloverSamples(
            RealBookKeeperCellSessionV1 session,
            BookKeeperCapabilitySnapshotV1 capability,
            Plan plan,
            List<RunLedgerHandleV1> recovered,
            Latencies create,
            Latencies append,
            Latencies close,
            AtomicLong metadataMutations,
            AtomicLong appendedEntries,
            AtomicLong appendedBytes)
            throws Exception {
        for (int base = 0; base < plan.rolloverSamples; base += plan.ioConcurrency) {
            int end = Math.min(plan.rolloverSamples, base + plan.ioConcurrency);
            List<CompletableFuture<Void>> batch = new ArrayList<>(end - base);
            for (int index = base; index < end; index++) {
                RunLedgerHandleV1 predecessor = recovered.get(index);
                int sample = index;
                long closeStarted = System.nanoTime();
                batch.add(session.closeRunLedger(predecessor)
                        .thenCompose(closeResult -> {
                            close.add(System.nanoTime() - closeStarted);
                            exact(closeResult, "close rollover predecessor " + sample);
                            metadataMutations.incrementAndGet();
                            RunLedgerConfigurationV1 successor = RunLedgerConfigurationV1.from(
                                    capability, new StorageRunId(new Id128(plan.partitions + 1L, (long) sample + 1)));
                            long createStarted = System.nanoTime();
                            return session.createRunLedger(successor).thenCompose(createResult -> {
                                create.add(System.nanoTime() - createStarted);
                                RunLedgerHandleV1 handle = exact(createResult, "create successor " + sample);
                                metadataMutations.incrementAndGet();
                                return append(
                                        session,
                                        handle,
                                        0,
                                        payload(plan.payloadBytes, plan.partitions + 1, sample, 0),
                                        append,
                                        appendedEntries,
                                        appendedBytes);
                            });
                        })
                        .toCompletableFuture());
            }
            CompletableFuture.allOf(batch.toArray(CompletableFuture[]::new))
                    .get(OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }

    private static byte[] payload(int length, int tier, int partition, long entryId) {
        byte[] bytes = new byte[length];
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(0x4b395631);
        buffer.putInt(tier);
        buffer.putInt(partition);
        buffer.putLong(entryId);
        for (int index = buffer.position(); index < bytes.length; index++) {
            bytes[index] = (byte) (partition * 31L + entryId * 17L + index);
        }
        return bytes;
    }

    private static <T> T exact(ProviderMutationResultV1<T> result, String operation) {
        if (result.outcome() != ProviderMutationOutcomeV1.APPLIED_EXACT) {
            throw new IllegalStateException(operation + " was not exact: " + result.outcome());
        }
        return result.exactProof().orElseThrow();
    }

    private static void digestHandle(MessageDigest digest, int partition, RunLedgerHandleV1 handle) {
        digest.update(ByteBuffer.allocate(24)
                .putLong(partition)
                .putLong(handle.runId().value().lowBits())
                .putLong(handle.ledgerIdentity().ledgerId())
                .array());
    }

    private static MessageDigest messageDigest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException(failure);
        }
    }

    private static String sha256(Path path) throws IOException {
        return HexFormat.of().formatHex(messageDigest().digest(Files.readAllBytes(path)));
    }

    private static String requireCommit(String value, String name) {
        if (!value.matches("[0-9a-f]{40}")) {
            throw new IllegalArgumentException(name + " must be 40 lowercase hex characters");
        }
        return value;
    }

    private static Properties load(Path path) throws IOException {
        Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(path)) {
            properties.load(input);
        }
        return properties;
    }

    private static String required(Properties properties, String key) {
        String value = properties.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("missing property " + key);
        }
        return value;
    }

    private static long positiveLong(Properties properties, String key) {
        long value = Long.parseLong(required(properties, key));
        if (value <= 0) {
            throw new IllegalArgumentException(key + " must be positive");
        }
        return value;
    }

    private static int positiveInt(Properties properties, String key) {
        return Math.toIntExact(positiveLong(properties, key));
    }

    private record Plan(
            int partitions,
            int ioConcurrency,
            int hotLedgerAdmission,
            int payloadBytes,
            int tailEntries,
            int readSamples,
            int recoverySamples,
            int rolloverSamples,
            long maxHarnessHeapBytes,
            long maxHarnessDirectBytes,
            long maxHarnessOpenFileDescriptors,
            long maxCreateP99Nanos,
            long maxAppendP99Nanos,
            long maxCloseP99Nanos,
            long maxReadP99Nanos,
            long maxRecoveryP99Nanos,
            long minimumPartitionOperationsPerSecond,
            long maximumElapsedNanos,
            long maxMetadataMutationsPerPartition,
            String bookKeeperClientSourceCommit,
            Sha256Digest bookKeeperClientJarSha256,
            String bookKeeperServerSourceCommit,
            Sha256Digest bookKeeperImageManifestSha256,
            String bookKeeperConfigurationSha256) {
        private static Plan load(Path path, int tier) throws IOException {
            Properties properties = KafkaBookKeeperScaleHarnessV1.load(path);
            if (!PLAN_SCHEMA.equals(required(properties, "schema"))) {
                throw new IllegalArgumentException("scale plan schema differs");
            }
            List<Integer> tiers = List.of(required(properties, "tiers").split(",")).stream()
                    .map(Integer::parseInt)
                    .toList();
            if (!tiers.contains(tier)) {
                throw new IllegalArgumentException("scale tier is not predeclared: " + tier);
            }
            Plan plan = new Plan(
                    tier,
                    positiveInt(properties, "ioConcurrency"),
                    positiveInt(properties, "hotLedgerAdmission"),
                    positiveInt(properties, "payloadBytes"),
                    positiveInt(properties, "tailEntries"),
                    positiveInt(properties, "readSamples"),
                    positiveInt(properties, "recoverySamples"),
                    positiveInt(properties, "rolloverSamples"),
                    positiveLong(properties, "maxHarnessHeapBytes"),
                    positiveLong(properties, "maxHarnessDirectBytes"),
                    positiveLong(properties, "maxHarnessOpenFileDescriptors"),
                    positiveLong(properties, "maxCreateP99Nanos"),
                    positiveLong(properties, "maxAppendP99Nanos"),
                    positiveLong(properties, "maxCloseP99Nanos"),
                    positiveLong(properties, "maxReadP99Nanos"),
                    positiveLong(properties, "maxRecoveryP99Nanos"),
                    positiveLong(properties, "minimumPartitionOperationsPerSecond"),
                    positiveLong(properties, "maxTier" + tier + "ElapsedNanos"),
                    positiveLong(properties, "maxMetadataMutationsPerPartition"),
                    requireCommit(required(properties, "bookKeeperClientSourceCommit"), "BookKeeper client commit"),
                    Sha256Digest.copyOf(HexFormat.of().parseHex(required(properties, "bookKeeperClientJarSha256"))),
                    requireCommit(required(properties, "bookKeeperServerSourceCommit"), "BookKeeper server commit"),
                    Sha256Digest.copyOf(HexFormat.of().parseHex(required(properties, "bookKeeperImageManifestSha256"))),
                    required(properties, "bookKeeperConfigurationSha256"));
            if (plan.hotLedgerAdmission > tier
                    || plan.readSamples > plan.hotLedgerAdmission
                    || plan.recoverySamples > plan.readSamples
                    || plan.rolloverSamples > plan.recoverySamples
                    || plan.ioConcurrency > plan.hotLedgerAdmission) {
                throw new IllegalArgumentException("scale sample hierarchy is invalid");
            }
            return plan;
        }
    }

    private record Conformance(
            String metadataServiceUri,
            int frameLimitBytes,
            int maximumAddPayloadBytes,
            int ensembleSize,
            int writeQuorumSize,
            int ackQuorumSize,
            long connectTimeoutMillis,
            long addTimeoutMillis,
            long readTimeoutMillis,
            long recoveryTimeoutMillis,
            String credentialIdentityVersion,
            Sha256Digest configurationDigest) {
        private static Conformance load(Path path, Plan plan) throws IOException {
            Properties properties = KafkaBookKeeperScaleHarnessV1.load(path);
            if (!CONFIG_SCHEMA.equals(required(properties, "schema"))) {
                throw new IllegalArgumentException("conformance schema differs");
            }
            String actualConfigurationSha = sha256(path);
            if (!plan.bookKeeperConfigurationSha256.equals(actualConfigurationSha)) {
                throw new IllegalArgumentException("conformance configuration SHA-256 differs from scale plan");
            }
            if (!"CRC32C".equals(required(properties, "digestType"))
                    || !"V3".equals(required(properties, "protocolMode"))) {
                throw new IllegalArgumentException("scale requires the exact CRC32C/v3 capability");
            }
            int clientFrame = positiveInt(properties, "clientFrameLimitBytes");
            int serverFrame = positiveInt(properties, "serverFrameLimitBytes");
            if (clientFrame != serverFrame) {
                throw new IllegalArgumentException("scale requires equal client/server frame limits");
            }
            int maximumPayload = positiveInt(properties, "maximumAddPayloadBytes");
            int projected = BookKeeperV3Crc32cAddPayloadLimitV1.maximumAddPayloadBytes(clientFrame, serverFrame);
            if (maximumPayload != projected || plan.payloadBytes >= maximumPayload) {
                throw new IllegalArgumentException("payload or CRC32C frame projection differs");
            }
            return new Conformance(
                    required(properties, "metadataServiceUri"),
                    clientFrame,
                    maximumPayload,
                    positiveInt(properties, "ensembleSize"),
                    positiveInt(properties, "writeQuorumSize"),
                    positiveInt(properties, "ackQuorumSize"),
                    positiveLong(properties, "connectTimeoutMillis"),
                    positiveLong(properties, "addTimeoutMillis"),
                    positiveLong(properties, "readTimeoutMillis"),
                    positiveLong(properties, "recoveryTimeoutMillis"),
                    required(properties, "credentialIdentityVersion"),
                    Sha256Digest.copyOf(HexFormat.of().parseHex(actualConfigurationSha)));
        }

        private BookKeeperCapabilitySnapshotV1 capability(Plan plan) {
            return new BookKeeperCapabilitySnapshotV1(
                    new CellProviderScopeId(Sha256Digest.hash(
                            CanonicalBytes.copyOf("k9-real-scale-v1".getBytes(StandardCharsets.UTF_8)))),
                    plan.bookKeeperClientSourceCommit,
                    plan.bookKeeperClientJarSha256,
                    plan.bookKeeperServerSourceCommit,
                    plan.bookKeeperImageManifestSha256,
                    BookKeeperProtocolModeV1.V3,
                    frameLimitBytes,
                    frameLimitBytes,
                    maximumAddPayloadBytes,
                    true,
                    ensembleSize,
                    writeQuorumSize,
                    ackQuorumSize,
                    BookKeeperDigestTypeV1.CRC32C,
                    true,
                    true,
                    new BookKeeperTimeoutClassV1(
                            connectTimeoutMillis, addTimeoutMillis, readTimeoutMillis, recoveryTimeoutMillis),
                    credentialIdentityVersion,
                    configurationDigest);
        }
    }

    private static final class Latencies {
        private final List<Long> nanos = new ArrayList<>();

        private synchronized void add(long value) {
            if (value < 0) {
                throw new IllegalArgumentException("latency must be non-negative");
            }
            nanos.add(value);
        }

        private synchronized LatencySnapshot snapshot() {
            if (nanos.isEmpty()) {
                return new LatencySnapshot(0, 0, 0, 0);
            }
            List<Long> ordered = nanos.stream().sorted().toList();
            return new LatencySnapshot(
                    ordered.size(), percentile(ordered, 50), percentile(ordered, 99), ordered.get(ordered.size() - 1));
        }

        private static long percentile(List<Long> ordered, int percentile) {
            int index = Math.max(0, (int) Math.ceil(ordered.size() * percentile / 100.0d) - 1);
            return ordered.get(index);
        }
    }

    private record LatencySnapshot(int samples, long p50Nanos, long p99Nanos, long maximumNanos) {}

    private static final class ResourceSampler {
        private final AtomicBoolean running = new AtomicBoolean();
        private final AtomicLong maximumHeap = new AtomicLong();
        private final AtomicLong maximumDirect = new AtomicLong();
        private final AtomicLong maximumFileDescriptors = new AtomicLong();
        private Thread thread;

        private void start() {
            running.set(true);
            thread = new Thread(
                    () -> {
                        while (running.get()) {
                            sample();
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException interrupted) {
                                Thread.currentThread().interrupt();
                                return;
                            }
                        }
                    },
                    "k9-resource-sampler");
            thread.setDaemon(true);
            thread.start();
        }

        private void stop() throws InterruptedException {
            running.set(false);
            if (thread != null) {
                thread.join(5_000);
            }
            sample();
        }

        private void sample() {
            Runtime runtime = Runtime.getRuntime();
            maximumHeap.accumulateAndGet(runtime.totalMemory() - runtime.freeMemory(), Math::max);
            long direct = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class).stream()
                    .filter(pool -> "direct".equals(pool.getName()))
                    .mapToLong(BufferPoolMXBean::getMemoryUsed)
                    .sum();
            maximumDirect.accumulateAndGet(direct, Math::max);
            var operatingSystem = ManagementFactory.getOperatingSystemMXBean();
            if (operatingSystem instanceof com.sun.management.UnixOperatingSystemMXBean unix) {
                maximumFileDescriptors.accumulateAndGet(unix.getOpenFileDescriptorCount(), Math::max);
            }
        }

        private ResourceSnapshot snapshot() {
            return new ResourceSnapshot(maximumHeap.get(), maximumDirect.get(), maximumFileDescriptors.get());
        }
    }

    private record ResourceSnapshot(long maximumHeapBytes, long maximumDirectBytes, long maximumOpenFileDescriptors) {
        private static ResourceSnapshot empty() {
            return new ResourceSnapshot(0, 0, 0);
        }
    }

    private record CreatedLedger(int partition, RunLedgerHandleV1 handle, boolean retained) {}

    private record RunResult(
            long partitions,
            long ledgersCreated,
            long entriesAppended,
            long bytesAppended,
            long metadataMutations,
            long maximumOwnedHandles,
            String handleSequenceSha256,
            LatencySnapshot create,
            LatencySnapshot append,
            LatencySnapshot close,
            LatencySnapshot read,
            LatencySnapshot recovery,
            long elapsedNanos,
            ResourceSnapshot resources) {
        private RunResult withElapsed(long value, ResourceSnapshot snapshot) {
            return new RunResult(
                    partitions,
                    ledgersCreated,
                    entriesAppended,
                    bytesAppended,
                    metadataMutations,
                    maximumOwnedHandles,
                    handleSequenceSha256,
                    create,
                    append,
                    close,
                    read,
                    recovery,
                    value,
                    snapshot);
        }

        private void validate(Plan plan) {
            long minimumEntries =
                    plan.partitions + (long) plan.recoverySamples * (plan.tailEntries - 1L) + plan.rolloverSamples;
            if (partitions != plan.partitions
                    || ledgersCreated != plan.partitions + plan.rolloverSamples
                    || entriesAppended != minimumEntries
                    || maximumOwnedHandles != plan.hotLedgerAdmission
                    || metadataMutations > plan.maxMetadataMutationsPerPartition * ledgersCreated
                    || resources.maximumHeapBytes > plan.maxHarnessHeapBytes
                    || resources.maximumDirectBytes > plan.maxHarnessDirectBytes
                    || resources.maximumOpenFileDescriptors > plan.maxHarnessOpenFileDescriptors
                    || create.p99Nanos > plan.maxCreateP99Nanos
                    || append.p99Nanos > plan.maxAppendP99Nanos
                    || close.p99Nanos > plan.maxCloseP99Nanos
                    || read.p99Nanos > plan.maxReadP99Nanos
                    || recovery.p99Nanos > plan.maxRecoveryP99Nanos
                    || elapsedNanos > plan.maximumElapsedNanos) {
                throw new IllegalStateException("K9 scale result crossed a predeclared bound");
            }
            long operationsPerSecond = partitions * 1_000_000_000L / elapsedNanos;
            if (operationsPerSecond < plan.minimumPartitionOperationsPerSecond) {
                throw new IllegalStateException("K9 scale throughput crossed the predeclared lower bound");
            }
        }

        private String toJson(
                Plan plan,
                Conformance conformance,
                String testedSourceCommit,
                String planSha256,
                String conformanceSha256) {
            long operationsPerSecond = partitions * 1_000_000_000L / elapsedNanos;
            StringBuilder output = new StringBuilder(3_000);
            output.append("{\n");
            field(output, "schema", "NEREUS_V2_M2_KAFKA_K9_SCALE_RESULT_V1", true);
            field(output, "result", "PASS", true);
            field(output, "generatedAt", Instant.now().toString(), true);
            field(output, "testedSourceCommit", testedSourceCommit, true);
            field(output, "tierPartitions", partitions, true);
            field(output, "planSha256", planSha256, true);
            field(output, "conformanceConfigurationSha256", conformanceSha256, true);
            field(output, "metadataServiceUri", conformance.metadataServiceUri, true);
            output.append("  \"workload\": {\n");
            field(output, "ioConcurrency", plan.ioConcurrency, true, 4);
            field(output, "hotLedgerAdmission", plan.hotLedgerAdmission, true, 4);
            field(output, "payloadBytes", plan.payloadBytes, true, 4);
            field(output, "tailEntries", plan.tailEntries, true, 4);
            field(output, "readSamples", plan.readSamples, true, 4);
            field(output, "recoverySamples", plan.recoverySamples, true, 4);
            field(output, "rolloverSamples", plan.rolloverSamples, false, 4);
            output.append("  },\n");
            output.append("  \"counts\": {\n");
            field(output, "partitions", partitions, true, 4);
            field(output, "ledgersCreated", ledgersCreated, true, 4);
            field(output, "entriesAppended", entriesAppended, true, 4);
            field(output, "bytesAppended", bytesAppended, true, 4);
            field(output, "metadataMutations", metadataMutations, true, 4);
            field(output, "maximumOwnedHandles", maximumOwnedHandles, false, 4);
            output.append("  },\n");
            field(output, "handleSequenceSha256", handleSequenceSha256, true);
            field(output, "elapsedNanos", elapsedNanos, true);
            field(output, "partitionOperationsPerSecond", operationsPerSecond, true);
            output.append("  \"resources\": {\n");
            field(output, "maximumHeapBytes", resources.maximumHeapBytes, true, 4);
            field(output, "maximumDirectBytes", resources.maximumDirectBytes, true, 4);
            field(output, "maximumOpenFileDescriptors", resources.maximumOpenFileDescriptors, false, 4);
            output.append("  },\n");
            output.append("  \"latencies\": {\n");
            latency(output, "create", create, true);
            latency(output, "append", append, true);
            latency(output, "close", close, true);
            latency(output, "read", read, true);
            latency(output, "recovery", recovery, false);
            output.append("  }\n");
            output.append("}\n");
            return output.toString();
        }

        private static void latency(StringBuilder output, String name, LatencySnapshot latency, boolean comma) {
            output.append("    \"").append(name).append("\": {");
            output.append("\"samples\":").append(latency.samples);
            output.append(",\"p50Nanos\":").append(latency.p50Nanos);
            output.append(",\"p99Nanos\":").append(latency.p99Nanos);
            output.append(",\"maximumNanos\":").append(latency.maximumNanos).append('}');
            output.append(comma ? ",\n" : "\n");
        }

        private static void field(StringBuilder output, String name, String value, boolean comma) {
            output.append("  \"")
                    .append(name)
                    .append("\": \"")
                    .append(value.replace("\\", "\\\\").replace("\"", "\\\""))
                    .append('\"')
                    .append(comma ? ",\n" : "\n");
        }

        private static void field(StringBuilder output, String name, long value, boolean comma) {
            field(output, name, value, comma, 2);
        }

        private static void field(StringBuilder output, String name, long value, boolean comma, int indent) {
            output.append(" ".repeat(indent))
                    .append('\"')
                    .append(name)
                    .append("\": ")
                    .append(value)
                    .append(comma ? ",\n" : "\n");
        }
    }
}
