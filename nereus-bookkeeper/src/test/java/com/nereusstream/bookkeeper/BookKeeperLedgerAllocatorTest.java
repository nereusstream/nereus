/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.AppendSession;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.BookKeeperMetadataStoreConfig;
import com.nereusstream.metadata.oxia.BookKeeperScanToken;
import com.nereusstream.metadata.oxia.FakeBookKeeperMetadataStore;
import com.nereusstream.metadata.oxia.records.AllocationSlotLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperWriterLifecycle;
import com.nereusstream.metadata.oxia.records.LedgerAllocationLifecycle;
import io.netty.buffer.ByteBuf;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteAdvHandle;
import org.junit.jupiter.api.Test;

class BookKeeperLedgerAllocatorTest {
    private static final String CLUSTER = "cluster/bk-allocator";
    private static final String DEPLOYMENT = "deployment-1";
    private static final StreamId STREAM = new StreamId("stream-bk-allocator");
    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC);
    private static final String ALLOCATION_ID = "abcdefghijklmnopqrstuvwxzy";

    @Test
    void activatesExactAdvancedLedgerOnlyAfterAllDurableStages() {
        BookKeeperWalConfiguration configuration = BookKeeperTestConfigurations.valid();
        BookKeeperMetadataStoreConfig metadataConfig = metadataConfig(configuration);
        try (FakeBookKeeperMetadataStore metadata = new FakeBookKeeperMetadataStore(metadataConfig, CLOCK)) {
            FakeOperations operations = new FakeOperations(false);
            BookKeeperLedgerAllocator allocator = allocator(configuration, metadata, operations, new Random(7));
            AppendSession session = session(1, 1, "token-1");

            AllocatedBookKeeperLedger result = allocator.allocate(
                    new BookKeeperLedgerAllocationRequest(STREAM, session, Duration.ofSeconds(10))).join();

            assertThat(result.handle().getId()).isEqualTo(result.root().value().ledgerId());
            assertThat(result.root().value().lifecycle()).isEqualTo(BookKeeperLedgerLifecycle.ACTIVE);
            assertThat(result.root().value().lateCreateHazard()).isFalse();
            assertThat(result.allocation().value().lifecycle()).isEqualTo(LedgerAllocationLifecycle.ACTIVATED);
            assertThat(result.writer().value().lifecycle()).isEqualTo(BookKeeperWriterLifecycle.ACTIVE);
            assertThat(result.writer().value().activeLedgerId()).isEqualTo(result.root().value().ledgerId());
            assertThat(metadata.getAllocationSlot(
                    CLUSTER, result.root().value().allocationSlot()).join()).isEmpty();
            assertThat(operations.createCalls).isOne();
            assertThat(operations.createdCustomMetadata).containsKey("nereus.format")
                    .doesNotContainKey("password");
            assertThat(new String(operations.createdCustomMetadata.get("nereus.format"), StandardCharsets.UTF_8))
                    .isEqualTo(BookKeeperLedgerCustomMetadata.FORMAT);
        }
    }

    @Test
    void unknownCreatePersistsHazardAndDetachesWriterWithoutPretendingToRecoverWriteHandle() {
        BookKeeperWalConfiguration configuration = BookKeeperTestConfigurations.valid();
        BookKeeperMetadataStoreConfig metadataConfig = metadataConfig(configuration);
        try (FakeBookKeeperMetadataStore metadata = new FakeBookKeeperMetadataStore(metadataConfig, CLOCK)) {
            FakeOperations operations = new FakeOperations(true);
            BookKeeperLedgerAllocator allocator = allocator(configuration, metadata, operations, new Random(11));
            AppendSession session = session(1, 1, "token-1");

            assertThatThrownBy(() -> allocator.allocate(
                            new BookKeeperLedgerAllocationRequest(
                                    STREAM, session, Duration.ofSeconds(10))).join())
                    .cause().isInstanceOf(NereusException.class)
                    .extracting(error -> ((NereusException) error).code())
                    .isEqualTo(ErrorCode.PRIMARY_WAL_WRITE_FAILED);

            var allocations = metadata.scanAllocations(
                    CLUSTER, STREAM, Optional.<BookKeeperScanToken>empty(), 10).join().values();
            assertThat(allocations).singleElement().satisfies(value -> {
                assertThat(value.value().lifecycle()).isEqualTo(LedgerAllocationLifecycle.CREATE_UNCERTAIN);
                assertThat(value.value().lateCreateHazard()).isTrue();
                assertThat(metadata.getAllocationSlot(CLUSTER, value.value().allocationSlot()).join())
                        .get().extracting(slot -> slot.value().lifecycle())
                        .isEqualTo(AllocationSlotLifecycle.CREATE_UNCERTAIN);
                assertThat(metadata.getRoot(
                        CLUSTER, configuration.providerScopeSha256(), value.value().candidateLedgerId()).join())
                        .get().satisfies(root -> {
                            assertThat(root.value().lifecycle()).isEqualTo(BookKeeperLedgerLifecycle.ALLOCATING);
                            assertThat(root.value().lateCreateHazard()).isTrue();
                        });
            });
            assertThat(metadata.getWriter(CLUSTER, STREAM).join())
                    .get().extracting(value -> value.value().lifecycle())
                    .isEqualTo(BookKeeperWriterLifecycle.IDLE);
        }
    }

    @Test
    void writerStateRejectsStaleSessionBeforeAnyProviderCreate() {
        BookKeeperWalConfiguration configuration = BookKeeperTestConfigurations.valid();
        try (FakeBookKeeperMetadataStore metadata = new FakeBookKeeperMetadataStore(
                metadataConfig(configuration), CLOCK)) {
            FakeOperations operations = new FakeOperations(false);
            BookKeeperWriterStateMachine state = writerState(configuration, metadata);
            state.requireIdle(session(2, 3, "token-new")).join();
            BookKeeperLedgerAllocator allocator = allocator(
                    configuration, metadata, operations, new Random(13), state);

            assertThatThrownBy(() -> allocator.allocate(new BookKeeperLedgerAllocationRequest(
                            STREAM, session(1, 2, "token-old"), Duration.ofSeconds(10))).join())
                    .hasRootCauseInstanceOf(NereusException.class)
                    .rootCause()
                    .extracting(error -> ((NereusException) error).code())
                    .isEqualTo(ErrorCode.FENCED_APPEND);
            assertThat(operations.createCalls).isZero();
        }
    }

    @Test
    void customMetadataDigestRejectsAnyProviderIdentityDrift() {
        BookKeeperWalConfiguration configuration = BookKeeperTestConfigurations.valid();
        BookKeeperLedgerCustomMetadata expected = BookKeeperLedgerCustomMetadata.create(
                CLUSTER, configuration, reservation(configuration), STREAM, 1, ALLOCATION_ID);
        long ledgerId = configuration.ledgerIdNamespace().candidate(new Random(17));
        LedgerMetadata exact = ledgerMetadata(ledgerId, configuration, expected.values());
        assertThat(expected.requireExactImmutableLedgerMetadata(ledgerId, configuration, exact).type())
                .isEqualTo(ChecksumType.SHA256);

        Map<String, byte[]> drifted = new java.util.HashMap<>(expected.values());
        drifted.put("nereus.allocation-id", "foreign".getBytes(StandardCharsets.UTF_8));
        assertThatThrownBy(() -> expected.requireExactImmutableLedgerMetadata(
                ledgerId, configuration, ledgerMetadata(ledgerId, configuration, drifted)))
                .isInstanceOf(NereusException.class)
                .extracting(error -> ((NereusException) error).code())
                .isEqualTo(ErrorCode.METADATA_INVARIANT_VIOLATION);
    }

    @Test
    void newOwnerRecoveryOpenFencesAndSealsBeforeReturningWriterToIdle() {
        BookKeeperWalConfiguration configuration = BookKeeperTestConfigurations.valid();
        try (FakeBookKeeperMetadataStore metadata = new FakeBookKeeperMetadataStore(
                metadataConfig(configuration), CLOCK)) {
            FakeOperations operations = new FakeOperations(false);
            BookKeeperWriterStateMachine state = writerState(configuration, metadata);
            BookKeeperLedgerAllocator allocator = allocator(
                    configuration, metadata, operations, new Random(19), state);
            allocator.allocate(new BookKeeperLedgerAllocationRequest(
                    STREAM, session(1, 1, "token-1"), Duration.ofSeconds(10))).join();
            BookKeeperLedgerRecovery recovery = new BookKeeperLedgerRecovery(
                    CLUSTER, configuration, metadata, metadata, namespaceVerifier(configuration), operations,
                    ignored -> "secret".getBytes(StandardCharsets.UTF_8), state, CLOCK);

            BookKeeperLedgerRecoveryResult result = recovery.recoverWriter(
                    session(2, 2, "token-2"), Duration.ofSeconds(10), "ownership transfer").join();

            assertThat(result.writer().value().lifecycle()).isEqualTo(BookKeeperWriterLifecycle.IDLE);
            assertThat(result.sealedRoot().value().lifecycle()).isEqualTo(BookKeeperLedgerLifecycle.SEALED);
            assertThat(result.sealedRoot().value().sealedLastEntryId()).isEqualTo(-1);
            assertThat(result.sealedRoot().value().sealedLength()).isZero();
            assertThat(operations.recoveryOpenCalls).isOne();
        }
    }

    private static BookKeeperLedgerAllocator allocator(
            BookKeeperWalConfiguration configuration,
            FakeBookKeeperMetadataStore metadata,
            FakeOperations operations,
            Random random) {
        return allocator(configuration, metadata, operations, random, writerState(configuration, metadata));
    }

    private static BookKeeperLedgerAllocator allocator(
            BookKeeperWalConfiguration configuration,
            FakeBookKeeperMetadataStore metadata,
            FakeOperations operations,
            Random random,
            BookKeeperWriterStateMachine state) {
        return new BookKeeperLedgerAllocator(CLUSTER, configuration, metadata, metadata,
                namespaceVerifier(configuration), operations, ignored -> "secret".getBytes(StandardCharsets.UTF_8),
                state, CLOCK, random, () -> ALLOCATION_ID);
    }

    private static BookKeeperWriterStateMachine writerState(
            BookKeeperWalConfiguration configuration, FakeBookKeeperMetadataStore metadata) {
        return new BookKeeperWriterStateMachine(CLUSTER, configuration, metadata, CLOCK, "process-run-1");
    }

    private static BookKeeperLedgerIdNamespaceReservationVerifier namespaceVerifier(
            BookKeeperWalConfiguration configuration) {
        BookKeeperLedgerIdNamespaceReservation value = reservation(configuration);
        return new BookKeeperLedgerIdNamespaceReservationVerifier(
                (scope, bits, prefix, timeout) -> CompletableFuture.completedFuture(Optional.of(value)), DEPLOYMENT);
    }

    private static BookKeeperLedgerIdNamespaceReservation reservation(
            BookKeeperWalConfiguration configuration) {
        return new BookKeeperLedgerIdNamespaceReservation(1,
                configuration.ledgerIdNamespaceReservationId(), DEPLOYMENT, configuration.clusterAlias(),
                configuration.providerScopeSha256(), configuration.ledgerIdPrefixBits(),
                configuration.ledgerIdPrefixValue(), BookKeeperLedgerIdNamespaceReservation.Lifecycle.ACTIVE,
                1, 100, 0, "a".repeat(64), 1,
                new Checksum(ChecksumType.SHA256, "b".repeat(64)), "/bookkeeper/reservation");
    }

    private static BookKeeperMetadataStoreConfig metadataConfig(BookKeeperWalConfiguration configuration) {
        return new BookKeeperMetadataStoreConfig(configuration.maxAppendRangesPerLedger(),
                configuration.protectionSlotsPerRange(), configuration.maxReaderLeasesPerLedger(),
                configuration.maxUncertainAllocations());
    }

    private static AppendSession session(long epoch, long leaseVersion, String token) {
        return new AppendSession(STREAM, "writer-1", epoch, token, leaseVersion, 10_000);
    }

    private static LedgerMetadata ledgerMetadata(
            long ledgerId, BookKeeperWalConfiguration configuration, Map<String, byte[]> customMetadata) {
        return ledgerMetadata(ledgerId, configuration, customMetadata, false);
    }

    private static LedgerMetadata ledgerMetadata(
            long ledgerId,
            BookKeeperWalConfiguration configuration,
            Map<String, byte[]> customMetadata,
            boolean closed) {
        Map<String, byte[]> copied = customMetadata.entrySet().stream().collect(
                java.util.stream.Collectors.toUnmodifiableMap(
                        Map.Entry::getKey, entry -> entry.getValue().clone()));
        return (LedgerMetadata) Proxy.newProxyInstance(
                LedgerMetadata.class.getClassLoader(), new Class<?>[] {LedgerMetadata.class}, (proxy, method, args) ->
                        switch (method.getName()) {
                            case "getLedgerId" -> ledgerId;
                            case "getEnsembleSize" -> configuration.ensembleSize();
                            case "getWriteQuorumSize" -> configuration.writeQuorumSize();
                            case "getAckQuorumSize" -> configuration.ackQuorumSize();
                            case "getDigestType" -> configuration.digestType().toClientType();
                            case "getCustomMetadata" -> copied;
                            case "getLastEntryId" -> -1L;
                            case "getLength", "getCtime", "getCToken" -> 0L;
                            case "isClosed" -> closed;
                            case "hasPassword" -> false;
                            case "getPassword" -> new byte[0];
                            case "toSafeString", "toString" -> "fake-ledger-" + ledgerId;
                            case "getMetadataFormatVersion" -> 1;
                            default -> defaultValue(method.getReturnType());
                        });
    }

    private static WriteAdvHandle writeHandle(long ledgerId, LedgerMetadata metadata) {
        return (WriteAdvHandle) Proxy.newProxyInstance(
                WriteAdvHandle.class.getClassLoader(), new Class<?>[] {WriteAdvHandle.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getId" -> ledgerId;
                    case "getLedgerMetadata" -> metadata;
                    case "closeAsync", "force" -> CompletableFuture.completedFuture(null);
                    case "getLastAddConfirmed", "getLength", "getLastAddPushed" -> -1L;
                    case "isClosed" -> false;
                    case "toString" -> "fake-write-handle-" + ledgerId;
                    default -> defaultValue(method.getReturnType());
                });
    }

    private static ReadHandle readHandle(long ledgerId, LedgerMetadata metadata) {
        return (ReadHandle) Proxy.newProxyInstance(
                ReadHandle.class.getClassLoader(), new Class<?>[] {ReadHandle.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getId" -> ledgerId;
                    case "getLedgerMetadata" -> metadata;
                    case "closeAsync" -> CompletableFuture.completedFuture(null);
                    case "getLastAddConfirmed", "getLength" -> -1L;
                    case "isClosed" -> false;
                    case "toString" -> "fake-read-handle-" + ledgerId;
                    default -> defaultValue(method.getReturnType());
                });
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) return null;
        if (type == boolean.class) return false;
        if (type == byte.class) return (byte) 0;
        if (type == short.class) return (short) 0;
        if (type == int.class) return 0;
        if (type == long.class) return 0L;
        if (type == float.class) return 0F;
        if (type == double.class) return 0D;
        if (type == char.class) return '\0';
        throw new IllegalArgumentException("unsupported primitive type");
    }

    private static final class FakeOperations implements BookKeeperClientOperations {
        private final boolean failCreate;
        private int createCalls;
        private int recoveryOpenCalls;
        private Map<String, byte[]> createdCustomMetadata = Map.of();
        private BookKeeperWalConfiguration createdConfiguration;
        private long createdLedgerId;

        private FakeOperations(boolean failCreate) {
            this.failCreate = failCreate;
        }

        @Override
        public CompletableFuture<WriteAdvHandle> createAdvanced(
                long ledgerId, BookKeeperWalConfiguration configuration, byte[] password,
                Map<String, byte[]> customMetadata, BookKeeperOperationDeadline deadline) {
            createCalls++;
            createdLedgerId = ledgerId;
            createdConfiguration = configuration;
            createdCustomMetadata = customMetadata.entrySet().stream().collect(
                    java.util.stream.Collectors.toUnmodifiableMap(
                            Map.Entry::getKey, entry -> entry.getValue().clone()));
            if (failCreate) {
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.TIMEOUT, true, "injected unknown create outcome"));
            }
            LedgerMetadata metadata = ledgerMetadata(ledgerId, configuration, customMetadata);
            return CompletableFuture.completedFuture(writeHandle(ledgerId, metadata));
        }

        @Override
        public CompletableFuture<ReadHandle> open(
                long ledgerId, BookKeeperDigestType digestType, byte[] password,
                boolean recovery, BookKeeperOperationDeadline deadline) {
            if (!recovery || ledgerId != createdLedgerId || createdConfiguration == null) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }
            recoveryOpenCalls++;
            LedgerMetadata metadata = ledgerMetadata(
                    ledgerId, createdConfiguration, createdCustomMetadata, true);
            return CompletableFuture.completedFuture(readHandle(ledgerId, metadata));
        }

        @Override
        public CompletableFuture<Long> write(
                WriteAdvHandle handle, long entryId, ByteBuf entry, BookKeeperOperationDeadline deadline) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public CompletableFuture<LedgerEntries> readUnconfirmed(
                ReadHandle handle, long firstEntryId, long lastEntryIdInclusive,
                BookKeeperOperationDeadline deadline) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public CompletableFuture<LedgerMetadata> metadata(long ledgerId, BookKeeperOperationDeadline deadline) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.PRIMARY_WAL_TARGET_NOT_FOUND, false, "injected absent ledger"));
        }

        @Override
        public CompletableFuture<Void> delete(long ledgerId, BookKeeperOperationDeadline deadline) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }
    }
}
