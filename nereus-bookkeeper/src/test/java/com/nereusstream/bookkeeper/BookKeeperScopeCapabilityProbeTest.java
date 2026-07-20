/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.BookKeeperMetadataStoreConfig;
import com.nereusstream.metadata.oxia.FakeBookKeeperMetadataStore;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteAdvHandle;
import org.junit.jupiter.api.Test;

final class BookKeeperScopeCapabilityProbeTest {
    private static final String CLUSTER = "cluster-a";
    private static final String DEPLOYMENT = "deployment-a";
    private static final byte[] PASSWORD = "bookkeeper-secret".getBytes(StandardCharsets.UTF_8);
    private static final Clock CLOCK = Clock.fixed(
            Instant.ofEpochMilli(10_000), ZoneOffset.UTC);

    @Test
    void provesRealCreateWriteReadFenceDeleteAndDualAbsence() {
        BookKeeperWalConfiguration configuration = BookKeeperTestConfigurations.valid();
        BookKeeperLedgerIdNamespaceReservation namespace = namespace(configuration);
        FakeOperations operations = new FakeOperations();
        try (FakeBookKeeperMetadataStore metadata = metadata(configuration)) {
            BookKeeperScopeCapabilityProbe probe = probe(
                    configuration, namespace, metadata, operations);

            BookKeeperScopeCapabilityProof proof = probe.probe(request()).join();

            assertThat(proof.canaryLedgerId()).isPositive();
            assertThat(configuration.ledgerIdNamespace().contains(proof.canaryLedgerId())).isTrue();
            assertThat(operations.createCalls).hasValue(1);
            assertThat(operations.writeCalls).hasValue(1);
            assertThat(operations.recoveryOpenCalls).hasValue(1);
            assertThat(operations.deleteCalls).hasValue(1);
            assertThat(operations.ledgers).isEmpty();
            assertThat(metadata.getRoot(
                                    CLUSTER,
                                    configuration.providerScopeSha256(),
                                    proof.canaryLedgerId())
                            .join())
                    .get()
                    .extracting(value -> value.value().lifecycle())
                    .isEqualTo(com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle.QUARANTINED);
        }
    }

    @Test
    void recoversAppliedCreateAndDeleteResponseLossWithoutTouchingForeignIdentity() {
        BookKeeperWalConfiguration configuration = BookKeeperTestConfigurations.valid();
        BookKeeperLedgerIdNamespaceReservation namespace = namespace(configuration);
        FakeOperations operations = new FakeOperations();
        operations.loseFirstCreateResponse = true;
        operations.loseFirstDeleteResponse = true;
        try (FakeBookKeeperMetadataStore metadata = metadata(configuration)) {
            BookKeeperScopeCapabilityProof proof = probe(
                            configuration, namespace, metadata, operations)
                    .probe(request())
                    .join();

            assertThat(proof.capabilitySha256().value()).hasSize(64);
            assertThat(operations.createCalls).hasValue(2);
            assertThat(operations.recoveryOpenCalls).hasValue(2);
            assertThat(operations.deleteCalls).hasValue(2);
            assertThat(operations.ledgers).isEmpty();
        }
    }

    private static BookKeeperScopeCapabilityProbe probe(
            BookKeeperWalConfiguration configuration,
            BookKeeperLedgerIdNamespaceReservation namespace,
            FakeBookKeeperMetadataStore metadata,
            FakeOperations operations) {
        BookKeeperLedgerIdNamespaceReservationVerifier verifier =
                new BookKeeperLedgerIdNamespaceReservationVerifier(
                        (scope, bits, prefix, timeout) ->
                                CompletableFuture.completedFuture(Optional.of(namespace)),
                        DEPLOYMENT);
        return new BookKeeperScopeCapabilityProbe(
                CLUSTER,
                configuration,
                namespace,
                verifier,
                metadata,
                operations,
                ignored -> PASSWORD.clone(),
                CLOCK,
                new Random(17));
    }

    private static BookKeeperScopeCapabilityRequest request() {
        return new BookKeeperScopeCapabilityRequest(
                "scope_run_0001",
                new BookKeeperBrokerReadiness(
                        7,
                        new Checksum(ChecksumType.SHA256, "88".repeat(32)),
                        2),
                Duration.ofSeconds(10));
    }

    private static BookKeeperLedgerIdNamespaceReservation namespace(
            BookKeeperWalConfiguration configuration) {
        return new BookKeeperLedgerIdNamespaceReservation(
                1,
                configuration.ledgerIdNamespaceReservationId(),
                DEPLOYMENT,
                configuration.clusterAlias(),
                configuration.providerScopeSha256(),
                configuration.ledgerIdPrefixBits(),
                configuration.ledgerIdPrefixValue(),
                BookKeeperLedgerIdNamespaceReservation.Lifecycle.ACTIVE,
                1,
                1,
                0,
                "66".repeat(32),
                3,
                new Checksum(ChecksumType.SHA256, "77".repeat(32)),
                "/bookkeeper/scope-probe-reservation");
    }

    private static FakeBookKeeperMetadataStore metadata(
            BookKeeperWalConfiguration configuration) {
        return new FakeBookKeeperMetadataStore(new BookKeeperMetadataStoreConfig(
                configuration.maxAppendRangesPerLedger(),
                configuration.protectionSlotsPerRange(),
                configuration.maxReaderLeasesPerLedger(),
                configuration.maxUncertainAllocations()));
    }

    private static final class FakeOperations implements BookKeeperClientOperations {
        private final Map<Long, LedgerState> ledgers = new java.util.HashMap<>();
        private final AtomicInteger createCalls = new AtomicInteger();
        private final AtomicInteger writeCalls = new AtomicInteger();
        private final AtomicInteger recoveryOpenCalls = new AtomicInteger();
        private final AtomicInteger deleteCalls = new AtomicInteger();
        private boolean loseFirstCreateResponse;
        private boolean loseFirstDeleteResponse;

        @Override
        public CompletableFuture<WriteAdvHandle> createAdvanced(
                long ledgerId,
                BookKeeperWalConfiguration configuration,
                byte[] password,
                Map<String, byte[]> customMetadata,
                BookKeeperOperationDeadline deadline) {
            createCalls.incrementAndGet();
            LedgerState state = new LedgerState(configuration, customMetadata);
            if (ledgers.putIfAbsent(ledgerId, state) != null) {
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.PRIMARY_WAL_WRITE_FAILED, false, "ledger exists"));
            }
            if (loseFirstCreateResponse) {
                loseFirstCreateResponse = false;
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.PRIMARY_WAL_WRITE_FAILED,
                        true,
                        "injected applied create response loss"));
            }
            return CompletableFuture.completedFuture(writeHandle(ledgerId, state));
        }

        @Override
        public CompletableFuture<ReadHandle> open(
                long ledgerId,
                BookKeeperDigestType digestType,
                byte[] password,
                boolean recovery,
                BookKeeperOperationDeadline deadline) {
            LedgerState state = ledgers.get(ledgerId);
            if (state == null) {
                return absent();
            }
            if (recovery) {
                recoveryOpenCalls.incrementAndGet();
                state.closed = true;
            }
            return CompletableFuture.completedFuture(readHandle(ledgerId, state));
        }

        @Override
        public CompletableFuture<Long> write(
                WriteAdvHandle handle,
                long entryId,
                ByteBuf entry,
                BookKeeperOperationDeadline deadline) {
            LedgerState state = ledgers.get(handle.getId());
            if (state == null) {
                return absent();
            }
            byte[] bytes = new byte[entry.readableBytes()];
            entry.getBytes(entry.readerIndex(), bytes);
            state.entries.put(entryId, bytes);
            writeCalls.incrementAndGet();
            return CompletableFuture.completedFuture(entryId);
        }

        @Override
        public CompletableFuture<LedgerEntries> readUnconfirmed(
                ReadHandle handle,
                long firstEntryId,
                long lastEntryIdInclusive,
                BookKeeperOperationDeadline deadline) {
            LedgerState state = ledgers.get(handle.getId());
            if (state == null) {
                return absent();
            }
            List<LedgerEntry> values = state.entries
                    .subMap(firstEntryId, true, lastEntryIdInclusive, true)
                    .entrySet()
                    .stream()
                    .map(entry -> ledgerEntry(
                            handle.getId(), entry.getKey(), entry.getValue()))
                    .toList();
            return CompletableFuture.completedFuture(ledgerEntries(values));
        }

        @Override
        public CompletableFuture<LedgerMetadata> metadata(
                long ledgerId, BookKeeperOperationDeadline deadline) {
            LedgerState state = ledgers.get(ledgerId);
            return state == null
                    ? absent()
                    : CompletableFuture.completedFuture(ledgerMetadata(ledgerId, state));
        }

        @Override
        public CompletableFuture<Void> delete(
                long ledgerId, BookKeeperOperationDeadline deadline) {
            deleteCalls.incrementAndGet();
            ledgers.remove(ledgerId);
            if (loseFirstDeleteResponse) {
                loseFirstDeleteResponse = false;
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.PRIMARY_WAL_WRITE_FAILED,
                        true,
                        "injected applied delete response loss"));
            }
            return CompletableFuture.completedFuture(null);
        }

        private static <T> CompletableFuture<T> absent() {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.PRIMARY_WAL_TARGET_NOT_FOUND,
                    false,
                    "ledger is absent"));
        }
    }

    private static WriteAdvHandle writeHandle(long ledgerId, LedgerState state) {
        return (WriteAdvHandle) Proxy.newProxyInstance(
                WriteAdvHandle.class.getClassLoader(),
                new Class<?>[] {WriteAdvHandle.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getId" -> ledgerId;
                    case "getLedgerMetadata" -> ledgerMetadata(ledgerId, state);
                    case "closeAsync" -> {
                        state.closed = true;
                        yield CompletableFuture.completedFuture(null);
                    }
                    case "force" -> CompletableFuture.completedFuture(null);
                    case "getLastAddConfirmed", "getLastAddPushed" ->
                            state.entries.isEmpty() ? -1L : state.entries.lastKey();
                    case "getLength" -> state.length();
                    case "isClosed" -> state.closed;
                    case "toString" -> "scope-write-handle-" + ledgerId;
                    default -> defaultValue(method.getReturnType());
                });
    }

    private static ReadHandle readHandle(long ledgerId, LedgerState state) {
        return (ReadHandle) Proxy.newProxyInstance(
                ReadHandle.class.getClassLoader(),
                new Class<?>[] {ReadHandle.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getId" -> ledgerId;
                    case "getLedgerMetadata" -> ledgerMetadata(ledgerId, state);
                    case "closeAsync" -> CompletableFuture.completedFuture(null);
                    case "getLastAddConfirmed" ->
                            state.entries.isEmpty() ? -1L : state.entries.lastKey();
                    case "getLength" -> state.length();
                    case "isClosed" -> state.closed;
                    case "toString" -> "scope-read-handle-" + ledgerId;
                    default -> defaultValue(method.getReturnType());
                });
    }

    private static LedgerMetadata ledgerMetadata(long ledgerId, LedgerState state) {
        return (LedgerMetadata) Proxy.newProxyInstance(
                LedgerMetadata.class.getClassLoader(),
                new Class<?>[] {LedgerMetadata.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getLedgerId" -> ledgerId;
                    case "getEnsembleSize" -> state.configuration.ensembleSize();
                    case "getWriteQuorumSize" -> state.configuration.writeQuorumSize();
                    case "getAckQuorumSize" -> state.configuration.ackQuorumSize();
                    case "getDigestType" -> state.configuration.digestType().toClientType();
                    case "getCustomMetadata" -> state.customMetadata;
                    case "getLastEntryId" ->
                            state.entries.isEmpty() ? -1L : state.entries.lastKey();
                    case "getLength" -> state.length();
                    case "getCtime", "getCToken" -> 0L;
                    case "isClosed" -> state.closed;
                    case "hasPassword" -> false;
                    case "getPassword" -> new byte[0];
                    case "toSafeString", "toString" -> "scope-ledger-" + ledgerId;
                    case "getMetadataFormatVersion" -> 1;
                    default -> defaultValue(method.getReturnType());
                });
    }

    private static LedgerEntry ledgerEntry(
            long ledgerId, long entryId, byte[] value) {
        ByteBuf buffer = Unpooled.wrappedBuffer(value.clone());
        return (LedgerEntry) Proxy.newProxyInstance(
                LedgerEntry.class.getClassLoader(),
                new Class<?>[] {LedgerEntry.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getLedgerId" -> ledgerId;
                    case "getEntryId" -> entryId;
                    case "getLength" -> (long) value.length;
                    case "getEntryBuffer" -> buffer;
                    case "getEntryBytes" -> value.clone();
                    case "close" -> {
                        if (buffer.refCnt() > 0) {
                            buffer.release();
                        }
                        yield null;
                    }
                    case "toString" -> "scope-entry-" + ledgerId + "-" + entryId;
                    default -> defaultValue(method.getReturnType());
                });
    }

    private static LedgerEntries ledgerEntries(List<LedgerEntry> values) {
        return (LedgerEntries) Proxy.newProxyInstance(
                LedgerEntries.class.getClassLoader(),
                new Class<?>[] {LedgerEntries.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "iterator" -> values.iterator();
                    case "getEntry" -> values.get((int) args[0]);
                    case "size" -> values.size();
                    case "close" -> {
                        values.forEach(LedgerEntry::close);
                        yield null;
                    }
                    case "toString" -> "scope-ledger-entries";
                    default -> defaultValue(method.getReturnType());
                });
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == byte.class) {
            return (byte) 0;
        }
        if (type == short.class) {
            return (short) 0;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == float.class) {
            return 0F;
        }
        if (type == double.class) {
            return 0D;
        }
        if (type == char.class) {
            return '\0';
        }
        throw new IllegalArgumentException("unsupported primitive type");
    }

    private static final class LedgerState {
        private final BookKeeperWalConfiguration configuration;
        private final Map<String, byte[]> customMetadata;
        private final TreeMap<Long, byte[]> entries = new TreeMap<>();
        private boolean closed;

        private LedgerState(
                BookKeeperWalConfiguration configuration,
                Map<String, byte[]> customMetadata) {
            this.configuration = configuration;
            this.customMetadata = customMetadata.entrySet().stream().collect(
                    java.util.stream.Collectors.toUnmodifiableMap(
                            Map.Entry::getKey,
                            entry -> entry.getValue().clone()));
        }

        private long length() {
            return entries.values().stream().mapToLong(value -> value.length).sum();
        }
    }
}
