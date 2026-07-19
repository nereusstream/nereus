/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.bookkeeper.BookKeeperClientOperations;
import com.nereusstream.bookkeeper.BookKeeperDigestType;
import com.nereusstream.bookkeeper.BookKeeperLedgerAllocator;
import com.nereusstream.bookkeeper.BookKeeperLedgerHandleCache;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservation;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservationVerifier;
import com.nereusstream.bookkeeper.BookKeeperLedgerRecovery;
import com.nereusstream.bookkeeper.BookKeeperOperationDeadline;
import com.nereusstream.bookkeeper.BookKeeperPrimaryPhysicalReferenceAdapter;
import com.nereusstream.bookkeeper.BookKeeperPrimaryWalAppender;
import com.nereusstream.bookkeeper.BookKeeperPrimaryWalReader;
import com.nereusstream.bookkeeper.BookKeeperReaderLeaseManager;
import com.nereusstream.bookkeeper.BookKeeperSecretRef;
import com.nereusstream.bookkeeper.BookKeeperWalConfiguration;
import com.nereusstream.bookkeeper.BookKeeperWriterStateMachine;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteAdvHandle;

/** Provider-neutral in-memory BookKeeper client fixture owned by the broker composition tests. */
final class InMemoryBookKeeperPrimaryWalFixture implements AutoCloseable {
    static final String CLUSTER = "cluster/bk-managed-ledger";
    static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC);
    private static final String DEPLOYMENT = "deployment-1";

    final BookKeeperWalConfiguration configuration = configuration();
    final FakeBookKeeperMetadataStore metadata = new FakeBookKeeperMetadataStore(
            new BookKeeperMetadataStoreConfig(
                    configuration.maxAppendRangesPerLedger(),
                    configuration.protectionSlotsPerRange(),
                    configuration.maxReaderLeasesPerLedger(),
                    configuration.maxUncertainAllocations()),
            CLOCK);
    final FakeOperations operations = new FakeOperations();
    private final BookKeeperWriterStateMachine writerState = new BookKeeperWriterStateMachine(
            CLUSTER, configuration, metadata, CLOCK, "process-run-1");
    private final BookKeeperLedgerIdNamespaceReservationVerifier verifier =
            new BookKeeperLedgerIdNamespaceReservationVerifier(
                    (scope, bits, prefix, timeout) -> CompletableFuture.completedFuture(
                            Optional.of(reservation(configuration))),
                    DEPLOYMENT);
    private final BookKeeperLedgerAllocator allocator = new BookKeeperLedgerAllocator(
            CLUSTER,
            configuration,
            metadata,
            metadata,
            verifier,
            operations,
            ignored -> "secret".getBytes(StandardCharsets.UTF_8),
            writerState,
            CLOCK);
    private final BookKeeperLedgerRecovery recovery = new BookKeeperLedgerRecovery(
            CLUSTER,
            configuration,
            metadata,
            metadata,
            verifier,
            operations,
            ignored -> "secret".getBytes(StandardCharsets.UTF_8),
            writerState,
            CLOCK);
    final BookKeeperPrimaryWalAppender appender = new BookKeeperPrimaryWalAppender(
            CLUSTER,
            configuration,
            metadata,
            metadata,
            allocator,
            recovery,
            writerState,
            operations,
            CLOCK);
    final BookKeeperPrimaryPhysicalReferenceAdapter references =
            new BookKeeperPrimaryPhysicalReferenceAdapter(
                    CLUSTER, configuration, metadata, metadata, CLOCK);
    private final BookKeeperLedgerHandleCache handles =
            new BookKeeperLedgerHandleCache(8, 8 * 1024, 1024, Duration.ofMinutes(1));
    private final BookKeeperReaderLeaseManager readerLeases = new BookKeeperReaderLeaseManager(
            CLUSTER, configuration, metadata, CLOCK, "reader-process-1");
    final BookKeeperPrimaryWalReader reader = new BookKeeperPrimaryWalReader(
            CLUSTER,
            configuration,
            metadata,
            operations,
            ignored -> "secret".getBytes(StandardCharsets.UTF_8),
            handles,
            readerLeases);

    @Override
    public void close() {
        reader.close();
        appender.close();
        metadata.close();
    }

    private static BookKeeperWalConfiguration configuration() {
        return new BookKeeperWalConfiguration(
                "primary",
                "11".repeat(32),
                12,
                0x801,
                "reservation-1",
                3,
                3,
                2,
                BookKeeperDigestType.CRC32C,
                new BookKeeperSecretRef("secret://bookkeeper/password", "v7"),
                2,
                256L * 1024 * 1024,
                1_000,
                8,
                64,
                32,
                Duration.ofHours(1),
                1,
                8,
                64L * 1024 * 1024,
                Duration.ofSeconds(30),
                Duration.ofSeconds(20),
                Duration.ofSeconds(30),
                Duration.ofSeconds(30),
                Duration.ofMinutes(2),
                Duration.ofSeconds(30),
                Duration.ofMinutes(1),
                256);
    }

    private static BookKeeperLedgerIdNamespaceReservation reservation(
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
                100,
                0,
                "a".repeat(64),
                1,
                new Checksum(ChecksumType.SHA256, "b".repeat(64)),
                "/bookkeeper/reservation");
    }

    static final class FakeOperations implements BookKeeperClientOperations {
        private final Map<Long, LedgerState> ledgers = new LinkedHashMap<>();
        int recoveryOpenCalls;
        int normalOpenCalls;

        @Override
        public CompletableFuture<WriteAdvHandle> createAdvanced(
                long ledgerId,
                BookKeeperWalConfiguration configuration,
                byte[] password,
                Map<String, byte[]> customMetadata,
                BookKeeperOperationDeadline deadline) {
            LedgerState state = new LedgerState(configuration, customMetadata);
            ledgers.put(ledgerId, state);
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
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.PRIMARY_WAL_TARGET_NOT_FOUND, false, "ledger is absent"));
            }
            if (recovery) {
                recoveryOpenCalls++;
                state.closed = true;
            } else {
                normalOpenCalls++;
            }
            return CompletableFuture.completedFuture(readHandle(ledgerId, state));
        }

        @Override
        public CompletableFuture<Long> write(
                WriteAdvHandle handle,
                long entryId,
                ByteBuf entry,
                BookKeeperOperationDeadline deadline) {
            byte[] bytes = new byte[entry.readableBytes()];
            entry.getBytes(entry.readerIndex(), bytes);
            ledgers.get(handle.getId()).entries.put(entryId, bytes);
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
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.PRIMARY_WAL_TARGET_NOT_FOUND, false, "ledger is absent"));
            }
            List<LedgerEntry> entries = state.entries
                    .subMap(firstEntryId, true, lastEntryIdInclusive, true)
                    .entrySet()
                    .stream()
                    .map(entry -> ledgerEntry(handle.getId(), entry.getKey(), entry.getValue()))
                    .toList();
            return CompletableFuture.completedFuture(ledgerEntries(entries));
        }

        @Override
        public CompletableFuture<LedgerMetadata> metadata(
                long ledgerId, BookKeeperOperationDeadline deadline) {
            LedgerState state = ledgers.get(ledgerId);
            return state == null
                    ? CompletableFuture.failedFuture(new NereusException(
                            ErrorCode.PRIMARY_WAL_TARGET_NOT_FOUND, false, "ledger is absent"))
                    : CompletableFuture.completedFuture(ledgerMetadata(ledgerId, state));
        }

        @Override
        public CompletableFuture<Void> delete(
                long ledgerId, BookKeeperOperationDeadline deadline) {
            ledgers.remove(ledgerId);
            return CompletableFuture.completedFuture(null);
        }
    }

    private static LedgerEntry ledgerEntry(long ledgerId, long entryId, byte[] value) {
        ByteBuf buffer = Unpooled.wrappedBuffer(value.clone());
        return (LedgerEntry) Proxy.newProxyInstance(
                LedgerEntry.class.getClassLoader(),
                new Class<?>[] {LedgerEntry.class},
                (proxy, method, arguments) -> switch (method.getName()) {
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
                    case "toString" -> "fake-entry-" + ledgerId + "-" + entryId;
                    default -> defaultValue(method.getReturnType());
                });
    }

    private static LedgerEntries ledgerEntries(List<LedgerEntry> values) {
        return (LedgerEntries) Proxy.newProxyInstance(
                LedgerEntries.class.getClassLoader(),
                new Class<?>[] {LedgerEntries.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "iterator" -> values.iterator();
                    case "getEntry" -> values.get((int) arguments[0]);
                    case "size" -> values.size();
                    case "close" -> {
                        values.forEach(LedgerEntry::close);
                        yield null;
                    }
                    case "toString" -> "fake-ledger-entries";
                    default -> defaultValue(method.getReturnType());
                });
    }

    private static LedgerMetadata ledgerMetadata(long ledgerId, LedgerState state) {
        return (LedgerMetadata) Proxy.newProxyInstance(
                LedgerMetadata.class.getClassLoader(),
                new Class<?>[] {LedgerMetadata.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getLedgerId" -> ledgerId;
                    case "getEnsembleSize" -> state.configuration.ensembleSize();
                    case "getWriteQuorumSize" -> state.configuration.writeQuorumSize();
                    case "getAckQuorumSize" -> state.configuration.ackQuorumSize();
                    case "getDigestType" -> state.configuration.digestType().toClientType();
                    case "getCustomMetadata" -> state.customMetadata;
                    case "getLastEntryId" -> state.entries.isEmpty() ? -1L : state.entries.lastKey();
                    case "getLength" -> state.entries.values().stream()
                            .mapToLong(value -> value.length)
                            .sum();
                    case "getCtime", "getCToken" -> 0L;
                    case "isClosed" -> state.closed;
                    case "hasPassword" -> false;
                    case "getPassword" -> new byte[0];
                    case "toSafeString", "toString" -> "fake-ledger-" + ledgerId;
                    case "getMetadataFormatVersion" -> 1;
                    default -> defaultValue(method.getReturnType());
                });
    }

    private static WriteAdvHandle writeHandle(long ledgerId, LedgerState state) {
        return (WriteAdvHandle) Proxy.newProxyInstance(
                WriteAdvHandle.class.getClassLoader(),
                new Class<?>[] {WriteAdvHandle.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getId" -> ledgerId;
                    case "getLedgerMetadata" -> ledgerMetadata(ledgerId, state);
                    case "closeAsync", "force" -> CompletableFuture.completedFuture(null);
                    case "getLastAddConfirmed", "getLength", "getLastAddPushed" -> -1L;
                    case "isClosed" -> state.closed;
                    case "toString" -> "fake-write-handle-" + ledgerId;
                    default -> defaultValue(method.getReturnType());
                });
    }

    private static ReadHandle readHandle(long ledgerId, LedgerState state) {
        return (ReadHandle) Proxy.newProxyInstance(
                ReadHandle.class.getClassLoader(),
                new Class<?>[] {ReadHandle.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getId" -> ledgerId;
                    case "getLedgerMetadata" -> ledgerMetadata(ledgerId, state);
                    case "closeAsync" -> CompletableFuture.completedFuture(null);
                    case "getLastAddConfirmed" -> state.entries.isEmpty() ? -1L : state.entries.lastKey();
                    case "getLength" -> state.entries.values().stream()
                            .mapToLong(value -> value.length)
                            .sum();
                    case "isClosed" -> state.closed;
                    case "toString" -> "fake-read-handle-" + ledgerId;
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
                BookKeeperWalConfiguration configuration, Map<String, byte[]> customMetadata) {
            this.configuration = configuration;
            this.customMetadata = customMetadata.entrySet().stream().collect(
                    java.util.stream.Collectors.toUnmodifiableMap(
                            Map.Entry::getKey, entry -> entry.getValue().clone()));
        }
    }
}
