/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.AppendAttemptId;
import com.nereusstream.api.AppendBatch;
import com.nereusstream.api.AppendEntry;
import com.nereusstream.api.AppendSession;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import com.nereusstream.core.wal.DurablePrimaryAppend;
import com.nereusstream.core.wal.PrimaryAppendRequest;
import com.nereusstream.metadata.oxia.BookKeeperMetadataStoreConfig;
import com.nereusstream.metadata.oxia.BookKeeperPhysicalReferenceProof;
import com.nereusstream.metadata.oxia.CommitAppendRequest;
import com.nereusstream.metadata.oxia.CommittedAppend;
import com.nereusstream.metadata.oxia.FakeBookKeeperMetadataStore;
import com.nereusstream.metadata.oxia.MaterializedGenerationZero;
import com.nereusstream.metadata.oxia.PreparedStableAppend;
import com.nereusstream.metadata.oxia.records.AppendReservationLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperProtectionType;
import com.nereusstream.metadata.oxia.records.BookKeeperWriterLifecycle;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
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

class BookKeeperPrimaryWalAppenderTest {
    static final String CLUSTER = "cluster/bk-appender";
    static final String DEPLOYMENT = "deployment-1";
    static final StreamId STREAM = new StreamId("stream-bk-appender");
    static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC);

    @Test
    void reservesWritesAndReturnsOneStableExactRange() {
        try (Runtime runtime = new Runtime()) {
            AppendSession session = session();
            DurablePrimaryAppend first;
            try (BookKeeperPreparedPrimaryAppend prepared = runtime.appender.prepare(request(
                    session, "attempt-1", 10, new byte[] {1, 2}, new byte[] {3}))) {
                first = runtime.appender.persist(prepared, Duration.ofSeconds(10)).join();
            }
            BookKeeperEntryRangeReadTarget firstTarget = (BookKeeperEntryRangeReadTarget) first.readTarget();
            BookKeeperProviderAppendToken firstToken = (BookKeeperProviderAppendToken) first.providerToken();

            assertThat(firstTarget.firstEntryId()).isZero();
            assertThat(firstTarget.entryCount()).isEqualTo(2);
            assertEntries(runtime.operations.entries(firstTarget.ledgerId()),
                    entry(0, 1, 2), entry(1, 3));
            assertThat(runtime.metadata.getReservation(CLUSTER, STREAM, firstToken.reservationId()).join())
                    .get().satisfies(value ->
                            assertThat(value.value().lifecycle()).isEqualTo(AppendReservationLifecycle.DURABLE));
            assertThat(runtime.metadata.getWriter(CLUSTER, STREAM).join()).get().satisfies(value -> {
                assertThat(value.value().lifecycle()).isEqualTo(BookKeeperWriterLifecycle.ACTIVE);
                assertThat(value.value().nextEntryId()).isEqualTo(2);
                assertThat(value.value().activeAppendRangeCount()).isOne();
                assertThat(value.value().activePhysicalBytes()).isEqualTo(3);
                assertThat(value.value().activeReservationId()).isEmpty();
            });
            for (int slot = 0; slot < 3; slot++) {
                int exactSlot = slot;
                assertThat(runtime.metadata.getProtection(
                                CLUSTER, runtime.configuration.providerScopeSha256(), firstTarget.ledgerId(), 0, slot)
                        .join()).get().satisfies(value -> {
                            assertThat(value.value().protectionType()).isEqualTo(switch (exactSlot) {
                                case 0 -> BookKeeperProtectionType.REACHABLE_APPEND;
                                case 1 -> BookKeeperProtectionType.VISIBLE_GENERATION;
                                default -> BookKeeperProtectionType.APPEND_RECOVERY;
                            });
                            assertThat(value.value().referenceId()).isEqualTo(firstToken.reservationId());
                        });
            }
            runtime.appender.validateBeforeHeadCommit(first, session, Duration.ofSeconds(10)).join();

            DurablePrimaryAppend second;
            try (BookKeeperPreparedPrimaryAppend prepared = runtime.appender.prepare(request(
                    session, "attempt-2", 12, new byte[] {4, 5}))) {
                second = runtime.appender.persist(prepared, Duration.ofSeconds(10)).join();
            }
            BookKeeperEntryRangeReadTarget secondTarget = (BookKeeperEntryRangeReadTarget) second.readTarget();
            assertThat(secondTarget.ledgerId()).isEqualTo(firstTarget.ledgerId());
            assertThat(secondTarget.firstEntryId()).isEqualTo(2);
            assertEntries(runtime.operations.entries(firstTarget.ledgerId()),
                    entry(0, 1, 2), entry(1, 3), entry(2, 4, 5));
        }
    }

    @Test
    void partialWriteAbandonsRangeRecoverySealsLedgerAndNextAppendAllocatesFreshLedger() {
        try (Runtime runtime = new Runtime()) {
            runtime.operations.failWriteCall = 2;
            AppendSession session = session();

            assertThatThrownBy(() -> {
                try (BookKeeperPreparedPrimaryAppend prepared = runtime.appender.prepare(request(
                        session, "attempt-failed", 0, new byte[] {1}, new byte[] {2}))) {
                    runtime.appender.persist(prepared, Duration.ofSeconds(10)).join();
                }
            }).hasRootCauseInstanceOf(NereusException.class)
                    .rootCause().extracting(error -> ((NereusException) error).code())
                    .isEqualTo(ErrorCode.PRIMARY_WAL_WRITE_FAILED);

            var failedReservation = runtime.metadata.scanReservations(
                    CLUSTER, STREAM, Optional.empty(), 10).join().values().get(0);
            assertThat(failedReservation.value().lifecycle()).isEqualTo(AppendReservationLifecycle.ABANDONED);
            long failedLedger = failedReservation.value().ledgerId();
            assertThat(runtime.metadata.getRoot(
                    CLUSTER, runtime.configuration.providerScopeSha256(), failedLedger).join())
                    .get().satisfies(value ->
                            assertThat(value.value().lifecycle()).isEqualTo(BookKeeperLedgerLifecycle.SEALED));
            assertThat(runtime.metadata.getWriter(CLUSTER, STREAM).join())
                    .get().satisfies(value -> assertThat(value.value().lifecycle())
                            .isEqualTo(BookKeeperWriterLifecycle.IDLE));
            assertThat(runtime.operations.recoveryOpenCalls).isOne();

            DurablePrimaryAppend retry;
            try (BookKeeperPreparedPrimaryAppend prepared = runtime.appender.prepare(request(
                    session, "attempt-new-ledger", 0, new byte[] {9}))) {
                retry = runtime.appender.persist(prepared, Duration.ofSeconds(10)).join();
            }
            assertThat(((BookKeeperEntryRangeReadTarget) retry.readTarget()).ledgerId()).isNotEqualTo(failedLedger);
            assertThat(runtime.operations.createCalls).isEqualTo(2);
        }
    }

    @Test
    void activatesReachableAndVisibleProtectionsUnderExactDurableOwners() {
        try (Runtime runtime = new Runtime()) {
            AppendSession session = session();
            DurablePrimaryAppend durable;
            try (BookKeeperPreparedPrimaryAppend prepared = runtime.appender.prepare(request(
                    session, "attempt-protection", 10, new byte[] {1, 2}, new byte[] {3}))) {
                durable = runtime.appender.persist(prepared, Duration.ofSeconds(10)).join();
            }
            BookKeeperEntryRangeReadTarget target = (BookKeeperEntryRangeReadTarget) durable.readTarget();
            CommitAppendRequest request = new CommitAppendRequest(
                    STREAM, session.writerId(), "generic-process-run", session.epoch(), session.fencingToken(),
                    10, target, PayloadFormat.OPAQUE_RECORD_BATCH, 2, 2, 3, List.of(), 1, 2, Optional.empty());
            Checksum commitSha = sha('c');
            PreparedStableAppend prepared = new PreparedStableAppend(
                    request, request.commitId(), "/commit/owner", 7, commitSha,
                    com.nereusstream.api.ReadTargetIdentities.sha256(target), false);

            var stable = runtime.references.protectBeforeHead(prepared, target, Duration.ofSeconds(10)).join();
            assertThat(stable.proof()).isInstanceOf(BookKeeperPhysicalReferenceProof.class);
            BookKeeperPhysicalReferenceProof stableProof = (BookKeeperPhysicalReferenceProof) stable.proof();
            assertThat(stableProof.protectionSlot()).isZero();
            var reachable = runtime.metadata.getProtection(
                    CLUSTER, runtime.configuration.providerScopeSha256(), target.ledgerId(),
                    stableProof.ledgerRangeSlot(), 0).join().orElseThrow();
            assertThat(reachable.value().lifecycle()).isEqualTo(
                    com.nereusstream.metadata.oxia.records.ProtectionLifecycle.ACTIVE);
            assertThat(reachable.value().ownerKey()).isEqualTo(prepared.commitKey());

            CommittedAppend committed = new CommittedAppend(
                    STREAM, prepared.commitId(), "", target, new OffsetRange(10, 12), 0, 3, 1,
                    PayloadFormat.OPAQUE_RECORD_BATCH, 2, 2, 3, List.of(), Optional.empty(), 1, 2);
            MaterializedGenerationZero generationZero = new MaterializedGenerationZero(
                    committed, "/index/generation-zero", 8, sha('d'));
            var visible = runtime.references.protectVisibleIndex(
                    generationZero, target, Duration.ofSeconds(10)).join();
            BookKeeperPhysicalReferenceProof visibleProof = (BookKeeperPhysicalReferenceProof) visible.proof();
            assertThat(visibleProof.protectionSlot()).isOne();
            assertThat(runtime.metadata.getProtection(
                    CLUSTER, runtime.configuration.providerScopeSha256(), target.ledgerId(),
                    visibleProof.ledgerRangeSlot(), 1).join()).get().satisfies(value -> {
                        assertThat(value.value().lifecycle()).isEqualTo(
                                com.nereusstream.metadata.oxia.records.ProtectionLifecycle.ACTIVE);
                        assertThat(value.value().ownerKey()).isEqualTo(generationZero.indexKey());
                        assertThat(value.value().commitVersion()).isEqualTo(committed.commitVersion());
                    });
            BookKeeperProviderAppendToken token = (BookKeeperProviderAppendToken) durable.providerToken();
            assertThat(runtime.metadata.getReservation(CLUSTER, STREAM, token.reservationId()).join())
                    .get().satisfies(value -> assertThat(value.value().lifecycle())
                            .isEqualTo(AppendReservationLifecycle.HEAD_COMMITTED));
        }
    }

    private static Checksum sha(char value) {
        return new Checksum(ChecksumType.SHA256, Character.toString(value).repeat(64));
    }

    private static Map.Entry<Long, byte[]> entry(long id, int... bytes) {
        byte[] value = new byte[bytes.length];
        for (int index = 0; index < bytes.length; index++) value[index] = (byte) bytes[index];
        return Map.entry(id, value);
    }

    @SafeVarargs
    private static void assertEntries(
            List<Map.Entry<Long, byte[]>> actual, Map.Entry<Long, byte[]>... expected) {
        assertThat(actual).hasSize(expected.length);
        for (int index = 0; index < expected.length; index++) {
            assertThat(actual.get(index).getKey()).isEqualTo(expected[index].getKey());
            assertThat(actual.get(index).getValue()).containsExactly(expected[index].getValue());
        }
    }

    static PrimaryAppendRequest request(
            AppendSession session, String attempt, long offset, byte[]... payloads) {
        List<AppendEntry> entries = new ArrayList<>();
        for (int index = 0; index < payloads.length; index++) {
            entries.add(new AppendEntry(payloads[index], 1, index + 1L, Map.of()));
        }
        AppendBatch batch = new AppendBatch(PayloadFormat.OPAQUE_RECORD_BATCH, entries,
                entries.size(), entries.size(), 1, entries.size(), List.of(), Map.of(), Optional.empty());
        return new PrimaryAppendRequest(STREAM, batch, session, offset,
                new AppendAttemptId(attempt), Duration.ofSeconds(10));
    }

    static AppendSession session() {
        return new AppendSession(STREAM, "writer-1", 1, "token-1", 1, 10_000);
    }

    static BookKeeperLedgerIdNamespaceReservation reservation(BookKeeperWalConfiguration configuration) {
        return new BookKeeperLedgerIdNamespaceReservation(1,
                configuration.ledgerIdNamespaceReservationId(), DEPLOYMENT, configuration.clusterAlias(),
                configuration.providerScopeSha256(), configuration.ledgerIdPrefixBits(),
                configuration.ledgerIdPrefixValue(), BookKeeperLedgerIdNamespaceReservation.Lifecycle.ACTIVE,
                1, 100, 0, "a".repeat(64), 1,
                new Checksum(ChecksumType.SHA256, "b".repeat(64)), "/bookkeeper/reservation");
    }

    static final class Runtime implements AutoCloseable {
        final BookKeeperWalConfiguration configuration = BookKeeperTestConfigurations.valid();
        final FakeBookKeeperMetadataStore metadata = new FakeBookKeeperMetadataStore(
                new BookKeeperMetadataStoreConfig(configuration.maxAppendRangesPerLedger(),
                        configuration.protectionSlotsPerRange(), configuration.maxReaderLeasesPerLedger(),
                        configuration.maxUncertainAllocations()), CLOCK);
        final FakeOperations operations = new FakeOperations();
        final BookKeeperWriterStateMachine writerState = new BookKeeperWriterStateMachine(
                CLUSTER, configuration, metadata, CLOCK, "process-run-1");
        final BookKeeperLedgerIdNamespaceReservationVerifier verifier =
                new BookKeeperLedgerIdNamespaceReservationVerifier(
                        (scope, bits, prefix, timeout) -> CompletableFuture.completedFuture(
                                Optional.of(reservation(configuration))), DEPLOYMENT);
        private final AtomicInteger allocationSequence = new AtomicInteger();
        private final BookKeeperLedgerAllocator allocator = new BookKeeperLedgerAllocator(
                CLUSTER, configuration, metadata, metadata, verifier, operations,
                ignored -> "secret".getBytes(StandardCharsets.UTF_8), writerState, CLOCK, new Random(41),
                () -> "allocation-" + allocationSequence.incrementAndGet());
        final BookKeeperLedgerRecovery recovery = new BookKeeperLedgerRecovery(
                CLUSTER, configuration, metadata, metadata, verifier, operations,
                ignored -> "secret".getBytes(StandardCharsets.UTF_8), writerState, CLOCK);
        final BookKeeperPrimaryWalAppender appender = new BookKeeperPrimaryWalAppender(
                CLUSTER, configuration, metadata, metadata, allocator, recovery, writerState, operations, CLOCK);
        final BookKeeperPrimaryPhysicalReferenceAdapter references =
                new BookKeeperPrimaryPhysicalReferenceAdapter(
                        CLUSTER, configuration, metadata, metadata, CLOCK);
        final BookKeeperLedgerHandleCache handles = new BookKeeperLedgerHandleCache(
                8, 8 * 1024, 1024, Duration.ofMinutes(1));
        final BookKeeperReaderLeaseManager readerLeases = new BookKeeperReaderLeaseManager(
                CLUSTER, configuration, metadata, CLOCK, "reader-process-1");
        final BookKeeperPrimaryWalReader reader = new BookKeeperPrimaryWalReader(
                CLUSTER, configuration, metadata, operations,
                ignored -> "secret".getBytes(StandardCharsets.UTF_8), handles, readerLeases);

        @Override
        public void close() {
            reader.close();
            appender.close();
            metadata.close();
        }
    }

    static final class FakeOperations implements BookKeeperClientOperations {
        private final Map<Long, LedgerState> ledgers = new LinkedHashMap<>();
        private int createCalls;
        private int writeCalls;
        int failWriteCall;
        int hangWriteCall;
        private CompletableFuture<Long> hungWrite;
        boolean failDeleteAfterRemoval;
        int recoveryOpenCalls;
        int normalOpenCalls;

        int writeCalls() {
            return writeCalls;
        }

        int providerCalls() {
            return createCalls + writeCalls + recoveryOpenCalls + normalOpenCalls;
        }

        void failHungWrite() {
            CompletableFuture<Long> pending = java.util.Objects.requireNonNull(hungWrite, "hungWrite");
            hungWrite = null;
            pending.completeExceptionally(new NereusException(
                    ErrorCode.PRIMARY_WAL_WRITE_FAILED, true, "terminated simulated crashed write"));
        }

        private List<Map.Entry<Long, byte[]>> entries(long ledgerId) {
            return ledgers.get(ledgerId).entries.entrySet().stream()
                    .map(entry -> Map.entry(entry.getKey(), entry.getValue().clone()))
                    .toList();
        }

        @Override
        public CompletableFuture<WriteAdvHandle> createAdvanced(
                long ledgerId, BookKeeperWalConfiguration configuration, byte[] password,
                Map<String, byte[]> customMetadata, BookKeeperOperationDeadline deadline) {
            createCalls++;
            LedgerState state = new LedgerState(configuration, customMetadata);
            ledgers.put(ledgerId, state);
            return CompletableFuture.completedFuture(writeHandle(ledgerId, state));
        }

        @Override
        public CompletableFuture<ReadHandle> open(
                long ledgerId, BookKeeperDigestType digestType, byte[] password,
                boolean recovery, BookKeeperOperationDeadline deadline) {
            LedgerState state = ledgers.get(ledgerId);
            if (state == null) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
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
                WriteAdvHandle handle, long entryId, ByteBuf entry, BookKeeperOperationDeadline deadline) {
            writeCalls++;
            if (writeCalls == failWriteCall) {
                failWriteCall = 0;
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.PRIMARY_WAL_WRITE_FAILED, true, "injected entry write failure"));
            }
            if (writeCalls == hangWriteCall) {
                hangWriteCall = 0;
                hungWrite = new CompletableFuture<>();
                return hungWrite;
            }
            byte[] bytes = new byte[entry.readableBytes()];
            entry.getBytes(entry.readerIndex(), bytes);
            ledgers.get(handle.getId()).entries.put(entryId, bytes);
            return CompletableFuture.completedFuture(entryId);
        }

        @Override
        public CompletableFuture<LedgerEntries> readUnconfirmed(
                ReadHandle handle, long firstEntryId, long lastEntryIdInclusive,
                BookKeeperOperationDeadline deadline) {
            LedgerState state = ledgers.get(handle.getId());
            if (state == null) {
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.PRIMARY_WAL_TARGET_NOT_FOUND, false, "ledger is absent"));
            }
            List<LedgerEntry> entries = state.entries.subMap(
                            firstEntryId, true, lastEntryIdInclusive, true).entrySet().stream()
                    .map(entry -> ledgerEntry(handle.getId(), entry.getKey(), entry.getValue()))
                    .toList();
            return CompletableFuture.completedFuture(ledgerEntries(entries));
        }

        @Override
        public CompletableFuture<LedgerMetadata> metadata(long ledgerId, BookKeeperOperationDeadline deadline) {
            LedgerState state = ledgers.get(ledgerId);
            return state == null
                    ? CompletableFuture.failedFuture(new NereusException(
                            ErrorCode.PRIMARY_WAL_TARGET_NOT_FOUND, false, "ledger is absent"))
                    : CompletableFuture.completedFuture(ledgerMetadata(ledgerId, state));
        }

        @Override
        public CompletableFuture<Void> delete(long ledgerId, BookKeeperOperationDeadline deadline) {
            ledgers.remove(ledgerId);
            if (failDeleteAfterRemoval) {
                failDeleteAfterRemoval = false;
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.PRIMARY_WAL_WRITE_FAILED, true, "injected lost delete response"));
            }
            return CompletableFuture.completedFuture(null);
        }
    }

    private static LedgerEntry ledgerEntry(long ledgerId, long entryId, byte[] value) {
        ByteBuf buffer = Unpooled.wrappedBuffer(value.clone());
        return (LedgerEntry) Proxy.newProxyInstance(
                LedgerEntry.class.getClassLoader(), new Class<?>[] {LedgerEntry.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getLedgerId" -> ledgerId;
                    case "getEntryId" -> entryId;
                    case "getLength" -> (long) value.length;
                    case "getEntryBuffer" -> buffer;
                    case "getEntryBytes" -> value.clone();
                    case "close" -> {
                        if (buffer.refCnt() > 0) buffer.release();
                        yield null;
                    }
                    case "toString" -> "fake-entry-" + ledgerId + "-" + entryId;
                    default -> defaultValue(method.getReturnType());
                });
    }

    private static LedgerEntries ledgerEntries(List<LedgerEntry> values) {
        return (LedgerEntries) Proxy.newProxyInstance(
                LedgerEntries.class.getClassLoader(), new Class<?>[] {LedgerEntries.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "iterator" -> values.iterator();
                    case "getEntry" -> values.get((int) args[0]);
                    case "size" -> values.size();
                    case "close" -> {
                        values.forEach(LedgerEntry::close);
                        yield null;
                    }
                    case "toString" -> "fake-ledger-entries";
                    default -> defaultValue(method.getReturnType());
                });
    }

    private static final class LedgerState {
        private final BookKeeperWalConfiguration configuration;
        private final Map<String, byte[]> customMetadata;
        private final TreeMap<Long, byte[]> entries = new TreeMap<>();
        private boolean closed;

        private LedgerState(BookKeeperWalConfiguration configuration, Map<String, byte[]> customMetadata) {
            this.configuration = configuration;
            this.customMetadata = customMetadata.entrySet().stream().collect(
                    java.util.stream.Collectors.toUnmodifiableMap(
                            Map.Entry::getKey, entry -> entry.getValue().clone()));
        }
    }

    private static LedgerMetadata ledgerMetadata(long ledgerId, LedgerState state) {
        return (LedgerMetadata) Proxy.newProxyInstance(
                LedgerMetadata.class.getClassLoader(), new Class<?>[] {LedgerMetadata.class}, (proxy, method, args) ->
                        switch (method.getName()) {
                            case "getLedgerId" -> ledgerId;
                            case "getEnsembleSize" -> state.configuration.ensembleSize();
                            case "getWriteQuorumSize" -> state.configuration.writeQuorumSize();
                            case "getAckQuorumSize" -> state.configuration.ackQuorumSize();
                            case "getDigestType" -> state.configuration.digestType().toClientType();
                            case "getCustomMetadata" -> state.customMetadata;
                            case "getLastEntryId" -> state.entries.isEmpty() ? -1L : state.entries.lastKey();
                            case "getLength" -> state.entries.values().stream().mapToLong(value -> value.length).sum();
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
                WriteAdvHandle.class.getClassLoader(), new Class<?>[] {WriteAdvHandle.class},
                (proxy, method, args) -> switch (method.getName()) {
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
                ReadHandle.class.getClassLoader(), new Class<?>[] {ReadHandle.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getId" -> ledgerId;
                    case "getLedgerMetadata" -> ledgerMetadata(ledgerId, state);
                    case "closeAsync" -> CompletableFuture.completedFuture(null);
                    case "getLastAddConfirmed" -> state.entries.isEmpty() ? -1L : state.entries.lastKey();
                    case "getLength" -> state.entries.values().stream().mapToLong(value -> value.length).sum();
                    case "isClosed" -> state.closed;
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
}
