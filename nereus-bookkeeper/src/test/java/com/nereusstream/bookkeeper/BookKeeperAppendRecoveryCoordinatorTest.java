/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.AppendAttemptId;
import com.nereusstream.api.AppendBatch;
import com.nereusstream.api.AppendCompletionPolicy;
import com.nereusstream.api.AppendEntry;
import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.AppendSession;
import com.nereusstream.api.AppendSessionOptions;
import com.nereusstream.api.DurabilityLevel;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamCreateOptions;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamName;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import com.nereusstream.core.wal.DurablePrimaryAppend;
import com.nereusstream.core.wal.PrimaryAppendRequest;
import com.nereusstream.core.append.RequiredObjectGenerationProof;
import com.nereusstream.core.append.RequiredObjectGenerationRequest;
import com.nereusstream.metadata.oxia.BookKeeperMetadataStoreConfig;
import com.nereusstream.metadata.oxia.testing.FakeOxiaMetadataStore;
import com.nereusstream.metadata.oxia.records.AppendReservationLifecycle;
import com.nereusstream.metadata.oxia.records.AppendSessionRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperWriterLifecycle;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class BookKeeperAppendRecoveryCoordinatorTest {
    @Test
    void syncRestartWaitsForExactObjectProofWithoutAnotherBookKeeperWrite() {
        try (Fixture fixture = new Fixture("sync-current-session")) {
            AppendAttemptId attempt = new AppendAttemptId("attempt-sync-current");
            DurablePrimaryAppend durable = fixture.persist(
                    attempt, fixture.session, new byte[] {1}, new byte[] {2});
            int writesBeforeRecovery = fixture.runtime.operations.writeCalls();
            CompletableFuture<RequiredObjectGenerationRequest> observed = new CompletableFuture<>();
            CompletableFuture<RequiredObjectGenerationProof> objectProof = new CompletableFuture<>();

            CompletableFuture<com.nereusstream.api.AppendResult> recovery =
                    fixture.coordinator(fixture.runtime.recovery).recoverAfterRestart(
                            fixture.session,
                            attempt,
                            DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED,
                            AppendCompletionPolicy.REQUIRED_OBJECT_GENERATION,
                            (request, timeout) -> {
                                observed.complete(request);
                                return objectProof;
                            },
                            Duration.ofSeconds(10));

            RequiredObjectGenerationRequest request = observed.join();
            assertThat(request.streamId()).isEqualTo(fixture.stream);
            assertThat(request.sourceRange()).isEqualTo(new com.nereusstream.api.OffsetRange(0, 2));
            assertThat(recovery).isNotDone();
            assertThat(fixture.runtime.operations.writeCalls()).isEqualTo(writesBeforeRecovery);
            Checksum sha = new Checksum(ChecksumType.SHA256, "a".repeat(64));
            objectProof.complete(new RequiredObjectGenerationProof(
                    request,
                    "sync-task",
                    1,
                    "/generation/sync/1",
                    1,
                    sha,
                    sha));

            assertThat(recovery.join().readTarget()).isEqualTo(durable.readTarget());
            assertThat(fixture.runtime.operations.writeCalls()).isEqualTo(writesBeforeRecovery);
            fixture.assertReservation(attempt, AppendReservationLifecycle.HEAD_COMMITTED);
        }
    }

    @Test
    void currentSessionCommitsTheSameDurableRangeWithoutAnotherBookKeeperWrite() {
        try (Fixture fixture = new Fixture("current-session")) {
            AppendAttemptId attempt = new AppendAttemptId("attempt-current");
            DurablePrimaryAppend durable = fixture.persist(attempt, fixture.session, new byte[] {1}, new byte[] {2});
            int writesBeforeRecovery = fixture.runtime.operations.writeCalls();

            var recovered = fixture.coordinator(fixture.runtime.recovery)
                    .recoverAfterRestart(
                            fixture.session,
                            attempt,
                            DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED,
                            Duration.ofSeconds(10))
                    .join();

            assertThat(recovered.readTarget()).isEqualTo(durable.readTarget());
            assertThat(recovered.range().startOffset()).isZero();
            assertThat(recovered.range().endOffset()).isEqualTo(2);
            assertThat(fixture.runtime.operations.writeCalls()).isEqualTo(writesBeforeRecovery);
            fixture.assertReservation(attempt, AppendReservationLifecycle.HEAD_COMMITTED);
            fixture.assertWriterAndRoot(
                    attempt, BookKeeperWriterLifecycle.IDLE, BookKeeperLedgerLifecycle.SEALED);
            assertThat(fixture.l0.scanOffsetIndex(
                            BookKeeperPrimaryWalAppenderTest.CLUSTER, fixture.stream, 0, 10)
                    .join()).singleElement().satisfies(index ->
                            assertThat(index.readTarget()).isEqualTo(durable.readTarget()));
        }
    }

    @Test
    void preparedIntentResponseLossRetriesTheSameRangeWithoutAnotherBookKeeperWrite() {
        try (Fixture fixture = new Fixture("intent-response-loss")) {
            AppendAttemptId attempt = new AppendAttemptId("attempt-intent-loss");
            DurablePrimaryAppend durable = fixture.persist(attempt, fixture.session, new byte[] {1}, new byte[] {2});
            int writesBeforeRecovery = fixture.runtime.operations.writeCalls();
            fixture.l0.failNext(FakeOxiaMetadataStore.FailurePoint.AFTER_COMMIT_LOG_PUT);

            assertThatThrownBy(() -> fixture.coordinator(fixture.runtime.recovery)
                            .recoverAfterRestart(
                                    fixture.session,
                                    attempt,
                                    DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED,
                                    Duration.ofSeconds(10))
                            .join())
                    .hasCauseInstanceOf(NereusException.class)
                    .rootCause()
                    .satisfies(error -> assertThat(((NereusException) error).appendOutcome())
                            .contains(AppendOutcome.KNOWN_NOT_COMMITTED));

            var recovered = fixture.coordinator(fixture.runtime.recovery)
                    .recoverAfterRestart(
                            fixture.session,
                            attempt,
                            DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED,
                            Duration.ofSeconds(10))
                    .join();

            assertThat(recovered.readTarget()).isEqualTo(durable.readTarget());
            assertThat(fixture.runtime.operations.writeCalls()).isEqualTo(writesBeforeRecovery);
            fixture.assertReservation(attempt, AppendReservationLifecycle.HEAD_COMMITTED);
        }
    }

    @Test
    void reachableHeadResponseLossRepairsFromTheSameRangeAfterLedgerSeal() {
        try (Fixture fixture = new Fixture("head-response-loss")) {
            AppendAttemptId attempt = new AppendAttemptId("attempt-head-loss");
            DurablePrimaryAppend durable = fixture.persist(attempt, fixture.session, new byte[] {1}, new byte[] {2});
            int writesBeforeRecovery = fixture.runtime.operations.writeCalls();
            fixture.l0.failNext(FakeOxiaMetadataStore.FailurePoint.AFTER_HEAD_CAS_BEFORE_DERIVED_INDEX);

            assertThatThrownBy(() -> fixture.coordinator(fixture.runtime.recovery)
                            .recoverAfterRestart(
                                    fixture.session,
                                    attempt,
                                    DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED,
                                    Duration.ofSeconds(10))
                            .join())
                    .hasCauseInstanceOf(NereusException.class)
                    .rootCause()
                    .satisfies(error -> assertThat(((NereusException) error).appendOutcome())
                            .contains(AppendOutcome.KNOWN_COMMITTED));
            fixture.runtime.recovery.recoverWriter(
                    fixture.session, Duration.ofSeconds(10), "simulated process restart after head response loss")
                    .join();

            var recovered = fixture.coordinator(fixture.runtime.recovery)
                    .recoverAfterRestart(
                            fixture.session,
                            attempt,
                            DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED,
                            Duration.ofSeconds(10))
                    .join();

            assertThat(recovered.readTarget()).isEqualTo(durable.readTarget());
            assertThat(fixture.runtime.operations.writeCalls()).isEqualTo(writesBeforeRecovery);
            fixture.assertReservation(attempt, AppendReservationLifecycle.HEAD_COMMITTED);
            fixture.assertWriterAndRoot(
                    attempt, BookKeeperWriterLifecycle.IDLE, BookKeeperLedgerLifecycle.SEALED);
        }
    }

    @Test
    void nonDurableWritingCutIsSealedAndProvenNotCommitted() {
        try (Fixture fixture = new Fixture("writing-cut")) {
            AppendAttemptId attempt = new AppendAttemptId("attempt-writing");
            fixture.runtime.operations.hangWriteCall = 1;
            BookKeeperPreparedPrimaryAppend prepared = fixture.prepare(
                    attempt, fixture.session, new byte[] {1}, new byte[] {2});
            CompletableFuture<DurablePrimaryAppend> pending = fixture.runtime.appender.persist(
                    prepared, Duration.ofSeconds(10));
            assertThat(fixture.runtime.operations.writeCalls()).isOne();

            assertThatThrownBy(() -> fixture.coordinator(fixture.runtime.recovery)
                            .recoverAfterRestart(
                                    fixture.session,
                                    attempt,
                                    DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED,
                                    Duration.ofSeconds(10))
                            .join())
                    .hasCauseInstanceOf(NereusException.class)
                    .rootCause()
                    .satisfies(error -> assertThat(((NereusException) error).appendOutcome())
                            .contains(AppendOutcome.KNOWN_NOT_COMMITTED));

            fixture.runtime.operations.failHungWrite();
            assertThatThrownBy(pending::join).hasCauseInstanceOf(NereusException.class);
            prepared.close();
            fixture.assertReservation(attempt, AppendReservationLifecycle.ABANDONED);
            fixture.assertWriterAndRoot(
                    attempt, BookKeeperWriterLifecycle.IDLE, BookKeeperLedgerLifecycle.SEALED);
        }
    }

    @Test
    void newSessionAbandonsUnreachableDurableRangeAndAllocatesAnotherLedger() {
        try (Fixture fixture = new Fixture("fenced-session")) {
            AppendAttemptId oldAttempt = new AppendAttemptId("attempt-old-session");
            DurablePrimaryAppend old = fixture.persist(oldAttempt, fixture.session, new byte[] {1}, new byte[] {2});
            long oldLedger = ((BookKeeperEntryRangeReadTarget) old.readTarget()).ledgerId();
            fixture.expireSession();
            AppendSession newSession = fixture.acquire("writer-2");
            BookKeeperWriterStateMachine newWriterState = new BookKeeperWriterStateMachine(
                    BookKeeperPrimaryWalAppenderTest.CLUSTER,
                    fixture.runtime.configuration,
                    fixture.runtime.metadata,
                    BookKeeperPrimaryWalAppenderTest.CLOCK,
                    "process-run-2");
            BookKeeperLedgerRecovery newRecovery = new BookKeeperLedgerRecovery(
                    BookKeeperPrimaryWalAppenderTest.CLUSTER,
                    fixture.runtime.configuration,
                    fixture.runtime.metadata,
                    fixture.runtime.metadata,
                    fixture.runtime.verifier,
                    fixture.runtime.operations,
                    ignored -> "secret".getBytes(StandardCharsets.UTF_8),
                    newWriterState,
                    BookKeeperPrimaryWalAppenderTest.CLOCK);

            assertThatThrownBy(() -> fixture.coordinator(newRecovery)
                            .recoverAfterRestart(
                                    newSession,
                                    oldAttempt,
                                    DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED,
                                    Duration.ofSeconds(10))
                            .join())
                    .hasCauseInstanceOf(NereusException.class)
                    .rootCause()
                    .satisfies(error -> assertThat(((NereusException) error).appendOutcome())
                            .contains(AppendOutcome.KNOWN_NOT_COMMITTED));
            fixture.assertReservation(oldAttempt, AppendReservationLifecycle.ABANDONED);
            fixture.assertWriterAndRoot(
                    oldAttempt, BookKeeperWriterLifecycle.IDLE, BookKeeperLedgerLifecycle.SEALED);
            var sealedRoot = fixture.runtime.metadata.getRoot(
                            BookKeeperPrimaryWalAppenderTest.CLUSTER,
                            fixture.runtime.configuration.providerScopeSha256(),
                            oldLedger)
                    .join()
                    .orElseThrow();
            BookKeeperWalOnlyRetirementAuthority authority = new BookKeeperWalOnlyRetirementAuthority(
                    BookKeeperPrimaryWalAppenderTest.CLUSTER,
                    fixture.l0,
                    fixture.runtime.metadata);
            BookKeeperWalReferenceManager references = new BookKeeperWalReferenceManager(
                    BookKeeperPrimaryWalAppenderTest.CLUSTER,
                    fixture.runtime.configuration,
                    fixture.runtime.metadata,
                    authority);
            assertThat(new BookKeeperWalOnlyReferenceRetirementCoordinator(
                            BookKeeperPrimaryWalAppenderTest.CLUSTER,
                            fixture.runtime.configuration,
                            fixture.runtime.metadata,
                            authority,
                            references)
                    .retireEligible(sealedRoot, Duration.ofSeconds(10))
                    .join()
                    .fullyRetired()).isTrue();

            BookKeeperLedgerAllocator allocator = new BookKeeperLedgerAllocator(
                    BookKeeperPrimaryWalAppenderTest.CLUSTER,
                    fixture.runtime.configuration,
                    fixture.runtime.metadata,
                    fixture.runtime.metadata,
                    fixture.runtime.verifier,
                    fixture.runtime.operations,
                    ignored -> "secret".getBytes(StandardCharsets.UTF_8),
                    newWriterState,
                    BookKeeperPrimaryWalAppenderTest.CLOCK,
                    new Random(73),
                    () -> "allocation-after-fence");
            try (BookKeeperPrimaryWalAppender appender = new BookKeeperPrimaryWalAppender(
                    BookKeeperPrimaryWalAppenderTest.CLUSTER,
                    fixture.runtime.configuration,
                    fixture.runtime.metadata,
                    fixture.runtime.metadata,
                    allocator,
                    newRecovery,
                    newWriterState,
                    fixture.runtime.operations,
                    BookKeeperPrimaryWalAppenderTest.CLOCK);
                    BookKeeperPreparedPrimaryAppend retry = fixture.prepare(
                            appender,
                            new AppendAttemptId("attempt-new-session"),
                            newSession,
                            new byte[] {9})) {
                DurablePrimaryAppend replacement = appender.persist(retry, Duration.ofSeconds(10)).join();
                BookKeeperEntryRangeReadTarget target =
                        (BookKeeperEntryRangeReadTarget) replacement.readTarget();
                assertThat(target.ledgerId()).isNotEqualTo(oldLedger);
                assertThat(target.firstEntryId()).isZero();
            }
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final BookKeeperPrimaryWalAppenderTest.Runtime runtime =
                new BookKeeperPrimaryWalAppenderTest.Runtime();
        private final AtomicLong now = new AtomicLong(BookKeeperPrimaryWalAppenderTest.CLOCK.millis());
        private final FakeOxiaMetadataStore l0 = new FakeOxiaMetadataStore(
                now::get,
                runtime.metadata,
                new BookKeeperMetadataStoreConfig(
                        runtime.configuration.maxAppendRangesPerLedger(),
                        runtime.configuration.protectionSlotsPerRange(),
                        runtime.configuration.maxReaderLeasesPerLedger(),
                        runtime.configuration.maxUncertainAllocations()));
        private final StreamId stream;
        private final AppendSession session;

        private Fixture(String suffix) {
            stream = new StreamId(l0.createOrGetStream(
                            BookKeeperPrimaryWalAppenderTest.CLUSTER,
                            new StreamName("persistent://tenant/namespace/bk-recovery-" + suffix),
                            new StreamCreateOptions(StorageProfile.BOOKKEEPER_WAL_ONLY, Map.of()))
                    .join()
                    .streamId());
            session = acquire("writer-1");
        }

        private AppendSession acquire(String writerId) {
            AppendSessionRecord record = l0.acquireAppendSession(
                            BookKeeperPrimaryWalAppenderTest.CLUSTER,
                            stream,
                            new AppendSessionOptions(writerId, Duration.ofMinutes(1), true))
                    .join();
            return new AppendSession(
                    stream,
                    record.writerId(),
                    record.epoch(),
                    record.fencingToken(),
                    record.leaseVersion(),
                    record.expiresAtMillis());
        }

        private void expireSession() {
            now.addAndGet(Duration.ofMinutes(2).toMillis());
        }

        private DurablePrimaryAppend persist(
                AppendAttemptId attempt,
                AppendSession appendSession,
                byte[]... payloads) {
            try (BookKeeperPreparedPrimaryAppend prepared = prepare(attempt, appendSession, payloads)) {
                return runtime.appender.persist(prepared, Duration.ofSeconds(10)).join();
            }
        }

        private BookKeeperPreparedPrimaryAppend prepare(
                AppendAttemptId attempt,
                AppendSession appendSession,
                byte[]... payloads) {
            return prepare(runtime.appender, attempt, appendSession, payloads);
        }

        private BookKeeperPreparedPrimaryAppend prepare(
                BookKeeperPrimaryWalAppender appender,
                AppendAttemptId attempt,
                AppendSession appendSession,
                byte[]... payloads) {
            List<AppendEntry> entries = java.util.stream.IntStream.range(0, payloads.length)
                    .mapToObj(index -> new AppendEntry(payloads[index], 1, index + 1L, Map.of()))
                    .toList();
            AppendBatch batch = new AppendBatch(
                    PayloadFormat.OPAQUE_RECORD_BATCH,
                    entries,
                    entries.size(),
                    entries.size(),
                    1,
                    entries.size(),
                    List.of(),
                    Map.of(),
                    Optional.empty());
            return appender.prepare(new PrimaryAppendRequest(
                    stream,
                    batch,
                    appendSession,
                    0,
                    attempt,
                    Duration.ofSeconds(10)));
        }

        private BookKeeperAppendRecoveryCoordinator coordinator(BookKeeperLedgerRecovery recovery) {
            return new BookKeeperAppendRecoveryCoordinator(
                    BookKeeperPrimaryWalAppenderTest.CLUSTER,
                    runtime.configuration,
                    runtime.metadata,
                    l0,
                    runtime.references,
                    recovery,
                    BookKeeperPrimaryWalAppenderTest.CLOCK);
        }

        private void assertReservation(
                AppendAttemptId attempt,
                AppendReservationLifecycle lifecycle) {
            String reservationId = BookKeeperAppendReservationIds.forAttempt(stream, attempt);
            assertThat(runtime.metadata.getReservation(
                            BookKeeperPrimaryWalAppenderTest.CLUSTER, stream, reservationId)
                    .join()).get().satisfies(value ->
                            assertThat(value.value().lifecycle()).isEqualTo(lifecycle));
        }

        private void assertWriterAndRoot(
                AppendAttemptId attempt,
                BookKeeperWriterLifecycle writerLifecycle,
                BookKeeperLedgerLifecycle rootLifecycle) {
            var writer = runtime.metadata.getWriter(BookKeeperPrimaryWalAppenderTest.CLUSTER, stream)
                    .join()
                    .orElseThrow();
            assertThat(writer.value().lifecycle()).isEqualTo(writerLifecycle);
            long ledgerId = runtime.metadata.getReservation(
                            BookKeeperPrimaryWalAppenderTest.CLUSTER,
                            stream,
                            BookKeeperAppendReservationIds.forAttempt(stream, attempt))
                    .join()
                    .orElseThrow()
                    .value()
                    .ledgerId();
            assertThat(runtime.metadata.getRoot(
                            BookKeeperPrimaryWalAppenderTest.CLUSTER,
                            runtime.configuration.providerScopeSha256(),
                            ledgerId)
                    .join()).get().satisfies(root ->
                            assertThat(root.value().lifecycle()).isEqualTo(rootLifecycle));
        }

        @Override
        public void close() {
            l0.close();
            runtime.close();
        }
    }
}
