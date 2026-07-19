/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import com.nereusstream.core.wal.DurablePrimaryAppend;
import com.nereusstream.metadata.oxia.BookKeeperVersionedValue;
import com.nereusstream.metadata.oxia.CommitAppendRequest;
import com.nereusstream.metadata.oxia.CommittedAppend;
import com.nereusstream.metadata.oxia.MaterializedGenerationZero;
import com.nereusstream.metadata.oxia.PreparedStableAppend;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerProtectionRecord;
import com.nereusstream.metadata.oxia.records.AppendReservationLifecycle;
import com.nereusstream.metadata.oxia.records.ProtectionLifecycle;
import java.time.Duration;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class BookKeeperWalRetentionGateTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    @Test
    void admitsOnlyACompleteRetiredInventoryWithoutReaderPins() {
        try (BookKeeperPrimaryWalAppenderTest.Runtime runtime = new BookKeeperPrimaryWalAppenderTest.Runtime()) {
            StableLedger ledger = appendCommitAndSeal(runtime);
            BookKeeperWalRetentionGate gate = gate(runtime, activation(runtime));

            BookKeeperRetentionEvaluation activeReferences = gate.evaluate(ledger.sealedRoot(), TIMEOUT).join();
            assertThat(activeReferences.blockers()).contains(BookKeeperRetentionBlocker.PROTECTION_PRESENT);

            BookKeeperWalReferenceManager references = new BookKeeperWalReferenceManager(
                    BookKeeperPrimaryWalAppenderTest.CLUSTER,
                    runtime.configuration,
                    runtime.metadata,
                    (proof, protection, timeout) -> CompletableFuture.completedFuture(null));
            for (int protectionSlot = 0; protectionSlot < 3; protectionSlot++) {
                var protection = runtime.metadata.getProtection(
                                BookKeeperPrimaryWalAppenderTest.CLUSTER,
                                runtime.configuration.providerScopeSha256(),
                                ledger.target().ledgerId(),
                                0,
                                protectionSlot)
                        .join().orElseThrow();
                var retired = references.retire(
                        ledger.target().ledgerId(), 0, protectionSlot, proof(protection), TIMEOUT).join();
                assertThat(retired.value().lifecycle()).isEqualTo(ProtectionLifecycle.RETIRED);
                assertThat(retired.value().ownerKey()).isEqualTo(protection.value().ownerKey());
            }

            BookKeeperReaderLeaseManager.Lease lease = runtime.readerLeases.claim(
                    ledger.sealedRoot(), TIMEOUT).join();
            assertThat(gate.evaluate(ledger.sealedRoot(), TIMEOUT).join().blockers())
                    .contains(BookKeeperRetentionBlocker.READER_LEASE_PRESENT);
            lease.release().join();

            BookKeeperRetentionEvaluation first = gate.evaluate(ledger.sealedRoot(), TIMEOUT).join();
            BookKeeperRetentionEvaluation second = gate.evaluate(ledger.sealedRoot(), TIMEOUT).join();
            assertThat(first.blockers()).isEmpty();
            assertThat(first.candidate()).isPresent();
            assertThat(first.candidate().orElseThrow().retiredProtections()).hasSize(3);
            assertThat(first.candidate().orElseThrow().referenceSetSha256())
                    .isEqualTo(second.candidate().orElseThrow().referenceSetSha256());
        }
    }

    @Test
    void retirementAuthorityFailureLeavesTheActiveProtectionUntouched() {
        try (BookKeeperPrimaryWalAppenderTest.Runtime runtime = new BookKeeperPrimaryWalAppenderTest.Runtime()) {
            StableLedger ledger = appendCommitAndSeal(runtime);
            var protection = runtime.metadata.getProtection(
                            BookKeeperPrimaryWalAppenderTest.CLUSTER,
                            runtime.configuration.providerScopeSha256(),
                            ledger.target().ledgerId(),
                            0,
                            0)
                    .join().orElseThrow();
            BookKeeperWalReferenceManager references = new BookKeeperWalReferenceManager(
                    BookKeeperPrimaryWalAppenderTest.CLUSTER,
                    runtime.configuration,
                    runtime.metadata,
                    (proof, current, timeout) -> CompletableFuture.failedFuture(
                            new IllegalStateException("retirement authority rejected")));

            assertThatThrownBy(() -> references.retire(
                            ledger.target().ledgerId(), 0, 0, proof(protection), TIMEOUT).join())
                    .hasRootCauseMessage("retirement authority rejected");
            assertThat(runtime.metadata.getProtection(
                            BookKeeperPrimaryWalAppenderTest.CLUSTER,
                            runtime.configuration.providerScopeSha256(),
                            ledger.target().ledgerId(),
                            0,
                            0)
                    .join()).get().satisfies(current ->
                            assertThat(current.value().lifecycle()).isEqualTo(ProtectionLifecycle.ACTIVE));
        }
    }

    @Test
    void exactAbandonedReservationCanRetireAReservedInventorySlot() {
        try (BookKeeperPrimaryWalAppenderTest.Runtime runtime = new BookKeeperPrimaryWalAppenderTest.Runtime()) {
            runtime.operations.failWriteCall = 2;
            assertThatThrownBy(() -> {
                try (BookKeeperPreparedPrimaryAppend prepared = runtime.appender.prepare(
                        BookKeeperPrimaryWalAppenderTest.request(
                                BookKeeperPrimaryWalAppenderTest.session(),
                                "attempt-abandoned-retirement",
                                0,
                                new byte[] {1},
                                new byte[] {2}))) {
                    runtime.appender.persist(prepared, TIMEOUT).join();
                }
            }).hasRootCauseInstanceOf(com.nereusstream.api.NereusException.class);
            var reservation = runtime.metadata.scanReservations(
                            BookKeeperPrimaryWalAppenderTest.CLUSTER,
                            BookKeeperPrimaryWalAppenderTest.STREAM,
                            Optional.empty(),
                            10)
                    .join().values().get(0);
            assertThat(reservation.value().lifecycle()).isEqualTo(AppendReservationLifecycle.ABANDONED);
            var protection = runtime.metadata.getProtection(
                            BookKeeperPrimaryWalAppenderTest.CLUSTER,
                            runtime.configuration.providerScopeSha256(),
                            reservation.value().ledgerId(),
                            reservation.value().ledgerRangeSlot(),
                            0)
                    .join().orElseThrow();
            assertThat(protection.value().lifecycle()).isEqualTo(ProtectionLifecycle.RESERVED);
            BookKeeperProtectionRetirementProof proof = new BookKeeperProtectionRetirementProof(
                    protection.key(),
                    protection.metadataVersion(),
                    protection.durableValueSha256(),
                    reservation.key(),
                    reservation.metadataVersion(),
                    reservation.durableValueSha256(),
                    "/retention/abandoned-authority",
                    reservation.metadataVersion(),
                    reservation.durableValueSha256(),
                    BookKeeperProtectionRetirementProof.Reason.ABANDONED_APPEND);
            BookKeeperWalReferenceManager references = new BookKeeperWalReferenceManager(
                    BookKeeperPrimaryWalAppenderTest.CLUSTER,
                    runtime.configuration,
                    runtime.metadata,
                    (retirementProof, current, timeout) -> {
                        assertThat(retirementProof.reason())
                                .isEqualTo(BookKeeperProtectionRetirementProof.Reason.ABANDONED_APPEND);
                        assertThat(current.value().lifecycle()).isIn(
                                ProtectionLifecycle.RESERVED, ProtectionLifecycle.RETIRED);
                        return CompletableFuture.completedFuture(null);
                    });

            var retired = references.retire(
                    reservation.value().ledgerId(),
                    reservation.value().ledgerRangeSlot(),
                    0,
                    proof,
                    TIMEOUT).join();
            assertThat(retired.value().lifecycle()).isEqualTo(ProtectionLifecycle.RETIRED);
            assertThat(retired.value().ownerKey()).isEqualTo(reservation.key());
            assertThat(retired.value().ownerIdentitySha256())
                    .isEqualTo(reservation.durableValueSha256().value());
        }
    }

    @Test
    void recoveryReconstructsMandatoryInventoryBeforeClearingWriterReservation() {
        try (BookKeeperPrimaryWalAppenderTest.Runtime runtime = new BookKeeperPrimaryWalAppenderTest.Runtime()) {
            runtime.metadata.failCreateProtectionCall(2);
            assertThatThrownBy(() -> {
                try (BookKeeperPreparedPrimaryAppend prepared = runtime.appender.prepare(
                        BookKeeperPrimaryWalAppenderTest.request(
                                BookKeeperPrimaryWalAppenderTest.session(),
                                "attempt-protection-cut",
                                0,
                                new byte[] {1}))) {
                    runtime.appender.persist(prepared, TIMEOUT).join();
                }
            }).hasRootCauseMessage("injected BookKeeper protection create failure");
            var reservation = runtime.metadata.scanReservations(
                            BookKeeperPrimaryWalAppenderTest.CLUSTER,
                            BookKeeperPrimaryWalAppenderTest.STREAM,
                            Optional.empty(),
                            10)
                    .join().values().get(0);
            assertThat(reservation.value().lifecycle()).isEqualTo(AppendReservationLifecycle.ABANDONED);
            for (int slot = 0; slot < 3; slot++) {
                assertThat(runtime.metadata.getProtection(
                                BookKeeperPrimaryWalAppenderTest.CLUSTER,
                                runtime.configuration.providerScopeSha256(),
                                reservation.value().ledgerId(),
                                reservation.value().ledgerRangeSlot(),
                                slot)
                        .join()).get().satisfies(protection -> {
                            assertThat(protection.value().lifecycle()).isEqualTo(ProtectionLifecycle.RESERVED);
                            assertThat(protection.value().referenceId())
                                    .isEqualTo(reservation.value().reservationId());
                        });
            }
            assertThat(runtime.metadata.getWriter(
                            BookKeeperPrimaryWalAppenderTest.CLUSTER,
                            BookKeeperPrimaryWalAppenderTest.STREAM)
                    .join()).get().satisfies(writer -> {
                        assertThat(writer.value().activeReservationId()).isEmpty();
                        assertThat(writer.value().lifecycle())
                                .isEqualTo(com.nereusstream.metadata.oxia.records.BookKeeperWriterLifecycle.IDLE);
                    });
        }
    }

    @Test
    void markDrainAndLostDeleteResponseConvergeThroughDualAbsence() {
        try (BookKeeperPrimaryWalAppenderTest.Runtime runtime = new BookKeeperPrimaryWalAppenderTest.Runtime()) {
            StableLedger ledger = appendCommitAndSeal(runtime);
            retireAll(runtime, ledger);
            MutableClock clock = new MutableClock(1_000);
            BookKeeperLedgerGcConfiguration gc = new BookKeeperLedgerGcConfiguration(
                    1,
                    Duration.ZERO,
                    Duration.ofMinutes(2),
                    Duration.ofSeconds(10),
                    true,
                    false);
            BookKeeperProtocolActivationProof activation = activation(runtime);
            BookKeeperWalRetentionGate gate = new BookKeeperWalRetentionGate(
                    BookKeeperPrimaryWalAppenderTest.CLUSTER,
                    runtime.configuration,
                    gc,
                    runtime.metadata,
                    runtime.metadata,
                    runtime.verifier,
                    timeout -> CompletableFuture.completedFuture(activation),
                    runtime.operations,
                    clock);
            BookKeeperLedgerRetentionManager manager = new BookKeeperLedgerRetentionManager(
                    BookKeeperPrimaryWalAppenderTest.CLUSTER,
                    runtime.configuration,
                    gc,
                    runtime.metadata,
                    runtime.verifier,
                    timeout -> CompletableFuture.completedFuture(activation),
                    runtime.operations,
                    gate,
                    clock);
            var candidate = gate.evaluate(ledger.sealedRoot(), TIMEOUT).join().candidate().orElseThrow();

            var marked = manager.mark(candidate, TIMEOUT).join();
            assertThat(marked.action()).isEqualTo(BookKeeperLedgerGcAction.MARKED);
            assertThat(manager.converge(marked.root().orElseThrow(), TIMEOUT).join().action())
                    .isEqualTo(BookKeeperLedgerGcAction.WAITING_DRAIN);

            clock.setMillis(121_000);
            var deleting = manager.converge(marked.root().orElseThrow(), TIMEOUT).join();
            assertThat(deleting.action()).isEqualTo(BookKeeperLedgerGcAction.DELETING);
            runtime.operations.failDeleteAfterRemoval = true;
            var firstAbsent = manager.converge(deleting.root().orElseThrow(), TIMEOUT).join();
            assertThat(firstAbsent.action()).isEqualTo(BookKeeperLedgerGcAction.FIRST_ABSENCE_RECORDED);
            assertThat(firstAbsent.root().orElseThrow().value().firstAbsentAtMillis()).isEqualTo(121_000);
            assertThat(manager.converge(firstAbsent.root().orElseThrow(), TIMEOUT).join().action())
                    .isEqualTo(BookKeeperLedgerGcAction.WAITING_SECOND_ABSENCE);

            clock.setMillis(131_001);
            var deleted = manager.converge(firstAbsent.root().orElseThrow(), TIMEOUT).join();
            assertThat(deleted.action()).isEqualTo(BookKeeperLedgerGcAction.DELETED);
            assertThat(deleted.root().orElseThrow().value().lifecycle())
                    .isEqualTo(com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle.DELETED);
        }
    }

    private static StableLedger appendCommitAndSeal(BookKeeperPrimaryWalAppenderTest.Runtime runtime) {
        var session = BookKeeperPrimaryWalAppenderTest.session();
        DurablePrimaryAppend durable;
        try (BookKeeperPreparedPrimaryAppend prepared = runtime.appender.prepare(
                BookKeeperPrimaryWalAppenderTest.request(
                        session, "attempt-retention", 10, new byte[] {1, 2}, new byte[] {3}))) {
            durable = runtime.appender.persist(prepared, TIMEOUT).join();
        }
        BookKeeperEntryRangeReadTarget target = (BookKeeperEntryRangeReadTarget) durable.readTarget();
        CommitAppendRequest commit = new CommitAppendRequest(
                BookKeeperPrimaryWalAppenderTest.STREAM,
                session.writerId(),
                "retention-process-run",
                session.epoch(),
                session.fencingToken(),
                10,
                target,
                PayloadFormat.OPAQUE_RECORD_BATCH,
                2,
                2,
                3,
                List.of(),
                1,
                2,
                Optional.empty());
        PreparedStableAppend stable = new PreparedStableAppend(
                commit,
                commit.commitId(),
                "/commit/retention-owner",
                7,
                sha('c'),
                com.nereusstream.api.ReadTargetIdentities.sha256(target),
                false);
        runtime.references.protectBeforeHead(stable, target, TIMEOUT).join();
        CommittedAppend committed = new CommittedAppend(
                BookKeeperPrimaryWalAppenderTest.STREAM,
                stable.commitId(),
                "",
                target,
                new OffsetRange(10, 12),
                0,
                3,
                1,
                PayloadFormat.OPAQUE_RECORD_BATCH,
                2,
                2,
                3,
                List.of(),
                Optional.empty(),
                1,
                2);
        runtime.references.protectVisibleIndex(
                new MaterializedGenerationZero(committed, "/index/retention-generation-zero", 8, sha('d')),
                target,
                TIMEOUT).join();
        var sealed = runtime.recovery.recoverWriter(session, TIMEOUT, "retention test seal").join().sealedRoot();
        return new StableLedger(target, sealed);
    }

    private static void retireAll(
            BookKeeperPrimaryWalAppenderTest.Runtime runtime,
            StableLedger ledger) {
        BookKeeperWalReferenceManager references = new BookKeeperWalReferenceManager(
                BookKeeperPrimaryWalAppenderTest.CLUSTER,
                runtime.configuration,
                runtime.metadata,
                (proof, protection, timeout) -> CompletableFuture.completedFuture(null));
        for (int protectionSlot = 0; protectionSlot < 3; protectionSlot++) {
            var protection = runtime.metadata.getProtection(
                            BookKeeperPrimaryWalAppenderTest.CLUSTER,
                            runtime.configuration.providerScopeSha256(),
                            ledger.target().ledgerId(),
                            0,
                            protectionSlot)
                    .join().orElseThrow();
            references.retire(
                    ledger.target().ledgerId(), 0, protectionSlot, proof(protection), TIMEOUT).join();
        }
    }

    private static BookKeeperWalRetentionGate gate(
            BookKeeperPrimaryWalAppenderTest.Runtime runtime,
            BookKeeperProtocolActivationProof activation) {
        return new BookKeeperWalRetentionGate(
                BookKeeperPrimaryWalAppenderTest.CLUSTER,
                runtime.configuration,
                BookKeeperLedgerGcConfiguration.safeDefault(),
                runtime.metadata,
                runtime.metadata,
                runtime.verifier,
                timeout -> CompletableFuture.completedFuture(activation),
                runtime.operations,
                BookKeeperPrimaryWalAppenderTest.CLOCK);
    }

    private static BookKeeperProtocolActivationProof activation(
            BookKeeperPrimaryWalAppenderTest.Runtime runtime) {
        var namespace = BookKeeperPrimaryWalAppenderTest.reservation(runtime.configuration);
        return new BookKeeperProtocolActivationProof(
                1,
                runtime.configuration.clusterAlias(),
                runtime.configuration.providerScopeSha256(),
                runtime.configuration.configurationBindingSha256().value(),
                namespace.ledgerIdNamespaceSha256().value(),
                1,
                sha('1'),
                sha('2'),
                sha('3'),
                sha('4'),
                true,
                9,
                sha('5'));
    }

    private static BookKeeperProtectionRetirementProof proof(
            BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> protection) {
        return new BookKeeperProtectionRetirementProof(
                protection.key(),
                protection.metadataVersion(),
                protection.durableValueSha256(),
                protection.value().ownerKey(),
                protection.value().ownerMetadataVersion(),
                new Checksum(ChecksumType.SHA256, protection.value().ownerIdentitySha256()),
                "/retention/authority",
                4,
                sha('a'),
                BookKeeperProtectionRetirementProof.Reason.LOGICAL_TRIM);
    }

    private static Checksum sha(char value) {
        return new Checksum(ChecksumType.SHA256, Character.toString(value).repeat(64));
    }

    private record StableLedger(
            BookKeeperEntryRangeReadTarget target,
            BookKeeperVersionedValue<com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord>
                    sealedRoot) { }

    private static final class MutableClock extends Clock {
        private long millis;

        private MutableClock(long millis) {
            this.millis = millis;
        }

        private void setMillis(long millis) {
            this.millis = millis;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            if (!ZoneOffset.UTC.equals(zone)) throw new IllegalArgumentException("test clock is UTC");
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }
    }
}
