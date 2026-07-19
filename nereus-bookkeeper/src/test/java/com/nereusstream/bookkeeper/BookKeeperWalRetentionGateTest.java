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
import com.nereusstream.materialization.CommittedGenerationRetirementAuthority;
import com.nereusstream.materialization.CommittedGenerationRetirementProof;
import com.nereusstream.metadata.oxia.BookKeeperLedgerMetadataStore;
import com.nereusstream.metadata.oxia.BookKeeperScanPage;
import com.nereusstream.metadata.oxia.BookKeeperVersionedValue;
import com.nereusstream.metadata.oxia.BookKeeperWriterMetadataStore;
import com.nereusstream.metadata.oxia.CommitAppendRequest;
import com.nereusstream.metadata.oxia.CommittedAppend;
import com.nereusstream.metadata.oxia.MaterializedGenerationZero;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.PreparedStableAppend;
import com.nereusstream.metadata.oxia.ResponseLossPartitionedOxiaBackend;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.records.AppendReservationLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerProtectionRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerReaderLeaseRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperProtectionType;
import com.nereusstream.metadata.oxia.records.BookKeeperWriterStateRecord;
import com.nereusstream.metadata.oxia.records.CommittedEndOffsetRecord;
import com.nereusstream.metadata.oxia.records.ProtectionLifecycle;
import com.nereusstream.metadata.oxia.records.StreamMetadataRecord;
import com.nereusstream.metadata.oxia.records.TrimRecord;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;
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
            BookKeeperWalOnlyRetirementAuthority authority = new BookKeeperWalOnlyRetirementAuthority(
                    BookKeeperPrimaryWalAppenderTest.CLUSTER,
                    l0(new AtomicReference<>(snapshot(0, 7))),
                    runtime.metadata);
            BookKeeperProtectionRetirementProof proof = authority.proveAbandonedAppend(protection, TIMEOUT)
                    .join().orElseThrow();
            BookKeeperWalReferenceManager references = new BookKeeperWalReferenceManager(
                    BookKeeperPrimaryWalAppenderTest.CLUSTER,
                    runtime.configuration,
                    runtime.metadata,
                    authority);

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
    void completedLogicalTrimProofIsExactRefreshableAndRetiresTheProtection() {
        try (BookKeeperPrimaryWalAppenderTest.Runtime runtime = new BookKeeperPrimaryWalAppenderTest.Runtime()) {
            StableLedger ledger = appendCommitAndSeal(runtime);
            var protection = runtime.metadata.getProtection(
                            BookKeeperPrimaryWalAppenderTest.CLUSTER,
                            runtime.configuration.providerScopeSha256(),
                            ledger.target().ledgerId(),
                            0,
                            0)
                    .join().orElseThrow();
            AtomicReference<StreamMetadataSnapshot> snapshot = new AtomicReference<>(snapshot(12, 7));
            BookKeeperWalOnlyRetirementAuthority authority = new BookKeeperWalOnlyRetirementAuthority(
                    BookKeeperPrimaryWalAppenderTest.CLUSTER,
                    l0(snapshot),
                    runtime.metadata);
            BookKeeperWalReferenceManager references = new BookKeeperWalReferenceManager(
                    BookKeeperPrimaryWalAppenderTest.CLUSTER,
                    runtime.configuration,
                    runtime.metadata,
                    authority);
            BookKeeperProtectionRetirementProof stale = authority.proveLogicalTrim(protection, TIMEOUT)
                    .join().orElseThrow();
            snapshot.set(snapshot(12, 8));

            assertThatThrownBy(() -> references.retire(
                            ledger.target().ledgerId(), 0, 0, stale, TIMEOUT).join())
                    .hasRootCauseInstanceOf(com.nereusstream.api.NereusException.class);
            assertThat(runtime.metadata.getProtection(
                            BookKeeperPrimaryWalAppenderTest.CLUSTER,
                            runtime.configuration.providerScopeSha256(),
                            ledger.target().ledgerId(),
                            0,
                            0)
                    .join()).get().satisfies(current ->
                            assertThat(current.value().lifecycle()).isEqualTo(ProtectionLifecycle.ACTIVE));

            BookKeeperProtectionRetirementProof refreshed = authority.proveLogicalTrim(protection, TIMEOUT)
                    .join().orElseThrow();
            var retired = references.retire(
                    ledger.target().ledgerId(), 0, 0, refreshed, TIMEOUT).join();
            assertThat(retired.value().lifecycle()).isEqualTo(ProtectionLifecycle.RETIRED);
        }
    }

    @Test
    void healthyCommittedObjectGenerationRetiresOnlyMandatoryAsyncSourceReferences() {
        try (BookKeeperPrimaryWalAppenderTest.Runtime runtime = new BookKeeperPrimaryWalAppenderTest.Runtime()) {
            StableLedger ledger = appendCommitAndSeal(runtime);
            AtomicInteger proveCalls = new AtomicInteger();
            AtomicInteger exactCalls = new AtomicInteger();
            Checksum indexSha = sha('9');
            CommittedGenerationRetirementAuthority replacements =
                    new CommittedGenerationRetirementAuthority() {
                        @Override
                        public CompletableFuture<Optional<CommittedGenerationRetirementProof>> proveRetirement(
                                com.nereusstream.api.StreamId streamId,
                                OffsetRange sourceRange,
                                long sourceCommitVersion) {
                            proveCalls.incrementAndGet();
                            return CompletableFuture.completedFuture(Optional.of(
                                    replacementProof(
                                            streamId, sourceRange, sourceCommitVersion, indexSha)));
                        }

                        @Override
                        public CompletableFuture<Optional<CommittedGenerationRetirementProof>>
                                proveExactRetirement(
                                        com.nereusstream.api.StreamId streamId,
                                        OffsetRange sourceRange,
                                        long sourceCommitVersion,
                                        String indexKey,
                                        long indexMetadataVersion,
                                        Checksum indexSha256) {
                            exactCalls.incrementAndGet();
                            CommittedGenerationRetirementProof proof = replacementProof(
                                    streamId, sourceRange, sourceCommitVersion, indexSha);
                            return CompletableFuture.completedFuture(
                                    proof.indexKey().equals(indexKey)
                                                    && proof.indexMetadataVersion() == indexMetadataVersion
                                                    && proof.indexSha256().equals(indexSha256)
                                            ? Optional.of(proof)
                                            : Optional.empty());
                        }

                        @Override
                        public CompletableFuture<Void> revalidateRetirement(
                                CommittedGenerationRetirementProof expected) {
                            return CompletableFuture.completedFuture(null);
                        }
                    };
            BookKeeperWalOnlyRetirementAuthority common = new BookKeeperWalOnlyRetirementAuthority(
                    BookKeeperPrimaryWalAppenderTest.CLUSTER,
                    l0(new AtomicReference<>(snapshot(0, 7))),
                    runtime.metadata);
            BookKeeperAsyncObjectRetirementAuthority authority =
                    new BookKeeperAsyncObjectRetirementAuthority(
                            BookKeeperPrimaryWalAppenderTest.CLUSTER,
                            runtime.configuration,
                            common,
                            replacements,
                            runtime.metadata);
            BookKeeperWalReferenceManager references = new BookKeeperWalReferenceManager(
                    BookKeeperPrimaryWalAppenderTest.CLUSTER,
                    runtime.configuration,
                    runtime.metadata,
                    authority);
            BookKeeperWalOnlyReferenceRetirementCoordinator coordinator =
                    new BookKeeperWalOnlyReferenceRetirementCoordinator(
                            BookKeeperPrimaryWalAppenderTest.CLUSTER,
                            runtime.configuration,
                            runtime.metadata,
                            authority,
                            references);

            BookKeeperWalReferenceRetirementResult result = coordinator.retireEligible(
                    ledger.sealedRoot(), TIMEOUT).join();

            assertThat(result.scannedProtections()).isEqualTo(3);
            assertThat(result.newlyRetiredProtections()).isEqualTo(3);
            assertThat(result.fullyRetired()).isTrue();
            assertThat(proveCalls).hasValue(3);
            assertThat(exactCalls).hasValue(6);
            assertThat(gate(runtime, activation(runtime)).evaluate(ledger.sealedRoot(), TIMEOUT)
                            .join().blockers())
                    .isEmpty();
        }
    }

    @Test
    void sealedLedgerTriggerRevalidatesExactRootAndUsesTheSharedMaterializationScanner() {
        try (BookKeeperPrimaryWalAppenderTest.Runtime runtime = new BookKeeperPrimaryWalAppenderTest.Runtime()) {
            StableLedger ledger = appendCommitAndSeal(runtime);
            AtomicReference<com.nereusstream.api.StreamId> triggered = new AtomicReference<>();
            BookKeeperSealedLedgerMaterializationTrigger trigger =
                    new BookKeeperSealedLedgerMaterializationTrigger(
                            BookKeeperPrimaryWalAppenderTest.CLUSTER,
                            runtime.configuration,
                            runtime.metadata,
                            streamId -> {
                                triggered.set(streamId);
                                return CompletableFuture.completedFuture(null);
                            });

            trigger.trigger(ledger.sealedRoot(), TIMEOUT).join();

            assertThat(triggered).hasValue(BookKeeperPrimaryWalAppenderTest.STREAM);
            assertThat(runtime.metadata.getRoot(
                            BookKeeperPrimaryWalAppenderTest.CLUSTER,
                            runtime.configuration.providerScopeSha256(),
                            ledger.target().ledgerId()).join())
                    .contains(ledger.sealedRoot());
        }
    }

    private static CommittedGenerationRetirementProof replacementProof(
            com.nereusstream.api.StreamId streamId,
            OffsetRange sourceRange,
            long sourceCommitVersion,
            Checksum indexSha) {
        return new CommittedGenerationRetirementProof(
                streamId,
                sourceRange,
                sourceCommitVersion,
                "/f4/committed-object-index",
                0,
                indexSha);
    }

    @Test
    void failsClosedOnIncompleteAuthority() {
        try (BookKeeperPrimaryWalAppenderTest.Runtime runtime = new BookKeeperPrimaryWalAppenderTest.Runtime()) {
            StableLedger ledger = appendCommitAndSeal(runtime);
            retireAll(runtime, ledger);
            var canonical = runtime.metadata.getProtection(
                            BookKeeperPrimaryWalAppenderTest.CLUSTER,
                            runtime.configuration.providerScopeSha256(),
                            ledger.target().ledgerId(),
                            0,
                            0)
                    .join().orElseThrow();
            BookKeeperLedgerProtectionRecord outsideCartesian = copyProtection(
                    canonical.value(),
                    runtime.configuration.maxAppendRangesPerLedger(),
                    0,
                    BookKeeperProtectionType.REACHABLE_APPEND,
                    ProtectionLifecycle.RETIRED);

            assertThatThrownBy(() -> runtime.metadata.createProtection(
                            BookKeeperPrimaryWalAppenderTest.CLUSTER,
                            runtime.configuration.providerScopeSha256(),
                            outsideCartesian)
                    .join())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("rangeSlot is outside its configured bound");

            BookKeeperLedgerMetadataStore missingInventory = protectionScanView(
                    runtime,
                    values -> values.subList(0, values.size() - 1));
            assertThat(gate(runtime, activation(runtime), runtime.configuration, missingInventory, runtime.metadata)
                            .evaluate(ledger.sealedRoot(), TIMEOUT).join().blockers())
                    .containsExactly(BookKeeperRetentionBlocker.PROTECTION_PRESENT);

            BookKeeperWalConfiguration narrowInventory = withInventoryBounds(runtime.configuration, 1, 4);
            BookKeeperLedgerMetadataStore oversizedInventory = protectionScanView(
                    runtime,
                    values -> List.of(values.get(0), values.get(0), values.get(1), values.get(1), values.get(2)));
            assertThat(gate(runtime, activation(narrowInventory), narrowInventory, oversizedInventory, runtime.metadata)
                            .evaluate(ledger.sealedRoot(), TIMEOUT).join().blockers())
                    .contains(
                            BookKeeperRetentionBlocker.INVENTORY_LIMIT_EXCEEDED,
                            BookKeeperRetentionBlocker.PROTECTION_PRESENT);
        }
    }

    @Test
    void enforcesEveryVetoDomain() {
        for (int activeMandatorySlot = 0; activeMandatorySlot < 3; activeMandatorySlot++) {
            try (BookKeeperPrimaryWalAppenderTest.Runtime runtime = new BookKeeperPrimaryWalAppenderTest.Runtime()) {
                StableLedger ledger = appendCommitAndSeal(runtime);
                retireMandatoryExcept(runtime, ledger, activeMandatorySlot);
                assertThat(gate(runtime, activation(runtime)).evaluate(ledger.sealedRoot(), TIMEOUT)
                                .join().blockers())
                        .containsExactly(BookKeeperRetentionBlocker.PROTECTION_PRESENT);
            }
        }

        assertAdditionalProtectionVeto(BookKeeperProtectionType.MATERIALIZATION_SOURCE, 3);
        assertAdditionalProtectionVeto(BookKeeperProtectionType.REPAIR, 4);

        try (BookKeeperPrimaryWalAppenderTest.Runtime runtime = new BookKeeperPrimaryWalAppenderTest.Runtime()) {
            StableLedger ledger = appendCommitAndSeal(runtime);
            retireAll(runtime, ledger);
            BookKeeperReaderLeaseManager.Lease lease = runtime.readerLeases.claim(ledger.sealedRoot(), TIMEOUT).join();
            assertThat(gate(runtime, activation(runtime)).evaluate(ledger.sealedRoot(), TIMEOUT)
                            .join().blockers())
                    .containsExactly(BookKeeperRetentionBlocker.READER_LEASE_PRESENT);
            lease.release().join();
        }

        try (BookKeeperPrimaryWalAppenderTest.Runtime runtime = new BookKeeperPrimaryWalAppenderTest.Runtime()) {
            StableLedger ledger = appendCommitAndSeal(runtime);
            retireAll(runtime, ledger);
            BookKeeperWriterMetadataStore selectedWriter = writerView(runtime, ledger.activeWriter());
            assertThat(gate(runtime, activation(runtime), runtime.configuration, runtime.metadata, selectedWriter)
                            .evaluate(ledger.sealedRoot(), TIMEOUT).join().blockers())
                    .containsExactly(BookKeeperRetentionBlocker.WRITER_SELECTS_LEDGER);
        }
    }

    @Test
    void referenceAppearingAfterMarkUnmarksToSealedBeforeDelete() {
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

            BookKeeperLedgerReaderLeaseRecord lateReader = new BookKeeperLedgerReaderLeaseRecord(
                    1,
                    ledger.sealedRoot().value().ledgerIdentitySha256(),
                    ledger.target().ledgerId(),
                    ledger.sealedRoot().value().lifecycleEpoch(),
                    0,
                    "reader-racing-mark/0",
                    1,
                    1_000,
                    301_000,
                    0);
            runtime.metadata.createReaderLease(
                            BookKeeperPrimaryWalAppenderTest.CLUSTER,
                            runtime.configuration.providerScopeSha256(),
                            lateReader)
                    .join();

            clock.setMillis(121_001);
            var unmarked = manager.converge(marked.root().orElseThrow(), TIMEOUT).join();
            assertThat(unmarked.action()).isEqualTo(BookKeeperLedgerGcAction.UNMARKED);
            assertThat(unmarked.root().orElseThrow().value().lifecycle())
                    .isEqualTo(com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle.SEALED);
            assertThat(runtime.operations.metadata(
                            ledger.target().ledgerId(), new BookKeeperOperationDeadline(TIMEOUT))
                    .join().getLedgerId()).isEqualTo(ledger.target().ledgerId());
        }
    }

    @Test
    void disabledAndDryRunGcNeverMutateRootOrProvider() {
        try (BookKeeperPrimaryWalAppenderTest.Runtime runtime = new BookKeeperPrimaryWalAppenderTest.Runtime()) {
            StableLedger ledger = appendCommitAndSeal(runtime);
            retireAll(runtime, ledger);
            BookKeeperProtocolActivationProof activation = activation(runtime);
            BookKeeperWalRetentionGate gate = gate(runtime, activation);
            var candidate = gate.evaluate(ledger.sealedRoot(), TIMEOUT).join().candidate().orElseThrow();
            int providerCalls = runtime.operations.providerCalls();

            BookKeeperLedgerGcConfiguration disabled = BookKeeperLedgerGcConfiguration.safeDefault();
            BookKeeperLedgerRetentionManager disabledManager = new BookKeeperLedgerRetentionManager(
                    BookKeeperPrimaryWalAppenderTest.CLUSTER,
                    runtime.configuration,
                    disabled,
                    runtime.metadata,
                    runtime.verifier,
                    timeout -> CompletableFuture.completedFuture(activation),
                    runtime.operations,
                    gate,
                    BookKeeperPrimaryWalAppenderTest.CLOCK);
            assertThat(disabledManager.mark(candidate, TIMEOUT).join().action())
                    .isEqualTo(BookKeeperLedgerGcAction.DISABLED);
            assertThat(disabledManager.converge(ledger.sealedRoot(), TIMEOUT).join().action())
                    .isEqualTo(BookKeeperLedgerGcAction.DISABLED);

            BookKeeperLedgerGcConfiguration dryRun = new BookKeeperLedgerGcConfiguration(
                    1,
                    Duration.ZERO,
                    Duration.ofMinutes(2),
                    Duration.ofSeconds(10),
                    true,
                    true);
            BookKeeperLedgerRetentionManager dryRunManager = new BookKeeperLedgerRetentionManager(
                    BookKeeperPrimaryWalAppenderTest.CLUSTER,
                    runtime.configuration,
                    dryRun,
                    runtime.metadata,
                    runtime.verifier,
                    timeout -> CompletableFuture.completedFuture(activation),
                    runtime.operations,
                    gate,
                    BookKeeperPrimaryWalAppenderTest.CLOCK);
            assertThat(dryRunManager.mark(candidate, TIMEOUT).join().action())
                    .isEqualTo(BookKeeperLedgerGcAction.DRY_RUN_ADMITTED);
            assertThat(dryRunManager.converge(ledger.sealedRoot(), TIMEOUT).join().action())
                    .isEqualTo(BookKeeperLedgerGcAction.DRY_RUN_ADMITTED);

            assertThat(runtime.operations.providerCalls()).isEqualTo(providerCalls);
            assertThat(runtime.metadata.getRoot(
                            BookKeeperPrimaryWalAppenderTest.CLUSTER,
                            runtime.configuration.providerScopeSha256(),
                            ledger.target().ledgerId())
                    .join()).contains(ledger.sealedRoot());
            assertThat(runtime.operations.metadata(
                            ledger.target().ledgerId(), new BookKeeperOperationDeadline(TIMEOUT))
                    .join().getLedgerId()).isEqualTo(ledger.target().ledgerId());
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

    @Test
    void everyGcRootCasConvergesAfterAppliedResponseLoss() {
        for (int lostRootCas = 1; lostRootCas <= 4; lostRootCas++) {
            ResponseLossPartitionedOxiaBackend backend = new ResponseLossPartitionedOxiaBackend();
            try (BookKeeperPrimaryWalAppenderTest.Runtime runtime =
                    new BookKeeperPrimaryWalAppenderTest.Runtime(backend)) {
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
                backend.loseResponse(ResponseLossPartitionedOxiaBackend.Operation.PUT_IF_VERSION, lostRootCas);

                var marked = manager.mark(candidate, TIMEOUT).join();
                assertThat(marked.action()).isEqualTo(BookKeeperLedgerGcAction.MARKED);
                clock.setMillis(121_001);
                var deleting = manager.converge(marked.root().orElseThrow(), TIMEOUT).join();
                assertThat(deleting.action()).isEqualTo(BookKeeperLedgerGcAction.DELETING);
                var firstAbsent = manager.converge(deleting.root().orElseThrow(), TIMEOUT).join();
                assertThat(firstAbsent.action()).isEqualTo(BookKeeperLedgerGcAction.FIRST_ABSENCE_RECORDED);

                clock.setMillis(131_002);
                var deleted = manager.converge(firstAbsent.root().orElseThrow(), TIMEOUT).join();
                assertThat(deleted.action()).isEqualTo(BookKeeperLedgerGcAction.DELETED);
                assertThat(deleted.root().orElseThrow().value().lifecycle())
                        .isEqualTo(com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle.DELETED);
                assertThat(backend.responseWasLost()).isTrue();
            }
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
        BookKeeperVersionedValue<BookKeeperWriterStateRecord> activeWriter = runtime.metadata.getWriter(
                        BookKeeperPrimaryWalAppenderTest.CLUSTER,
                        BookKeeperPrimaryWalAppenderTest.STREAM)
                .join().orElseThrow();
        var sealed = runtime.recovery.recoverWriter(session, TIMEOUT, "retention test seal").join().sealedRoot();
        return new StableLedger(target, sealed, activeWriter);
    }

    private static void retireAll(
            BookKeeperPrimaryWalAppenderTest.Runtime runtime,
            StableLedger ledger) {
        BookKeeperWalOnlyRetirementAuthority authority = new BookKeeperWalOnlyRetirementAuthority(
                BookKeeperPrimaryWalAppenderTest.CLUSTER,
                l0(new AtomicReference<>(snapshot(12, 7))),
                runtime.metadata);
        BookKeeperWalReferenceManager references = new BookKeeperWalReferenceManager(
                BookKeeperPrimaryWalAppenderTest.CLUSTER,
                runtime.configuration,
                runtime.metadata,
                authority);
        BookKeeperWalOnlyReferenceRetirementCoordinator coordinator =
                new BookKeeperWalOnlyReferenceRetirementCoordinator(
                        BookKeeperPrimaryWalAppenderTest.CLUSTER,
                        runtime.configuration,
                        runtime.metadata,
                        authority,
                        references);
        BookKeeperWalReferenceRetirementResult result = coordinator.retireEligible(
                ledger.sealedRoot(), TIMEOUT).join();
        assertThat(result.scannedProtections()).isEqualTo(3);
        assertThat(result.newlyRetiredProtections()).isEqualTo(3);
        assertThat(result.fullyRetired()).isTrue();
    }

    private static BookKeeperWalRetentionGate gate(
            BookKeeperPrimaryWalAppenderTest.Runtime runtime,
            BookKeeperProtocolActivationProof activation) {
        return gate(runtime, activation, runtime.configuration, runtime.metadata, runtime.metadata);
    }

    private static BookKeeperWalRetentionGate gate(
            BookKeeperPrimaryWalAppenderTest.Runtime runtime,
            BookKeeperProtocolActivationProof activation,
            BookKeeperWalConfiguration configuration,
            BookKeeperLedgerMetadataStore ledgerMetadata,
            BookKeeperWriterMetadataStore writerMetadata) {
        return new BookKeeperWalRetentionGate(
                BookKeeperPrimaryWalAppenderTest.CLUSTER,
                configuration,
                BookKeeperLedgerGcConfiguration.safeDefault(),
                writerMetadata,
                ledgerMetadata,
                runtime.verifier,
                timeout -> CompletableFuture.completedFuture(activation),
                runtime.operations,
                BookKeeperPrimaryWalAppenderTest.CLOCK);
    }

    private static BookKeeperProtocolActivationProof activation(
            BookKeeperPrimaryWalAppenderTest.Runtime runtime) {
        return activation(runtime.configuration);
    }

    private static BookKeeperProtocolActivationProof activation(BookKeeperWalConfiguration configuration) {
        var namespace = BookKeeperPrimaryWalAppenderTest.reservation(configuration);
        return new BookKeeperProtocolActivationProof(
                1,
                configuration.clusterAlias(),
                configuration.providerScopeSha256(),
                configuration.configurationBindingSha256().value(),
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

    private static void retireMandatoryExcept(
            BookKeeperPrimaryWalAppenderTest.Runtime runtime,
            StableLedger ledger,
            int retainedSlot) {
        BookKeeperWalReferenceManager references = new BookKeeperWalReferenceManager(
                BookKeeperPrimaryWalAppenderTest.CLUSTER,
                runtime.configuration,
                runtime.metadata,
                (retirementProof, protection, timeout) -> CompletableFuture.completedFuture(null));
        for (int slot = 0; slot < 3; slot++) {
            if (slot == retainedSlot) continue;
            var protection = runtime.metadata.getProtection(
                            BookKeeperPrimaryWalAppenderTest.CLUSTER,
                            runtime.configuration.providerScopeSha256(),
                            ledger.target().ledgerId(),
                            0,
                            slot)
                    .join().orElseThrow();
            references.retire(ledger.target().ledgerId(), 0, slot, proof(protection), TIMEOUT).join();
        }
    }

    private static void assertAdditionalProtectionVeto(BookKeeperProtectionType type, int slot) {
        try (BookKeeperPrimaryWalAppenderTest.Runtime runtime = new BookKeeperPrimaryWalAppenderTest.Runtime()) {
            StableLedger ledger = appendCommitAndSeal(runtime);
            retireAll(runtime, ledger);
            var canonical = runtime.metadata.getProtection(
                            BookKeeperPrimaryWalAppenderTest.CLUSTER,
                            runtime.configuration.providerScopeSha256(),
                            ledger.target().ledgerId(),
                            0,
                            0)
                    .join().orElseThrow();
            runtime.metadata.createProtection(
                            BookKeeperPrimaryWalAppenderTest.CLUSTER,
                            runtime.configuration.providerScopeSha256(),
                            copyProtection(canonical.value(), 0, slot, type, ProtectionLifecycle.ACTIVE))
                    .join();

            assertThat(gate(runtime, activation(runtime)).evaluate(ledger.sealedRoot(), TIMEOUT)
                            .join().blockers())
                    .containsExactly(BookKeeperRetentionBlocker.PROTECTION_PRESENT);
        }
    }

    private static BookKeeperLedgerProtectionRecord copyProtection(
            BookKeeperLedgerProtectionRecord value,
            int rangeSlot,
            int protectionSlot,
            BookKeeperProtectionType type,
            ProtectionLifecycle lifecycle) {
        long expiresAtMillis = type == BookKeeperProtectionType.REPAIR
                ? Math.addExact(value.createdAtMillis(), 60_000)
                : 0;
        return new BookKeeperLedgerProtectionRecord(
                value.schemaVersion(), value.ledgerIdentitySha256(), value.clusterAlias(), value.ledgerId(),
                value.rootLifecycleEpoch(), rangeSlot, protectionSlot, type.wireId(),
                type.name().toLowerCase() + "-retention-veto", value.firstEntryId(), value.entryCount(),
                value.rangeChecksumSha256(), value.streamId(), value.offsetStart(), value.offsetEnd(),
                value.commitVersion(), "/retention-owner/" + type.name().toLowerCase(), 17,
                sha('e').value(), lifecycle, value.createdAtMillis(), expiresAtMillis, 0);
    }

    private static BookKeeperWalConfiguration withInventoryBounds(
            BookKeeperWalConfiguration value,
            int maxAppendRanges,
            int protectionSlots) {
        return new BookKeeperWalConfiguration(
                value.clusterAlias(), value.providerScopeSha256(), value.ledgerIdPrefixBits(),
                value.ledgerIdPrefixValue(), value.ledgerIdNamespaceReservationId(), value.ensembleSize(),
                value.writeQuorumSize(), value.ackQuorumSize(), value.digestType(), value.passwordRef(),
                value.maxEntriesPerLedger(), value.maxBytesPerLedger(), maxAppendRanges, protectionSlots,
                value.maxReaderLeasesPerLedger(), value.maxUncertainAllocations(), value.maxLedgerAge(),
                value.maxWritesInFlight(), value.maxReadsInFlight(), value.maxReadBytesInFlight(),
                value.operationTimeout(), value.allocationTimeout(), value.sealTimeout(), value.deleteTimeout(),
                value.readerLeaseTtl(), value.readerLeaseRenewInterval(), value.retentionScanInterval(),
                value.retentionPageSize());
    }

    @SuppressWarnings("unchecked")
    private static BookKeeperLedgerMetadataStore protectionScanView(
            BookKeeperPrimaryWalAppenderTest.Runtime runtime,
            UnaryOperator<List<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>>> transformation) {
        return (BookKeeperLedgerMetadataStore) Proxy.newProxyInstance(
                BookKeeperLedgerMetadataStore.class.getClassLoader(),
                new Class<?>[] {BookKeeperLedgerMetadataStore.class},
                (proxy, method, args) -> {
                    Object result = invoke(runtime.metadata, method, args);
                    if (!method.getName().equals("scanProtections")) return result;
                    return ((CompletableFuture<BookKeeperScanPage<
                                    BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>>>) result)
                            .thenApply(page -> new BookKeeperScanPage<>(
                                    transformation.apply(page.values()), page.continuation()));
                });
    }

    private static BookKeeperWriterMetadataStore writerView(
            BookKeeperPrimaryWalAppenderTest.Runtime runtime,
            BookKeeperVersionedValue<BookKeeperWriterStateRecord> writer) {
        return (BookKeeperWriterMetadataStore) Proxy.newProxyInstance(
                BookKeeperWriterMetadataStore.class.getClassLoader(),
                new Class<?>[] {BookKeeperWriterMetadataStore.class},
                (proxy, method, args) -> method.getName().equals("getWriter")
                        ? CompletableFuture.completedFuture(Optional.of(writer))
                        : invoke(runtime.metadata, method, args));
    }

    private static Object invoke(Object target, java.lang.reflect.Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException failure) {
            throw failure.getCause();
        }
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

    private static StreamMetadataSnapshot snapshot(long trimOffset, long metadataVersion) {
        return new StreamMetadataSnapshot(
                new StreamMetadataRecord(
                        BookKeeperPrimaryWalAppenderTest.STREAM.value(),
                        "persistent://tenant/namespace/topic",
                        "stream-name-hash",
                        "ACTIVE",
                        "BOOKKEEPER_WAL_ONLY",
                        Map.of("profile", "bk-only"),
                        1,
                        1,
                        metadataVersion),
                new CommittedEndOffsetRecord(
                        BookKeeperPrimaryWalAppenderTest.STREAM.value(), 12, 3, 1, metadataVersion),
                new TrimRecord(
                        BookKeeperPrimaryWalAppenderTest.STREAM.value(),
                        trimOffset,
                        "retention-test",
                        1,
                        metadataVersion));
    }

    private static OxiaMetadataStore l0(AtomicReference<StreamMetadataSnapshot> snapshot) {
        return (OxiaMetadataStore) Proxy.newProxyInstance(
                OxiaMetadataStore.class.getClassLoader(),
                new Class<?>[] {OxiaMetadataStore.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getStreamSnapshot" -> CompletableFuture.completedFuture(snapshot.get());
                    case "close" -> null;
                    case "toString" -> "trim-authority-l0";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private record StableLedger(
            BookKeeperEntryRangeReadTarget target,
            BookKeeperVersionedValue<com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord>
                    sealedRoot,
            BookKeeperVersionedValue<BookKeeperWriterStateRecord> activeWriter) { }

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
