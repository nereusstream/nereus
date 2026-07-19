/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.AppendSession;
import com.nereusstream.metadata.oxia.BookKeeperMetadataStoreConfig;
import com.nereusstream.metadata.oxia.FakeBookKeeperMetadataStore;
import com.nereusstream.metadata.oxia.ResponseLossPartitionedOxiaBackend;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperWriterLifecycle;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class BookKeeperLedgerRecoveryTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    @Test
    void recoversEverySealCut() {
        for (int lostCas = 1; lostCas <= 4; lostCas++) {
            int exactLostCas = lostCas;
            ResponseLossPartitionedOxiaBackend backend = new ResponseLossPartitionedOxiaBackend();
            try (Fixture fixture = new Fixture(backend, "seal-cut-" + exactLostCas)) {
                AppendSession oldOwner = BookKeeperPrimaryWalAppenderTest.session();
                var allocated = fixture.allocator.allocate(new BookKeeperLedgerAllocationRequest(
                                BookKeeperPrimaryWalAppenderTest.STREAM, oldOwner, TIMEOUT))
                        .join();
                long ledgerId = allocated.root().value().ledgerId();
                AppendSession newOwner = new AppendSession(
                        oldOwner.streamId(), "writer-2", 2, "token-2", 2, 20_000);

                backend.loseResponse(ResponseLossPartitionedOxiaBackend.Operation.PUT_IF_VERSION, exactLostCas);
                var recovered = fixture.recovery
                        .recoverWriter(newOwner, TIMEOUT, "injected seal cut " + exactLostCas)
                        .join();
                assertThat(backend.responseWasLost()).isTrue();

                var convergedWriter = fixture.metadata.getWriter(
                                BookKeeperPrimaryWalAppenderTest.CLUSTER,
                                BookKeeperPrimaryWalAppenderTest.STREAM)
                        .join()
                        .orElseThrow();
                var sealed = fixture.metadata.getRoot(
                                BookKeeperPrimaryWalAppenderTest.CLUSTER,
                                fixture.configuration.providerScopeSha256(),
                                ledgerId)
                        .join()
                        .orElseThrow();
                assertThat(convergedWriter.value().lifecycle()).isEqualTo(BookKeeperWriterLifecycle.IDLE);
                assertThat(sealed.value().lifecycle()).isEqualTo(BookKeeperLedgerLifecycle.SEALED);
                assertThat(recovered.writer()).isEqualTo(convergedWriter);
                assertThat(recovered.sealedRoot()).isEqualTo(sealed);
                assertThat(sealed.value().sealedLastEntryId()).isEqualTo(-1);
                assertThat(sealed.value().sealedLength()).isZero();
                assertThat(sealed.value().lifecycleEpoch()).isEqualTo(4);
            }
        }
    }

    @Test
    void serializesTwoRecoveryOwners() {
        try (Fixture fixture = new Fixture(new ResponseLossPartitionedOxiaBackend(), "recovery-contenders")) {
            AppendSession oldOwner = BookKeeperPrimaryWalAppenderTest.session();
            var original = fixture.allocator.allocate(new BookKeeperLedgerAllocationRequest(
                            BookKeeperPrimaryWalAppenderTest.STREAM, oldOwner, TIMEOUT))
                    .join();
            AppendSession newOwner = new AppendSession(
                    oldOwner.streamId(), "writer-2", 2, "token-2", 2, 20_000);
            var observed = fixture.metadata.getWriter(
                            BookKeeperPrimaryWalAppenderTest.CLUSTER,
                            BookKeeperPrimaryWalAppenderTest.STREAM)
                    .join()
                    .orElseThrow();
            BookKeeperWriterStateMachine contenderA = fixture.writerState("contender-a");
            BookKeeperWriterStateMachine contenderB = fixture.writerState("contender-b");

            CompletableFuture<?> adoptionA = contenderA.adoptForRecovery(observed, newOwner, "contender a");
            CompletableFuture<?> adoptionB = contenderB.adoptForRecovery(observed, newOwner, "contender b");
            assertThat(adoptionA.isCompletedExceptionally() ^ adoptionB.isCompletedExceptionally()).isTrue();
            BookKeeperWriterStateMachine winner = adoptionA.isCompletedExceptionally() ? contenderB : contenderA;
            BookKeeperLedgerRecovery winningRecovery = fixture.recovery(winner);
            winningRecovery.recoverWriter(newOwner, TIMEOUT, "winning recovery owner").join();

            BookKeeperLedgerAllocator winningAllocator = fixture.allocator(winner, "winner-allocation");
            var replacement = winningAllocator.allocate(new BookKeeperLedgerAllocationRequest(
                            BookKeeperPrimaryWalAppenderTest.STREAM, newOwner, TIMEOUT))
                    .join();
            assertThat(replacement.root().value().ledgerId()).isNotEqualTo(original.root().value().ledgerId());
            assertThat(fixture.metadata.getWriter(
                            BookKeeperPrimaryWalAppenderTest.CLUSTER,
                            BookKeeperPrimaryWalAppenderTest.STREAM)
                    .join()).get().satisfies(writer -> {
                        assertThat(writer.value().lifecycle()).isEqualTo(BookKeeperWriterLifecycle.ACTIVE);
                        assertThat(writer.value().activeLedgerId()).isEqualTo(replacement.root().value().ledgerId());
                    });
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final BookKeeperWalConfiguration configuration = BookKeeperTestConfigurations.valid();
        private final FakeBookKeeperMetadataStore metadata;
        private final BookKeeperPrimaryWalAppenderTest.FakeOperations operations =
                new BookKeeperPrimaryWalAppenderTest.FakeOperations();
        private final AtomicInteger allocationSequence = new AtomicInteger();
        private final BookKeeperLedgerIdNamespaceReservationVerifier verifier;
        private final BookKeeperWriterStateMachine state;
        private final BookKeeperLedgerAllocator allocator;
        private final BookKeeperLedgerRecovery recovery;

        private Fixture(ResponseLossPartitionedOxiaBackend backend, String processRunId) {
            metadata = new FakeBookKeeperMetadataStore(
                    new BookKeeperMetadataStoreConfig(
                            configuration.maxAppendRangesPerLedger(),
                            configuration.protectionSlotsPerRange(),
                            configuration.maxReaderLeasesPerLedger(),
                            configuration.maxUncertainAllocations()),
                    BookKeeperPrimaryWalAppenderTest.CLOCK,
                    backend);
            verifier = new BookKeeperLedgerIdNamespaceReservationVerifier(
                    (scope, bits, prefix, timeout) -> CompletableFuture.completedFuture(Optional.of(
                            BookKeeperPrimaryWalAppenderTest.reservation(configuration))),
                    BookKeeperPrimaryWalAppenderTest.DEPLOYMENT);
            state = writerState(processRunId);
            allocator = allocator(state, processRunId + "-allocation");
            recovery = recovery(state);
        }

        private BookKeeperWriterStateMachine writerState(String processRunId) {
            return new BookKeeperWriterStateMachine(
                    BookKeeperPrimaryWalAppenderTest.CLUSTER,
                    configuration,
                    metadata,
                    BookKeeperPrimaryWalAppenderTest.CLOCK,
                    processRunId);
        }

        private BookKeeperLedgerAllocator allocator(
                BookKeeperWriterStateMachine writerState, String allocationPrefix) {
            return new BookKeeperLedgerAllocator(
                    BookKeeperPrimaryWalAppenderTest.CLUSTER,
                    configuration,
                    metadata,
                    metadata,
                    verifier,
                    operations,
                    ignored -> "secret".getBytes(StandardCharsets.UTF_8),
                    writerState,
                    BookKeeperPrimaryWalAppenderTest.CLOCK,
                    new Random(allocationPrefix.hashCode()),
                    () -> allocationPrefix + "-" + allocationSequence.incrementAndGet());
        }

        private BookKeeperLedgerRecovery recovery(BookKeeperWriterStateMachine writerState) {
            return new BookKeeperLedgerRecovery(
                    BookKeeperPrimaryWalAppenderTest.CLUSTER,
                    configuration,
                    metadata,
                    metadata,
                    verifier,
                    operations,
                    ignored -> "secret".getBytes(StandardCharsets.UTF_8),
                    writerState,
                    BookKeeperPrimaryWalAppenderTest.CLOCK);
        }

        @Override
        public void close() {
            metadata.close();
        }
    }
}
