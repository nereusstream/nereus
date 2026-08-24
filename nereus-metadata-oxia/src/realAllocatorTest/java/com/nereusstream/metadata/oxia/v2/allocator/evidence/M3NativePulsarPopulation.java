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

package com.nereusstream.metadata.oxia.v2.allocator.evidence;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.bookkeeper.client.M3PayloadReleasingPulsarMockBookKeeper;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

/** Exact pinned {@link ManagedLedgerImpl} rollover decisions and native ledger-ID path at the full population. */
final class M3NativePulsarPopulation implements AutoCloseable {
    private static final int ONE_MIB = 1024 * 1024;
    private static final int BYTE_PREFILL_ENTRIES = 15;
    private static final byte[] PAYLOAD_64_KIB = new byte[M3AllocatorWorkloadPlan.BYTE_PAYLOAD_BYTES];

    private final ExecutorService workers;
    private final OrderedScheduler scheduler = OrderedScheduler.newSchedulerBuilder()
            .numThreads(4)
            .name("m3-native-pulsar")
            .build();
    private final M3ControlledLatencyMetadataStore metadataStore;
    private final M3PayloadReleasingPulsarMockBookKeeper bookKeeper;
    private final List<ManagedLedgerFactory> factories = new ArrayList<>(4);
    private final AtomicReferenceArray<NativeLedger> ledgers = new AtomicReferenceArray<>(100_000);
    private final AtomicInteger activePopulation = new AtomicInteger();

    M3NativePulsarPopulation(ExecutorService workers) throws Exception {
        this.workers = workers;
        MetadataStoreExtended local = MetadataStoreExtended.create(
                "memory:local",
                MetadataStoreConfig.builder().metadataStoreName("m3-native-allocator").build());
        metadataStore = new M3ControlledLatencyMetadataStore(local);
        for (int index = 0; index < 3; index++) {
            metadataStore
                    .put(
                            "/ledgers/available/192.168.1.1:" + (5_000 + index),
                            new byte[0],
                            Optional.empty())
                    .join();
        }
        metadataStore
                .put(
                        "/ledgers/LAYOUT",
                        "1\nflat:1".getBytes(StandardCharsets.US_ASCII),
                        Optional.empty())
                .join();
        bookKeeper = new M3PayloadReleasingPulsarMockBookKeeper(scheduler);
        bookKeeper.setDefaultAddEntryDelayMillis(0);
        bookKeeper.setDefaultReadEntriesDelayMillis(0);
        for (int actorId = 0; actorId < M3AllocatorWorkloadPlan.BROKER_ACTORS; actorId++) {
            factories.add(new ManagedLedgerFactoryImpl(metadataStore, bookKeeper));
        }
    }

    long ensurePopulation(int requestedPopulation) throws Exception {
        if (!M3AllocatorWorkloadPlan.ACTIVE_POPULATIONS.contains(requestedPopulation)) {
            throw new IllegalArgumentException("native population differs from ADR 0094");
        }
        int from = activePopulation.get();
        if (requestedPopulation < from) {
            return 0;
        }
        long started = System.nanoTime();
        parallel(from, requestedPopulation, this::createLedger);
        if (!activePopulation.compareAndSet(from, requestedPopulation)) {
            throw new IllegalStateException("native ManagedLedger population construction raced");
        }
        return TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - started);
    }

    void setMetadataLatencyMillis(int latencyMillis) {
        metadataStore.setLatencyMillis(latencyMillis);
    }

    NativeRollover rollover(
            M3AllocatorRequestTelemetry.RequestTrace trace,
            int ledgerIndex,
            M3AllocatorWorkloadPlan.Trigger trigger) throws Exception {
        if (ledgerIndex < 0 || ledgerIndex >= activePopulation.get()) {
            throw new IllegalArgumentException("native request selected a non-active ManagedLedger");
        }
        NativeLedger nativeLedger = ledgers.get(ledgerIndex);
        nativeLedger.lock.lock();
        try {
            long predecessorLedgerId = nativeLedger.currentLedgerId;
            if (trigger == M3AllocatorWorkloadPlan.Trigger.BYTE) {
                configureUnlimited(nativeLedger);
                if (nativeLedger.ledger.getCurrentLedgerSize() != M3AllocatorWorkloadPlan.BYTE_PAYLOAD_BYTES) {
                    throw new AssertionError("native BYTE invariant must start with one exact 64-KiB entry");
                }
                for (int index = 0; index < BYTE_PREFILL_ENTRIES; index++) {
                    Position prefill = nativeLedger.ledger.addEntry(PAYLOAD_64_KIB);
                    if (prefill.getLedgerId() != predecessorLedgerId) {
                        throw new AssertionError("native BYTE prefill rolled before reaching exact 1 MiB");
                    }
                }
                if (nativeLedger.ledger.getCurrentLedgerSize() != ONE_MIB
                        || nativeLedger.ledger.getCurrentLedgerEntries() != 16) {
                    throw new AssertionError("native BYTE sixteenth 64-KiB entry did not reach exact 1 MiB");
                }
                configureTrigger(nativeLedger, trigger);
            } else {
                configureTrigger(nativeLedger, trigger);
            }
            if (trigger == M3AllocatorWorkloadPlan.Trigger.AGE) {
                nativeLedger.clock.advanceExactlyOneSecond();
            }

            long predecessorEntriesBeforeTrigger = nativeLedger.ledger.getCurrentLedgerEntries();
            long predecessorBytesBeforeTrigger = nativeLedger.ledger.getCurrentLedgerSize();
            trace.admitted();
            // The pinned ManagedLedger implementation first counts this append against the open
            // predecessor and only then invokes currentLedgerIsFull(). Its synchronous callback is
            // released after predecessor close. Keep that actual append stall distinct from the
            // longer rollover interval, which ends only after successor entry 0 is established.
            trace.appendAdmissionStart();
            Position triggerAppend = nativeLedger.ledger.addEntry(PAYLOAD_64_KIB);
            if (triggerAppend.getLedgerId() != predecessorLedgerId) {
                throw new AssertionError("native production trigger append did not close its exact predecessor");
            }
            trace.appendAdmissionRelease();
            // ManagedLedger applies the entry/byte/age decision after admitting the trigger append.
            // A following append waits for native close/create and establishes the one-entry invariant
            // for the next offered rollover without changing the measured trigger decision.
            configureUnlimited(nativeLedger);
            Position successorAppend = nativeLedger.ledger.addEntry(PAYLOAD_64_KIB);
            if (successorAppend.getLedgerId() == predecessorLedgerId
                    || successorAppend.getEntryId() != 0
                    || nativeLedger.ledger.getCurrentLedgerEntries() != 1
                    || nativeLedger.ledger.getCurrentLedgerSize() != M3AllocatorWorkloadPlan.BYTE_PAYLOAD_BYTES) {
                throw new AssertionError("native production trigger did not establish one fresh 64-KiB entry");
            }
            nativeLedger.currentLedgerId = successorAppend.getLedgerId();
            trace.allocatedLedgerId(successorAppend.getLedgerId());
            bookKeeper.discardClosedLedger(predecessorLedgerId);
            return new NativeRollover(
                    predecessorLedgerId,
                    predecessorEntriesBeforeTrigger,
                    predecessorBytesBeforeTrigger,
                    triggerAppend.getLedgerId(),
                    triggerAppend.getEntryId(),
                    successorAppend.getLedgerId(),
                    successorAppend.getEntryId());
        } finally {
            nativeLedger.lock.unlock();
        }
    }

    /**
     * Executes the rejected byte-threshold order against the exact pinned ManagedLedger path.
     *
     * <p>Arming 1 MiB before the fifteen-entry prefill closes on the sixteenth total entry. The
     * formal path therefore must prefill under unlimited size and arm 1 MiB only after reaching
     * the exact boundary.
     */
    PrematureByteThresholdProbe probePrematureByteThreshold() throws Exception {
        MutableClock clock = new MutableClock();
        ManagedLedgerConfig config = baseConfig(clock);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factories.get(0)
                .open("m3-native-allocator-premature-byte-threshold", config);
        Position initial = ledger.addEntry(PAYLOAD_64_KIB);
        NativeLedger probe = new NativeLedger(ledger, config, clock, initial.getLedgerId());
        configureTrigger(probe, M3AllocatorWorkloadPlan.Trigger.BYTE);
        Position prematureTrigger = null;
        for (int index = 0; index < BYTE_PREFILL_ENTRIES; index++) {
            prematureTrigger = ledger.addEntry(PAYLOAD_64_KIB);
        }
        if (prematureTrigger == null
                || prematureTrigger.getLedgerId() != initial.getLedgerId()
                || prematureTrigger.getEntryId() != 15) {
            throw new AssertionError("premature byte threshold did not close on exact 1-MiB entry 15");
        }
        configureUnlimited(probe);
        Position successor = ledger.addEntry(PAYLOAD_64_KIB);
        return new PrematureByteThresholdProbe(
                initial.getLedgerId(),
                prematureTrigger.getLedgerId(),
                prematureTrigger.getEntryId(),
                successor.getLedgerId(),
                successor.getEntryId());
    }

    private void configureTrigger(NativeLedger nativeLedger, M3AllocatorWorkloadPlan.Trigger trigger) {
        ManagedLedgerConfig config = nativeLedger.config;
        config.setMinimumRolloverTime(0, TimeUnit.MILLISECONDS);
        switch (trigger) {
            case ENTRY -> {
                config.setMaxEntriesPerLedger(1);
                config.setMaxSizePerLedgerMb(Integer.MAX_VALUE);
                config.setMaximumRolloverTime(Integer.MAX_VALUE, TimeUnit.SECONDS);
            }
            case BYTE -> {
                config.setMaxEntriesPerLedger(Integer.MAX_VALUE);
                config.setMaxSizePerLedgerMb(1);
                config.setMaximumRolloverTime(Integer.MAX_VALUE, TimeUnit.SECONDS);
            }
            case AGE -> {
                config.setMaxEntriesPerLedger(Integer.MAX_VALUE);
                config.setMaxSizePerLedgerMb(Integer.MAX_VALUE);
                config.setMaximumRolloverTime(1, TimeUnit.SECONDS);
            }
        }
    }

    private void configureUnlimited(NativeLedger nativeLedger) {
        ManagedLedgerConfig config = nativeLedger.config;
        config.setMinimumRolloverTime(0, TimeUnit.MILLISECONDS);
        config.setMaxEntriesPerLedger(Integer.MAX_VALUE);
        config.setMaxSizePerLedgerMb(Integer.MAX_VALUE);
        config.setMaximumRolloverTime(Integer.MAX_VALUE, TimeUnit.SECONDS);
    }

    private void createLedger(int index) throws Exception {
        MutableClock clock = new MutableClock();
        ManagedLedgerConfig config = baseConfig(clock);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factories.get(index & 3)
                .open("m3-native-allocator-" + index, config);
        Position initial = ledger.addEntry(PAYLOAD_64_KIB);
        if (initial.getEntryId() != 0
                || ledger.getCurrentLedgerEntries() != 1
                || ledger.getCurrentLedgerSize() != M3AllocatorWorkloadPlan.BYTE_PAYLOAD_BYTES) {
            throw new AssertionError("native ManagedLedger population did not establish its exact initial invariant");
        }
        ledgers.set(index, new NativeLedger(ledger, config, clock, initial.getLedgerId()));
    }

    void ensureOneForComponentTest() throws Exception {
        if (!activePopulation.compareAndSet(0, 1)) {
            throw new IllegalStateException("native component-test population was already created");
        }
        try {
            createLedger(0);
        } catch (Exception failure) {
            activePopulation.set(0);
            throw failure;
        }
    }

    long retainedPayloadBytes() {
        return bookKeeper.retainedPayloadBytes();
    }

    private static ManagedLedgerConfig baseConfig(MutableClock clock) {
        ManagedLedgerConfig config = new ManagedLedgerConfig()
                .setClock(clock)
                .setMaxEntriesPerLedger(Integer.MAX_VALUE)
                .setMaxSizePerLedgerMb(Integer.MAX_VALUE)
                .setEnsembleSize(1)
                .setWriteQuorumSize(1)
                .setAckQuorumSize(1)
                .setMetadataEnsembleSize(1)
                .setMetadataWriteQuorumSize(1)
                .setMetadataAckQuorumSize(1);
        config.setMinimumRolloverTime(0, TimeUnit.MILLISECONDS);
        config.setMaximumRolloverTime(Integer.MAX_VALUE, TimeUnit.SECONDS);
        return config;
    }

    private void parallel(int fromInclusive, int toExclusive, IndexedOperation operation) throws Exception {
        CompletionService<Void> completions = new ExecutorCompletionService<>(workers);
        for (int index = fromInclusive; index < toExclusive; index++) {
            int exactIndex = index;
            completions.submit(() -> {
                operation.run(exactIndex);
                return null;
            });
        }
        for (int index = fromInclusive; index < toExclusive; index++) {
            try {
                completions.take().get();
            } catch (ExecutionException failure) {
                Throwable cause = failure.getCause();
                if (cause instanceof Exception exception) {
                    throw exception;
                }
                throw new RuntimeException(cause);
            }
        }
    }

    @Override
    public void close() throws Exception {
        Exception failure = null;
        for (int index = factories.size() - 1; index >= 0; index--) {
            try {
                factories.get(index).shutdownAsync().get(5, TimeUnit.MINUTES);
            } catch (Exception closeFailure) {
                if (failure == null) {
                    failure = closeFailure;
                } else {
                    failure.addSuppressed(closeFailure);
                }
            }
        }
        try {
            bookKeeper.shutdown();
        } catch (Exception closeFailure) {
            if (failure == null) {
                failure = closeFailure;
            } else {
                failure.addSuppressed(closeFailure);
            }
        }
        try {
            metadataStore.close();
        } catch (Exception closeFailure) {
            if (failure == null) {
                failure = closeFailure;
            } else {
                failure.addSuppressed(closeFailure);
            }
        }
        scheduler.shutdownNow();
        if (failure != null) {
            throw failure;
        }
    }

    record NativeRollover(
            long predecessorLedgerId,
            long predecessorEntriesBeforeTrigger,
            long predecessorBytesBeforeTrigger,
            long triggerLedgerId,
            long triggerEntryId,
            long successorLedgerId,
            long successorEntryId) {}

    record PrematureByteThresholdProbe(
            long predecessorLedgerId,
            long thresholdTriggerLedgerId,
            long thresholdTriggerEntryId,
            long successorLedgerId,
            long successorEntryId) {}

    private static final class NativeLedger {
        private final ManagedLedgerImpl ledger;
        private final ManagedLedgerConfig config;
        private final MutableClock clock;
        private final ReentrantLock lock = new ReentrantLock();
        private long currentLedgerId;

        private NativeLedger(
                ManagedLedgerImpl ledger, ManagedLedgerConfig config, MutableClock clock, long currentLedgerId) {
            this.ledger = ledger;
            this.config = config;
            this.clock = clock;
            this.currentLedgerId = currentLedgerId;
        }
    }

    private static final class MutableClock extends Clock {
        private final AtomicLong epochMillis = new AtomicLong(1_000_000L);

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            if (!ZoneOffset.UTC.equals(zone)) {
                throw new IllegalArgumentException("native allocator evidence clock is fixed to UTC");
            }
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(epochMillis.get());
        }

        @Override
        public long millis() {
            return epochMillis.get();
        }

        void advanceExactlyOneSecond() {
            epochMillis.addAndGet(TimeUnit.NANOSECONDS.toMillis(M3AllocatorWorkloadPlan.AGE_ADVANCE_NANOS));
        }
    }

    @FunctionalInterface
    private interface IndexedOperation {
        void run(int index) throws Exception;
    }
}
