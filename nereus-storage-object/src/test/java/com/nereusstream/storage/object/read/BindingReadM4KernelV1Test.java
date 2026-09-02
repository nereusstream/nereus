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

package com.nereusstream.storage.object.read;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.storage.object.read.BindingReadAttemptControllerV1.Outcome;
import com.nereusstream.storage.object.read.BindingReadHazardPoolV1.CaptureOutcome;
import com.nereusstream.storage.object.read.BindingReadHazardPoolV1.ScanOutcome;
import com.nereusstream.storage.object.read.BindingReadRouteV1.FailureClass;
import com.nereusstream.storage.object.read.BindingReadRouteV1.SourcePurity;
import com.nereusstream.storage.object.read.BindingReadSourceRefV1.SourceKind;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class BindingReadM4KernelV1Test {
    @Test
    void capturedGenerationRemainsPinnedThroughProviderAndBufferDrain() {
        Fixture fixture = fixture(1, true, List.of(route(0, 10, true)));
        BindingReadBatchContextV1 batch = new BindingReadBatchContextV1();

        assertThat(fixture.pool.tryCapture(fixture.current, batch)).isEqualTo(CaptureOutcome.CAPTURED);
        assertThat(fixture.pool.scan(fixture.binding, 1)).isEqualTo(ScanOutcome.PINNED);
        assertThat(batch.beginAttempt(11)).isTrue();
        batch.closeNewSourceUse();
        assertThat(batch.terminalClearExactLease()).isFalse();
        assertThat(batch.endAttempt(11)).isTrue();
        assertThat(batch.terminalClearExactLease()).isTrue();
        assertThat(fixture.pool.scan(fixture.binding, 1)).isEqualTo(ScanOutcome.CLEAN);
    }

    @Test
    void authoritySwitchRejectsOldAdmissionAndNewCaptureUsesSuccessorGeneration() {
        Fixture fixture = fixture(1, true, List.of(route(0, 10, true)));
        BindingReadAuthorityV1 successor = authority(fixture.binding, 2, true, List.of(route(0, 10, false)));
        fixture.current.set(successor);
        BindingReadBatchContextV1 batch = new BindingReadBatchContextV1();

        assertThat(fixture.pool.tryCapture(fixture.current, batch)).isEqualTo(CaptureOutcome.CAPTURED);
        assertThat(batch.authority()).isSameAs(successor);
        assertThat(fixture.pool.scan(fixture.binding, 1)).isEqualTo(ScanOutcome.CLEAN);
        assertThat(fixture.pool.scan(fixture.binding, 2)).isEqualTo(ScanOutcome.PINNED);
        batch.closeNewSourceUse();
        assertThat(batch.terminalClearExactLease()).isTrue();
    }

    @Test
    void stoppedAuthorityAndExhaustedPoolFailBeforeSourceUse() {
        Fixture fixture = fixture(1, false, List.of(route(0, 10, false)));
        assertThat(fixture.pool.tryCapture(fixture.current, new BindingReadBatchContextV1()))
                .isEqualTo(CaptureOutcome.ADMISSION_CLOSED);

        fixture.current.set(authority(fixture.binding, 1, true, List.of(route(0, 10, false))));
        BindingReadBatchContextV1 occupied = new BindingReadBatchContextV1();
        assertThat(fixture.pool.tryCapture(fixture.current, occupied)).isEqualTo(CaptureOutcome.CAPTURED);
        assertThat(fixture.pool.tryCapture(fixture.current, new BindingReadBatchContextV1()))
                .isEqualTo(CaptureOutcome.EXHAUSTED);
        occupied.closeNewSourceUse();
        assertThat(occupied.terminalClearExactLease()).isTrue();
    }

    @Test
    void deterministicPlanRejectsGapsAndCapacityWithoutRepairReads() {
        BindingReadPublicationCellV1 cell = authority(
                        binding("plan"), 3, true, List.of(route(0, 4, false), route(4, 8, true)))
                .publicationCell();
        BindingReadPlanBufferV1 output = new BindingReadPlanBufferV1(2);

        assertThat(BindingReadPlannerV1.plan(cell, 1, 8, 8, output)).isEqualTo(BindingReadPlannerV1.Outcome.PLANNED);
        assertThat(output.size()).isEqualTo(2);
        assertThat(output.startInclusive(0)).isEqualTo(1);
        assertThat(output.endExclusive(1)).isEqualTo(8);

        BindingReadPublicationCellV1 gap = authority(
                        binding("gap"), 3, true, List.of(route(0, 3, false), route(4, 8, false)))
                .publicationCell();
        assertThat(BindingReadPlannerV1.plan(gap, 0, 8, 8, output))
                .isEqualTo(BindingReadPlannerV1.Outcome.SAFE_FAILURE_GAP_OR_AMBIGUITY);
        assertThat(output.size()).isZero();

        assertThat(BindingReadPlannerV1.plan(cell, 0, 8, 8, new BindingReadPlanBufferV1(1)))
                .isEqualTo(BindingReadPlannerV1.Outcome.SAFE_FAILURE_CAPACITY);
    }

    @Test
    void pulsarPlannerPreservesTypedVirtualLedgerAndNeverFlattensLargePositions() {
        long ledger = Long.MAX_VALUE - 17;
        PulsarBindingReadRouteV1 first = pulsarRoute(ledger, 0, 4, false);
        PulsarBindingReadRouteV1 second = pulsarRoute(ledger, 4, 9, true);
        PulsarBindingReadRouteTableV1 table = new PulsarBindingReadRouteTableV1(List.of(first, second));
        PulsarBindingReadPlanBufferV1 output = new PulsarBindingReadPlanBufferV1(2);

        assertThat(PulsarBindingReadPlannerV1.plan(table, ledger, 1, 20, 9, output))
                .isEqualTo(BindingReadPlannerV1.Outcome.PLANNED);
        assertThat(output.size()).isEqualTo(2);
        assertThat(output.startEntryIdInclusive(0)).isEqualTo(1);
        assertThat(output.endEntryIdExclusive(1)).isEqualTo(9);
        assertThat(output.route(1).virtualLedgerId()).isEqualTo(ledger);

        PulsarBindingReadRouteTableV1 gap =
                new PulsarBindingReadRouteTableV1(List.of(first, pulsarRoute(ledger, 5, 9, false)));
        assertThat(PulsarBindingReadPlannerV1.plan(gap, ledger, 0, 9, 9, output))
                .isEqualTo(BindingReadPlannerV1.Outcome.SAFE_FAILURE_GAP_OR_AMBIGUITY);
        assertThat(output.size()).isZero();
        assertThat(PulsarBindingReadPlannerV1.plan(table, ledger, 0, 9, 9, new PulsarBindingReadPlanBufferV1(1)))
                .isEqualTo(BindingReadPlannerV1.Outcome.SAFE_FAILURE_CAPACITY);
    }

    @Test
    void fallbackIsSingleTransferPreObservabilityAndPreservesPrimaryCause() {
        Fixture fixture = fixture(1, true, List.of(route(0, 10, true)));
        BindingReadBatchContextV1 batch = new BindingReadBatchContextV1();
        assertThat(fixture.pool.tryCapture(fixture.current, batch)).isEqualTo(CaptureOutcome.CAPTURED);
        BindingReadAttemptControllerV1 attempt = new BindingReadAttemptControllerV1(
                batch, fixture.current.get().publicationCell().routes().route(0));

        assertThat(attempt.startPrimary()).isTrue();
        assertThat(attempt.completePrimary(FailureClass.CORRUPT_OR_FORMAT, true))
                .isEqualTo(Outcome.FALLBACK_READY);
        assertThat(attempt.startFallback()).isTrue();
        assertThat(attempt.completeFallback(null, true)).isEqualTo(Outcome.FALLBACK);
        assertThat(attempt.primaryFailure()).isEqualTo(FailureClass.CORRUPT_OR_FORMAT);
        assertThat(batch.terminalClearExactLease()).isTrue();
    }

    @Test
    void completedEarlierIntervalDoesNotCloseBatchOrForbidLaterIntervalFallback() {
        Fixture fixture = fixture(1, true, List.of(route(0, 5, false), route(5, 10, true)));
        BindingReadBatchContextV1 batch = new BindingReadBatchContextV1();
        assertThat(fixture.pool.tryCapture(fixture.current, batch)).isEqualTo(CaptureOutcome.CAPTURED);

        BindingReadAttemptControllerV1 first = new BindingReadAttemptControllerV1(
                batch, fixture.current.get().publicationCell().routes().route(0), 1, 2, false);
        assertThat(first.startPrimary()).isTrue();
        batch.markObservable();
        assertThat(first.completePrimary(null, true)).isEqualTo(Outcome.PRIMARY);
        assertThat(batch.newSourceUseOpen()).isTrue();

        BindingReadAttemptControllerV1 second = new BindingReadAttemptControllerV1(
                batch, fixture.current.get().publicationCell().routes().route(1), 3, 4, true);
        assertThat(second.startPrimary()).isTrue();
        assertThat(second.completePrimary(FailureClass.UNAVAILABLE, true)).isEqualTo(Outcome.FALLBACK_READY);
        assertThat(second.startFallback()).isTrue();
        assertThat(second.completeFallback(null, true)).isEqualTo(Outcome.FALLBACK);
        assertThat(batch.terminalClearExactLease()).isTrue();
    }

    @Test
    void observabilityForbidsFallbackAndUnprovedTerminationQuarantinesLease() {
        Fixture first = fixture(1, true, List.of(route(0, 10, true)));
        BindingReadBatchContextV1 observed = new BindingReadBatchContextV1();
        assertThat(first.pool.tryCapture(first.current, observed)).isEqualTo(CaptureOutcome.CAPTURED);
        BindingReadAttemptControllerV1 observedAttempt = new BindingReadAttemptControllerV1(
                observed, first.current.get().publicationCell().routes().route(0));
        assertThat(observedAttempt.startPrimary()).isTrue();
        observed.markObservable();
        assertThat(observedAttempt.completePrimary(FailureClass.UNAVAILABLE, true))
                .isEqualTo(Outcome.SAFE_FAILURE);
        assertThat(observed.terminalClearExactLease()).isTrue();

        Fixture second = fixture(1, true, List.of(route(0, 10, true)));
        BindingReadBatchContextV1 stuck = new BindingReadBatchContextV1();
        assertThat(second.pool.tryCapture(second.current, stuck)).isEqualTo(CaptureOutcome.CAPTURED);
        BindingReadAttemptControllerV1 stuckAttempt = new BindingReadAttemptControllerV1(
                stuck, second.current.get().publicationCell().routes().route(0));
        assertThat(stuckAttempt.startPrimary()).isTrue();
        assertThat(stuckAttempt.completePrimary(FailureClass.UNAVAILABLE, false))
                .isEqualTo(Outcome.QUARANTINED);
        assertThat(stuck.terminalClearExactLease()).isFalse();
        assertThat(second.pool.scan(second.binding, 1)).isEqualTo(ScanOutcome.PINNED);
    }

    @Test
    void multiBindingReservationReleasesEveryPartialLease() {
        Fixture first = fixture(1, true, List.of(route(0, 10, false)));
        Fixture second = fixture(1, true, List.of(route(0, 10, false)));
        BindingReadBatchContextV1 blocker = new BindingReadBatchContextV1();
        assertThat(second.pool.tryCapture(second.current, blocker)).isEqualTo(CaptureOutcome.CAPTURED);

        @SuppressWarnings("unchecked")
        AtomicReference<BindingReadAuthorityV1>[] authorities = new AtomicReference[] {first.current, second.current};
        BindingReadBatchContextV1[] batches = {
            new BindingReadBatchContextV1(first.pool), new BindingReadBatchContextV1(second.pool)
        };
        assertThat(BindingReadHazardPoolV1.tryCaptureAll(authorities, batches, 2))
                .isEqualTo(CaptureOutcome.EXHAUSTED);
        assertThat(batches[0].active()).isFalse();
        assertThat(first.pool.scan(first.binding, 1)).isEqualTo(ScanOutcome.CLEAN);
        blocker.closeNewSourceUse();
        assertThat(blocker.terminalClearExactLease()).isTrue();
    }

    @Test
    void lifecycleRejectsCrossThreadMutation() throws Exception {
        Fixture fixture = fixture(1, true, List.of(route(0, 10, false)));
        BindingReadBatchContextV1 batch = new BindingReadBatchContextV1();
        assertThat(fixture.pool.tryCapture(fixture.current, batch)).isEqualTo(CaptureOutcome.CAPTURED);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread callback = new Thread(() -> {
            try {
                batch.closeNewSourceUse();
            } catch (Throwable error) {
                failure.set(error);
            }
        });
        callback.start();
        callback.join();

        assertThat(failure.get()).isInstanceOf(IllegalStateException.class);
        batch.closeNewSourceUse();
        assertThat(batch.terminalClearExactLease()).isTrue();
    }

    @Test
    void plannedPoolCloseRejectsNewAdmissionWithoutForceClearingLiveLease() {
        Fixture fixture = fixture(1, true, List.of(route(0, 10, false)));
        BindingReadBatchContextV1 live = new BindingReadBatchContextV1();
        assertThat(fixture.pool.tryCapture(fixture.current, live)).isEqualTo(CaptureOutcome.CAPTURED);

        fixture.pool.closeAdmission();

        assertThat(fixture.pool.tryCapture(fixture.current, new BindingReadBatchContextV1()))
                .isEqualTo(CaptureOutcome.ADMISSION_CLOSED);
        assertThat(fixture.pool.scan(fixture.binding, 1)).isEqualTo(ScanOutcome.PINNED);
        live.closeNewSourceUse();
        assertThat(live.terminalClearExactLease()).isTrue();
        assertThat(fixture.pool.scan(fixture.binding, 1)).isEqualTo(ScanOutcome.CLEAN);
    }

    @Test
    void leaseGenerationWrapRetiresSlotInsteadOfReusingAbaIdentity() {
        TopicBindingId binding = binding("wrap");
        BindingReadHazardPoolV1 pool = new BindingReadHazardPoolV1(1, 1, Long.MAX_VALUE, null);
        AtomicReference<BindingReadAuthorityV1> current =
                new AtomicReference<>(authority(binding, 1, true, List.of(route(0, 10, false))));
        BindingReadBatchContextV1 finalLease = new BindingReadBatchContextV1();

        assertThat(pool.tryCapture(current, finalLease)).isEqualTo(CaptureOutcome.CAPTURED);
        assertThat(finalLease.leaseWord()).isEqualTo(Long.MAX_VALUE);
        finalLease.closeNewSourceUse();
        assertThat(finalLease.terminalClearExactLease()).isTrue();
        assertThat(pool.tryCapture(current, new BindingReadBatchContextV1())).isEqualTo(CaptureOutcome.EXHAUSTED);
    }

    @Test
    void scanTreatsClaimedLeaseWithUnpublishedPayloadAsInconclusive() throws Exception {
        CountDownLatch leaseClaimed = new CountDownLatch(1);
        CountDownLatch publishPayload = new CountDownLatch(1);
        TopicBindingId binding = binding("inconclusive");
        BindingReadHazardPoolV1 pool = new BindingReadHazardPoolV1(1, 1, 1, () -> {
            leaseClaimed.countDown();
            await(publishPayload);
        });
        AtomicReference<BindingReadAuthorityV1> current =
                new AtomicReference<>(authority(binding, 1, true, List.of(route(0, 10, false))));
        BindingReadBatchContextV1 batch = new BindingReadBatchContextV1();
        AtomicReference<CaptureOutcome> capture = new AtomicReference<>();
        Thread reader = new Thread(() -> capture.set(pool.tryCapture(current, batch)));

        reader.start();
        assertThat(leaseClaimed.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(pool.scan(binding, 1)).isEqualTo(ScanOutcome.INCONCLUSIVE);
        publishPayload.countDown();
        reader.join();
        assertThat(capture.get()).isEqualTo(CaptureOutcome.CAPTURED);
        assertThat(pool.scan(binding, 1)).isEqualTo(ScanOutcome.PINNED);
    }

    @Test
    void asyncCancellationClosesGateButRetainsLeaseUntilRealProviderCompletion() throws Exception {
        Fixture fixture = fixture(1, true, List.of(route(0, 10, false)));
        ExecutorService eventLoop = Executors.newSingleThreadExecutor();
        try {
            BindingReadAsyncExecutorV1 executor = new BindingReadAsyncExecutorV1(eventLoop);
            CompletableFuture<String> provider = new CompletableFuture<>();
            CountDownLatch started = new CountDownLatch(1);
            CompletableFuture<String> result = executor.execute(fixture.current, fixture.pool, authority -> {
                started.countDown();
                return provider;
            });

            assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(fixture.pool.scan(fixture.binding, 1)).isEqualTo(ScanOutcome.PINNED);
            assertThat(result.cancel(false)).isTrue();
            assertThat(fixture.pool.scan(fixture.binding, 1)).isEqualTo(ScanOutcome.PINNED);

            provider.complete("late-value");
            awaitClean(fixture.pool, fixture.binding, 1);
            assertThat(result).isCancelled();
        } finally {
            eventLoop.shutdownNow();
        }
    }

    @Test
    void asyncSuccessReleasesExactLeaseBeforeMakingHeapOwnedResultObservable() throws Exception {
        Fixture fixture = fixture(1, true, List.of(route(0, 10, false)));
        ExecutorService eventLoop = Executors.newSingleThreadExecutor();
        try {
            BindingReadAsyncExecutorV1 executor = new BindingReadAsyncExecutorV1(eventLoop);
            CompletableFuture<String> result = executor.execute(
                    fixture.current, fixture.pool, authority -> CompletableFuture.completedFuture("value"));

            assertThat(result.get(10, TimeUnit.SECONDS)).isEqualTo("value");
            assertThat(fixture.pool.scan(fixture.binding, 1)).isEqualTo(ScanOutcome.CLEAN);
        } finally {
            eventLoop.shutdownNow();
        }
    }

    @Test
    void steadyCapturePlanAndClearAllocateNoHeapBytesOnCurrentThread() {
        com.sun.management.ThreadMXBean bean = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        if (!bean.isThreadAllocatedMemorySupported()) {
            return;
        }
        bean.setThreadAllocatedMemoryEnabled(true);
        Fixture fixture = fixture(1, true, List.of(route(0, 10, false)));
        BindingReadBatchContextV1 batch = new BindingReadBatchContextV1();
        BindingReadPlanBufferV1 plan = new BindingReadPlanBufferV1(1);
        for (int index = 0; index < 20_000; index++) {
            runSteadyIteration(fixture, batch, plan);
        }

        long threadId = Thread.currentThread().getId();
        long before = bean.getThreadAllocatedBytes(threadId);
        for (int index = 0; index < 100_000; index++) {
            runSteadyIteration(fixture, batch, plan);
        }
        long allocated = bean.getThreadAllocatedBytes(threadId) - before;
        assertThat(allocated).isZero();
    }

    @Test
    void sourceRouteRejectsNotEligibleFallbackAndSemanticMismatch() {
        BindingReadSourceRefV1 primary = source(SourceKind.OBJECT, "primary", "same", 1);
        BindingReadSourceRefV1 different = source(SourceKind.BOOKKEEPER, "fallback", "different", 2);
        assertThatThrownBy(() -> new BindingReadRouteV1(
                        0, 1, primary, different, FailureClass.MISSING.mask(), SourcePurity.KAFKA_APPEND_UNIT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BindingReadRouteV1(
                        0, 1, primary, primary, FailureClass.NOT_ELIGIBLE.mask(), SourcePurity.KAFKA_APPEND_UNIT))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static void runSteadyIteration(
            Fixture fixture, BindingReadBatchContextV1 batch, BindingReadPlanBufferV1 plan) {
        if (fixture.pool.tryCapture(fixture.current, batch) != CaptureOutcome.CAPTURED
                || BindingReadPlannerV1.plan(batch.authority().publicationCell(), 0, 10, 10, plan)
                        != BindingReadPlannerV1.Outcome.PLANNED) {
            throw new AssertionError("steady read admission failed");
        }
        batch.closeNewSourceUse();
        if (!batch.terminalClearExactLease()) {
            throw new AssertionError("steady terminal clear failed");
        }
    }

    private static Fixture fixture(int capacity, boolean admitting, List<BindingReadRouteV1> routes) {
        TopicBindingId binding = binding("binding-" + capacity + "-" + admitting + "-" + routes.size());
        BindingReadHazardPoolV1 pool = new BindingReadHazardPoolV1(capacity, 4);
        return new Fixture(binding, pool, new AtomicReference<>(authority(binding, 1, admitting, routes)));
    }

    private static BindingReadAuthorityV1 authority(
            TopicBindingId binding, long generation, boolean admitting, List<BindingReadRouteV1> routes) {
        BindingReadPublicationCellV1 cell = new BindingReadPublicationCellV1(
                generation, 10, generation, new BindingReadRouteTableV1(routes), routes);
        return new BindingReadAuthorityV1(
                binding,
                digest("incarnation"),
                new StorageEpochId(digest("storage-epoch")),
                BindingReadProtocolV1.KAFKA_OFFSET,
                digest("view-" + generation),
                1,
                generation,
                admitting,
                1,
                digest("capability"),
                cell);
    }

    private static BindingReadRouteV1 route(long start, long end, boolean fallback) {
        BindingReadSourceRefV1 primary = source(SourceKind.OBJECT, "object-" + start, "semantic-" + start, 1);
        BindingReadSourceRefV1 secondary =
                fallback ? source(SourceKind.BOOKKEEPER, "bookkeeper-" + start, "semantic-" + start, 2) : null;
        int mask = fallback
                ? FailureClass.MISSING.mask() | FailureClass.UNAVAILABLE.mask() | FailureClass.CORRUPT_OR_FORMAT.mask()
                : 0;
        return new BindingReadRouteV1(start, end, primary, secondary, mask, SourcePurity.KAFKA_APPEND_UNIT);
    }

    private static PulsarBindingReadRouteV1 pulsarRoute(long ledger, long start, long end, boolean fallback) {
        BindingReadSourceRefV1 primary = source(SourceKind.OBJECT, "pulsar-object-" + start, "pulsar-" + start, 1);
        BindingReadSourceRefV1 secondary =
                fallback ? source(SourceKind.BOOKKEEPER, "pulsar-bk-" + start, "pulsar-" + start, 2) : null;
        int mask = fallback ? FailureClass.MISSING.mask() | FailureClass.UNAVAILABLE.mask() : 0;
        return new PulsarBindingReadRouteV1(
                ledger, start, end, primary, secondary, mask, SourcePurity.PULSAR_WHOLE_REQUEST);
    }

    private static BindingReadSourceRefV1 source(
            SourceKind kind, String identity, String semantic, long protectionGeneration) {
        return new BindingReadSourceRefV1(
                kind, digest(identity), digest(identity + "-version"), digest(semantic), protectionGeneration);
    }

    private static TopicBindingId binding(String value) {
        return new TopicBindingId(digest(value));
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8)));
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new AssertionError("timed out waiting for deterministic hazard race");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for deterministic hazard race", interrupted);
        }
    }

    private static void awaitClean(BindingReadHazardPoolV1 pool, TopicBindingId binding, long generation) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            if (pool.scan(binding, generation) == ScanOutcome.CLEAN) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("timed out waiting for exact async lease drain");
    }

    private record Fixture(
            TopicBindingId binding, BindingReadHazardPoolV1 pool, AtomicReference<BindingReadAuthorityV1> current) {}
}
