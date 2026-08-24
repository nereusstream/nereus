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

import com.nereusstream.domain.registry.allocator.AllocatorEvidenceContextV1;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/** Paces one exact 10s warm-up plus 30s measured interval onto the single 96-worker executor. */
final class M3AllocatorIntervalRunner {
    private static final long INTERVAL_SECONDS =
            M3AllocatorWorkloadPlan.WARM_UP_SECONDS + M3AllocatorWorkloadPlan.MEASURED_SECONDS;
    private static final long MAX_POST_DEADLINE_DRAIN_SECONDS = 120;

    private final ThreadPoolExecutor workers;

    M3AllocatorIntervalRunner(ThreadPoolExecutor workers) {
        this.workers = Objects.requireNonNull(workers, "workers");
        if (workers.getCorePoolSize() != M3AllocatorWorkloadPlan.WORKER_THREADS
                || workers.getMaximumPoolSize() != M3AllocatorWorkloadPlan.WORKER_THREADS) {
            throw new IllegalArgumentException("allocator interval executor is not the exact 96-worker envelope");
        }
    }

    void runCandidate(
            AllocatorEvidenceContextV1 context,
            M3CandidateAllocatorPopulation population,
            M3AllocatorRequestTelemetry telemetry) throws InterruptedException {
        requireContext(context, false);
        run(
                context,
                telemetry,
                request -> population.rollover(
                        request.trace(),
                        request.planned().ledgerIndex(),
                        M3CandidateAllocatorPopulation.ResponseLossAt.NONE));
    }

    void runNative(
            AllocatorEvidenceContextV1 context,
            M3NativePulsarPopulation population,
            M3AllocatorRequestTelemetry telemetry) throws InterruptedException {
        requireContext(context, true);
        run(context, telemetry, request -> population.rollover(
                request.trace(), request.planned().ledgerIndex(), request.planned().trigger()));
    }

    private void run(
            AllocatorEvidenceContextV1 context,
            M3AllocatorRequestTelemetry telemetry,
            RolloverOperation operation) throws InterruptedException {
        int requestCount = M3AllocatorWorkloadPlan.requestCount(
                context.offeredRolloverRequestsPerSecond());
        CountDownLatch completed = new CountDownLatch(requestCount);
        AtomicInteger queueDepth = new AtomicInteger();
        Object queueTransitionLock = new Object();
        long intervalStartNanos = System.nanoTime();
        long intervalStartMicros = telemetry.currentTimestampMicros();
        long deadlineNanos = intervalStartNanos + TimeUnit.SECONDS.toNanos(INTERVAL_SECONDS);

        for (M3AllocatorWorkloadPlan.PlannedRequest planned : M3AllocatorWorkloadPlan.requests(
                context.activeManagedLedgers(), context.offeredRolloverRequestsPerSecond())) {
            waitUntil(intervalStartNanos + TimeUnit.MICROSECONDS.toNanos(planned.arrivalOffsetMicros()));
            M3AllocatorRequestTelemetry.RequestTrace trace = telemetry.trace(context, planned, null, 1);
            trace.offeredAt(intervalStartMicros + planned.arrivalOffsetMicros());
            synchronized (queueTransitionLock) {
                int afterEnqueue = queueDepth.incrementAndGet();
                trace.enqueuedAtDepth(afterEnqueue);
            }
            workers.execute(() -> {
                synchronized (queueTransitionLock) {
                    int afterDequeue = queueDepth.decrementAndGet();
                    trace.dispatchedAtDepth(afterDequeue);
                }
                try {
                    if (System.nanoTime() >= deadlineNanos) {
                        trace.admitted();
                        trace.appendAdmissionStart();
                        trace.appendAdmissionRelease();
                        trace.timedOut();
                        return;
                    }
                    operation.run(new ScheduledRequest(planned, trace));
                    if (System.nanoTime() <= deadlineNanos) {
                        trace.completed();
                    } else {
                        trace.timedOut();
                    }
                } catch (Throwable failure) {
                    try {
                        trace.completeFailureLifecycle();
                    } catch (RuntimeException duplicateOrWriteFailure) {
                        failure.addSuppressed(duplicateOrWriteFailure);
                    }
                    trace.failed();
                } finally {
                    completed.countDown();
                }
            });
        }

        long remainingToDeadline = Math.max(0, deadlineNanos - System.nanoTime());
        completed.await(remainingToDeadline, TimeUnit.NANOSECONDS);
        if (!completed.await(MAX_POST_DEADLINE_DRAIN_SECONDS, TimeUnit.SECONDS)) {
            throw new IllegalStateException("allocator interval did not drain within its bounded post-deadline cap");
        }
        if (queueDepth.get() != 0) {
            throw new IllegalStateException("allocator interval queue depth did not return to zero");
        }
    }

    private static void waitUntil(long targetNanos) throws InterruptedException {
        while (true) {
            long remaining = targetNanos - System.nanoTime();
            if (remaining <= 0) {
                return;
            }
            LockSupport.parkNanos(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(2)));
            if (Thread.interrupted()) {
                throw new InterruptedException("allocator interval coordinator was interrupted");
            }
        }
    }

    private static void requireContext(AllocatorEvidenceContextV1 context, boolean nativePath) {
        Objects.requireNonNull(context, "context");
        if (context.nativePath() != nativePath) {
            throw new IllegalArgumentException("allocator interval context belongs to another execution path");
        }
    }

    private record ScheduledRequest(
            M3AllocatorWorkloadPlan.PlannedRequest planned, M3AllocatorRequestTelemetry.RequestTrace trace) {}

    @FunctionalInterface
    private interface RolloverOperation {
        void run(ScheduledRequest request) throws Exception;
    }
}
