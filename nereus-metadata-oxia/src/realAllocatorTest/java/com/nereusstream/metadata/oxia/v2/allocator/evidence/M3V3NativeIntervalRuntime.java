/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.metadata.oxia.v2.allocator.evidence;

import com.nereusstream.domain.registry.allocator.AllocatorEvidenceContextV1;
import com.nereusstream.domain.registry.allocator.AllocatorNativeExecutionProfileV3;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** Shared ADR-0109 Native interval runtime used by both formal execution and diagnostic canaries. */
final class M3V3NativeIntervalRuntime {
    private final M3NativePulsarPopulation population;

    M3V3NativeIntervalRuntime(M3NativePulsarPopulation population) {
        this.population = Objects.requireNonNull(population, "population");
    }

    Result run(
            int activePopulation,
            int metadataLatencyMillis,
            int offeredRate,
            M3AllocatorRequestTelemetry.EventSink eventSink)
            throws Exception {
        population.ensurePopulation(activePopulation);
        population.setMetadataLatencyMillis(metadataLatencyMillis);
        NativeMetrics nativeMetrics = new NativeMetrics();
        M3AllocatorRequestTelemetry telemetry =
                new M3AllocatorRequestTelemetry(Objects.requireNonNull(eventSink, "eventSink"), System.nanoTime());
        AllocatorEvidenceContextV1 context =
                AllocatorEvidenceContextV1.nativeContext(activePopulation, metadataLatencyMillis, offeredRate);
        List<M3V3AsyncActorLaneRunner.ScheduledOffer<NativeOffer>> schedule = schedule(activePopulation, offeredRate);
        M3V3AsyncActorLaneRunner<NativeOffer> runner = M3V3AsyncActorLaneRunner.formal();
        M3V3AsyncActorLaneRunner.IntervalResult interval;
        try {
            interval = runner.run(offeredRate, schedule, (actorId, offer, operationContext) -> {
                M3AllocatorWorkloadPlan.PlannedRequest request = offer.request();
                M3AllocatorRequestTelemetry.RequestTrace trace = telemetry.trace(context, request, null, 1);
                trace.offered();
                trace.enqueued();
                trace.dispatched();
                RequestMetrics requestMetrics = nativeMetrics.request(request.trigger());
                return population
                        .rolloverAsync(
                                trace,
                                request.ledgerIndex(),
                                request.trigger(),
                                operationContext::allowsNextMetadataOperation,
                                requestMetrics)
                        .handle((rollover, failure) -> {
                            requestMetrics.terminal(failure);
                            if (failure != null) {
                                trace.completeFailureLifecycle();
                                trace.failed();
                                throw new CompletionException(unwrap(failure));
                            }
                            if (rollover.successorLedgerId() <= 0) {
                                throw new CompletionException(
                                        new IllegalStateException("native interval produced a nonpositive ledger ID"));
                            }
                            offer.allocatedLedgerId().set(rollover.successorLedgerId());
                            trace.completed();
                            return null;
                        });
            });
        } finally {
            population.setMetadataLatencyMillis(0);
        }
        Set<Long> allocated = new HashSet<>();
        long duplicates = 0;
        for (M3V3AsyncActorLaneRunner.ScheduledOffer<NativeOffer> offer : schedule) {
            long ledgerId = offer.request().allocatedLedgerId().get();
            if (ledgerId > 0 && !allocated.add(ledgerId)) {
                duplicates++;
            }
        }
        return new Result(interval, nativeMetrics.snapshot(), duplicates, allocated.size(), schedule.size());
    }

    static List<M3V3AsyncActorLaneRunner.ScheduledOffer<NativeOffer>> schedule(
            int activePopulation, int offeredRate) {
        List<M3V3AsyncActorLaneRunner.ScheduledOffer<NativeOffer>> schedule = new ArrayList<>();
        for (M3AllocatorWorkloadPlan.PlannedRequest request :
                M3AllocatorWorkloadPlan.v3Requests(activePopulation, offeredRate)) {
            schedule.add(new M3V3AsyncActorLaneRunner.ScheduledOffer<>(
                    request.requestOrdinal(),
                    request.actorId(),
                    request.ledgerIndex(),
                    TimeUnit.MICROSECONDS.toNanos(request.arrivalOffsetMicros()),
                    request.phase() != M3AllocatorWorkloadPlan.Phase.WARM_UP,
                    new NativeOffer(request)));
        }
        return List.copyOf(schedule);
    }

    static String executionModel() {
        return AllocatorNativeExecutionProfileV3.MODEL;
    }

    static int hiddenDispatchQueue() {
        return AllocatorNativeExecutionProfileV3.HIDDEN_DISPATCH_QUEUE;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    record Result(
            M3V3AsyncActorLaneRunner.IntervalResult interval,
            NativeTelemetry telemetry,
            long duplicateLedgerIds,
            long distinctAllocatedLedgerIds,
            long scheduledRequests) {
        Result {
            Objects.requireNonNull(interval, "interval");
            Objects.requireNonNull(telemetry, "telemetry");
            if (duplicateLedgerIds < 0 || distinctAllocatedLedgerIds < 0 || scheduledRequests <= 0) {
                throw new IllegalArgumentException("native interval result counters differ");
            }
        }
    }

    record NativeTelemetry(
            String nativeExecutionModel,
            int nativeBridgeWorkers,
            int nativeBridgeQueueCapacity,
            int hiddenNativeQueueDepthMaximum,
            long managedLedgerOperations,
            long managedLedgerOperationOutstandingMaximum,
            long managedLedgerOperationOutstandingAtEnd,
            long admissionToFirstMetadataDispatchP99Micros,
            long metadataOperationsPerRequestP99,
            long triggerAppendCompletions,
            long successorEstablishments,
            Map<M3AllocatorWorkloadPlan.Trigger, Long> rolloverP99Micros) {
        NativeTelemetry {
            Objects.requireNonNull(nativeExecutionModel, "nativeExecutionModel");
            rolloverP99Micros = Map.copyOf(rolloverP99Micros);
        }
    }

    record NativeOffer(M3AllocatorWorkloadPlan.PlannedRequest request, AtomicLong allocatedLedgerId) {
        NativeOffer(M3AllocatorWorkloadPlan.PlannedRequest request) {
            this(request, new AtomicLong());
        }
    }

    private static final class NativeMetrics {
        private final AtomicLong operations = new AtomicLong();
        private final AtomicLong operationOutstanding = new AtomicLong();
        private final AtomicLong operationOutstandingMaximum = new AtomicLong();
        private final AtomicLong triggerAppendCompletions = new AtomicLong();
        private final AtomicLong successorEstablishments = new AtomicLong();
        private final List<Long> admissionToFirstDispatchMicros = Collections.synchronizedList(new ArrayList<>());
        private final List<Long> operationsPerRequest = Collections.synchronizedList(new ArrayList<>());
        private final Map<M3AllocatorWorkloadPlan.Trigger, List<Long>> rolloverMicros =
                new EnumMap<>(M3AllocatorWorkloadPlan.Trigger.class);

        private NativeMetrics() {
            for (M3AllocatorWorkloadPlan.Trigger trigger : M3AllocatorWorkloadPlan.Trigger.values()) {
                rolloverMicros.put(trigger, Collections.synchronizedList(new ArrayList<>()));
            }
        }

        private RequestMetrics request(M3AllocatorWorkloadPlan.Trigger trigger) {
            return new RequestMetrics(this, trigger);
        }

        private void operationStarted() {
            operations.incrementAndGet();
            long outstanding = operationOutstanding.incrementAndGet();
            operationOutstandingMaximum.accumulateAndGet(outstanding, Math::max);
        }

        private void operationCompleted() {
            long outstanding = operationOutstanding.decrementAndGet();
            if (outstanding < 0) {
                throw new IllegalStateException("native ManagedLedger operation outstanding became negative");
            }
        }

        private NativeTelemetry snapshot() {
            Map<M3AllocatorWorkloadPlan.Trigger, Long> p99 = new EnumMap<>(M3AllocatorWorkloadPlan.Trigger.class);
            rolloverMicros.forEach((trigger, values) -> p99.put(trigger, percentile(values, 0.99)));
            return new NativeTelemetry(
                    AllocatorNativeExecutionProfileV3.MODEL,
                    AllocatorNativeExecutionProfileV3.NATIVE_BRIDGE_WORKERS,
                    AllocatorNativeExecutionProfileV3.NATIVE_BRIDGE_QUEUE_CAPACITY,
                    AllocatorNativeExecutionProfileV3.HIDDEN_DISPATCH_QUEUE,
                    operations.get(),
                    operationOutstandingMaximum.get(),
                    operationOutstanding.get(),
                    percentile(admissionToFirstDispatchMicros, 0.99),
                    percentile(operationsPerRequest, 0.99),
                    triggerAppendCompletions.get(),
                    successorEstablishments.get(),
                    p99);
        }
    }

    private static final class RequestMetrics implements M3NativePulsarPopulation.NativeOperationObserver {
        private final NativeMetrics parent;
        private final M3AllocatorWorkloadPlan.Trigger trigger;
        private final AtomicLong admittedNanos = new AtomicLong();
        private final AtomicLong firstDispatchNanos = new AtomicLong();
        private final AtomicLong successorNanos = new AtomicLong();
        private final AtomicInteger operations = new AtomicInteger();
        private final AtomicInteger terminal = new AtomicInteger();

        private RequestMetrics(NativeMetrics parent, M3AllocatorWorkloadPlan.Trigger trigger) {
            this.parent = parent;
            this.trigger = trigger;
        }

        @Override
        public void requestAdmitted(M3AllocatorWorkloadPlan.Trigger exactTrigger, long admitted) {
            requireTrigger(exactTrigger);
            if (!admittedNanos.compareAndSet(0, admitted)) {
                throw new IllegalStateException("native request admission telemetry repeated");
            }
        }

        @Override
        public void operationDispatched(
                M3NativePulsarPopulation.NativeOperationStage stage,
                M3AllocatorWorkloadPlan.Trigger exactTrigger,
                long startedNanos) {
            requireTrigger(exactTrigger);
            operations.incrementAndGet();
            if (firstDispatchNanos.compareAndSet(0, startedNanos)) {
                parent.admissionToFirstDispatchMicros.add(
                        TimeUnit.NANOSECONDS.toMicros(startedNanos - admittedNanos.get()));
            }
            parent.operationStarted();
        }

        @Override
        public void operationCompleted(
                M3NativePulsarPopulation.NativeOperationStage stage,
                M3AllocatorWorkloadPlan.Trigger exactTrigger,
                long startedNanos,
                long completedNanos,
                Throwable failure) {
            requireTrigger(exactTrigger);
            parent.operationCompleted();
        }

        @Override
        public void triggerAppendCompleted(M3AllocatorWorkloadPlan.Trigger exactTrigger, long completedNanos) {
            requireTrigger(exactTrigger);
            parent.triggerAppendCompletions.incrementAndGet();
        }

        @Override
        public void successorEstablished(M3AllocatorWorkloadPlan.Trigger exactTrigger, long completedNanos) {
            requireTrigger(exactTrigger);
            successorNanos.compareAndSet(0, completedNanos);
            parent.successorEstablishments.incrementAndGet();
        }

        private void terminal(Throwable failure) {
            if (!terminal.compareAndSet(0, 1)) {
                throw new IllegalStateException("native request terminal telemetry repeated");
            }
            parent.operationsPerRequest.add((long) operations.get());
            long successor = successorNanos.get();
            if (failure == null && successor > 0) {
                parent.rolloverMicros
                        .get(trigger)
                        .add(TimeUnit.NANOSECONDS.toMicros(successor - admittedNanos.get()));
            }
        }

        private void requireTrigger(M3AllocatorWorkloadPlan.Trigger exactTrigger) {
            if (trigger != exactTrigger) {
                throw new IllegalArgumentException("native request telemetry trigger drifted");
            }
        }
    }

    private static long percentile(List<Long> values, double percentile) {
        List<Long> ordered;
        synchronized (values) {
            if (values.isEmpty()) {
                return 0;
            }
            ordered = values.stream().sorted().toList();
        }
        int index = Math.max(0, (int) Math.ceil(ordered.size() * percentile) - 1);
        return ordered.get(index);
    }
}
