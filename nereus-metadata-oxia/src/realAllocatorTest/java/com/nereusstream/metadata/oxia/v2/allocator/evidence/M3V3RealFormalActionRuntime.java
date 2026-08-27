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

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Candidate;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Cell;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.IntervalEvidence;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Row;
import com.nereusstream.domain.registry.allocator.AllocatorEvidenceCandidateV1;
import com.nereusstream.domain.registry.allocator.AllocatorEvidenceContextV1;
import com.nereusstream.domain.registry.allocator.AllocatorFaultCutV1;
import com.nereusstream.domain.registry.allocator.AllocatorRawEvidenceEventV1;
import com.nereusstream.domain.registry.allocator.AllocatorRawEvidenceEventV1.EventKind;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3CandidateAllocatorPopulation.FormalAllocationObserver;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3FormalActionExecutorAdapter.FaultActionResult;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3FormalActionExecutorAdapter.IntervalActionResult;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3FormalActionExecutorAdapter.RealActionRuntime;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3FormalActionExecutorAdapter.ScaleActionResult;
import com.nereusstream.metadata.spi.allocator.BoundedVirtualLedgerAllocatorWorkflowV2.Result;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** Real native-Pulsar/Oxia action runtime used only by the explicitly authorized formal task. */
final class M3V3RealFormalActionRuntime implements RealActionRuntime, AutoCloseable {
    private final Path actionDirectory;
    private final String executionDiscriminator;
    private final ThreadPoolExecutor constructionAndFaultWorkers;
    private final ThreadPoolExecutor nativeDispatchWorkers;
    private final M3RealOxiaActors actors;
    private final Map<Candidate, M3CandidateAllocatorPopulation> candidatePopulations = new EnumMap<>(Candidate.class);
    private final Map<Candidate, Set<Long>> candidateAllocatedLedgerIds = new EnumMap<>(Candidate.class);
    private M3NativePulsarPopulation nativePopulation;

    M3V3RealFormalActionRuntime(Path outputDirectory, String oxiaServiceAddress, String executionDiscriminator)
            throws Exception {
        Path exactOutput = Objects.requireNonNull(outputDirectory, "outputDirectory").toAbsolutePath().normalize();
        if (!Files.isDirectory(exactOutput) || Files.isSymbolicLink(exactOutput)) {
            throw new IllegalArgumentException("allocator V3 formal output directory is absent or a link");
        }
        this.executionDiscriminator = requireSafeIdentity(executionDiscriminator);
        actionDirectory = Files.createDirectory(exactOutput.resolve("actions"));
        constructionAndFaultWorkers = M3RealAllocatorEvidenceTest.exactWorkers();
        constructionAndFaultWorkers.prestartAllCoreThreads();
        nativeDispatchWorkers = boundedNativeDispatchWorkers();
        nativeDispatchWorkers.prestartAllCoreThreads();
        actors = new M3RealOxiaActors(oxiaServiceAddress);
    }

    @Override
    public IntervalActionResult executeNativeInterval(Cell cell, int offeredRate) throws Exception {
        if (!cell.candidate().nativePath()) {
            throw new IllegalArgumentException("allocator native action received a candidate Cell");
        }
        M3NativePulsarPopulation population = nativePopulation();
        population.ensurePopulation(cell.activeManagedLedgers());
        population.setMetadataLatencyMillis(cell.metadataLatencyP99Millis());
        List<AllocatorRawEvidenceEventV1> raw = Collections.synchronizedList(new ArrayList<>());
        M3AllocatorRequestTelemetry telemetry = new M3AllocatorRequestTelemetry(raw::add, System.nanoTime());
        AllocatorEvidenceContextV1 context = AllocatorEvidenceContextV1.nativeContext(
                cell.activeManagedLedgers(),
                cell.metadataLatencyP99Millis(),
                offeredRate);
        List<M3V3AsyncActorLaneRunner.ScheduledOffer<NativeOffer>> schedule = new ArrayList<>();
        for (M3AllocatorWorkloadPlan.PlannedRequest request : M3AllocatorWorkloadPlan.requests(
                cell.activeManagedLedgers(), offeredRate)) {
            schedule.add(new M3V3AsyncActorLaneRunner.ScheduledOffer<>(
                    request.requestOrdinal(),
                    request.actorId(),
                    request.ledgerIndex(),
                    TimeUnit.MICROSECONDS.toNanos(request.arrivalOffsetMicros()),
                    request.phase() != M3AllocatorWorkloadPlan.Phase.WARM_UP,
                    new NativeOffer(request)));
        }
        M3V3AsyncActorLaneRunner<NativeOffer> runner = M3V3AsyncActorLaneRunner.formal();
        M3V3AsyncActorLaneRunner.IntervalResult interval;
        try {
            interval = runner.run(offeredRate, schedule, (actorId, offer, operationContext) -> {
                M3AllocatorWorkloadPlan.PlannedRequest request = offer.request();
                M3AllocatorRequestTelemetry.RequestTrace trace = telemetry.trace(context, request, null, 1);
                trace.offered();
                trace.enqueued();
                trace.dispatched();
                return java.util.concurrent.CompletableFuture.runAsync(() -> {
                    if (!operationContext.allowsNextMetadataOperation()) {
                        throw new java.util.concurrent.CompletionException(
                                new java.util.concurrent.TimeoutException(
                                        "allocator V3 cleanup deadline elapsed"));
                    }
                    try {
                        M3NativePulsarPopulation.NativeRollover rollover = population.rollover(
                                trace, request.ledgerIndex(), request.trigger());
                        trace.completed();
                        offer.allocatedLedgerId().set(rollover.successorLedgerId());
                    } catch (Throwable failure) {
                        trace.completeFailureLifecycle();
                        trace.failed();
                        throw new java.util.concurrent.CompletionException(failure);
                    }
                }, nativeDispatchWorkers);
            });
        } finally {
            population.setMetadataLatencyMillis(0);
        }
        Set<Long> allocated = new HashSet<>();
        long duplicate = 0;
        for (M3V3AsyncActorLaneRunner.ScheduledOffer<NativeOffer> offer : schedule) {
            long ledgerId = offer.request().allocatedLedgerId().get();
            if (ledgerId > 0 && !allocated.add(ledgerId)) {
                duplicate++;
            }
        }
        IntervalEvidence evidence = intervalEvidence(
                cell,
                offeredRate,
                interval,
                duplicate,
                0,
                0,
                interval.rolloverP99Micros());
        Attachment attachment = writeJsonAttachment("interval-" + cell.contextId(), intervalJson(evidence));
        boolean infrastructureValid = M3V3AllocatorFormalHarness.infrastructureValid(interval);
        return new IntervalActionResult(
                evidence,
                attachment.digest(),
                infrastructureValid,
                M3V3AllocatorFormalHarness.infrastructureDetail(interval));
    }

    @Override
    public IntervalActionResult executeCandidateInterval(Cell cell, int offeredRate) throws Exception {
        if (cell.candidate().nativePath()) {
            throw new IllegalArgumentException("allocator candidate action received a native Cell");
        }
        M3CandidateAllocatorPopulation population = candidatePopulation(cell.candidate());
        population.ensurePopulation(cell.activeManagedLedgers());
        actors.setControlledLatencyMillis(cell.metadataLatencyP99Millis());
        IntervalMeasurements measurements = new IntervalMeasurements(
                candidateAllocatedLedgerIds.computeIfAbsent(
                        cell.candidate(), ignored -> ConcurrentHashMap.newKeySet()));
        M3V3AllocatorFormalHarness harness = M3V3AllocatorFormalHarness.formalActors(
                population.formalActorEndpointsV3(cell, measurements));
        List<M3V3AsyncActorLaneRunner.ScheduledOffer<M3V3AllocatorFormalHarness.CandidateRequest>> schedule =
                new ArrayList<>();
        for (M3AllocatorWorkloadPlan.PlannedRequest request : M3AllocatorWorkloadPlan.requests(
                cell.activeManagedLedgers(), offeredRate)) {
            schedule.add(new M3V3AsyncActorLaneRunner.ScheduledOffer<>(
                    request.requestOrdinal(),
                    request.actorId(),
                    request.ledgerIndex(),
                    TimeUnit.MICROSECONDS.toNanos(request.arrivalOffsetMicros()),
                    request.phase() != M3AllocatorWorkloadPlan.Phase.WARM_UP,
                    new M3V3AllocatorFormalHarness.CandidateRequest(
                            request.requestOrdinal(), request.ledgerIndex(), request)));
        }
        M3V3AllocatorFormalHarness.HarnessResult result;
        try {
            result = harness.runCandidate(cell, offeredRate, schedule, measurements::supplementary);
        } finally {
            actors.setControlledLatencyMillis(0);
        }
        Attachment attachment = writeJsonAttachment("interval-" + cell.contextId(), intervalJson(result.evidence()));
        return new IntervalActionResult(
                result.evidence(),
                attachment.digest(),
                result.infrastructureValid(),
                M3V3AllocatorFormalHarness.infrastructureDetail(result.runnerResult()));
    }

    @Override
    public FaultActionResult executeFaultAction(Row row, AllocatorFaultCutV1 cut) throws Exception {
        Objects.requireNonNull(cut, "cut");
        M3CandidateAllocatorPopulation population = candidatePopulation(row.candidate());
        population.ensurePopulation(row.activeManagedLedgers());
        actors.setControlledLatencyMillis(row.metadataLatencyP99Millis());
        List<AllocatorRawEvidenceEventV1> raw = Collections.synchronizedList(new ArrayList<>());
        M3AllocatorRequestTelemetry telemetry = new M3AllocatorRequestTelemetry(raw::add, System.nanoTime());
        M3AllocatorFaultRunner runner = new M3AllocatorFaultRunner(constructionAndFaultWorkers, actors, telemetry);
        AllocatorEvidenceContextV1 context = AllocatorEvidenceContextV1.candidateContext(
                evidenceCandidate(row.candidate()),
                row.activeManagedLedgers(),
                row.metadataLatencyP99Millis(),
                200);
        try {
            runner.runOne(context, population, cut);
        } finally {
            actors.setControlledLatencyMillis(0);
        }
        FaultCounters counters = FaultCounters.from(raw);
        Attachment attachment = writeRawAttachment(
                "fault-" + row.candidate() + '-' + row.activeManagedLedgers() + '-'
                        + row.metadataLatencyP99Millis() + '-' + cut,
                raw);
        return new FaultActionResult(
                row,
                cut,
                counters.failed(),
                counters.timedOut(),
                counters.unexpectedErrors(),
                counters.failedAssertions(),
                counters.skipped(),
                counters.duplicateLedgerIds(),
                counters.reusedLedgerIds(),
                counters.permanentOrphans(),
                counters.staleCandidateBurnMaximum(),
                counters.massTakeoverRecoveryMicros(),
                attachment.digest(),
                true);
    }

    @Override
    public ScaleActionResult executeScaleAction(Row row) throws Exception {
        if (!row.candidate().range()) {
            throw new IllegalArgumentException("allocator scale action is not a RANGE row");
        }
        actors.setControlledLatencyMillis(0);
        long elapsedMicros = candidatePopulation(row.candidate()).ensurePopulation(row.activeManagedLedgers());
        String json = "{\"schema\":\"NEREUS_V2_M3_ALLOCATOR_SCALE_ACTION_V3\",\"candidate\":\""
                + row.candidate() + "\",\"activeManagedLedgers\":" + row.activeManagedLedgers()
                + ",\"metadataLatencyP99Millis\":" + row.metadataLatencyP99Millis()
                + ",\"elapsedMicros\":" + elapsedMicros
                + ",\"campaignAction\":true,\"selectionMetric\":false}\n";
        Attachment attachment = writeJsonAttachment(
                "scale-" + row.candidate() + '-' + row.activeManagedLedgers() + '-'
                        + row.metadataLatencyP99Millis(),
                json);
        return new ScaleActionResult(row, attachment.digest(), true);
    }

    private M3NativePulsarPopulation nativePopulation() throws Exception {
        if (nativePopulation == null) {
            nativePopulation = new M3NativePulsarPopulation(constructionAndFaultWorkers);
        }
        return nativePopulation;
    }

    private M3CandidateAllocatorPopulation candidatePopulation(Candidate candidate) {
        if (candidate.nativePath()) {
            throw new IllegalArgumentException("native path has no Oxia candidate population");
        }
        return candidatePopulations.computeIfAbsent(candidate, exact -> new M3CandidateAllocatorPopulation(
                evidenceCandidate(exact),
                exact.ordinal() - 1,
                executionDiscriminator,
                actors,
                constructionAndFaultWorkers));
    }

    private static AllocatorEvidenceCandidateV1 evidenceCandidate(Candidate candidate) {
        if (candidate.strict()) {
            return AllocatorEvidenceCandidateV1.strict();
        }
        if (candidate.range()) {
            return AllocatorEvidenceCandidateV1.range(candidate.rangeSize());
        }
        throw new IllegalArgumentException("native campaign path has no allocator candidate");
    }

    private static IntervalEvidence intervalEvidence(
            Cell cell,
            int offeredRate,
            M3V3AsyncActorLaneRunner.IntervalResult interval,
            long duplicateLedgerIds,
            long reusedLedgerIds,
            long oxiaOperationP99Micros,
            long appendStallP99Micros) {
        return new IntervalEvidence(
                cell,
                offeredRate,
                interval.offered(),
                interval.admitted(),
                interval.overloadDroppedBeforeAdmission(),
                interval.completed(),
                interval.failedAfterAdmission(),
                interval.timedOutAfterAdmission(),
                interval.terminal(),
                0,
                0,
                0,
                duplicateLedgerIds,
                reusedLedgerIds,
                interval.rolloverP99Micros(),
                oxiaOperationP99Micros,
                interval.queueWaitP99Micros(),
                interval.queueDepthMaximum(),
                interval.queueWaitMaximumMicros(),
                appendStallP99Micros,
                interval.queueDepthAtEnd(),
                interval.globalOutstandingAtEnd(),
                interval.pendingPermitAtEnd());
    }

    private Attachment writeJsonAttachment(String identity, String json) throws Exception {
        return writeAttachment(identity, json.getBytes(StandardCharsets.UTF_8), ".json");
    }

    private Attachment writeRawAttachment(
            String identity, List<AllocatorRawEvidenceEventV1> events) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream(events.size() * AllocatorRawEvidenceEventV1.BYTES);
        for (AllocatorRawEvidenceEventV1 event : events) {
            output.write(event.encode().toByteArray());
        }
        return writeAttachment(identity, output.toByteArray(), ".nare1");
    }

    private Attachment writeAttachment(String identity, byte[] bytes, String suffix) throws Exception {
        CanonicalBytes canonical = CanonicalBytes.copyOf(bytes);
        Sha256Digest digest = Sha256Digest.hash(canonical);
        Path target = actionDirectory.resolve(requireSafeIdentity(identity) + '-' + digest.toHex() + suffix);
        Files.write(
                target,
                bytes,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.DSYNC);
        return new Attachment(target, digest);
    }

    private static String intervalJson(IntervalEvidence value) {
        return "{\"schema\":\"NEREUS_V2_M3_ALLOCATOR_INTERVAL_ACTION_V3\",\"contextId\":"
                + value.cell().contextId() + ",\"offeredRate\":" + value.offeredRate()
                + ",\"offered\":" + value.offered() + ",\"admitted\":"
                + value.admitted() + ",\"overloadDroppedBeforeAdmission\":"
                + value.overloadDroppedBeforeAdmission() + ",\"completed\":" + value.completed()
                + ",\"failedAfterAdmission\":" + value.failedAfterAdmission()
                + ",\"timedOutAfterAdmission\":" + value.timedOutAfterAdmission() + ",\"terminal\":"
                + value.terminal() + ",\"duplicateLedgerIds\":" + value.duplicateLedgerIds()
                + ",\"reusedLedgerIds\":" + value.reusedLedgerIds() + ",\"rolloverP99Micros\":"
                + value.rolloverP99Micros() + ",\"oxiaOperationP99Micros\":"
                + value.oxiaOperationP99Micros() + ",\"queueAgeP99Micros\":"
                + value.queueAgeP99Micros() + ",\"queueDepthMaximum\":" + value.queueDepthMaximum()
                + ",\"starvationMaximumMicros\":" + value.starvationMaximumMicros()
                + ",\"appendStallP99Micros\":" + value.appendStallP99Micros() + ",\"backlogAtEnd\":"
                + value.backlogAtEnd() + ",\"inFlightAtEnd\":" + value.inFlightAtEnd()
                + ",\"waiterCountAtEnd\":" + value.waiterCountAtEnd() + "}\n";
    }

    private static String requireSafeIdentity(String value) {
        Objects.requireNonNull(value, "identity");
        if (!value.matches("[A-Za-z0-9_.-]+")) {
            throw new IllegalArgumentException("allocator formal identity is unsafe");
        }
        return value;
    }

    @Override
    public void close() throws Exception {
        Exception failure = null;
        if (nativePopulation != null) {
            try {
                nativePopulation.close();
            } catch (Exception closeFailure) {
                failure = closeFailure;
            }
        }
        try {
            actors.close();
        } catch (Exception closeFailure) {
            if (failure == null) {
                failure = closeFailure;
            } else {
                failure.addSuppressed(closeFailure);
            }
        }
        constructionAndFaultWorkers.shutdownNow();
        nativeDispatchWorkers.shutdownNow();
        if (!constructionAndFaultWorkers.awaitTermination(5, TimeUnit.MINUTES)) {
            IllegalStateException termination = new IllegalStateException("allocator formal worker pool did not stop");
            if (failure == null) {
                failure = termination;
            } else {
                failure.addSuppressed(termination);
            }
        }
        if (!nativeDispatchWorkers.awaitTermination(5, TimeUnit.MINUTES)) {
            IllegalStateException termination =
                    new IllegalStateException("allocator V3 native dispatch worker pool did not stop");
            if (failure == null) {
                failure = termination;
            } else {
                failure.addSuppressed(termination);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private record NativeOffer(M3AllocatorWorkloadPlan.PlannedRequest request, AtomicLong allocatedLedgerId) {
        private NativeOffer(M3AllocatorWorkloadPlan.PlannedRequest request) {
            this(request, new AtomicLong());
        }
    }

    private static ThreadPoolExecutor boundedNativeDispatchWorkers() {
        AtomicInteger threadId = new AtomicInteger();
        ThreadFactory factory = runnable -> {
            Thread thread = new Thread(runnable, "m3-v3-native-dispatch-" + threadId.getAndIncrement());
            thread.setDaemon(false);
            return thread;
        };
        return new ThreadPoolExecutor(
                M3V3AsyncActorLaneRunner.ACTOR_COUNT,
                M3V3AsyncActorLaneRunner.ACTOR_COUNT,
                0,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(M3V3AsyncActorLaneRunner.MAX_GLOBAL_OUTSTANDING),
                factory,
                new ThreadPoolExecutor.AbortPolicy());
    }

    private static final class IntervalMeasurements implements FormalAllocationObserver {
        private final Set<Long> campaignLedgerIds;
        private final Set<Long> intervalLedgerIds = ConcurrentHashMap.newKeySet();
        private final List<Long> operationMicros = Collections.synchronizedList(new ArrayList<>());
        private final AtomicLong duplicates = new AtomicLong();
        private final AtomicLong reused = new AtomicLong();

        private IntervalMeasurements(Set<Long> campaignLedgerIds) {
            this.campaignLedgerIds = Objects.requireNonNull(campaignLedgerIds, "campaignLedgerIds");
        }

        @Override
        public void completed(
                int actorId, M3AllocatorWorkloadPlan.PlannedRequest request, Result result, long elapsedMicros) {
            long ledgerId = result.exactNode().value().ledgerId();
            if (!intervalLedgerIds.add(ledgerId)) {
                duplicates.incrementAndGet();
            }
            if (!campaignLedgerIds.add(ledgerId)) {
                reused.incrementAndGet();
            }
            operationMicros.add(elapsedMicros);
        }

        @Override
        public void failed(
                int actorId, M3AllocatorWorkloadPlan.PlannedRequest request, Throwable failure, long elapsedMicros) {
            operationMicros.add(elapsedMicros);
        }

        M3V3AllocatorFormalHarness.SupplementaryMeasurements supplementary() {
            long p99 = p99(operationMicros);
            return new M3V3AllocatorFormalHarness.SupplementaryMeasurements(
                    0, 0, 0, duplicates.get(), reused.get(), p99, p99);
        }
    }

    private static long p99(List<Long> values) {
        if (values.isEmpty()) {
            return 0;
        }
        List<Long> ordered;
        synchronized (values) {
            ordered = values.stream().sorted().toList();
        }
        int index = (int) Math.ceil(ordered.size() * 0.99) - 1;
        return ordered.get(Math.max(0, index));
    }

    private record FaultCounters(
            long failed,
            long timedOut,
            long unexpectedErrors,
            long failedAssertions,
            long skipped,
            long duplicateLedgerIds,
            long reusedLedgerIds,
            long permanentOrphans,
            long staleCandidateBurnMaximum,
            long massTakeoverRecoveryMicros) {
        static FaultCounters from(List<AllocatorRawEvidenceEventV1> events) {
            long failed = count(events, EventKind.FAILED);
            long timedOut = count(events, EventKind.TIMED_OUT);
            long unexpected = count(events, EventKind.UNEXPECTED_ERROR);
            long assertions = count(events, EventKind.ASSERTION_FAILED);
            long skipped = count(events, EventKind.TEST_SKIPPED);
            long orphans = count(events, EventKind.PERMANENT_ORPHAN);
            long stale = count(events, EventKind.STALE_CANDIDATE_BURN);
            Set<Long> allocated = new HashSet<>();
            long duplicates = 0;
            long ownerLoss = Long.MAX_VALUE;
            long recovery = 0;
            for (AllocatorRawEvidenceEventV1 event : events) {
                if (event.kind() == EventKind.ALLOCATED_LEDGER_ID
                        && event.allocatedLedgerId() > 0
                        && !allocated.add(event.allocatedLedgerId())) {
                    duplicates++;
                }
                if (event.kind() == EventKind.OWNER_LOSS_DETECTED) {
                    ownerLoss = Math.min(ownerLoss, event.monotonicTimestampMicros());
                }
                if (event.kind() == EventKind.FRESH_OWNER_APPEND_COMPLETE && ownerLoss != Long.MAX_VALUE) {
                    recovery = Math.max(recovery, event.monotonicTimestampMicros() - ownerLoss);
                }
            }
            return new FaultCounters(
                    failed, timedOut, unexpected, assertions, skipped, duplicates, 0, orphans, stale, recovery);
        }

        private static long count(List<AllocatorRawEvidenceEventV1> events, EventKind kind) {
            return events.stream().filter(event -> event.kind() == kind).count();
        }
    }

    private record Attachment(Path path, Sha256Digest digest) {}
}
