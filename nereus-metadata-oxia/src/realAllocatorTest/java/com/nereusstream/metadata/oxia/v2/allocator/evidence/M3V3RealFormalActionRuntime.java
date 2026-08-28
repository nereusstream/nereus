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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Real native-Pulsar/Oxia action runtime used only by the explicitly authorized formal task. */
final class M3V3RealFormalActionRuntime implements RealActionRuntime, AutoCloseable {
    private final Path actionDirectory;
    private final String executionDiscriminator;
    private final int protocolVersion;
    private final String attachmentProtocol;
    private final ThreadPoolExecutor constructionAndFaultWorkers;
    private final M3RealOxiaActors actors;
    private final Map<Candidate, M3CandidateAllocatorPopulation> candidatePopulations = new EnumMap<>(Candidate.class);
    private final Map<Candidate, Set<Long>> candidateAllocatedLedgerIds = new EnumMap<>(Candidate.class);
    private M3NativePulsarPopulation nativePopulation;
    private M3V3NativeIntervalRuntime nativeIntervalRuntime;

    M3V3RealFormalActionRuntime(Path outputDirectory, String oxiaServiceAddress, String executionDiscriminator)
            throws Exception {
        this(outputDirectory, oxiaServiceAddress, executionDiscriminator, 3);
    }

    M3V3RealFormalActionRuntime(
            Path outputDirectory,
            String oxiaServiceAddress,
            String executionDiscriminator,
            boolean terminalDrainV4)
            throws Exception {
        this(outputDirectory, oxiaServiceAddress, executionDiscriminator, terminalDrainV4 ? 4 : 3);
    }

    M3V3RealFormalActionRuntime(
            Path outputDirectory,
            String oxiaServiceAddress,
            String executionDiscriminator,
            int protocolVersion)
            throws Exception {
        Path exactOutput = Objects.requireNonNull(outputDirectory, "outputDirectory").toAbsolutePath().normalize();
        if (!Files.isDirectory(exactOutput) || Files.isSymbolicLink(exactOutput)) {
            throw new IllegalArgumentException("allocator V3 formal output directory is absent or a link");
        }
        this.executionDiscriminator = requireSafeIdentity(executionDiscriminator);
        if (protocolVersion < 3 || protocolVersion > 5) {
            throw new IllegalArgumentException("allocator formal action runtime protocol version differs");
        }
        this.protocolVersion = protocolVersion;
        attachmentProtocol = "V" + protocolVersion;
        actionDirectory = Files.createDirectory(exactOutput.resolve("actions"));
        constructionAndFaultWorkers = M3RealAllocatorEvidenceTest.exactWorkers();
        constructionAndFaultWorkers.prestartAllCoreThreads();
        actors = new M3RealOxiaActors(oxiaServiceAddress);
    }

    @Override
    public IntervalActionResult executeNativeInterval(Cell cell, int offeredRate) throws Exception {
        if (!cell.candidate().nativePath()) {
            throw new IllegalArgumentException("allocator native action received a candidate Cell");
        }
        List<AllocatorRawEvidenceEventV1> raw = Collections.synchronizedList(new ArrayList<>());
        M3V3NativeIntervalRuntime.Result nativeResult = nativeIntervalRuntime().run(
                cell.activeManagedLedgers(), cell.metadataLatencyP99Millis(), offeredRate, raw::add);
        M3V3AsyncActorLaneRunner.IntervalResult interval = nativeResult.interval();
        IntervalEvidence evidence = intervalEvidence(
                cell,
                offeredRate,
                interval,
                nativeResult.duplicateLedgerIds(),
                0,
                0,
                interval.rolloverP99Micros());
        Attachment attachment = writeJsonAttachment("interval-" + cell.contextId(), intervalJson(evidence, interval));
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
        List<M3V3AllocatorFormalHarness.ActorEndpoint> endpoints =
                population.formalActorEndpointsV3(cell, measurements);
        M3V3AllocatorFormalHarness harness = switch (protocolVersion) {
            case 3 -> M3V3AllocatorFormalHarness.formalActors(endpoints);
            case 4 -> M3V3AllocatorFormalHarness.formalActorsV4(endpoints);
            case 5 -> M3V3AllocatorFormalHarness.formalActorsV5(endpoints);
            default -> throw new IllegalStateException("allocator formal action runtime protocol version differs");
        };
        List<M3V3AsyncActorLaneRunner.ScheduledOffer<M3V3AllocatorFormalHarness.CandidateRequest>> schedule =
                candidateSchedule(cell.activeManagedLedgers(), offeredRate);
        M3V3AllocatorFormalHarness.HarnessResult result;
        try {
            result = harness.runCandidate(cell, offeredRate, schedule, measurements::supplementary);
        } finally {
            actors.setControlledLatencyMillis(0);
        }
        Attachment attachment = writeJsonAttachment(
                "interval-" + cell.contextId(), intervalJson(result.evidence(), result.runnerResult()));
        return new IntervalActionResult(
                result.evidence(),
                attachment.digest(),
                result.infrastructureValid(),
                M3V3AllocatorFormalHarness.infrastructureDetail(result.runnerResult()));
    }

    static List<M3V3AsyncActorLaneRunner.ScheduledOffer<M3V3AllocatorFormalHarness.CandidateRequest>>
            candidateSchedule(int activePopulation, int offeredRate) {
        List<M3V3AsyncActorLaneRunner.ScheduledOffer<M3V3AllocatorFormalHarness.CandidateRequest>> schedule =
                new ArrayList<>();
        for (M3AllocatorWorkloadPlan.PlannedRequest request :
                M3AllocatorWorkloadPlan.v3Requests(activePopulation, offeredRate)) {
            schedule.add(new M3V3AsyncActorLaneRunner.ScheduledOffer<>(
                    request.requestOrdinal(),
                    request.actorId(),
                    request.ledgerIndex(),
                    TimeUnit.MICROSECONDS.toNanos(request.arrivalOffsetMicros()),
                    request.phase() != M3AllocatorWorkloadPlan.Phase.WARM_UP,
                    new M3V3AllocatorFormalHarness.CandidateRequest(
                            request.requestOrdinal(), request.ledgerIndex(), request)));
        }
        return List.copyOf(schedule);
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
        String json = "{\"schema\":\"NEREUS_V2_M3_ALLOCATOR_SCALE_ACTION_" + attachmentProtocol
                + "\",\"candidate\":\""
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

    private M3V3NativeIntervalRuntime nativeIntervalRuntime() throws Exception {
        if (nativeIntervalRuntime == null) {
            nativeIntervalRuntime = new M3V3NativeIntervalRuntime(nativePopulation(), protocolVersion);
        }
        return nativeIntervalRuntime;
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

    private String intervalJson(
            IntervalEvidence value, M3V3AsyncActorLaneRunner.IntervalResult interval) {
        return "{\"schema\":\"NEREUS_V2_M3_ALLOCATOR_INTERVAL_ACTION_" + attachmentProtocol
                + "\",\"contextId\":"
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
                + ",\"waiterCountAtEnd\":" + value.waiterCountAtEnd() + ",\"warmupOffered\":"
                + interval.warmupOffered() + ",\"warmupDroppedBeforeAdmission\":"
                + interval.warmupDroppedBeforeAdmission() + ",\"warmupCompleted\":"
                + interval.warmupCompleted() + ",\"warmupFailedAfterAdmission\":"
                + interval.warmupFailedAfterAdmission() + ",\"warmupLoadRejectedAfterAdmission\":"
                + interval.warmupLoadRejectedAfterAdmission() + ",\"warmupUnexpectedFailedAfterAdmission\":"
                + interval.warmupUnexpectedFailedAfterAdmission() + ",\"warmupTimedOutAfterAdmission\":"
                + interval.warmupTimedOutAfterAdmission() + ",\"warmupFirstFailure\":"
                + jsonString(interval.warmupFirstFailure()) + ",\"actorLanesStoppedAtCleanupDeadline\":"
                + interval.actorLanesStoppedAtCleanupDeadline() + "}\n";
    }

    private static String jsonString(String value) {
        return '"' + value.replace("\\", "\\\\").replace("\"", "\\\"") + '"';
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
        if (!constructionAndFaultWorkers.awaitTermination(5, TimeUnit.MINUTES)) {
            IllegalStateException termination = new IllegalStateException("allocator formal worker pool did not stop");
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
