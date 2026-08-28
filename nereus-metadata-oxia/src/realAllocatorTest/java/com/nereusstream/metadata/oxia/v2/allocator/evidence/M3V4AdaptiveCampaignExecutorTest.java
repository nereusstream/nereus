/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.metadata.oxia.v2.allocator.evidence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.SourceBinding;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.Status;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV4;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignEvaluationSealV4;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Candidate;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Cell;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.IntervalEvidence;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Row;
import com.nereusstream.domain.registry.allocator.AllocatorFaultCutV1;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AdaptiveCampaignExecutor.TerminalReason;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3FormalActionExecutorAdapter.FaultActionResult;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3FormalActionExecutorAdapter.IntervalActionResult;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3FormalActionExecutorAdapter.RealActionRuntime;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3FormalActionExecutorAdapter.ScaleActionResult;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3FormalCampaignPlan.ActionKind;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3FormalCampaignPlan.PlannedActionV3;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class M3V4AdaptiveCampaignExecutorTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void physicalInventoryIsUnchangedButPlanAndBudgetIdentityAreV4() {
        List<PlannedActionV3> actions = M3V4FormalCampaignPlan.zeroDecisionActions();

        assertThat(actions).hasSize(720);
        assertThat(actions.stream().collect(Collectors.groupingBy(PlannedActionV3::kind, Collectors.counting())))
                .containsEntry(ActionKind.NATIVE_INTERVAL, 48L)
                .containsEntry(ActionKind.CANDIDATE_INTERVAL, 280L)
                .containsEntry(ActionKind.FAULT_ACTION, 360L)
                .containsEntry(ActionKind.SCALE_ACTION, 32L);
        assertThat(M3V4FormalCampaignPlan.zeroDecisionPlanDigest().toHex())
                .isEqualTo("1121c56cb6cd59c319c7d2eacedc8de9978bcbc2edc0008f08ef87393e0eb975");
    }

    @Test
    void strictCampaignPersistsOnlyNacp4WithExactOuterLineageAndBudget() throws Exception {
        CollectingSink sink = new CollectingSink();
        FakeRuntime runtime = new FakeRuntime();
        M3V4AdaptiveCampaignExecutor.Result result = executor(source("a"), runtime, sink, () -> false).start();
        AllocatorCampaignCheckpointV4 checkpoint = AllocatorCampaignCheckpointV4.decode(result.checkpointBytes());

        assertThat(result.completed()).isTrue();
        assertThat(result.reason()).isEqualTo(TerminalReason.COMPLETED);
        assertThat(checkpoint.status()).isEqualTo(Status.COMPLETED);
        assertThat(checkpoint.executionRecords()).hasSize(28);
        assertThat(checkpoint.campaign().observations().stream().filter(IntervalEvidence.class::isInstance))
                .hasSize(20);
        assertThat(checkpoint.remainingBudgets().intervalSeconds()).isEqualTo(12_936);
        assertThat(checkpoint.remainingBudgets().cleanupSeconds()).isEqualTo(1_540);
        assertThat(sink.values).hasSize(29);
        assertThat(runtime.calls()).isEqualTo(96);
        assertThat(AllocatorCampaignEvaluationSealV4.decode(
                                AllocatorCampaignEvaluationSealV4.seal(result.checkpointBytes()))
                        .selectedCandidate())
                .contains(Candidate.STRICT);
        assertExactLineage(sink.values);
    }

    @Test
    void stopResumeAndActionFailureRemainFailClosedInV4Authority() throws Exception {
        CollectingSink firstSink = new CollectingSink();
        FakeRuntime first = new FakeRuntime();
        SourceBinding source = source("b");
        M3V4AdaptiveCampaignExecutor.Result stopped = executor(source, first, firstSink, () -> first.calls() >= 3)
                .start();
        AllocatorCampaignCheckpointV4 interrupted = AllocatorCampaignCheckpointV4.decode(stopped.checkpointBytes());

        assertThat(stopped.reason()).isEqualTo(TerminalReason.STOP_REQUESTED);
        assertThat(interrupted.status()).isEqualTo(Status.INTERRUPTED);
        assertThat(interrupted.executionRecords()).hasSize(3);
        assertThat(interrupted.checkpointSequence()).isEqualTo(4);
        CollectingSink resumeSink = new CollectingSink(interrupted.checkpointSequence() + 1);
        M3V4AdaptiveCampaignExecutor.Result resumed = executor(source, new FakeRuntime(), resumeSink, () -> false)
                .resume(stopped.checkpointBytes());
        assertThat(resumed.completed()).isTrue();
        assertThat(AllocatorCampaignCheckpointV4.decode(resumed.checkpointBytes()).executionRecords())
                .hasSize(28);

        CollectingSink failureSink = new CollectingSink();
        RealActionRuntime failing = new FakeRuntime() {
            @Override
            public IntervalActionResult executeNativeInterval(Cell cell, int offeredRate) throws Exception {
                throw new IOException("V4 action failure");
            }
        };
        M3V4AdaptiveCampaignExecutor.Result failed = executor(source("c"), failing, failureSink, () -> false).start();
        AllocatorCampaignCheckpointV4 failedCheckpoint =
                AllocatorCampaignCheckpointV4.decode(failed.checkpointBytes());
        assertThat(failed.status()).isEqualTo(Status.INFRASTRUCTURE_FAILED);
        assertThat(failed.reason()).isEqualTo(TerminalReason.INFRASTRUCTURE_FAILED);
        assertThat(failed.detail()).contains("V4 action failure");
        assertThat(failedCheckpoint.executionRecords()).isEmpty();
        assertThat(failedCheckpoint.remainingBudgets().intervalSeconds()).isEqualTo(13_734);
        assertThatThrownBy(() -> AllocatorCampaignEvaluationSealV4.seal(failed.checkpointBytes()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void createNewCheckpointSinkNeverOverwrites() throws Exception {
        Path directory = Files.createDirectory(temporaryDirectory.resolve("checkpoints"));
        M3V4AdaptiveCampaignExecutor.CheckpointSink sink =
                M3V4AdaptiveCampaignExecutor.CheckpointSink.createNewDirectory(directory);
        CanonicalBytes checkpoint = new CollectingSinkResult().initial(source("d"));

        sink.persist(0, checkpoint);

        assertThatThrownBy(() -> sink.persist(0, checkpoint))
                .isInstanceOf(java.nio.file.FileAlreadyExistsException.class);
        try (var files = Files.list(directory)) {
            assertThat(files).hasSize(1).allMatch(path -> path.getFileName().toString().endsWith(".nacp4"));
        }
    }

    private static M3V4AdaptiveCampaignExecutor executor(
            SourceBinding source,
            RealActionRuntime runtime,
            M3V4AdaptiveCampaignExecutor.CheckpointSink sink,
            M3V3AdaptiveCampaignExecutor.StopSignal stopSignal) {
        return new M3V4AdaptiveCampaignExecutor(
                source,
                new M3V3FormalActionExecutorAdapter(runtime),
                sink,
                stopSignal,
                M3V3AdaptiveCampaignExecutor.HardDeadline.never());
    }

    private static void assertExactLineage(List<CanonicalBytes> checkpoints) {
        for (int index = 0; index < checkpoints.size(); index++) {
            AllocatorCampaignCheckpointV4 current = AllocatorCampaignCheckpointV4.decode(checkpoints.get(index));
            assertThat(current.checkpointSequence()).isEqualTo(index);
            if (index > 0) {
                assertThat(current.predecessorCheckpointDigest())
                        .isEqualTo(AllocatorCampaignCheckpointV4.digest(checkpoints.get(index - 1)));
            }
        }
    }

    private static SourceBinding source(String value) {
        return new SourceBinding(
                value.repeat(40),
                digest("oxia-" + value),
                digest("dependency-" + value),
                digest("executor-" + value),
                M3V4FormalCampaignPlan.zeroDecisionPlanDigest());
    }

    private static IntervalEvidence passing(Cell cell, int offeredRate) {
        long offered = (long) offeredRate * AllocatorCampaignV3.MEASURED_SECONDS;
        return new IntervalEvidence(
                cell,
                offeredRate,
                offered,
                offered,
                0,
                offered,
                0,
                0,
                offered,
                0,
                0,
                0,
                0,
                0,
                100_000,
                100_000,
                100_000,
                offeredRate,
                100_000,
                100_000,
                0,
                0,
                0);
    }

    private static IntervalEvidence relativeFailure(IntervalEvidence passing) {
        return new IntervalEvidence(
                passing.cell(),
                passing.offeredRate(),
                passing.offered(),
                passing.admitted(),
                0,
                passing.completed(),
                0,
                0,
                passing.terminal(),
                0,
                0,
                0,
                0,
                0,
                passing.rolloverP99Micros(),
                passing.oxiaOperationP99Micros(),
                passing.queueAgeP99Micros(),
                passing.queueDepthMaximum(),
                passing.starvationMaximumMicros(),
                400_001,
                0,
                0,
                0);
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8)));
    }

    private static final class CollectingSink implements M3V4AdaptiveCampaignExecutor.CheckpointSink {
        private final List<CanonicalBytes> values = new ArrayList<>();
        private long expectedSequence;

        private CollectingSink() {}

        private CollectingSink(long expectedSequence) {
            this.expectedSequence = expectedSequence;
        }

        @Override
        public void persist(long sequence, CanonicalBytes checkpointBytes) {
            if (sequence != expectedSequence) {
                throw new AssertionError("unexpected checkpoint sequence " + sequence);
            }
            values.add(checkpointBytes);
            expectedSequence++;
        }
    }

    private static class FakeRuntime implements RealActionRuntime {
        private final AtomicInteger calls = new AtomicInteger();

        int calls() {
            return calls.get();
        }

        @Override
        public IntervalActionResult executeNativeInterval(Cell cell, int offeredRate) throws Exception {
            calls.incrementAndGet();
            return intervalResult(passing(cell, offeredRate));
        }

        @Override
        public IntervalActionResult executeCandidateInterval(Cell cell, int offeredRate) throws Exception {
            calls.incrementAndGet();
            IntervalEvidence evidence = passing(cell, offeredRate);
            return intervalResult(cell.candidate().strict() ? evidence : relativeFailure(evidence));
        }

        @Override
        public FaultActionResult executeFaultAction(Row row, AllocatorFaultCutV1 cut) throws Exception {
            calls.incrementAndGet();
            return new FaultActionResult(
                    row,
                    cut,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    1,
                    cut == AllocatorFaultCutV1.BROKER_SESSION_CRASH_MASS_TAKEOVER ? 1_000_000 : 0,
                    digest("fault-" + row + '-' + cut),
                    true);
        }

        @Override
        public ScaleActionResult executeScaleAction(Row row) throws Exception {
            calls.incrementAndGet();
            return new ScaleActionResult(row, digest("scale-" + row), true);
        }

        private static IntervalActionResult intervalResult(IntervalEvidence evidence) {
            return new IntervalActionResult(
                    evidence, digest("interval-" + evidence.cell().contextId()), true, "");
        }
    }

    private static final class CollectingSinkResult {
        private CanonicalBytes initial(SourceBinding source) throws Exception {
            CollectingSink sink = new CollectingSink();
            FakeRuntime runtime = new FakeRuntime();
            M3V4AdaptiveCampaignExecutor.Result stopped = executor(source, runtime, sink, () -> true).start();
            return sink.values.getFirst();
        }
    }
}
