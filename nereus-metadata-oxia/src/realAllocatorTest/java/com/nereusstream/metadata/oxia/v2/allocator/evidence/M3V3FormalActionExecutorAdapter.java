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
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Candidate;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Cell;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.ExecuteCell;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.ExecuteFaultRow;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.FaultEvidence;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.IntervalEvidence;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.RequiredAction;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Row;
import com.nereusstream.domain.registry.allocator.AllocatorFaultCutV1;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AdaptiveCampaignExecutor.ActionExecutor;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AdaptiveCampaignExecutor.ActionResult;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AdaptiveCampaignExecutor.BudgetCharge;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AdaptiveCampaignExecutor.PhysicalActionResult;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3FormalCampaignPlan.ActionKind;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3FormalCampaignPlan.PlannedActionV3;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;

/**
 * Maps one frozen physical action to exactly one real runtime call. Population preparation is charged once per
 * candidate/population boundary and never introduces a hidden interval, fault cut, or scale row.
 */
final class M3V3FormalActionExecutorAdapter implements ActionExecutor {
    private static final long SETUP_SECONDS = 900;
    private static final long POPULATION_PATH_SECONDS = 900;
    private static final long FAULT_ACTION_SECONDS = 20;
    private static final long SCALE_PATH_SECONDS = 900;
    private static final long INTERVAL_SECONDS = 40;
    private static final long CLEANUP_SECONDS = 5;

    private final RealActionRuntime runtime;

    M3V3FormalActionExecutorAdapter(RealActionRuntime runtime) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
    }

    @Override
    public BudgetCharge budgetFor(PlannedActionV3 action) {
        Objects.requireNonNull(action, "action");
        PopulationKey key = populationKey(action);
        boolean firstPopulationAction = key != null && firstPopulationAction(action);
        long setup = firstCampaignAction(action) ? SETUP_SECONDS : 0;
        long population = firstPopulationAction && key.population() == 10_000 ? POPULATION_PATH_SECONDS : 0;
        long scale = firstPopulationAction && key.population() == 100_000 ? SCALE_PATH_SECONDS : 0;
        long fault = action.kind() == ActionKind.FAULT_ACTION ? FAULT_ACTION_SECONDS : 0;
        long interval = action.kind().interval() ? INTERVAL_SECONDS : 0;
        long cleanup = action.kind().interval() ? CLEANUP_SECONDS : 0;
        return new BudgetCharge(setup, population, fault, scale, interval, cleanup, 0);
    }

    @Override
    public PhysicalActionResult execute(PlannedActionV3 action) throws Exception {
        Objects.requireNonNull(action, "action");
        RuntimeActionResult result = switch (action.kind()) {
            case NATIVE_INTERVAL -> runtime.executeNativeInterval(action.cell(), action.offeredRate());
            case CANDIDATE_INTERVAL -> runtime.executeCandidateInterval(action.cell(), action.offeredRate());
            case FAULT_ACTION -> runtime.executeFaultAction(action.row(), action.faultCut());
            case SCALE_ACTION -> runtime.executeScaleAction(action.row());
        };
        validateRuntimeResult(action, result);
        return new PhysicalActionResult(
                action, result, result.attachmentDigest(), result.infrastructureValid());
    }

    @Override
    public ActionResult complete(RequiredAction required, List<PhysicalActionResult> physicalResults) {
        Objects.requireNonNull(required, "required");
        List<PhysicalActionResult> results = List.copyOf(
                Objects.requireNonNull(physicalResults, "physicalResults"));
        if (required instanceof ExecuteCell executeCell) {
            return completeInterval(executeCell.cell(), results);
        }
        if (required instanceof ExecuteFaultRow executeFaultRow) {
            return completeFault(executeFaultRow.row(), results);
        }
        throw new IllegalArgumentException("allocator V3 completed an unknown validator action");
    }

    private static ActionResult completeInterval(Cell cell, List<PhysicalActionResult> results) {
        if (results.isEmpty() || results.size() > 2) {
            throw new IllegalArgumentException("allocator V3 interval physical action count differs");
        }
        int intervalIndex = results.size() - 1;
        if (results.size() == 2) {
            PlannedActionV3 scale = results.get(0).action();
            if (!cell.candidate().range()
                    || scale.kind() != ActionKind.SCALE_ACTION
                    || !scale.row().equals(cell.row())) {
                throw new IllegalArgumentException("allocator V3 interval scale prerequisite differs");
            }
        }
        PhysicalActionResult intervalResult = results.get(intervalIndex);
        PlannedActionV3 intervalAction = intervalResult.action();
        if (!intervalAction.kind().interval()
                || !intervalAction.cell().equals(cell)
                || !(intervalResult.rawEvidence() instanceof IntervalActionResult runtimeResult)
                || !runtimeResult.evidence().cell().equals(cell)
                || runtimeResult.evidence().offeredRate() != intervalAction.offeredRate()) {
            throw new IllegalArgumentException("allocator V3 interval runtime result differs from planned cell");
        }
        return new ActionResult(
                runtimeResult.evidence(),
                aggregateDigest(results),
                results.stream().allMatch(PhysicalActionResult::infrastructureValid),
                infrastructureDetail(results));
    }

    private static ActionResult completeFault(Row row, List<PhysicalActionResult> results) {
        if (results.size() != AllocatorFaultCutV1.values().length) {
            throw new IllegalArgumentException("allocator V3 fault row does not contain all nine physical cuts");
        }
        EnumSet<AllocatorFaultCutV1> cuts = EnumSet.noneOf(AllocatorFaultCutV1.class);
        long failed = 0;
        long timedOut = 0;
        long unexpectedErrors = 0;
        long failedAssertions = 0;
        long skipped = 0;
        long duplicateLedgerIds = 0;
        long reusedLedgerIds = 0;
        long permanentOrphans = 0;
        long staleCandidateBurnMaximum = 0;
        long massTakeoverRecoveryMicros = 0;
        for (int index = 0; index < results.size(); index++) {
            PhysicalActionResult result = results.get(index);
            PlannedActionV3 action = result.action();
            AllocatorFaultCutV1 expectedCut = AllocatorFaultCutV1.values()[index];
            if (action.kind() != ActionKind.FAULT_ACTION
                    || !action.row().equals(row)
                    || action.faultCut() != expectedCut
                    || !(result.rawEvidence() instanceof FaultActionResult fault)
                    || !fault.row().equals(row)
                    || fault.cut() != expectedCut
                    || !cuts.add(fault.cut())) {
                throw new IllegalArgumentException("allocator V3 fault physical action order or identity differs");
            }
            failed = Math.addExact(failed, fault.failed());
            timedOut = Math.addExact(timedOut, fault.timedOut());
            unexpectedErrors = Math.addExact(unexpectedErrors, fault.unexpectedErrors());
            failedAssertions = Math.addExact(failedAssertions, fault.failedAssertions());
            skipped = Math.addExact(skipped, fault.skipped());
            duplicateLedgerIds = Math.addExact(duplicateLedgerIds, fault.duplicateLedgerIds());
            reusedLedgerIds = Math.addExact(reusedLedgerIds, fault.reusedLedgerIds());
            permanentOrphans = Math.addExact(permanentOrphans, fault.permanentOrphans());
            staleCandidateBurnMaximum = Math.max(staleCandidateBurnMaximum, fault.staleCandidateBurnMaximum());
            massTakeoverRecoveryMicros = Math.max(massTakeoverRecoveryMicros, fault.massTakeoverRecoveryMicros());
        }
        FaultEvidence evidence = new FaultEvidence(
                row,
                cuts,
                failed,
                timedOut,
                unexpectedErrors,
                failedAssertions,
                skipped,
                duplicateLedgerIds,
                reusedLedgerIds,
                permanentOrphans,
                staleCandidateBurnMaximum,
                massTakeoverRecoveryMicros);
        return new ActionResult(
                evidence,
                aggregateDigest(results),
                results.stream().allMatch(PhysicalActionResult::infrastructureValid),
                infrastructureDetail(results));
    }

    private static void validateRuntimeResult(PlannedActionV3 action, RuntimeActionResult result) {
        Objects.requireNonNull(result, "allocator V3 runtime action result");
        if (result.attachmentDigest().isZero()) {
            throw new IllegalArgumentException("allocator V3 runtime action attachment digest is zero");
        }
        switch (action.kind()) {
            case NATIVE_INTERVAL, CANDIDATE_INTERVAL -> {
                if (!(result instanceof IntervalActionResult interval)
                        || !interval.evidence().cell().equals(action.cell())) {
                    throw new IllegalArgumentException("allocator V3 runtime interval identity differs");
                }
            }
            case FAULT_ACTION -> {
                if (!(result instanceof FaultActionResult fault)
                        || !fault.row().equals(action.row())
                        || fault.cut() != action.faultCut()) {
                    throw new IllegalArgumentException("allocator V3 runtime fault identity differs");
                }
            }
            case SCALE_ACTION -> {
                if (!(result instanceof ScaleActionResult scale) || !scale.row().equals(action.row())) {
                    throw new IllegalArgumentException("allocator V3 runtime scale identity differs");
                }
            }
        }
    }

    private static Sha256Digest aggregateDigest(List<PhysicalActionResult> results) {
        ByteBuffer bytes = ByteBuffer.allocate(Math.multiplyExact(results.size(), Sha256Digest.LENGTH));
        results.forEach(result -> bytes.put(result.attachmentDigest().bytes().toByteArray()));
        return Sha256Digest.hash(CanonicalBytes.copyOf(bytes.array()));
    }

    private static String infrastructureDetail(List<PhysicalActionResult> results) {
        StringBuilder detail = new StringBuilder();
        for (PhysicalActionResult result : results) {
            if (!result.infrastructureValid()) {
                if (!detail.isEmpty()) {
                    detail.append("; ");
                }
                RuntimeActionResult runtimeResult = (RuntimeActionResult) result.rawEvidence();
                detail.append(result.action().kind())
                        .append(':')
                        .append(runtimeResult.infrastructureDetail());
            }
        }
        return detail.toString();
    }

    private static PopulationKey populationKey(PlannedActionV3 action) {
        if (action.kind().interval()) {
            return new PopulationKey(action.cell().candidate(), action.cell().activeManagedLedgers());
        }
        if (action.row() != null) {
            return new PopulationKey(action.row().candidate(), action.row().activeManagedLedgers());
        }
        return null;
    }

    private static boolean firstCampaignAction(PlannedActionV3 action) {
        return action.kind() == ActionKind.NATIVE_INTERVAL
                && action.cell().activeManagedLedgers() == 10_000
                && action.cell().metadataLatencyP99Millis() == 1
                && highestFixedRate(action);
    }

    private static boolean firstPopulationAction(PlannedActionV3 action) {
        if (action.kind() == ActionKind.SCALE_ACTION) {
            return action.row().metadataLatencyP99Millis() == 1;
        }
        return action.kind().interval()
                && !action.cell().candidate().range()
                && action.cell().metadataLatencyP99Millis() == 1
                && highestFixedRate(action);
    }

    private static boolean highestFixedRate(PlannedActionV3 action) {
        return !action.cell().rateSlot().derivedFloor()
                && action.cell().rateSlot().ordinal() == 0
                && action.offeredRate() == AllocatorCampaignV3.DESCENDING_FIXED_RATES.get(0);
    }

    interface RealActionRuntime {
        IntervalActionResult executeNativeInterval(Cell cell, int offeredRate) throws Exception;

        IntervalActionResult executeCandidateInterval(Cell cell, int offeredRate) throws Exception;

        FaultActionResult executeFaultAction(Row row, AllocatorFaultCutV1 cut) throws Exception;

        ScaleActionResult executeScaleAction(Row row) throws Exception;
    }

    sealed interface RuntimeActionResult permits IntervalActionResult, FaultActionResult, ScaleActionResult {
        Sha256Digest attachmentDigest();

        boolean infrastructureValid();

        default String infrastructureDetail() {
            return infrastructureValid() ? "" : "runtime action returned infrastructure-invalid evidence";
        }
    }

    record IntervalActionResult(
            IntervalEvidence evidence,
            Sha256Digest attachmentDigest,
            boolean infrastructureValid,
            String infrastructureDetail)
            implements RuntimeActionResult {
        IntervalActionResult {
            Objects.requireNonNull(evidence, "evidence");
            Objects.requireNonNull(attachmentDigest, "attachmentDigest");
            Objects.requireNonNull(infrastructureDetail, "infrastructureDetail");
        }
    }

    record FaultActionResult(
            Row row,
            AllocatorFaultCutV1 cut,
            long failed,
            long timedOut,
            long unexpectedErrors,
            long failedAssertions,
            long skipped,
            long duplicateLedgerIds,
            long reusedLedgerIds,
            long permanentOrphans,
            long staleCandidateBurnMaximum,
            long massTakeoverRecoveryMicros,
            Sha256Digest attachmentDigest,
            boolean infrastructureValid)
            implements RuntimeActionResult {
        FaultActionResult {
            Objects.requireNonNull(row, "row");
            Objects.requireNonNull(cut, "cut");
            Objects.requireNonNull(attachmentDigest, "attachmentDigest");
            List<Long> counters = new ArrayList<>(List.of(
                    failed,
                    timedOut,
                    unexpectedErrors,
                    failedAssertions,
                    skipped,
                    duplicateLedgerIds,
                    reusedLedgerIds,
                    permanentOrphans,
                    staleCandidateBurnMaximum,
                    massTakeoverRecoveryMicros));
            if (row.candidate().nativePath() || counters.stream().anyMatch(value -> value < 0)) {
                throw new IllegalArgumentException("allocator V3 fault action result dimensions differ");
            }
        }
    }

    record ScaleActionResult(Row row, Sha256Digest attachmentDigest, boolean infrastructureValid)
            implements RuntimeActionResult {
        ScaleActionResult {
            Objects.requireNonNull(row, "row");
            Objects.requireNonNull(attachmentDigest, "attachmentDigest");
            if (!row.candidate().range()) {
                throw new IllegalArgumentException("allocator V3 scale result is not a RANGE row");
            }
        }
    }

    private record PopulationKey(Candidate candidate, int population) {}
}
