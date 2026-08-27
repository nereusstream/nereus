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

package com.nereusstream.domain.registry.allocator;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Pure ADR-0108 V3 campaign schema. It contains no metadata-store or runtime activation authority. */
public final class AllocatorCampaignV3 {
    public static final String SCHEMA = "NEREUS_V2_M3_ALLOCATOR_CAMPAIGN_V3";
    public static final int PLANNER_VERSION = 3;
    public static final int LOGICAL_PERFORMANCE_CELLS = 328;
    public static final int EXECUTED_PERFORMANCE_CELLS_MIN = 13;
    public static final int EXECUTED_PERFORMANCE_CELLS_MIN_PROMOTABLE = 17;
    public static final int EXECUTED_PERFORMANCE_CELLS_MAX = 328;
    public static final int MEASURED_SECONDS = 30;
    public static final int ABSOLUTE_FLOOR = 200;
    public static final List<Integer> DESCENDING_FIXED_RATES = List.of(1000, 750, 500, 333, 250, 200);
    public static final List<Integer> POPULATIONS = List.of(10_000, 100_000);
    public static final List<Integer> LATENCIES_MILLIS = List.of(1, 5, 10, 25);
    private static final int CONTEXT_BASE = 3_000_000;

    private AllocatorCampaignV3() {}

    public enum Candidate {
        NATIVE(true, 0),
        STRICT(false, 1),
        RANGE_16(false, 16),
        RANGE_64(false, 64),
        RANGE_256(false, 256),
        RANGE_1024(false, 1024);

        private final boolean nativePath;
        private final long rangeSize;

        Candidate(boolean nativePath, long rangeSize) {
            this.nativePath = nativePath;
            this.rangeSize = rangeSize;
        }

        public boolean nativePath() {
            return nativePath;
        }

        public boolean strict() {
            return this == STRICT;
        }

        public boolean range() {
            return !nativePath && !strict();
        }

        public long rangeSize() {
            if (nativePath) {
                throw new IllegalStateException("native campaign path has no allocator range size");
            }
            return rangeSize;
        }
    }

    public enum SlotKind {
        FIXED,
        DERIVED
    }

    /** Stable logical slot identity; the derived slot deliberately does not alias a same-valued fixed slot. */
    public record RateSlot(SlotKind kind, int ordinal) {
        public static final int DERIVED_ORDINAL = DESCENDING_FIXED_RATES.size();

        public RateSlot {
            Objects.requireNonNull(kind, "kind");
            boolean validFixed = kind == SlotKind.FIXED && ordinal >= 0 && ordinal < DESCENDING_FIXED_RATES.size();
            boolean validDerived = kind == SlotKind.DERIVED && ordinal == DERIVED_ORDINAL;
            if (!validFixed && !validDerived) {
                throw new IllegalArgumentException("allocator V3 rate-slot kind or ordinal differs");
            }
        }

        public static RateSlot fixed(int ordinal) {
            return new RateSlot(SlotKind.FIXED, ordinal);
        }

        public static RateSlot derived() {
            return new RateSlot(SlotKind.DERIVED, DERIVED_ORDINAL);
        }

        public boolean derivedFloor() {
            return kind == SlotKind.DERIVED;
        }

        public int fixedRate() {
            if (derivedFloor()) {
                throw new IllegalStateException("allocator V3 derived slot has no baseline-independent rate");
            }
            return DESCENDING_FIXED_RATES.get(ordinal);
        }
    }

    public record Row(Candidate candidate, int activeManagedLedgers, int metadataLatencyP99Millis) {
        public Row {
            Objects.requireNonNull(candidate, "candidate");
            if (!POPULATIONS.contains(activeManagedLedgers) || !LATENCIES_MILLIS.contains(metadataLatencyP99Millis)) {
                throw new IllegalArgumentException("allocator V3 row is outside the frozen population/latency matrix");
            }
        }
    }

    public record Cell(
            int contextId,
            Candidate candidate,
            int activeManagedLedgers,
            int metadataLatencyP99Millis,
            RateSlot rateSlot) {
        public Cell {
            Objects.requireNonNull(candidate, "candidate");
            Objects.requireNonNull(rateSlot, "rateSlot");
            if (!POPULATIONS.contains(activeManagedLedgers)
                    || !LATENCIES_MILLIS.contains(metadataLatencyP99Millis)
                    || (candidate.nativePath() && rateSlot.derivedFloor())) {
                throw new IllegalArgumentException("allocator V3 cell is outside the frozen ADR-0108 matrix");
            }
            int expectedContextId =
                    AllocatorCampaignV3.contextId(candidate, activeManagedLedgers, metadataLatencyP99Millis, rateSlot);
            if (contextId != expectedContextId) {
                throw new IllegalArgumentException("allocator V3 context ID aliases different logical dimensions");
            }
        }

        public static Cell fixed(Candidate candidate, int population, int latencyMillis, int fixedSlotOrdinal) {
            RateSlot slot = RateSlot.fixed(fixedSlotOrdinal);
            return new Cell(
                    AllocatorCampaignV3.contextId(candidate, population, latencyMillis, slot),
                    candidate,
                    population,
                    latencyMillis,
                    slot);
        }

        public static Cell fixedRate(Candidate candidate, int population, int latencyMillis, int rate) {
            int ordinal = DESCENDING_FIXED_RATES.indexOf(rate);
            if (ordinal < 0) {
                throw new IllegalArgumentException("allocator V3 fixed rate is outside the frozen rate inventory");
            }
            return fixed(candidate, population, latencyMillis, ordinal);
        }

        public static Cell derived(Candidate candidate, int population, int latencyMillis) {
            if (candidate.nativePath()) {
                throw new IllegalArgumentException("native allocator V3 row has no derived rate slot");
            }
            RateSlot slot = RateSlot.derived();
            return new Cell(
                    AllocatorCampaignV3.contextId(candidate, population, latencyMillis, slot),
                    candidate,
                    population,
                    latencyMillis,
                    slot);
        }

        public Row row() {
            return new Row(candidate, activeManagedLedgers, metadataLatencyP99Millis);
        }
    }

    /** Raw bounded-runner counters. The offered rate is validated against the slot and native baseline. */
    public record IntervalEvidence(
            Cell cell,
            int offeredRate,
            long offered,
            long admitted,
            long overloadDroppedBeforeAdmission,
            long completed,
            long failedAfterAdmission,
            long timedOutAfterAdmission,
            long terminal,
            long failedAssertions,
            long unexpectedErrors,
            long skipped,
            long duplicateLedgerIds,
            long reusedLedgerIds,
            long rolloverP99Micros,
            long oxiaOperationP99Micros,
            long queueAgeP99Micros,
            long queueDepthMaximum,
            long starvationMaximumMicros,
            long appendStallP99Micros,
            long backlogAtEnd,
            long inFlightAtEnd,
            long waiterCountAtEnd)
            implements Observation {
        public IntervalEvidence {
            Objects.requireNonNull(cell, "cell");
            if (offeredRate < ABSOLUTE_FLOOR || offeredRate > DESCENDING_FIXED_RATES.get(0)) {
                throw new IllegalArgumentException("allocator V3 offered rate is outside the frozen envelope");
            }
            if (!cell.rateSlot().derivedFloor()
                    && offeredRate != cell.rateSlot().fixedRate()) {
                throw new IllegalArgumentException("allocator V3 fixed slot and offered rate differ");
            }
            requireNonNegative(
                    offered,
                    admitted,
                    overloadDroppedBeforeAdmission,
                    completed,
                    failedAfterAdmission,
                    timedOutAfterAdmission,
                    terminal,
                    failedAssertions,
                    unexpectedErrors,
                    skipped,
                    duplicateLedgerIds,
                    reusedLedgerIds,
                    rolloverP99Micros,
                    oxiaOperationP99Micros,
                    queueAgeP99Micros,
                    queueDepthMaximum,
                    starvationMaximumMicros,
                    appendStallP99Micros,
                    backlogAtEnd,
                    inFlightAtEnd,
                    waiterCountAtEnd);
        }
    }

    /** Raw nine-cut row counters. The validator recomputes the population-specific recovery bound. */
    public record FaultEvidence(
            Row row,
            Set<AllocatorFaultCutV1> cuts,
            long failed,
            long timedOut,
            long unexpectedErrors,
            long failedAssertions,
            long skipped,
            long duplicateLedgerIds,
            long reusedLedgerIds,
            long permanentOrphans,
            long staleCandidateBurnMaximum,
            long massTakeoverRecoveryMicros)
            implements Observation {
        public FaultEvidence {
            Objects.requireNonNull(row, "row");
            if (row.candidate().nativePath()) {
                throw new IllegalArgumentException("native allocator row has no ADR-0094 candidate fault matrix");
            }
            cuts = Set.copyOf(Objects.requireNonNull(cuts, "cuts"));
            requireNonNegative(
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
        }
    }

    public sealed interface Observation permits IntervalEvidence, FaultEvidence {}

    public enum DispositionKind {
        NATIVE_SUSTAINABLE_RATE_FOUND,
        BELOW_DERIVED_FLOOR,
        DUPLICATE_DERIVED_FLOOR,
        ROW_TERMINAL_AT_SUSTAINABLE_RATE,
        COMPARISON_UNAVAILABLE,
        CANDIDATE_ELIMINATED,
        SMALLER_RANGE_QUALIFIED
    }

    /** A claimed disposition accepted only when it exactly equals deterministic validator recomputation. */
    public record Disposition(Cell cell, DispositionKind kind, List<Integer> dependencyContextIds) {
        public Disposition {
            Objects.requireNonNull(cell, "cell");
            Objects.requireNonNull(kind, "kind");
            dependencyContextIds = List.copyOf(Objects.requireNonNull(dependencyContextIds, "dependencyContextIds"));
            if (dependencyContextIds.isEmpty()) {
                throw new IllegalArgumentException("allocator V3 disposition must bind executed dependencies");
            }
        }
    }

    public sealed interface RequiredAction permits ExecuteCell, ExecuteFaultRow {}

    public record ExecuteCell(Cell cell, int offeredRate) implements RequiredAction {
        public ExecuteCell {
            Objects.requireNonNull(cell, "cell");
            if (offeredRate < ABSOLUTE_FLOOR || offeredRate > DESCENDING_FIXED_RATES.get(0)) {
                throw new IllegalArgumentException("allocator V3 action offered rate is outside the frozen envelope");
            }
            if (!cell.rateSlot().derivedFloor()
                    && offeredRate != cell.rateSlot().fixedRate()) {
                throw new IllegalArgumentException("allocator V3 fixed action slot and rate differ");
            }
        }
    }

    public record ExecuteFaultRow(Row row) implements RequiredAction {
        public ExecuteFaultRow {
            Objects.requireNonNull(row, "row");
            if (row.candidate().nativePath()) {
                throw new IllegalArgumentException("native allocator row does not execute candidate fault cuts");
            }
        }
    }

    public record Campaign(List<Observation> observations, List<Disposition> dispositions) {
        public Campaign {
            observations = List.copyOf(Objects.requireNonNull(observations, "observations"));
            dispositions = List.copyOf(Objects.requireNonNull(dispositions, "dispositions"));
        }
    }

    public enum BaselineStatus {
        INCOMPLETE,
        AVAILABLE,
        UNAVAILABLE
    }

    public record Plan(
            int plannerVersion,
            List<Cell> logicalCells,
            List<Disposition> dispositions,
            Optional<RequiredAction> nextAction,
            int executedPerformanceCells,
            List<Candidate> qualifiedCandidates,
            BaselineStatus baselineStatus,
            boolean completed) {
        public Plan {
            if (plannerVersion != PLANNER_VERSION) {
                throw new IllegalArgumentException("allocator V3 planner version differs");
            }
            logicalCells = List.copyOf(Objects.requireNonNull(logicalCells, "logicalCells"));
            dispositions = List.copyOf(Objects.requireNonNull(dispositions, "dispositions"));
            nextAction = Objects.requireNonNull(nextAction, "nextAction");
            qualifiedCandidates = List.copyOf(Objects.requireNonNull(qualifiedCandidates, "qualifiedCandidates"));
            Objects.requireNonNull(baselineStatus, "baselineStatus");
            if (logicalCells.size() != LOGICAL_PERFORMANCE_CELLS
                    || executedPerformanceCells < 0
                    || executedPerformanceCells > EXECUTED_PERFORMANCE_CELLS_MAX
                    || completed == nextAction.isPresent()
                    || (completed && baselineStatus == BaselineStatus.INCOMPLETE)) {
                throw new IllegalArgumentException("allocator V3 plan inventory or terminal state differs");
            }
        }
    }

    public static List<Cell> logicalCells() {
        List<Cell> cells = new ArrayList<>(LOGICAL_PERFORMANCE_CELLS);
        for (Candidate candidate : Candidate.values()) {
            for (int population : POPULATIONS) {
                for (int latency : LATENCIES_MILLIS) {
                    for (int fixed = 0; fixed < DESCENDING_FIXED_RATES.size(); fixed++) {
                        cells.add(Cell.fixed(candidate, population, latency, fixed));
                    }
                    if (!candidate.nativePath()) {
                        cells.add(Cell.derived(candidate, population, latency));
                    }
                }
            }
        }
        if (cells.size() != LOGICAL_PERFORMANCE_CELLS) {
            throw new IllegalStateException("allocator V3 logical inventory differs from ADR 0108");
        }
        return List.copyOf(cells);
    }

    public static int derivedRate(int nativeSustainableRate) {
        if (!DESCENDING_FIXED_RATES.contains(nativeSustainableRate)) {
            throw new IllegalArgumentException("allocator V3 native rate is outside the frozen inventory");
        }
        int eightyPercentCeiling = Math.subtractExact(nativeSustainableRate, nativeSustainableRate / 5);
        return Math.max(ABSOLUTE_FLOOR, eightyPercentCeiling);
    }

    static int contextId(Candidate candidate, int population, int latencyMillis, RateSlot slot) {
        Objects.requireNonNull(candidate, "candidate");
        Objects.requireNonNull(slot, "slot");
        int populationIndex = POPULATIONS.indexOf(population);
        int latencyIndex = LATENCIES_MILLIS.indexOf(latencyMillis);
        if (populationIndex < 0 || latencyIndex < 0 || (candidate.nativePath() && slot.derivedFloor())) {
            throw new IllegalArgumentException("allocator V3 context dimensions are outside the frozen inventory");
        }
        int rowIndex = populationIndex * LATENCIES_MILLIS.size() + latencyIndex;
        int logicalOrdinal;
        if (candidate.nativePath()) {
            logicalOrdinal = rowIndex * DESCENDING_FIXED_RATES.size() + slot.ordinal();
        } else {
            int candidateOffset = candidate.ordinal() - 1;
            int nativeCells = POPULATIONS.size() * LATENCIES_MILLIS.size() * DESCENDING_FIXED_RATES.size();
            int candidateCells = POPULATIONS.size() * LATENCIES_MILLIS.size() * (DESCENDING_FIXED_RATES.size() + 1);
            logicalOrdinal = nativeCells
                    + candidateOffset * candidateCells
                    + rowIndex * (DESCENDING_FIXED_RATES.size() + 1)
                    + slot.ordinal();
        }
        return Math.addExact(CONTEXT_BASE, logicalOrdinal);
    }

    private static void requireNonNegative(long... values) {
        for (long value : values) {
            if (value < 0) {
                throw new IllegalArgumentException("allocator V3 raw counter or latency is negative");
            }
        }
    }
}
