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

/** Pure ADR-0104 V2 campaign schema. It contains no metadata-store or runtime activation authority. */
public final class AllocatorCampaignV2 {
    public static final String SCHEMA = "NEREUS_V2_M3_ALLOCATOR_CAMPAIGN_V2";
    public static final int PLANNER_VERSION = 2;
    public static final int LOGICAL_PERFORMANCE_CELLS = 288;
    public static final int EXECUTED_PERFORMANCE_CELLS_MIN = 13;
    public static final int EXECUTED_PERFORMANCE_CELLS_MIN_PROMOTABLE = 17;
    public static final int EXECUTED_PERFORMANCE_CELLS_MAX = 288;
    public static final int MEASURED_SECONDS = 30;
    public static final List<Integer> DESCENDING_RATES = List.of(1000, 750, 500, 333, 250, 200);
    public static final List<Integer> POPULATIONS = List.of(10_000, 100_000);
    public static final List<Integer> LATENCIES_MILLIS = List.of(1, 5, 10, 25);

    private AllocatorCampaignV2() {}

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

        AllocatorEvidenceCandidateV1 evidenceCandidate() {
            if (strict()) {
                return AllocatorEvidenceCandidateV1.strict();
            }
            if (range()) {
                return AllocatorEvidenceCandidateV1.range(rangeSize);
            }
            throw new IllegalStateException("native campaign path has no allocator evidence candidate");
        }
    }

    public record Row(Candidate candidate, int activeManagedLedgers, int metadataLatencyP99Millis) {
        public Row {
            Objects.requireNonNull(candidate, "candidate");
            if (!POPULATIONS.contains(activeManagedLedgers) || !LATENCIES_MILLIS.contains(metadataLatencyP99Millis)) {
                throw new IllegalArgumentException("allocator V2 row is outside the frozen population/latency matrix");
            }
        }
    }

    public record Cell(
            int contextId,
            Candidate candidate,
            int activeManagedLedgers,
            int metadataLatencyP99Millis,
            int offeredRolloverRequestsPerSecond) {
        public Cell {
            Objects.requireNonNull(candidate, "candidate");
            if (!POPULATIONS.contains(activeManagedLedgers)
                    || !LATENCIES_MILLIS.contains(metadataLatencyP99Millis)
                    || !DESCENDING_RATES.contains(offeredRolloverRequestsPerSecond)) {
                throw new IllegalArgumentException("allocator V2 cell is outside the frozen ADR-0104 matrix");
            }
            int expectedContextId = AllocatorCampaignV2.contextId(
                    candidate, activeManagedLedgers, metadataLatencyP99Millis, offeredRolloverRequestsPerSecond);
            if (contextId != expectedContextId) {
                throw new IllegalArgumentException("allocator V2 context ID aliases different logical dimensions");
            }
        }

        public static Cell of(Candidate candidate, int population, int latencyMillis, int rate) {
            return new Cell(
                    AllocatorCampaignV2.contextId(candidate, population, latencyMillis, rate),
                    candidate,
                    population,
                    latencyMillis,
                    rate);
        }

        public Row row() {
            return new Row(candidate, activeManagedLedgers, metadataLatencyP99Millis);
        }
    }

    /** Raw bounded-runner interval counters and independently measured maxima; no caller pass/fail field exists. */
    public record IntervalEvidence(
            Cell cell,
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
        BELOW_NATIVE_RELATIVE_FLOOR,
        ROW_TERMINAL_AT_SUSTAINABLE_RATE,
        NATIVE_BASELINE_UNAVAILABLE,
        CANDIDATE_ELIMINATED,
        SMALLER_RANGE_QUALIFIED
    }

    /** A claimed campaign disposition. The validator accepts it only when it exactly equals its recomputation. */
    public record Disposition(Cell cell, DispositionKind kind, List<Integer> dependencyContextIds) {
        public Disposition {
            Objects.requireNonNull(cell, "cell");
            Objects.requireNonNull(kind, "kind");
            dependencyContextIds = List.copyOf(Objects.requireNonNull(dependencyContextIds, "dependencyContextIds"));
            if (dependencyContextIds.isEmpty()) {
                throw new IllegalArgumentException(
                        "allocator V2 disposition must bind at least one executed dependency");
            }
        }
    }

    public sealed interface RequiredAction permits ExecuteCell, ExecuteFaultRow {}

    public record ExecuteCell(Cell cell) implements RequiredAction {
        public ExecuteCell {
            Objects.requireNonNull(cell, "cell");
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

    public record Plan(
            int plannerVersion,
            List<Cell> logicalCells,
            List<Disposition> dispositions,
            Optional<RequiredAction> nextAction,
            int executedPerformanceCells,
            List<Candidate> qualifiedCandidates,
            boolean completed) {
        public Plan {
            if (plannerVersion != PLANNER_VERSION) {
                throw new IllegalArgumentException("allocator V2 planner version differs");
            }
            logicalCells = List.copyOf(Objects.requireNonNull(logicalCells, "logicalCells"));
            dispositions = List.copyOf(Objects.requireNonNull(dispositions, "dispositions"));
            nextAction = Objects.requireNonNull(nextAction, "nextAction");
            qualifiedCandidates = List.copyOf(Objects.requireNonNull(qualifiedCandidates, "qualifiedCandidates"));
            if (logicalCells.size() != LOGICAL_PERFORMANCE_CELLS
                    || executedPerformanceCells < 0
                    || executedPerformanceCells > EXECUTED_PERFORMANCE_CELLS_MAX
                    || completed == nextAction.isPresent()) {
                throw new IllegalArgumentException("allocator V2 plan inventory or terminal state differs");
            }
        }
    }

    public static List<Cell> logicalCells() {
        List<Cell> cells = new ArrayList<>(LOGICAL_PERFORMANCE_CELLS);
        for (Candidate candidate : Candidate.values()) {
            for (int population : POPULATIONS) {
                for (int latency : LATENCIES_MILLIS) {
                    for (int rate : DESCENDING_RATES) {
                        cells.add(Cell.of(candidate, population, latency, rate));
                    }
                }
            }
        }
        if (cells.size() != LOGICAL_PERFORMANCE_CELLS) {
            throw new IllegalStateException("allocator V2 logical inventory differs from ADR 0104");
        }
        return List.copyOf(cells);
    }

    static int contextId(Candidate candidate, int population, int latencyMillis, int rate) {
        Objects.requireNonNull(candidate, "candidate");
        if (candidate.nativePath()) {
            return AllocatorEvidenceContextV1.nativeContext(population, latencyMillis, rate)
                    .contextId();
        }
        return AllocatorEvidenceContextV1.candidateContext(
                        candidate.evidenceCandidate(), population, latencyMillis, rate)
                .contextId();
    }

    private static void requireNonNegative(long... values) {
        for (long value : values) {
            if (value < 0) {
                throw new IllegalArgumentException("allocator V2 raw counter or latency is negative");
            }
        }
    }
}
