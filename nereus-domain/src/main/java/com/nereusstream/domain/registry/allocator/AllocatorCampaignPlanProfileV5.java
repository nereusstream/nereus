/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.domain.registry.allocator;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Candidate;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Cell;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Row;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Canonical source-independent physical inventory and budget identity for ADR-0137. */
public final class AllocatorCampaignPlanProfileV5 {
    public static final String SCHEMA = "NEREUS_V2_M3_ALLOCATOR_CAMPAIGN_PLAN_V5";
    public static final int LOGICAL_INTERVAL_CELLS = 328;
    public static final int MAXIMUM_EXECUTED_FAULT_ACTIONS = 360;
    public static final int MAXIMUM_EXECUTED_SCALE_ACTIONS = 32;
    public static final int MAXIMUM_TOTAL_EXECUTED_ACTIONS = 720;
    public static final long CAMPAIGN_WALL_CLOCK_CAP_SECONDS = 48_000;

    private AllocatorCampaignPlanProfileV5() {}

    public static Sha256Digest zeroDecisionPlanDigest() {
        StringBuilder canonical = new StringBuilder(64 * MAXIMUM_TOTAL_EXECUTED_ACTIONS);
        canonical
                .append(SCHEMA)
                .append('\n')
                .append("logicalPlannerVersion=")
                .append(AllocatorCampaignV3.PLANNER_VERSION)
                .append('\n')
                .append("logicalIntervalCells=")
                .append(LOGICAL_INTERVAL_CELLS)
                .append('\n')
                .append("minimumValidEvaluationCells=")
                .append(AllocatorCampaignV3.EXECUTED_PERFORMANCE_CELLS_MIN)
                .append('\n')
                .append("minimumPromotableCells=")
                .append(AllocatorCampaignV3.EXECUTED_PERFORMANCE_CELLS_MIN_PROMOTABLE)
                .append('\n')
                .append("maximumExecutedIntervalCells=")
                .append(AllocatorCampaignV3.EXECUTED_PERFORMANCE_CELLS_MAX)
                .append('\n')
                .append("maximumExecutedFaultActions=")
                .append(MAXIMUM_EXECUTED_FAULT_ACTIONS)
                .append('\n')
                .append("maximumExecutedScaleActions=")
                .append(MAXIMUM_EXECUTED_SCALE_ACTIONS)
                .append('\n')
                .append("maximumTotalExecutedActions=")
                .append(MAXIMUM_TOTAL_EXECUTED_ACTIONS)
                .append('\n')
                .append("campaignWallClockCapSeconds=")
                .append(CAMPAIGN_WALL_CLOCK_CAP_SECONDS)
                .append('\n')
                .append("offerHorizonSeconds=")
                .append(AllocatorEvidenceAdmissionPolicyV5.OFFER_HORIZON_SECONDS)
                .append('\n')
                .append("terminalAdmissionDrainSeconds=")
                .append(AllocatorEvidenceAdmissionPolicyV5.TERMINAL_ADMISSION_DRAIN_SECONDS)
                .append('\n')
                .append("cleanupGraceSeconds=")
                .append(AllocatorEvidenceAdmissionPolicyV5.CLEANUP_GRACE_SECONDS)
                .append('\n')
                .append("phaseBudgetSeconds=900,5400,7200,5400,13776,1640,600\n")
                .append("nativeExecutionProfileSha256=")
                .append(AllocatorNativeExecutionProfileV5.executionProfileDigest()
                        .toHex())
                .append('\n')
                .append("workloadScheduleSha256=")
                .append(AllocatorNativeExecutionProfileV5.scheduleDigest().toHex())
                .append('\n');
        for (String action : zeroDecisionActionIdentities()) {
            canonical.append(action).append('\n');
        }
        return Sha256Digest.hash(CanonicalBytes.copyOf(canonical.toString().getBytes(StandardCharsets.UTF_8)));
    }

    public static List<String> zeroDecisionActionIdentities() {
        List<String> actions = new ArrayList<>(MAXIMUM_TOTAL_EXECUTED_ACTIONS);
        for (Cell cell : AllocatorCampaignV3.logicalCells()) {
            actions.add(
                    (cell.candidate().nativePath() ? "NATIVE_INTERVAL:" : "CANDIDATE_INTERVAL:") + cell.contextId());
        }
        for (Candidate candidate : Candidate.values()) {
            if (candidate.nativePath()) {
                continue;
            }
            for (int population : AllocatorCampaignV3.POPULATIONS) {
                for (int latency : AllocatorCampaignV3.LATENCIES_MILLIS) {
                    Row row = new Row(candidate, population, latency);
                    for (AllocatorFaultCutV1 cut : AllocatorFaultCutV1.values()) {
                        actions.add("FAULT_ACTION:" + row.candidate() + ':' + row.activeManagedLedgers() + ':'
                                + row.metadataLatencyP99Millis() + ':' + cut);
                    }
                }
            }
        }
        for (Candidate candidate : Candidate.values()) {
            if (!candidate.range()) {
                continue;
            }
            for (int population : AllocatorCampaignV3.POPULATIONS) {
                for (int latency : AllocatorCampaignV3.LATENCIES_MILLIS) {
                    Row row = new Row(candidate, population, latency);
                    actions.add("SCALE_ACTION:" + row.candidate() + ':' + row.activeManagedLedgers() + ':'
                            + row.metadataLatencyP99Millis());
                }
            }
        }
        Set<String> unique = new HashSet<>(actions);
        if (AllocatorCampaignV3.LOGICAL_PERFORMANCE_CELLS != LOGICAL_INTERVAL_CELLS
                || actions.size() != MAXIMUM_TOTAL_EXECUTED_ACTIONS
                || unique.size() != actions.size()) {
            throw new IllegalStateException("allocator V5 physical action inventory differs from ADR 0137");
        }
        return List.copyOf(actions);
    }
}
