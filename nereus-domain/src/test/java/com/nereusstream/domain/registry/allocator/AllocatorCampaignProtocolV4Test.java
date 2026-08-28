/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.domain.registry.allocator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.ExecutionRecord;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.RemainingBudgets;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.SourceBinding;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.Status;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Cell;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.ExecuteCell;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.IntervalEvidence;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class AllocatorCampaignProtocolV4Test {
    @Test
    void canonicalCheckpointBindsV4ProfilePlanLineageAndFortyTwoSecondIntervalBudget() {
        var initialPlan = AllocatorCampaignPlannerV3.plan(List.of());
        AllocatorCampaignCheckpointV3 innerInitial = AllocatorCampaignCheckpointV3.initial(
                source("1"), v3Budgets(), List.of(), initialPlan.dispositions(), Status.RUNNING);
        CanonicalBytes innerInitialBytes = AllocatorCampaignCheckpointV3.encode(innerInitial);
        AllocatorCampaignCheckpointV4 outerInitial = AllocatorCampaignCheckpointV4.initial(innerInitialBytes);
        CanonicalBytes outerInitialBytes = AllocatorCampaignCheckpointV4.encode(outerInitial);

        Cell firstCell = ((ExecuteCell) initialPlan.nextAction().orElseThrow()).cell();
        ExecutionRecord record = new ExecutionRecord(passing(firstCell), digest("attachment"));
        var nextPlan = AllocatorCampaignPlannerV3.plan(List.of(record.observation()));
        AllocatorCampaignCheckpointV3 innerNext = AllocatorCampaignCheckpointV3.resume(
                innerInitialBytes,
                source("1"),
                new RemainingBudgets(0, 4_500, 7_200, 5_400, 13_080, 1_635, 600),
                List.of(record),
                nextPlan.dispositions(),
                Status.RUNNING);
        AllocatorCampaignCheckpointV4 outerNext = AllocatorCampaignCheckpointV4.resume(
                outerInitialBytes, AllocatorCampaignCheckpointV3.encode(innerNext));
        CanonicalBytes outerNextBytes = AllocatorCampaignCheckpointV4.encode(outerNext);
        AllocatorCampaignCheckpointV4 decoded = AllocatorCampaignCheckpointV4.decode(outerNextBytes);

        assertThat(decoded.checkpointSequence()).isEqualTo(1);
        assertThat(decoded.predecessorCheckpointDigest())
                .isEqualTo(AllocatorCampaignCheckpointV4.digest(outerInitialBytes));
        assertThat(decoded.remainingBudgets().intervalSeconds()).isEqualTo(13_734);
        assertThat(decoded.remainingBudgets().cleanupSeconds()).isEqualTo(1_635);
        assertThat(decoded.executionProfileDigest())
                .isEqualTo(AllocatorNativeExecutionProfileV4.executionProfileDigest());
        assertThat(decoded.planDigest()).isEqualTo(AllocatorCampaignPlanProfileV4.zeroDecisionPlanDigest());
        assertThat(decoded.campaignId()).isNotEqualTo(innerNext.campaignId());
        assertThat(decoded.logicalCheckpointBytes()).isEqualTo(AllocatorCampaignCheckpointV3.encode(innerNext));
        assertThat(AllocatorCampaignCheckpointV4.encode(decoded)).isEqualTo(outerNextBytes);
    }

    @Test
    void v3AndV4ParsersRejectEachOthersWireAndV4RejectsAV3PlanBinding() {
        var plan = AllocatorCampaignPlannerV3.plan(List.of());
        AllocatorCampaignCheckpointV3 v4Logical = AllocatorCampaignCheckpointV3.initial(
                source("2"), v3Budgets(), List.of(), plan.dispositions(), Status.RUNNING);
        CanonicalBytes nacp3 = AllocatorCampaignCheckpointV3.encode(v4Logical);
        CanonicalBytes nacp4 = AllocatorCampaignCheckpointV4.encode(AllocatorCampaignCheckpointV4.initial(nacp3));

        assertThatThrownBy(() -> AllocatorCampaignCheckpointV3.decode(nacp4))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("magic");
        assertThatThrownBy(() -> AllocatorCampaignCheckpointV4.decode(nacp3))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("magic");

        SourceBinding v3Source = new SourceBinding(
                "3".repeat(40), digest("oxia-3"), digest("dependency-3"), digest("executor-3"), digest("old-v3-plan"));
        CanonicalBytes oldV3 = AllocatorCampaignCheckpointV3.encode(AllocatorCampaignCheckpointV3.initial(
                v3Source, v3Budgets(), List.of(), plan.dispositions(), Status.RUNNING));
        assertThatThrownBy(() -> AllocatorCampaignCheckpointV4.initial(oldV3))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("profile or plan");
    }

    @Test
    void canonicalParserRejectsTamperedProfileAndTrailingBytes() {
        var plan = AllocatorCampaignPlannerV3.plan(List.of());
        CanonicalBytes encoded = AllocatorCampaignCheckpointV4.encode(AllocatorCampaignCheckpointV4.initial(
                AllocatorCampaignCheckpointV3.encode(AllocatorCampaignCheckpointV3.initial(
                        source("4"), v3Budgets(), List.of(), plan.dispositions(), Status.RUNNING))));
        byte[] profile = encoded.toByteArray();
        profile[20] ^= 1;
        byte[] trailing = Arrays.copyOf(encoded.toByteArray(), encoded.length() + 1);

        assertThatThrownBy(() -> AllocatorCampaignCheckpointV4.decode(CanonicalBytes.copyOf(profile)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("profile or plan");
        assertThatThrownBy(() -> AllocatorCampaignCheckpointV4.decode(CanonicalBytes.copyOf(trailing)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("trailing");
    }

    @Test
    void planProfileRetainsTheFrozenPhysicalInventoryWithANewDigest() {
        assertThat(AllocatorCampaignPlanProfileV4.zeroDecisionActionIdentities())
                .hasSize(720)
                .doesNotHaveDuplicates();
        assertThat(AllocatorCampaignPlanProfileV4.zeroDecisionPlanDigest())
                .isNotEqualTo(AllocatorNativeExecutionProfileV3.executionProfileDigest());
    }

    private static SourceBinding source(String digit) {
        return new SourceBinding(
                digit.repeat(40),
                digest("oxia-" + digit),
                digest("dependency-" + digit),
                digest("executor-" + digit),
                AllocatorCampaignPlanProfileV4.zeroDecisionPlanDigest());
    }

    private static RemainingBudgets v3Budgets() {
        return new RemainingBudgets(900, 5_400, 7_200, 5_400, 13_120, 1_640, 600);
    }

    private static IntervalEvidence passing(Cell cell) {
        int rate = cell.rateSlot().derivedFloor() ? 800 : cell.rateSlot().fixedRate();
        long offered = (long) rate * AllocatorCampaignV3.MEASURED_SECONDS;
        return new IntervalEvidence(
                cell, rate, offered, offered, 0, offered, 0, 0, offered, 0, 0, 0, 0, 0, 100_000, 100_000, 100_000, rate,
                100_000, 100_000, 0, 0, 0);
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8)));
    }
}
