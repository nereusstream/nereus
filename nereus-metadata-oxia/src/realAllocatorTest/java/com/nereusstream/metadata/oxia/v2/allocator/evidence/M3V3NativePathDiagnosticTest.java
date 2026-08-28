/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.metadata.oxia.v2.allocator.evidence;

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.domain.registry.allocator.AllocatorNativeExecutionProfileV3;
import java.lang.reflect.Field;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Source-level formal/diagnostic Native runtime and frozen-schedule equivalence contract. */
class M3V3NativePathDiagnosticTest {
    @Test
    void formalAndDiagnosticUseOneNonBlockingRuntimeAndFrozenSchedule() {
        assertThat(M3V3RealFormalActionRuntime.class.getDeclaredFields())
                .extracting(Field::getType)
                .contains(M3V3NativeIntervalRuntime.class)
                .doesNotContain(java.util.concurrent.ExecutorService.class);
        assertThat(M3V3NativeIntervalRuntime.executionModel())
                .isEqualTo(AllocatorNativeExecutionProfileV3.MODEL);
        assertThat(M3V3NativeIntervalRuntime.hiddenDispatchQueue()).isZero();
        for (int derivedRate : List.of(800, 600, 400, 267)) {
            assertThat(M3V3NativeIntervalRuntime.schedule(10_000, derivedRate))
                    .hasSize(Math.multiplyExact(40, derivedRate));
            assertThat(M3V3RealFormalActionRuntime.candidateSchedule(10_000, derivedRate))
                    .hasSize(Math.multiplyExact(40, derivedRate));
        }
        assertThat(AllocatorNativeExecutionProfileV3.NATIVE_BRIDGE_WORKERS).isZero();
        assertThat(AllocatorNativeExecutionProfileV3.NATIVE_BRIDGE_QUEUE_CAPACITY).isZero();

        List<M3V3AsyncActorLaneRunner.ScheduledOffer<M3V3NativeIntervalRuntime.NativeOffer>> schedule =
                M3V3NativeIntervalRuntime.schedule(10_000, 200);
        assertThat(schedule).hasSize(M3AllocatorWorkloadPlan.requestCount(200));
        assertThat(schedule.subList(0, M3AllocatorWorkloadPlan.WARM_UP_SECONDS * 200))
                .noneMatch(M3V3AsyncActorLaneRunner.ScheduledOffer::measured);
        assertThat(schedule.subList(M3AllocatorWorkloadPlan.WARM_UP_SECONDS * 200, schedule.size()))
                .allMatch(M3V3AsyncActorLaneRunner.ScheduledOffer::measured);
        assertThat(schedule.stream().map(offer -> offer.request().request().trigger()).distinct())
                .containsExactlyInAnyOrder(M3AllocatorWorkloadPlan.Trigger.values());
    }
}
