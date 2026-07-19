/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static com.nereusstream.materialization.MaterializationPlannerTestSupport.CLUSTER;
import static com.nereusstream.materialization.MaterializationPlannerTestSupport.STREAM;
import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.OffsetRange;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStoreTestFactory;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.codec.F4MetadataCodecs;
import com.nereusstream.metadata.oxia.records.MaterializationTaskRecord;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class MaterializationPlannerScaleTest {
    private static final int SOURCE_RANGE_COUNT =
            MaterializationPolicy.MAX_SOURCE_RANGES;
    private static final int RECORD_COUNT =
            Math.toIntExact(MaterializationPolicy.MAX_RANGE_RECORDS);
    private static final int RECORDS_PER_SOURCE =
            RECORD_COUNT / SOURCE_RANGE_COUNT;
    private static final Clock CLOCK = Clock.fixed(
            Instant.ofEpochMilli(1_000), ZoneOffset.UTC);

    @Test
    void plansAndDurablyRoundTripsOneTaskAtBothSourceAndRecordLimits() {
        assertThat(RECORD_COUNT % SOURCE_RANGE_COUNT).isZero();
        List<VersionedGenerationCandidate> sources = sources();
        GenerationMetadataStore durable =
                GenerationMetadataStoreTestFactory.inMemory(CLOCK);
        GenerationMetadataStore generations =
                MaterializationPlannerTestSupport.generationStore(
                        sources, List.of(), durable);
        MaterializationPolicy policy = MaterializationPolicyFactory.losslessCommitted(
                2,
                SOURCE_RANGE_COUNT,
                RECORD_COUNT,
                2L * RECORD_COUNT,
                MaterializationPolicy.MAX_ROW_GROUP_RECORDS,
                "ZSTD");
        DefaultMaterializationPlanner planner = new DefaultMaterializationPlanner(
                CLUSTER,
                MaterializationPlannerTestSupport.l0Store(
                        MaterializationPlannerTestSupport.snapshot(
                                0, RECORD_COUNT)),
                generations,
                17);

        List<MaterializationTask> planned = planner.plan(
                        STREAM,
                        new OffsetRange(0, RECORD_COUNT),
                        policy,
                        1)
                .join();

        assertThat(planned).singleElement().satisfies(task -> {
            assertThat(task.coverage())
                    .isEqualTo(new OffsetRange(0, RECORD_COUNT));
            assertThat(task.coverage().recordCount()).isEqualTo(RECORD_COUNT);
            assertThat(task.sources()).hasSize(SOURCE_RANGE_COUNT);
            assertThat(task.sources())
                    .extracting(SourceGeneration::range)
                    .containsExactlyElementsOf(expectedRanges());

            MaterializationTaskRecord record =
                    MaterializationRecordMapper.plannedTask(task, CLOCK.millis());
            assertThat(F4MetadataCodecs.encodeEnvelope(
                            record, MaterializationTaskRecord.class).length)
                    .isPositive()
                    .isLessThanOrEqualTo(
                            MaterializationTaskStore.MAX_ENCODED_TASK_BYTES);

            MaterializationTaskStore store = new MaterializationTaskStore(
                    CLUSTER, generations, CLOCK);
            var created = store.create(task).join();
            assertThat(created.value().sources()).hasSize(SOURCE_RANGE_COUNT);
            assertThat(store.requireTask(created)).isEqualTo(task);
        });
    }

    private static List<VersionedGenerationCandidate> sources() {
        List<VersionedGenerationCandidate> result =
                new ArrayList<>(SOURCE_RANGE_COUNT);
        for (int index = 0; index < SOURCE_RANGE_COUNT; index++) {
            long start = (long) index * RECORDS_PER_SOURCE;
            long end = start + RECORDS_PER_SOURCE;
            result.add(MaterializationPlannerTestSupport.zero(
                    String.format("/index/scale-%03d", index),
                    start,
                    end,
                    start,
                    RECORDS_PER_SOURCE,
                    end));
        }
        return List.copyOf(result);
    }

    private static List<OffsetRange> expectedRanges() {
        List<OffsetRange> result = new ArrayList<>(SOURCE_RANGE_COUNT);
        for (int index = 0; index < SOURCE_RANGE_COUNT; index++) {
            long start = (long) index * RECORDS_PER_SOURCE;
            result.add(new OffsetRange(start, start + RECORDS_PER_SOURCE));
        }
        return List.copyOf(result);
    }
}
