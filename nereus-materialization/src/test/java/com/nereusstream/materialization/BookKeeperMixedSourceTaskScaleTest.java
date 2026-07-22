/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static com.nereusstream.materialization.MaterializationPlannerTestSupport.CLUSTER;
import static com.nereusstream.materialization.MaterializationPlannerTestSupport.STREAM;
import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.target.BookKeeperEntryMapping;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
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

/** Exact F4 limits with alternating BookKeeper and Object generation-zero sources. */
final class BookKeeperMixedSourceTaskScaleTest {
    private static final int SOURCE_RANGE_COUNT = MaterializationPolicy.MAX_SOURCE_RANGES;
    private static final int RECORD_COUNT = Math.toIntExact(MaterializationPolicy.MAX_RANGE_RECORDS);
    private static final int RECORDS_PER_SOURCE = RECORD_COUNT / SOURCE_RANGE_COUNT;
    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC);

    @Test
    void executesBothTaskLimits() {
        assertThat(RECORD_COUNT % SOURCE_RANGE_COUNT).isZero();
        GenerationMetadataStore durable = GenerationMetadataStoreTestFactory.inMemory(CLOCK);
        GenerationMetadataStore generations = MaterializationPlannerTestSupport.generationStore(
                sources(),
                List.of(),
                durable,
                StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT);
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
                                0,
                                RECORD_COUNT,
                                StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT)),
                generations,
                17);

        MaterializationTask task = planner.plan(
                        STREAM,
                        new OffsetRange(0, RECORD_COUNT),
                        policy,
                        1)
                .join()
                .get(0);

        assertThat(task.coverage()).isEqualTo(new OffsetRange(0, RECORD_COUNT));
        assertThat(task.coverage().recordCount()).isEqualTo(RECORD_COUNT);
        assertThat(task.sources()).hasSize(SOURCE_RANGE_COUNT);
        assertThat(task.sources().stream()
                        .filter(source -> source.readTarget()
                                instanceof BookKeeperEntryRangeReadTarget)
                        .count())
                .isEqualTo(SOURCE_RANGE_COUNT / 2);
        assertThat(task.sources().stream()
                        .filter(source -> !(source.readTarget()
                                instanceof BookKeeperEntryRangeReadTarget))
                        .count())
                .isEqualTo(SOURCE_RANGE_COUNT / 2);

        MaterializationTaskRecord encoded = MaterializationRecordMapper.plannedTask(task, CLOCK.millis());
        assertThat(F4MetadataCodecs.encodeEnvelope(encoded, MaterializationTaskRecord.class).length)
                .isPositive()
                .isLessThanOrEqualTo(MaterializationTaskStore.MAX_ENCODED_TASK_BYTES);
        MaterializationTaskStore store = new MaterializationTaskStore(CLUSTER, generations, CLOCK);
        var created = store.create(task).join();
        assertThat(store.requireTask(created)).isEqualTo(task);
    }

    private static List<VersionedGenerationCandidate> sources() {
        List<VersionedGenerationCandidate> result = new ArrayList<>(SOURCE_RANGE_COUNT);
        for (int index = 0; index < SOURCE_RANGE_COUNT; index++) {
            long start = (long) index * RECORDS_PER_SOURCE;
            long end = start + RECORDS_PER_SOURCE;
            if ((index & 1) == 0) {
                result.add(MaterializationPlannerTestSupport.zero(
                        String.format("/index/mixed-scale-%03d", index),
                        start,
                        end,
                        start,
                        RECORDS_PER_SOURCE,
                        end,
                        new BookKeeperEntryRangeReadTarget(
                                1,
                                "primary",
                                10_000L + index,
                                start,
                                RECORDS_PER_SOURCE,
                                BookKeeperEntryMapping.ONE_NEREUS_ENTRY_PER_BOOKKEEPER_ENTRY,
                                new Checksum(ChecksumType.SHA256, String.format("%064x", index + 1)))));
            } else {
                result.add(MaterializationPlannerTestSupport.zero(
                        String.format("/index/mixed-scale-%03d", index),
                        start,
                        end,
                        start,
                        RECORDS_PER_SOURCE,
                        end));
            }
        }
        return List.copyOf(result);
    }
}
