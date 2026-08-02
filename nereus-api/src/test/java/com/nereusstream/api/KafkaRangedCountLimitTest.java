/* Licensed under the Apache License, Version 2.0 */

package com.nereusstream.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.api.target.BookKeeperEntryMapping;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * F9-M7 checked metadata math at the exact signed-int ranged-count boundary.
 */
class KafkaRangedCountLimitTest {
    @Test
    void scenarioKfScl004() {
        byte[] onePhysicalByte = {(byte) 0x7f};
        AppendEntry maximum = new AppendEntry(onePhysicalByte, Integer.MAX_VALUE, 1, Map.of("format", "kafka"));
        AppendBatch batch = new AppendBatch(
                PayloadFormat.KAFKA_RECORD_BATCH,
                List.of(maximum),
                Integer.MAX_VALUE,
                1,
                1,
                1,
                List.of(),
                Map.of(),
                Optional.empty());

        long start = Long.MAX_VALUE - Integer.MAX_VALUE;
        OffsetRange range = new OffsetRange(start, Long.MAX_VALUE);
        BookKeeperEntryRangeReadTarget target = new BookKeeperEntryRangeReadTarget(
                1,
                "primary",
                1,
                0,
                1,
                BookKeeperEntryMapping.ONE_NEREUS_ENTRY_PER_BOOKKEEPER_ENTRY,
                new Checksum(ChecksumType.SHA256, "a".repeat(64)));
        AppendResult appended = new AppendResult(
                new StreamId("stream-ranged-count-limit"),
                range,
                range.endOffset(),
                onePhysicalByte.length,
                0,
                target,
                PayloadFormat.KAFKA_RECORD_BATCH,
                Integer.MAX_VALUE,
                1,
                onePhysicalByte.length,
                List.of(),
                Optional.empty(),
                1);
        ResolvedRange resolved = new ResolvedRange(
                range,
                0,
                target,
                PayloadFormat.KAFKA_RECORD_BATCH,
                Integer.MAX_VALUE,
                1,
                onePhysicalByte.length,
                List.of(),
                Optional.empty(),
                1);

        assertThat(batch.entries()).singleElement().isSameAs(maximum);
        assertThat(batch.recordCount()).isEqualTo(Integer.MAX_VALUE);
        assertThat(batch.entries()).hasSize(1);
        assertThat(batch.entries().getFirst().payload()).hasSize(1);
        assertThat(range.recordCount()).isEqualTo(Integer.MAX_VALUE);
        assertThat(appended.range()).isEqualTo(range);
        assertThat(appended.recordCount()).isEqualTo(Integer.MAX_VALUE);
        assertThat(resolved.offsetRange()).isEqualTo(range);
        assertThat(resolved.recordCount()).isEqualTo(Integer.MAX_VALUE);

        AppendEntry one = new AppendEntry(new byte[0], 1, 1, Map.of());
        assertThatThrownBy(() -> new AppendBatch(
                        PayloadFormat.KAFKA_RECORD_BATCH,
                        List.of(maximum, one),
                        Integer.MAX_VALUE,
                        2,
                        1,
                        1,
                        List.of(),
                        Map.of(),
                        Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("overflows int");
        assertThatThrownBy(() -> new OffsetRange(Long.MAX_VALUE - Integer.MAX_VALUE, Math.addExact(Long.MAX_VALUE, 1)))
                .isInstanceOf(ArithmeticException.class);
    }
}
