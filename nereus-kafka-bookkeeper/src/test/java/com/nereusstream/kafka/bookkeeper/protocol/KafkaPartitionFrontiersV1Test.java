/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.kafka.bookkeeper.protocol;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.junit.jupiter.api.Test;

class KafkaPartitionFrontiersV1Test {
    @Test
    void acceptsTheExactOrderedHalfOpenFrontiers() {
        KafkaPartitionFrontiersV1 frontiers = new KafkaPartitionFrontiersV1(2, 12, 11, 10, 8, 7);

        assertThat(frontiers.trimStartOffset()).isEqualTo(2);
        assertThat(frontiers.lastStableOffset()).isEqualTo(7);
        assertThat(frontiers.highWatermark()).isEqualTo(8);
        assertThat(frontiers.readableEndOffset()).isEqualTo(10);
        assertThat(frontiers.durableEndOffset()).isEqualTo(11);
        assertThat(frontiers.allocatedEndOffset()).isEqualTo(12);
    }

    @Test
    void rejectsNegativeTrim() {
        assertThatThrownBy(() -> new KafkaPartitionFrontiersV1(-1, 0, 0, 0, 0, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsLastStableBeyondHighWatermark() {
        assertThatThrownBy(() -> new KafkaPartitionFrontiersV1(0, 4, 4, 4, 2, 3))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsHighWatermarkBeyondReadable() {
        assertThatThrownBy(() -> new KafkaPartitionFrontiersV1(0, 4, 4, 2, 3, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsReadableBeyondDurableOrDurableBeyondAllocated() {
        assertThatThrownBy(() -> new KafkaPartitionFrontiersV1(0, 4, 2, 3, 1, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new KafkaPartitionFrontiersV1(0, 3, 4, 2, 1, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void distinguishesMonotonicAdvanceFromAnyRegression() {
        KafkaPartitionFrontiersV1 before = new KafkaPartitionFrontiersV1(1, 6, 5, 4, 3, 2);

        assertThat(new KafkaPartitionFrontiersV1(1, 9, 8, 7, 4, 3).noRegressionFrom(before))
                .isTrue();
        assertThat(new KafkaPartitionFrontiersV1(1, 9, 8, 7, 3, 1).noRegressionFrom(before))
                .isFalse();
    }
}
