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
import com.nereusstream.domain.bytes.Sha256Digest;
import org.junit.jupiter.api.Test;

class KafkaPartitionProtocolStateV1Test {
    @Test
    void immutableStateRootIsItsOwnAllocationFreeReadSnapshot() {
        KafkaPartitionProtocolStateV1 state = new KafkaPartitionProtocolStateV1(
                KafkaProtocolStateFixtures.fence(1, 2, 3, 4),
                9,
                new KafkaPartitionFrontiersV1(1, 12, 11, 10, 8, 7),
                KafkaProtocolStateFixtures.references(2));
        KafkaPartitionPublicationCellV1 cell = new KafkaPartitionPublicationCellV1(state, event -> {});

        assertThat(cell.captureReadSnapshot()).isSameAs(state);
        assertThat(cell.capture()).isSameAs(state);
    }

    @Test
    void readIsolationSelectsLeoHwAndLsoWithoutUsingDurableEnd() {
        KafkaPartitionProtocolStateV1 state = new KafkaPartitionProtocolStateV1(
                KafkaProtocolStateFixtures.fence(1, 2, 3, 4),
                9,
                new KafkaPartitionFrontiersV1(1, 12, 11, 10, 8, 7),
                KafkaProtocolStateFixtures.references(2));

        assertThat(state.readUpperBound(KafkaReadIsolationV1.REPLICA)).isEqualTo(10);
        assertThat(state.readUpperBound(KafkaReadIsolationV1.READ_UNCOMMITTED)).isEqualTo(8);
        assertThat(state.readUpperBound(KafkaReadIsolationV1.READ_COMMITTED)).isEqualTo(7);
    }

    @Test
    void rejectsNegativeStateVersion() {
        assertThatThrownBy(() -> new KafkaPartitionProtocolStateV1(
                        KafkaProtocolStateFixtures.fence(1, 2, 3, 4),
                        -1,
                        new KafkaPartitionFrontiersV1(0, 0, 0, 0, 0, 0),
                        KafkaProtocolStateFixtures.references(0)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new KafkaPartitionProtocolStateV1(
                        KafkaProtocolStateFixtures.fence(1, 2, 3, 4),
                        Long.MAX_VALUE,
                        new KafkaPartitionFrontiersV1(0, 0, 0, 0, 0, 0),
                        KafkaProtocolStateFixtures.references(0)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void referenceIdentityRejectsZeroDigestAndDetectsGenerationRegression() {
        assertThatThrownBy(() -> new KafkaPartitionStateReferenceV1(0, Sha256Digest.copyOf(new byte[32])))
                .isInstanceOf(IllegalArgumentException.class);

        KafkaPartitionStateReferenceV1 current = KafkaProtocolStateFixtures.reference(4, 7);
        assertThat(KafkaProtocolStateFixtures.reference(4, 7).doesNotRegress(current))
                .isTrue();
        assertThat(KafkaProtocolStateFixtures.reference(4, 8).doesNotRegress(current))
                .isFalse();
        assertThat(KafkaProtocolStateFixtures.reference(5, 8).doesNotRegress(current))
                .isTrue();
        assertThat(KafkaProtocolStateFixtures.reference(3, 8).doesNotRegress(current))
                .isFalse();
    }
}
