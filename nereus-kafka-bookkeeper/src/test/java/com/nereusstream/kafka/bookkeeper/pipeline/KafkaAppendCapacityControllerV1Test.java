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

package com.nereusstream.kafka.bookkeeper.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.junit.jupiter.api.Test;

class KafkaAppendCapacityControllerV1Test {
    @Test
    void reservesAndReleasesTheExactThreeDimensionalDelta() {
        KafkaAppendCapacityControllerV1 controller = controller(2, 5, 100);

        KafkaAppendCapacityControllerV1.Lease lease =
                controller.tryReserve(3, 60).orElseThrow();

        assertThat(controller.snapshot()).isEqualTo(new KafkaAppendCapacitySnapshotV1(1, 3, 60));
        lease.close();
        assertThat(controller.snapshot()).isEqualTo(new KafkaAppendCapacitySnapshotV1(0, 0, 0));
    }

    @Test
    void rejectsAtOneAfterTheGroupLimitWithoutChangingCounters() {
        KafkaAppendCapacityControllerV1 controller = controller(1, 10, 100);
        controller.tryReserve(1, 1).orElseThrow();

        assertThat(controller.tryReserve(1, 1)).isEmpty();
        assertThat(controller.snapshot()).isEqualTo(new KafkaAppendCapacitySnapshotV1(1, 1, 1));
    }

    @Test
    void rejectsAtOneAfterTheEntryLimitWithoutChangingCounters() {
        KafkaAppendCapacityControllerV1 controller = controller(3, 2, 100);

        assertThat(controller.tryReserve(3, 1)).isEmpty();
        assertThat(controller.snapshot()).isEqualTo(new KafkaAppendCapacitySnapshotV1(0, 0, 0));
    }

    @Test
    void rejectsAtOneAfterTheByteLimitWithoutChangingCounters() {
        KafkaAppendCapacityControllerV1 controller = controller(3, 10, 9);

        assertThat(controller.tryReserve(1, 10)).isEmpty();
        assertThat(controller.snapshot()).isEqualTo(new KafkaAppendCapacitySnapshotV1(0, 0, 0));
    }

    @Test
    void leaseReleaseIsIdempotent() {
        KafkaAppendCapacityControllerV1 controller = controller(1, 1, 1);
        KafkaAppendCapacityControllerV1.Lease lease =
                controller.tryReserve(1, 1).orElseThrow();

        lease.close();
        lease.close();

        assertThat(controller.snapshot()).isEqualTo(new KafkaAppendCapacitySnapshotV1(0, 0, 0));
    }

    @Test
    void rejectsInvalidBudgetRequestAndSnapshotDomains() {
        assertThatThrownBy(() -> new KafkaAppendCapacityBudgetV1(0, 1, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> controller(1, 1, 1).tryReserve(0, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new KafkaAppendCapacitySnapshotV1(0, -1, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static KafkaAppendCapacityControllerV1 controller(long groups, long entries, long bytes) {
        return new KafkaAppendCapacityControllerV1(new KafkaAppendCapacityBudgetV1(groups, entries, bytes));
    }
}
