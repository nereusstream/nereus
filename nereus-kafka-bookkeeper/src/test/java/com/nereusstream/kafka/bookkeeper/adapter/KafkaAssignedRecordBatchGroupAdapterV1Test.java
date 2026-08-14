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

package com.nereusstream.kafka.bookkeeper.adapter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperDataAdmissionTicketV1;
import com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperDataAdmissionV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import java.util.List;
import org.junit.jupiter.api.Test;

class KafkaAssignedRecordBatchGroupAdapterV1Test {
    @Test
    void adaptsOneAssignedBatchWithTheTerminalDescriptor() {
        KafkaNativeAssignedRecordBatchV1 batch = KafkaK2AdapterFixtures.assigned(100, 2);
        KafkaNbke2AssignedAppendGroupV1 group = adapt(List.of(batch), tickets(List.of(batch)));

        assertThat(group.dataFrames()).singleElement().satisfies(frame -> {
            assertThat(frame.baseOffset()).isEqualTo(100);
            assertThat(frame.endOffsetExclusive()).isEqualTo(103);
            assertThat(frame.rawAssignedRecordBatch()).isEqualTo(batch.rawAssignedRecordBatch());
            assertThat(frame.terminalDescriptor()).isPresent();
            assertThat(frame.terminalDescriptor().orElseThrow().firstDataEntryId())
                    .isEqualTo(1);
            assertThat(frame.terminalDescriptor().orElseThrow().lastDataEntryId())
                    .isEqualTo(1);
        });
    }

    @Test
    void adaptsOnlyContiguousMultiBatchCoverageAndOneTerminalMember() {
        List<KafkaNativeAssignedRecordBatchV1> batches =
                List.of(KafkaK2AdapterFixtures.assigned(100, 1), KafkaK2AdapterFixtures.assigned(102, 2));
        KafkaNbke2AssignedAppendGroupV1 group = adapt(batches, tickets(batches));

        assertThat(group.dataFrames()).hasSize(2);
        assertThat(group.dataFrames().get(0).terminalDescriptor()).isEmpty();
        assertThat(group.dataFrames().get(1).terminalDescriptor()).isPresent();
        assertThat(group.dataFrames().get(1).terminalDescriptor().orElseThrow().groupStartOffset())
                .isEqualTo(100);
        assertThat(group.dataFrames().get(1).terminalDescriptor().orElseThrow().groupEndOffsetExclusive())
                .isEqualTo(105);
    }

    @Test
    void rejectsEveryRunFenceMismatch() {
        KafkaPartitionFenceV1 fence = KafkaK2AdapterFixtures.fence();
        KafkaPartitionFenceV1 stale = new KafkaPartitionFenceV1(
                fence.bindingId(),
                fence.topicIncarnation(),
                fence.partitionId(),
                fence.bindingGeneration(),
                fence.storageEpochId(),
                fence.ownerEpoch(),
                fence.kafkaLeaderEpoch() + 1);
        KafkaNativeAssignedRecordBatchV1 batch = KafkaK2AdapterFixtures.assigned(100, 0);

        assertRejected(
                () -> adapt(stale, List.of(batch), tickets(List.of(batch))),
                KafkaAssignedRecordBatchRejectionV1.RUN_FENCE_MISMATCH);
    }

    @Test
    void rejectsGapsAndOverlapsInAssignedCoverage() {
        KafkaNativeAssignedRecordBatchV1 first = KafkaK2AdapterFixtures.assigned(100, 1);
        KafkaNativeAssignedRecordBatchV1 gap = KafkaK2AdapterFixtures.assigned(103, 0);
        KafkaNativeAssignedRecordBatchV1 overlap = KafkaK2AdapterFixtures.assigned(101, 0);

        assertRejected(
                () -> adapt(List.of(first, gap), tickets(List.of(first, gap))),
                KafkaAssignedRecordBatchRejectionV1.APPEND_GROUP_MISMATCH);
        assertRejected(
                () -> adapt(List.of(first, overlap), tickets(List.of(first, overlap))),
                KafkaAssignedRecordBatchRejectionV1.APPEND_GROUP_MISMATCH);
    }

    @Test
    void rejectsTicketLengthOrdinalCountAndTerminalMismatch() {
        KafkaNativeAssignedRecordBatchV1 batch = KafkaK2AdapterFixtures.assigned(100, 0);
        KafkaBookKeeperDataAdmissionV1 admission = KafkaK2AdapterFixtures.admission();
        KafkaBookKeeperDataAdmissionTicketV1 wrongCount = admission.admitBeforeOffsetAllocation(
                batch.rawAssignedRecordBatch().length(), 0, 2);

        assertRejected(
                () -> adapt(List.of(batch), List.of(wrongCount)),
                KafkaAssignedRecordBatchRejectionV1.APPEND_GROUP_MISMATCH);
    }

    @Test
    void rejectsEmptyIdsEmptyGroupsAndOverflowingEntryRanges() {
        KafkaNativeAssignedRecordBatchV1 batch = KafkaK2AdapterFixtures.assigned(100, 0);

        assertRejected(
                () -> KafkaAssignedRecordBatchGroupAdapterV1.adapt(
                        KafkaK2AdapterFixtures.fence(),
                        KafkaK2AdapterFixtures.binding(),
                        1,
                        new Id128(0, 0),
                        new Id128(0, 9),
                        List.of(batch),
                        tickets(List.of(batch))),
                KafkaAssignedRecordBatchRejectionV1.APPEND_GROUP_MISMATCH);
        assertRejected(() -> adapt(List.of(), List.of()), KafkaAssignedRecordBatchRejectionV1.APPEND_GROUP_MISMATCH);
        assertThatThrownBy(() -> new KafkaNbke2AssignedAppendGroupV1(
                        Long.MAX_VALUE,
                        List.of(
                                adapt(List.of(batch), tickets(List.of(batch)))
                                        .dataFrames()
                                        .get(0),
                                adapt(List.of(batch), tickets(List.of(batch)))
                                        .dataFrames()
                                        .get(0))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("overflows");
    }

    private static List<KafkaBookKeeperDataAdmissionTicketV1> tickets(List<KafkaNativeAssignedRecordBatchV1> batches) {
        KafkaBookKeeperDataAdmissionV1 admission = KafkaK2AdapterFixtures.admission();
        java.util.ArrayList<KafkaBookKeeperDataAdmissionTicketV1> tickets = new java.util.ArrayList<>();
        for (int index = 0; index < batches.size(); index++) {
            tickets.add(admission.admitBeforeOffsetAllocation(
                    batches.get(index).rawAssignedRecordBatch().length(), index, batches.size()));
        }
        return List.copyOf(tickets);
    }

    private static KafkaNbke2AssignedAppendGroupV1 adapt(
            List<KafkaNativeAssignedRecordBatchV1> batches, List<KafkaBookKeeperDataAdmissionTicketV1> tickets) {
        return adapt(KafkaK2AdapterFixtures.fence(), batches, tickets);
    }

    private static KafkaNbke2AssignedAppendGroupV1 adapt(
            KafkaPartitionFenceV1 fence,
            List<KafkaNativeAssignedRecordBatchV1> batches,
            List<KafkaBookKeeperDataAdmissionTicketV1> tickets) {
        return KafkaAssignedRecordBatchGroupAdapterV1.adapt(
                fence, KafkaK2AdapterFixtures.binding(), 1, new Id128(0, 8), new Id128(0, 9), batches, tickets);
    }

    private static void assertRejected(Runnable operation, KafkaAssignedRecordBatchRejectionV1 expected) {
        assertThatThrownBy(operation::run)
                .isInstanceOfSatisfying(
                        KafkaAssignedRecordBatchException.class,
                        rejection -> assertThat(rejection.rejection()).isEqualTo(expected));
    }
}
