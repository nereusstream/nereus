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
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperDataAdmissionTicketV1;
import com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperDataAdmissionV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2CodecV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2DataV1;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class KafkaNbke2AssignedAppendGroupV1Test {
    @Test
    void codecRoundTripPreservesTheCompleteAssignedRecordBatchBytes() {
        KafkaNativeAssignedRecordBatchV1 batch = KafkaK2AdapterFixtures.assigned(100, 2);
        KafkaNbke2AssignedAppendGroupV1 group = group(List.of(batch));

        CanonicalBytes encoded = group.encode(41).get(0);
        Nbke2DataV1 decoded = (Nbke2DataV1) Nbke2CodecV1.decode(encoded.toByteArray(), 41, 1);

        assertThat(decoded.rawAssignedRecordBatch()).isEqualTo(batch.rawAssignedRecordBatch());
        assertThat(decoded.baseOffset()).isEqualTo(batch.baseOffset());
        assertThat(decoded.lastOffsetDelta()).isEqualTo(batch.lastOffsetDelta());
    }

    @Test
    void codecBindsOrderedPhysicalEntryIdsAndOnlyTheLastDescriptor() {
        List<KafkaNativeAssignedRecordBatchV1> batches =
                List.of(KafkaK2AdapterFixtures.assigned(100, 0), KafkaK2AdapterFixtures.assigned(101, 1));
        KafkaNbke2AssignedAppendGroupV1 group = group(batches);

        List<CanonicalBytes> encoded = group.encode(41);
        Nbke2DataV1 first = (Nbke2DataV1) Nbke2CodecV1.decode(encoded.get(0).toByteArray(), 41, 1);
        Nbke2DataV1 last = (Nbke2DataV1) Nbke2CodecV1.decode(encoded.get(1).toByteArray(), 41, 2);

        assertThat(first.terminalDescriptor()).isEmpty();
        assertThat(last.terminalDescriptor()).isPresent();
        assertThat(last.terminalDescriptor().orElseThrow().lastDataEntryId()).isEqualTo(2);
        assertThat(first.rawAssignedRecordBatch()).isEqualTo(batches.get(0).rawAssignedRecordBatch());
        assertThat(last.rawAssignedRecordBatch()).isEqualTo(batches.get(1).rawAssignedRecordBatch());
    }

    @Test
    void outputFramesAndEncodedBytesAreImmutableSnapshots() {
        KafkaNativeAssignedRecordBatchV1 batch = KafkaK2AdapterFixtures.assigned(100, 0);
        KafkaNbke2AssignedAppendGroupV1 original = group(List.of(batch));
        List<Nbke2DataV1> mutable = new ArrayList<>(original.dataFrames());
        KafkaNbke2AssignedAppendGroupV1 copied = new KafkaNbke2AssignedAppendGroupV1(1, mutable);
        mutable.clear();
        byte[] encoded = copied.encode(41).get(0).toByteArray();
        encoded[0] ^= 1;

        assertThat(copied.dataFrames()).hasSize(1);
        assertThat(copied.encode(41).get(0).toByteArray()[0]).isNotEqualTo(encoded[0]);
        assertThatThrownBy(() -> copied.dataFrames().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    private static KafkaNbke2AssignedAppendGroupV1 group(List<KafkaNativeAssignedRecordBatchV1> batches) {
        KafkaBookKeeperDataAdmissionV1 admission = KafkaK2AdapterFixtures.admission();
        List<KafkaBookKeeperDataAdmissionTicketV1> tickets = new ArrayList<>();
        for (int index = 0; index < batches.size(); index++) {
            tickets.add(admission.admitBeforeOffsetAllocation(
                    batches.get(index).rawAssignedRecordBatch().length(), index, batches.size()));
        }
        return KafkaAssignedRecordBatchGroupAdapterV1.adapt(
                KafkaK2AdapterFixtures.fence(),
                KafkaK2AdapterFixtures.binding(),
                1,
                new Id128(0, 8),
                new Id128(0, 9),
                batches,
                tickets);
    }
}
