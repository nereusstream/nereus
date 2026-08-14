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

import com.nereusstream.domain.identity.Id128;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaAssignedRecordBatchGroupAdapterV1;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaNativeAssignedRecordBatchV1;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaNativeRecordBatchFactsV1;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaNbke2AssignedAppendGroupV1;
import com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperDataAdmissionTicketV1;
import com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperDataAdmissionV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaBookKeeperRunLifecycleV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaRunTestFixtures;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongFunction;
import java.util.zip.CRC32C;

final class KafkaPipelineTestFixtures {
    private static final int LOG_OVERHEAD = 12;
    private static final int LEADER_EPOCH_OFFSET = 12;
    private static final int MAGIC_OFFSET = 16;
    private static final int CRC_OFFSET = 17;
    private static final int CRC_DOMAIN_OFFSET = 21;
    private static final int LAST_OFFSET_DELTA_OFFSET = 23;
    private static final int BATCH_OVERHEAD = 61;

    private KafkaPipelineTestFixtures() {}

    static KafkaBookKeeperRunLifecycleV1 lifecycle(
            KafkaRunTestFixtures.FakeSession session, KafkaRunTestFixtures.FakeRootAuthority roots) {
        return KafkaBookKeeperRunLifecycleV1.createActive(session, roots, binding(), 100)
                .toCompletableFuture()
                .join();
    }

    static Nbke2RunBindingV1 binding() {
        return KafkaRunTestFixtures.binding(6, 11, 5);
    }

    static Plan plan(KafkaBookKeeperRunLifecycleV1 lifecycle, int members, long startOffset) {
        List<KafkaNativeAssignedRecordBatchV1> batches = new ArrayList<>();
        for (int index = 0; index < members; index++) {
            batches.add(assigned(startOffset + index));
        }
        KafkaBookKeeperDataAdmissionV1 admission = KafkaBookKeeperDataAdmissionV1.admitProfile(
                binding(), new KafkaRunTestFixtures.FakeSession().capability, 1_000_000);
        List<KafkaBookKeeperDataAdmissionTicketV1> tickets = new ArrayList<>();
        for (int index = 0; index < members; index++) {
            tickets.add(admission.admitBeforeOffsetAllocation(
                    batches.get(index).rawAssignedRecordBatch().length(), index, members));
        }
        KafkaPartitionFenceV1 fence = new KafkaPartitionFenceV1(
                binding().bindingId(),
                binding().topicIncarnation(),
                binding().partitionId(),
                13,
                binding().storageEpochId(),
                binding().creatorOwnerEpoch(),
                binding().kafkaLeaderEpoch());
        LongFunction<KafkaNbke2AssignedAppendGroupV1> factory =
                firstEntryId -> KafkaAssignedRecordBatchGroupAdapterV1.adapt(
                        fence,
                        binding(),
                        firstEntryId,
                        new Id128(0, startOffset + 8),
                        new Id128(0, startOffset + 9),
                        batches,
                        tickets);
        KafkaNbke2AssignedAppendGroupV1 sample = factory.apply(1);
        long encodedBytes = sample
                .encode(lifecycle.snapshot().handle().ledgerIdentity().ledgerId())
                .stream()
                .mapToLong(bytes -> bytes.length())
                .sum();
        KafkaOffsetAssignedAppendV1 assignment =
                new KafkaOffsetAssignedAppendV1(startOffset, startOffset + members, factory);
        return new Plan(new KafkaAppendAdmissionRequestV1(members, encodedBytes), assignment);
    }

    private static KafkaNativeAssignedRecordBatchV1 assigned(long baseOffset) {
        byte[] raw = new byte[BATCH_OVERHEAD];
        ByteBuffer buffer = ByteBuffer.wrap(raw).order(ByteOrder.BIG_ENDIAN);
        buffer.putLong(0, baseOffset);
        buffer.putInt(Long.BYTES, raw.length - LOG_OVERHEAD);
        buffer.putInt(LEADER_EPOCH_OFFSET, 5);
        buffer.put(MAGIC_OFFSET, (byte) 2);
        buffer.putInt(LAST_OFFSET_DELTA_OFFSET, 0);
        buffer.putLong(43, -1L);
        buffer.putShort(51, (short) -1);
        buffer.putInt(53, -1);
        long crc = crc(raw);
        buffer.putInt(CRC_OFFSET, (int) crc);
        return KafkaNativeAssignedRecordBatchV1.validate(new Facts(raw, baseOffset, crc));
    }

    private static long crc(byte[] raw) {
        CRC32C crc = new CRC32C();
        crc.update(raw, CRC_DOMAIN_OFFSET, raw.length - CRC_DOMAIN_OFFSET);
        return crc.getValue();
    }

    record Plan(KafkaAppendAdmissionRequestV1 request, KafkaOffsetAssignedAppendV1 assignment) {}

    private record Facts(byte[] rawAssignedRecordBatch, long baseOffset, long crc)
            implements KafkaNativeRecordBatchFactsV1 {
        @Override
        public int batchCount() {
            return 1;
        }

        @Override
        public int completeBytes() {
            return rawAssignedRecordBatch.length;
        }

        @Override
        public long lastOffset() {
            return baseOffset;
        }

        @Override
        public int partitionLeaderEpoch() {
            return 5;
        }

        @Override
        public byte magic() {
            return 2;
        }

        @Override
        public long storedCrc32c() {
            return crc;
        }

        @Override
        public long computedCrc32c() {
            return crc;
        }
    }
}
