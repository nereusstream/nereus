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

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.KafkaTopicId;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.KafkaTopicName;
import com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperDataAdmissionV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperDigestTypeV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperProtocolModeV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperTimeoutClassV1;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.CRC32C;

final class KafkaK2AdapterFixtures {
    private KafkaK2AdapterFixtures() {}

    static KafkaNativeAssignedRecordBatchV1 assigned(long baseOffset, int lastOffsetDelta) {
        return KafkaNativeAssignedRecordBatchV1.validate(facts(batch(baseOffset, lastOffsetDelta, 5)));
    }

    static byte[] batch(long baseOffset, int lastOffsetDelta, int leaderEpoch) {
        byte[] raw = new byte[KafkaNativeAssignedRecordBatchV1.RECORD_BATCH_OVERHEAD_BYTES];
        ByteBuffer buffer = ByteBuffer.wrap(raw).order(ByteOrder.BIG_ENDIAN);
        buffer.putLong(0, baseOffset);
        buffer.putInt(Long.BYTES, raw.length - KafkaNativeAssignedRecordBatchV1.LOG_OVERHEAD_BYTES);
        buffer.putInt(KafkaNativeAssignedRecordBatchV1.PARTITION_LEADER_EPOCH_OFFSET, leaderEpoch);
        buffer.put(KafkaNativeAssignedRecordBatchV1.MAGIC_OFFSET, (byte) 2);
        buffer.putInt(KafkaNativeAssignedRecordBatchV1.LAST_OFFSET_DELTA_OFFSET, lastOffsetDelta);
        buffer.putLong(43, -1L);
        buffer.putShort(51, (short) -1);
        buffer.putInt(53, -1);
        long crc = crc32c(raw);
        buffer.putInt(KafkaNativeAssignedRecordBatchV1.CRC_OFFSET, (int) crc);
        return raw;
    }

    static Facts facts(byte[] raw) {
        ByteBuffer buffer = ByteBuffer.wrap(raw).order(ByteOrder.BIG_ENDIAN);
        long baseOffset = buffer.getLong(0);
        int lastOffsetDelta = buffer.getInt(KafkaNativeAssignedRecordBatchV1.LAST_OFFSET_DELTA_OFFSET);
        return new Facts(
                raw,
                1,
                raw.length,
                baseOffset,
                baseOffset + lastOffsetDelta,
                buffer.getInt(KafkaNativeAssignedRecordBatchV1.PARTITION_LEADER_EPOCH_OFFSET),
                buffer.get(KafkaNativeAssignedRecordBatchV1.MAGIC_OFFSET),
                Integer.toUnsignedLong(buffer.getInt(KafkaNativeAssignedRecordBatchV1.CRC_OFFSET)),
                crc32c(raw));
    }

    static Nbke2RunBindingV1 binding() {
        return new Nbke2RunBindingV1(
                new TopicBindingId(digest(1)),
                new KafkaTopicIncarnationIdentity(new KafkaTopicId(new Id128(0, 2)), new KafkaTopicName("orders")),
                7,
                new StorageEpochId(digest(3)),
                11,
                5,
                new CellProviderScopeId(digest(4)),
                new StorageRunId(new Id128(0, 6)));
    }

    static KafkaPartitionFenceV1 fence() {
        Nbke2RunBindingV1 binding = binding();
        return new KafkaPartitionFenceV1(
                binding.bindingId(),
                binding.topicIncarnation(),
                binding.partitionId(),
                13,
                binding.storageEpochId(),
                binding.creatorOwnerEpoch(),
                binding.kafkaLeaderEpoch());
    }

    static KafkaBookKeeperDataAdmissionV1 admission() {
        Nbke2RunBindingV1 binding = binding();
        int frameLimit = 10_000_000;
        BookKeeperCapabilitySnapshotV1 capability = new BookKeeperCapabilitySnapshotV1(
                binding.providerScopeId(),
                "cd06340851d6d657b7c7546df01df365c18980de",
                digest(5),
                "cd06340851d6d657b7c7546df01df365c18980de",
                digest(6),
                BookKeeperProtocolModeV1.V3,
                frameLimit,
                frameLimit,
                4_000_000,
                true,
                3,
                3,
                2,
                BookKeeperDigestTypeV1.CRC32C,
                true,
                true,
                new BookKeeperTimeoutClassV1(1_000, 2_000, 2_000, 5_000),
                "bk-credential:v7",
                digest(7));
        return KafkaBookKeeperDataAdmissionV1.admitProfile(binding, capability, 1_000_000);
    }

    static Sha256Digest digest(int lastByte) {
        byte[] bytes = new byte[Sha256Digest.LENGTH];
        bytes[bytes.length - 1] = (byte) lastByte;
        return Sha256Digest.copyOf(bytes);
    }

    static long crc32c(byte[] raw) {
        CRC32C crc = new CRC32C();
        crc.update(
                raw,
                KafkaNativeAssignedRecordBatchV1.CRC_DOMAIN_OFFSET,
                raw.length - KafkaNativeAssignedRecordBatchV1.CRC_DOMAIN_OFFSET);
        return crc.getValue();
    }

    record Facts(
            byte[] rawAssignedRecordBatch,
            int batchCount,
            int completeBytes,
            long baseOffset,
            long lastOffset,
            int partitionLeaderEpoch,
            byte magic,
            long storedCrc32c,
            long computedCrc32c)
            implements KafkaNativeRecordBatchFactsV1 {}
}
