/*
 * Copyright 2026 NereusStream.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.kafka.bookkeeper.broker;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.DeterministicTopicIdsV1;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaNativeAssignedRecordBatchV1;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaNativeProtocolBatchAdapterV1;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaRawAssignedRecordBatchFactsV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.storage.object.control.WalRunControlCodec;
import com.nereusstream.storage.object.control.WalRunRootRecord;
import com.nereusstream.storage.object.nwg1.Nwg1ConstantsV1;
import com.nereusstream.storage.object.nwg1.Nwg1EnvelopeV1;
import com.nereusstream.storage.object.nwg1.Nwg1VerificationContextV1;
import java.nio.ByteBuffer;
import java.util.Arrays;

/** The exact native partition fence supplies both the authenticated witness and frame identity. */
public final class KafkaObjectNativeVerificationV1 {
    private static final int WITNESS_KIND = 0x4b4f5731; // KOW1

    private KafkaObjectNativeVerificationV1() {}

    /**
     * For writes the fence comes from the current native assignment; for recovery it comes from exact closed
     * authorization history. Verification of these bytes never grants ownership or makes an extent readable.
     */
    public static Nwg1VerificationContextV1 context(WalRunRootRecord root, KafkaPartitionFenceV1 fence) {
        if (!DeterministicTopicIdsV1.deriveBindingId(root.protocolCellIdentity(), fence.topicIncarnation())
                .equals(fence.bindingId())) {
            throw new IllegalArgumentException("native Kafka fence belongs to another Protocol Cell");
        }
        byte[] binding = fence.bindingId().digest().bytes().toByteArray();
        return new Nwg1VerificationContextV1(
                root.protocolCellIdentity(),
                root.providerScopeId().digest().bytes().toByteArray(),
                WalRunControlCodec.rootSha256(root).bytes().toByteArray(),
                Nwg1EnvelopeV1.decode(root.wrappedRunKey().framedBytes().toByteArray()),
                (actual, kind, version) -> kind == Nwg1ConstantsV1.CLOSED_KIND
                                && version == Nwg1ConstantsV1.CLOSED_VERSION
                                && Arrays.equals(binding, actual)
                        ? witness(fence).toByteArray()
                        : null,
                (bytes, partition, epoch, start, end) -> verifyFrame(fence, bytes, partition, epoch, start, end),
                0,
                0);
    }

    static CanonicalBytes witness(KafkaPartitionFenceV1 fence) {
        return CanonicalBytes.copyOf(ByteBuffer.allocate(3 * Integer.BYTES + 2 * Long.BYTES + 2 * Sha256Digest.LENGTH)
                .putInt(WITNESS_KIND)
                .put(fence.bindingId().digest().bytes().toByteArray())
                .put(fence.storageEpochId().digest().bytes().toByteArray())
                .putInt(fence.partitionId())
                .putLong(fence.bindingGeneration())
                .putLong(fence.ownerEpoch())
                .putInt(fence.kafkaLeaderEpoch())
                .array());
    }

    private static Nwg1VerificationContextV1.NativeCoverage verifyFrame(
            KafkaPartitionFenceV1 fence, byte[] bytes, long partition, long epoch, long start, long end) {
        var batch = KafkaNativeAssignedRecordBatchV1.validate(
                KafkaRawAssignedRecordBatchFactsV1.parse(CanonicalBytes.copyOf(bytes)));
        new KafkaNativeProtocolBatchAdapterV1().protocolDelta(batch);
        if (partition != fence.partitionId()
                || epoch != fence.kafkaLeaderEpoch()
                || batch.partitionLeaderEpoch() != epoch
                || batch.baseOffset() != start
                || batch.endOffsetExclusive() != end) {
            throw new IllegalArgumentException(
                    "Object WAL frame differs from the authenticated native partition/epoch");
        }
        return new Nwg1VerificationContextV1.NativeCoverage(start, end);
    }
}
