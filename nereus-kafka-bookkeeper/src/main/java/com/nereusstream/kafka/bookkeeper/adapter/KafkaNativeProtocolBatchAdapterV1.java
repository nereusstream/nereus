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

import com.nereusstream.kafka.bookkeeper.commit.KafkaBatchDuplicateIdentityV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaProtocolBatchDeltaV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionBatchKindV1;
import com.nereusstream.kafka.bookkeeper.recovery.KafkaRecoveryBatchProtocolAdapterV1;
import java.nio.ByteBuffer;
import java.util.Optional;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;

/** Native RecordBatch semantics used by both append preparation and closed-history replay. */
public final class KafkaNativeProtocolBatchAdapterV1 implements KafkaRecoveryBatchProtocolAdapterV1 {
    @Override
    public KafkaProtocolBatchDeltaV1 protocolDelta(KafkaNativeAssignedRecordBatchV1 assigned) {
        var records = MemoryRecords.readableRecords(
                ByteBuffer.wrap(assigned.rawAssignedRecordBatch().toByteArray()));
        var batches = records.batches().iterator();
        if (!batches.hasNext()) {
            throw new IllegalArgumentException("assigned bytes have no complete native batch");
        }
        var batch = batches.next();
        batch.ensureValid();
        if (batches.hasNext()
                || batch.magic() != RecordBatch.MAGIC_VALUE_V2
                || batch.sizeInBytes() != assigned.rawAssignedRecordBatch().length()
                || batch.baseOffset() != assigned.baseOffset()
                || batch.lastOffset() + 1 != assigned.endOffsetExclusive()
                || batch.partitionLeaderEpoch() != assigned.partitionLeaderEpoch()) {
            throw new IllegalArgumentException("native batch differs from assigned exact bytes");
        }
        long count = assigned.endOffsetExclusive() - assigned.baseOffset();
        if (batch.isControlBatch()) {
            var iterator = batch.iterator();
            if (!batch.hasProducerId() || !batch.isTransactional() || count != 1 || !iterator.hasNext()) {
                throw new IllegalArgumentException("transaction marker has invalid native producer or record coverage");
            }
            var marker = EndTransactionMarker.deserialize(iterator.next());
            if (iterator.hasNext()
                    || batch.baseSequence() != RecordBatch.NO_SEQUENCE
                    || batch.lastSequence() != RecordBatch.NO_SEQUENCE) {
                throw new IllegalArgumentException("transaction marker must contain one record and no sequence");
            }
            return new KafkaProtocolBatchDeltaV1(
                    1,
                    Optional.empty(),
                    marker.controlType() == ControlRecordType.ABORT
                            ? KafkaTransactionBatchKindV1.ABORT_MARKER
                            : KafkaTransactionBatchKindV1.COMMIT_MARKER,
                    batch.producerId(),
                    marker.coordinatorEpoch(),
                    batch.producerEpoch(),
                    batch.maxTimestamp());
        }
        var identity = batch.hasProducerId()
                ? Optional.of(new KafkaBatchDuplicateIdentityV1(
                        batch.producerId(), batch.producerEpoch(), batch.baseSequence(), batch.lastSequence()))
                : Optional.<KafkaBatchDuplicateIdentityV1>empty();
        if (batch.isTransactional() && identity.isEmpty()) {
            throw new IllegalArgumentException("transactional native DATA has no producer");
        }
        return new KafkaProtocolBatchDeltaV1(
                count,
                identity,
                batch.isTransactional()
                        ? KafkaTransactionBatchKindV1.TRANSACTIONAL_DATA
                        : KafkaTransactionBatchKindV1.NONE,
                batch.isTransactional() ? batch.producerId() : -1,
                -1,
                (short) -1,
                batch.maxTimestamp());
    }
}
