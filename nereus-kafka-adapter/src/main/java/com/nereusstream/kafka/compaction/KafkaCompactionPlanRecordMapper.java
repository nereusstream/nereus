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

package com.nereusstream.kafka.compaction;

import com.nereusstream.metadata.oxia.KafkaPartitionId;
import com.nereusstream.metadata.oxia.records.KafkaCompactionPlanRecord;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/**
 * Closed mapping between validated KCP1 bytes and their immutable Oxia attachment.
 */
public final class KafkaCompactionPlanRecordMapper {
    private final KafkaCompactionPlanCodecV1 codec;

    public KafkaCompactionPlanRecordMapper() {
        this(new KafkaCompactionPlanCodecV1());
    }

    KafkaCompactionPlanRecordMapper(KafkaCompactionPlanCodecV1 codec) {
        this.codec = Objects.requireNonNull(codec, "codec");
    }

    public KafkaCompactionPlanRecord toRecord(
            KafkaPartitionId partition, KafkaCompactionPlan plan, long createdAtMillis) {
        KafkaPartitionId exactPartition = Objects.requireNonNull(partition, "partition");
        KafkaCompactionPlan exactPlan = Objects.requireNonNull(plan, "plan");
        if (createdAtMillis < 0) {
            throw new IllegalArgumentException("Kafka compaction plan creation time is negative");
        }
        byte[] encoded = codec.encode(exactPlan);
        return new KafkaCompactionPlanRecord(
                KafkaCompactionPlanRecord.FORMAT_VERSION,
                exactPartition.kafkaClusterId(),
                exactPartition.topicId(),
                exactPartition.partitionId(),
                exactPlan.streamId().value(),
                exactPlan.planId(),
                exactPlan.materializationTaskId(),
                exactPlan.candidate().outputCoverage().startOffset(),
                exactPlan.candidate().outputCoverage().endOffset(),
                exactPlan.candidate().decisionHorizon().endOffset(),
                sha256(encoded),
                encoded,
                createdAtMillis,
                0);
    }

    public KafkaCompactionPlan fromRecord(KafkaCompactionPlanRecord record) {
        KafkaCompactionPlanRecord exact = Objects.requireNonNull(record, "record");
        KafkaCompactionPlan plan = codec.decode(exact.planBytes());
        if (!plan.streamId().value().equals(exact.streamId())
                || !plan.planId().equals(exact.planId())
                || !plan.materializationTaskId().equals(exact.materializationTaskId())
                || plan.candidate().outputCoverage().startOffset() != exact.outputStartOffset()
                || plan.candidate().outputCoverage().endOffset() != exact.outputEndOffset()
                || plan.candidate().decisionHorizon().endOffset() != exact.decisionEndOffset()) {
            throw new IllegalArgumentException("Kafka compaction plan attachment differs from canonical KCP1 bytes");
        }
        return plan;
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }
}
