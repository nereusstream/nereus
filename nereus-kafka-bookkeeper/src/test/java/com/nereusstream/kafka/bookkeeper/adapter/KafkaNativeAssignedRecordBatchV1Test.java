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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.jupiter.api.Test;

class KafkaNativeAssignedRecordBatchV1Test {
    @Test
    void acceptsOneCompleteMagicV2BatchAndDerivesHalfOpenCoverage() {
        KafkaNativeAssignedRecordBatchV1 assigned = KafkaK2AdapterFixtures.assigned(100, 2);

        assertThat(assigned.baseOffset()).isEqualTo(100);
        assertThat(assigned.lastOffsetDelta()).isEqualTo(2);
        assertThat(assigned.endOffsetExclusive()).isEqualTo(103);
        assertThat(assigned.partitionLeaderEpoch()).isEqualTo(5);
        assertThat(assigned.rawAssignedRecordBatch().length()).isEqualTo(61);
    }

    @Test
    void copiesNativeBytesBeforeReturningValidatedFacts() {
        byte[] raw = KafkaK2AdapterFixtures.batch(100, 0, 5);
        KafkaNativeAssignedRecordBatchV1 assigned =
                KafkaNativeAssignedRecordBatchV1.validate(KafkaK2AdapterFixtures.facts(raw));

        raw[0] ^= 1;

        assertThat(assigned.rawAssignedRecordBatch().toByteArray()).isEqualTo(KafkaK2AdapterFixtures.batch(100, 0, 5));
    }

    @Test
    void rejectsZeroOrMultipleNativeBatches() {
        KafkaK2AdapterFixtures.Facts facts = KafkaK2AdapterFixtures.facts(KafkaK2AdapterFixtures.batch(100, 0, 5));

        assertRejected(
                new KafkaK2AdapterFixtures.Facts(
                        facts.rawAssignedRecordBatch(),
                        0,
                        facts.completeBytes(),
                        facts.baseOffset(),
                        facts.lastOffset(),
                        facts.partitionLeaderEpoch(),
                        facts.magic(),
                        facts.storedCrc32c(),
                        facts.computedCrc32c()),
                KafkaAssignedRecordBatchRejectionV1.BATCH_COUNT_MISMATCH);
        assertRejected(
                new KafkaK2AdapterFixtures.Facts(
                        facts.rawAssignedRecordBatch(),
                        2,
                        facts.completeBytes(),
                        facts.baseOffset(),
                        facts.lastOffset(),
                        facts.partitionLeaderEpoch(),
                        facts.magic(),
                        facts.storedCrc32c(),
                        facts.computedCrc32c()),
                KafkaAssignedRecordBatchRejectionV1.BATCH_COUNT_MISMATCH);
    }

    @Test
    void rejectsTruncationAndDeclaredLengthMismatchBeforeHeaderFacts() {
        byte[] truncated = new byte[60];
        truncated[KafkaNativeAssignedRecordBatchV1.MAGIC_OFFSET] = 2;
        assertRejected(
                new KafkaK2AdapterFixtures.Facts(truncated, 1, 60, 0, 0, 0, (byte) 2, 0, 0),
                KafkaAssignedRecordBatchRejectionV1.TRUNCATED_BATCH);

        byte[] wrongLength = KafkaK2AdapterFixtures.batch(100, 0, 5);
        ByteBuffer.wrap(wrongLength).order(ByteOrder.BIG_ENDIAN).putInt(Long.BYTES, wrongLength.length);
        assertRejected(
                KafkaK2AdapterFixtures.facts(wrongLength),
                KafkaAssignedRecordBatchRejectionV1.DECLARED_LENGTH_MISMATCH);
    }

    @Test
    void rejectsLegacyOrMismatchedMagic() {
        byte[] legacy = KafkaK2AdapterFixtures.batch(100, 0, 5);
        legacy[KafkaNativeAssignedRecordBatchV1.MAGIC_OFFSET] = 1;

        assertRejected(KafkaK2AdapterFixtures.facts(legacy), KafkaAssignedRecordBatchRejectionV1.UNSUPPORTED_MAGIC);
    }

    @Test
    void rejectsInvalidOrNativeMismatchedOffsetCoverage() {
        byte[] negative = KafkaK2AdapterFixtures.batch(-1, 0, 5);
        assertRejected(
                KafkaK2AdapterFixtures.facts(negative), KafkaAssignedRecordBatchRejectionV1.INVALID_OFFSET_COVERAGE);

        KafkaK2AdapterFixtures.Facts facts = KafkaK2AdapterFixtures.facts(KafkaK2AdapterFixtures.batch(100, 2, 5));
        assertRejected(
                new KafkaK2AdapterFixtures.Facts(
                        facts.rawAssignedRecordBatch(),
                        1,
                        facts.completeBytes(),
                        facts.baseOffset(),
                        facts.lastOffset() + 1,
                        facts.partitionLeaderEpoch(),
                        facts.magic(),
                        facts.storedCrc32c(),
                        facts.computedCrc32c()),
                KafkaAssignedRecordBatchRejectionV1.NATIVE_FACT_MISMATCH);
    }

    @Test
    void rejectsUnassignedOrMismatchedLeaderEpoch() {
        assertRejected(
                KafkaK2AdapterFixtures.facts(KafkaK2AdapterFixtures.batch(100, 0, -1)),
                KafkaAssignedRecordBatchRejectionV1.LEADER_EPOCH_MISMATCH);

        KafkaK2AdapterFixtures.Facts facts = KafkaK2AdapterFixtures.facts(KafkaK2AdapterFixtures.batch(100, 0, 5));
        assertRejected(
                new KafkaK2AdapterFixtures.Facts(
                        facts.rawAssignedRecordBatch(),
                        1,
                        facts.completeBytes(),
                        facts.baseOffset(),
                        facts.lastOffset(),
                        6,
                        facts.magic(),
                        facts.storedCrc32c(),
                        facts.computedCrc32c()),
                KafkaAssignedRecordBatchRejectionV1.LEADER_EPOCH_MISMATCH);
    }

    @Test
    void separatesNativeCrcFactMismatchFromRawCrcCorruption() {
        KafkaK2AdapterFixtures.Facts facts = KafkaK2AdapterFixtures.facts(KafkaK2AdapterFixtures.batch(100, 0, 5));
        assertRejected(
                new KafkaK2AdapterFixtures.Facts(
                        facts.rawAssignedRecordBatch(),
                        1,
                        facts.completeBytes(),
                        facts.baseOffset(),
                        facts.lastOffset(),
                        facts.partitionLeaderEpoch(),
                        facts.magic(),
                        facts.storedCrc32c(),
                        facts.computedCrc32c() + 1),
                KafkaAssignedRecordBatchRejectionV1.NATIVE_FACT_MISMATCH);

        byte[] corrupt = facts.rawAssignedRecordBatch().clone();
        corrupt[corrupt.length - 1] ^= 1;
        KafkaK2AdapterFixtures.Facts corruptFacts = KafkaK2AdapterFixtures.facts(corrupt);
        assertRejected(corruptFacts, KafkaAssignedRecordBatchRejectionV1.CRC_MISMATCH);
    }

    private static void assertRejected(
            KafkaNativeRecordBatchFactsV1 facts, KafkaAssignedRecordBatchRejectionV1 expected) {
        assertThatThrownBy(() -> KafkaNativeAssignedRecordBatchV1.validate(facts))
                .isInstanceOfSatisfying(
                        KafkaAssignedRecordBatchException.class,
                        rejection -> assertThat(rejection.rejection()).isEqualTo(expected));
    }
}
