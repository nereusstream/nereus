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

import com.nereusstream.kafka.bookkeeper.adapter.KafkaAssignedRecordBatchException;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaAssignedRecordBatchRejectionV1;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaNativeAssignedRecordBatchV1;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaNativeRecordBatchFactsV1;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.Crc32C;

/** Source-qualified executable bridge between Kafka 4.3 native batches and the SDK-free Nereus K2 boundary. */
public final class Kafka43AssignedRecordBatchConformance {
    private static final int CRC_OFFSET = 17;
    private static final int CRC_DOMAIN_OFFSET = 21;
    private static int tests;

    private Kafka43AssignedRecordBatchConformance() {}

    public static void main(String[] args) {
        byte[] oneRecord = raw(MemoryRecords.withRecords(
                100L, Compression.NONE, 5, new SimpleRecord("key".getBytes(), "value".getBytes())));
        KafkaNativeAssignedRecordBatchV1 one = validate(oneRecord);
        check(one.baseOffset() == 100L && one.endOffsetExclusive() == 101L, "single-record coverage");
        check(one.partitionLeaderEpoch() == 5, "assigned leader epoch");
        check(java.util.Arrays.equals(one.rawAssignedRecordBatch().toByteArray(), oneRecord), "unchanged raw bytes");

        byte[] threeRecords = raw(MemoryRecords.withRecords(
                200L,
                Compression.NONE,
                7,
                new SimpleRecord("a".getBytes()),
                new SimpleRecord("b".getBytes()),
                new SimpleRecord("c".getBytes())));
        KafkaNativeAssignedRecordBatchV1 three = validate(threeRecords);
        check(three.endOffsetExclusive() == 203L && three.lastOffsetDelta() == 2, "multi-record coverage");
        check(three.storedCrc32c() == Crc32C.compute(threeRecords, CRC_DOMAIN_OFFSET,
                threeRecords.length - CRC_DOMAIN_OFFSET), "Kafka CRC32C parity");

        byte[] idempotent = raw(MemoryRecords.withIdempotentRecords(
                300L,
                Compression.NONE,
                19L,
                (short) 2,
                8,
                9,
                new SimpleRecord("idempotent".getBytes())));
        check(validate(idempotent).endOffsetExclusive() == 301L, "idempotent native batch");

        byte[] transactional = raw(MemoryRecords.withTransactionalRecords(
                400L,
                Compression.NONE,
                23L,
                (short) 3,
                11,
                12,
                new SimpleRecord("transactional".getBytes())));
        check(validate(transactional).endOffsetExclusive() == 401L, "transactional native batch");

        byte[] corruptPayload = oneRecord.clone();
        corruptPayload[corruptPayload.length - 1] ^= 1;
        expectNativeCorruption(corruptPayload, "payload corruption uses Kafka native rejection");

        byte[] corruptStoredCrc = oneRecord.clone();
        corruptStoredCrc[CRC_OFFSET] ^= 1;
        expectNativeCorruption(corruptStoredCrc, "stored CRC corruption uses Kafka native rejection");

        byte[] legacy = raw(MemoryRecords.withRecords(
                RecordBatch.MAGIC_VALUE_V1, 500L, Compression.NONE, new SimpleRecord("legacy".getBytes())));
        expectNereusRejection(
                nativeFacts(legacy),
                KafkaAssignedRecordBatchRejectionV1.UNSUPPORTED_MAGIC,
                "native-valid legacy magic remains outside K2");

        byte[] unassignedLeader = raw(MemoryRecords.withRecords(
                600L, Compression.NONE, RecordBatch.NO_PARTITION_LEADER_EPOCH,
                new SimpleRecord("unassigned".getBytes())));
        expectNereusRejection(
                nativeFacts(unassignedLeader),
                KafkaAssignedRecordBatchRejectionV1.LEADER_EPOCH_MISMATCH,
                "native-valid unassigned leader epoch remains outside K2");

        byte[] twoBatches = concatenate(oneRecord, threeRecords);
        expectNereusRejection(
                nativeFacts(twoBatches),
                KafkaAssignedRecordBatchRejectionV1.BATCH_COUNT_MISMATCH,
                "one K2 DATA member cannot contain two batches");

        KafkaNativeRecordBatchFactsV1 canonical = nativeFacts(oneRecord);
        expectNereusRejection(
                new Facts(
                        canonical.rawAssignedRecordBatch(),
                        canonical.batchCount(),
                        canonical.completeBytes(),
                        canonical.baseOffset() + 1,
                        canonical.lastOffset(),
                        canonical.partitionLeaderEpoch(),
                        canonical.magic(),
                        canonical.storedCrc32c(),
                        canonical.computedCrc32c()),
                KafkaAssignedRecordBatchRejectionV1.NATIVE_FACT_MISMATCH,
                "Nereus rejects substituted native facts");

        if (tests != 13) {
            throw new AssertionError("unexpected conformance test count: " + tests);
        }
        System.out.println("Kafka 4.3 K2 exact-source conformance: suites=1 tests=13 failures=0 errors=0 skips=0");
    }

    private static KafkaNativeAssignedRecordBatchV1 validate(byte[] raw) {
        return KafkaNativeAssignedRecordBatchV1.validate(nativeFacts(raw));
    }

    private static KafkaNativeRecordBatchFactsV1 nativeFacts(byte[] raw) {
        MemoryRecords records = MemoryRecords.readableRecords(ByteBuffer.wrap(raw));
        List<MutableRecordBatch> batches = new ArrayList<>();
        for (MutableRecordBatch batch : records.batches()) {
            batch.ensureValid();
            batches.add(batch);
        }
        if (batches.isEmpty()) {
            throw new IllegalArgumentException("Kafka native parser returned no complete RecordBatch");
        }
        MutableRecordBatch first = batches.get(0);
        long computedCrc = Crc32C.compute(raw, CRC_DOMAIN_OFFSET, first.sizeInBytes() - CRC_DOMAIN_OFFSET);
        return new Facts(
                raw,
                batches.size(),
                records.validBytes(),
                first.baseOffset(),
                first.lastOffset(),
                first.partitionLeaderEpoch(),
                first.magic(),
                first.checksum(),
                computedCrc);
    }

    private static byte[] raw(MemoryRecords records) {
        ByteBuffer buffer = records.buffer().duplicate();
        byte[] raw = new byte[buffer.remaining()];
        buffer.get(raw);
        return raw;
    }

    private static byte[] concatenate(byte[] first, byte[] second) {
        byte[] combined = new byte[first.length + second.length];
        System.arraycopy(first, 0, combined, 0, first.length);
        System.arraycopy(second, 0, combined, first.length, second.length);
        return combined;
    }

    private static void expectNativeCorruption(byte[] raw, String label) {
        try {
            nativeFacts(raw);
            throw new AssertionError(label + " did not fail");
        } catch (CorruptRecordException expected) {
            check(true, label);
        }
    }

    private static void expectNereusRejection(
            KafkaNativeRecordBatchFactsV1 facts,
            KafkaAssignedRecordBatchRejectionV1 expected,
            String label) {
        try {
            KafkaNativeAssignedRecordBatchV1.validate(facts);
            throw new AssertionError(label + " did not fail");
        } catch (KafkaAssignedRecordBatchException failure) {
            check(failure.rejection() == expected, label);
        }
    }

    private static void check(boolean condition, String label) {
        if (!condition) {
            throw new AssertionError(label);
        }
        tests++;
    }

    private record Facts(
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
