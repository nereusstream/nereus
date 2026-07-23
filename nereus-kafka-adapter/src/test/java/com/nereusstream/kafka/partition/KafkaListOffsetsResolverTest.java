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

package com.nereusstream.kafka.partition;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.AcquiredAppendSession;
import com.nereusstream.api.AppendAuthority;
import com.nereusstream.api.AppendSession;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.kafka.checkpoint.KafkaCheckpointSourceState;
import com.nereusstream.kafka.codec.KafkaAppendBatchEncoder;
import com.nereusstream.kafka.codec.KafkaFetchAssembler;
import com.nereusstream.kafka.codec.KafkaRecordBatchCodec;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.junit.jupiter.api.Test;

class KafkaListOffsetsResolverTest {
    @Test
    void resolvesEarliestAndLatestFromOneStableSnapshotAndFencesStaleLeaders() {
        Fixture fixture = fixture();
        append(fixture, 0, KafkaPartitionStorageTestSupport.batch(
                0, CompressionType.NONE, 1_000, "a", "b"));
        append(fixture, 2, KafkaPartitionStorageTestSupport.batch(
                2, CompressionType.GZIP, 2_000, "c"));
        KafkaListOffsetsResolver resolver = resolver(fixture);

        KafkaListOffsetResult earliest = resolver.resolve(request(
                KafkaListOffsetQuery.EARLIEST, OptionalLong.empty(), 1_024 * 1_024)).join().orElseThrow();
        KafkaListOffsetResult latest = resolver.resolve(request(
                KafkaListOffsetQuery.LATEST, OptionalLong.empty(), 1_024 * 1_024)).join().orElseThrow();

        assertThat(earliest.offset()).isZero();
        assertThat(earliest.timestampMillis()).isEmpty();
        assertThat(latest.offset()).isEqualTo(3);
        assertThat(latest.timestampMillis()).isEmpty();
        assertThat(earliest.sourceSnapshot()).isEqualTo(latest.sourceSnapshot());
        assertFailureCode(resolver.resolve(new KafkaListOffsetsRequest(
                KafkaListOffsetQuery.LATEST,
                OptionalLong.empty(),
                4,
                100,
                1_024 * 1_024,
                4_096,
                1_024 * 1_024,
                10,
                Duration.ofSeconds(5))), ErrorCode.FENCED_APPEND);

        fixture.storage.resign().join();
        assertFailureCode(resolver.resolve(request(
                KafkaListOffsetQuery.LATEST, OptionalLong.empty(), 1_024 * 1_024)), ErrorCode.FENCED_APPEND);
    }

    @Test
    void findsTheFirstExactRecordTimestampAcrossCompressedPages() {
        Fixture fixture = fixture();
        byte[] first = KafkaPartitionStorageTestSupport.batch(
                0, CompressionType.GZIP, 1_000, "a", "b");
        byte[] second = KafkaPartitionStorageTestSupport.batch(
                2, CompressionType.NONE, 2_000, "c", "d");
        append(fixture, 0, first);
        append(fixture, 2, second);
        KafkaListOffsetsResolver resolver = resolver(fixture);
        long scanBytes = Math.addExact(first.length, second.length);

        KafkaListOffsetResult insideFirst = resolver.resolve(request(
                KafkaListOffsetQuery.TIMESTAMP, OptionalLong.of(1_001), scanBytes, first.length))
                .join().orElseThrow();
        KafkaListOffsetResult inSecondPage = resolver.resolve(request(
                KafkaListOffsetQuery.TIMESTAMP, OptionalLong.of(1_500), scanBytes, first.length))
                .join().orElseThrow();
        Optional<KafkaListOffsetResult> missing = resolver.resolve(request(
                KafkaListOffsetQuery.TIMESTAMP, OptionalLong.of(3_000), scanBytes, first.length)).join();

        assertThat(insideFirst.timestampMillis()).hasValue(1_001);
        assertThat(insideFirst.offset()).isEqualTo(1);
        assertThat(inSecondPage.timestampMillis()).hasValue(2_000);
        assertThat(inSecondPage.offset()).isEqualTo(2);
        assertThat(missing).isEmpty();
    }

    @Test
    void maxTimestampUsesTheExactRecordAndLowestOffsetTieBreak() {
        Fixture fixture = fixture();
        byte[] first = KafkaPartitionStorageTestSupport.batch(
                0, CompressionType.GZIP, 5_000, "a", "b");
        byte[] second = KafkaPartitionStorageTestSupport.batch(
                2, CompressionType.NONE, 1_000, "c");
        byte[] tied = KafkaPartitionStorageTestSupport.batch(
                3, CompressionType.NONE, 5_001, "d");
        append(fixture, 0, first);
        append(fixture, 2, second);
        append(fixture, 3, tied);
        long scanBytes = Math.addExact(Math.addExact(first.length, second.length), tied.length);

        KafkaListOffsetResult maximum = resolver(fixture).resolve(request(
                KafkaListOffsetQuery.MAX_TIMESTAMP, OptionalLong.empty(), scanBytes, first.length))
                .join().orElseThrow();

        assertThat(maximum.timestampMillis()).hasValue(5_001);
        assertThat(maximum.offset()).isEqualTo(1);
    }

    @Test
    void scanBudgetAndInspectorViolationsFailWithoutAnApproximateOffset() {
        Fixture fixture = fixture();
        byte[] batch = KafkaPartitionStorageTestSupport.batch(
                0, CompressionType.GZIP, 1_000, "a", "b");
        append(fixture, 0, batch);
        KafkaListOffsetsResolver resolver = resolver(fixture);

        assertFailureCode(resolver.resolve(request(
                KafkaListOffsetQuery.TIMESTAMP,
                OptionalLong.of(9_000),
                batch.length - 1)), ErrorCode.METADATA_LIMIT_EXCEEDED);

        KafkaRecordTimestampInspector invalid = new KafkaRecordTimestampInspector() {
            @Override
            public Optional<KafkaTimestampAndOffset> firstAtOrAfter(
                    ByteBuffer exactRecords, long minimumOffset, long targetTimestampMillis) {
                return Optional.of(new KafkaTimestampAndOffset(targetTimestampMillis, 99, OptionalInt.empty()));
            }

            @Override
            public Optional<KafkaTimestampAndOffset> maximum(ByteBuffer exactRecords, long minimumOffset) {
                return Optional.empty();
            }
        };
        assertFailureCode(new KafkaListOffsetsResolver(fixture.storage, invalid).resolve(request(
                KafkaListOffsetQuery.TIMESTAMP,
                OptionalLong.of(1_000),
                1_024 * 1_024)), ErrorCode.METADATA_INVARIANT_VIOLATION);
    }

    @Test
    void fencesWhenLeadershipIsLostWhileInspectingAReadPage() {
        Fixture fixture = fixture();
        append(fixture, 0, KafkaPartitionStorageTestSupport.batch(
                0, CompressionType.NONE, 1_000, "a"));
        KafkaRecordTimestampInspector resigning = new KafkaRecordTimestampInspector() {
            @Override
            public Optional<KafkaTimestampAndOffset> firstAtOrAfter(
                    ByteBuffer exactRecords, long minimumOffset, long targetTimestampMillis) {
                fixture.storage.resign().join();
                return Optional.of(new KafkaTimestampAndOffset(
                        targetTimestampMillis, minimumOffset, OptionalInt.empty()));
            }

            @Override
            public Optional<KafkaTimestampAndOffset> maximum(ByteBuffer exactRecords, long minimumOffset) {
                return Optional.empty();
            }
        };

        assertFailureCode(new KafkaListOffsetsResolver(fixture.storage, resigning).resolve(request(
                KafkaListOffsetQuery.TIMESTAMP,
                OptionalLong.of(1_000),
                1_024 * 1_024)), ErrorCode.FENCED_APPEND);
    }

    private static KafkaListOffsetsResolver resolver(Fixture fixture) {
        return new KafkaListOffsetsResolver(fixture.storage, new StockKafkaTimestampInspector());
    }

    private static KafkaListOffsetsRequest request(
            KafkaListOffsetQuery query,
            OptionalLong targetTimestamp,
            long maxScanBytes) {
        return request(
                query,
                targetTimestamp,
                maxScanBytes,
                (int) Math.min(1_024 * 1_024, maxScanBytes));
    }

    private static KafkaListOffsetsRequest request(
            KafkaListOffsetQuery query,
            OptionalLong targetTimestamp,
            long maxScanBytes,
            int readTargetBytes) {
        return new KafkaListOffsetsRequest(
                query,
                targetTimestamp,
                5,
                100,
                maxScanBytes,
                readTargetBytes,
                1_024 * 1_024,
                10,
                Duration.ofSeconds(5));
    }

    private static void append(Fixture fixture, long startOffset, byte[] records) {
        CompletableFuture<KafkaStableAppendResult> append = fixture.storage.append(
                ByteBuffer.wrap(records),
                new KafkaAppendContext(
                        startOffset, 5, (short) 1, Duration.ofSeconds(5), java.util.Map.of()));
        fixture.streams.completeNextSuccess();
        long stableEndOffset = append.join().stableSnapshot().stableEndOffset();
        fixture.storage.publishDerivedOffsets(
                stableEndOffset, stableEndOffset, stableEndOffset);
    }

    private static Fixture fixture() {
        KafkaPartitionIdentity identity = KafkaPartitionStorageTestSupport.identity();
        StreamId streamId = new StreamId("kafka-list-offsets-stream");
        AppendAuthority authority = new AppendAuthority(
                "kafka-partition-leader-v1",
                identity.durableId().canonicalIdentity(),
                5,
                "1",
                9);
        AppendSession session = new AppendSession(streamId, "broker-run", 7, "token", 11, 100_000);
        AcquiredAppendSession acquired = new AcquiredAppendSession(session, Optional.of(authority));
        KafkaCheckpointSourceState source = new KafkaCheckpointSourceState(
                authority,
                session.writerId(),
                session.epoch(),
                session.fencingToken(),
                session.leaseVersion(),
                0,
                0,
                0,
                "",
                new Checksum(ChecksumType.SHA256, "00".repeat(32)),
                false,
                0);
        KafkaPartitionStreamStorageFake streams = new KafkaPartitionStreamStorageFake(streamId, 0, 0);
        KafkaRecordBatchCodec codec = new KafkaRecordBatchCodec();
        DefaultKafkaPartitionStorage storage = new DefaultKafkaPartitionStorage(
                identity,
                streams,
                streamId,
                acquired,
                source,
                KafkaStorageProfilePolicy.forProfile(StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT),
                new KafkaAppendBatchEncoder(codec),
                new KafkaFetchAssembler(codec));
        return new Fixture(streams, storage);
    }

    private static void assertFailureCode(CompletableFuture<?> future, ErrorCode expected) {
        assertThatThrownBy(future::join)
                .isInstanceOf(CompletionException.class)
                .hasRootCauseInstanceOf(NereusException.class)
                .rootCause()
                .extracting(value -> ((NereusException) value).code())
                .isEqualTo(expected);
    }

    private record Fixture(
            KafkaPartitionStreamStorageFake streams,
            DefaultKafkaPartitionStorage storage) {}

    private static final class StockKafkaTimestampInspector implements KafkaRecordTimestampInspector {
        @Override
        public Optional<KafkaTimestampAndOffset> firstAtOrAfter(
                ByteBuffer exactRecords,
                long minimumOffset,
                long targetTimestampMillis) {
            for (RecordBatch batch : MemoryRecords.readableRecords(exactRecords.duplicate()).batches()) {
                for (org.apache.kafka.common.record.Record record : batch) {
                    if (record.offset() >= minimumOffset && record.timestamp() >= targetTimestampMillis) {
                        return Optional.of(timestampAndOffset(batch, record));
                    }
                }
            }
            return Optional.empty();
        }

        @Override
        public Optional<KafkaTimestampAndOffset> maximum(
                ByteBuffer exactRecords,
                long minimumOffset) {
            KafkaTimestampAndOffset maximum = null;
            for (RecordBatch batch : MemoryRecords.readableRecords(exactRecords.duplicate()).batches()) {
                for (org.apache.kafka.common.record.Record record : batch) {
                    if (record.offset() < minimumOffset || record.timestamp() < 0) {
                        continue;
                    }
                    KafkaTimestampAndOffset candidate = timestampAndOffset(batch, record);
                    if (maximum == null
                            || candidate.timestampMillis() > maximum.timestampMillis()
                            || (candidate.timestampMillis() == maximum.timestampMillis()
                                    && candidate.offset() < maximum.offset())) {
                        maximum = candidate;
                    }
                }
            }
            return Optional.ofNullable(maximum);
        }

        private static KafkaTimestampAndOffset timestampAndOffset(
                RecordBatch batch,
                org.apache.kafka.common.record.Record record) {
            OptionalInt epoch = batch.partitionLeaderEpoch() >= 0
                    ? OptionalInt.of(batch.partitionLeaderEpoch())
                    : OptionalInt.empty();
            return new KafkaTimestampAndOffset(record.timestamp(), record.offset(), epoch);
        }
    }
}
