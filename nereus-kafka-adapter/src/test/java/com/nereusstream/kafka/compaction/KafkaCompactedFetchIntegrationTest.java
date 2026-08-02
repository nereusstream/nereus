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

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.FirstEntryPolicy;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.PublicationId;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadRequest;
import com.nereusstream.api.ReadResult;
import com.nereusstream.api.ReadSourceRef;
import com.nereusstream.api.ReadTargetIdentities;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.SemanticReadResult;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.read.GenerationReadConstraint;
import com.nereusstream.kafka.codec.KafkaFetchAssembler;
import com.nereusstream.kafka.codec.KafkaFetchAssembly;
import com.nereusstream.kafka.codec.KafkaRecordBatchCodec;
import com.nereusstream.kafka.partition.KafkaStableSnapshot;
import com.nereusstream.kafka.partition.KafkaStorageReadRequest;
import com.nereusstream.kafka.testing.TestStreamStorage;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataStore;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.junit.jupiter.api.Test;

class KafkaCompactedFetchIntegrationTest {
    private static final StreamId STREAM = new StreamId("kafka-compacted-plan-stream");

    @Test
    void committedReadStopsAfterAccumulatedBatchWhenRemainingBudgetCannotFitTheNextBatch() {
        ArrayList<ReadRequest> requests = new ArrayList<>();
        TestStreamStorage streams = new TestStreamStorage();
        byte[] first = batch(0, "first");
        streams.semanticReader((streamId, request) -> {
            requests.add(request);
            if (request.startOffset() == 0) {
                return CompletableFuture.completedFuture(
                        result(request, List.of(readBatch(new OffsetRange(0, 1), first, 0, 1)), 1));
            }
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.READ_LIMIT_TOO_SMALL,
                    false,
                    "the next committed batch does not fit the remaining Fetch budget"));
        });
        KafkaCompactedFetchReader reader =
                KafkaCompactedFetchReader.committedOnly(KafkaCompactedFetchPlannerTest.identity(), STREAM, streams);
        KafkaStorageReadRequest request = new KafkaStorageReadRequest(
                0, 2, 100, first.length + 1, first.length + 1, true, 0, 0, Duration.ofSeconds(5));

        KafkaCompactedFetchReader.Result read = reader.read(request, KafkaStableSnapshot.nonTransactional(0, 2, 7))
                .join();

        assertThat(requests).hasSize(2);
        assertThat(requests.get(1).startOffset()).isEqualTo(1);
        assertThat(requests.get(1).firstEntryPolicy()).isEqualTo(FirstEntryPolicy.LEGACY_STRICT_LIMIT);
        assertThat(read.semanticRead().result().batches())
                .extracting(ReadBatch::range)
                .containsExactly(new OffsetRange(0, 1));
        assertThat(read.semanticRead().result().nextOffset()).isEqualTo(1);
        assertThat(read.semanticRead().sourceCoverageEndOffset()).isEqualTo(1);
    }

    @Test
    void scenarioKfFet012() {
        VersionedKafkaPartitionBinding binding = KafkaCompactedFetchPlannerTest.binding(0, 14, 0, 10);
        ArrayList<ReadRequest> requests = new ArrayList<>();
        TestStreamStorage streams = new TestStreamStorage();
        byte[] compacted = batch(5, "latest");
        byte[] tailFirst = batch(10, "tail-10", "tail-11");
        byte[] tailSecond = batch(12, "tail-12", "tail-13");
        streams.semanticReader((streamId, request) -> {
            requests.add(request);
            if (request.view() == ReadView.TOPIC_COMPACTED) {
                return CompletableFuture.completedFuture(
                        result(request, List.of(readBatch(new OffsetRange(5, 6), compacted, 2, 1)), 10));
            }
            return CompletableFuture.completedFuture(result(
                    request,
                    List.of(
                            readBatch(new OffsetRange(10, 12), tailFirst, 0, 2),
                            readBatch(new OffsetRange(12, 14), tailSecond, 0, 3)),
                    14));
        });
        KafkaCompactedFetchReader reader = new KafkaCompactedFetchReader(
                KafkaCompactedFetchPlannerTest.identity(), STREAM, streams, bindingStore(binding), authority(binding));

        KafkaCompactedFetchReader.Result read = reader.read(
                        KafkaCompactedFetchPlannerTest.request(2, 14), KafkaStableSnapshot.nonTransactional(0, 14, 7))
                .join();
        KafkaFetchAssembly assembly = new KafkaFetchAssembler(new KafkaRecordBatchCodec())
                .assemble(read.semanticRead(), 1024 * 1024, read.firstEntryOverflow(), 0, 0, List.of());

        assertThat(requests)
                .extracting(ReadRequest::view)
                .containsExactly(ReadView.TOPIC_COMPACTED, ReadView.COMMITTED);
        assertThat(requests).extracting(ReadRequest::startOffset).containsExactly(2L, 10L);
        assertThat(read.semanticRead().view()).isEqualTo(ReadView.TOPIC_COMPACTED);
        assertThat(read.semanticRead().sourceCoverageEndOffset()).isEqualTo(14);
        assertThat(assembly.actualFirstBatchBaseOffset()).hasValue(5);
        assertThat(assembly.nextLogicalOffset()).isEqualTo(14);
        assertThat(assembly.encodedRecords()).isEqualTo(concat(compacted, tailFirst, tailSecond));
    }

    @Test
    void scenarioKfFet011() {
        VersionedKafkaPartitionBinding binding = KafkaCompactedFetchPlannerTest.binding(0, 12, 0, 10);
        ArrayList<ReadRequest> requests = new ArrayList<>();
        TestStreamStorage streams = new TestStreamStorage();
        byte[] tail = batch(10, "tail");
        streams.semanticReader((streamId, request) -> {
            requests.add(request);
            return request.view() == ReadView.TOPIC_COMPACTED
                    ? CompletableFuture.completedFuture(result(request, List.of(), 10))
                    : CompletableFuture.completedFuture(
                            result(request, List.of(readBatch(new OffsetRange(10, 11), tail, 0, 3)), 11));
        });
        KafkaCompactedFetchReader reader = new KafkaCompactedFetchReader(
                KafkaCompactedFetchPlannerTest.identity(), STREAM, streams, bindingStore(binding), authority(binding));

        KafkaCompactedFetchReader.Result read = reader.read(
                        KafkaCompactedFetchPlannerTest.request(4, 11), KafkaStableSnapshot.nonTransactional(0, 12, 7))
                .join();

        assertThat(requests)
                .extracting(ReadRequest::view)
                .containsExactly(ReadView.TOPIC_COMPACTED, ReadView.COMMITTED);
        assertThat(read.semanticRead().result().batches())
                .extracting(ReadBatch::range)
                .containsExactly(new OffsetRange(10, 11));
        assertThat(read.semanticRead().sourceCoverageEndOffset()).isEqualTo(11);
    }

    static SemanticReadResult result(ReadRequest request, List<ReadBatch> batches, long coverageEnd) {
        long next = batches.isEmpty()
                ? request.startOffset()
                : batches.get(batches.size() - 1).range().endOffset();
        return SemanticReadResult.forRequest(
                request, new ReadResult(STREAM, request.startOffset(), next, batches, false), coverageEnd);
    }

    static ReadBatch readBatch(OffsetRange range, byte[] payload, long generation, int ordinal) {
        Checksum checksum = new Checksum(ChecksumType.CRC32C, "00000000");
        EntryIndexRef index = new EntryIndexRef(
                EntryIndexLocation.OBJECT_FOOTER, Optional.empty(), Optional.empty(), Optional.empty(), 0, 1, checksum);
        ObjectSliceReadTarget target = new ObjectSliceReadTarget(
                1,
                new ObjectId("kafka-compacted-read-" + ordinal),
                new ObjectKey("f9/kafka-compacted-read-" + ordinal),
                generation == 0 ? ObjectType.MULTI_STREAM_WAL_OBJECT : ObjectType.STREAM_COMPACTED_OBJECT,
                generation == 0 ? "WAL_OBJECT_V1" : "NEREUS_TOPIC_COMPACTED_KAFKA_PARQUET_V2",
                "KAFKA_RECORD_BATCH_V1",
                "slice-" + ordinal,
                0,
                payload.length,
                checksum,
                index);
        return new ReadBatch(
                range,
                PayloadFormat.KAFKA_RECORD_BATCH,
                payload,
                List.of(),
                Optional.empty(),
                new ReadSourceRef(range, generation, 1, target, ReadTargetIdentities.sha256(target)));
    }

    static byte[] batch(long baseOffset, String... values) {
        SimpleRecord[] records = new SimpleRecord[values.length];
        for (int index = 0; index < values.length; index++) {
            records[index] = new SimpleRecord(1_000 + index, values[index].getBytes());
        }
        ByteBuffer buffer = MemoryRecords.withRecords(
                        baseOffset, Compression.of(CompressionType.NONE).build(), records)
                .buffer()
                .duplicate();
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    static byte[] concat(byte[]... values) {
        int size = 0;
        for (byte[] value : values) {
            size = Math.addExact(size, value.length);
        }
        ByteBuffer result = ByteBuffer.allocate(size);
        for (byte[] value : values) {
            result.put(value);
        }
        return result.array();
    }

    static KafkaPartitionMetadataStore bindingStore(VersionedKafkaPartitionBinding binding) {
        return (KafkaPartitionMetadataStore) java.lang.reflect.Proxy.newProxyInstance(
                KafkaPartitionMetadataStore.class.getClassLoader(),
                new Class<?>[] {KafkaPartitionMetadataStore.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "get" -> CompletableFuture.completedFuture(Optional.of(binding));
                    case "close" -> null;
                    case "toString" -> "compacted-binding-store";
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == arguments[0];
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    static KafkaActivatedGenerationAuthority authority(VersionedKafkaPartitionBinding binding) {
        var coverage = binding.value().compactionCoverage();
        return (streamId, actual) -> {
            assertThat(actual).isEqualTo(coverage);
            OffsetRange range = new OffsetRange(coverage.startOffset(), coverage.endOffset());
            GenerationReadConstraint constraint = new GenerationReadConstraint(
                    streamId,
                    ReadView.TOPIC_COMPACTED,
                    range,
                    List.of(new GenerationReadConstraint.Identity(
                            range,
                            2,
                            new PublicationId("a".repeat(26)),
                            "/activated/test-generation",
                            3,
                            new Checksum(ChecksumType.SHA256, "a".repeat(64)))));
            return CompletableFuture.completedFuture(constraint);
        };
    }
}
