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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.ReadRequest;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.kafka.partition.KafkaStableSnapshot;
import com.nereusstream.kafka.testing.TestStreamStorage;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class KafkaCompactedNoResurrectionIntegrationTest {
    private static final StreamId STREAM = new StreamId("kafka-compacted-plan-stream");

    @Test
    void scenarioKfFet013() {
        VersionedKafkaPartitionBinding binding = KafkaCompactedFetchPlannerTest.binding(0, 12, 0, 10);
        TestStreamStorage streams = new TestStreamStorage();
        AtomicInteger primaryNtc2Attempts = new AtomicInteger();
        AtomicInteger fallbackNtc2Attempts = new AtomicInteger();
        byte[] fallback = KafkaCompactedFetchIntegrationTest.batch(7, "fallback-latest");
        streams.semanticReader((streamId, request) -> {
            assertThat(request.view()).isEqualTo(ReadView.TOPIC_COMPACTED);
            // F4 performs candidate fallback inside this same-view call. The adapter never receives
            // a failure that it can turn into a cross-view retry.
            primaryNtc2Attempts.incrementAndGet();
            fallbackNtc2Attempts.incrementAndGet();
            return CompletableFuture.completedFuture(KafkaCompactedFetchIntegrationTest.result(
                    request,
                    List.of(KafkaCompactedFetchIntegrationTest.readBatch(new OffsetRange(7, 8), fallback, 3, 7)),
                    10));
        });
        KafkaCompactedFetchReader reader = new KafkaCompactedFetchReader(
                KafkaCompactedFetchPlannerTest.identity(),
                STREAM,
                streams,
                KafkaCompactedFetchIntegrationTest.bindingStore(binding),
                KafkaCompactedFetchIntegrationTest.authority(binding));

        KafkaCompactedFetchReader.Result result = reader.read(
                        KafkaCompactedFetchPlannerTest.request(4, 10), KafkaStableSnapshot.nonTransactional(0, 12, 7))
                .join();

        assertThat(primaryNtc2Attempts).hasValue(1);
        assertThat(fallbackNtc2Attempts).hasValue(1);
        assertThat(result.semanticRead().view()).isEqualTo(ReadView.TOPIC_COMPACTED);
        assertThat(result.semanticRead().result().batches())
                .extracting(batch -> batch.range())
                .containsExactly(new OffsetRange(7, 8));
        assertThat(result.semanticRead().sourceCoverageEndOffset()).isEqualTo(10);
    }

    @Test
    void scenarioKfFet014() {
        VersionedKafkaPartitionBinding binding = KafkaCompactedFetchPlannerTest.binding(0, 12, 0, 10);
        ArrayList<ReadRequest> requests = new ArrayList<>();
        TestStreamStorage streams = new TestStreamStorage();
        streams.semanticReader((streamId, request) -> {
            requests.add(request);
            if (request.view() == ReadView.COMMITTED) {
                return CompletableFuture.failedFuture(new AssertionError("mandatory range must not reach COMMITTED"));
            }
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.OBJECT_NOT_FOUND, true, "all same-view NTC2 candidates are unavailable"));
        });
        KafkaCompactedFetchReader reader = new KafkaCompactedFetchReader(
                KafkaCompactedFetchPlannerTest.identity(),
                STREAM,
                streams,
                KafkaCompactedFetchIntegrationTest.bindingStore(binding),
                KafkaCompactedFetchIntegrationTest.authority(binding));

        assertThatThrownBy(() -> reader.read(
                                KafkaCompactedFetchPlannerTest.request(4, 12),
                                KafkaStableSnapshot.nonTransactional(0, 12, 7))
                        .join())
                .hasRootCauseInstanceOf(NereusException.class)
                .rootCause()
                .extracting(value -> ((NereusException) value).code())
                .isEqualTo(ErrorCode.OBJECT_NOT_FOUND);
        assertThat(requests).extracting(ReadRequest::view).containsExactly(ReadView.TOPIC_COMPACTED);
    }

    @Test
    void committedContainingBatchCannotCrossBackIntoMandatoryCoverage() {
        VersionedKafkaPartitionBinding binding = KafkaCompactedFetchPlannerTest.binding(0, 12, 0, 10);
        TestStreamStorage streams = new TestStreamStorage();
        byte[] crossing = KafkaCompactedFetchIntegrationTest.batch(9, "old", "tail");
        streams.semanticReader((streamId, request) -> request.view() == ReadView.TOPIC_COMPACTED
                ? CompletableFuture.completedFuture(KafkaCompactedFetchIntegrationTest.result(request, List.of(), 10))
                : CompletableFuture.completedFuture(KafkaCompactedFetchIntegrationTest.result(
                        request,
                        List.of(KafkaCompactedFetchIntegrationTest.readBatch(new OffsetRange(9, 11), crossing, 0, 9)),
                        11)));
        KafkaCompactedFetchReader reader = new KafkaCompactedFetchReader(
                KafkaCompactedFetchPlannerTest.identity(),
                STREAM,
                streams,
                KafkaCompactedFetchIntegrationTest.bindingStore(binding),
                KafkaCompactedFetchIntegrationTest.authority(binding));

        assertThatThrownBy(() -> reader.read(
                                KafkaCompactedFetchPlannerTest.request(4, 11),
                                KafkaStableSnapshot.nonTransactional(0, 12, 7))
                        .join())
                .hasRootCauseInstanceOf(NereusException.class)
                .rootCause()
                .extracting(value -> ((NereusException) value).code())
                .isEqualTo(ErrorCode.METADATA_INVARIANT_VIOLATION);
    }
}
