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
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PublicationId;
import com.nereusstream.api.ReadRequest;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.read.GenerationReadConstraint;
import com.nereusstream.kafka.partition.KafkaStableSnapshot;
import com.nereusstream.kafka.testing.TestStreamStorage;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class KafkaInternalTopicNoResurrectionTest {
    private static final StreamId STREAM = new StreamId("kafka-compacted-plan-stream");

    @Test
    void scenarioKfTxn016() {
        VersionedKafkaPartitionBinding binding = KafkaCompactedFetchPlannerTest.binding(0, 12, 0, 10);
        ArrayList<ReadRequest> requests = new ArrayList<>();
        TestStreamStorage streams = new TestStreamStorage();
        streams.semanticReader((streamId, request) -> {
            requests.add(request);
            if (request.view() == ReadView.COMMITTED) {
                return CompletableFuture.failedFuture(
                        new AssertionError("internal-topic readiness must not reach COMMITTED"));
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

        assertThatThrownBy(() -> reader.probeMandatoryCompactedRead(
                                KafkaStableSnapshot.nonTransactional(0, 12, 7), Duration.ofSeconds(5))
                        .join())
                .hasRootCauseInstanceOf(NereusException.class)
                .rootCause()
                .extracting(value -> ((NereusException) value).code())
                .isEqualTo(ErrorCode.OBJECT_NOT_FOUND);
        assertThat(requests).extracting(ReadRequest::view).containsExactly(ReadView.TOPIC_COMPACTED);
        assertThat(requests.get(0).startOffset()).isZero();
        assertThat(requests.get(0).options().maxRecords()).isOne();
    }

    @Test
    void healthySparseMandatoryPrefixCompletesTheBoundedProbe() {
        VersionedKafkaPartitionBinding binding = KafkaCompactedFetchPlannerTest.binding(0, 12, 0, 10);
        ArrayList<ReadRequest> requests = new ArrayList<>();
        TestStreamStorage streams = new TestStreamStorage();
        streams.semanticReader((streamId, request) -> {
            requests.add(request);
            return CompletableFuture.completedFuture(KafkaCompactedFetchIntegrationTest.result(request, List.of(), 10));
        });
        KafkaCompactedFetchReader reader = reader(binding, streams);

        reader.probeMandatoryCompactedRead(KafkaStableSnapshot.nonTransactional(0, 12, 7), Duration.ofSeconds(5))
                .join();

        assertThat(requests).extracting(ReadRequest::view).containsExactly(ReadView.TOPIC_COMPACTED);
        assertThat(requests.get(0).startOffset()).isZero();
    }

    @Test
    void unavailableLaterActivatedGenerationAlsoFailsTheProbe() {
        VersionedKafkaPartitionBinding binding = KafkaCompactedFetchPlannerTest.binding(0, 12, 0, 10);
        ArrayList<ReadRequest> requests = new ArrayList<>();
        TestStreamStorage streams = new TestStreamStorage();
        streams.semanticReader((streamId, request) -> {
            requests.add(request);
            if (request.startOffset() == 0) {
                return CompletableFuture.completedFuture(
                        KafkaCompactedFetchIntegrationTest.result(request, List.of(), 5));
            }
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.OBJECT_NOT_FOUND, true, "later activated NTC2 generation is unavailable"));
        });
        KafkaActivatedGenerationAuthority twoGenerations =
                (streamId, coverage) -> CompletableFuture.completedFuture(new GenerationReadConstraint(
                        streamId,
                        ReadView.TOPIC_COMPACTED,
                        new OffsetRange(0, 10),
                        List.of(generation(0, 5, 2), generation(5, 10, 3))));
        KafkaCompactedFetchReader reader = new KafkaCompactedFetchReader(
                KafkaCompactedFetchPlannerTest.identity(),
                STREAM,
                streams,
                KafkaCompactedFetchIntegrationTest.bindingStore(binding),
                twoGenerations);

        assertThatThrownBy(() -> reader.probeMandatoryCompactedRead(
                                KafkaStableSnapshot.nonTransactional(0, 12, 7), Duration.ofSeconds(5))
                        .join())
                .hasRootCauseInstanceOf(NereusException.class)
                .rootCause()
                .extracting(value -> ((NereusException) value).code())
                .isEqualTo(ErrorCode.OBJECT_NOT_FOUND);
        assertThat(requests).extracting(ReadRequest::startOffset).containsExactlyInAnyOrder(0L, 5L);
        assertThat(requests).extracting(ReadRequest::view).containsOnly(ReadView.TOPIC_COMPACTED);
    }

    @Test
    void fullyTrimmedMandatoryPrefixRequiresNoSourceRead() {
        VersionedKafkaPartitionBinding binding = KafkaCompactedFetchPlannerTest.binding(0, 12, 0, 10);
        TestStreamStorage streams = new TestStreamStorage();
        streams.semanticReader((streamId, request) ->
                CompletableFuture.failedFuture(new AssertionError("trimmed mandatory coverage must not be probed")));
        KafkaCompactedFetchReader reader = reader(binding, streams);

        reader.probeMandatoryCompactedRead(KafkaStableSnapshot.nonTransactional(10, 12, 7), Duration.ofSeconds(5))
                .join();
    }

    private static KafkaCompactedFetchReader reader(VersionedKafkaPartitionBinding binding, TestStreamStorage streams) {
        return new KafkaCompactedFetchReader(
                KafkaCompactedFetchPlannerTest.identity(),
                STREAM,
                streams,
                KafkaCompactedFetchIntegrationTest.bindingStore(binding),
                KafkaCompactedFetchIntegrationTest.authority(binding));
    }

    private static GenerationReadConstraint.Identity generation(long start, long end, long value) {
        return new GenerationReadConstraint.Identity(
                new OffsetRange(start, end),
                value,
                new PublicationId(Long.toString(value).repeat(26)),
                "/activated/test-generation-" + value,
                value,
                new Checksum(ChecksumType.SHA256, Long.toHexString(value).repeat(64)));
    }
}
