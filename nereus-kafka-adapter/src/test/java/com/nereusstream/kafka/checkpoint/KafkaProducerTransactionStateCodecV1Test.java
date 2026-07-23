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

package com.nereusstream.kafka.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.kafka.checkpoint.KafkaProducerTransactionState.AbortedTransaction;
import com.nereusstream.kafka.checkpoint.KafkaProducerTransactionState.BatchMetadata;
import com.nereusstream.kafka.checkpoint.KafkaProducerTransactionState.OpenTransaction;
import com.nereusstream.kafka.checkpoint.KafkaProducerTransactionState.ProducerState;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointFormatV1;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointSection;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointSectionType;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class KafkaProducerTransactionStateCodecV1Test {
    private static final long CHECKPOINT_OFFSET = 30;
    private final KafkaProducerTransactionStateCodecV1 codec =
            new KafkaProducerTransactionStateCodecV1();

    @Test
    void roundTripsCanonicalSectionsWithFrozenBytesAndDefensiveCollections() throws Exception {
        ArrayList<BatchMetadata> sourceBatches = new ArrayList<>(List.of(
                new BatchMetadata(Integer.MAX_VALUE, 12, 1, 1_000),
                new BatchMetadata(1, 22, 1, 2_000)));
        ArrayList<ProducerState> sourceProducers = new ArrayList<>(List.of(
                new ProducerState(
                        7,
                        (short) 3,
                        5,
                        2_000,
                        OptionalLong.of(10),
                        sourceBatches),
                new ProducerState(
                        9,
                        (short) -1,
                        -1,
                        -1,
                        OptionalLong.empty(),
                        List.of())));
        KafkaProducerTransactionState state = new KafkaProducerTransactionState(
                CHECKPOINT_OFFSET,
                sourceProducers,
                List.of(new OpenTransaction(7, 10, OptionalLong.empty())),
                List.of(
                        new AbortedTransaction((short) 0, 11, 2, 4, 5),
                        new AbortedTransaction((short) 0, 12, 6, 8, 10)));

        List<KafkaCheckpointSection> sections =
                codec.encodeSections(state, CHECKPOINT_OFFSET);
        KafkaProducerTransactionState decoded =
                codec.decodeSections(sections, CHECKPOINT_OFFSET);
        sourceBatches.clear();
        sourceProducers.clear();

        assertThat(decoded).isEqualTo(state);
        assertThat(state.producers()).hasSize(2);
        assertThat(state.producers().get(0).batches()).hasSize(2);
        assertThat(sections).extracting(KafkaCheckpointSection::sectionType)
                .containsExactly(
                        KafkaCheckpointSectionType.PRODUCER_STATE.wireId(),
                        KafkaCheckpointSectionType.ABORTED_TRANSACTION_INDEX.wireId(),
                        KafkaCheckpointSectionType.OPEN_TRANSACTION_SUMMARY.wireId());
        assertThat(sha256(sections))
                .isEqualTo("8c767c45f573ac9c8c15b972905a8a6e904dcecb15e7509447d85c84ecf11d73");
    }

    @Test
    void acceptsEmptyGenesisStateAndRequiresExactCheckpointOffset() {
        KafkaProducerTransactionState empty =
                new KafkaProducerTransactionState(0, List.of(), List.of(), List.of());

        List<KafkaCheckpointSection> sections = codec.encodeSections(empty, 0);

        assertThat(codec.decodeSections(sections, 0)).isEqualTo(empty);
        assertThatThrownBy(() -> codec.decodeSections(sections, 1))
                .hasMessageContaining("malformed")
                .hasRootCauseMessage(
                        "Kafka producer map end offset must equal the NKC1 checkpoint offset");
    }

    @Test
    void rejectsUnsortedOrInconsistentProducerAndTransactionFacts() {
        ProducerState first = producer(2, 0, 0, OptionalLong.empty());
        ProducerState second = producer(1, 1, 1, OptionalLong.empty());
        assertThatThrownBy(() -> new KafkaProducerTransactionState(
                10, List.of(first, second), List.of(), List.of()))
                .hasMessageContaining("producer IDs");

        ProducerState gapped = new ProducerState(
                1,
                (short) 0,
                -1,
                2,
                OptionalLong.empty(),
                List.of(
                        new BatchMetadata(2, 2, 0, 1),
                        new BatchMetadata(4, 4, 0, 2)));
        assertThatThrownBy(() -> new KafkaProducerTransactionState(
                10, List.of(gapped), List.of(), List.of()))
                .hasMessageContaining("sequences");

        ProducerState transactional = producer(1, 1, 1, OptionalLong.of(1));
        assertThatThrownBy(() -> new KafkaProducerTransactionState(
                10,
                List.of(transactional),
                List.of(new OpenTransaction(1, 2, OptionalLong.empty())),
                List.of()))
                .hasMessageContaining("does not match");

        assertThatThrownBy(() -> new KafkaProducerTransactionState(
                10,
                List.of(transactional),
                List.of(new OpenTransaction(1, 1, OptionalLong.of(5))),
                List.of()))
                .hasMessageContaining("normal NKC1");
    }

    @Test
    void rejectsAbortedTransactionOrderingAndCheckpointOverflow() {
        AbortedTransaction later = new AbortedTransaction((short) 0, 1, 3, 7, 8);
        AbortedTransaction earlier = new AbortedTransaction((short) 0, 2, 1, 4, 2);

        assertThatThrownBy(() -> new KafkaProducerTransactionState(
                10, List.of(), List.of(), List.of(later, earlier)))
                .hasMessageContaining("strictly increasing");
        assertThatThrownBy(() -> new KafkaProducerTransactionState(
                7, List.of(), List.of(), List.of(later)))
                .hasMessageContaining("extends beyond");

        KafkaProducerTransactionState overlapping = new KafkaProducerTransactionState(
                20,
                List.of(),
                List.of(),
                List.of(
                        new AbortedTransaction((short) 0, 2, 1, 10, 2),
                        new AbortedTransaction((short) 0, 3, 5, 15, 13)));
        assertThat(codec.decodeSections(codec.encodeSections(overlapping, 20), 20))
                .isEqualTo(overlapping);

        assertThatThrownBy(() -> new KafkaProducerTransactionState(
                20,
                List.of(),
                List.of(),
                List.of(
                        new AbortedTransaction((short) 0, 2, 1, 10, 2),
                        new AbortedTransaction((short) 0, 3, 5, 10, 13))))
                .hasMessageContaining("strictly increasing");
    }

    @Test
    void failsClosedForOuterHeaderPayloadVersionCountsFlagsAndTrailingBytes() {
        KafkaProducerTransactionState state = new KafkaProducerTransactionState(
                10,
                List.of(producer(1, 1, 1, OptionalLong.empty())),
                List.of(),
                List.of());
        List<KafkaCheckpointSection> sections = codec.encodeSections(state, 10);
        KafkaCheckpointSection producer = sections.get(0);

        byte[] badPayloadVersion = producer.payload();
        badPayloadVersion[1] = 2;
        assertThatThrownBy(() -> codec.decodeSections(
                replace(sections, 0, KafkaCheckpointSection.required(
                        KafkaCheckpointSectionType.PRODUCER_STATE, badPayloadVersion)),
                10)).hasMessageContaining("payload version");

        byte[] oversizedCount = producer.payload();
        ByteBuffer.wrap(oversizedCount).putInt(Short.BYTES + Long.BYTES, -1);
        assertThatThrownBy(() -> codec.decodeSections(
                replace(sections, 0, KafkaCheckpointSection.required(
                        KafkaCheckpointSectionType.PRODUCER_STATE, oversizedCount)),
                10)).hasMessageContaining("producerCount");

        byte[] trailing = Arrays.copyOf(producer.payload(), producer.payload().length + 1);
        assertThatThrownBy(() -> codec.decodeSections(
                replace(sections, 0, KafkaCheckpointSection.required(
                        KafkaCheckpointSectionType.PRODUCER_STATE, trailing)),
                10)).hasMessageContaining("trailing");

        KafkaCheckpointSection optional = new KafkaCheckpointSection(
                producer.sectionType(), 1, 0, producer.payload());
        assertThatThrownBy(() -> codec.decodeSections(replace(sections, 0, optional), 10))
                .hasMessageContaining("section header");

        assertThatThrownBy(() -> codec.decodeSections(sections.subList(0, 2), 10))
                .hasMessageContaining("missing")
                .hasMessageContaining("OPEN_TRANSACTION_SUMMARY");
    }

    private static ProducerState producer(
            long producerId,
            int lastSequence,
            long lastOffset,
            OptionalLong transaction) {
        long timestamp = 100 + lastOffset;
        return new ProducerState(
                producerId,
                (short) 1,
                -1,
                timestamp,
                transaction,
                List.of(new BatchMetadata(lastSequence, lastOffset, 0, timestamp)));
    }

    private static List<KafkaCheckpointSection> replace(
            List<KafkaCheckpointSection> sections,
            int index,
            KafkaCheckpointSection replacement) {
        ArrayList<KafkaCheckpointSection> copy = new ArrayList<>(sections);
        copy.set(index, replacement);
        return copy;
    }

    private static String sha256(List<KafkaCheckpointSection> sections) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        for (KafkaCheckpointSection section : sections) {
            ByteBuffer header = ByteBuffer.allocate(Integer.BYTES * 3);
            header.putInt(section.sectionType());
            header.putInt(section.sectionVersion());
            header.putInt(section.sectionFlags());
            digest.update(header.array());
            digest.update(section.payload());
        }
        return HexFormat.of().formatHex(digest.digest());
    }
}
