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

import com.nereusstream.kafka.checkpoint.KafkaProducerTransactionState.AbortedTransaction;
import com.nereusstream.kafka.checkpoint.KafkaProducerTransactionState.BatchMetadata;
import com.nereusstream.kafka.checkpoint.KafkaProducerTransactionState.OpenTransaction;
import com.nereusstream.kafka.checkpoint.KafkaProducerTransactionState.ProducerState;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointSection;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.OptionalLong;
import java.util.Random;
import org.junit.jupiter.api.Test;

class KafkaProducerStatePropertyTest {
    private static final int ITERATIONS = 200;
    private static final long SEED = 0x4e4b4331L;
    private static final long CHECKPOINT_OFFSET = 1_000_000L;

    private final KafkaProducerTransactionStateCodecV1 codec =
            new KafkaProducerTransactionStateCodecV1();

    @Test
    void canonicalStateRoundTripsAndReencodesByteExactly() {
        Random random = new Random(SEED);
        for (int iteration = 0; iteration < ITERATIONS; iteration++) {
            KafkaProducerTransactionState state = randomState(random, iteration);

            List<KafkaCheckpointSection> first =
                    codec.encodeSections(state, CHECKPOINT_OFFSET);
            KafkaProducerTransactionState decoded =
                    codec.decodeSections(first, CHECKPOINT_OFFSET);
            List<KafkaCheckpointSection> second =
                    codec.encodeSections(decoded, CHECKPOINT_OFFSET);

            assertThat(decoded)
                    .as("state iteration %s", iteration)
                    .isEqualTo(state);
            assertThat(second)
                    .as("section headers iteration %s", iteration)
                    .zipSatisfy(first, (actual, expected) -> {
                        assertThat(actual.sectionType()).isEqualTo(expected.sectionType());
                        assertThat(actual.sectionVersion()).isEqualTo(expected.sectionVersion());
                        assertThat(actual.sectionFlags()).isEqualTo(expected.sectionFlags());
                        assertThat(actual.payload()).isEqualTo(expected.payload());
                    });
        }
    }

    private static KafkaProducerTransactionState randomState(
            Random random, int iteration) {
        int producerCount = random.nextInt(9);
        ArrayList<ProducerState> producers = new ArrayList<>(producerCount);
        ArrayList<OpenTransaction> openTransactions = new ArrayList<>();
        long nextOffset = 100 + iteration * 1_000L;
        for (int producerIndex = 0; producerIndex < producerCount; producerIndex++) {
            long producerId = producerIndex * 3L + 1;
            int batchCount = 1 + random.nextInt(KafkaProducerTransactionState.RETAINED_BATCH_LIMIT);
            ArrayList<BatchMetadata> batches = new ArrayList<>(batchCount);
            int lastSequence = random.nextInt(Integer.MAX_VALUE);
            if (iteration == 0 && producerIndex == 0) {
                lastSequence = Integer.MAX_VALUE;
            }
            long lastTimestamp = -1;
            for (int batchIndex = 0; batchIndex < batchCount; batchIndex++) {
                int offsetDelta = random.nextInt(4);
                if (batchIndex > 0) {
                    lastSequence = addSequence(incrementSequence(lastSequence), offsetDelta);
                }
                long lastOffset = nextOffset + offsetDelta;
                lastTimestamp = iteration * 10_000L + producerIndex * 100L + batchIndex;
                batches.add(new BatchMetadata(
                        lastSequence, lastOffset, offsetDelta, lastTimestamp));
                nextOffset = lastOffset + 1 + random.nextInt(3);
            }
            OptionalLong currentTransactionFirstOffset =
                    random.nextBoolean()
                            ? OptionalLong.of(10_000L + iteration * 100L + producerIndex)
                            : OptionalLong.empty();
            producers.add(new ProducerState(
                    producerId,
                    (short) random.nextInt(Short.MAX_VALUE + 1),
                    random.nextInt(20) - 1,
                    lastTimestamp,
                    currentTransactionFirstOffset,
                    batches));
            currentTransactionFirstOffset.ifPresent(firstOffset ->
                    openTransactions.add(new OpenTransaction(
                            producerId, firstOffset, OptionalLong.empty())));
        }
        openTransactions.sort(Comparator.comparingLong(OpenTransaction::firstOffset)
                .thenComparingLong(OpenTransaction::producerId));

        int abortedCount = random.nextInt(6);
        ArrayList<AbortedTransaction> abortedTransactions =
                new ArrayList<>(abortedCount);
        for (int index = 0; index < abortedCount; index++) {
            long lastOffset = 700_000L + iteration * 10L + index;
            abortedTransactions.add(new AbortedTransaction(
                    KafkaProducerTransactionState.ABORTED_TRANSACTION_VERSION,
                    index + 100L,
                    lastOffset - random.nextInt(5),
                    lastOffset,
                    lastOffset + 1));
        }
        return new KafkaProducerTransactionState(
                CHECKPOINT_OFFSET, producers, openTransactions, abortedTransactions);
    }

    private static int incrementSequence(int sequence) {
        return sequence == Integer.MAX_VALUE ? 0 : sequence + 1;
    }

    private static int addSequence(int sequence, int increment) {
        long result = sequence + (long) increment;
        return result > Integer.MAX_VALUE
                ? Math.toIntExact(result - Integer.MAX_VALUE - 1L)
                : (int) result;
    }
}
