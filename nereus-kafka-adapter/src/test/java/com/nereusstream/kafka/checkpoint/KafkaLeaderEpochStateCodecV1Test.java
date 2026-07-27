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

import com.nereusstream.kafka.checkpoint.KafkaLeaderEpochState.LeaderEpochRange;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointFormatV1;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointSection;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointSectionType;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

class KafkaLeaderEpochStateCodecV1Test {
    private static final int PAYLOAD_HEADER_BYTES =
            Short.BYTES + Integer.BYTES;
    private static final int ENTRY_BYTES = Integer.BYTES + Long.BYTES;
    private final KafkaLeaderEpochStateCodecV1 codec =
            new KafkaLeaderEpochStateCodecV1();

    @Test
    void roundTripsCanonicalRangesWithFrozenBytes() throws Exception {
        ArrayList<LeaderEpochRange> source = new ArrayList<>(List.of(
                new LeaderEpochRange(3, 0),
                new LeaderEpochRange(5, 10),
                new LeaderEpochRange(7, 30)));
        KafkaLeaderEpochState state =
                new KafkaLeaderEpochState(4, 30, source);

        KafkaCheckpointSection section =
                codec.encodeSection(state, 4, 30);
        KafkaLeaderEpochState decoded =
                codec.decodeSection(List.of(section), 4, 30);
        source.clear();

        assertThat(decoded).isEqualTo(state);
        assertThat(decoded.ranges()).hasSize(3);
        assertThat(section.sectionType()).isEqualTo(
                KafkaCheckpointSectionType.LEADER_EPOCH_RANGES.wireId());
        assertThat(section.sectionFlags()).isEqualTo(
                KafkaCheckpointFormatV1.SECTION_REQUIRED_FLAG);
        assertThat(sha256(section.payload()))
                .isEqualTo("f9146c7453e584a6a61a1719daa4e4a3b4d82bdf9f7b114a6becd7b644e3a972");
    }

    @Test
    void acceptsEmptyCacheAndExactStableEndEpochStart() {
        KafkaLeaderEpochState empty =
                new KafkaLeaderEpochState(10, 10, List.of());
        assertThat(codec.decodeSection(
                List.of(codec.encodeSection(empty, 10, 10)), 10, 10))
                .isEqualTo(empty);

        KafkaLeaderEpochState currentEmptyEpoch =
                new KafkaLeaderEpochState(
                        10,
                        20,
                        List.of(
                                new LeaderEpochRange(4, 10),
                                new LeaderEpochRange(5, 20)));
        assertThat(codec.decodeSection(
                List.of(codec.encodeSection(
                        currentEmptyEpoch, 10, 20)),
                10,
                20)).isEqualTo(currentEmptyEpoch);
    }

    @Test
    void rejectsUnorderedUnboundedOrAmbiguousCarriedRanges() {
        assertThatThrownBy(() -> new KafkaLeaderEpochState(
                10,
                20,
                List.of(
                        new LeaderEpochRange(3, 10),
                        new LeaderEpochRange(3, 11))))
                .hasMessageContaining("strictly ordered");
        assertThatThrownBy(() -> new KafkaLeaderEpochState(
                10,
                20,
                List.of(
                        new LeaderEpochRange(2, 5),
                        new LeaderEpochRange(3, 9))))
                .hasMessageContaining("bounded");
        assertThatThrownBy(() -> new KafkaLeaderEpochState(
                10,
                20,
                List.of(new LeaderEpochRange(3, 21))))
                .hasMessageContaining("bounded");

        KafkaLeaderEpochState state =
                new KafkaLeaderEpochState(
                        10,
                        20,
                        List.of(new LeaderEpochRange(3, 10)));
        assertThatThrownBy(() -> codec.encodeSection(state, 11, 20))
                .hasMessageContaining("checkpoint bounds");
    }

    @Test
    void failsClosedForHeaderCountOrderAndTrailingBytes() {
        KafkaLeaderEpochState state =
                new KafkaLeaderEpochState(
                        0,
                        10,
                        List.of(
                                new LeaderEpochRange(1, 0),
                                new LeaderEpochRange(2, 5)));
        KafkaCheckpointSection exact =
                codec.encodeSection(state, 0, 10);

        byte[] badVersion = exact.payload();
        badVersion[1] = 2;
        assertThatThrownBy(() -> codec.decodeSection(
                List.of(required(badVersion)), 0, 10))
                .hasMessageContaining("payload version");

        byte[] oversizedCount = exact.payload();
        ByteBuffer.wrap(oversizedCount)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(Short.BYTES, -1);
        assertThatThrownBy(() -> codec.decodeSection(
                List.of(required(oversizedCount)), 0, 10))
                .hasMessageContaining("entry count");

        byte[] unordered = exact.payload();
        ByteBuffer.wrap(unordered)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(PAYLOAD_HEADER_BYTES + ENTRY_BYTES, 1);
        assertThatThrownBy(() -> codec.decodeSection(
                List.of(required(unordered)), 0, 10))
                .hasMessageContaining("malformed");

        byte[] trailing = Arrays.copyOf(
                exact.payload(), exact.payload().length + 1);
        assertThatThrownBy(() -> codec.decodeSection(
                List.of(required(trailing)), 0, 10))
                .hasMessageContaining("trailing");

        assertThatThrownBy(() -> codec.decodeSection(
                List.of(exact, exact), 0, 10))
                .hasMessageContaining("duplicate");
        assertThatThrownBy(() -> codec.decodeSection(List.of(), 0, 10))
                .hasMessageContaining("missing");
        assertThatThrownBy(() -> codec.decodeSection(
                List.of(new KafkaCheckpointSection(
                        KafkaCheckpointSectionType.LEADER_EPOCH_RANGES.wireId(),
                        1,
                        0,
                        exact.payload())),
                0,
                10)).hasMessageContaining("section header");
    }

    @Test
    void deterministicPropertyRoundTripsTwoHundredBoundedStates() {
        Random random = new Random(0x4e4b4331L);
        for (int iteration = 0; iteration < 200; iteration++) {
            long logStart = random.nextInt(1_000);
            long stableEnd = logStart + 100 + random.nextInt(1_000);
            int rangeCount = random.nextInt(11);
            ArrayList<LeaderEpochRange> ranges =
                    new ArrayList<>(rangeCount);
            int epoch = random.nextInt(10);
            long start = rangeCount == 0
                    ? 0
                    : random.nextLong(logStart + 1);
            for (int index = 0; index < rangeCount; index++) {
                if (index > 0) {
                    long minimum = Math.max(logStart, start + 1);
                    long remainingSlots = rangeCount - index - 1L;
                    long maximum = stableEnd - remainingSlots;
                    start = minimum + random.nextLong(
                            maximum - minimum + 1);
                    epoch += 1 + random.nextInt(5);
                }
                ranges.add(new LeaderEpochRange(epoch, start));
            }
            KafkaLeaderEpochState state =
                    new KafkaLeaderEpochState(
                            logStart, stableEnd, ranges);
            KafkaCheckpointSection encoded =
                    codec.encodeSection(state, logStart, stableEnd);

            assertThat(codec.decodeSection(
                    List.of(encoded), logStart, stableEnd))
                    .as("iteration %s", iteration)
                    .isEqualTo(state);
            assertThat(codec.encodeSection(
                    codec.decodeSection(
                            List.of(encoded), logStart, stableEnd),
                    logStart,
                    stableEnd).payload())
                    .isEqualTo(encoded.payload());
        }
    }

    private static KafkaCheckpointSection required(byte[] payload) {
        return KafkaCheckpointSection.required(
                KafkaCheckpointSectionType.LEADER_EPOCH_RANGES,
                payload);
    }

    private static String sha256(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(bytes));
    }
}
