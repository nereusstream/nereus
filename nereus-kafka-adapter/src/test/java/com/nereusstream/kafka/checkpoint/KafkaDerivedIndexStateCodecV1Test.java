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
import com.nereusstream.kafka.checkpoint.KafkaDerivedIndexState.LogicalByteSample;
import com.nereusstream.kafka.checkpoint.KafkaDerivedIndexState.SegmentLogicalByteIndex;
import com.nereusstream.kafka.checkpoint.KafkaDerivedIndexState.SegmentTimeIndex;
import com.nereusstream.kafka.checkpoint.KafkaDerivedIndexState.TimeIndexEntry;
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

class KafkaDerivedIndexStateCodecV1Test {
    private static final int PAYLOAD_HEADER_BYTES = Short.BYTES + Integer.BYTES;
    private final KafkaDerivedIndexStateCodecV1 codec = new KafkaDerivedIndexStateCodecV1();

    @Test
    void roundTripsCanonicalIndexesWithFrozenBytes() throws Exception {
        KafkaDerivedIndexState state = canonicalState();

        List<KafkaCheckpointSection> sections = codec.encodeSections(state, 5, 30);
        KafkaDerivedIndexState decoded = codec.decodeSections(sections, 5, 30);

        assertThat(decoded).isEqualTo(state);
        assertThat(sections)
                .extracting(KafkaCheckpointSection::sectionType)
                .containsExactly(
                        KafkaCheckpointSectionType.TIME_INDEX.wireId(),
                        KafkaCheckpointSectionType.LOGICAL_BYTE_POSITION_INDEX.wireId());
        assertThat(sections).allSatisfy(section -> assertThat(section.sectionFlags())
                .isEqualTo(KafkaCheckpointFormatV1.SECTION_REQUIRED_FLAG));
        assertThat(sha256(sections)).isEqualTo("67ea931320ef762459b6d825dccd30868a77f2fa556d96d1aa38e2f7c8da8df4");
    }

    @Test
    void acceptsEmptyGenesisAndCurrentEmptySegment() {
        KafkaDerivedIndexState genesis = new KafkaDerivedIndexState(0, 0, List.of(), List.of());
        assertThat(codec.decodeSections(codec.encodeSections(genesis, 0, 0), 0, 0))
                .isEqualTo(genesis);

        KafkaDerivedIndexState currentEmpty = new KafkaDerivedIndexState(
                10,
                20,
                List.of(new SegmentTimeIndex(20, List.of())),
                List.of(new SegmentLogicalByteIndex(20, 0, List.of())));
        assertThat(codec.decodeSections(codec.encodeSections(currentEmpty, 10, 20), 10, 20))
                .isEqualTo(currentEmpty);
    }

    @Test
    void rejectsCrossSectionAndLogicalByteInconsistencies() {
        assertThatThrownBy(() -> new KafkaDerivedIndexState(
                        5,
                        30,
                        List.of(new SegmentTimeIndex(10, List.of())),
                        List.of(new SegmentLogicalByteIndex(0, 100, List.of()))))
                .hasMessageContaining("reference");

        assertThatThrownBy(() -> new KafkaDerivedIndexState(
                        5,
                        30,
                        List.of(),
                        List.of(new SegmentLogicalByteIndex(
                                0, 10, List.of(new LogicalByteSample(5, 7), new LogicalByteSample(6, 7))))))
                .hasMessageContaining("strictly ordered");

        assertThatThrownBy(() -> new KafkaDerivedIndexState(
                        5,
                        30,
                        List.of(),
                        List.of(new SegmentLogicalByteIndex(0, 10, List.of(new LogicalByteSample(5, 10))))))
                .hasMessageContaining("bounded");

        assertThatThrownBy(() -> new KafkaDerivedIndexState(
                        5,
                        30,
                        List.of(),
                        List.of(
                                new SegmentLogicalByteIndex(0, 10, List.of()),
                                new SegmentLogicalByteIndex(4, 10, List.of()))))
                .hasMessageContaining("bounded");

        assertThatThrownBy(() -> new KafkaDerivedIndexState(
                        5, 30, List.of(), List.of(new SegmentLogicalByteIndex(30, 1, List.of()))))
                .hasMessageContaining("must be empty");
    }

    @Test
    void rejectsNonMonotonicOrUnboundedTimeEntries() {
        SegmentLogicalByteIndex logical = new SegmentLogicalByteIndex(0, 100, List.of());
        assertThatThrownBy(() -> new KafkaDerivedIndexState(
                        5,
                        30,
                        List.of(new SegmentTimeIndex(0, List.of(new TimeIndexEntry(20, 5), new TimeIndexEntry(19, 6)))),
                        List.of(logical)))
                .hasMessageContaining("monotonic");

        assertThatThrownBy(() -> new KafkaDerivedIndexState(
                        5, 30, List.of(new SegmentTimeIndex(0, List.of(new TimeIndexEntry(20, 30)))), List.of(logical)))
                .hasMessageContaining("bounded");

        KafkaDerivedIndexState state = canonicalState();
        assertThatThrownBy(() -> codec.encodeSections(state, 6, 30)).hasMessageContaining("checkpoint bounds");
    }

    @Test
    void failsClosedForHeadersCountsCrossSectionStateAndTrailingBytes() {
        List<KafkaCheckpointSection> exact = codec.encodeSections(canonicalState(), 5, 30);

        byte[] badVersion = exact.get(0).payload();
        badVersion[1] = 2;
        assertThatThrownBy(() -> codec.decodeSections(replace(exact, 0, timeSection(badVersion)), 5, 30))
                .hasMessageContaining("payload version");

        byte[] oversizedSegmentCount = exact.get(0).payload();
        ByteBuffer.wrap(oversizedSegmentCount).order(ByteOrder.BIG_ENDIAN).putInt(Short.BYTES, -1);
        assertThatThrownBy(() -> codec.decodeSections(replace(exact, 0, timeSection(oversizedSegmentCount)), 5, 30))
                .hasMessageContaining("segmentCount");

        byte[] oversizedEntryCount = exact.get(0).payload();
        ByteBuffer.wrap(oversizedEntryCount).order(ByteOrder.BIG_ENDIAN).putInt(PAYLOAD_HEADER_BYTES + Long.BYTES, -1);
        assertThatThrownBy(() -> codec.decodeSections(replace(exact, 0, timeSection(oversizedEntryCount)), 5, 30))
                .hasMessageContaining("entryCount");

        byte[] trailing = Arrays.copyOf(exact.get(1).payload(), exact.get(1).payload().length + 1);
        assertThatThrownBy(() -> codec.decodeSections(replace(exact, 1, logicalSection(trailing)), 5, 30))
                .hasMessageContaining("trailing");

        byte[] mismatchedBase = exact.get(1).payload();
        ByteBuffer.wrap(mismatchedBase).order(ByteOrder.BIG_ENDIAN).putLong(PAYLOAD_HEADER_BYTES, 1);
        assertThatThrownBy(() -> codec.decodeSections(replace(exact, 1, logicalSection(mismatchedBase)), 5, 30))
                .hasMessageContaining("malformed");

        assertThatThrownBy(() -> codec.decodeSections(List.of(exact.get(0), exact.get(0), exact.get(1)), 5, 30))
                .hasMessageContaining("duplicate");
        assertThatThrownBy(() -> codec.decodeSections(List.of(exact.get(0)), 5, 30))
                .hasMessageContaining("missing");

        KafkaCheckpointSection optional = new KafkaCheckpointSection(
                exact.get(0).sectionType(), 1, 0, exact.get(0).payload());
        assertThatThrownBy(() -> codec.decodeSections(replace(exact, 0, optional), 5, 30))
                .hasMessageContaining("section header");
    }

    @Test
    void deterministicPropertyRoundTripsTwoHundredBoundedStates() {
        Random random = new Random(0x4e4b433156L);
        for (int iteration = 0; iteration < 200; iteration++) {
            long logStart = random.nextInt(1_000);
            long stableEnd = logStart + 100 + random.nextInt(1_000);
            int segmentCount = random.nextInt(7);
            ArrayList<SegmentLogicalByteIndex> logicalIndexes = new ArrayList<>(segmentCount);
            ArrayList<SegmentTimeIndex> timeIndexes = new ArrayList<>();
            long baseOffset = segmentCount == 0 ? 0 : random.nextLong(logStart + 1);
            for (int index = 0; index < segmentCount; index++) {
                if (index > 0) {
                    long minimum = Math.max(logStart, baseOffset + 1);
                    long remainingSlots = segmentCount - index - 1L;
                    long maximum = stableEnd - remainingSlots;
                    baseOffset = minimum + random.nextLong(maximum - minimum + 1);
                }
                long firstOffset = Math.max(baseOffset, logStart);
                int availableOffsets = Math.toIntExact(Math.min(5, stableEnd - firstOffset));
                int sampleCount = availableOffsets == 0 ? 0 : random.nextInt(availableOffsets + 1);
                long logicalBytes = baseOffset == stableEnd ? 0 : sampleCount + 1L + random.nextInt(1_000);
                ArrayList<LogicalByteSample> samples = new ArrayList<>(sampleCount);
                for (int sample = 0; sample < sampleCount; sample++) {
                    samples.add(new LogicalByteSample(firstOffset + sample, sample));
                }
                logicalIndexes.add(new SegmentLogicalByteIndex(baseOffset, logicalBytes, samples));

                if (random.nextBoolean()) {
                    int entryCount = availableOffsets == 0 ? 0 : random.nextInt(availableOffsets + 1);
                    ArrayList<TimeIndexEntry> entries = new ArrayList<>(entryCount);
                    long timestamp = random.nextInt(10_000);
                    for (int entry = 0; entry < entryCount; entry++) {
                        timestamp += random.nextInt(3);
                        entries.add(new TimeIndexEntry(timestamp, firstOffset + entry));
                    }
                    timeIndexes.add(new SegmentTimeIndex(baseOffset, entries));
                }
            }
            KafkaDerivedIndexState state = new KafkaDerivedIndexState(logStart, stableEnd, timeIndexes, logicalIndexes);
            List<KafkaCheckpointSection> encoded = codec.encodeSections(state, logStart, stableEnd);

            assertThat(codec.decodeSections(encoded, logStart, stableEnd))
                    .as("iteration %s", iteration)
                    .isEqualTo(state);
            List<KafkaCheckpointSection> reencoded =
                    codec.encodeSections(codec.decodeSections(encoded, logStart, stableEnd), logStart, stableEnd);
            for (int section = 0; section < encoded.size(); section++) {
                assertThat(reencoded.get(section).payload())
                        .isEqualTo(encoded.get(section).payload());
            }
        }
    }

    private static KafkaDerivedIndexState canonicalState() {
        return new KafkaDerivedIndexState(
                5,
                30,
                List.of(
                        new SegmentTimeIndex(0, List.of(new TimeIndexEntry(1_000, 5), new TimeIndexEntry(2_000, 10))),
                        new SegmentTimeIndex(
                                20, List.of(new TimeIndexEntry(2_500, 20), new TimeIndexEntry(3_000, 25)))),
                List.of(
                        new SegmentLogicalByteIndex(
                                0, 100, List.of(new LogicalByteSample(5, 20), new LogicalByteSample(10, 40))),
                        new SegmentLogicalByteIndex(
                                20, 50, List.of(new LogicalByteSample(20, 0), new LogicalByteSample(25, 25)))));
    }

    private static KafkaCheckpointSection timeSection(byte[] payload) {
        return KafkaCheckpointSection.required(KafkaCheckpointSectionType.TIME_INDEX, payload);
    }

    private static KafkaCheckpointSection logicalSection(byte[] payload) {
        return KafkaCheckpointSection.required(KafkaCheckpointSectionType.LOGICAL_BYTE_POSITION_INDEX, payload);
    }

    private static List<KafkaCheckpointSection> replace(
            List<KafkaCheckpointSection> sections, int index, KafkaCheckpointSection replacement) {
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
