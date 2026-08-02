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
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.LogConfigHistoryEntry;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.RollReason;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.SegmentState;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.VirtualSegment;
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

class KafkaVirtualSegmentStateCodecV1Test {
    private static final int PAYLOAD_HEADER_BYTES = Short.BYTES + Integer.BYTES;
    private static final int SEGMENT_BYTES = Long.BYTES * 11 + 32 + Integer.BYTES * 2;
    private final KafkaVirtualSegmentStateCodecV1 codec = new KafkaVirtualSegmentStateCodecV1();

    @Test
    void roundTripsCanonicalSegmentsAndConfigHistoryWithFrozenBytes() throws Exception {
        KafkaVirtualSegmentState state = canonicalState();

        KafkaCheckpointSection section = codec.encodeSection(state, 5, 30);
        KafkaVirtualSegmentState decoded = codec.decodeSection(List.of(section), 5, 30);

        assertThat(decoded).isEqualTo(state);
        assertThat(section.sectionType()).isEqualTo(KafkaCheckpointSectionType.VIRTUAL_SEGMENT_DESCRIPTORS.wireId());
        assertThat(section.sectionFlags()).isEqualTo(KafkaCheckpointFormatV1.SECTION_REQUIRED_FLAG);
        assertThat(sha256(section)).isEqualTo("75df2b19d1359464a4fd1428c8fc2d1cf7dd4691b15cbf20664eb7e41abefa57");
    }

    @Test
    void acceptsEmptyGenesisAndCurrentEmptySegment() {
        KafkaVirtualSegmentState genesis = new KafkaVirtualSegmentState(0, 0, List.of(), List.of());
        assertThat(codec.decodeSection(List.of(codec.encodeSection(genesis, 0, 0)), 0, 0))
                .isEqualTo(genesis);

        LogConfigHistoryEntry config = config(0, 10, 1_024, 0.5);
        KafkaVirtualSegmentState currentEmpty = new KafkaVirtualSegmentState(
                10,
                10,
                List.of(new VirtualSegment(
                        10,
                        10,
                        0,
                        1_000,
                        0,
                        0,
                        -1,
                        -1,
                        0,
                        0,
                        0,
                        config.configDigest(),
                        RollReason.INITIAL,
                        SegmentState.ACTIVE)),
                List.of(config));
        assertThat(codec.decodeSection(List.of(codec.encodeSection(currentEmpty, 10, 10)), 10, 10))
                .isEqualTo(currentEmpty);
    }

    @Test
    void rejectsSegmentRangeLifecycleByteAndConfigMismatches() {
        KafkaVirtualSegmentState exact = canonicalState();
        VirtualSegment first = exact.segments().get(0);
        VirtualSegment second = exact.segments().get(1);

        assertThatThrownBy(() -> new KafkaVirtualSegmentState(
                        5,
                        30,
                        List.of(
                                first,
                                copy(
                                        second,
                                        21,
                                        30,
                                        second.firstCumulativeBytes(),
                                        second.lastCumulativeBytes(),
                                        second.configDigest(),
                                        second.state(),
                                        second.rollJitterMillis())),
                        exact.configHistory()))
                .hasMessageContaining("dense");

        assertThatThrownBy(() -> new KafkaVirtualSegmentState(
                        5,
                        30,
                        List.of(
                                copy(
                                        first,
                                        first.baseOffset(),
                                        first.endOffset(),
                                        first.firstCumulativeBytes(),
                                        first.lastCumulativeBytes(),
                                        first.configDigest(),
                                        SegmentState.ACTIVE,
                                        first.rollJitterMillis()),
                                second),
                        exact.configHistory()))
                .hasMessageContaining("final");

        assertThatThrownBy(() -> new KafkaVirtualSegmentState(
                        5,
                        30,
                        List.of(
                                first,
                                copy(
                                        second,
                                        second.baseOffset(),
                                        second.endOffset(),
                                        101,
                                        151,
                                        second.configDigest(),
                                        second.state(),
                                        second.rollJitterMillis())),
                        exact.configHistory()))
                .hasMessageContaining("dense");

        Checksum unknown = new Checksum(ChecksumType.SHA256, "00".repeat(32));
        assertThatThrownBy(() -> new KafkaVirtualSegmentState(
                        5,
                        30,
                        List.of(
                                first,
                                copy(
                                        second,
                                        second.baseOffset(),
                                        second.endOffset(),
                                        second.firstCumulativeBytes(),
                                        second.lastCumulativeBytes(),
                                        unknown,
                                        second.state(),
                                        second.rollJitterMillis())),
                        exact.configHistory()))
                .hasMessageContaining("unknown log config");

        assertThatThrownBy(() -> new KafkaVirtualSegmentState(
                        5,
                        30,
                        List.of(
                                first,
                                copy(
                                        second,
                                        second.baseOffset(),
                                        second.endOffset(),
                                        second.firstCumulativeBytes(),
                                        second.lastCumulativeBytes(),
                                        second.configDigest(),
                                        second.state(),
                                        201)),
                        exact.configHistory()))
                .hasMessageContaining("jitter");

        LogConfigHistoryEntry lateConfig = config(30, 21, 2_048, 0.6);
        assertThatThrownBy(() -> new KafkaVirtualSegmentState(
                        5,
                        30,
                        List.of(
                                first,
                                copy(
                                        second,
                                        second.baseOffset(),
                                        second.endOffset(),
                                        second.firstCumulativeBytes(),
                                        second.lastCumulativeBytes(),
                                        lateConfig.configDigest(),
                                        second.state(),
                                        second.rollJitterMillis())),
                        List.of(exact.configHistory().get(0), lateConfig)))
                .hasMessageContaining("not yet effective");

        assertThatThrownBy(() -> new KafkaVirtualSegmentState(
                        5,
                        30,
                        exact.segments(),
                        List.of(
                                exact.configHistory().get(1),
                                exact.configHistory().get(0))))
                .hasMessageContaining("history");
    }

    @Test
    void rejectsNonCanonicalConfigDigestAndInvalidEmptyClosedSegment() {
        LogConfigHistoryEntry exact = config(1, 0, 1_024, 0.5);
        assertThatThrownBy(() -> new LogConfigHistoryEntry(
                        exact.metadataOffset(),
                        exact.effectiveFromOffset(),
                        exact.segmentBytes(),
                        exact.segmentMs(),
                        exact.segmentJitterMillis(),
                        exact.segmentIndexBytes(),
                        exact.indexIntervalBytes(),
                        exact.retentionBytes(),
                        exact.retentionMs(),
                        exact.fileDeleteDelayMs(),
                        exact.deleteRetentionMs(),
                        exact.minCompactionLagMs(),
                        exact.maxCompactionLagMs(),
                        exact.minCleanableDirtyRatio(),
                        exact.cleanupPolicyFlags(),
                        new Checksum(ChecksumType.SHA256, "00".repeat(32))))
                .hasMessageContaining("digest");

        assertThatThrownBy(() -> new VirtualSegment(
                        10,
                        10,
                        0,
                        1_000,
                        1_001,
                        0,
                        -1,
                        -1,
                        0,
                        0,
                        0,
                        exact.configDigest(),
                        RollReason.SIZE,
                        SegmentState.CLOSED))
                .hasMessageContaining("cannot be empty");

        assertThatThrownBy(() -> codec.encodeSection(canonicalState(), 6, 30))
                .hasMessageContaining("checkpoint bounds");
    }

    @Test
    void failsClosedForHeadersCountsEnumsDigestsAndTrailingBytes() {
        KafkaCheckpointSection exact = codec.encodeSection(canonicalState(), 5, 30);

        byte[] badVersion = exact.payload();
        badVersion[1] = 2;
        assertThatThrownBy(() -> codec.decodeSection(List.of(required(badVersion)), 5, 30))
                .hasMessageContaining("payload version");

        byte[] oversizedSegments = exact.payload();
        ByteBuffer.wrap(oversizedSegments).order(ByteOrder.BIG_ENDIAN).putInt(Short.BYTES, -1);
        assertThatThrownBy(() -> codec.decodeSection(List.of(required(oversizedSegments)), 5, 30))
                .hasMessageContaining("segmentCount");

        byte[] unknownReason = exact.payload();
        ByteBuffer.wrap(unknownReason)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(PAYLOAD_HEADER_BYTES + Long.BYTES * 11 + 32, 99);
        assertThatThrownBy(() -> codec.decodeSection(List.of(required(unknownReason)), 5, 30))
                .hasMessageContaining("malformed");

        byte[] oversizedConfigs = exact.payload();
        ByteBuffer.wrap(oversizedConfigs)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(PAYLOAD_HEADER_BYTES + SEGMENT_BYTES * 2, -1);
        assertThatThrownBy(() -> codec.decodeSection(List.of(required(oversizedConfigs)), 5, 30))
                .hasMessageContaining("configCount");

        byte[] mismatchedConfigDigest = exact.payload();
        int firstConfigDigest =
                PAYLOAD_HEADER_BYTES + SEGMENT_BYTES * 2 + Integer.BYTES + Long.BYTES * 12 + Integer.BYTES * 3;
        mismatchedConfigDigest[firstConfigDigest] ^= 1;
        assertThatThrownBy(() -> codec.decodeSection(List.of(required(mismatchedConfigDigest)), 5, 30))
                .hasMessageContaining("malformed");

        byte[] trailing = Arrays.copyOf(exact.payload(), exact.payload().length + 1);
        assertThatThrownBy(() -> codec.decodeSection(List.of(required(trailing)), 5, 30))
                .hasMessageContaining("trailing");

        assertThatThrownBy(() -> codec.decodeSection(List.of(exact, exact), 5, 30))
                .hasMessageContaining("duplicate");
        assertThatThrownBy(() -> codec.decodeSection(List.of(), 5, 30)).hasMessageContaining("missing");
        assertThatThrownBy(() -> codec.decodeSection(
                        List.of(new KafkaCheckpointSection(exact.sectionType(), 1, 0, exact.payload())), 5, 30))
                .hasMessageContaining("section header");
    }

    @Test
    void deterministicPropertyRoundTripsTwoHundredBoundedStates() {
        Random random = new Random(0x4e4b433134L);
        for (int iteration = 0; iteration < 200; iteration++) {
            long logStart = random.nextInt(1_000);
            long stableEnd = logStart + 50 + random.nextInt(1_000);
            int segmentCount = 1 + random.nextInt(5);
            ArrayList<Long> bases = new ArrayList<>(segmentCount);
            long baseOffset = random.nextLong(logStart + 1);
            bases.add(baseOffset);
            for (int index = 1; index < segmentCount; index++) {
                long minimum = Math.max(logStart, baseOffset + 1);
                long remainingSlots = segmentCount - index - 1L;
                long maximum = stableEnd - remainingSlots;
                baseOffset = minimum + random.nextLong(maximum - minimum + 1);
                bases.add(baseOffset);
            }

            ArrayList<LogConfigHistoryEntry> history = new ArrayList<>(segmentCount);
            for (int index = 0; index < segmentCount; index++) {
                history.add(config(index, bases.get(index), 1_024 + index, 0.1 + index * 0.1));
            }

            ArrayList<VirtualSegment> segments = new ArrayList<>(segmentCount);
            long cumulativeBytes = 0;
            for (int index = 0; index < segmentCount; index++) {
                long base = bases.get(index);
                long end = index + 1 < segmentCount ? bases.get(index + 1) : stableEnd;
                boolean empty = base == end;
                long logicalBytes = empty ? 0 : 1 + random.nextInt(10_000);
                long nextCumulative = cumulativeBytes + logicalBytes;
                boolean last = index == segmentCount - 1;
                segments.add(new VirtualSegment(
                        base,
                        end,
                        index,
                        1_000 + index * 10L,
                        last ? 0 : 1_010 + index * 10L,
                        random.nextInt(101),
                        empty ? -1 : 2_000 + index,
                        empty ? -1 : base,
                        logicalBytes,
                        cumulativeBytes,
                        nextCumulative,
                        history.get(index).configDigest(),
                        index == 0 ? RollReason.INITIAL : RollReason.SIZE,
                        last ? SegmentState.ACTIVE : SegmentState.CLOSED));
                cumulativeBytes = nextCumulative;
            }
            KafkaVirtualSegmentState state = new KafkaVirtualSegmentState(logStart, stableEnd, segments, history);
            KafkaCheckpointSection encoded = codec.encodeSection(state, logStart, stableEnd);

            assertThat(codec.decodeSection(List.of(encoded), logStart, stableEnd))
                    .as("iteration %s", iteration)
                    .isEqualTo(state);
            assertThat(codec.encodeSection(
                                    codec.decodeSection(List.of(encoded), logStart, stableEnd), logStart, stableEnd)
                            .payload())
                    .isEqualTo(encoded.payload());
        }
    }

    private static KafkaVirtualSegmentState canonicalState() {
        LogConfigHistoryEntry first = config(10, 0, 1_024, 0.5);
        LogConfigHistoryEntry second = config(20, 20, 2_048, 0.6);
        return new KafkaVirtualSegmentState(
                5,
                30,
                List.of(
                        new VirtualSegment(
                                0,
                                20,
                                3,
                                1_000,
                                2_000,
                                50,
                                1_500,
                                10,
                                100,
                                0,
                                100,
                                first.configDigest(),
                                RollReason.INITIAL,
                                SegmentState.CLOSED),
                        new VirtualSegment(
                                20,
                                30,
                                4,
                                2_000,
                                0,
                                75,
                                2_500,
                                25,
                                50,
                                100,
                                150,
                                second.configDigest(),
                                RollReason.SIZE,
                                SegmentState.ACTIVE)),
                List.of(first, second));
    }

    private static LogConfigHistoryEntry config(
            long metadataOffset, long effectiveFromOffset, long segmentBytes, double minCleanableRatio) {
        return LogConfigHistoryEntry.create(
                metadataOffset,
                effectiveFromOffset,
                segmentBytes,
                60_000,
                200,
                1_048_576,
                4_096,
                -1,
                604_800_000,
                60_000,
                86_400_000,
                0,
                Long.MAX_VALUE,
                minCleanableRatio,
                LogConfigHistoryEntry.CLEANUP_DELETE_FLAG | LogConfigHistoryEntry.CLEANUP_COMPACT_FLAG);
    }

    private static VirtualSegment copy(
            VirtualSegment source,
            long baseOffset,
            long endOffset,
            long firstCumulativeBytes,
            long lastCumulativeBytes,
            Checksum configDigest,
            SegmentState state,
            long rollJitterMillis) {
        return new VirtualSegment(
                baseOffset,
                endOffset,
                source.rollSequence(),
                source.createdAtMillis(),
                state == SegmentState.ACTIVE ? 0 : source.closedAtMillis(),
                rollJitterMillis,
                source.largestTimestamp(),
                source.maxTimestampOffset(),
                source.logicalBytes(),
                firstCumulativeBytes,
                lastCumulativeBytes,
                configDigest,
                source.rollReason(),
                state);
    }

    private static KafkaCheckpointSection required(byte[] payload) {
        return KafkaCheckpointSection.required(KafkaCheckpointSectionType.VIRTUAL_SEGMENT_DESCRIPTORS, payload);
    }

    private static String sha256(KafkaCheckpointSection section) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        ByteBuffer header = ByteBuffer.allocate(Integer.BYTES * 3);
        header.putInt(section.sectionType());
        header.putInt(section.sectionVersion());
        header.putInt(section.sectionFlags());
        digest.update(header.array());
        digest.update(section.payload());
        return HexFormat.of().formatHex(digest.digest());
    }
}
