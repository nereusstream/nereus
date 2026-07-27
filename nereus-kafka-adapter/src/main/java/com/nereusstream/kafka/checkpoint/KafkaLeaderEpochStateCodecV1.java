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

import com.nereusstream.kafka.checkpoint.KafkaLeaderEpochState.LeaderEpochRange;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointFormatException;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointFormatV1;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointSection;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointSectionType;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Strict big-endian V1 codec for NKC1 section 3. */
public final class KafkaLeaderEpochStateCodecV1 {
    private static final int PAYLOAD_VERSION = 1;
    private static final int ENTRY_BYTES = Integer.BYTES + Long.BYTES;
    private static final int HEADER_BYTES = Short.BYTES + Integer.BYTES;

    public KafkaCheckpointSection encodeSection(
            KafkaLeaderEpochState state,
            long expectedLogStartOffset,
            long expectedStableEndOffset) {
        KafkaLeaderEpochState exact = Objects.requireNonNull(state, "state");
        exact.requireBounds(expectedLogStartOffset, expectedStableEndOffset);
        int payloadBytes;
        try {
            payloadBytes = Math.addExact(
                    HEADER_BYTES,
                    Math.multiplyExact(exact.ranges().size(), ENTRY_BYTES));
        } catch (ArithmeticException failure) {
            throw malformed("NKC1 leader-epoch section length overflows", failure);
        }
        if (payloadBytes > KafkaCheckpointFormatV1.MAX_SECTION_BYTES) {
            throw malformed("NKC1 leader-epoch section exceeds its hard limit");
        }
        ByteBuffer payload = ByteBuffer.allocate(payloadBytes)
                .order(ByteOrder.BIG_ENDIAN);
        payload.putShort((short) PAYLOAD_VERSION);
        payload.putInt(exact.ranges().size());
        for (LeaderEpochRange range : exact.ranges()) {
            payload.putInt(range.leaderEpoch());
            payload.putLong(range.startOffset());
        }
        return KafkaCheckpointSection.required(
                KafkaCheckpointSectionType.LEADER_EPOCH_RANGES,
                payload.array());
    }

    public KafkaLeaderEpochState decodeSection(
            List<KafkaCheckpointSection> sections,
            long expectedLogStartOffset,
            long expectedStableEndOffset) {
        if (expectedLogStartOffset < 0
                || expectedStableEndOffset < expectedLogStartOffset) {
            throw new IllegalArgumentException(
                    "invalid Kafka leader-epoch checkpoint bounds");
        }
        try {
            KafkaCheckpointSection section = locate(
                    List.copyOf(Objects.requireNonNull(sections, "sections")));
            ByteBuffer payload = ByteBuffer.wrap(section.payload())
                    .order(ByteOrder.BIG_ENDIAN);
            requireRemaining(payload, HEADER_BYTES, "header");
            if (Short.toUnsignedInt(payload.getShort()) != PAYLOAD_VERSION) {
                throw malformed(
                        "unsupported NKC1 leader-epoch payload version");
            }
            long unsignedCount = Integer.toUnsignedLong(payload.getInt());
            if (unsignedCount > Integer.MAX_VALUE
                    || unsignedCount > payload.remaining() / ENTRY_BYTES) {
                throw malformed("invalid NKC1 leader-epoch entry count");
            }
            int count = (int) unsignedCount;
            ArrayList<LeaderEpochRange> ranges =
                    new ArrayList<>(Math.min(count, 1 << 16));
            for (int index = 0; index < count; index++) {
                ranges.add(new LeaderEpochRange(
                        payload.getInt(), payload.getLong()));
            }
            if (payload.hasRemaining()) {
                throw malformed(
                        "NKC1 leader-epoch section contains trailing bytes");
            }
            return new KafkaLeaderEpochState(
                    expectedLogStartOffset,
                    expectedStableEndOffset,
                    ranges);
        } catch (KafkaCheckpointFormatException failure) {
            throw failure;
        } catch (IllegalArgumentException failure) {
            throw malformed("malformed NKC1 leader-epoch state", failure);
        }
    }

    private static KafkaCheckpointSection locate(
            List<KafkaCheckpointSection> sections) {
        KafkaCheckpointSection found = null;
        for (KafkaCheckpointSection section : sections) {
            Objects.requireNonNull(section, "section");
            if (section.sectionType()
                    != KafkaCheckpointSectionType.LEADER_EPOCH_RANGES.wireId()) {
                continue;
            }
            if (found != null) {
                throw malformed("duplicate NKC1 leader-epoch section");
            }
            if (!section.required()
                    || section.sectionVersion() != PAYLOAD_VERSION
                    || section.sectionFlags()
                    != KafkaCheckpointFormatV1.SECTION_REQUIRED_FLAG) {
                throw malformed(
                        "unsupported NKC1 leader-epoch section header");
            }
            found = section;
        }
        if (found == null) {
            throw malformed("missing NKC1 leader-epoch section");
        }
        return found;
    }

    private static void requireRemaining(
            ByteBuffer input,
            int required,
            String field) {
        if (input.remaining() < required) {
            throw malformed("truncated NKC1 leader-epoch " + field);
        }
    }

    private static KafkaCheckpointFormatException malformed(String message) {
        return new KafkaCheckpointFormatException(message);
    }

    private static KafkaCheckpointFormatException malformed(
            String message,
            Throwable cause) {
        return new KafkaCheckpointFormatException(message, cause);
    }
}
