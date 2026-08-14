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

package com.nereusstream.kafka.bookkeeper.nbke2;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.KafkaTopicId;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.KafkaTopicName;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;

/** Reproducibly emits reviewable NBKE2 minimum/representative vectors and maximum-vector digests. */
public final class Nbke2GoldenVectorEmitter {
    private Nbke2GoldenVectorEmitter() {}

    public static void main(String[] arguments) {
        emit("minimum", minimumFrames(), true);
        emit(
                "representative",
                List.of(
                        Nbke2TestFrames.runHeader(),
                        Nbke2TestFrames.data(),
                        Nbke2TestFrames.rangeIndexBlock(),
                        Nbke2TestFrames.protocolCheckpoint(),
                        Nbke2TestFrames.runFooter()),
                true);
        emit("maximum", maximumFrames(), false);
    }

    private static void emit(String vectorClass, List<Nbke2FrameV1> frames, boolean includeHex) {
        long[] entryIds = entryIds(vectorClass);
        for (int index = 0; index < frames.size(); index++) {
            Nbke2FrameV1 frame = frames.get(index);
            long entryId = entryIds[index];
            byte[] bytes = Nbke2CodecV1.encode(Nbke2TestFrames.LEDGER_ID, entryId, frame);
            String sha = Sha256Digest.hash(CanonicalBytes.copyOf(bytes)).toHex();
            StringBuilder line = new StringBuilder()
                    .append(vectorClass)
                    .append('/')
                    .append(frame.frameType().name().toLowerCase())
                    .append(".hex entry=")
                    .append(entryId)
                    .append(" length=")
                    .append(bytes.length)
                    .append(" sha256=")
                    .append(sha);
            if (includeHex) {
                line.append(" hex=").append(HexFormat.of().formatHex(bytes));
            }
            System.out.println(line);
        }
    }

    static List<Nbke2FrameV1> minimumFrames() {
        Nbke2RunBindingV1 binding = binding("a");
        CanonicalBytes raw = CanonicalBytes.copyOf(new byte[] {1});
        return List.of(
                new Nbke2RunHeaderV1(binding, 0, 1, digest(7)),
                new Nbke2DataV1(
                        binding,
                        0,
                        0,
                        0,
                        1,
                        new Id128(0, 8),
                        new Id128(0, 9),
                        Optional.of(new Nbke2AppendGroupDescriptorV1(0, 1, 1, 1, Sha256Digest.hash(raw))),
                        raw),
                new Nbke2RangeIndexBlockV1(
                        binding, 0, 1, 1, 1, 1, -1, 3, List.of(new Nbke2BatchLocatorV1(0, 1, 0, 0, 0, 1, 0))),
                new Nbke2ProtocolCheckpointV1(
                        binding, 0, 0, 0, 0, CanonicalBytes.empty(), CanonicalBytes.empty(), CanonicalBytes.empty()),
                new Nbke2RunFooterV1(binding, 0, 1, -1, -1, 1, List.of()));
    }

    static List<Nbke2FrameV1> maximumFrames() {
        Nbke2RunBindingV1 binding = binding("x".repeat(Nbke2ConstantsV1.FORMAT_MAX_TOPIC_NAME_BYTES));
        byte[] payload = new byte[Nbke2ConstantsV1.FORMAT_MAX_DATA_PAYLOAD_BYTES];
        payload[0] = 1;
        payload[payload.length - 1] = 2;
        CanonicalBytes raw = CanonicalBytes.copyOf(payload);

        List<Nbke2BatchLocatorV1> locators = new ArrayList<>(Nbke2ConstantsV1.FORMAT_MAX_LOCATOR_COUNT);
        for (int index = 0; index < Nbke2ConstantsV1.FORMAT_MAX_LOCATOR_COUNT; index++) {
            locators.add(new Nbke2BatchLocatorV1(index, 1, index, index, 0, 1, 0xffff_ffffL));
        }

        byte[] checkpointSection = new byte[Nbke2ConstantsV1.FORMAT_MAX_CHECKPOINT_SECTION_BYTES];
        checkpointSection[0] = 1;
        checkpointSection[checkpointSection.length - 1] = 2;
        CanonicalBytes section = CanonicalBytes.copyOf(checkpointSection);

        List<Nbke2IndexDirectoryEntryV1> directory = new ArrayList<>(Nbke2ConstantsV1.FORMAT_MAX_INDEX_DIRECTORY_COUNT);
        for (int index = 0; index < Nbke2ConstantsV1.FORMAT_MAX_INDEX_DIRECTORY_COUNT; index++) {
            directory.add(new Nbke2IndexDirectoryEntryV1(index, index, index + 1L));
        }

        return List.of(
                new Nbke2RunHeaderV1(binding, Long.MAX_VALUE, Long.MAX_VALUE, digest(7)),
                new Nbke2DataV1(
                        binding,
                        0,
                        Integer.MAX_VALUE,
                        0,
                        1,
                        new Id128(0, 8),
                        new Id128(0, 9),
                        Optional.of(new Nbke2AppendGroupDescriptorV1(
                                0, (long) Integer.MAX_VALUE + 1L, 1, 1, Sha256Digest.hash(raw))),
                        raw),
                new Nbke2RangeIndexBlockV1(
                        binding,
                        0,
                        0,
                        Nbke2ConstantsV1.FORMAT_MAX_LOCATOR_COUNT,
                        0,
                        Nbke2ConstantsV1.FORMAT_MAX_LOCATOR_COUNT - 1L,
                        -1,
                        Nbke2ConstantsV1.FORMAT_MAX_LOCATOR_COUNT + 1L,
                        locators),
                new Nbke2ProtocolCheckpointV1(
                        binding,
                        Long.MAX_VALUE,
                        Long.MAX_VALUE,
                        Long.MAX_VALUE,
                        Long.MAX_VALUE,
                        section,
                        section,
                        section),
                new Nbke2RunFooterV1(
                        binding,
                        Nbke2ConstantsV1.FORMAT_MAX_INDEX_DIRECTORY_COUNT,
                        Nbke2ConstantsV1.FORMAT_MAX_INDEX_DIRECTORY_COUNT + 1L,
                        Nbke2ConstantsV1.FORMAT_MAX_INDEX_DIRECTORY_COUNT - 1L,
                        -1,
                        Long.MAX_VALUE,
                        directory));
    }

    private static long[] entryIds(String vectorClass) {
        return switch (vectorClass) {
            case "minimum" -> new long[] {0, 1, 2, 3, 0};
            case "representative" -> new long[] {0, 1, 3, 4, 5};
            case "maximum" ->
                new long[] {
                    0,
                    1,
                    Nbke2ConstantsV1.FORMAT_MAX_LOCATOR_COUNT,
                    2,
                    Nbke2ConstantsV1.FORMAT_MAX_INDEX_DIRECTORY_COUNT
                };
            default -> throw new IllegalArgumentException("unknown vector class: " + vectorClass);
        };
    }

    private static Nbke2RunBindingV1 binding(String topicName) {
        return new Nbke2RunBindingV1(
                new TopicBindingId(digest(1)),
                new KafkaTopicIncarnationIdentity(new KafkaTopicId(new Id128(0, 2)), new KafkaTopicName(topicName)),
                0,
                new StorageEpochId(digest(3)),
                1,
                0,
                new CellProviderScopeId(digest(4)),
                new StorageRunId(new Id128(0, 6)));
    }

    private static Sha256Digest digest(int lastByte) {
        byte[] bytes = new byte[Sha256Digest.LENGTH];
        bytes[bytes.length - 1] = (byte) lastByte;
        return Sha256Digest.copyOf(bytes);
    }
}
