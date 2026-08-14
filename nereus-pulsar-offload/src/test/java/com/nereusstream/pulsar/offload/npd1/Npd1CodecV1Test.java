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

package com.nereusstream.pulsar.offload.npd1;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.pulsar.offload.PulsarOffloadLimitCandidateV1;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.CompressionFamily;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.CompressionPolicy;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.DataObject;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.EntryPayload;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.Npd1RejectedException;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.SparseBlock;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.StreamingEncoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class Npd1CodecV1Test {
    private static final UUID ATTEMPT = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
    private static final SecretKey KEY = new SecretKeySpec(sequence(32, 1), "AES");
    private static final PulsarOffloadLimitCandidateV1 LIMITS =
            PulsarOffloadLimitCandidateV1.adr0056EvidenceCandidate();

    @TempDir
    Path temporaryDirectory;

    @Test
    void roundTripsGapFreeEntriesThroughTargetedNoneBlocks() throws Exception {
        List<EntryPayload> entries = entries(12, 300_000, false);
        DataObject object = encode(entries, CompressionFamily.NONE);

        assertThat(object.blocks()).hasSize(4);
        assertThat(Npd1CodecV1.parseDataHeader(
                        Arrays.copyOf(Files.readAllBytes(object.path()), Npd1CodecV1.DATA_HEADER_BYTES)))
                .isNotNull();
        assertThat(decodeAll(object))
                .usingRecursiveFieldByFieldElementComparator()
                .containsExactlyElementsOf(entries);
    }

    @Test
    void roundTripsCompressibleEntriesWithIndependentZstdBlocks() throws Exception {
        List<EntryPayload> entries = entries(20, 100_000, true);
        DataObject object = encode(entries, CompressionFamily.ZSTD);

        assertThat(object.bytes()).isLessThan(2_000_000);
        assertThat(decodeAll(object))
                .usingRecursiveFieldByFieldElementComparator()
                .containsExactlyElementsOf(entries);
    }

    @Test
    void eligibleZstdPersistsTheActualFamilyIndependentlyPerBlock() throws Exception {
        int payloadBytes = 600_000;
        byte[] incompressible = new byte[payloadBytes];
        new Random(7).nextBytes(incompressible);
        Path target = temporaryDirectory.resolve("adaptive.npd1");
        DataObject object;
        try (StreamingEncoder encoder = Npd1CodecV1.openStreaming(
                target, PulsarOffloadLimitCandidateV1.MIB, CompressionPolicy.ZSTD_IF_SMALLER, KEY, ATTEMPT, LIMITS)) {
            encoder.append(new EntryPayload(0, new byte[payloadBytes]));
            encoder.append(new EntryPayload(1, incompressible));
            object = encoder.finish();
        }

        assertThat(object.blocks())
                .extracting(SparseBlock::compressionFamily)
                .containsExactly(CompressionFamily.ZSTD, CompressionFamily.NONE);
        List<EntryPayload> decoded = decodeAll(object);
        assertThat(decoded).extracting(EntryPayload::entryId).containsExactly(0L, 1L);
        assertThat(decoded.get(0).payload()).containsOnly(0);
        assertThat(decoded.get(1).payload()).containsExactly(incompressible);
    }

    @Test
    void givesAnOversizeEntryOneDedicatedBoundedBlock() {
        List<EntryPayload> entries = List.of(
                new EntryPayload(0, sequence(100, 1)),
                new EntryPayload(1, sequence(PulsarOffloadLimitCandidateV1.MIB + 7, 2)),
                new EntryPayload(2, sequence(100, 3)));

        DataObject object = encode(entries, CompressionFamily.NONE);

        assertThat(object.blocks()).hasSize(3);
        assertThat(object.blocks().get(1).entryCount()).isEqualTo(1);
        assertThat(object.blocks().get(1).firstEntryId()).isEqualTo(1);
    }

    @Test
    void writesTheExactNpd1HeaderAfterStreamingBlocks() throws Exception {
        DataObject object = encode(entries(2, 100, false), CompressionFamily.NONE);
        byte[] all = Files.readAllBytes(object.path());

        Npd1CodecV1.DataHeader header = Npd1CodecV1.parseDataHeader(Arrays.copyOf(all, Npd1CodecV1.DATA_HEADER_BYTES));
        assertThat(header.blockCount()).isEqualTo(object.blocks().size());
        assertThat(header.totalBytes()).isEqualTo(object.bytes());
    }

    @Test
    void rejectsGapDuplicateAndNonZeroFirstEntryBeforeOutput() {
        for (List<EntryPayload> entries : List.of(
                List.of(new EntryPayload(1, new byte[] {1})),
                List.of(new EntryPayload(0, new byte[] {1}), new EntryPayload(2, new byte[] {2})),
                List.of(new EntryPayload(0, new byte[] {1}), new EntryPayload(0, new byte[] {2})))) {
            assertThatThrownBy(() -> encode(entries, CompressionFamily.NONE)).isInstanceOf(Npd1RejectedException.class);
        }
    }

    @Test
    void rejectsUnsupportedBlockTargetAndWrongAttemptKey() {
        assertThatThrownBy(() -> Npd1CodecV1.encode(
                        temporaryDirectory.resolve("bad-target.npd1"),
                        entries(1, 1, false),
                        2 * PulsarOffloadLimitCandidateV1.MIB,
                        CompressionFamily.NONE,
                        KEY,
                        ATTEMPT,
                        LIMITS))
                .isInstanceOf(Npd1RejectedException.class);
        assertThatThrownBy(() -> Npd1CodecV1.encode(
                        temporaryDirectory.resolve("bad-key.npd1"),
                        entries(1, 1, false),
                        PulsarOffloadLimitCandidateV1.MIB,
                        CompressionFamily.NONE,
                        new SecretKeySpec(new byte[16], "AES"),
                        ATTEMPT,
                        LIMITS))
                .isInstanceOf(Npd1RejectedException.class);
    }

    @Test
    void rejectsEntryAndDecodedBlockHardCapBeforeUpload() {
        PulsarOffloadLimitCandidateV1 tiny = new PulsarOffloadLimitCandidateV1(
                4L * PulsarOffloadLimitCandidateV1.MIB,
                4,
                PulsarOffloadLimitCandidateV1.MIB,
                16L * PulsarOffloadLimitCandidateV1.MIB,
                4,
                LIMITS.blockTargetBytes());
        assertThatThrownBy(() -> Npd1CodecV1.encode(
                        temporaryDirectory.resolve("entry-cap.npd1"),
                        List.of(new EntryPayload(0, new byte[PulsarOffloadLimitCandidateV1.MIB + 1])),
                        PulsarOffloadLimitCandidateV1.MIB,
                        CompressionFamily.NONE,
                        KEY,
                        ATTEMPT,
                        tiny))
                .isInstanceOf(Npd1RejectedException.class);
    }

    @Test
    void rejectsCiphertextDigestAndGcmCorruption() throws Exception {
        DataObject object = encode(entries(3, 100, false), CompressionFamily.NONE);
        SparseBlock block = object.blocks().get(0);
        byte[] encoded = blockBytes(object, block);
        encoded[encoded.length - 1] ^= 1;

        assertThatThrownBy(() -> Npd1CodecV1.decodeBlock(encoded, block, KEY, ATTEMPT, LIMITS))
                .isInstanceOf(Npd1RejectedException.class)
                .hasMessageContaining("SHA-256");

        SparseBlock rebound = new SparseBlock(
                block.blockOrdinal(),
                block.firstEntryId(),
                block.entryCount(),
                block.blockOffset(),
                block.encodedBlockBytes(),
                block.decodedBlockBytes(),
                block.compressionFamily(),
                block.encryptionFamily(),
                sha(encoded));
        assertThatThrownBy(() -> Npd1CodecV1.decodeBlock(encoded, rebound, KEY, ATTEMPT, LIMITS))
                .isInstanceOf(Npd1RejectedException.class)
                .hasMessageContaining("AES-GCM");
    }

    @Test
    void rejectsBlockSubstitutionAcrossAttemptAndOrdinal() throws Exception {
        DataObject object = encode(entries(8, 300_000, false), CompressionFamily.NONE);
        SparseBlock first = object.blocks().get(0);
        byte[] encoded = blockBytes(object, first);

        assertThatThrownBy(() -> Npd1CodecV1.decodeBlock(
                        encoded, first, KEY, UUID.fromString("123e4567-e89b-12d3-a456-426614174001"), LIMITS))
                .isInstanceOf(Npd1RejectedException.class);

        SparseBlock wrongOrdinal = new SparseBlock(
                1,
                first.firstEntryId(),
                first.entryCount(),
                first.blockOffset(),
                first.encodedBlockBytes(),
                first.decodedBlockBytes(),
                first.compressionFamily(),
                first.encryptionFamily(),
                first.encodedBlockSha256());
        assertThatThrownBy(() -> Npd1CodecV1.decodeBlock(encoded, wrongOrdinal, KEY, ATTEMPT, LIMITS))
                .isInstanceOf(Npd1RejectedException.class);
    }

    @Test
    void rejectsUnknownHeaderFlagsReservedBitsAndTruncatedHeader() throws Exception {
        DataObject object = encode(entries(1, 10, false), CompressionFamily.NONE);
        byte[] header = Arrays.copyOf(Files.readAllBytes(object.path()), Npd1CodecV1.DATA_HEADER_BYTES);

        header[11] = 1;
        assertThatThrownBy(() -> Npd1CodecV1.parseDataHeader(header)).isInstanceOf(Npd1RejectedException.class);
        assertThatThrownBy(() -> Npd1CodecV1.parseDataHeader(new byte[31])).isInstanceOf(Npd1RejectedException.class);
    }

    @Test
    void rejectsSparseLengthDigestAndDecodedFactDrift() throws Exception {
        DataObject object = encode(entries(2, 100, false), CompressionFamily.NONE);
        SparseBlock block = object.blocks().get(0);
        byte[] encoded = blockBytes(object, block);
        SparseBlock changed = new SparseBlock(
                block.blockOrdinal(),
                block.firstEntryId(),
                block.entryCount(),
                block.blockOffset(),
                block.encodedBlockBytes(),
                block.decodedBlockBytes() + 1,
                block.compressionFamily(),
                block.encryptionFamily(),
                block.encodedBlockSha256());

        assertThatThrownBy(() -> Npd1CodecV1.decodeBlock(encoded, changed, KEY, ATTEMPT, LIMITS))
                .isInstanceOf(Npd1RejectedException.class);
    }

    @Test
    void producesDeterministicCanonicalBytesForTheSameAttemptAndInputs() throws Exception {
        List<EntryPayload> entries = entries(5, 222, false);
        DataObject first = Npd1CodecV1.encode(
                temporaryDirectory.resolve("first.npd1"),
                entries,
                PulsarOffloadLimitCandidateV1.MIB,
                CompressionFamily.NONE,
                KEY,
                ATTEMPT,
                LIMITS);
        DataObject second = Npd1CodecV1.encode(
                temporaryDirectory.resolve("second.npd1"),
                entries,
                PulsarOffloadLimitCandidateV1.MIB,
                CompressionFamily.NONE,
                KEY,
                ATTEMPT,
                LIMITS);

        assertThat(second.sha256()).isEqualTo(first.sha256());
        assertThat(first.sha256()).isEqualTo("e599560c6ff8ac43683d70d4cf25ea487869259b1bc0d92197e1f71963c1c2ad");
        assertThat(Files.readAllBytes(second.path())).containsExactly(Files.readAllBytes(first.path()));
    }

    @Test
    void incrementalStreamingMatchesTheCanonicalListEncoder() throws Exception {
        List<EntryPayload> entries = entries(9, 300_000, false);
        DataObject canonical = Npd1CodecV1.encode(
                temporaryDirectory.resolve("canonical-list.npd1"),
                entries,
                PulsarOffloadLimitCandidateV1.MIB,
                CompressionFamily.NONE,
                KEY,
                ATTEMPT,
                LIMITS);
        DataObject streamed;
        try (StreamingEncoder encoder = Npd1CodecV1.openStreaming(
                temporaryDirectory.resolve("canonical-stream.npd1"),
                PulsarOffloadLimitCandidateV1.MIB,
                CompressionFamily.NONE,
                KEY,
                ATTEMPT,
                LIMITS)) {
            entries.forEach(encoder::append);
            streamed = encoder.finish();
        }

        assertThat(streamed).usingRecursiveComparison().ignoringFields("path").isEqualTo(canonical);
        assertThat(Files.readAllBytes(streamed.path())).containsExactly(Files.readAllBytes(canonical.path()));
    }

    @Test
    void abortedStreamingDeletesTheIncompleteTarget() {
        Path target = temporaryDirectory.resolve("aborted-stream.npd1");

        try (StreamingEncoder encoder = Npd1CodecV1.openStreaming(
                target, PulsarOffloadLimitCandidateV1.MIB, CompressionFamily.NONE, KEY, ATTEMPT, LIMITS)) {
            encoder.append(new EntryPayload(0, new byte[] {1, 2, 3}));
        }

        assertThat(target).doesNotExist();
    }

    private DataObject encode(List<EntryPayload> entries, CompressionFamily compression) {
        return Npd1CodecV1.encode(
                temporaryDirectory.resolve(UUID.randomUUID() + ".npd1"),
                entries,
                PulsarOffloadLimitCandidateV1.MIB,
                compression,
                KEY,
                ATTEMPT,
                LIMITS);
    }

    private List<EntryPayload> decodeAll(DataObject object) throws Exception {
        List<EntryPayload> result = new ArrayList<>();
        for (SparseBlock block : object.blocks()) {
            result.addAll(Npd1CodecV1.decodeBlock(blockBytes(object, block), block, KEY, ATTEMPT, LIMITS));
        }
        return result;
    }

    private static byte[] blockBytes(DataObject object, SparseBlock block) throws Exception {
        byte[] all = Files.readAllBytes(object.path());
        return Arrays.copyOfRange(
                all,
                Math.toIntExact(block.blockOffset()),
                Math.toIntExact(block.blockOffset() + block.encodedBlockBytes()));
    }

    private static List<EntryPayload> entries(int count, int bytes, boolean compressible) {
        List<EntryPayload> result = new ArrayList<>();
        for (int entry = 0; entry < count; entry++) {
            result.add(new EntryPayload(entry, compressible ? new byte[bytes] : sequence(bytes, entry)));
        }
        return result;
    }

    private static byte[] sequence(int length, int seed) {
        byte[] result = new byte[length];
        for (int index = 0; index < result.length; index++) {
            result[index] = (byte) (seed + index * 31);
        }
        return result;
    }

    private static String sha(byte[] bytes) throws Exception {
        return java.util.HexFormat.of()
                .formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes));
    }
}
