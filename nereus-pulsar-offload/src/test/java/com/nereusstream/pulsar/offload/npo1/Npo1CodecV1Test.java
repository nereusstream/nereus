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

package com.nereusstream.pulsar.offload.npo1;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.pulsar.offload.PulsarOffloadLimitCandidateV1;
import com.nereusstream.pulsar.offload.PulsarSealedLedgerAttemptV1.RetentionClass;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.CompressionFamily;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.EncryptionFamily;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.SparseBlock;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.AttemptSection;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.CustomMetadataValue;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.DataExtentSection;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.DigestType;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.EnsembleSegment;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.Npo1RejectedException;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.Root;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.SealedLedgerSection;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class Npo1CodecV1Test {
    private static final PulsarOffloadLimitCandidateV1 LIMITS =
            PulsarOffloadLimitCandidateV1.adr0056EvidenceCandidate();
    private static final UUID ATTEMPT = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");

    @Test
    void roundTripsExactlyFourCanonicalSectionsAndCompleteFacts() {
        Root root = withNativeBinaryMetadata(fixture());
        byte[] bytes = Npo1CodecV1.canonicalBytes(root, LIMITS);
        Root decoded = Npo1CodecV1.parseCanonical(bytes, LIMITS);

        assertThat(decoded).usingRecursiveComparison().isEqualTo(root);
        assertThat(ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getInt()).isEqualTo(0x4e504f31);
        assertThat(bytes).hasSizeLessThan(Npo1CodecV1.MAX_ROOT_BYTES);
    }

    @Test
    void producesOneStableCanonicalGolden() {
        byte[] bytes = Npo1CodecV1.canonicalBytes(fixture(), LIMITS);

        assertThat(Npo1CodecV1.rootSha256(bytes))
                .isEqualTo("a73794af0ffc13ecc07d29d6925d9cda11681995a2aa0b5f4cbc22de924a412c");
    }

    @Test
    void canonicalizesCustomMetadataByUnsignedUtf8KeyBytes() {
        Map<String, String> first = new LinkedHashMap<>();
        first.put("z", "last");
        first.put("a", "first");
        Map<String, String> second = new LinkedHashMap<>();
        second.put("a", "first");
        second.put("z", "last");

        assertThat(Npo1CodecV1.canonicalBytes(withMetadata(first), LIMITS))
                .containsExactly(Npo1CodecV1.canonicalBytes(withMetadata(second), LIMITS));

        byte[] ordered = Npo1CodecV1.canonicalBytes(withMetadata(Map.of("aa", "1", "bb", "2")), LIMITS);
        byte[] duplicate = ordered.clone();
        overwrite(duplicate, "bb", "aa");
        resign(duplicate);
        assertThatThrownBy(() -> Npo1CodecV1.parseCanonical(duplicate, LIMITS))
                .isInstanceOf(Npo1RejectedException.class)
                .hasMessageContaining("canonical");

        byte[] reversed = ordered.clone();
        overwrite(reversed, "aa", "cc");
        resign(reversed);
        assertThatThrownBy(() -> Npo1CodecV1.parseCanonical(reversed, LIMITS))
                .isInstanceOf(Npo1RejectedException.class)
                .hasMessageContaining("canonical");
    }

    @Test
    void rejectsSelfDigestCorruptionBeforeVariableFields() {
        byte[] bytes = Npo1CodecV1.canonicalBytes(fixture(), LIMITS);
        bytes[bytes.length / 2] ^= 1;

        assertThatThrownBy(() -> Npo1CodecV1.parseCanonical(bytes, LIMITS))
                .isInstanceOf(Npo1RejectedException.class)
                .hasMessageContaining("self-digest");
    }

    @Test
    void rejectsWrongMagicFlagsLengthAndTrailingBytes() {
        byte[] canonical = Npo1CodecV1.canonicalBytes(fixture(), LIMITS);
        for (int offset : List.of(0, 11, 27)) {
            byte[] changed = canonical.clone();
            changed[offset] ^= 1;
            resign(changed);
            assertThatThrownBy(() -> Npo1CodecV1.parseCanonical(changed, LIMITS))
                    .isInstanceOf(Npo1RejectedException.class);
        }
        byte[] trailing = Arrays.copyOf(canonical, canonical.length + 1);
        assertThatThrownBy(() -> Npo1CodecV1.parseCanonical(trailing, LIMITS))
                .isInstanceOf(Npo1RejectedException.class);
    }

    @Test
    void rejectsReorderedOrUnknownRequiredSections() {
        byte[] bytes = Npo1CodecV1.canonicalBytes(fixture(), LIMITS);
        bytes[Npo1CodecV1.HEADER_BYTES + 1] = 2;
        resign(bytes);

        assertThatThrownBy(() -> Npo1CodecV1.parseCanonical(bytes, LIMITS))
                .isInstanceOf(Npo1RejectedException.class)
                .hasMessageContaining("section");
    }

    @Test
    void rejectsMalformedUtf8EvenWithARecomputedDigest() {
        byte[] bytes = Npo1CodecV1.canonicalBytes(fixture(), LIMITS);
        byte[] scope = "cells/pulsar-a".getBytes(StandardCharsets.UTF_8);
        int offset = indexOf(bytes, scope);
        bytes[offset] = (byte) 0xc3;
        bytes[offset + 1] = (byte) 0x28;
        resign(bytes);

        assertThatThrownBy(() -> Npo1CodecV1.parseCanonical(bytes, LIMITS))
                .isInstanceOf(Npo1RejectedException.class)
                .hasMessageContaining("UTF-8");
    }

    @Test
    void rejectsDataKeyThatDoesNotDeriveFromTheAttempt() {
        Root root = fixture();
        Root changed = new Root(
                root.attempt(),
                root.sealedLedger(),
                new DataExtentSection(1, "cells/pulsar-a/wrong/data", 232, "a".repeat(64), "version-1"),
                root.sparseIndex());

        assertThatThrownBy(() -> Npo1CodecV1.canonicalBytes(changed, LIMITS))
                .isInstanceOf(Npo1RejectedException.class)
                .hasMessageContaining("key");
    }

    @Test
    void rejectsSparseEntryGapOverlapAndOrdinalSubstitution() {
        Root root = fixture();
        for (SparseBlock changedRow : List.of(
                row(1, 3, 3, 132, 100, 30, "b"), row(2, 2, 3, 132, 100, 30, "b"), row(1, 1, 3, 132, 100, 30, "b"))) {
            Root changed = new Root(
                    root.attempt(),
                    root.sealedLedger(),
                    root.dataExtent(),
                    List.of(root.sparseIndex().get(0), changedRow));
            assertThatThrownBy(() -> Npo1CodecV1.canonicalBytes(changed, LIMITS))
                    .isInstanceOf(Npo1RejectedException.class);
        }
    }

    @Test
    void rejectsSparseByteGapOverlapAndFinalLengthDrift() {
        Root root = fixture();
        for (SparseBlock changedRow : List.of(
                row(1, 2, 3, 133, 99, 30, "b"), row(1, 2, 3, 131, 101, 30, "b"), row(1, 2, 3, 132, 99, 30, "b"))) {
            Root changed = new Root(
                    root.attempt(),
                    root.sealedLedger(),
                    root.dataExtent(),
                    List.of(root.sparseIndex().get(0), changedRow));
            assertThatThrownBy(() -> Npo1CodecV1.canonicalBytes(changed, LIMITS))
                    .isInstanceOf(Npo1RejectedException.class);
        }
    }

    @Test
    void rejectsEmptyOrMismatchedLedgerCountAndLac() {
        assertThatThrownBy(() -> ledger(-1, 0, Map.of(), List.of(new EnsembleSegment(0, List.of("bookie-1")))))
                .isInstanceOf(Npo1RejectedException.class);
        assertThatThrownBy(() -> ledger(4, 4, Map.of(), List.of(new EnsembleSegment(0, List.of("bookie-1")))))
                .isInstanceOf(Npo1RejectedException.class);
    }

    @Test
    void rejectsEnsembleThatDoesNotBeginAtZeroOrMovesBackward() {
        assertThatThrownBy(() -> ledger(4, 5, Map.of(), List.of(new EnsembleSegment(1, List.of("bookie-1")))))
                .isInstanceOf(Npo1RejectedException.class);
        assertThatThrownBy(() -> ledger(
                        4,
                        5,
                        Map.of(),
                        List.of(
                                new EnsembleSegment(0, List.of("bookie-1")),
                                new EnsembleSegment(3, List.of("bookie-2")),
                                new EnsembleSegment(2, List.of("bookie-3")))))
                .isInstanceOf(Npo1RejectedException.class);
    }

    @Test
    void rejectsMetadataCountAndStringBoundsBeforeAllocation() {
        Map<String, String> excessive = new LinkedHashMap<>();
        for (int index = 0; index <= Npo1CodecV1.MAX_CUSTOM_METADATA_ENTRIES; index++) {
            excessive.put("key-" + index, "value");
        }
        assertThatThrownBy(() -> withMetadata(excessive)).isInstanceOf(Npo1RejectedException.class);
        assertThatThrownBy(() -> new EnsembleSegment(0, List.of("b".repeat(Npo1CodecV1.MAX_BOOKIE_ID_BYTES + 1))))
                .isInstanceOf(Npo1RejectedException.class);
    }

    @Test
    void rejectsCandidateBlockTargetAndDataObjectHardCapDrift() {
        Root root = fixture();
        AttemptSection changedAttempt = new AttemptSection(
                42,
                ATTEMPT,
                "cells/pulsar-a",
                1,
                RetentionClass.DELETE_AFTER_VERIFIED,
                2 * PulsarOffloadLimitCandidateV1.MIB);
        assertThatThrownBy(() -> Npo1CodecV1.canonicalBytes(
                        new Root(changedAttempt, root.sealedLedger(), root.dataExtent(), root.sparseIndex()), LIMITS))
                .isInstanceOf(Npo1RejectedException.class);

        DataExtentSection huge = new DataExtentSection(
                1, root.dataExtent().dataKey(), LIMITS.maxDataObjectBytes() + 1, "a".repeat(64), "version-1");
        assertThatThrownBy(() -> Npo1CodecV1.canonicalBytes(
                        new Root(root.attempt(), root.sealedLedger(), huge, root.sparseIndex()), LIMITS))
                .isInstanceOf(Npo1RejectedException.class);
    }

    @Test
    void rejectsSectionLengthThatEscapesTheVerifiedRoot() {
        byte[] bytes = Npo1CodecV1.canonicalBytes(fixture(), LIMITS);
        ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).putLong(Npo1CodecV1.HEADER_BYTES + 8, Long.MAX_VALUE);
        resign(bytes);

        assertThatThrownBy(() -> Npo1CodecV1.parseCanonical(bytes, LIMITS))
                .isInstanceOf(Npo1RejectedException.class)
                .hasMessageContaining("section");
    }

    @Test
    void rejectsSparseReservedBitsAndCodecFamilyDrift() {
        byte[] canonical = Npo1CodecV1.canonicalBytes(fixture(), LIMITS);
        int sparseBody = sparseBodyOffset(canonical);
        byte[] reserved = canonical.clone();
        ByteBuffer.wrap(reserved).order(ByteOrder.BIG_ENDIAN).putInt(sparseBody + 8, 1);
        resign(reserved);
        assertThatThrownBy(() -> Npo1CodecV1.parseCanonical(reserved, LIMITS))
                .isInstanceOf(Npo1RejectedException.class)
                .hasMessageContaining("reserved");

        byte[] codec = canonical.clone();
        ByteBuffer.wrap(codec).order(ByteOrder.BIG_ENDIAN).putShort(sparseBody + 24, (short) 99);
        resign(codec);
        assertThatThrownBy(() -> Npo1CodecV1.parseCanonical(codec, LIMITS))
                .isInstanceOf(Npo1RejectedException.class)
                .hasMessageContaining("codec");
    }

    private static Root fixture() {
        AttemptSection attempt = new AttemptSection(
                42,
                ATTEMPT,
                "cells/pulsar-a",
                1,
                RetentionClass.DELETE_AFTER_VERIFIED,
                PulsarOffloadLimitCandidateV1.MIB);
        SealedLedgerSection ledger = ledger(
                4,
                5,
                Map.of("application", "test", "tenant", "public"),
                List.of(
                        new EnsembleSegment(0, List.of("bookie-1", "bookie-2", "bookie-3")),
                        new EnsembleSegment(3, List.of("bookie-2", "bookie-3", "bookie-4"))));
        DataExtentSection data = new DataExtentSection(1, attempt.keys().dataKey(), 232, "a".repeat(64), "version-1");
        return new Root(
                attempt, ledger, data, List.of(row(0, 0, 2, 32, 100, 20, "a"), row(1, 2, 3, 132, 100, 30, "b")));
    }

    private static Root withMetadata(Map<String, String> metadata) {
        Root root = fixture();
        return new Root(
                root.attempt(),
                ledger(4, 5, metadata, root.sealedLedger().ensembles()),
                root.dataExtent(),
                root.sparseIndex());
    }

    private static Root withNativeBinaryMetadata(Root root) {
        SealedLedgerSection ledger = root.sealedLedger();
        Map<String, CustomMetadataValue> metadata = new LinkedHashMap<>(ledger.customMetadata());
        metadata.put("native-binary", new CustomMetadataValue(new byte[] {0, (byte) 0xff}));
        SealedLedgerSection changed = new SealedLedgerSection(
                ledger.lastAddConfirmed(),
                ledger.entryCount(),
                ledger.logicalLength(),
                ledger.creationTimestampMillis(),
                ledger.fencedOwnerEpoch(),
                ledger.ensembleSize(),
                ledger.writeQuorum(),
                ledger.ackQuorum(),
                ledger.digestType(),
                metadata,
                ledger.ensembles());
        return new Root(root.attempt(), changed, root.dataExtent(), root.sparseIndex());
    }

    private static SealedLedgerSection ledger(
            long lac, long count, Map<String, String> metadata, List<EnsembleSegment> ensembles) {
        Map<String, CustomMetadataValue> binaryMetadata = new LinkedHashMap<>();
        metadata.forEach((key, value) ->
                binaryMetadata.put(key, new CustomMetadataValue(value.getBytes(StandardCharsets.UTF_8))));
        return new SealedLedgerSection(
                lac, count, 1_024, 100, 7, 3, 3, 2, DigestType.CRC32C, binaryMetadata, ensembles);
    }

    private static SparseBlock row(
            int ordinal,
            long firstEntry,
            int count,
            long offset,
            int encodedBytes,
            long decodedBytes,
            String shaCharacter) {
        return new SparseBlock(
                ordinal,
                firstEntry,
                count,
                offset,
                encodedBytes,
                decodedBytes,
                CompressionFamily.NONE,
                EncryptionFamily.AES_GCM_256,
                shaCharacter.repeat(64));
    }

    private static void resign(byte[] bytes) {
        try {
            int digestOffset = bytes.length - Npo1CodecV1.SELF_DIGEST_BYTES;
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(Arrays.copyOf(bytes, digestOffset));
            System.arraycopy(digest, 0, bytes, digestOffset, digest.length);
        } catch (Exception failure) {
            throw new AssertionError(failure);
        }
    }

    private static int indexOf(byte[] bytes, byte[] target) {
        outer:
        for (int offset = 0; offset <= bytes.length - target.length; offset++) {
            for (int index = 0; index < target.length; index++) {
                if (bytes[offset + index] != target[index]) {
                    continue outer;
                }
            }
            return offset;
        }
        throw new AssertionError("target bytes not found");
    }

    private static void overwrite(byte[] bytes, String target, String replacement) {
        byte[] targetBytes = target.getBytes(StandardCharsets.UTF_8);
        byte[] replacementBytes = replacement.getBytes(StandardCharsets.UTF_8);
        if (targetBytes.length != replacementBytes.length) {
            throw new AssertionError("replacement must preserve canonical length");
        }
        System.arraycopy(replacementBytes, 0, bytes, indexOf(bytes, targetBytes), replacementBytes.length);
    }

    private static int sparseBodyOffset(byte[] bytes) {
        ByteBuffer input = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        input.position(Npo1CodecV1.HEADER_BYTES);
        for (int section = 1; section < 4; section++) {
            input.position(input.position() + 8);
            long length = input.getLong();
            input.position(Math.addExact(input.position(), Math.toIntExact(length)));
        }
        input.position(input.position() + Npo1CodecV1.SECTION_HEADER_BYTES);
        return input.position();
    }
}
