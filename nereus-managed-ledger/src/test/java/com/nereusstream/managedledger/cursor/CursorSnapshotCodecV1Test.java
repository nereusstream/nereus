/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ObjectKey;
import com.nereusstream.objectstore.Crc32cChecksums;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class CursorSnapshotCodecV1Test {
    @Test
    void canonicalSnapshotMatchesFrozenGoldenBytes() throws IOException {
        CursorSnapshotCodecV1.EncodedSnapshot encoded = CursorSnapshotCodecV1.encode(
                CursorTestSamples.request(), CursorTestSamples.SNAPSHOT, CursorStorageConfig.defaults());
        Properties expected = new Properties();
        try (var input = CursorSnapshotCodecV1Test.class.getResourceAsStream(
                "cursor-snapshot-v1-golden.properties")) {
            expected.load(new InputStreamReader(input, StandardCharsets.UTF_8));
        }

        assertThat(HexFormat.of().formatHex(bytes(encoded.payload())))
                .isEqualTo(expected.getProperty("snapshot.complex"));
        assertThat(Integer.toUnsignedString(encoded.formatCrc32c(), 16))
                .isEqualTo(expected.getProperty("snapshot.formatCrc"));
        assertThat(encoded.storageChecksum().value())
                .isEqualTo(expected.getProperty("snapshot.storageCrc"));
    }

    @Test
    void roundTripsCanonicalStateAndEveryChecksumReferenceField() {
        CursorSnapshotCodecV1.EncodedSnapshot encoded = CursorSnapshotCodecV1.encode(
                CursorTestSamples.request(), CursorTestSamples.SNAPSHOT, CursorStorageConfig.defaults());
        CursorSnapshotReference reference = reference(encoded);

        CursorAckState decoded = CursorSnapshotCodecV1.decode(
                encoded.payload(), reference, CursorTestSamples.identity(), CursorStorageConfig.defaults());

        assertThat(decoded).isEqualTo(CursorTestSamples.complexState());
        assertThat(encoded.objectLength()).isEqualTo(encoded.payload().remaining());
        assertThat(encoded.storageChecksum()).isEqualTo(Crc32cChecksums.checksum(encoded.payload()));
    }

    @Test
    void rejectsStableCorruptionWithoutPublishingPartialState() {
        CursorSnapshotCodecV1.EncodedSnapshot encoded = CursorSnapshotCodecV1.encode(
                CursorTestSamples.request(), CursorTestSamples.SNAPSHOT, CursorStorageConfig.defaults());
        byte[] bytes = bytes(encoded.payload());
        bytes[0] ^= 1;
        CursorSnapshotReference corruptedReference = new CursorSnapshotReference(
                new ObjectKey("key"),
                CursorTestSamples.SNAPSHOT,
                1,
                8,
                10,
                bytes.length,
                Crc32cChecksums.checksum(bytes),
                encoded.formatCrc32c(),
                1,
                105);

        assertThatThrownBy(() -> CursorSnapshotCodecV1.decode(
                ByteBuffer.wrap(bytes),
                corruptedReference,
                CursorTestSamples.identity(),
                CursorStorageConfig.defaults()))
                .isInstanceOf(CursorSnapshotCodecV1.CursorSnapshotCorruptionException.class)
                .hasMessageContaining("magic");
    }

    @Test
    void rejectsEveryIdentityAndReferenceMismatch() {
        CursorSnapshotCodecV1.EncodedSnapshot encoded = CursorSnapshotCodecV1.encode(
                CursorTestSamples.request(), CursorTestSamples.SNAPSHOT, CursorStorageConfig.defaults());
        CursorSnapshotReference reference = reference(encoded);
        CursorIdentity identity = CursorTestSamples.identity();
        CursorIdentity wrongGeneration = new CursorIdentity(
                identity.ledger(), identity.cursorName(), identity.cursorNameHash(), 2);

        assertThatThrownBy(() -> CursorSnapshotCodecV1.decode(
                encoded.payload(), reference, wrongGeneration, CursorStorageConfig.defaults()))
                .isInstanceOf(CursorSnapshotCodecV1.CursorSnapshotCorruptionException.class)
                .hasMessageContaining("cursor generation");
        CursorSnapshotReference wrongFormat = new CursorSnapshotReference(
                reference.objectKey(),
                reference.snapshotId(),
                reference.cursorGeneration(),
                reference.sourceMutationSequence(),
                reference.baseMarkDeleteOffset(),
                reference.objectLength(),
                reference.storageChecksum(),
                reference.formatCrc32c() + 1,
                1,
                reference.createdAtMillis());
        assertThatThrownBy(() -> CursorSnapshotCodecV1.decode(
                encoded.payload(), wrongFormat, identity, CursorStorageConfig.defaults()))
                .isInstanceOf(CursorSnapshotCodecV1.CursorSnapshotCorruptionException.class)
                .hasMessageContaining("format CRC32C");
    }

    private static CursorSnapshotReference reference(
            CursorSnapshotCodecV1.EncodedSnapshot encoded) {
        return new CursorSnapshotReference(
                new ObjectKey("key"),
                CursorTestSamples.SNAPSHOT,
                1,
                8,
                10,
                encoded.objectLength(),
                encoded.storageChecksum(),
                encoded.formatCrc32c(),
                1,
                105);
    }

    private static byte[] bytes(ByteBuffer buffer) {
        ByteBuffer copy = buffer.asReadOnlyBuffer();
        byte[] result = new byte[copy.remaining()];
        copy.get(result);
        return result;
    }
}
