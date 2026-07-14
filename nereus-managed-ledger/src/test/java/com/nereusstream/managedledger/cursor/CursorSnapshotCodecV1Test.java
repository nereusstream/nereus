/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ObjectKey;
import com.nereusstream.metadata.oxia.CursorNames;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
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
    void rejectsEverySingleByteMutationEveryTruncationAndTrailingBytes() {
        CursorSnapshotCodecV1.EncodedSnapshot encoded = CursorSnapshotCodecV1.encode(
                CursorTestSamples.request(), CursorTestSamples.SNAPSHOT, CursorStorageConfig.defaults());
        byte[] canonical = bytes(encoded.payload());

        for (int index = 0; index < canonical.length; index++) {
            byte[] mutated = canonical.clone();
            mutated[index] ^= 1;
            assertThatThrownBy(() -> CursorSnapshotCodecV1.decode(
                    ByteBuffer.wrap(mutated),
                    reference(encoded, mutated),
                    CursorTestSamples.identity(),
                    CursorStorageConfig.defaults()))
                    .as("single-byte mutation at index %s", index)
                    .isInstanceOf(CursorSnapshotCodecV1.CursorSnapshotCorruptionException.class);
        }

        for (int length = 1; length < canonical.length; length++) {
            byte[] truncated = java.util.Arrays.copyOf(canonical, length);
            assertThatThrownBy(() -> CursorSnapshotCodecV1.decode(
                    ByteBuffer.wrap(truncated),
                    reference(encoded, truncated),
                    CursorTestSamples.identity(),
                    CursorStorageConfig.defaults()))
                    .as("truncation at length %s", length)
                    .isInstanceOf(CursorSnapshotCodecV1.CursorSnapshotCorruptionException.class);
        }

        byte[] withTrailingByte = java.util.Arrays.copyOf(canonical, canonical.length + 1);
        assertThatThrownBy(() -> CursorSnapshotCodecV1.decode(
                ByteBuffer.wrap(withTrailingByte),
                reference(encoded, withTrailingByte),
                CursorTestSamples.identity(),
                CursorStorageConfig.defaults()))
                .isInstanceOf(CursorSnapshotCodecV1.CursorSnapshotCorruptionException.class);
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

    @Test
    void rejectsAllHeaderIdentityAndRootReferenceMismatches() {
        CursorSnapshotCodecV1.EncodedSnapshot encoded = CursorSnapshotCodecV1.encode(
                CursorTestSamples.request(), CursorTestSamples.SNAPSHOT, CursorStorageConfig.defaults());
        CursorIdentity identity = CursorTestSamples.identity();
        CursorSnapshotReference reference = reference(encoded);
        ManagedLedgerProjectionIdentity projection = identity.ledger().projection();

        assertCorruption(encoded, reference, identity(
                identity.ledger().managedLedgerName(),
                new ManagedLedgerProjectionIdentity(
                        projection.storageClassBindingGeneration(),
                        projection.incarnation(),
                        ManagedLedgerProjectionNames.streamId("persistent://tenant/ns/other", 2).value(),
                        projection.virtualLedgerId()),
                identity.cursorName(),
                identity.cursorGeneration()), "stream ID");
        assertCorruption(encoded, reference, identity(
                "persistent://tenant/ns/other-name",
                projection,
                identity.cursorName(),
                identity.cursorGeneration()), "managed-ledger name hash");
        assertCorruption(encoded, reference, identity(
                identity.ledger().managedLedgerName(),
                projection,
                "other-subscription",
                identity.cursorGeneration()), "cursor name hash");
        assertCorruption(encoded, reference, identity(
                identity.ledger().managedLedgerName(),
                new ManagedLedgerProjectionIdentity(
                        projection.storageClassBindingGeneration() + 1,
                        projection.incarnation(),
                        projection.streamId(),
                        projection.virtualLedgerId()),
                identity.cursorName(),
                identity.cursorGeneration()), "binding generation");
        assertCorruption(encoded, reference, identity(
                identity.ledger().managedLedgerName(),
                new ManagedLedgerProjectionIdentity(
                        projection.storageClassBindingGeneration(),
                        projection.incarnation() + 1,
                        projection.streamId(),
                        projection.virtualLedgerId()),
                identity.cursorName(),
                identity.cursorGeneration()), "incarnation");
        assertCorruption(encoded, reference, identity(
                identity.ledger().managedLedgerName(),
                new ManagedLedgerProjectionIdentity(
                        projection.storageClassBindingGeneration(),
                        projection.incarnation(),
                        projection.streamId(),
                        projection.virtualLedgerId() + 1),
                identity.cursorName(),
                identity.cursorGeneration()), "virtual ledger ID");

        assertCorruption(encoded, copyReference(reference,
                "00112233445566778899aabbccddeeff",
                reference.sourceMutationSequence(),
                reference.baseMarkDeleteOffset(),
                reference.objectLength(),
                reference.storageChecksum(),
                reference.formatCrc32c(),
                reference.createdAtMillis()), identity, "snapshot ID");
        assertCorruption(encoded, copyReference(reference,
                reference.snapshotId(),
                reference.sourceMutationSequence() + 1,
                reference.baseMarkDeleteOffset(),
                reference.objectLength(),
                reference.storageChecksum(),
                reference.formatCrc32c(),
                reference.createdAtMillis()), identity, "source mutation sequence");
        assertCorruption(encoded, copyReference(reference,
                reference.snapshotId(),
                reference.sourceMutationSequence(),
                reference.baseMarkDeleteOffset() + 1,
                reference.objectLength(),
                reference.storageChecksum(),
                reference.formatCrc32c(),
                reference.createdAtMillis()), identity, "base mark-delete offset");
        assertCorruption(encoded, copyReference(reference,
                reference.snapshotId(),
                reference.sourceMutationSequence(),
                reference.baseMarkDeleteOffset(),
                reference.objectLength() + 1,
                reference.storageChecksum(),
                reference.formatCrc32c(),
                reference.createdAtMillis()), identity, "snapshot length");
        assertCorruption(encoded, copyReference(reference,
                reference.snapshotId(),
                reference.sourceMutationSequence(),
                reference.baseMarkDeleteOffset(),
                reference.objectLength(),
                Crc32cChecksums.checksum(new byte[] {1}),
                reference.formatCrc32c(),
                reference.createdAtMillis()), identity, "full-object checksum");
        assertCorruption(encoded, copyReference(reference,
                reference.snapshotId(),
                reference.sourceMutationSequence(),
                reference.baseMarkDeleteOffset(),
                reference.objectLength(),
                reference.storageChecksum(),
                reference.formatCrc32c(),
                reference.createdAtMillis() + 1), identity, "snapshot creation time");
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

    private static CursorSnapshotReference reference(
            CursorSnapshotCodecV1.EncodedSnapshot encoded, byte[] storedBytes) {
        return new CursorSnapshotReference(
                new ObjectKey("key"),
                CursorTestSamples.SNAPSHOT,
                1,
                8,
                10,
                storedBytes.length,
                Crc32cChecksums.checksum(storedBytes),
                encoded.formatCrc32c(),
                1,
                105);
    }

    private static CursorIdentity identity(
            String managedLedgerName,
            ManagedLedgerProjectionIdentity projection,
            String cursorName,
            long generation) {
        return new CursorIdentity(
                new CursorLedgerIdentity(
                        managedLedgerName,
                        ManagedLedgerProjectionNames.managedLedgerNameHash(managedLedgerName),
                        projection),
                cursorName,
                CursorNames.cursorNameHash(cursorName),
                generation);
    }

    private static CursorSnapshotReference copyReference(
            CursorSnapshotReference source,
            String snapshotId,
            long sourceSequence,
            long baseMarkDelete,
            long objectLength,
            com.nereusstream.api.Checksum checksum,
            int formatCrc,
            long createdAtMillis) {
        return new CursorSnapshotReference(
                source.objectKey(),
                snapshotId,
                source.cursorGeneration(),
                sourceSequence,
                baseMarkDelete,
                objectLength,
                checksum,
                formatCrc,
                source.formatVersion(),
                createdAtMillis);
    }

    private static void assertCorruption(
            CursorSnapshotCodecV1.EncodedSnapshot encoded,
            CursorSnapshotReference reference,
            CursorIdentity identity,
            String message) {
        assertThatThrownBy(() -> CursorSnapshotCodecV1.decode(
                encoded.payload(), reference, identity, CursorStorageConfig.defaults()))
                .isInstanceOf(CursorSnapshotCodecV1.CursorSnapshotCorruptionException.class)
                .hasMessageContaining(message);
    }

    private static byte[] bytes(ByteBuffer buffer) {
        ByteBuffer copy = buffer.asReadOnlyBuffer();
        byte[] result = new byte[copy.remaining()];
        copy.get(result);
        return result;
    }
}
