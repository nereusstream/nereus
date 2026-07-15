/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.checkpoint;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.objectstore.Crc32cChecksums;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.objectstore.staging.StagingFileManager;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RecoveryCheckpointStrictDecodeTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void lookupRejectsBadEntryCrcEvenWhenObjectAndFooterDigestsWereRecomputed() throws Exception {
        try (Fixture fixture = fixture("entry-crc")) {
            byte[] corrupted = fixture.bytes().clone();
            long firstEntryOffset = firstCommitEntryOffset(corrupted, fixture.written().directory());
            ByteBuffer entryBytes = ByteBuffer.wrap(corrupted)
                    .position(Math.toIntExact(firstEntryOffset))
                    .slice()
                    .asReadOnlyBuffer();
            int entryLength = RecoveryCheckpointBinary.decodeEntry(entryBytes).bytesConsumed();
            corrupted[Math.toIntExact(firstEntryOffset) + entryLength - 1] ^= 0x01;
            UploadedCorruption uploaded = uploadSelfConsistent(fixture, corrupted);

            RecoveryCheckpointObject opened = fixture.codec().openAndVerify(
                            uploaded.key(),
                            corrupted.length,
                            uploaded.contentSha256(),
                            RecoveryCheckpointTestSupport.TIMEOUT)
                    .join();
            assertThatThrownBy(() -> fixture.codec()
                            .findCommit(
                                    opened,
                                    5,
                                    "commit-5",
                                    RecoveryCheckpointTestSupport.TIMEOUT)
                            .join())
                    .hasRootCauseInstanceOf(RecoveryCheckpointFormatException.class)
                    .hasRootCauseMessage("NRC1 commit entry CRC32C mismatch");
        }
    }

    @Test
    void openRejectsNonCanonicalDirectoryEvenWhenAllObjectDigestsMatch() throws Exception {
        try (Fixture fixture = fixture("directory")) {
            byte[] corrupted = fixture.bytes().clone();
            int firstIndex = Math.toIntExact(
                    fixture.written().directory().publicationDirectoryOffset() + Integer.BYTES);
            ByteBuffer.wrap(corrupted).order(ByteOrder.BIG_ENDIAN).putInt(firstIndex, 1);
            UploadedCorruption uploaded = uploadSelfConsistent(fixture, corrupted);

            assertThatThrownBy(() -> fixture.codec()
                            .openAndVerify(
                                    uploaded.key(),
                                    corrupted.length,
                                    uploaded.contentSha256(),
                                    RecoveryCheckpointTestSupport.TIMEOUT)
                            .join())
                    .hasRootCauseInstanceOf(RecoveryCheckpointFormatException.class)
                    .hasRootCauseMessage(
                            "NRC1 publication directory is not canonical and strictly ordered");
        }
    }

    private Fixture fixture(String suffix) throws Exception {
        LocalFileObjectStore objectStore = new LocalFileObjectStore(
                temporaryDirectory.resolve("objects-" + suffix));
        StagingFileManager staging = RecoveryCheckpointTestSupport.staging(
                temporaryDirectory.resolve("staging-parent-" + suffix), 64L << 20);
        DefaultRecoveryCheckpointCodecV1 codec = new DefaultRecoveryCheckpointCodecV1(
                objectStore, staging, Runnable::run, RecoveryCheckpointTestSupport.verifier());
        RecoveryCheckpointWriteResult written = codec.write(
                        RecoveryCheckpointTestSupport.request("a".repeat(26)),
                        RecoveryCheckpointTestSupport.publisher(RecoveryCheckpointTestSupport.publications()),
                        RecoveryCheckpointTestSupport.publisher(RecoveryCheckpointTestSupport.entries()))
                .join();
        return new Fixture(
                objectStore,
                staging,
                codec,
                written,
                RecoveryCheckpointTestSupport.collect(written.stagingFile()));
    }

    private static long firstCommitEntryOffset(
            byte[] bytes,
            RecoveryCheckpointDirectory directory) {
        int valueOffset = Math.toIntExact(directory.commitDirectoryOffset())
                + Integer.BYTES * 2
                + Long.BYTES * 2;
        return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getLong(valueOffset);
    }

    private static UploadedCorruption uploadSelfConsistent(Fixture fixture, byte[] bytes) {
        int footerOffset = bytes.length - RecoveryCheckpointFormatV1.FOOTER_BYTES;
        Checksum body = RecoveryCheckpointTestSupport.sha256(
                java.util.Arrays.copyOfRange(bytes, 0, footerOffset));
        ByteBuffer footer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        byte[] bodyDigest = java.util.HexFormat.of().parseHex(body.value());
        footer.position(footerOffset + 4 + Long.BYTES * 4);
        footer.put(bodyDigest);
        byte[] protectedFooter = java.util.Arrays.copyOfRange(
                bytes, footerOffset, bytes.length - Integer.BYTES);
        footer.putInt(
                bytes.length - Integer.BYTES,
                Crc32cChecksums.intValue(Crc32cChecksums.checksum(protectedFooter)));
        Checksum content = RecoveryCheckpointTestSupport.sha256(bytes);
        ObjectKey key = RecoveryCheckpointFormatV1.objectKey(
                RecoveryCheckpointTestSupport.request("a".repeat(26)), content);
        fixture.objectStore().putObject(
                        key,
                        ByteBuffer.wrap(bytes),
                        new PutObjectOptions(
                                RecoveryCheckpointFormatV1.CONTENT_TYPE,
                                RecoveryCheckpointTestSupport.crc32c(bytes),
                                true,
                                Map.of("nereus-format", "NRC1"),
                                RecoveryCheckpointTestSupport.TIMEOUT))
                .join();
        return new UploadedCorruption(key, content);
    }

    private record UploadedCorruption(ObjectKey key, Checksum contentSha256) {
    }

    private record Fixture(
            LocalFileObjectStore objectStore,
            StagingFileManager staging,
            DefaultRecoveryCheckpointCodecV1 codec,
            RecoveryCheckpointWriteResult written,
            byte[] bytes) implements AutoCloseable {
        @Override
        public void close() {
            written.close();
            staging.close();
            objectStore.close();
        }
    }
}
