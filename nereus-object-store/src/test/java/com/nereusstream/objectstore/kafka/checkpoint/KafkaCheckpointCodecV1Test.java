/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.kafka.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.StreamId;
import com.nereusstream.objectstore.staging.StagingFileManager;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KafkaCheckpointCodecV1Test {
    @TempDir Path temporaryDirectory;

    @Test
    void frozenNkc1BytesRoundTrip() throws Exception {
        KafkaCheckpointCodecV1 codec = new KafkaCheckpointCodecV1();
        try (StagingFileManager staging = staging();
             EncodedKafkaCheckpoint encoded = codec.encodeToStaging(staging, header(0), sections())) {
            byte[] bytes = Files.readAllBytes(encoded.stagingFile().path());
            KafkaCheckpointCodecV1.Decoded decoded = codec.decode(bytes);

            assertThat(decoded.header()).isEqualTo(header(0));
            assertThat(decoded.sections()).isEqualTo(sections());
            assertThat(decoded.storageCrc32c()).isEqualTo(encoded.storageCrc32c());
            assertThat(decoded.objectSha256()).isEqualTo(encoded.objectSha256());
            assertThat(decoded.contentSha256()).isEqualTo(encoded.contentSha256());
            assertThat(decoded.objectSha256().value())
                    .isEqualTo("c6d8848d7e946917e649b0fb0679f390ce76c8660a88bf447c797581285ce91c");
        }
    }

    @Test
    void rejectsCorruptionMissingRequiredAndUnknownRequiredSections() throws Exception {
        KafkaCheckpointCodecV1 codec = new KafkaCheckpointCodecV1();
        byte[] bytes;
        try (StagingFileManager staging = staging();
             EncodedKafkaCheckpoint encoded = codec.encodeToStaging(staging, header(0), sections())) {
            bytes = Files.readAllBytes(encoded.stagingFile().path());
        }
        byte[] badMagic = bytes.clone();
        badMagic[0] ^= 1;
        byte[] badTrailer = bytes.clone();
        badTrailer[badTrailer.length - 1] ^= 1;

        assertThatThrownBy(() -> codec.decode(badMagic))
                .isInstanceOf(KafkaCheckpointFormatException.class);
        assertThatThrownBy(() -> codec.decode(badTrailer))
                .isInstanceOf(KafkaCheckpointFormatException.class);
        assertThatThrownBy(() -> {
            try (StagingFileManager staging = staging()) {
                codec.encodeToStaging(staging, header(0), sections().subList(0, 6));
            }
        }).isInstanceOf(KafkaCheckpointFormatException.class).hasMessageContaining("missing required");

        ArrayList<KafkaCheckpointSection> unknown = new ArrayList<>(sections());
        unknown.add(new KafkaCheckpointSection(
                100, 1, KafkaCheckpointFormatV1.SECTION_REQUIRED_FLAG, new byte[] {9}));
        assertThatThrownBy(() -> {
            try (StagingFileManager staging = staging()) {
                codec.encodeToStaging(staging, header(
                        KafkaCheckpointFormatV1.HEADER_ALLOW_OPTIONAL_SECTIONS_FLAG), unknown);
            }
        }).isInstanceOf(KafkaCheckpointFormatException.class).hasMessageContaining("unsupported section");
    }

    @Test
    void optionalUnknownSectionRequiresHeaderCapability() throws Exception {
        KafkaCheckpointCodecV1 codec = new KafkaCheckpointCodecV1();
        ArrayList<KafkaCheckpointSection> values = new ArrayList<>(sections());
        values.add(new KafkaCheckpointSection(100, 1, 0, new byte[] {9}));

        assertThatThrownBy(() -> {
            try (StagingFileManager staging = staging()) {
                codec.encodeToStaging(staging, header(0), values);
            }
        }).isInstanceOf(KafkaCheckpointFormatException.class);
        try (StagingFileManager staging = staging();
             EncodedKafkaCheckpoint encoded = codec.encodeToStaging(
                     staging,
                     header(KafkaCheckpointFormatV1.HEADER_ALLOW_OPTIONAL_SECTIONS_FLAG),
                     values)) {
            assertThat(codec.decode(Files.readAllBytes(encoded.stagingFile().path())).sections()).hasSize(8);
        }
    }

    private StagingFileManager staging() throws Exception {
        Path directory = Files.createTempDirectory(temporaryDirectory, "nkc1-");
        Files.setPosixFilePermissions(directory, PosixFilePermissions.fromString("rwx------"));
        return new StagingFileManager(
                directory, 32L << 20, StagingFileManager.MIN_UPLOAD_CHUNK_BYTES,
                Duration.ofHours(1), Runnable::run);
    }

    static KafkaCheckpointHeader header(int flags) {
        return new KafkaCheckpointHeader(
                flags, "kraft-cluster", "EjRWeJq83vAAAAAAAAAAAQ", 3, 1,
                new StreamId("stream-1"), 1, 7, 42, 5, 42, 12,
                "commit-12", sha256('a'));
    }

    static List<KafkaCheckpointSection> sections() {
        return java.util.Arrays.stream(KafkaCheckpointSectionType.values())
                .map(type -> KafkaCheckpointSection.required(
                        type, new byte[] {(byte) type.wireId(), (byte) (type.wireId() + 10)}))
                .toList();
    }

    static Checksum sha256(char value) {
        return new Checksum(ChecksumType.SHA256, Character.toString(value).repeat(64));
    }
}
