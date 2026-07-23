/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.kafka.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import com.nereusstream.objectstore.staging.StagingFileManager;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KafkaCheckpointWriterReaderTest {
    @TempDir Path temporaryDirectory;

    @Test
    void immutablePutAndAlreadyExistsResponseConvergeToExactObject() throws Exception {
        Path objects = Files.createDirectory(temporaryDirectory.resolve("objects"));
        Path stagingDirectory = Files.createDirectory(temporaryDirectory.resolve("staging"));
        Files.setPosixFilePermissions(stagingDirectory, PosixFilePermissions.fromString("rwx------"));
        try (LocalFileObjectStore store = new LocalFileObjectStore(objects);
             StagingFileManager staging = new StagingFileManager(
                     stagingDirectory, 32L << 20, StagingFileManager.MIN_UPLOAD_CHUNK_BYTES,
                     Duration.ofHours(1), Runnable::run)) {
            KafkaCheckpointCodecV1 codec = new KafkaCheckpointCodecV1();
            KafkaCheckpointReader reader = new KafkaCheckpointReader(store, codec);
            KafkaCheckpointVerifier verifier = new KafkaCheckpointVerifier();
            KafkaCheckpointWriter writer = new KafkaCheckpointWriter(
                    store, staging, Runnable::run, codec, reader, verifier);
            KafkaCheckpointWriteRequest request = new KafkaCheckpointWriteRequest(
                    "nereus", KafkaCheckpointCodecV1Test.header(0),
                    KafkaCheckpointCodecV1Test.sections(), KafkaCheckpointCodecV1Test.sha256('b'),
                    Duration.ofSeconds(5));

            KafkaCheckpointObject first = writer.write(request).join();
            KafkaCheckpointObject replayed = writer.write(request).join();

            assertThat(replayed.objectKey()).isEqualTo(first.objectKey());
            assertThat(replayed.objectSha256()).isEqualTo(first.objectSha256());
            assertThat(replayed.sections()).isEqualTo(request.sections());
            verifier.verifyExpected(replayed, "nereus", request.header(), request.contentPolicySha256());
            verifier.verifyRecoveryWindow(
                    replayed, "kraft-cluster", request.header().topicId(), 3, 1, "stream-1", 1, 5, 42);
        }
    }
}
