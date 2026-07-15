/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.objectstore.staging.StagingFileManager;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RecoveryCheckpointAttemptIdentityTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void identicalAttemptIsByteStableAndFreshAttemptCannotReuseDeletedIdentity() throws Exception {
        try (LocalFileObjectStore objectStore = new LocalFileObjectStore(
                        temporaryDirectory.resolve("objects"));
                StagingFileManager staging = RecoveryCheckpointTestSupport.staging(
                        temporaryDirectory, 128L << 20)) {
            DefaultRecoveryCheckpointCodecV1 codec = new DefaultRecoveryCheckpointCodecV1(
                    objectStore, staging, Runnable::run, RecoveryCheckpointTestSupport.verifier());
            try (RecoveryCheckpointWriteResult first = write(codec, "a".repeat(26));
                    RecoveryCheckpointWriteResult retry = write(codec, "a".repeat(26));
                    RecoveryCheckpointWriteResult fresh = write(codec, "b".repeat(26))) {
                assertThat(retry.contentSha256()).isEqualTo(first.contentSha256());
                assertThat(retry.objectKey()).isEqualTo(first.objectKey());
                assertThat(RecoveryCheckpointTestSupport.collect(retry.stagingFile()))
                        .containsExactly(RecoveryCheckpointTestSupport.collect(first.stagingFile()));

                assertThat(fresh.contentSha256()).isNotEqualTo(first.contentSha256());
                assertThat(fresh.objectKey()).isNotEqualTo(first.objectKey());
                assertThat(fresh.objectKey().value()).endsWith("-" + "b".repeat(26) + ".nrc");
            }
        }
    }

    private static RecoveryCheckpointWriteResult write(
            DefaultRecoveryCheckpointCodecV1 codec,
            String attemptId) {
        return codec.write(
                        RecoveryCheckpointTestSupport.request(attemptId),
                        RecoveryCheckpointTestSupport.publisher(RecoveryCheckpointTestSupport.publications()),
                        RecoveryCheckpointTestSupport.publisher(RecoveryCheckpointTestSupport.entries()))
                .join();
    }
}
