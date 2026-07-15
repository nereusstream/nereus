/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.PublicationId;
import com.nereusstream.objectstore.staging.StagingFileManager;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RecoveryCheckpointGoldenTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void writesOpensAndRangeLooksUpOneCanonicalNrc1Object() {
        try (LocalFileObjectStore objectStore = new LocalFileObjectStore(
                        temporaryDirectory.resolve("objects"));
                StagingFileManager staging = RecoveryCheckpointTestSupport.staging(
                        temporaryDirectory, 64L << 20);
                RecoveryCheckpointWriteResult written = new DefaultRecoveryCheckpointCodecV1(
                                objectStore,
                                staging,
                                Runnable::run,
                                RecoveryCheckpointTestSupport.verifier())
                        .write(
                                RecoveryCheckpointTestSupport.request("a".repeat(26)),
                                RecoveryCheckpointTestSupport.publisher(
                                        RecoveryCheckpointTestSupport.publications()),
                                RecoveryCheckpointTestSupport.publisher(
                                        RecoveryCheckpointTestSupport.entries()))
                        .join()) {
            byte[] bytes = RecoveryCheckpointTestSupport.collect(written.stagingFile());
            assertThat(new String(bytes, 0, 4, StandardCharsets.US_ASCII)).isEqualTo("NRC1");
            assertThat(new String(
                            bytes,
                            bytes.length - RecoveryCheckpointFormatV1.FOOTER_BYTES,
                            4,
                            StandardCharsets.US_ASCII))
                    .isEqualTo("NRF1");
            assertThat(written.objectLength()).isEqualTo(bytes.length);
            assertThat(written.objectLength()).isEqualTo(861);
            assertThat(written.contentSha256().value())
                    .isEqualTo("3c1b07ea94e74da1a7b381725b200eb9b66e26daaf16d1b01fca3ac5eb1d770b");
            assertThat(written.bodySha256()).isNotEqualTo(written.contentSha256());
            assertThat(written.objectKey().value())
                    .isEqualTo("test-cluster/recovery-checkpoints/v1/s-recovery-test/"
                            + "0000000000000000007-"
                            + written.contentSha256().value()
                            + "-"
                            + "a".repeat(26)
                            + ".nrc");

            RecoveryCheckpointTestSupport.upload(objectStore, written);
            DefaultRecoveryCheckpointCodecV1 codec = new DefaultRecoveryCheckpointCodecV1(
                    objectStore, staging, Runnable::run, RecoveryCheckpointTestSupport.verifier());
            RecoveryCheckpointObject opened = codec.openAndVerify(
                            written.objectKey(),
                            written.objectLength(),
                            written.contentSha256(),
                            RecoveryCheckpointTestSupport.TIMEOUT)
                    .join();
            assertThat(opened.header()).isEqualTo(RecoveryCheckpointTestSupport.request("a".repeat(26)));
            assertThat(opened.bodySha256()).isEqualTo(written.bodySha256());
            assertThat(opened.directory()).isEqualTo(written.directory());

            assertThat(codec.findPublication(
                            opened,
                            2,
                            new PublicationId("b".repeat(26)),
                            RecoveryCheckpointTestSupport.TIMEOUT)
                    .join())
                    .contains(RecoveryCheckpointTestSupport.publications().get(1));
            assertThat(codec.findPublication(
                            opened,
                            3,
                            new PublicationId("c".repeat(26)),
                            RecoveryCheckpointTestSupport.TIMEOUT)
                    .join())
                    .isEmpty();
            assertThat(codec.findCommit(
                            opened, 6, "commit-6", RecoveryCheckpointTestSupport.TIMEOUT)
                    .join())
                    .contains(RecoveryCheckpointTestSupport.entries().get(1));
            assertThat(codec.findCommit(
                            opened, 6, "wrong-commit", RecoveryCheckpointTestSupport.TIMEOUT)
                    .join())
                    .isEqualTo(Optional.empty());
        } catch (Exception failure) {
            throw new AssertionError(failure);
        }
    }
}
