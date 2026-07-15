/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PublicationId;
import com.nereusstream.api.StreamId;
import com.nereusstream.objectstore.staging.StagingFileManager;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RecoveryCheckpointSparseDirectoryTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void findsFirstMiddleAndLastCommitAcrossThreeSparseBlocks() throws Exception {
        int count = 513;
        long firstVersion = 10;
        RecoveryCheckpointWriteRequest request = new RecoveryCheckpointWriteRequest(
                "test-cluster",
                new StreamId("s-recovery-test"),
                9,
                "a".repeat(26),
                new OffsetRange(0, count),
                firstVersion,
                firstVersion + count - 1,
                0,
                count,
                "commit-10",
                "commit-522",
                "commit-600",
                600,
                RecoveryCheckpointTestSupport.sha256("projection"),
                count,
                2);
        List<RecoveryCheckpointPublication> publications = List.of(
                RecoveryCheckpointTestSupport.publication(1, "a".repeat(26), 0, count),
                RecoveryCheckpointTestSupport.publication(2, "b".repeat(26), 256, count));
        List<RecoveryCheckpointEntry> entries = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            long version = firstVersion + index;
            entries.add(RecoveryCheckpointTestSupport.entry(
                    version,
                    index,
                    index + 1L,
                    index + 1L,
                    "commit-" + version,
                    index == 0 ? "commit-9" : "commit-" + (version - 1),
                    List.of(0)));
        }
        try (LocalFileObjectStore objectStore = new LocalFileObjectStore(
                        temporaryDirectory.resolve("objects"));
                StagingFileManager staging = RecoveryCheckpointTestSupport.staging(
                        temporaryDirectory, 96L << 20);
                RecoveryCheckpointWriteResult written = new DefaultRecoveryCheckpointCodecV1(
                                objectStore,
                                staging,
                                Runnable::run,
                                RecoveryCheckpointTestSupport.verifier())
                        .write(
                                request,
                                RecoveryCheckpointTestSupport.publisher(publications),
                                RecoveryCheckpointTestSupport.publisher(entries))
                        .join()) {
            RecoveryCheckpointTestSupport.upload(objectStore, written);
            DefaultRecoveryCheckpointCodecV1 codec = new DefaultRecoveryCheckpointCodecV1(
                    objectStore, staging, Runnable::run, RecoveryCheckpointTestSupport.verifier());
            RecoveryCheckpointObject opened = codec.openAndVerify(
                            written.objectKey(),
                            written.objectLength(),
                            written.contentSha256(),
                            RecoveryCheckpointTestSupport.TIMEOUT)
                    .join();
            assertThat(codec.findCommit(opened, 10, "commit-10", RecoveryCheckpointTestSupport.TIMEOUT).join())
                    .contains(entries.get(0));
            assertThat(codec.findCommit(opened, 266, "commit-266", RecoveryCheckpointTestSupport.TIMEOUT).join())
                    .contains(entries.get(256));
            assertThat(codec.findCommit(opened, 522, "commit-522", RecoveryCheckpointTestSupport.TIMEOUT).join())
                    .contains(entries.get(512));
            assertThat(codec.findPublication(
                            opened,
                            1,
                            new PublicationId("a".repeat(26)),
                            RecoveryCheckpointTestSupport.TIMEOUT)
                    .join())
                    .contains(publications.get(0));
        }
    }
}
