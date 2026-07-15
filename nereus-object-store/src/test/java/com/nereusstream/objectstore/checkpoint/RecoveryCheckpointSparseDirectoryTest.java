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
import java.util.OptionalInt;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RecoveryCheckpointSparseDirectoryTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void findsFirstMiddleAndLastCommitAcrossThreeSparseBlocks() throws Exception {
        int count = 513;
        int coveredRecords = count * 2;
        long firstVersion = 10;
        RecoveryCheckpointWriteRequest request = new RecoveryCheckpointWriteRequest(
                "test-cluster",
                new StreamId("s-recovery-test"),
                9,
                "a".repeat(26),
                new OffsetRange(0, coveredRecords),
                firstVersion,
                firstVersion + count - 1,
                0,
                coveredRecords,
                "commit-10",
                "commit-522",
                "commit-600",
                600,
                RecoveryCheckpointTestSupport.sha256("projection"),
                count,
                2);
        List<RecoveryCheckpointPublication> publications = List.of(
                RecoveryCheckpointTestSupport.publication(1, "a".repeat(26), 0, coveredRecords),
                RecoveryCheckpointTestSupport.publication(2, "b".repeat(26), count, coveredRecords));
        List<RecoveryCheckpointEntry> entries = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            long version = firstVersion + index;
            entries.add(RecoveryCheckpointTestSupport.entry(
                    version,
                    index * 2L,
                    index * 2L + 2,
                    index * 2L + 2,
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
            assertThat(codec.findCommitCoveringOffset(
                            opened, 0, RecoveryCheckpointTestSupport.TIMEOUT).join())
                    .contains(entries.get(0));
            assertThat(codec.findCommitCoveringOffset(
                            opened, 1, RecoveryCheckpointTestSupport.TIMEOUT).join())
                    .contains(entries.get(0));
            assertThat(codec.findCommitCoveringOffset(
                            opened, 512, RecoveryCheckpointTestSupport.TIMEOUT).join())
                    .contains(entries.get(256));
            assertThat(codec.findCommitCoveringOffset(
                            opened, 513, RecoveryCheckpointTestSupport.TIMEOUT).join())
                    .contains(entries.get(256));
            assertThat(codec.findCommitCoveringOffset(
                            opened, 1024, RecoveryCheckpointTestSupport.TIMEOUT).join())
                    .contains(entries.get(512));
            assertThat(codec.findCommitCoveringOffset(
                            opened, 1025, RecoveryCheckpointTestSupport.TIMEOUT).join())
                    .contains(entries.get(512));
            assertThat(codec.findCommitCoveringOffset(
                            opened, 1026, RecoveryCheckpointTestSupport.TIMEOUT).join())
                    .isEmpty();
            assertThat(codec.findPublication(
                            opened,
                            1,
                            new PublicationId("a".repeat(26)),
                            RecoveryCheckpointTestSupport.TIMEOUT)
                    .join())
                    .contains(publications.get(0));
            RecoveryCheckpointPublicationPage first = codec.scanPublications(
                            opened,
                            OptionalInt.empty(),
                            1,
                            RecoveryCheckpointTestSupport.TIMEOUT)
                    .join();
            assertThat(first.values()).containsExactly(publications.get(0));
            assertThat(first.continuation()).hasValue(1);
            RecoveryCheckpointPublicationPage second = codec.scanPublications(
                            opened,
                            first.continuation(),
                            1,
                            RecoveryCheckpointTestSupport.TIMEOUT)
                    .join();
            assertThat(second.values()).containsExactly(publications.get(1));
            assertThat(second.continuation()).isEmpty();
        }
    }
}
