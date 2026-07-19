/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.StreamId;
import com.nereusstream.objectstore.staging.StagingFileManager;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RecoveryCheckpointMergeTest {
    private static final String BASE32 = "abcdefghijklmnopqrstuvwxyz234567";

    @TempDir
    Path temporaryDirectory;

    @Test
    void mergesThirtyTwoActiveReferencesWithBoundedRemappingAndExactEntries()
            throws Exception {
        try (LocalFileObjectStore objectStore = new LocalFileObjectStore(
                        temporaryDirectory.resolve("objects"));
                StagingFileManager staging = RecoveryCheckpointTestSupport.staging(
                        temporaryDirectory, 128L << 20)) {
            DefaultRecoveryCheckpointCodecV1 codec = new DefaultRecoveryCheckpointCodecV1(
                    objectStore,
                    staging,
                    Runnable::run,
                    RecoveryCheckpointTestSupport.verifier());
            List<RecoveryCheckpointObject> sources = new ArrayList<>();
            List<RecoveryCheckpointEntry> expectedEntries = new ArrayList<>();
            for (int index = 0; index < 32; index++) {
                long version = index + 1L;
                long start = index * 2L;
                long end = start + 2;
                String commitId = "commit-" + version;
                RecoveryCheckpointPublication publication =
                        RecoveryCheckpointTestSupport.publication(
                                version, publicationId(index), start, end);
                RecoveryCheckpointEntry entry = RecoveryCheckpointTestSupport.entry(
                        version,
                        start,
                        end,
                        end,
                        commitId,
                        index == 0 ? "genesis" : "commit-" + index,
                        List.of(0));
                RecoveryCheckpointWriteRequest request = new RecoveryCheckpointWriteRequest(
                        "test-cluster",
                        new StreamId("s-recovery-test"),
                        version,
                        attemptId(index),
                        new OffsetRange(start, end),
                        version,
                        version,
                        start,
                        end,
                        commitId,
                        commitId,
                        "commit-32",
                        32,
                        RecoveryCheckpointTestSupport.sha256("projection"),
                        1,
                        1);
                try (RecoveryCheckpointWriteResult written = codec.write(
                                request,
                                RecoveryCheckpointTestSupport.publisher(List.of(publication)),
                                RecoveryCheckpointTestSupport.publisher(List.of(entry)))
                        .join()) {
                    RecoveryCheckpointTestSupport.upload(objectStore, written);
                    sources.add(codec.openAndVerify(
                                    written.objectKey(),
                                    written.objectLength(),
                                    written.contentSha256(),
                                    RecoveryCheckpointTestSupport.TIMEOUT)
                            .join());
                }
                expectedEntries.add(entry);
            }
            assertThat(staging.reservedBytes()).isZero();

            try (RecoveryCheckpointMergeResult merged = codec.merge(
                            sources,
                            33,
                            "z".repeat(26),
                            RecoveryCheckpointTestSupport.TIMEOUT)
                    .join()) {
                assertThat(merged.sourceCount()).isEqualTo(32);
                assertThat(merged.request().checkpointSequence()).isEqualTo(33);
                assertThat(merged.request().coverage()).isEqualTo(new OffsetRange(0, 64));
                assertThat(merged.request().firstCommitVersion()).isEqualTo(1);
                assertThat(merged.request().lastCommitVersion()).isEqualTo(32);
                assertThat(merged.request().expectedEntryCount()).isEqualTo(32);
                assertThat(merged.request().expectedPublicationCount()).isEqualTo(32);

                RecoveryCheckpointTestSupport.upload(objectStore, merged.object());
                RecoveryCheckpointObject opened = codec.openAndVerify(
                                merged.object().objectKey(),
                                merged.object().objectLength(),
                                merged.object().contentSha256(),
                                RecoveryCheckpointTestSupport.TIMEOUT)
                        .join();
                assertThat(opened.header()).isEqualTo(merged.request());
                assertThat(codec.findCommit(
                                opened,
                                1,
                                "commit-1",
                                RecoveryCheckpointTestSupport.TIMEOUT)
                        .join())
                        .contains(expectedEntries.get(0));
                assertThat(codec.findCommit(
                                opened,
                                17,
                                "commit-17",
                                RecoveryCheckpointTestSupport.TIMEOUT)
                        .join())
                        .contains(rewritten(expectedEntries.get(16), 16));
                assertThat(codec.findCommitCoveringOffset(
                                opened,
                                63,
                                RecoveryCheckpointTestSupport.TIMEOUT)
                        .join())
                        .contains(rewritten(expectedEntries.get(31), 31));
                RecoveryCheckpointPublicationPage page = codec.scanPublications(
                                opened,
                                OptionalInt.empty(),
                                1_000,
                                RecoveryCheckpointTestSupport.TIMEOUT)
                        .join();
                assertThat(page.values()).hasSize(32);
                assertThat(page.values())
                        .extracting(RecoveryCheckpointPublication::generation)
                        .containsExactlyElementsOf(
                                java.util.stream.LongStream.rangeClosed(1, 32)
                                        .boxed()
                                        .toList());
                assertThat(page.continuation()).isEmpty();
            }
            assertThat(staging.reservedBytes()).isZero();
        }
    }

    private static RecoveryCheckpointEntry rewritten(
            RecoveryCheckpointEntry source,
            int publicationIndex) {
        return new RecoveryCheckpointEntry(
                source.commitVersion(),
                source.range(),
                source.cumulativeSizeAtEnd(),
                source.commitId(),
                source.previousCommitId(),
                source.canonicalCommitRecord(),
                source.canonicalCommitRecordSha256(),
                List.of(publicationIndex));
    }

    private static String attemptId(int index) {
        return "a".repeat(24)
                + BASE32.charAt(index / BASE32.length())
                + BASE32.charAt(index % BASE32.length());
    }

    private static String publicationId(int index) {
        return "b".repeat(24)
                + BASE32.charAt(index / BASE32.length())
                + BASE32.charAt(index % BASE32.length());
    }
}
