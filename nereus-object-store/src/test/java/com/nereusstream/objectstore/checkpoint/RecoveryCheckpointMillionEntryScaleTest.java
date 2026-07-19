/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.StreamId;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.objectstore.staging.StagingFileManager;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RecoveryCheckpointMillionEntryScaleTest {
    private static final int ENTRY_COUNT =
            RecoveryCheckpointFormatV1.MAX_ENTRY_COUNT;
    private static final Duration SCALE_TIMEOUT = Duration.ofMinutes(2);

    @TempDir
    Path temporaryDirectory;

    @Test
    void streamsOneMillionEntriesAndReadsSparseDirectoryBoundaries()
            throws Exception {
        RecoveryCheckpointWriteRequest request = request();
        RecoveryCheckpointPublication publication =
                RecoveryCheckpointTestSupport.publication(
                        1, "p".repeat(26), 0, ENTRY_COUNT);
        RecoveryCheckpointTestSupport.TrackingPublisher<RecoveryCheckpointPublication>
                publications = new RecoveryCheckpointTestSupport.TrackingPublisher<>(
                        List.of(publication));
        GeneratedEntryPublisher entries = new GeneratedEntryPublisher();

        try (LocalFileObjectStore objectStore = new LocalFileObjectStore(
                        temporaryDirectory.resolve("objects"));
                StagingFileManager staging = RecoveryCheckpointTestSupport.staging(
                        temporaryDirectory, 384L << 20)) {
            DefaultRecoveryCheckpointCodecV1 codec =
                    new DefaultRecoveryCheckpointCodecV1(
                            objectStore,
                            staging,
                            Runnable::run,
                            RecoveryCheckpointTestSupport.verifier());
            try (RecoveryCheckpointWriteResult written = codec.write(
                            request, publications, entries)
                    .join()) {
                assertThat(publications.maximumRequest()).isEqualTo(1);
                assertThat(publications.requestCalls()).isOne();
                assertThat(entries.maximumRequest()).isOne();
                assertThat(entries.requestCalls()).isEqualTo(ENTRY_COUNT);
                assertThat(entries.emitted()).isEqualTo(ENTRY_COUNT);
                assertThat(entries.cancelled()).isFalse();
                assertThat(written.objectLength())
                        .isPositive()
                        .isLessThanOrEqualTo(
                                RecoveryCheckpointFormatV1.MAX_OBJECT_BYTES);
                assertThat(written.directory().publicationDirectoryLength())
                        .isEqualTo(Integer.BYTES + Integer.BYTES + Long.BYTES);
                int commitAnchors = (ENTRY_COUNT
                                + RecoveryCheckpointFormatV1.COMMIT_DIRECTORY_STRIDE
                                - 1)
                        / RecoveryCheckpointFormatV1.COMMIT_DIRECTORY_STRIDE;
                assertThat(commitAnchors).isEqualTo(3_907);
                assertThat(written.directory().commitDirectoryLength())
                        .isEqualTo(Integer.BYTES * 2L
                                + commitAnchors * Long.BYTES * 3L);
                assertThat(written.directory().publicationDirectoryLength()
                                + written.directory().commitDirectoryLength())
                        .isLessThanOrEqualTo(
                                RecoveryCheckpointFormatV1.MAX_DIRECTORY_BYTES);
                assertThat(staging.reservedBytes())
                        .isPositive()
                        .isLessThanOrEqualTo(staging.maxStagingBytes());

                objectStore.putObject(
                                written.objectKey(),
                                written.stagingFile(),
                                new PutObjectOptions(
                                        RecoveryCheckpointFormatV1.CONTENT_TYPE,
                                        written.storageCrc32c(),
                                        true,
                                        Map.of("nereus-format", "NRC1"),
                                        SCALE_TIMEOUT))
                        .join();
                RecoveryCheckpointObject opened = codec.openAndVerify(
                                written.objectKey(),
                                written.objectLength(),
                                written.contentSha256(),
                                SCALE_TIMEOUT)
                        .join();

                assertThat(opened.header()).isEqualTo(request);
                assertThat(opened.directory()).isEqualTo(written.directory());
                assertEntry(codec.findCommit(
                                opened, 1, commitId(1), SCALE_TIMEOUT)
                        .join()
                        .orElseThrow(), 1);
                assertEntry(codec.findCommitCoveringOffset(
                                opened, ENTRY_COUNT / 2L, SCALE_TIMEOUT)
                        .join()
                        .orElseThrow(), ENTRY_COUNT / 2L + 1);
                assertEntry(codec.findCommit(
                                opened,
                                ENTRY_COUNT,
                                commitId(ENTRY_COUNT),
                                SCALE_TIMEOUT)
                        .join()
                        .orElseThrow(), ENTRY_COUNT);
                assertThat(codec.findCommitCoveringOffset(
                                opened, ENTRY_COUNT, SCALE_TIMEOUT)
                        .join())
                        .isEmpty();
            }
            assertThat(staging.reservedBytes()).isZero();
        }
    }

    private static RecoveryCheckpointWriteRequest request() {
        return new RecoveryCheckpointWriteRequest(
                "test-cluster",
                new StreamId("s-recovery-test"),
                1,
                "m".repeat(26),
                new OffsetRange(0, ENTRY_COUNT),
                1,
                ENTRY_COUNT,
                0,
                ENTRY_COUNT,
                commitId(1),
                commitId(ENTRY_COUNT),
                commitId(ENTRY_COUNT),
                ENTRY_COUNT,
                RecoveryCheckpointTestSupport.sha256("projection"),
                ENTRY_COUNT,
                1);
    }

    private static void assertEntry(
            RecoveryCheckpointEntry entry,
            long version) {
        assertThat(entry.commitVersion()).isEqualTo(version);
        assertThat(entry.range()).isEqualTo(
                new OffsetRange(version - 1, version));
        assertThat(entry.cumulativeSizeAtEnd()).isEqualTo(version);
        assertThat(entry.commitId()).isEqualTo(commitId(version));
        assertThat(entry.previousCommitId()).isEqualTo(previousCommitId(version));
        assertThat(entry.coveringPublicationIndexes()).containsExactly(0);
        assertThat(RecoveryCheckpointTestSupport.text(
                        entry.canonicalCommitRecord()))
                .isEqualTo("commit:" + version + ":" + commitId(version));
    }

    private static RecoveryCheckpointEntry entry(long version) {
        return RecoveryCheckpointTestSupport.entry(
                version,
                version - 1,
                version,
                version,
                commitId(version),
                previousCommitId(version),
                List.of(0));
    }

    private static String commitId(long version) {
        return "commit-" + version;
    }

    private static String previousCommitId(long version) {
        return version == 1 ? "genesis" : commitId(version - 1);
    }

    private static final class GeneratedEntryPublisher
            implements Flow.Publisher<RecoveryCheckpointEntry> {
        private final AtomicBoolean subscribed = new AtomicBoolean();
        private final AtomicBoolean cancelled = new AtomicBoolean();
        private final AtomicLong maximumRequest = new AtomicLong();
        private final AtomicLong requestCalls = new AtomicLong();
        private final AtomicLong emitted = new AtomicLong();

        @Override
        public void subscribe(
                Flow.Subscriber<? super RecoveryCheckpointEntry> subscriber) {
            if (!subscribed.compareAndSet(false, true)) {
                subscriber.onSubscribe(new EmptySubscription());
                subscriber.onError(new AssertionError(
                        "generated checkpoint entries subscribed more than once"));
                return;
            }
            subscriber.onSubscribe(new Flow.Subscription() {
                private long nextVersion = 1;
                private boolean terminal;

                @Override
                public void request(long count) {
                    if (terminal) {
                        return;
                    }
                    requestCalls.incrementAndGet();
                    maximumRequest.accumulateAndGet(count, Math::max);
                    if (count != 1) {
                        terminal = true;
                        subscriber.onError(new AssertionError(
                                "checkpoint codec demand must be exactly one entry"));
                        return;
                    }
                    long version = nextVersion++;
                    subscriber.onNext(entry(version));
                    emitted.incrementAndGet();
                    if (version == ENTRY_COUNT) {
                        terminal = true;
                        subscriber.onComplete();
                    }
                }

                @Override
                public void cancel() {
                    terminal = true;
                    cancelled.set(true);
                }
            });
        }

        long maximumRequest() {
            return maximumRequest.get();
        }

        long requestCalls() {
            return requestCalls.get();
        }

        long emitted() {
            return emitted.get();
        }

        boolean cancelled() {
            return cancelled.get();
        }
    }

    private static final class EmptySubscription
            implements Flow.Subscription {
        @Override
        public void request(long count) {
        }

        @Override
        public void cancel() {
        }
    }
}
