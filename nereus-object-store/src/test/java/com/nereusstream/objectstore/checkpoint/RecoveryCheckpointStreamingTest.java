/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.objectstore.staging.StagingFileManager;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RecoveryCheckpointStreamingTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void requestsExactlyOnePublicationAndCommitAtATime() throws Exception {
        RecoveryCheckpointTestSupport.TrackingPublisher<RecoveryCheckpointPublication> publications =
                new RecoveryCheckpointTestSupport.TrackingPublisher<>(
                        RecoveryCheckpointTestSupport.publications());
        RecoveryCheckpointTestSupport.TrackingPublisher<RecoveryCheckpointEntry> entries =
                new RecoveryCheckpointTestSupport.TrackingPublisher<>(
                        RecoveryCheckpointTestSupport.entries());
        try (LocalFileObjectStore objectStore = new LocalFileObjectStore(
                        temporaryDirectory.resolve("objects"));
                StagingFileManager staging = RecoveryCheckpointTestSupport.staging(
                        temporaryDirectory, 64L << 20);
                RecoveryCheckpointWriteResult ignored = new DefaultRecoveryCheckpointCodecV1(
                                objectStore,
                                staging,
                                Runnable::run,
                                RecoveryCheckpointTestSupport.verifier())
                        .write(
                                RecoveryCheckpointTestSupport.request("a".repeat(26)),
                                publications,
                                entries)
                        .join()) {
            assertThat(publications.maximumRequest()).isEqualTo(1);
            assertThat(publications.requestCalls())
                    .isEqualTo(RecoveryCheckpointTestSupport.publications().size());
            assertThat(publications.cancelled()).isFalse();
            assertThat(entries.maximumRequest()).isEqualTo(1);
            assertThat(entries.requestCalls()).isEqualTo(RecoveryCheckpointTestSupport.entries().size());
            assertThat(entries.cancelled()).isFalse();
        }
    }

    @Test
    void cancellationOwnsAllPartialMainAndSpillFiles() throws Exception {
        NeverPublisher<RecoveryCheckpointPublication> publications = new NeverPublisher<>();
        try (LocalFileObjectStore objectStore = new LocalFileObjectStore(
                        temporaryDirectory.resolve("objects-cancel"));
                StagingFileManager staging = RecoveryCheckpointTestSupport.staging(
                        temporaryDirectory.resolve("cancel"), 64L << 20)) {
            CompletableFuture<RecoveryCheckpointWriteResult> result = new DefaultRecoveryCheckpointCodecV1(
                            objectStore,
                            staging,
                            Runnable::run,
                            RecoveryCheckpointTestSupport.verifier())
                    .write(
                            RecoveryCheckpointTestSupport.request("a".repeat(26)),
                            publications,
                            RecoveryCheckpointTestSupport.publisher(
                                    RecoveryCheckpointTestSupport.entries()));
            assertThat(publications.subscribed()).isTrue();
            assertThat(result.cancel(true)).isTrue();
            assertThat(publications.cancelled()).isTrue();
            assertThat(staging.reservedBytes()).isZero();
        }
    }

    @Test
    void prematurePublisherCompletionFailsAndReleasesAllStaging() throws Exception {
        try (LocalFileObjectStore objectStore = new LocalFileObjectStore(
                        temporaryDirectory.resolve("objects-short"));
                StagingFileManager staging = RecoveryCheckpointTestSupport.staging(
                        temporaryDirectory.resolve("short"), 64L << 20)) {
            CompletableFuture<RecoveryCheckpointWriteResult> result = new DefaultRecoveryCheckpointCodecV1(
                            objectStore,
                            staging,
                            Runnable::run,
                            RecoveryCheckpointTestSupport.verifier())
                    .write(
                            RecoveryCheckpointTestSupport.request("a".repeat(26)),
                            subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
                                @Override
                                public void request(long count) {
                                    subscriber.onComplete();
                                }

                                @Override
                                public void cancel() {
                                }
                            }),
                            RecoveryCheckpointTestSupport.publisher(
                                    RecoveryCheckpointTestSupport.entries()));
            assertThatThrownBy(result::join)
                    .hasRootCauseInstanceOf(RecoveryCheckpointFormatException.class);
            assertThat(staging.reservedBytes()).isZero();
        }
    }

    private static final class NeverPublisher<T> implements Flow.Publisher<T> {
        private final AtomicBoolean subscribed = new AtomicBoolean();
        private final AtomicBoolean cancelled = new AtomicBoolean();

        @Override
        public void subscribe(Flow.Subscriber<? super T> subscriber) {
            subscribed.set(true);
            subscriber.onSubscribe(new Flow.Subscription() {
                @Override
                public void request(long count) {
                }

                @Override
                public void cancel() {
                    cancelled.set(true);
                }
            });
        }

        boolean subscribed() {
            return subscribed.get();
        }

        boolean cancelled() {
            return cancelled.get();
        }
    }
}
