/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.AppendBatch;
import com.nereusstream.api.AppendEntry;
import com.nereusstream.api.AppendOptions;
import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.AppendRecoveryOptions;
import com.nereusstream.api.DurabilityLevel;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamCreateOptions;
import com.nereusstream.api.StreamName;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import com.nereusstream.core.DefaultStreamStorage;
import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.core.append.AppendAdmissionGuard;
import com.nereusstream.core.read.ReadMetricsObserver;
import com.nereusstream.core.recovery.MetadataAppendRecoverySearcher;
import com.nereusstream.core.trim.TrimMetricsObserver;
import com.nereusstream.metadata.oxia.testing.FakeOxiaMetadataStore;
import com.nereusstream.metadata.oxia.BookKeeperMetadataStoreConfig;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class BookKeeperStreamStorageIntegrationTest {
    @Test
    void strictBkOnlyAppendAndColdReadTraverseTheProviderNeutralL0Pipeline() {
        try (BookKeeperPrimaryWalAppenderTest.Runtime runtime = new BookKeeperPrimaryWalAppenderTest.Runtime()) {
            FakeOxiaMetadataStore l0 = new FakeOxiaMetadataStore(
                    () -> 1_000L,
                    runtime.metadata,
                    new BookKeeperMetadataStoreConfig(
                            runtime.configuration.maxAppendRangesPerLedger(),
                            runtime.configuration.protectionSlotsPerRange(),
                            runtime.configuration.maxReaderLeasesPerLedger(),
                            runtime.configuration.maxUncertainAllocations()));
            try {
            StreamStorageConfig config = StreamStorageConfig.defaults(
                    BookKeeperPrimaryWalAppenderTest.CLUSTER, "writer-1");
            try (BookKeeperWalRuntime bkRuntime = new BookKeeperWalRuntime(
                            runtime.appender, runtime.reader, runtime.references);
                    DefaultStreamStorage storage = bkRuntime.newGenerationZeroStorage(
                            config,
                            l0,
                            new MetadataAppendRecoverySearcher(config.cluster(), l0),
                            AppendAdmissionGuard.noOp(),
                            BookKeeperPrimaryWalAppenderTest.CLOCK,
                            Runnable::run,
                            ReadMetricsObserver.noop(),
                            TrimMetricsObserver.noop())) {
                var stream = storage.createOrGetStream(
                        new StreamName("persistent://tenant/namespace/bk-only"),
                        new StreamCreateOptions(StorageProfile.BOOKKEEPER_WAL_ONLY, Map.of()))
                        .join();
                AppendBatch batch = new AppendBatch(
                        PayloadFormat.OPAQUE_RECORD_BATCH,
                        List.of(
                                new AppendEntry(new byte[] {1, 2}, 1, 1, Map.of()),
                                new AppendEntry(new byte[] {3}, 1, 2, Map.of())),
                        2,
                        2,
                        1,
                        2,
                        List.of(),
                        Map.of(),
                        Optional.empty());
                var appended = storage.append(
                        stream.streamId(),
                        batch,
                        new AppendOptions(
                                Optional.empty(),
                                DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED,
                                Duration.ofSeconds(10),
                                true,
                                Map.of()))
                        .join();
                assertThat(appended.range().startOffset()).isZero();
                assertThat(appended.range().endOffset()).isEqualTo(2);

                var read = storage.read(
                        stream.streamId(),
                        0,
                        new ReadOptions(10, 1024, ReadIsolation.COMMITTED, Duration.ofSeconds(10)))
                        .join();
                assertThat(read.nextOffset()).isEqualTo(2);
                assertThat(read.batches()).hasSize(2);
                assertThat(read.batches().get(0).payload()).containsExactly(1, 2);
                assertThat(read.batches().get(1).payload()).containsExactly(3);
                assertThat(read.batches()).allSatisfy(returned ->
                        assertThat(returned.source().target().type())
                                .isEqualTo(com.nereusstream.api.target.ReadTargetType.BOOKKEEPER_ENTRY_RANGE));
            }
            } finally {
                l0.close();
            }
        }
    }

    @Test
    void reachableHeadRecoveryRepairsGenerationZeroWithoutRewritingBookKeeper() {
        try (BookKeeperPrimaryWalAppenderTest.Runtime runtime = new BookKeeperPrimaryWalAppenderTest.Runtime()) {
            FakeOxiaMetadataStore l0 = l0(runtime);
            try (DefaultStreamStorage storage = storage(runtime, l0)) {
                var stream = storage.createOrGetStream(
                                new StreamName("persistent://tenant/namespace/bk-repair"),
                                new StreamCreateOptions(StorageProfile.BOOKKEEPER_WAL_ONLY, Map.of()))
                        .join();
                l0.failNext(FakeOxiaMetadataStore.FailurePoint.AFTER_HEAD_CAS_BEFORE_DERIVED_INDEX);

                CompletionException appendError = org.junit.jupiter.api.Assertions.assertThrows(
                        CompletionException.class,
                        () -> storage.append(
                                        stream.streamId(),
                                        batch(new byte[] {1, 2}, new byte[] {3}),
                                        strictOptions())
                                .join());
                NereusException failure = (NereusException) appendError.getCause();
                assertThat(failure.appendOutcome()).contains(AppendOutcome.KNOWN_COMMITTED);
                assertThat(failure.appendAttemptId()).isPresent();
                assertThat(l0.scanOffsetIndex(
                                BookKeeperPrimaryWalAppenderTest.CLUSTER,
                                stream.streamId(),
                                0,
                                10)
                        .join()).isEmpty();
                int writesBeforeRecovery = runtime.operations.writeCalls();

                var recovered = storage.recoverAppend(
                                stream.streamId(),
                                failure.appendAttemptId().orElseThrow(),
                                new AppendRecoveryOptions(Duration.ofSeconds(2)))
                        .join();

                assertThat(recovered.range().startOffset()).isZero();
                assertThat(recovered.range().endOffset()).isEqualTo(2);
                assertThat(recovered.readTarget()).isInstanceOf(BookKeeperEntryRangeReadTarget.class);
                assertThat(((BookKeeperEntryRangeReadTarget) recovered.readTarget()).entryCount()).isEqualTo(2);
                assertThat(runtime.operations.writeCalls()).isEqualTo(writesBeforeRecovery);
                assertThat(l0.scanOffsetIndex(
                                BookKeeperPrimaryWalAppenderTest.CLUSTER,
                                stream.streamId(),
                                0,
                                10)
                        .join()).singleElement().satisfies(index ->
                                assertThat(index.readTarget()).isEqualTo(recovered.readTarget()));
                assertThat(storage.read(
                                stream.streamId(),
                                0,
                                new ReadOptions(10, 1024, ReadIsolation.COMMITTED, Duration.ofSeconds(2)))
                        .join().batches())
                        .extracting(batch -> batch.payload())
                        .containsExactly(new byte[] {1, 2}, new byte[] {3});
            } finally {
                l0.close();
            }
        }
    }

    @Test
    void unsupportedProfileAndOversizeBatchReachNoBookKeeperOperation() {
        try (BookKeeperPrimaryWalAppenderTest.Runtime runtime = new BookKeeperPrimaryWalAppenderTest.Runtime()) {
            FakeOxiaMetadataStore l0 = l0(runtime);
            try (DefaultStreamStorage storage = storage(runtime, l0)) {
                var unsupported = storage.createOrGetStream(
                                new StreamName("persistent://tenant/namespace/bk-future-profile"),
                                new StreamCreateOptions(StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT, Map.of()))
                        .join();
                assertThatThrownBy(() -> storage.append(
                                unsupported.streamId(), batch(new byte[] {1}), strictOptions()).join())
                        .hasCauseInstanceOf(NereusException.class);
                assertThat(runtime.operations.providerCalls()).isZero();

                var admitted = storage.createOrGetStream(
                                new StreamName("persistent://tenant/namespace/bk-oversize"),
                                new StreamCreateOptions(StorageProfile.BOOKKEEPER_WAL_ONLY, Map.of()))
                        .join();
                assertThatThrownBy(() -> storage.append(
                                admitted.streamId(),
                                batchWithEntryCount(Math.toIntExact(
                                        runtime.configuration.maxEntriesPerLedger() + 1)),
                                strictOptions()).join())
                        .hasCauseInstanceOf(NereusException.class);
                assertThat(runtime.operations.providerCalls()).isZero();
            } finally {
                l0.close();
            }
        }
    }

    private static FakeOxiaMetadataStore l0(BookKeeperPrimaryWalAppenderTest.Runtime runtime) {
        return new FakeOxiaMetadataStore(
                () -> 1_000L,
                runtime.metadata,
                new BookKeeperMetadataStoreConfig(
                        runtime.configuration.maxAppendRangesPerLedger(),
                        runtime.configuration.protectionSlotsPerRange(),
                        runtime.configuration.maxReaderLeasesPerLedger(),
                        runtime.configuration.maxUncertainAllocations()));
    }

    private static DefaultStreamStorage storage(
            BookKeeperPrimaryWalAppenderTest.Runtime runtime,
            FakeOxiaMetadataStore l0) {
        StreamStorageConfig config = StreamStorageConfig.defaults(
                BookKeeperPrimaryWalAppenderTest.CLUSTER, "writer-1");
        BookKeeperWalRuntime bkRuntime = new BookKeeperWalRuntime(
                runtime.appender, runtime.reader, runtime.references);
        return bkRuntime.newGenerationZeroStorage(
                config,
                l0,
                new MetadataAppendRecoverySearcher(config.cluster(), l0),
                AppendAdmissionGuard.noOp(),
                BookKeeperPrimaryWalAppenderTest.CLOCK,
                Runnable::run,
                ReadMetricsObserver.noop(),
                TrimMetricsObserver.noop());
    }

    private static AppendBatch batch(byte[]... payloads) {
        List<AppendEntry> entries = java.util.stream.IntStream.range(0, payloads.length)
                .mapToObj(index -> new AppendEntry(payloads[index], 1, index + 1L, Map.of()))
                .toList();
        return new AppendBatch(
                PayloadFormat.OPAQUE_RECORD_BATCH,
                entries,
                entries.size(),
                entries.size(),
                1,
                entries.size(),
                List.of(),
                Map.of(),
                Optional.empty());
    }

    private static AppendBatch batchWithEntryCount(int entryCount) {
        List<AppendEntry> entries = java.util.stream.IntStream.range(0, entryCount)
                .mapToObj(index -> new AppendEntry(new byte[0], 1, index + 1L, Map.of()))
                .toList();
        return new AppendBatch(
                PayloadFormat.OPAQUE_RECORD_BATCH,
                entries,
                entryCount,
                entryCount,
                0,
                entryCount,
                List.of(),
                Map.of(),
                Optional.empty());
    }

    private static AppendOptions strictOptions() {
        return new AppendOptions(
                Optional.empty(),
                DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED,
                Duration.ofSeconds(10),
                true,
                Map.of());
    }
}
