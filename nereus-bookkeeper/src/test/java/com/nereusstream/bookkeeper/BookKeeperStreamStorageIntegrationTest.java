/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.AppendBatch;
import com.nereusstream.api.AppendEntry;
import com.nereusstream.api.AppendOptions;
import com.nereusstream.api.DurabilityLevel;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamCreateOptions;
import com.nereusstream.api.StreamName;
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
}
