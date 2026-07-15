/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.PublicationId;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ReadResult;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.ResolvedRange;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.core.physical.ObjectReadLease;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.objectstore.wal.WalReadResult;
import com.nereusstream.objectstore.wal.WalSliceReadStats;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class PinnedReadCoordinatorTest {
    private static final StreamId STREAM = new StreamId("stream");

    @Test
    void keepsLeaseUntilReaderAndCleanupComplete() {
        ObjectSliceReadTarget target = ReadTargetReaderRegistryTest.target(
                ObjectType.STREAM_COMPACTED_OBJECT, "NEREUS_COMPACTED_PARQUET_V1");
        GenerationReadCandidate candidate = candidate(target, 3);
        TestLease lease = new TestLease(target);
        CompletableFuture<WalReadResult> readerCompletion = new CompletableFuture<>();
        TestReader reader = new TestReader(ReadTargetReaderKey.from(target), readerCompletion);
        ReadCoordinator coordinator = coordinator(
                (stream, offset, view, deadline, repair, excluded) -> CompletableFuture.completedFuture(
                        Optional.of(new PinnedResolvedRange(candidate, lease))),
                new ReadTargetReaderRegistry(List.of(reader)),
                GenerationReadFailureHandler.noOp());

        CompletableFuture<ReadResult> read = coordinator.read(STREAM, 0, options());

        assertThat(reader.calls).hasValue(1);
        assertThat(lease.releases).hasValue(0);
        assertThat(read).isNotDone();

        readerCompletion.complete(result(target, (byte) 7));

        assertThat(read.join().nextOffset()).isEqualTo(1);
        assertThat(lease.releases).hasValue(1);
        coordinator.close();
    }

    @Test
    void objectCorruptionExcludesFailedCandidateAndFallsBackOnlyWithinTheSameView() {
        ObjectSliceReadTarget higherTarget = ReadTargetReaderRegistryTest.target(
                ObjectType.STREAM_COMPACTED_OBJECT, "NEREUS_COMPACTED_PARQUET_V1");
        ObjectSliceReadTarget zeroTarget = ReadTargetReaderRegistryTest.target(
                ObjectType.MULTI_STREAM_WAL_OBJECT, "WAL_OBJECT_V1");
        GenerationReadCandidate higher = candidate(higherTarget, 5);
        GenerationReadCandidate zero = candidate(zeroTarget, 0);
        List<ReadView> views = new ArrayList<>();
        List<Set<GenerationReadCandidate>> exclusions = new ArrayList<>();
        AtomicInteger handled = new AtomicInteger();
        TestReader broken = new TestReader(
                ReadTargetReaderKey.from(higherTarget),
                CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.OBJECT_CHECKSUM_MISMATCH, false, "corrupt")));
        TestReader healthy = new TestReader(
                ReadTargetReaderKey.from(zeroTarget),
                CompletableFuture.completedFuture(result(zeroTarget, (byte) 9)));
        ReadCoordinator coordinator = coordinator(
                (stream, offset, view, deadline, repair, excluded) -> {
                    views.add(view);
                    exclusions.add(Set.copyOf(excluded));
                    GenerationReadCandidate selected = excluded.contains(higher) ? zero : higher;
                    ObjectSliceReadTarget target = selected == higher ? higherTarget : zeroTarget;
                    return CompletableFuture.completedFuture(Optional.of(
                            new PinnedResolvedRange(selected, new TestLease(target))));
                },
                new ReadTargetReaderRegistry(List.of(broken, healthy)),
                (stream, candidate, failure) -> {
                    handled.incrementAndGet();
                    assertThat(stream).isEqualTo(STREAM);
                    assertThat(candidate).isEqualTo(higher);
                    return CompletableFuture.completedFuture(null);
                });

        ReadResult result = coordinator.read(STREAM, 0, options()).join();

        assertThat(result.batches().getFirst().payload()).containsExactly(9);
        assertThat(views).containsExactly(ReadView.COMMITTED, ReadView.COMMITTED);
        assertThat(exclusions.getFirst()).isEmpty();
        assertThat(exclusions.get(1)).containsExactly(higher);
        assertThat(handled).hasValue(1);
        assertThat(broken.calls).hasValue(1);
        assertThat(healthy.calls).hasValue(1);
        coordinator.close();
    }

    @Test
    void retriesRetriableTransientFailureOnTheSameCandidateBeforeFallback() {
        ObjectSliceReadTarget target = ReadTargetReaderRegistryTest.target(
                ObjectType.STREAM_COMPACTED_OBJECT, "NEREUS_COMPACTED_PARQUET_V1");
        GenerationReadCandidate candidate = candidate(target, 5);
        AtomicInteger resolves = new AtomicInteger();
        AtomicInteger handled = new AtomicInteger();
        SequencedReader reader = new SequencedReader(
                ReadTargetReaderKey.from(target),
                List.of(
                        CompletableFuture.failedFuture(new NereusException(
                                ErrorCode.OBJECT_READ_FAILED, true, "throttled-1")),
                        CompletableFuture.failedFuture(new NereusException(
                                ErrorCode.OBJECT_READ_FAILED, true, "throttled-2")),
                        CompletableFuture.completedFuture(result(target, (byte) 11))));
        ReadCoordinator coordinator = coordinator(
                (stream, offset, view, deadline, repair, excluded) -> {
                    resolves.incrementAndGet();
                    assertThat(excluded).isEmpty();
                    return CompletableFuture.completedFuture(Optional.of(
                            new PinnedResolvedRange(candidate, new TestLease(target))));
                },
                new ReadTargetReaderRegistry(List.of(reader)),
                (stream, failed, failure) -> {
                    handled.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                });

        ReadResult value = coordinator.read(STREAM, 0, options()).join();

        assertThat(value.batches().getFirst().payload()).containsExactly(11);
        assertThat(resolves).hasValue(3);
        assertThat(reader.calls).hasValue(3);
        assertThat(handled).hasValue(0);
        coordinator.close();
    }

    @Test
    void fallsBackOnlyAfterTheConfiguredTransientRetryThreshold() {
        ObjectSliceReadTarget higherTarget = ReadTargetReaderRegistryTest.target(
                ObjectType.STREAM_COMPACTED_OBJECT, "NEREUS_COMPACTED_PARQUET_V1");
        ObjectSliceReadTarget zeroTarget = ReadTargetReaderRegistryTest.target(
                ObjectType.MULTI_STREAM_WAL_OBJECT, "WAL_OBJECT_V1");
        GenerationReadCandidate higher = candidate(higherTarget, 5);
        GenerationReadCandidate zero = candidate(zeroTarget, 0);
        AtomicInteger resolves = new AtomicInteger();
        AtomicInteger handled = new AtomicInteger();
        TestReader transientFailure = new TestReader(
                ReadTargetReaderKey.from(higherTarget),
                CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.OBJECT_READ_FAILED, true, "throttled")));
        TestReader healthy = new TestReader(
                ReadTargetReaderKey.from(zeroTarget),
                CompletableFuture.completedFuture(result(zeroTarget, (byte) 12)));
        ReadCoordinator coordinator = coordinator(
                (stream, offset, view, deadline, repair, excluded) -> {
                    resolves.incrementAndGet();
                    GenerationReadCandidate selected = excluded.contains(higher) ? zero : higher;
                    ObjectSliceReadTarget target = selected == higher ? higherTarget : zeroTarget;
                    return CompletableFuture.completedFuture(Optional.of(
                            new PinnedResolvedRange(selected, new TestLease(target))));
                },
                new ReadTargetReaderRegistry(List.of(transientFailure, healthy)),
                (stream, failed, failure) -> {
                    handled.incrementAndGet();
                    assertThat(failed).isEqualTo(higher);
                    return CompletableFuture.completedFuture(null);
                },
                new GenerationReadRetryPolicy(1));

        ReadResult value = coordinator.read(STREAM, 0, options()).join();

        assertThat(value.batches().getFirst().payload()).containsExactly(12);
        assertThat(resolves).hasValue(3);
        assertThat(transientFailure.calls).hasValue(2);
        assertThat(healthy.calls).hasValue(1);
        assertThat(handled).hasValue(1);
        coordinator.close();
    }

    private static ReadCoordinator coordinator(
            ReadCoordinator.PinnedGenerationResolver generationResolver,
            ReadTargetReaderRegistry readers,
            GenerationReadFailureHandler failureHandler) {
        return coordinator(
                generationResolver,
                readers,
                failureHandler,
                GenerationReadRetryPolicy.defaults());
    }

    private static ReadCoordinator coordinator(
            ReadCoordinator.PinnedGenerationResolver generationResolver,
            ReadTargetReaderRegistry readers,
            GenerationReadFailureHandler failureHandler,
            GenerationReadRetryPolicy retryPolicy) {
        StreamStorageConfig config = StreamStorageConfig.defaults("cluster", "writer");
        OxiaMetadataStore metadata = (OxiaMetadataStore) Proxy.newProxyInstance(
                PinnedReadCoordinatorTest.class.getClassLoader(),
                new Class<?>[] {OxiaMetadataStore.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "close" -> null;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        ReadResolver legacy = new ReadResolver(
                config, metadata, Clock.systemUTC(), ReadMetricsObserver.noop(), Runnable::run);
        return new ReadCoordinator(
                config,
                legacy,
                generationResolver,
                readers,
                failureHandler,
                retryPolicy,
                ReadMetricsObserver.noop(),
                Runnable::run);
    }

    private static GenerationReadCandidate candidate(
            ObjectSliceReadTarget target,
            long generation) {
        ResolvedRange range = new ResolvedRange(
                new OffsetRange(0, 1),
                generation,
                target,
                PayloadFormat.OPAQUE_RECORD_BATCH,
                1,
                1,
                1,
                List.of(),
                Optional.empty(),
                1);
        return new GenerationReadCandidate(
                ReadView.COMMITTED,
                range,
                "/index/1/" + generation,
                generation + 1,
                new Checksum(ChecksumType.SHA256, String.format("%064x", generation + 1)),
                generation == 0,
                generation == 0
                        ? Optional.empty()
                        : Optional.of(new PublicationId("a".repeat(26))));
    }

    private static ReadOptions options() {
        return new ReadOptions(10, 1024, ReadIsolation.COMMITTED, Duration.ofSeconds(5));
    }

    private static WalReadResult result(
            ObjectSliceReadTarget target,
            byte value) {
        ReadBatch batch = new ReadBatch(
                new OffsetRange(0, 1),
                PayloadFormat.OPAQUE_RECORD_BATCH,
                new byte[] {value},
                List.of(),
                target.entryIndexRef(),
                Optional.empty(),
                target.objectId(),
                target.objectOffset(),
                target.objectLength());
        WalSliceReadStats stats = new WalSliceReadStats(
                target.objectId(),
                target.objectOffset(),
                target.objectLength(),
                target.entryIndexRef().length(),
                1);
        return new WalReadResult(List.of(batch), List.of(stats));
    }

    private static final class TestReader implements ReadTargetReader {
        private final ReadTargetReaderKey key;
        private final CompletableFuture<WalReadResult> result;
        private final AtomicInteger calls = new AtomicInteger();

        private TestReader(
                ReadTargetReaderKey key,
                CompletableFuture<WalReadResult> result) {
            this.key = key;
            this.result = result;
        }

        @Override
        public ReadTargetReaderKey key() {
            return key;
        }

        @Override
        public long reservationBytes(ResolvedRange range) {
            return 1;
        }

        @Override
        public CompletableFuture<WalReadResult> readWithStats(
                long startOffset,
                List<ResolvedRange> ranges,
                ReadOptions options) {
            calls.incrementAndGet();
            return result;
        }
    }

    private static final class SequencedReader implements ReadTargetReader {
        private final ReadTargetReaderKey key;
        private final List<CompletableFuture<WalReadResult>> results;
        private final AtomicInteger calls = new AtomicInteger();

        private SequencedReader(
                ReadTargetReaderKey key,
                List<CompletableFuture<WalReadResult>> results) {
            this.key = key;
            this.results = List.copyOf(results);
        }

        @Override
        public ReadTargetReaderKey key() {
            return key;
        }

        @Override
        public long reservationBytes(ResolvedRange range) {
            return 1;
        }

        @Override
        public CompletableFuture<WalReadResult> readWithStats(
                long startOffset,
                List<ResolvedRange> ranges,
                ReadOptions options) {
            int index = calls.getAndIncrement();
            if (index >= results.size()) {
                return CompletableFuture.failedFuture(new AssertionError(
                        "sequenced reader received too many calls"));
            }
            return results.get(index);
        }
    }

    private static final class TestLease implements ObjectReadLease {
        private final PhysicalObjectIdentity identity;
        private final AtomicInteger releases = new AtomicInteger();

        private TestLease(ObjectSliceReadTarget target) {
            this.identity = PhysicalObjectIdentity.create(
                    target.objectKey(),
                    Optional.of(target.objectId()),
                    target.objectType() == ObjectType.MULTI_STREAM_WAL_OBJECT
                            ? PhysicalObjectKind.OBJECT_WAL
                            : PhysicalObjectKind.COMMITTED_COMPACTED,
                    target.objectLength(),
                    new Checksum(ChecksumType.CRC32C, "00000002"),
                    Optional.empty(),
                    Optional.empty());
        }

        @Override
        public PhysicalObjectIdentity object() {
            return identity;
        }

        @Override
        public String leaseId() {
            return "lease";
        }

        @Override
        public long maximumReadDeadlineMillis() {
            return Long.MAX_VALUE;
        }

        @Override
        public CompletableFuture<Void> release() {
            releases.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public boolean isReleased() {
            return releases.get() > 0;
        }
    }
}
