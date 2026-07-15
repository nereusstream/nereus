/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static com.nereusstream.materialization.MaterializationPlannerTestSupport.CLUSTER;
import static com.nereusstream.materialization.MaterializationPlannerTestSupport.PROJECTION;
import static com.nereusstream.materialization.MaterializationPlannerTestSupport.SCHEMAS;
import static com.nereusstream.materialization.MaterializationPlannerTestSupport.STREAM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ResolvedRange;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.physical.ObjectReadLease;
import com.nereusstream.core.physical.ObjectReadPinManager;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.core.read.ReadTargetDispatcher;
import com.nereusstream.core.read.ReadTargetReader;
import com.nereusstream.core.read.ReadTargetReaderKey;
import com.nereusstream.core.read.ReadTargetReaderRegistry;
import com.nereusstream.metadata.oxia.VersionedGenerationZeroIndex;
import com.nereusstream.objectstore.wal.WalReadResult;
import com.nereusstream.objectstore.wal.WalSliceReadStats;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ExactSourceRangeReaderTest {
    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC);

    @Test
    void readsEveryFrozenOffsetUnderDemandAndReleasesTheDurablePin() {
        VersionedGenerationZeroIndex candidate = MaterializationPlannerTestSupport.zero(
                "/index/source-2", 0, 2, 0, 100, 2);
        SourceGeneration source = source(candidate);
        TrackingPins pins = new TrackingPins();
        AtomicInteger physicalReads = new AtomicInteger();
        DefaultExactSourceRangeReader reader = reader(
                List.of(candidate), pins, physicalReads);

        ExactSourceRead exact = reader.read(source, options()).join();
        CollectingSubscriber subscriber = new CollectingSubscriber();
        exact.batches().subscribe(subscriber);
        ExactSourceReadSummary summary = exact.completion().join();

        assertThat(subscriber.terminal.join()).isNull();
        assertThat(subscriber.batches).extracting(ReadBatch::range)
                .containsExactly(new OffsetRange(0, 1), new OffsetRange(1, 2));
        assertThat(subscriber.batches).allSatisfy(batch -> {
            assertThat(batch.payload()).hasSize(50);
            assertThat(batch.projectionRef()).contains(PROJECTION);
            assertThat(batch.schemaRefs()).isEqualTo(SCHEMAS);
        });
        assertThat(summary.coverage()).isEqualTo(source.range());
        assertThat(summary.recordCount()).isEqualTo(2);
        assertThat(summary.entryCount()).isEqualTo(2);
        assertThat(summary.logicalBytes()).isEqualTo(100);
        assertThat(summary.orderedPayloadSha256().value()).hasSize(64);
        assertThat(physicalReads).hasValue(2);
        assertThat(pins.revalidations).hasValue(1);
        assertThat(pins.releases).hasValue(1);
    }

    @Test
    void refusesToSubstituteWhenTheFrozenIndexDisappears() {
        VersionedGenerationZeroIndex candidate = MaterializationPlannerTestSupport.zero(
                "/index/source-2", 0, 2, 0, 100, 2);
        SourceGeneration source = source(candidate);
        TrackingPins pins = new TrackingPins();
        DefaultExactSourceRangeReader reader = reader(List.of(), pins, new AtomicInteger());

        assertThatThrownBy(() -> reader.read(source, options()).join())
                .isInstanceOf(CompletionException.class)
                .hasRootCauseInstanceOf(NereusException.class)
                .hasRootCauseMessage("task-frozen materialization source changed");
        assertThat(pins.releases).hasValue(0);
    }

    private static DefaultExactSourceRangeReader reader(
            List<VersionedGenerationZeroIndex> candidates,
            TrackingPins pins,
            AtomicInteger reads) {
        ReadTargetReader physical = new ReadTargetReader() {
            private final ReadTargetReaderKey key = ReadTargetReaderKey.from(
                    candidates.isEmpty()
                            ? MaterializationPlannerTestSupport.zero(
                                    "/index/template", 0, 2, 0, 100, 2)
                                    .value()
                                    .readTarget()
                            : candidates.get(0).value().readTarget());

            @Override
            public ReadTargetReaderKey key() {
                return key;
            }

            @Override
            public long reservationBytes(ResolvedRange range) {
                return 64 * 1024;
            }

            @Override
            public CompletableFuture<WalReadResult> readWithStats(
                    StreamId streamId,
                    long startOffset,
                    List<ResolvedRange> ranges,
                    ReadOptions options) {
                reads.incrementAndGet();
                ResolvedRange range = ranges.get(0);
                ObjectSliceReadTarget target = (ObjectSliceReadTarget) range.readTarget();
                byte[] payload = new byte[50];
                java.util.Arrays.fill(payload, (byte) (startOffset + 1));
                ReadBatch batch = new ReadBatch(
                        new OffsetRange(startOffset, startOffset + 1),
                        range.payloadFormat(),
                        payload,
                        range.schemaRefs(),
                        target.entryIndexRef(),
                        range.projectionRef(),
                        target.objectId(),
                        target.objectOffset(),
                        target.objectLength());
                return CompletableFuture.completedFuture(new WalReadResult(
                        List.of(batch),
                        List.of(new WalSliceReadStats(
                                target.objectId(), 0, 100, 0, payload.length))));
            }
        };
        var store = MaterializationPlannerTestSupport.generationStore(
                new ArrayList<>(candidates), List.of(), null);
        return new DefaultExactSourceRangeReader(
                CLUSTER,
                STREAM,
                store,
                (target, view) -> CompletableFuture.completedFuture(PhysicalObjectIdentity.create(
                        target.objectKey(),
                        Optional.of(target.objectId()),
                        PhysicalObjectKind.OBJECT_WAL,
                        100,
                        new Checksum(ChecksumType.CRC32C, "11223344"),
                        Optional.empty(),
                        Optional.empty())),
                pins,
                new ReadTargetDispatcher(new ReadTargetReaderRegistry(List.of(physical))),
                1,
                64 * 1024,
                CLOCK,
                Runnable::run);
    }

    private static SourceGeneration source(VersionedGenerationZeroIndex candidate) {
        return MaterializationSourceMapper.committedSource(
                        candidate,
                        STREAM,
                        com.nereusstream.api.ReadView.COMMITTED,
                        2,
                        2,
                        Optional.of(PROJECTION))
                .orElseThrow();
    }

    private static ReadOptions options() {
        return new ReadOptions(
                1, 64 * 1024, ReadIsolation.COMMITTED, Duration.ofSeconds(10));
    }

    private static final class TrackingPins implements ObjectReadPinManager {
        private final AtomicInteger revalidations = new AtomicInteger();
        private final AtomicInteger releases = new AtomicInteger();

        @Override
        public CompletableFuture<ObjectReadLease> acquire(
                PhysicalObjectIdentity object,
                long maximumReadDeadlineMillis,
                SelectionRevalidator selectionRevalidator) {
            return selectionRevalidator.revalidate().thenApply(ignored -> {
                revalidations.incrementAndGet();
                return new ObjectReadLease() {
                    private boolean released;

                    @Override
                    public PhysicalObjectIdentity object() {
                        return object;
                    }

                    @Override
                    public String leaseId() {
                        return "lease-exact-source";
                    }

                    @Override
                    public long maximumReadDeadlineMillis() {
                        return maximumReadDeadlineMillis;
                    }

                    @Override
                    public CompletableFuture<Void> release() {
                        if (!released) {
                            released = true;
                            releases.incrementAndGet();
                        }
                        return CompletableFuture.completedFuture(null);
                    }

                    @Override
                    public boolean isReleased() {
                        return released;
                    }
                };
            });
        }

        @Override
        public void close() {
        }
    }

    private static final class CollectingSubscriber implements Flow.Subscriber<ReadBatch> {
        private final List<ReadBatch> batches = new ArrayList<>();
        private final CompletableFuture<Void> terminal = new CompletableFuture<>();
        private Flow.Subscription subscription;

        @Override
        public void onSubscribe(Flow.Subscription value) {
            subscription = value;
            value.request(1);
        }

        @Override
        public void onNext(ReadBatch item) {
            batches.add(item);
            subscription.request(1);
        }

        @Override
        public void onError(Throwable failure) {
            terminal.completeExceptionally(failure);
        }

        @Override
        public void onComplete() {
            terminal.complete(null);
        }
    }
}
