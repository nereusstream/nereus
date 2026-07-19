/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.ResolvedRange;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamState;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.physical.ObjectReadLease;
import com.nereusstream.core.physical.ObjectReadPinManager;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.metadata.oxia.CommitSliceRequest;
import com.nereusstream.metadata.oxia.GenerationIndexValidator;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationScanPage;
import com.nereusstream.metadata.oxia.GenerationZeroIndexEncoding;
import com.nereusstream.metadata.oxia.F4ScanKind;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.OffsetIndexEntry;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedGenerationZeroIndex;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.CommittedEndOffsetRecord;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.StreamMetadataRecord;
import com.nereusstream.metadata.oxia.records.TrimRecord;
import com.nereusstream.objectstore.wal.WalReadResult;
import java.lang.reflect.Proxy;
import java.lang.reflect.Constructor;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class GenerationReadResolverTest {
    private static final String BASE32 = "abcdefghijklmnopqrstuvwxyz234567";
    private static final String CLUSTER = "cluster";
    private static final StreamId STREAM = new StreamId("stream");
    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(1_000_000), ZoneOffset.UTC);

    @Test
    void choosesHighestCommittedCoveringGenerationAndIgnoresPrepared() {
        GenerationStoreState store = new GenerationStoreState(List.of(
                generationZero(1),
                higher(4, GenerationLifecycle.PREPARED, "NEREUS_COMPACTED_PARQUET_V1", ReadView.COMMITTED),
                higher(3, GenerationLifecycle.COMMITTED, "NEREUS_COMPACTED_PARQUET_V1", ReadView.COMMITTED)));
        TestPinManager pins = new TestPinManager();
        GenerationReadResolver resolver = resolver(store, pins, readers(true));

        PinnedResolvedRange selected = resolver.resolve(
                        STREAM, 0, ReadView.COMMITTED, Duration.ofSeconds(5))
                .join()
                .orElseThrow();

        assertThat(selected.resolvedRange().generation()).isEqualTo(3);
        assertThat(selected.candidate().publicationId()).isPresent();
        assertThat(pins.validations).hasValue(1);
        selected.release().join();
    }

    @Test
    void staleHigherCandidateAtPinTimeFallsBackOnlyWithinCommittedView() {
        VersionedGenerationCandidate zero = generationZero(1);
        VersionedGenerationCandidate higher = higher(
                3, GenerationLifecycle.COMMITTED, "NEREUS_COMPACTED_PARQUET_V1", ReadView.COMMITTED);
        GenerationStoreState store = new GenerationStoreState(List.of(zero, higher));
        TestPinManager pins = new TestPinManager();
        pins.beforeFirstValidation = () -> store.exact.remove(identity(higher));
        GenerationReadResolver resolver = resolver(store, pins, readers(true));

        PinnedResolvedRange selected = resolver.resolve(
                        STREAM, 0, ReadView.COMMITTED, Duration.ofSeconds(5))
                .join()
                .orElseThrow();

        assertThat(selected.resolvedRange().generation()).isZero();
        assertThat(pins.validations).hasValue(2);
        selected.release().join();
    }

    @Test
    void unknownHigherFormatFailsClosedInsteadOfFallingThroughToWalReader() {
        GenerationStoreState store = new GenerationStoreState(List.of(
                generationZero(1),
                higher(3, GenerationLifecycle.COMMITTED, "UNKNOWN_FORMAT", ReadView.COMMITTED)));
        GenerationReadResolver resolver = resolver(store, new TestPinManager(), readers(false));

        assertThatThrownBy(() -> resolver.resolve(
                        STREAM, 0, ReadView.COMMITTED, Duration.ofSeconds(5)).join())
                .satisfies(error -> assertThat(unwrap(error))
                        .isInstanceOfSatisfying(NereusException.class,
                                nereus -> assertThat(nereus.code())
                                        .isEqualTo(ErrorCode.UNSUPPORTED_READ_TARGET)));
    }

    @Test
    void eachResolvePerformsAFreshAuthoritativeScanAndViewNeverCrosses() {
        VersionedGenerationCandidate zero = generationZero(1);
        GenerationStoreState store = new GenerationStoreState(List.of(
                zero,
                higher(9, GenerationLifecycle.COMMITTED,
                        "NEREUS_COMPACTED_PARQUET_V1", ReadView.TOPIC_COMPACTED)));
        GenerationReadResolver resolver = resolver(store, new TestPinManager(), readers(true));

        PinnedResolvedRange first = resolver.resolve(
                        STREAM, 0, ReadView.COMMITTED, Duration.ofSeconds(5))
                .join().orElseThrow();
        assertThat(first.resolvedRange().generation()).isZero();
        first.release().join();

        store.values.add(higher(
                5, GenerationLifecycle.COMMITTED,
                "NEREUS_COMPACTED_PARQUET_V1", ReadView.COMMITTED));
        store.rebuildExact();
        PinnedResolvedRange second = resolver.resolve(
                        STREAM, 0, ReadView.COMMITTED, Duration.ofSeconds(5))
                .join().orElseThrow();

        assertThat(second.resolvedRange().generation()).isEqualTo(5);
        assertThat(store.scannedViews).containsOnly(ReadView.COMMITTED);
        assertThat(store.scanCalls).hasValue(2);
        second.release().join();
    }

    @Test
    void admitsExactlyFourThousandNinetySixGenerationCandidates() throws Exception {
        AtomicInteger pages = new AtomicInteger();
        F4ScanToken continuation = scanToken();
        GenerationMetadataStore admitted = (GenerationMetadataStore) Proxy.newProxyInstance(
                GenerationReadResolverTest.class.getClassLoader(),
                new Class<?>[] {GenerationMetadataStore.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "scanIndex" -> {
                        int page = pages.getAndIncrement();
                        List<VersionedGenerationCandidate> values = page < 8
                                ? java.util.stream.IntStream.range(0, 512)
                                        .mapToObj(index -> (VersionedGenerationCandidate) higher(
                                                page * 512L + index + 1,
                                                GenerationLifecycle.COMMITTED,
                                                "NEREUS_COMPACTED_PARQUET_V1",
                                                ReadView.COMMITTED))
                                        .sorted(Comparator.comparing(
                                                VersionedGenerationCandidate::key))
                                        .toList()
                                : List.of();
                        yield CompletableFuture.completedFuture(new GenerationScanPage(
                                values,
                                page < 8
                                        ? Optional.of(continuation)
                                        : Optional.empty()));
                    }
                    case "getCandidate" -> {
                        long generation = (long) args[4];
                        yield CompletableFuture.completedFuture(Optional.of(higher(
                                generation,
                                GenerationLifecycle.COMMITTED,
                                "NEREUS_COMPACTED_PARQUET_V1",
                                ReadView.COMMITTED)));
                    }
                    case "close" -> null;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        TestPinManager pins = new TestPinManager();
        GenerationReadResolver resolver = resolver(
                admitted, pins, readers(true));

        PinnedResolvedRange selected = resolver.resolve(
                        STREAM, 0, ReadView.COMMITTED, Duration.ofSeconds(5))
                .join()
                .orElseThrow();

        assertThat(selected.resolvedRange().generation()).isEqualTo(
                GenerationReadResolver.MAX_GENERATION_CANDIDATES_PER_RESOLVE);
        assertThat(pages).hasValue(9);
        assertThat(pins.validations).hasValue(1);
        selected.release().join();
    }

    @Test
    void candidateOverflowFailsInsteadOfSilentlyIgnoringAHigherGeneration() throws Exception {
        AtomicInteger pages = new AtomicInteger();
        F4ScanToken continuation = scanToken();
        GenerationMetadataStore overflowing = (GenerationMetadataStore) Proxy.newProxyInstance(
                GenerationReadResolverTest.class.getClassLoader(),
                new Class<?>[] {GenerationMetadataStore.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "scanIndex" -> {
                        int page = pages.getAndIncrement();
                        int count = page < 8 ? 512 : 1;
                        int first = page * 512;
                        List<VersionedGenerationCandidate> values = java.util.stream.IntStream.range(0, count)
                                .mapToObj(index -> (VersionedGenerationCandidate) generationZero(
                                        first + index + 1L))
                                .toList();
                        yield CompletableFuture.completedFuture(new GenerationScanPage(
                                values,
                                page < 8 ? Optional.of(continuation) : Optional.empty()));
                    }
                    case "close" -> null;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        GenerationReadResolver resolver = resolver(
                overflowing, new TestPinManager(), readers(true));

        assertThatThrownBy(() -> resolver.resolve(
                        STREAM, 0, ReadView.COMMITTED, Duration.ofSeconds(5)).join())
                .satisfies(error -> assertThat(unwrap(error))
                        .isInstanceOfSatisfying(NereusException.class,
                                nereus -> assertThat(nereus.code())
                                        .isEqualTo(ErrorCode.METADATA_LIMIT_EXCEEDED)));
        assertThat(pages).hasValue(9);
    }

    @Test
    void checkpointRepairerRestoresAuthorityBeforeResolverRescans() {
        GenerationStoreState store = new GenerationStoreState(List.of());
        VersionedGenerationIndex restored = higher(
                3,
                GenerationLifecycle.COMMITTED,
                "NEREUS_COMPACTED_PARQUET_V1",
                ReadView.COMMITTED);
        AtomicInteger repairs = new AtomicInteger();
        GenerationIndexRepairer repairer = (streamId, targetOffset, timeout) -> {
            repairs.incrementAndGet();
            store.values.add(restored);
            store.rebuildExact();
            return CompletableFuture.completedFuture(
                    GenerationIndexRepairResult.checkpoint(
                            streamId, targetOffset, 0, restored));
        };
        GenerationReadResolver resolver = resolver(
                store.proxy(),
                new TestPinManager(),
                readers(true),
                repairer);

        PinnedResolvedRange selected = resolver.resolve(
                        STREAM, 0, ReadView.COMMITTED, Duration.ofSeconds(5))
                .join().orElseThrow();

        assertThat(repairs).hasValue(1);
        assertThat(store.scanCalls).hasValue(2);
        assertThat(selected.resolvedRange().generation()).isEqualTo(3);
        selected.release().join();
    }

    @Test
    void asyncObjectWalProfileUsesTheSameCommittedGenerationResolver() {
        GenerationStoreState store = new GenerationStoreState(
                List.of(generationZero(1)));
        GenerationReadResolver resolver = new GenerationReadResolver(
                CLUSTER,
                l0Store(StorageProfile.OBJECT_WAL_ASYNC_OBJECT),
                store.proxy(),
                GenerationIndexValidator.phase15Targets(),
                readers(true),
                GenerationReadResolverTest::identity,
                new TestPinManager(),
                1_000,
                CLOCK,
                Runnable::run);

        PinnedResolvedRange selected = resolver.resolve(
                        STREAM,
                        0,
                        ReadView.COMMITTED,
                        Duration.ofSeconds(5))
                .join()
                .orElseThrow();

        assertThat(selected.resolvedRange().generation()).isZero();
        selected.release().join();
    }

    private static GenerationReadResolver resolver(
            GenerationStoreState store,
            TestPinManager pins,
            ReadTargetReaderRegistry readers) {
        return resolver(store.proxy(), pins, readers);
    }

    private static GenerationReadResolver resolver(
            GenerationMetadataStore store,
            TestPinManager pins,
            ReadTargetReaderRegistry readers) {
        return new GenerationReadResolver(
                CLUSTER,
                l0Store(),
                store,
                GenerationIndexValidator.phase15Targets(),
                readers,
                GenerationReadResolverTest::identity,
                pins,
                1_000,
                CLOCK,
                Runnable::run);
    }

    private static GenerationReadResolver resolver(
            GenerationMetadataStore store,
            TestPinManager pins,
            ReadTargetReaderRegistry readers,
            GenerationIndexRepairer repairer) {
        return new GenerationReadResolver(
                CLUSTER,
                l0Store(),
                store,
                GenerationIndexValidator.phase15Targets(),
                readers,
                GenerationReadResolverTest::identity,
                pins,
                repairer,
                CLOCK,
                Runnable::run);
    }

    private static ReadTargetReaderRegistry readers(boolean compacted) {
        List<ReadTargetReader> values = new ArrayList<>();
        values.add(new NoopReader(ReadTargetReaderRegistryTest.key(
                ObjectType.MULTI_STREAM_WAL_OBJECT, "WAL_OBJECT_V1")));
        if (compacted) {
            values.add(new NoopReader(ReadTargetReaderRegistryTest.key(
                    ObjectType.STREAM_COMPACTED_OBJECT, "NEREUS_COMPACTED_PARQUET_V1")));
        }
        return new ReadTargetReaderRegistry(values);
    }

    private static OxiaMetadataStore l0Store() {
        return l0Store(StorageProfile.OBJECT_WAL_SYNC_OBJECT);
    }

    private static OxiaMetadataStore l0Store(StorageProfile profile) {
        StreamMetadataSnapshot snapshot = new StreamMetadataSnapshot(
                new StreamMetadataRecord(
                        STREAM.value(),
                        "persistent://tenant/ns/topic",
                        "hash",
                        StreamState.ACTIVE.name(),
                        profile.name(),
                        Map.of(),
                        1,
                        1,
                        7),
                new CommittedEndOffsetRecord(STREAM.value(), 10, 10, 10, 7),
                new TrimRecord(STREAM.value(), 0, "", 1, 7));
        return (OxiaMetadataStore) Proxy.newProxyInstance(
                GenerationReadResolverTest.class.getClassLoader(),
                new Class<?>[] {OxiaMetadataStore.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getStreamSnapshot" -> CompletableFuture.completedFuture(snapshot);
                    case "close" -> null;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static CompletableFuture<PhysicalObjectIdentity> identity(
            ObjectSliceReadTarget target, ReadView view) {
        PhysicalObjectKind kind = target.objectType() == ObjectType.MULTI_STREAM_WAL_OBJECT
                ? PhysicalObjectKind.OBJECT_WAL
                : view == ReadView.COMMITTED
                        ? PhysicalObjectKind.COMMITTED_COMPACTED
                        : PhysicalObjectKind.TOPIC_COMPACTED;
        return CompletableFuture.completedFuture(PhysicalObjectIdentity.create(
                target.objectKey(),
                Optional.of(target.objectId()),
                kind,
                target.objectLength(),
                new Checksum(ChecksumType.CRC32C, "00000002"),
                Optional.empty(),
                Optional.empty()));
    }

    private static VersionedGenerationZeroIndex generationZero(long version) {
        ObjectSliceReadTarget target = ReadTargetReaderRegistryTest.target(
                ObjectType.MULTI_STREAM_WAL_OBJECT, "WAL_OBJECT_V1");
        long offsetEnd = Math.addExact(version, 1);
        OffsetIndexEntry entry = new OffsetIndexEntry(
                STREAM,
                new OffsetRange(0, offsetEnd),
                0,
                offsetEnd,
                target,
                PayloadFormat.OPAQUE_RECORD_BATCH,
                Math.toIntExact(offsetEnd),
                Math.toIntExact(offsetEnd),
                offsetEnd,
                List.of(),
                Optional.empty(),
                1,
                false,
                version);
        return new VersionedGenerationZeroIndex(
                String.format("/index/%019d/0", offsetEnd),
                GenerationZeroIndexEncoding.GENERIC_OFFSET_INDEX_TARGET_RECORD,
                entry,
                version,
                sha(version));
    }

    private static VersionedGenerationIndex higher(
            long generation,
            GenerationLifecycle lifecycle,
            String format,
            ReadView view) {
        ObjectSliceReadTarget target = ReadTargetReaderRegistryTest.target(
                ObjectType.STREAM_COMPACTED_OBJECT, format);
        var encoded = ReadTargetCodecRegistry.phase15().encode(target);
        long committedAt = lifecycle == GenerationLifecycle.COMMITTED ? 110 : 0;
        String reason = switch (lifecycle) {
            case QUARANTINED, DRAINING, RETIRED, ABORTED -> "reason";
            default -> "";
        };
        long version = generation + 10;
        GenerationIndexRecord record = new GenerationIndexRecord(
                1,
                STREAM.value(),
                view.wireId(),
                0,
                2,
                generation,
                publication(generation),
                "task-" + generation,
                lifecycle,
                "a".repeat(64),
                "b".repeat(64),
                encoded,
                encoded.identityChecksumValue(),
                "b".repeat(64),
                PayloadFormat.OPAQUE_RECORD_BATCH.name(),
                2,
                2,
                2,
                2,
                0,
                2,
                1,
                1,
                List.of(),
                CommitSliceRequest.emptyProjectionIdentity(),
                100,
                committedAt,
                reason,
                120,
                version);
        return new VersionedGenerationIndex(
                "/" + view.name() + "/index/2/" + generation,
                record,
                version,
                sha(version));
    }

    private static Checksum sha(long seed) {
        return new Checksum(ChecksumType.SHA256, String.format("%064x", seed));
    }

    private static String publication(long generation) {
        if (generation < 0 || generation >= 1L << 20) {
            throw new IllegalArgumentException("test generation is outside the base32 fixture range");
        }
        char[] value = "p".repeat(26).toCharArray();
        long remaining = generation;
        for (int index = value.length - 1; index >= value.length - 4; index--) {
            value[index] = BASE32.charAt((int) (remaining & 31));
            remaining >>>= 5;
        }
        return new String(value);
    }

    private static String identity(VersionedGenerationCandidate candidate) {
        long end = candidate instanceof VersionedGenerationIndex higher
                ? higher.value().offsetEnd()
                : ((VersionedGenerationZeroIndex) candidate).value().offsetEnd();
        long generation = candidate instanceof VersionedGenerationIndex higher
                ? higher.value().generation()
                : 0;
        ReadView view = candidate instanceof VersionedGenerationIndex higher
                ? ReadView.fromWireId(higher.value().readViewId())
                : ReadView.COMMITTED;
        return view + ":" + end + ":" + generation;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static F4ScanToken scanToken() throws Exception {
        Constructor<F4ScanToken> constructor = F4ScanToken.class.getDeclaredConstructor(
                String.class, F4ScanKind.class, String.class, String.class, String.class);
        constructor.setAccessible(true);
        return constructor.newInstance(
                CLUSTER,
                F4ScanKind.GENERATION_INDEX,
                "a".repeat(64),
                "/index/",
                "/index/page");
    }

    private static final class GenerationStoreState {
        private final List<VersionedGenerationCandidate> values;
        private final Map<String, VersionedGenerationCandidate> exact = new java.util.HashMap<>();
        private final List<ReadView> scannedViews = new ArrayList<>();
        private final AtomicInteger scanCalls = new AtomicInteger();

        private GenerationStoreState(List<VersionedGenerationCandidate> values) {
            this.values = new ArrayList<>(values);
            rebuildExact();
        }

        private void rebuildExact() {
            exact.clear();
            values.forEach(value -> exact.put(identity(value), value));
        }

        private GenerationMetadataStore proxy() {
            return (GenerationMetadataStore) Proxy.newProxyInstance(
                    GenerationReadResolverTest.class.getClassLoader(),
                    new Class<?>[] {GenerationMetadataStore.class},
                    (proxy, method, args) -> switch (method.getName()) {
                        case "scanIndex" -> {
                            ReadView view = (ReadView) args[2];
                            scannedViews.add(view);
                            scanCalls.incrementAndGet();
                            List<VersionedGenerationCandidate> selected = values.stream()
                                    .filter(value -> value instanceof VersionedGenerationZeroIndex
                                            ? view == ReadView.COMMITTED
                                            : ReadView.fromWireId(
                                                            ((VersionedGenerationIndex) value).value().readViewId())
                                                    == view)
                                    .sorted(Comparator.comparing(VersionedGenerationCandidate::key))
                                    .toList();
                            yield CompletableFuture.completedFuture(
                                    new GenerationScanPage(selected, Optional.empty()));
                        }
                        case "getCandidate" -> CompletableFuture.completedFuture(Optional.ofNullable(
                                exact.get(args[2] + ":" + args[3] + ":" + args[4])));
                        case "close" -> null;
                        default -> throw new UnsupportedOperationException(method.getName());
                    });
        }
    }

    private static final class NoopReader implements ReadTargetReader {
        private final ReadTargetReaderKey key;

        private NoopReader(ReadTargetReaderKey key) {
            this.key = key;
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
                StreamId streamId,
                long startOffset,
                List<ResolvedRange> ranges,
                ReadOptions options) {
            return CompletableFuture.completedFuture(new WalReadResult(List.of(), List.of()));
        }
    }

    private static final class TestPinManager implements ObjectReadPinManager {
        private final AtomicInteger validations = new AtomicInteger();
        private Runnable beforeFirstValidation = () -> {
        };

        @Override
        public CompletableFuture<ObjectReadLease> acquire(
                PhysicalObjectIdentity object,
                long maximumReadDeadlineMillis,
                SelectionRevalidator selectionRevalidator) {
            if (validations.get() == 0) {
                beforeFirstValidation.run();
            }
            validations.incrementAndGet();
            return selectionRevalidator.revalidate().thenApply(ignored -> new TestLease(
                    object, maximumReadDeadlineMillis));
        }

        @Override
        public void close() {
        }
    }

    private static final class TestLease implements ObjectReadLease {
        private final PhysicalObjectIdentity object;
        private final long deadline;
        private final AtomicReference<CompletableFuture<Void>> release = new AtomicReference<>();

        private TestLease(PhysicalObjectIdentity object, long deadline) {
            this.object = object;
            this.deadline = deadline;
        }

        @Override
        public PhysicalObjectIdentity object() {
            return object;
        }

        @Override
        public String leaseId() {
            return "lease";
        }

        @Override
        public long maximumReadDeadlineMillis() {
            return deadline;
        }

        @Override
        public CompletableFuture<Void> release() {
            release.compareAndSet(null, CompletableFuture.completedFuture(null));
            return release.get();
        }

        @Override
        public boolean isReleased() {
            return release.get() != null;
        }
    }
}
