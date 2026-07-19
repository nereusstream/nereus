/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.records.BookKeeperAllocationSlotRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperAppendReservationRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerProtectionRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerReaderLeaseRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperWriterStateRecord;
import com.nereusstream.metadata.oxia.records.LedgerAllocationIntentRecord;
import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

/** Production BookKeeper writer/ledger metadata adapter over a caller-owned shared Oxia runtime. */
public final class OxiaJavaBookKeeperMetadataStore
        implements BookKeeperWriterMetadataStore, BookKeeperLedgerMetadataStore {
    private final F4MetadataStoreSupport support;
    private final BookKeeperMetadataStoreConfig configuration;

    public static OxiaJavaBookKeeperMetadataStore usingSharedRuntime(
            OxiaClientConfiguration clientConfiguration,
            SharedOxiaClientRuntime runtime,
            Clock clock,
            BookKeeperMetadataStoreConfig storeConfiguration) {
        Objects.requireNonNull(clientConfiguration, "clientConfiguration");
        Objects.requireNonNull(runtime, "runtime");
        runtime.requireCompatible(clientConfiguration);
        return new OxiaJavaBookKeeperMetadataStore(runtime.client(), clock, storeConfiguration);
    }

    OxiaJavaBookKeeperMetadataStore(
            PartitionedOxiaClient client,
            Clock clock,
            BookKeeperMetadataStoreConfig configuration) {
        this.support = new F4MetadataStoreSupport(client, clock);
        this.configuration = Objects.requireNonNull(configuration, "configuration");
    }

    @Override
    public CompletableFuture<Optional<BookKeeperVersionedValue<BookKeeperWriterStateRecord>>> getWriter(
            String cluster, StreamId stream) {
        BookKeeperKeyspace keys = keys(cluster);
        StreamId exactStream = Objects.requireNonNull(stream, "stream");
        String key = keys.writerStateKey(exactStream);
        return getExact(key, keys.streamPartitionKey(exactStream), BookKeeperWriterStateRecord.class,
                (storedKey, value) -> {
                    requireWriterBound(value);
                    requireKey(storedKey, keys.writerStateKey(new StreamId(value.streamId())));
                });
    }

    @Override
    public CompletableFuture<BookKeeperVersionedValue<BookKeeperWriterStateRecord>> createWriter(
            String cluster, BookKeeperWriterStateRecord value) {
        BookKeeperWriterStateRecord exact = Objects.requireNonNull(value, "value");
        BookKeeperKeyspace keys = keys(cluster);
        requireWriterBound(exact);
        StreamId stream = new StreamId(exact.streamId());
        String key = keys.writerStateKey(stream);
        return createExact(key, keys.streamPartitionKey(stream), exact, BookKeeperWriterStateRecord.class,
                () -> getWriter(cluster, stream));
    }

    @Override
    public CompletableFuture<BookKeeperVersionedValue<BookKeeperWriterStateRecord>> compareAndSetWriter(
            String cluster, BookKeeperWriterStateRecord value, long expectedVersion) {
        BookKeeperWriterStateRecord exact = Objects.requireNonNull(value, "value");
        BookKeeperKeyspace keys = keys(cluster);
        requireWriterBound(exact);
        StreamId stream = new StreamId(exact.streamId());
        return compareAndSetExact(keys.writerStateKey(stream), keys.streamPartitionKey(stream), exact,
                BookKeeperWriterStateRecord.class, expectedVersion, () -> getWriter(cluster, stream),
                BookKeeperMetadataTransitions::writer);
    }

    @Override
    public CompletableFuture<Optional<BookKeeperVersionedValue<LedgerAllocationIntentRecord>>> getAllocation(
            String cluster, StreamId stream, String allocationId) {
        BookKeeperKeyspace keys = keys(cluster);
        StreamId exactStream = Objects.requireNonNull(stream, "stream");
        String key = keys.allocationKey(exactStream, allocationId);
        return getExact(key, keys.streamPartitionKey(exactStream), LedgerAllocationIntentRecord.class,
                (storedKey, value) -> {
                    keys.requireAllocationSlot(value.allocationSlot());
                    requireKey(storedKey, keys.allocationKey(new StreamId(value.streamId()), value.allocationId()));
                });
    }

    @Override
    public CompletableFuture<BookKeeperVersionedValue<LedgerAllocationIntentRecord>> createAllocation(
            String cluster, LedgerAllocationIntentRecord value) {
        LedgerAllocationIntentRecord exact = Objects.requireNonNull(value, "value");
        BookKeeperKeyspace keys = keys(cluster);
        keys.requireAllocationSlot(exact.allocationSlot());
        StreamId stream = new StreamId(exact.streamId());
        String key = keys.allocationKey(stream, exact.allocationId());
        return createExact(key, keys.streamPartitionKey(stream), exact, LedgerAllocationIntentRecord.class,
                () -> getAllocation(cluster, stream, exact.allocationId()));
    }

    @Override
    public CompletableFuture<BookKeeperVersionedValue<LedgerAllocationIntentRecord>> compareAndSetAllocation(
            String cluster, LedgerAllocationIntentRecord value, long expectedVersion) {
        LedgerAllocationIntentRecord exact = Objects.requireNonNull(value, "value");
        BookKeeperKeyspace keys = keys(cluster);
        keys.requireAllocationSlot(exact.allocationSlot());
        StreamId stream = new StreamId(exact.streamId());
        return compareAndSetExact(keys.allocationKey(stream, exact.allocationId()), keys.streamPartitionKey(stream),
                exact, LedgerAllocationIntentRecord.class, expectedVersion,
                () -> getAllocation(cluster, stream, exact.allocationId()), BookKeeperMetadataTransitions::allocation);
    }

    @Override
    public CompletableFuture<Void> deleteAllocation(
            String cluster, StreamId stream, String allocationId, long expectedVersion) {
        BookKeeperKeyspace keys = keys(cluster);
        StreamId exactStream = Objects.requireNonNull(stream, "stream");
        return deleteExact(keys.allocationKey(exactStream, allocationId),
                keys.streamPartitionKey(exactStream), expectedVersion,
                () -> getAllocation(cluster, exactStream, allocationId));
    }

    @Override
    public CompletableFuture<BookKeeperScanPage<BookKeeperVersionedValue<LedgerAllocationIntentRecord>>>
            scanAllocations(String cluster, StreamId stream, Optional<BookKeeperScanToken> continuation, int limit) {
        BookKeeperKeyspace keys = keys(cluster);
        StreamId exactStream = Objects.requireNonNull(stream, "stream");
        return scan(cluster, BookKeeperScanKind.ALLOCATION, "stream\0" + exactStream.value(),
                keys.allocationPrefix(exactStream), 1, keys.streamPartitionKey(exactStream), continuation, limit,
                LedgerAllocationIntentRecord.class,
                (key, value) -> {
                    keys.requireAllocationSlot(value.allocationSlot());
                    requireKey(key, keys.allocationKey(new StreamId(value.streamId()), value.allocationId()));
                });
    }

    @Override
    public CompletableFuture<Optional<BookKeeperVersionedValue<BookKeeperAllocationSlotRecord>>> getAllocationSlot(
            String cluster, int slot) {
        BookKeeperKeyspace keys = keys(cluster);
        String key = keys.allocationSlotKey(slot);
        return getExact(key, keys.allocationSlotPartitionKey(keys.allocationSlotShard(slot)),
                BookKeeperAllocationSlotRecord.class,
                (storedKey, value) -> requireKey(storedKey, keys.allocationSlotKey(value.slot())));
    }

    @Override
    public CompletableFuture<BookKeeperVersionedValue<BookKeeperAllocationSlotRecord>> createAllocationSlot(
            String cluster, BookKeeperAllocationSlotRecord value) {
        BookKeeperAllocationSlotRecord exact = Objects.requireNonNull(value, "value");
        BookKeeperKeyspace keys = keys(cluster);
        String key = keys.allocationSlotKey(exact.slot());
        PartitionKey partition = keys.allocationSlotPartitionKey(keys.allocationSlotShard(exact.slot()));
        return createExact(key, partition, exact, BookKeeperAllocationSlotRecord.class,
                () -> getAllocationSlot(cluster, exact.slot()));
    }

    @Override
    public CompletableFuture<BookKeeperVersionedValue<BookKeeperAllocationSlotRecord>> compareAndSetAllocationSlot(
            String cluster, BookKeeperAllocationSlotRecord value, long expectedVersion) {
        BookKeeperAllocationSlotRecord exact = Objects.requireNonNull(value, "value");
        BookKeeperKeyspace keys = keys(cluster);
        String key = keys.allocationSlotKey(exact.slot());
        PartitionKey partition = keys.allocationSlotPartitionKey(keys.allocationSlotShard(exact.slot()));
        return compareAndSetExact(key, partition, exact, BookKeeperAllocationSlotRecord.class, expectedVersion,
                () -> getAllocationSlot(cluster, exact.slot()), BookKeeperMetadataTransitions::allocationSlot);
    }

    @Override
    public CompletableFuture<Void> deleteAllocationSlot(String cluster, int slot, long expectedVersion) {
        BookKeeperKeyspace keys = keys(cluster);
        return deleteExact(keys.allocationSlotKey(slot),
                keys.allocationSlotPartitionKey(keys.allocationSlotShard(slot)), expectedVersion,
                () -> getAllocationSlot(cluster, slot));
    }

    @Override
    public CompletableFuture<BookKeeperScanPage<BookKeeperVersionedValue<BookKeeperAllocationSlotRecord>>>
            scanAllocationSlots(
                    String cluster, int slotShard, Optional<BookKeeperScanToken> continuation, int limit) {
        BookKeeperKeyspace keys = keys(cluster);
        return scan(cluster, BookKeeperScanKind.ALLOCATION_SLOT, "allocation-slot-shard\0" + slotShard,
                keys.allocationSlotShardPrefix(slotShard), 1, keys.allocationSlotPartitionKey(slotShard),
                continuation, limit, BookKeeperAllocationSlotRecord.class,
                (key, value) -> keys.parseAllocationSlotKey(requireKey(key, keys.allocationSlotKey(value.slot()))));
    }

    @Override
    public CompletableFuture<Optional<BookKeeperVersionedValue<BookKeeperAppendReservationRecord>>> getReservation(
            String cluster, StreamId stream, String reservationId) {
        BookKeeperKeyspace keys = keys(cluster);
        StreamId exactStream = Objects.requireNonNull(stream, "stream");
        String key = keys.appendReservationKey(exactStream, reservationId);
        return getExact(key, keys.streamPartitionKey(exactStream), BookKeeperAppendReservationRecord.class,
                (storedKey, value) -> {
                    keys.requireLedgerRangeSlot(value.ledgerRangeSlot());
                    requireKey(storedKey,
                            keys.appendReservationKey(new StreamId(value.streamId()), value.reservationId()));
                });
    }

    @Override
    public CompletableFuture<BookKeeperVersionedValue<BookKeeperAppendReservationRecord>> createReservation(
            String cluster, BookKeeperAppendReservationRecord value) {
        BookKeeperAppendReservationRecord exact = Objects.requireNonNull(value, "value");
        BookKeeperKeyspace keys = keys(cluster);
        keys.requireLedgerRangeSlot(exact.ledgerRangeSlot());
        StreamId stream = new StreamId(exact.streamId());
        String key = keys.appendReservationKey(stream, exact.reservationId());
        return createExact(key, keys.streamPartitionKey(stream), exact, BookKeeperAppendReservationRecord.class,
                () -> getReservation(cluster, stream, exact.reservationId()));
    }

    @Override
    public CompletableFuture<BookKeeperVersionedValue<BookKeeperAppendReservationRecord>> compareAndSetReservation(
            String cluster, BookKeeperAppendReservationRecord value, long expectedVersion) {
        BookKeeperAppendReservationRecord exact = Objects.requireNonNull(value, "value");
        BookKeeperKeyspace keys = keys(cluster);
        keys.requireLedgerRangeSlot(exact.ledgerRangeSlot());
        StreamId stream = new StreamId(exact.streamId());
        return compareAndSetExact(keys.appendReservationKey(stream, exact.reservationId()),
                keys.streamPartitionKey(stream), exact, BookKeeperAppendReservationRecord.class, expectedVersion,
                () -> getReservation(cluster, stream, exact.reservationId()), BookKeeperMetadataTransitions::reservation);
    }

    @Override
    public CompletableFuture<Void> deleteReservation(
            String cluster, StreamId stream, String reservationId, long expectedVersion) {
        BookKeeperKeyspace keys = keys(cluster);
        StreamId exactStream = Objects.requireNonNull(stream, "stream");
        return deleteExact(keys.appendReservationKey(exactStream, reservationId),
                keys.streamPartitionKey(exactStream), expectedVersion,
                () -> getReservation(cluster, exactStream, reservationId));
    }

    @Override
    public CompletableFuture<BookKeeperScanPage<BookKeeperVersionedValue<BookKeeperAppendReservationRecord>>>
            scanReservations(String cluster, StreamId stream, Optional<BookKeeperScanToken> continuation, int limit) {
        BookKeeperKeyspace keys = keys(cluster);
        StreamId exactStream = Objects.requireNonNull(stream, "stream");
        return scan(cluster, BookKeeperScanKind.APPEND_RESERVATION, "stream\0" + exactStream.value(),
                keys.appendReservationPrefix(exactStream), 1, keys.streamPartitionKey(exactStream), continuation,
                limit, BookKeeperAppendReservationRecord.class,
                (key, value) -> {
                    keys.requireLedgerRangeSlot(value.ledgerRangeSlot());
                    requireKey(key, keys.appendReservationKey(new StreamId(value.streamId()), value.reservationId()));
                });
    }

    @Override
    public CompletableFuture<Optional<BookKeeperVersionedValue<BookKeeperLedgerRootRecord>>> getRoot(
            String cluster, String providerScopeSha256, long ledgerId) {
        BookKeeperKeyspace keys = keys(cluster);
        String key = keys.ledgerRootKey(providerScopeSha256, ledgerId);
        return getExact(key, keys.ledgerPartitionKey(providerScopeSha256, ledgerId), BookKeeperLedgerRootRecord.class,
                (storedKey, value) -> validateRoot(keys, storedKey, value));
    }

    @Override
    public CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerRootRecord>> createRoot(
            String cluster, BookKeeperLedgerRootRecord value) {
        BookKeeperLedgerRootRecord exact = Objects.requireNonNull(value, "value");
        BookKeeperKeyspace keys = keys(cluster);
        String key = keys.ledgerRootKey(exact.providerScopeSha256(), exact.ledgerId());
        validateRoot(keys, key, exact);
        return createExact(key, keys.ledgerPartitionKey(exact.providerScopeSha256(), exact.ledgerId()), exact,
                BookKeeperLedgerRootRecord.class,
                () -> getRoot(cluster, exact.providerScopeSha256(), exact.ledgerId()));
    }

    @Override
    public CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerRootRecord>> compareAndSetRoot(
            String cluster, BookKeeperLedgerRootRecord value, long expectedVersion) {
        BookKeeperLedgerRootRecord exact = Objects.requireNonNull(value, "value");
        BookKeeperKeyspace keys = keys(cluster);
        String key = keys.ledgerRootKey(exact.providerScopeSha256(), exact.ledgerId());
        validateRoot(keys, key, exact);
        return compareAndSetExact(key, keys.ledgerPartitionKey(exact.providerScopeSha256(), exact.ledgerId()), exact,
                BookKeeperLedgerRootRecord.class, expectedVersion,
                () -> getRoot(cluster, exact.providerScopeSha256(), exact.ledgerId()), BookKeeperMetadataTransitions::root);
    }

    @Override
    public CompletableFuture<BookKeeperScanPage<BookKeeperVersionedValue<BookKeeperLedgerRootRecord>>> scanRoots(
            String cluster, int shard, Optional<BookKeeperScanToken> continuation, int limit) {
        BookKeeperKeyspace keys = keys(cluster);
        return scan(cluster, BookKeeperScanKind.LEDGER_ROOT, "ledger-root-shard\0" + shard,
                keys.ledgerRootShardPrefix(shard), 1, keys.ledgerPartitionKeyByShard(shard), continuation, limit,
                BookKeeperLedgerRootRecord.class, (key, value) -> validateRoot(keys, key, value));
    }

    @Override
    public CompletableFuture<Optional<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>>> getProtection(
            String cluster, String providerScopeSha256, long ledgerId, int rangeSlot, int protectionSlot) {
        BookKeeperKeyspace keys = keys(cluster);
        String key = keys.protectionKey(providerScopeSha256, ledgerId, rangeSlot, protectionSlot);
        return getExact(key, keys.ledgerPartitionKey(providerScopeSha256, ledgerId),
                BookKeeperLedgerProtectionRecord.class,
                (storedKey, value) -> validateProtection(keys, storedKey, providerScopeSha256, value));
    }

    @Override
    public CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>> createProtection(
            String cluster, String providerScopeSha256, BookKeeperLedgerProtectionRecord value) {
        BookKeeperLedgerProtectionRecord exact = Objects.requireNonNull(value, "value");
        BookKeeperKeyspace keys = keys(cluster);
        String key = keys.protectionKey(providerScopeSha256, exact.ledgerId(),
                exact.ledgerRangeSlot(), exact.protectionSlot());
        validateProtection(keys, key, providerScopeSha256, exact);
        return createExact(key, keys.ledgerPartitionKey(providerScopeSha256, exact.ledgerId()), exact,
                BookKeeperLedgerProtectionRecord.class,
                () -> getProtection(cluster, providerScopeSha256, exact.ledgerId(),
                        exact.ledgerRangeSlot(), exact.protectionSlot()));
    }

    @Override
    public CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>> compareAndSetProtection(
            String cluster, String providerScopeSha256, BookKeeperLedgerProtectionRecord value,
            long expectedVersion) {
        BookKeeperLedgerProtectionRecord exact = Objects.requireNonNull(value, "value");
        BookKeeperKeyspace keys = keys(cluster);
        String key = keys.protectionKey(providerScopeSha256, exact.ledgerId(),
                exact.ledgerRangeSlot(), exact.protectionSlot());
        validateProtection(keys, key, providerScopeSha256, exact);
        return compareAndSetExact(key, keys.ledgerPartitionKey(providerScopeSha256, exact.ledgerId()), exact,
                BookKeeperLedgerProtectionRecord.class, expectedVersion,
                () -> getProtection(cluster, providerScopeSha256, exact.ledgerId(),
                        exact.ledgerRangeSlot(), exact.protectionSlot()),
                BookKeeperMetadataTransitions::protection);
    }

    @Override
    public CompletableFuture<Void> deleteProtection(
            String cluster, String providerScopeSha256, long ledgerId, int rangeSlot, int protectionSlot,
            long expectedVersion) {
        BookKeeperKeyspace keys = keys(cluster);
        return deleteExact(keys.protectionKey(providerScopeSha256, ledgerId, rangeSlot, protectionSlot),
                keys.ledgerPartitionKey(providerScopeSha256, ledgerId), expectedVersion,
                () -> getProtection(cluster, providerScopeSha256, ledgerId, rangeSlot, protectionSlot));
    }

    @Override
    public CompletableFuture<BookKeeperScanPage<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>>>
            scanProtections(String cluster, String providerScopeSha256, long ledgerId,
                    Optional<BookKeeperScanToken> continuation, int limit) {
        BookKeeperKeyspace keys = keys(cluster);
        String identity = keys.ledgerIdentitySha256(providerScopeSha256, ledgerId);
        return scan(cluster, BookKeeperScanKind.PROTECTION, "ledger-protection\0" + identity,
                keys.protectionPrefix(providerScopeSha256, ledgerId), 2,
                keys.ledgerPartitionKey(providerScopeSha256, ledgerId), continuation, limit,
                BookKeeperLedgerProtectionRecord.class,
                (key, value) -> validateProtection(keys, key, providerScopeSha256, value));
    }

    @Override
    public CompletableFuture<Optional<BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord>>> getReaderLease(
            String cluster, String providerScopeSha256, long ledgerId, int readerSlot) {
        BookKeeperKeyspace keys = keys(cluster);
        String key = keys.readerLeaseKey(providerScopeSha256, ledgerId, readerSlot);
        return getExact(key, keys.ledgerPartitionKey(providerScopeSha256, ledgerId),
                BookKeeperLedgerReaderLeaseRecord.class,
                (storedKey, value) -> validateReaderLease(keys, storedKey, providerScopeSha256, value));
    }

    @Override
    public CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord>> createReaderLease(
            String cluster, String providerScopeSha256, BookKeeperLedgerReaderLeaseRecord value) {
        BookKeeperLedgerReaderLeaseRecord exact = Objects.requireNonNull(value, "value");
        BookKeeperKeyspace keys = keys(cluster);
        String key = keys.readerLeaseKey(providerScopeSha256, exact.ledgerId(), exact.readerSlot());
        validateReaderLease(keys, key, providerScopeSha256, exact);
        return createExact(key, keys.ledgerPartitionKey(providerScopeSha256, exact.ledgerId()), exact,
                BookKeeperLedgerReaderLeaseRecord.class,
                () -> getReaderLease(cluster, providerScopeSha256, exact.ledgerId(), exact.readerSlot()));
    }

    @Override
    public CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord>> compareAndSetReaderLease(
            String cluster, String providerScopeSha256, BookKeeperLedgerReaderLeaseRecord value,
            long expectedVersion) {
        BookKeeperLedgerReaderLeaseRecord exact = Objects.requireNonNull(value, "value");
        BookKeeperKeyspace keys = keys(cluster);
        String key = keys.readerLeaseKey(providerScopeSha256, exact.ledgerId(), exact.readerSlot());
        validateReaderLease(keys, key, providerScopeSha256, exact);
        return compareAndSetExact(key, keys.ledgerPartitionKey(providerScopeSha256, exact.ledgerId()), exact,
                BookKeeperLedgerReaderLeaseRecord.class, expectedVersion,
                () -> getReaderLease(cluster, providerScopeSha256, exact.ledgerId(), exact.readerSlot()),
                BookKeeperMetadataTransitions::readerLease);
    }

    @Override
    public CompletableFuture<Void> deleteReaderLease(
            String cluster, String providerScopeSha256, long ledgerId, int readerSlot, long expectedVersion) {
        BookKeeperKeyspace keys = keys(cluster);
        return deleteExact(keys.readerLeaseKey(providerScopeSha256, ledgerId, readerSlot),
                keys.ledgerPartitionKey(providerScopeSha256, ledgerId), expectedVersion,
                () -> getReaderLease(cluster, providerScopeSha256, ledgerId, readerSlot));
    }

    @Override
    public CompletableFuture<BookKeeperScanPage<BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord>>>
            scanReaderLeases(String cluster, String providerScopeSha256, long ledgerId,
                    Optional<BookKeeperScanToken> continuation, int limit) {
        BookKeeperKeyspace keys = keys(cluster);
        String identity = keys.ledgerIdentitySha256(providerScopeSha256, ledgerId);
        return scan(cluster, BookKeeperScanKind.READER_LEASE, "ledger-reader\0" + identity,
                keys.readerLeasePrefix(providerScopeSha256, ledgerId), 1,
                keys.ledgerPartitionKey(providerScopeSha256, ledgerId), continuation, limit,
                BookKeeperLedgerReaderLeaseRecord.class,
                (key, value) -> validateReaderLease(keys, key, providerScopeSha256, value));
    }

    @Override
    public void close() {
        support.close();
    }

    private BookKeeperKeyspace keys(String cluster) {
        return configuration.keyspace(cluster);
    }

    private <T> CompletableFuture<Optional<BookKeeperVersionedValue<T>>> getExact(
            String key, PartitionKey partition, Class<T> type, BiConsumer<String, T> keyValidator) {
        return support.get(key, partition, type).thenApply(optional -> optional.map(item -> {
            keyValidator.accept(item.key(), item.value());
            return versioned(item);
        }));
    }

    private <T> CompletableFuture<BookKeeperVersionedValue<T>> createExact(
            String key,
            PartitionKey partition,
            T value,
            Class<T> type,
            Supplier<CompletableFuture<Optional<BookKeeperVersionedValue<T>>>> reload) {
        var expectedDigest = support.encodedDigest(value, type);
        CompletableFuture<BookKeeperVersionedValue<T>> create = support.create(key, partition, value, type)
                .thenApply(OxiaJavaBookKeeperMetadataStore::versioned);
        return create.handle((created, failure) -> {
            if (failure == null) {
                return CompletableFuture.completedFuture(created);
            }
            return reload.get().thenApply(existing -> {
                if (existing.isEmpty()) {
                    throw rethrow(F4MetadataStoreSupport.unwrap(failure));
                }
                BookKeeperVersionedValue<T> result = existing.orElseThrow();
                if (!result.durableValueSha256().equals(expectedDigest)) {
                    throw new BookKeeperMetadataConditionFailedException(
                            "BookKeeper metadata exact key is occupied by another identity");
                }
                return result;
            });
        }).thenCompose(Function.identity());
    }

    private <T> CompletableFuture<BookKeeperVersionedValue<T>> compareAndSetExact(
            String key,
            PartitionKey partition,
            T replacement,
            Class<T> type,
            long expectedVersion,
            Supplier<CompletableFuture<Optional<BookKeeperVersionedValue<T>>>> reload,
            BiConsumer<T, T> transition) {
        return reload.get().thenCompose(optional -> {
            BookKeeperVersionedValue<T> current = optional.orElseThrow(
                    () -> new BookKeeperMetadataConditionFailedException("BookKeeper metadata is absent"));
            if (current.metadataVersion() != expectedVersion) {
                return F4MetadataStoreSupport.failed(
                        new BookKeeperMetadataConditionFailedException("BookKeeper metadata version mismatch"));
            }
            transition.accept(current.value(), replacement);
            var expectedDigest = support.encodedDigest(replacement, type);
            CompletableFuture<BookKeeperVersionedValue<T>> write = support.compareAndSet(
                            key, partition, replacement, type, expectedVersion)
                    .thenApply(OxiaJavaBookKeeperMetadataStore::versioned);
            return write.handle((updated, failure) -> {
                if (failure == null) {
                    return CompletableFuture.completedFuture(updated);
                }
                return reload.get().thenCompose(reloaded -> {
                    if (reloaded.isPresent()) {
                        BookKeeperVersionedValue<T> observed = reloaded.orElseThrow();
                        if (observed.metadataVersion() > expectedVersion
                                && observed.durableValueSha256().equals(expectedDigest)) {
                            return CompletableFuture.completedFuture(observed);
                        }
                        if (observed.metadataVersion() != expectedVersion) {
                            return F4MetadataStoreSupport.failed(new BookKeeperMetadataConditionFailedException(
                                    "BookKeeper metadata changed after uncertain CAS"));
                        }
                    }
                    return F4MetadataStoreSupport.failed(F4MetadataStoreSupport.unwrap(failure));
                });
            }).thenCompose(Function.identity());
        });
    }

    private <T> CompletableFuture<Void> deleteExact(
            String key,
            PartitionKey partition,
            long expectedVersion,
            Supplier<CompletableFuture<Optional<BookKeeperVersionedValue<T>>>> reload) {
        CompletableFuture<Void> delete = support.delete(key, partition, expectedVersion);
        return delete.handle((ignored, failure) -> {
            if (failure == null) {
                return CompletableFuture.<Void>completedFuture(null);
            }
            return reload.get().thenCompose(observed -> {
                if (observed.isEmpty()) {
                    return CompletableFuture.<Void>completedFuture(null);
                }
                if (observed.orElseThrow().metadataVersion() != expectedVersion) {
                    return F4MetadataStoreSupport.<Void>failed(new BookKeeperMetadataConditionFailedException(
                            "BookKeeper metadata changed after uncertain conditional delete"));
                }
                return F4MetadataStoreSupport.<Void>failed(F4MetadataStoreSupport.unwrap(failure));
            });
        }).thenCompose(Function.identity());
    }

    private <T> CompletableFuture<BookKeeperScanPage<BookKeeperVersionedValue<T>>> scan(
            String cluster,
            BookKeeperScanKind kind,
            String canonicalScope,
            String basePrefix,
            int descendantSegments,
            PartitionKey partition,
            Optional<BookKeeperScanToken> continuation,
            int limit,
            Class<T> type,
            BiConsumer<String, T> keyValidator) {
        requirePageLimit(limit);
        String prefix = F4MetadataStoreSupport.fixedDepthStart(basePrefix, descendantSegments);
        String end = F4MetadataStoreSupport.fixedDepthEnd(basePrefix, descendantSegments);
        String scopeSha256 = support.scopeSha256(kind.name() + "\0" + canonicalScope + "\0" + basePrefix);
        BookKeeperScanToken token = validateToken(
                continuation, cluster, kind, scopeSha256, prefix, limit);
        String start = token == null ? prefix : token.resumeFromInclusive();
        return support.client().rangeScan(start, end, limit + 1, partition).thenApply(rows -> {
            boolean hasMore = rows.size() > limit;
            List<BookKeeperVersionedValue<T>> values = rows.stream().limit(limit).map(row -> {
                F4MetadataStoreSupport.Decoded<T> decoded = support.decode(row, type);
                keyValidator.accept(decoded.key(), decoded.value());
                return versioned(decoded);
            }).toList();
            Optional<BookKeeperScanToken> next = hasMore
                    ? Optional.of(new BookKeeperScanToken(cluster, kind, scopeSha256, prefix,
                            values.get(values.size() - 1).key(), limit))
                    : Optional.empty();
            return new BookKeeperScanPage<>(values, next);
        });
    }

    private static BookKeeperScanToken validateToken(
            Optional<BookKeeperScanToken> continuation,
            String cluster,
            BookKeeperScanKind kind,
            String scopeSha256,
            String prefix,
            int limit) {
        Objects.requireNonNull(continuation, "continuation");
        if (continuation.isEmpty()) {
            return null;
        }
        BookKeeperScanToken token = continuation.orElseThrow();
        if (!token.cluster().equals(cluster) || token.kind() != kind
                || !token.scopeIdentitySha256().equals(scopeSha256)
                || !token.scanPrefix().equals(prefix)
                || token.pageSize() != limit) {
            throw new IllegalArgumentException("BookKeeper scan continuation belongs to another scan scope");
        }
        return token;
    }

    private static void requirePageLimit(int limit) {
        if (limit <= 0 || limit > 1_024) {
            throw new IllegalArgumentException("scan limit must be in [1,1024]");
        }
    }

    private static void validateRoot(
            BookKeeperKeyspace keys, String key, BookKeeperLedgerRootRecord value) {
        keys.requireAllocationSlot(value.allocationSlot());
        String expectedIdentity = keys.ledgerIdentitySha256(value.providerScopeSha256(), value.ledgerId());
        if (!expectedIdentity.equals(value.ledgerIdentitySha256())) {
            throw F4MetadataStoreSupport.invariant("BookKeeper root ledger identity digest mismatch");
        }
        keys.parseRootKey(requireKey(key, keys.ledgerRootKey(value.providerScopeSha256(), value.ledgerId())),
                value.providerScopeSha256(), value.ledgerId());
    }

    private static void validateProtection(
            BookKeeperKeyspace keys,
            String key,
            String providerScopeSha256,
            BookKeeperLedgerProtectionRecord value) {
        keys.requireLedgerRangeSlot(value.ledgerRangeSlot());
        keys.requireProtectionSlot(value.protectionSlot());
        String expectedIdentity = keys.ledgerIdentitySha256(providerScopeSha256, value.ledgerId());
        if (!expectedIdentity.equals(value.ledgerIdentitySha256())) {
            throw F4MetadataStoreSupport.invariant("BookKeeper protection ledger identity digest mismatch");
        }
        keys.parseProtectionKey(requireKey(key, keys.protectionKey(providerScopeSha256, value.ledgerId(),
                        value.ledgerRangeSlot(), value.protectionSlot())),
                providerScopeSha256, value.ledgerId());
    }

    private static void validateReaderLease(
            BookKeeperKeyspace keys,
            String key,
            String providerScopeSha256,
            BookKeeperLedgerReaderLeaseRecord value) {
        keys.requireReaderSlot(value.readerSlot());
        String expectedIdentity = keys.ledgerIdentitySha256(providerScopeSha256, value.ledgerId());
        if (!expectedIdentity.equals(value.ledgerIdentitySha256())) {
            throw F4MetadataStoreSupport.invariant("BookKeeper reader lease ledger identity digest mismatch");
        }
        keys.parseReaderLeaseKey(requireKey(key,
                        keys.readerLeaseKey(providerScopeSha256, value.ledgerId(), value.readerSlot())),
                providerScopeSha256, value.ledgerId());
    }

    private static String requireKey(String actual, String expected) {
        if (!expected.equals(actual)) {
            throw F4MetadataStoreSupport.invariant("BookKeeper metadata record does not match its canonical key");
        }
        return actual;
    }

    private static RuntimeException rethrow(Throwable failure) {
        if (failure instanceof RuntimeException runtime) {
            return runtime;
        }
        return new RuntimeException(failure);
    }

    private void requireWriterBound(BookKeeperWriterStateRecord value) {
        if (value.activeAppendRangeCount() > configuration.maxAppendRangesPerLedger()) {
            throw new IllegalArgumentException("writer activeAppendRangeCount exceeds configured ledger bound");
        }
    }

    private static <T> BookKeeperVersionedValue<T> versioned(F4MetadataStoreSupport.Decoded<T> value) {
        return new BookKeeperVersionedValue<>(
                value.key(), value.value(), value.version(), value.durableSha256());
    }
}
