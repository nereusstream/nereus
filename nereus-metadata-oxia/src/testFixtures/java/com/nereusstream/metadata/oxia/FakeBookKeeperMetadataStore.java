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
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Deterministic in-memory BookKeeper metadata adapter for lifecycle and recovery tests. */
public final class FakeBookKeeperMetadataStore
        implements BookKeeperWriterMetadataStore, BookKeeperLedgerMetadataStore {
    private final OxiaJavaBookKeeperMetadataStore delegate;
    private int createProtectionCalls;
    private int failCreateProtectionCall;

    public FakeBookKeeperMetadataStore(BookKeeperMetadataStoreConfig configuration) {
        this(configuration, Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC));
    }

    public FakeBookKeeperMetadataStore(BookKeeperMetadataStoreConfig configuration, Clock clock) {
        this(configuration, clock, new InMemoryPartitionedOxiaBackend());
    }

    public FakeBookKeeperMetadataStore(
            BookKeeperMetadataStoreConfig configuration,
            Clock clock,
            ResponseLossPartitionedOxiaBackend backend) {
        this(configuration, clock, (PartitionedOxiaClient.Backend) backend);
    }

    private FakeBookKeeperMetadataStore(
            BookKeeperMetadataStoreConfig configuration,
            Clock clock,
            PartitionedOxiaClient.Backend backend) {
        this.delegate = new OxiaJavaBookKeeperMetadataStore(
                new PartitionedOxiaClient(backend), clock, configuration);
    }

    @Override
    public CompletableFuture<Optional<BookKeeperVersionedValue<BookKeeperWriterStateRecord>>> getWriter(
            String cluster, StreamId stream) {
        return delegate.getWriter(cluster, stream);
    }

    @Override
    public CompletableFuture<BookKeeperVersionedValue<BookKeeperWriterStateRecord>> createWriter(
            String cluster, BookKeeperWriterStateRecord value) {
        return delegate.createWriter(cluster, value);
    }

    @Override
    public CompletableFuture<BookKeeperVersionedValue<BookKeeperWriterStateRecord>> compareAndSetWriter(
            String cluster, BookKeeperWriterStateRecord value, long expectedVersion) {
        return delegate.compareAndSetWriter(cluster, value, expectedVersion);
    }

    @Override
    public CompletableFuture<Optional<BookKeeperVersionedValue<LedgerAllocationIntentRecord>>> getAllocation(
            String cluster, StreamId stream, String allocationId) {
        return delegate.getAllocation(cluster, stream, allocationId);
    }

    @Override
    public CompletableFuture<BookKeeperVersionedValue<LedgerAllocationIntentRecord>> createAllocation(
            String cluster, LedgerAllocationIntentRecord value) {
        return delegate.createAllocation(cluster, value);
    }

    @Override
    public CompletableFuture<BookKeeperVersionedValue<LedgerAllocationIntentRecord>> compareAndSetAllocation(
            String cluster, LedgerAllocationIntentRecord value, long expectedVersion) {
        return delegate.compareAndSetAllocation(cluster, value, expectedVersion);
    }

    @Override
    public CompletableFuture<Void> deleteAllocation(
            String cluster, StreamId stream, String allocationId, long expectedVersion) {
        return delegate.deleteAllocation(cluster, stream, allocationId, expectedVersion);
    }

    @Override
    public CompletableFuture<BookKeeperScanPage<BookKeeperVersionedValue<LedgerAllocationIntentRecord>>>
            scanAllocations(String cluster, StreamId stream, Optional<BookKeeperScanToken> continuation, int limit) {
        return delegate.scanAllocations(cluster, stream, continuation, limit);
    }

    @Override
    public CompletableFuture<Optional<BookKeeperVersionedValue<BookKeeperAllocationSlotRecord>>> getAllocationSlot(
            String cluster, int slot) {
        return delegate.getAllocationSlot(cluster, slot);
    }

    @Override
    public CompletableFuture<BookKeeperVersionedValue<BookKeeperAllocationSlotRecord>> createAllocationSlot(
            String cluster, BookKeeperAllocationSlotRecord value) {
        return delegate.createAllocationSlot(cluster, value);
    }

    @Override
    public CompletableFuture<BookKeeperVersionedValue<BookKeeperAllocationSlotRecord>> compareAndSetAllocationSlot(
            String cluster, BookKeeperAllocationSlotRecord value, long expectedVersion) {
        return delegate.compareAndSetAllocationSlot(cluster, value, expectedVersion);
    }

    @Override
    public CompletableFuture<Void> deleteAllocationSlot(String cluster, int slot, long expectedVersion) {
        return delegate.deleteAllocationSlot(cluster, slot, expectedVersion);
    }

    @Override
    public CompletableFuture<BookKeeperScanPage<BookKeeperVersionedValue<BookKeeperAllocationSlotRecord>>>
            scanAllocationSlots(
                    String cluster, int slotShard, Optional<BookKeeperScanToken> continuation, int limit) {
        return delegate.scanAllocationSlots(cluster, slotShard, continuation, limit);
    }

    @Override
    public CompletableFuture<Optional<BookKeeperVersionedValue<BookKeeperAppendReservationRecord>>> getReservation(
            String cluster, StreamId stream, String reservationId) {
        return delegate.getReservation(cluster, stream, reservationId);
    }

    @Override
    public CompletableFuture<BookKeeperVersionedValue<BookKeeperAppendReservationRecord>> createReservation(
            String cluster, BookKeeperAppendReservationRecord value) {
        return delegate.createReservation(cluster, value);
    }

    @Override
    public CompletableFuture<BookKeeperVersionedValue<BookKeeperAppendReservationRecord>> compareAndSetReservation(
            String cluster, BookKeeperAppendReservationRecord value, long expectedVersion) {
        return delegate.compareAndSetReservation(cluster, value, expectedVersion);
    }

    @Override
    public CompletableFuture<Void> deleteReservation(
            String cluster, StreamId stream, String reservationId, long expectedVersion) {
        return delegate.deleteReservation(cluster, stream, reservationId, expectedVersion);
    }

    @Override
    public CompletableFuture<BookKeeperScanPage<BookKeeperVersionedValue<BookKeeperAppendReservationRecord>>>
            scanReservations(String cluster, StreamId stream, Optional<BookKeeperScanToken> continuation, int limit) {
        return delegate.scanReservations(cluster, stream, continuation, limit);
    }

    @Override
    public CompletableFuture<Optional<BookKeeperVersionedValue<BookKeeperLedgerRootRecord>>> getRoot(
            String cluster, String providerScopeSha256, long ledgerId) {
        return delegate.getRoot(cluster, providerScopeSha256, ledgerId);
    }

    @Override
    public CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerRootRecord>> createRoot(
            String cluster, BookKeeperLedgerRootRecord value) {
        return delegate.createRoot(cluster, value);
    }

    @Override
    public CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerRootRecord>> compareAndSetRoot(
            String cluster, BookKeeperLedgerRootRecord value, long expectedVersion) {
        return delegate.compareAndSetRoot(cluster, value, expectedVersion);
    }

    @Override
    public CompletableFuture<BookKeeperScanPage<BookKeeperVersionedValue<BookKeeperLedgerRootRecord>>> scanRoots(
            String cluster, int shard, Optional<BookKeeperScanToken> continuation, int limit) {
        return delegate.scanRoots(cluster, shard, continuation, limit);
    }

    @Override
    public CompletableFuture<Optional<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>>> getProtection(
            String cluster, String providerScopeSha256, long ledgerId, int rangeSlot, int protectionSlot) {
        return delegate.getProtection(cluster, providerScopeSha256, ledgerId, rangeSlot, protectionSlot);
    }

    @Override
    public CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>> createProtection(
            String cluster, String providerScopeSha256, BookKeeperLedgerProtectionRecord value) {
        createProtectionCalls++;
        if (createProtectionCalls == failCreateProtectionCall) {
            failCreateProtectionCall = 0;
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "injected BookKeeper protection create failure"));
        }
        return delegate.createProtection(cluster, providerScopeSha256, value);
    }

    public void failCreateProtectionCall(int call) {
        if (call <= 0) throw new IllegalArgumentException("call must be positive");
        failCreateProtectionCall = call;
    }

    @Override
    public CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>> compareAndSetProtection(
            String cluster, String providerScopeSha256, BookKeeperLedgerProtectionRecord value,
            long expectedVersion) {
        return delegate.compareAndSetProtection(cluster, providerScopeSha256, value, expectedVersion);
    }

    @Override
    public CompletableFuture<Void> deleteProtection(
            String cluster, String providerScopeSha256, long ledgerId, int rangeSlot, int protectionSlot,
            long expectedVersion) {
        return delegate.deleteProtection(
                cluster, providerScopeSha256, ledgerId, rangeSlot, protectionSlot, expectedVersion);
    }

    @Override
    public CompletableFuture<BookKeeperScanPage<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>>>
            scanProtections(String cluster, String providerScopeSha256, long ledgerId,
                    Optional<BookKeeperScanToken> continuation, int limit) {
        return delegate.scanProtections(cluster, providerScopeSha256, ledgerId, continuation, limit);
    }

    @Override
    public CompletableFuture<Optional<BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord>>> getReaderLease(
            String cluster, String providerScopeSha256, long ledgerId, int readerSlot) {
        return delegate.getReaderLease(cluster, providerScopeSha256, ledgerId, readerSlot);
    }

    @Override
    public CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord>> createReaderLease(
            String cluster, String providerScopeSha256, BookKeeperLedgerReaderLeaseRecord value) {
        return delegate.createReaderLease(cluster, providerScopeSha256, value);
    }

    @Override
    public CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord>> compareAndSetReaderLease(
            String cluster, String providerScopeSha256, BookKeeperLedgerReaderLeaseRecord value,
            long expectedVersion) {
        return delegate.compareAndSetReaderLease(cluster, providerScopeSha256, value, expectedVersion);
    }

    @Override
    public CompletableFuture<Void> deleteReaderLease(
            String cluster, String providerScopeSha256, long ledgerId, int readerSlot, long expectedVersion) {
        return delegate.deleteReaderLease(cluster, providerScopeSha256, ledgerId, readerSlot, expectedVersion);
    }

    @Override
    public CompletableFuture<BookKeeperScanPage<BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord>>>
            scanReaderLeases(String cluster, String providerScopeSha256, long ledgerId,
                    Optional<BookKeeperScanToken> continuation, int limit) {
        return delegate.scanReaderLeases(cluster, providerScopeSha256, ledgerId, continuation, limit);
    }

    @Override
    public void close() {
        delegate.close();
    }
}
