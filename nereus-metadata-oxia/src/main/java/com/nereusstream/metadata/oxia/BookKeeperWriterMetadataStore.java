/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.records.BookKeeperAllocationSlotRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperAppendReservationRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperWriterStateRecord;
import com.nereusstream.metadata.oxia.records.LedgerAllocationIntentRecord;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Focused stream-writer/allocation/reservation metadata surface. */
public interface BookKeeperWriterMetadataStore extends AutoCloseable {
    CompletableFuture<Optional<BookKeeperVersionedValue<BookKeeperWriterStateRecord>>> getWriter(String cluster, StreamId stream);
    CompletableFuture<BookKeeperVersionedValue<BookKeeperWriterStateRecord>> createWriter(String cluster, BookKeeperWriterStateRecord value);
    CompletableFuture<BookKeeperVersionedValue<BookKeeperWriterStateRecord>> compareAndSetWriter(
            String cluster, BookKeeperWriterStateRecord value, long expectedVersion);

    CompletableFuture<Optional<BookKeeperVersionedValue<LedgerAllocationIntentRecord>>> getAllocation(
            String cluster, StreamId stream, String allocationId);
    CompletableFuture<BookKeeperVersionedValue<LedgerAllocationIntentRecord>> createAllocation(
            String cluster, LedgerAllocationIntentRecord value);
    CompletableFuture<BookKeeperVersionedValue<LedgerAllocationIntentRecord>> compareAndSetAllocation(
            String cluster, LedgerAllocationIntentRecord value, long expectedVersion);
    CompletableFuture<Void> deleteAllocation(String cluster, StreamId stream, String allocationId, long expectedVersion);
    CompletableFuture<BookKeeperScanPage<BookKeeperVersionedValue<LedgerAllocationIntentRecord>>> scanAllocations(
            String cluster, StreamId stream, Optional<BookKeeperScanToken> continuation, int limit);

    CompletableFuture<Optional<BookKeeperVersionedValue<BookKeeperAllocationSlotRecord>>> getAllocationSlot(
            String cluster, int slot);
    CompletableFuture<BookKeeperVersionedValue<BookKeeperAllocationSlotRecord>> createAllocationSlot(
            String cluster, BookKeeperAllocationSlotRecord value);
    CompletableFuture<BookKeeperVersionedValue<BookKeeperAllocationSlotRecord>> compareAndSetAllocationSlot(
            String cluster, BookKeeperAllocationSlotRecord value, long expectedVersion);
    CompletableFuture<Void> deleteAllocationSlot(String cluster, int slot, long expectedVersion);
    CompletableFuture<BookKeeperScanPage<BookKeeperVersionedValue<BookKeeperAllocationSlotRecord>>> scanAllocationSlots(
            String cluster, int slotShard, Optional<BookKeeperScanToken> continuation, int limit);

    CompletableFuture<Optional<BookKeeperVersionedValue<BookKeeperAppendReservationRecord>>> getReservation(
            String cluster, StreamId stream, String reservationId);
    CompletableFuture<BookKeeperVersionedValue<BookKeeperAppendReservationRecord>> createReservation(
            String cluster, BookKeeperAppendReservationRecord value);
    CompletableFuture<BookKeeperVersionedValue<BookKeeperAppendReservationRecord>> compareAndSetReservation(
            String cluster, BookKeeperAppendReservationRecord value, long expectedVersion);
    CompletableFuture<Void> deleteReservation(String cluster, StreamId stream, String reservationId, long expectedVersion);
    CompletableFuture<BookKeeperScanPage<BookKeeperVersionedValue<BookKeeperAppendReservationRecord>>> scanReservations(
            String cluster, StreamId stream, Optional<BookKeeperScanToken> continuation, int limit);

    @Override void close();
}
