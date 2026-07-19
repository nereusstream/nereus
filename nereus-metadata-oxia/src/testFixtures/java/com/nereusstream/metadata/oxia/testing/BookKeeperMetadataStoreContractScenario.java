/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.testing;

import com.nereusstream.api.SchemaRef;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.BookKeeperKeyspace;
import com.nereusstream.metadata.oxia.BookKeeperLedgerMetadataStore;
import com.nereusstream.metadata.oxia.BookKeeperMetadataStoreConfig;
import com.nereusstream.metadata.oxia.BookKeeperScanPage;
import com.nereusstream.metadata.oxia.BookKeeperScanToken;
import com.nereusstream.metadata.oxia.BookKeeperVersionedValue;
import com.nereusstream.metadata.oxia.BookKeeperWriterMetadataStore;
import com.nereusstream.metadata.oxia.records.AllocationSlotLifecycle;
import com.nereusstream.metadata.oxia.records.AppendReservationLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperAllocationSlotRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperAppendReservationRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerProtectionRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerReaderLeaseRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperProtectionType;
import com.nereusstream.metadata.oxia.records.BookKeeperWriterLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperWriterStateRecord;
import com.nereusstream.metadata.oxia.records.LedgerAllocationIntentRecord;
import com.nereusstream.metadata.oxia.records.LedgerAllocationLifecycle;
import com.nereusstream.metadata.oxia.records.ProtectionLifecycle;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionException;

/** Store-independent BookKeeper metadata sequence shared by fake and production adapters. */
public final class BookKeeperMetadataStoreContractScenario {
    public static final String CLUSTER = "cluster/bk-contract";
    public static final String PROVIDER_SCOPE = "a".repeat(64);
    public static final String CONFIGURATION = "b".repeat(64);
    public static final String WRITER_RUN = "c".repeat(64);
    public static final String FENCE = "d".repeat(64);
    public static final String CUSTOM_METADATA = "e".repeat(64);
    public static final String RANGE_CHECKSUM = "f".repeat(64);
    public static final StreamId STREAM = new StreamId("stream-bk-contract");

    private BookKeeperMetadataStoreContractScenario() { }

    public static Result run(
            BookKeeperWriterMetadataStore writerStore,
            BookKeeperLedgerMetadataStore ledgerStore,
            BookKeeperMetadataStoreConfig configuration) {
        BookKeeperKeyspace keys = configuration.keyspace(CLUSTER);
        long ledgerId = 101;
        String ledgerIdentity = keys.ledgerIdentitySha256(PROVIDER_SCOPE, ledgerId);

        BookKeeperWriterStateRecord writer = activeWriter(3, 3, 300, 2);
        BookKeeperVersionedValue<BookKeeperWriterStateRecord> createdWriter =
                writerStore.createWriter(CLUSTER, writer).join();
        require(writerStore.createWriter(CLUSTER, writer).join().equals(createdWriter),
                "writer create is not idempotent");
        BookKeeperVersionedValue<BookKeeperWriterStateRecord> updatedWriter = writerStore.compareAndSetWriter(
                CLUSTER, activeWriter(4, 5, 350, 3), createdWriter.metadataVersion()).join();
        requireCondition(() -> writerStore.compareAndSetWriter(
                CLUSTER, activeWriter(5, 6, 360, 4), createdWriter.metadataVersion()).join());

        LedgerAllocationIntentRecord allocation = allocation(LedgerAllocationLifecycle.PREPARED, false, "", 120);
        BookKeeperVersionedValue<LedgerAllocationIntentRecord> createdAllocation =
                writerStore.createAllocation(CLUSTER, allocation).join();
        require(writerStore.createAllocation(CLUSTER, allocation).join().equals(createdAllocation),
                "allocation create is not idempotent");
        BookKeeperVersionedValue<LedgerAllocationIntentRecord> updatedAllocation =
                writerStore.compareAndSetAllocation(CLUSTER,
                        allocation(LedgerAllocationLifecycle.ROOT_RESERVED, false, "", 130),
                        createdAllocation.metadataVersion()).join();

        BookKeeperAllocationSlotRecord slot = slot(ledgerIdentity, AllocationSlotLifecycle.CLAIMED, 120);
        BookKeeperVersionedValue<BookKeeperAllocationSlotRecord> createdSlot =
                writerStore.createAllocationSlot(CLUSTER, slot).join();
        BookKeeperVersionedValue<BookKeeperAllocationSlotRecord> updatedSlot =
                writerStore.compareAndSetAllocationSlot(CLUSTER,
                        slot(ledgerIdentity, AllocationSlotLifecycle.CREATE_STARTED, 130),
                        createdSlot.metadataVersion()).join();

        BookKeeperAppendReservationRecord reservation = reservation(AppendReservationLifecycle.DURABLE, 120);
        BookKeeperVersionedValue<BookKeeperAppendReservationRecord> createdReservation =
                writerStore.createReservation(CLUSTER, reservation).join();
        BookKeeperVersionedValue<BookKeeperAppendReservationRecord> updatedReservation =
                writerStore.compareAndSetReservation(
                        CLUSTER, committedReservation(AppendReservationLifecycle.COMMIT_PREPARED, 130),
                        createdReservation.metadataVersion()).join();

        BookKeeperLedgerRootRecord root = root(keys, ledgerId, BookKeeperLedgerLifecycle.ACTIVE, 2);
        BookKeeperVersionedValue<BookKeeperLedgerRootRecord> createdRoot = ledgerStore.createRoot(CLUSTER, root).join();
        require(ledgerStore.createRoot(CLUSTER, root).join().equals(createdRoot),
                "root create is not idempotent");
        BookKeeperVersionedValue<BookKeeperLedgerRootRecord> updatedRoot = ledgerStore.compareAndSetRoot(
                CLUSTER, root(keys, ledgerId, BookKeeperLedgerLifecycle.SEALING, 3),
                createdRoot.metadataVersion()).join();

        BookKeeperLedgerProtectionRecord protection = protection(ledgerIdentity, ProtectionLifecycle.RESERVED);
        BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> createdProtection =
                ledgerStore.createProtection(CLUSTER, PROVIDER_SCOPE, protection).join();
        BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> updatedProtection =
                ledgerStore.compareAndSetProtection(CLUSTER, PROVIDER_SCOPE,
                        protection(ledgerIdentity, ProtectionLifecycle.ACTIVE),
                        createdProtection.metadataVersion()).join();

        BookKeeperLedgerReaderLeaseRecord lease = readerLease(ledgerIdentity, 1, 220);
        BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord> createdLease =
                ledgerStore.createReaderLease(CLUSTER, PROVIDER_SCOPE, lease).join();
        BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord> updatedLease =
                ledgerStore.compareAndSetReaderLease(
                        CLUSTER, PROVIDER_SCOPE, readerLease(ledgerIdentity, 2, 320),
                        createdLease.metadataVersion()).join();

        require(scanAllAllocations(writerStore).equals(List.of(updatedAllocation)),
                "allocation scan was incomplete");
        require(scanAllReservations(writerStore).equals(List.of(updatedReservation)),
                "reservation scan was incomplete");
        require(ledgerStore.scanRoots(CLUSTER, keys.ledgerShard(ledgerIdentity), Optional.empty(), 1)
                        .join().values().equals(List.of(updatedRoot)),
                "root scan was incomplete");
        require(ledgerStore.scanProtections(CLUSTER, PROVIDER_SCOPE, ledgerId, Optional.empty(), 1)
                        .join().values().equals(List.of(updatedProtection)),
                "protection scan was incomplete");
        require(ledgerStore.scanReaderLeases(CLUSTER, PROVIDER_SCOPE, ledgerId, Optional.empty(), 1)
                        .join().values().equals(List.of(updatedLease)),
                "reader scan was incomplete");

        BookKeeperScanToken wrongPageToken = writerStore.scanAllocations(
                CLUSTER, STREAM, Optional.empty(), 1).join().continuation().orElse(null);
        if (wrongPageToken != null) {
            requireIllegalArgument(() -> writerStore.scanAllocations(
                    CLUSTER, STREAM, Optional.of(wrongPageToken), 2));
        }

        ledgerStore.deleteProtection(CLUSTER, PROVIDER_SCOPE, ledgerId, 2, 0,
                updatedProtection.metadataVersion()).join();
        ledgerStore.deleteReaderLease(CLUSTER, PROVIDER_SCOPE, ledgerId, 3,
                updatedLease.metadataVersion()).join();
        writerStore.deleteReservation(CLUSTER, STREAM, reservation.reservationId(),
                updatedReservation.metadataVersion()).join();
        writerStore.deleteAllocation(CLUSTER, STREAM, allocation.allocationId(),
                updatedAllocation.metadataVersion()).join();
        writerStore.deleteAllocationSlot(CLUSTER, slot.slot(), updatedSlot.metadataVersion()).join();

        require(ledgerStore.getProtection(CLUSTER, PROVIDER_SCOPE, ledgerId, 2, 0).join().isEmpty(),
                "protection conditional delete did not apply");
        require(ledgerStore.getReaderLease(CLUSTER, PROVIDER_SCOPE, ledgerId, 3).join().isEmpty(),
                "reader conditional delete did not apply");
        require(writerStore.getReservation(CLUSTER, STREAM, reservation.reservationId()).join().isEmpty(),
                "reservation conditional delete did not apply");

        return new Result(updatedWriter.metadataVersion(), updatedRoot.metadataVersion(),
                updatedWriter.durableValueSha256().value(), updatedRoot.durableValueSha256().value());
    }

    private static List<BookKeeperVersionedValue<LedgerAllocationIntentRecord>> scanAllAllocations(
            BookKeeperWriterMetadataStore store) {
        List<BookKeeperVersionedValue<LedgerAllocationIntentRecord>> values = new ArrayList<>();
        Optional<BookKeeperScanToken> continuation = Optional.empty();
        do {
            BookKeeperScanPage<BookKeeperVersionedValue<LedgerAllocationIntentRecord>> page =
                    store.scanAllocations(CLUSTER, STREAM, continuation, 1).join();
            values.addAll(page.values());
            continuation = page.continuation();
        } while (continuation.isPresent());
        return values;
    }

    private static List<BookKeeperVersionedValue<BookKeeperAppendReservationRecord>> scanAllReservations(
            BookKeeperWriterMetadataStore store) {
        List<BookKeeperVersionedValue<BookKeeperAppendReservationRecord>> values = new ArrayList<>();
        Optional<BookKeeperScanToken> continuation = Optional.empty();
        do {
            BookKeeperScanPage<BookKeeperVersionedValue<BookKeeperAppendReservationRecord>> page =
                    store.scanReservations(CLUSTER, STREAM, continuation, 1).join();
            values.addAll(page.values());
            continuation = page.continuation();
        } while (continuation.isPresent());
        return values;
    }

    public static BookKeeperWriterStateRecord activeWriter(
            long epoch, long nextEntryId, long bytes, int ranges) {
        return new BookKeeperWriterStateRecord(1, STREAM.value(), "bk-a", CONFIGURATION,
                BookKeeperWriterLifecycle.ACTIVE, epoch, "writer-1", WRITER_RUN, 7, FENCE, 9,
                12, "", 0, 11, 101, 2, nextEntryId, bytes, ranges,
                "reservation-1", 100, 100 + epoch, "", 0);
    }

    public static LedgerAllocationIntentRecord allocation(
            LedgerAllocationLifecycle lifecycle, boolean hazard, String metadataSha256, long updatedAt) {
        return new LedgerAllocationIntentRecord(1, "allocation-contract", STREAM.value(), 11, "bk-a", 101, 4,
                CONFIGURATION, "writer-1", WRITER_RUN, 7, FENCE, 3, lifecycle, hazard, metadataSha256,
                100, updatedAt, "", 0);
    }

    public static BookKeeperAllocationSlotRecord slot(
            String ledgerIdentity, AllocationSlotLifecycle lifecycle, long updatedAt) {
        return new BookKeeperAllocationSlotRecord(1, 4, "allocation-contract", STREAM.value(), 101,
                ledgerIdentity, CONFIGURATION, lifecycle, 100, updatedAt, 0);
    }

    public static BookKeeperAppendReservationRecord reservation(
            AppendReservationLifecycle lifecycle, long updatedAt) {
        return new BookKeeperAppendReservationRecord(1, "reservation-1", "attempt-1", STREAM.value(),
                "writer-1", WRITER_RUN, 7, FENCE, 3, 101, 2, 2, 3, 2, RANGE_CHECKSUM,
                17, "PULSAR_ENTRY_V1", 2, 12, 300,
                List.of(new SchemaRef("tenant/ns", "schema", 1)), "projection-1", 1_000, 1_100,
                lifecycle, "", "", 0, "", 100, updatedAt,
                lifecycle == AppendReservationLifecycle.ABANDONED ? "abandoned" : "", 0);
    }

    public static BookKeeperAppendReservationRecord committedReservation(
            AppendReservationLifecycle lifecycle, long updatedAt) {
        BookKeeperAppendReservationRecord base = reservation(AppendReservationLifecycle.DURABLE, updatedAt);
        return new BookKeeperAppendReservationRecord(base.schemaVersion(), base.reservationId(),
                base.appendAttemptId(), base.streamId(), base.writerId(), base.writerRunIdHash(),
                base.appendSessionEpoch(), base.fencingTokenHash(), base.writerStateEpoch(), base.ledgerId(),
                base.ledgerRootEpoch(), base.ledgerRangeSlot(), base.firstEntryId(), base.entryCount(),
                base.rangeChecksumSha256(), base.expectedStartOffset(), base.payloadFormat(), base.recordCount(),
                base.logicalBytes(), base.physicalBytes(), base.schemaRefs(), base.projectionIdentity(),
                base.minEventTimeMillis(), base.maxEventTimeMillis(), lifecycle, "commit-1", "/commit/key", 9,
                CONFIGURATION, base.createdAtMillis(), updatedAt, "", 0);
    }

    public static BookKeeperLedgerRootRecord root(
            BookKeeperKeyspace keys, long ledgerId, BookKeeperLedgerLifecycle lifecycle, long epoch) {
        String identity = keys.ledgerIdentitySha256(PROVIDER_SCOPE, ledgerId);
        return new BookKeeperLedgerRootRecord(1, identity, "bk-a", PROVIDER_SCOPE, ledgerId, STREAM.value(), 11,
                "allocation-contract", 4, CONFIGURATION, CUSTOM_METADATA, false, "writer-1", WRITER_RUN, 7,
                FENCE, 3, 3, 2, "CRC32C", CUSTOM_METADATA, lifecycle, epoch, 100, 110,
                0, 0, -1, 0, "", "", "", 0, 0, 0, 0, 0,
                lifecycle == BookKeeperLedgerLifecycle.QUARANTINED ? "quarantined" : "", 0);
    }

    public static BookKeeperLedgerProtectionRecord protection(
            String ledgerIdentity, ProtectionLifecycle lifecycle) {
        boolean active = lifecycle == ProtectionLifecycle.ACTIVE;
        return new BookKeeperLedgerProtectionRecord(1, ledgerIdentity, "bk-a", 101, 2, 2, 0,
                BookKeeperProtectionType.REACHABLE_APPEND.wireId(), "reference-1", 3, 2, RANGE_CHECKSUM,
                STREAM.value(), 17, 19, 8, active ? "/owner/key" : "", active ? 5 : 0,
                active ? CONFIGURATION : "", lifecycle, 120, 0, 0);
    }

    public static BookKeeperLedgerReaderLeaseRecord readerLease(
            String ledgerIdentity, long leaseEpoch, long expiresAt) {
        return new BookKeeperLedgerReaderLeaseRecord(1, ledgerIdentity, 101, 2, 3, "process-1",
                leaseEpoch, 120, expiresAt, 0);
    }

    private static void requireCondition(Runnable operation) {
        try {
            operation.run();
            throw new AssertionError("stale metadata condition unexpectedly succeeded");
        } catch (CompletionException expected) {
            // Any exact-version condition from the backing adapter is acceptable here.
        }
    }

    private static void requireIllegalArgument(Runnable operation) {
        try {
            operation.run();
            throw new AssertionError("cross-scope continuation unexpectedly succeeded");
        } catch (IllegalArgumentException expected) {
            // Expected.
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    public record Result(
            long writerVersion, long rootVersion, String writerDurableSha256, String rootDurableSha256) { }
}
