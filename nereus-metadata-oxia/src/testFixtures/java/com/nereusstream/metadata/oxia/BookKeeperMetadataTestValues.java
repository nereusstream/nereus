/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.SchemaRef;
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
import java.util.List;

/** Canonical V1 samples shared by BookKeeper codec/store contracts. */
public final class BookKeeperMetadataTestValues {
    public static final String A = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    public static final String B = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    public static final String C = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    public static final String D = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
    public static final String STREAM = "stream-bk-1";
    public static final String ALLOCATION = "abcdefghijklmnopqrstuvwxzy234567";

    private BookKeeperMetadataTestValues() { }

    public static BookKeeperWriterStateRecord writer() {
        return new BookKeeperWriterStateRecord(1, STREAM, "bk-a", A, BookKeeperWriterLifecycle.ACTIVE, 3,
                "writer-1", B, 7, C, 9, 12, "", 0, 11, 101, 4, 3, 300, 2,
                "reservation-1", 100, 120, "", 0);
    }

    public static LedgerAllocationIntentRecord allocation() {
        return new LedgerAllocationIntentRecord(1, ALLOCATION, STREAM, 11, "bk-a", 101, 4, A,
                "writer-1", B, 7, C, 3, LedgerAllocationLifecycle.ACTIVATED, false, D,
                100, 120, "", 0);
    }

    public static BookKeeperAllocationSlotRecord slot() {
        return new BookKeeperAllocationSlotRecord(1, 4, ALLOCATION, STREAM, 101, B, A,
                AllocationSlotLifecycle.CREATE_STARTED, 100, 110, 0);
    }

    public static BookKeeperLedgerRootRecord root() {
        return new BookKeeperLedgerRootRecord(1, B, "bk-a", A, 101, STREAM, 11, ALLOCATION, 4, C, D,
                false, "writer-1", B, 7, C, 3, 3, 2, "CRC32C", A,
                BookKeeperLedgerLifecycle.ACTIVE, 2, 100, 110, 0, 0, -1, 0,
                "", "", "", 0, 0, 0, 0, 0, "", 0);
    }

    public static BookKeeperAppendReservationRecord reservation() {
        return new BookKeeperAppendReservationRecord(1, "reservation-1", "attempt-1", STREAM,
                "writer-1", B, 7, C, 3, 101, 2, 2, 3, 2, D, 17, "PULSAR_ENTRY_V1", 2,
                12, 300, List.of(new SchemaRef("tenant/ns", "schema", 1)), "projection-1",
                1_000, 1_100, AppendReservationLifecycle.DURABLE, "", "", 0, "",
                100, 120, "", 0);
    }

    public static BookKeeperLedgerProtectionRecord protection() {
        return new BookKeeperLedgerProtectionRecord(1, B, "bk-a", 101, 2, 2, 0,
                BookKeeperProtectionType.REACHABLE_APPEND.wireId(), "reference-1", 3, 2, D,
                STREAM, 17, 19, 8, "/owner/key", 5, A, ProtectionLifecycle.ACTIVE,
                120, 0, 0);
    }

    public static BookKeeperLedgerReaderLeaseRecord readerLease() {
        return new BookKeeperLedgerReaderLeaseRecord(1, B, 101, 2, 3, "process-1", 4,
                120, 220, 0);
    }
}
