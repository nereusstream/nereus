/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import com.nereusstream.metadata.oxia.CursorNames;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import java.util.List;
import java.util.TreeMap;

final class CursorTestSamples {
    static final String CLUSTER = "cluster/a";
    static final String TOPIC = "persistent://tenant/ns/snapshot";
    static final String CURSOR = "subscription-a";
    static final String SNAPSHOT = "ffeeddccbbaa99887766554433221100";

    private CursorTestSamples() {
    }

    static CursorIdentity identity() {
        ManagedLedgerProjectionIdentity projection = new ManagedLedgerProjectionIdentity(
                3,
                2,
                ManagedLedgerProjectionNames.streamId(TOPIC, 2).value(),
                ManagedLedgerProjectionNames.MIN_VIRTUAL_LEDGER_ID + 9);
        CursorLedgerIdentity ledger = new CursorLedgerIdentity(
                TOPIC,
                ManagedLedgerProjectionNames.managedLedgerNameHash(TOPIC),
                projection);
        return new CursorIdentity(
                ledger, CURSOR, CursorNames.cursorNameHash(CURSOR), 1);
    }

    static CursorAckState complexState() {
        TreeMap<Long, BatchAckState> partials = new TreeMap<>();
        partials.put(15L, new BatchAckState(65, new long[] {-2L, 1L}));
        return new CursorAckState(
                10,
                List.of(new OffsetRange(12, 14), new OffsetRange(20, 21)),
                partials);
    }

    static CursorSnapshotWriteRequest request() {
        return new CursorSnapshotWriteRequest(identity(), 8, complexState(), 105);
    }
}
