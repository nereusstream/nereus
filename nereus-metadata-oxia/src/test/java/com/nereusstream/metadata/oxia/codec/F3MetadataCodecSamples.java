/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.CursorNames;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import com.nereusstream.metadata.oxia.records.CursorAckRangeRecord;
import com.nereusstream.metadata.oxia.records.CursorPartialBatchAckRecord;
import com.nereusstream.metadata.oxia.records.CursorProtectionIntentRecord;
import com.nereusstream.metadata.oxia.records.CursorProtectionKind;
import com.nereusstream.metadata.oxia.records.CursorRecordLifecycle;
import com.nereusstream.metadata.oxia.records.CursorRetentionLifecycle;
import com.nereusstream.metadata.oxia.records.CursorRetentionRecord;
import com.nereusstream.metadata.oxia.records.CursorSnapshotReferenceRecord;
import com.nereusstream.metadata.oxia.records.CursorStateRecord;
import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

final class F3MetadataCodecSamples {
    static final String TOPIC = "persistent://tenant/ns/f3-topic";
    static final String CURSOR = "subscription-a";
    static final String OWNER = "00112233445566778899aabbccddeeff";
    static final String ATTEMPT = "102132435465768798a9bacbdcedfe0f";
    static final String SNAPSHOT = "ffeeddccbbaa99887766554433221100";
    static final String TRIM_ATTEMPT = "11112222333344445555666677778888";

    private F3MetadataCodecSamples() {
    }

    static ManagedLedgerProjectionIdentity projection() {
        return new ManagedLedgerProjectionIdentity(
                3,
                2,
                ManagedLedgerProjectionNames.streamId(TOPIC, 2).value(),
                ManagedLedgerProjectionNames.MIN_VIRTUAL_LEDGER_ID + 9);
    }

    static CursorStateRecord minimalActive() {
        return new CursorStateRecord(
                0,
                projection(),
                OWNER,
                CURSOR,
                CursorNames.cursorNameHash(CURSOR),
                1,
                CursorRecordLifecycle.ACTIVE,
                1,
                1,
                ATTEMPT,
                0,
                Optional.empty(),
                List.of(),
                List.of(),
                Map.of(),
                Map.of(),
                100,
                100,
                OptionalLong.empty());
    }

    static CursorStateRecord fullActive() {
        CursorSnapshotReferenceRecord reference = new CursorSnapshotReferenceRecord(
                "cluster/cursor-snapshots/v1/stream/hash/0000000000000000001/" + SNAPSHOT + ".ncs",
                SNAPSHOT,
                1,
                8,
                8,
                4096,
                "CRC32C",
                "89abcdef",
                0x87654321,
                1,
                105);
        return new CursorStateRecord(
                0,
                projection(),
                OWNER,
                CURSOR,
                CursorNames.cursorNameHash(CURSOR),
                1,
                CursorRecordLifecycle.ACTIVE,
                9,
                2,
                ATTEMPT,
                10,
                Optional.of(reference),
                List.of(new CursorAckRangeRecord(12, 14), new CursorAckRangeRecord(20, 21)),
                List.of(new CursorPartialBatchAckRecord(15, 65, new long[] {-2L, 1L})),
                Map.of("z", Long.MIN_VALUE, "a", Long.MAX_VALUE),
                Map.of("external", "value", "#pulsar.internal.key", "internal"),
                100,
                110,
                OptionalLong.empty());
    }

    static CursorStateRecord deleted() {
        return new CursorStateRecord(
                0,
                projection(),
                OWNER,
                CURSOR,
                CursorNames.cursorNameHash(CURSOR),
                1,
                CursorRecordLifecycle.DELETED,
                10,
                2,
                ATTEMPT,
                10,
                Optional.empty(),
                List.of(),
                List.of(),
                Map.of(),
                Map.of(),
                100,
                111,
                OptionalLong.of(111));
    }

    static CursorProtectionIntentRecord createIntent() {
        return new CursorProtectionIntentRecord(
                ATTEMPT,
                CursorProtectionKind.CREATE,
                CURSOR,
                CursorNames.cursorNameHash(CURSOR),
                0,
                1,
                7,
                Optional.empty(),
                Map.of("position", -1L),
                Map.of("cursor", "value"),
                200);
    }

    static CursorProtectionIntentRecord recreateIntent() {
        return new CursorProtectionIntentRecord(
                ATTEMPT,
                CursorProtectionKind.RECREATE,
                CURSOR,
                CursorNames.cursorNameHash(CURSOR),
                2,
                3,
                9,
                Optional.empty(),
                Map.of(),
                Map.of(),
                201);
    }

    static CursorProtectionIntentRecord backwardResetIntent() {
        return new CursorProtectionIntentRecord(
                ATTEMPT,
                CursorProtectionKind.BACKWARD_RESET,
                CURSOR,
                CursorNames.cursorNameHash(CURSOR),
                3,
                3,
                5,
                Optional.of(new CursorPartialBatchAckRecord(5, 65, new long[] {-2L, 1L})),
                Map.of(),
                Map.of(),
                202);
    }

    static CursorRetentionRecord activeRetention() {
        return new CursorRetentionRecord(
                0,
                projection(),
                OWNER,
                CursorRetentionLifecycle.ACTIVE,
                1,
                7,
                4,
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                200);
    }

    static CursorRetentionRecord protectionRetention(CursorProtectionIntentRecord intent) {
        return new CursorRetentionRecord(
                0,
                projection(),
                OWNER,
                CursorRetentionLifecycle.PROTECTION_PENDING,
                2,
                4,
                4,
                Optional.of(intent),
                Optional.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                201);
    }

    static CursorRetentionRecord trimRetention() {
        return new CursorRetentionRecord(
                0,
                projection(),
                OWNER,
                CursorRetentionLifecycle.TRIM_PENDING,
                3,
                7,
                4,
                Optional.empty(),
                Optional.of(TRIM_ATTEMPT),
                OptionalLong.of(7),
                Optional.of("nereus-cursor-retention/" + TRIM_ATTEMPT + ":policy"),
                202);
    }
}
