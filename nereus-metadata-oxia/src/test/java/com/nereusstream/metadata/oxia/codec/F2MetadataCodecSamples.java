/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.api.StorageProfile;
import com.nereusstream.metadata.oxia.ManagedLedgerFacadeState;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import com.nereusstream.metadata.oxia.records.LedgerIdAllocatorRecord;
import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import com.nereusstream.metadata.oxia.records.PositionIndexRecord;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import com.nereusstream.metadata.oxia.records.VirtualLedgerProjectionRecord;
import java.util.List;
import java.util.Map;

final class F2MetadataCodecSamples {
    static final String NAME = "tenant/ns/persistent/topic";
    static final long LEDGER_ID = ManagedLedgerProjectionNames.MIN_VIRTUAL_LEDGER_ID + 7;

    private F2MetadataCodecSamples() {
    }

    static List<Sample<?>> samples() {
        TopicProjectionRecord topic = topic(0);
        ManagedLedgerProjectionIdentity identity = topic.projectionIdentity();
        return List.of(
                new Sample<>(new LedgerIdAllocatorRecord(LEDGER_ID + 1, 8, 0), LedgerIdAllocatorRecord.class),
                new Sample<>(topic, TopicProjectionRecord.class),
                new Sample<>(new VirtualLedgerProjectionRecord(
                        NAME, topic.managedLedgerNameHash(), identity, 0, 1, 0),
                        VirtualLedgerProjectionRecord.class),
                new Sample<>(new PositionIndexRecord(
                        NAME, topic.managedLedgerNameHash(), identity, 1,
                        ManagedLedgerProjectionNames.POSITION_FORMULA_V1, 0),
                        PositionIndexRecord.class));
    }

    static TopicProjectionRecord topic(long metadataVersion) {
        return new TopicProjectionRecord(
                NAME,
                ManagedLedgerProjectionNames.managedLedgerNameHash(NAME),
                3,
                1,
                ManagedLedgerProjectionNames.streamName(NAME, 1).value(),
                ManagedLedgerProjectionNames.streamId(NAME, 1).value(),
                ManagedLedgerProjectionNames.STORAGE_CLASS,
                StorageProfile.OBJECT_WAL_SYNC_OBJECT.name(),
                LEDGER_ID,
                1,
                ManagedLedgerProjectionNames.PAYLOAD_MAPPING_V1,
                ManagedLedgerFacadeState.OPEN.name(),
                Map.of("a", "1", "z", "2"),
                100,
                0,
                metadataVersion);
    }

    record Sample<T>(T record, Class<T> recordClass) {
    }
}
