/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.records.CommittedAppendRecord;
import com.nereusstream.metadata.oxia.records.OffsetIndexTargetRecord;
import com.nereusstream.metadata.oxia.records.StreamCommitTargetRecord;
import java.util.List;

/** Phase 1.5 record family kept separate from the frozen Phase 1 registry. */
public final class L0TargetMetadataCodecs {
    private static final MapMetadataCodecRegistry REGISTRY = new MapMetadataCodecRegistry(List.of(
            registered(CommittedAppendRecord.class),
            registered(OffsetIndexTargetRecord.class),
            registered(StreamCommitTargetRecord.class)));

    private L0TargetMetadataCodecs() { }

    public static MetadataCodecRegistry registry() { return REGISTRY; }

    private static <T extends Record> MapMetadataCodecRegistry.RegisteredCodec<T> registered(Class<T> type) {
        return new MapMetadataCodecRegistry.RegisteredCodec<>(type, Phase1MetadataCodecs.recordCodec(type));
    }
}
