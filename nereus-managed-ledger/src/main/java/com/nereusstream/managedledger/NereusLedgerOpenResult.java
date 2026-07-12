/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger;

import com.nereusstream.api.StreamMetadata;
import com.nereusstream.managedledger.projection.VirtualLedgerProjection;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import java.util.Objects;

/** Validated current authority used to construct one writable or read-only facade. */
public record NereusLedgerOpenResult(
        TopicProjectionRecord topicProjection,
        VirtualLedgerProjection projection,
        StreamMetadata streamMetadata) {
    public NereusLedgerOpenResult {
        Objects.requireNonNull(topicProjection, "topicProjection");
        Objects.requireNonNull(projection, "projection");
        Objects.requireNonNull(streamMetadata, "streamMetadata");
        if (!topicProjection.projectionIdentity().equals(
                        new com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity(
                                projection.storageClassBindingGeneration(),
                                projection.incarnation(),
                                projection.streamId().value(),
                                projection.virtualLedgerId()))
                || !projection.streamId().equals(streamMetadata.streamId())) {
            throw new IllegalArgumentException("open result records do not share one projection identity");
        }
    }
}
