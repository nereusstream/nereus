/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.VersionedMaterializationStreamRegistration;
import java.util.Objects;

/** Exact source set plus the stream/registration authority observed while resolving it. */
public record CommittedSourceSetResolution(
        StreamId streamId,
        ExactSourceSet sourceSet,
        StreamMetadataSnapshot streamSnapshot,
        VersionedMaterializationStreamRegistration registration) {
    public CommittedSourceSetResolution {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(sourceSet, "sourceSet");
        Objects.requireNonNull(streamSnapshot, "streamSnapshot");
        Objects.requireNonNull(registration, "registration");
        if (sourceSet.view() != ReadView.COMMITTED
                || !streamSnapshot.metadata().streamId().equals(streamId.value())
                || !registration.value().streamId().equals(streamId.value())
                || sourceSet.coverage().startOffset() < streamSnapshot.trim().trimOffset()
                || sourceSet.coverage().endOffset()
                        > streamSnapshot.committedEnd().committedEndOffset()
                || sourceSet.sources().get(sourceSet.sources().size() - 1).commitVersion()
                        > streamSnapshot.committedEnd().commitVersion()) {
            throw new IllegalArgumentException(
                    "COMMITTED source resolution does not match its authoritative snapshot");
        }
    }
}
