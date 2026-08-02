/* Licensed under the Apache License, Version 2.0 */

package com.nereusstream.materialization;

import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.VersionedMaterializationStreamRegistration;
import java.util.Objects;
import java.util.Optional;

/**
 * Exact source set plus the stream authority observed while resolving it.
 *
 * <p>The registration is absent only for Kafka topic compaction over
 * {@code BOOKKEEPER_WAL_ONLY}; that profile deliberately has no object-materialization stream
 * registration.
 */
public record CommittedSourceSetResolution(
        StreamId streamId,
        ExactSourceSet sourceSet,
        StreamMetadataSnapshot streamSnapshot,
        Optional<VersionedMaterializationStreamRegistration> registration) {
    public CommittedSourceSetResolution {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(sourceSet, "sourceSet");
        Objects.requireNonNull(streamSnapshot, "streamSnapshot");
        Objects.requireNonNull(registration, "registration");
        if (sourceSet.view() != ReadView.COMMITTED
                || !streamSnapshot.metadata().streamId().equals(streamId.value())
                || registration
                        .filter(value -> !value.value().streamId().equals(streamId.value()))
                        .isPresent()
                || sourceSet.coverage().startOffset() < streamSnapshot.trim().trimOffset()
                || sourceSet.coverage().endOffset()
                        > streamSnapshot.committedEnd().committedEndOffset()
                || sourceSet.sources().get(sourceSet.sources().size() - 1).commitVersion()
                        > streamSnapshot.committedEnd().commitVersion()) {
            throw new IllegalArgumentException("COMMITTED source resolution does not match its authoritative snapshot");
        }
    }
}
