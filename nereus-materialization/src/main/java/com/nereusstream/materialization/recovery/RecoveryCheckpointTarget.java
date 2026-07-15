/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.recovery;

import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointPublication;
import java.util.Objects;

/** Exact committed generation index corresponding to one NRC1 publication-table row. */
public record RecoveryCheckpointTarget(
        RecoveryCheckpointPublication publication,
        VersionedGenerationIndex index) {
    public RecoveryCheckpointTarget {
        Objects.requireNonNull(publication, "publication");
        Objects.requireNonNull(index, "index");
        var value = index.value();
        if (publication.generation() != value.generation()
                || !publication.publicationId().value().equals(value.publicationId())
                || publication.coverage().startOffset() != value.offsetStart()
                || publication.coverage().endOffset() != value.offsetEnd()
                || !publication.generationIndexRecordSha256().equals(index.durableValueSha256())) {
            throw new IllegalArgumentException("checkpoint publication does not match its exact index");
        }
    }
}
