/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@FunctionalInterface
interface GenerationCandidateLoader {
    CompletableFuture<Optional<VersionedGenerationCandidate>> load(
            StreamId streamId,
            ReadView view,
            String key);
}

@FunctionalInterface
interface HigherGenerationIndexCas {
    CompletableFuture<VersionedGenerationIndex> compareAndSet(
            GenerationIndexRecord replacement,
            long expectedVersion);
}

@FunctionalInterface
interface GenerationZeroIndexDelete {
    CompletableFuture<Void> delete(
            StreamId streamId,
            long offsetEnd,
            long expectedVersion,
            com.nereusstream.api.Checksum expectedDurableValueSha256);
}
