/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.retirement;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.StreamId;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Exact read-before-delete surface for generation-zero source metadata. */
public interface SourceRetirementMetadataStore extends AutoCloseable {
    CompletableFuture<Optional<VersionedGenerationZeroMarker>> getCommittedMarker(
            String cluster, StreamId streamId, GenerationZeroMarkerIdentity marker);

    CompletableFuture<Void> deleteGenerationZeroIndex(
            String cluster,
            StreamId streamId,
            long offsetEnd,
            long expectedVersion,
            Checksum expectedDurableValueSha256);

    CompletableFuture<Void> deleteCommittedMarker(
            String cluster,
            StreamId streamId,
            GenerationZeroMarkerIdentity marker,
            long expectedVersion,
            Checksum expectedDurableValueSha256);

    CompletableFuture<Void> deleteCommitNode(
            String cluster,
            StreamId streamId,
            String commitId,
            long expectedVersion,
            Checksum expectedDurableValueSha256);

    @Override
    void close();
}
