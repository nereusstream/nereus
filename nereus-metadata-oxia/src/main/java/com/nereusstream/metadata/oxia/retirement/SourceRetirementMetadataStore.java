/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.retirement;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.StreamId;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Exact read-before-delete surface for generation-zero source metadata. */
public interface SourceRetirementMetadataStore extends AutoCloseable {
    /**
     * Reloads one journaled committed-marker key and derives its complete identity from the
     * strictly decoded durable value.
     */
    CompletableFuture<Optional<VersionedGenerationZeroMarker>> getCommittedMarkerByKey(
            String cluster, String exactKey);

    CompletableFuture<Optional<VersionedGenerationZeroMarker>> getCommittedMarker(
            String cluster, StreamId streamId, GenerationZeroMarkerIdentity marker);

    /** Reloads one journaled commit-node key as exact source and canonical checkpoint facts. */
    CompletableFuture<Optional<VersionedGenerationZeroCommit>> getCommitNodeByKey(
            String cluster, String exactKey);

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

    CompletableFuture<Void> deleteCommittedMarkerByKey(
            String cluster,
            String exactKey,
            long expectedVersion,
            Checksum expectedDurableValueSha256);

    CompletableFuture<Void> deleteCommitNode(
            String cluster,
            StreamId streamId,
            String commitId,
            long expectedVersion,
            Checksum expectedDurableValueSha256);

    CompletableFuture<Void> deleteCommitNodeByKey(
            String cluster,
            String exactKey,
            long expectedVersion,
            Checksum expectedDurableValueSha256);

    @Override
    void close();
}
