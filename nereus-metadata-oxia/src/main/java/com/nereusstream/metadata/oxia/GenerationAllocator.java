/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.PublicationId;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Validating facade for the view-scoped single-key generation allocator. */
public final class GenerationAllocator {
    private final String cluster;
    private final GenerationMetadataStore store;

    public GenerationAllocator(String cluster, GenerationMetadataStore store) {
        this.cluster = requireText(cluster, "cluster");
        this.store = Objects.requireNonNull(store, "store");
    }

    public CompletableFuture<AllocatedGeneration> allocate(
            StreamId streamId,
            ReadView view,
            PublicationId publicationId) {
        StreamId exactStream = Objects.requireNonNull(streamId, "streamId");
        ReadView exactView = Objects.requireNonNull(view, "view");
        PublicationId exactPublication = Objects.requireNonNull(publicationId, "publicationId");
        CompletableFuture<AllocatedGeneration> operation;
        try {
            operation = Objects.requireNonNull(
                    store.allocateGeneration(cluster, exactStream, exactView, exactPublication),
                    "generation allocation future");
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return operation.thenApply(allocation -> validate(
                Objects.requireNonNull(allocation, "allocation"),
                exactStream,
                exactView,
                exactPublication));
    }

    private static AllocatedGeneration validate(
            AllocatedGeneration allocation,
            StreamId streamId,
            ReadView view,
            PublicationId publicationId) {
        if (!allocation.streamId().equals(streamId)
                || allocation.view() != view
                || !allocation.publicationId().equals(publicationId)
                || allocation.generation().value() != allocation.allocationSequence()) {
            throw F4MetadataStoreSupport.invariant(
                    "generation allocator returned a mismatched allocation proof");
        }
        return allocation;
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }
}
