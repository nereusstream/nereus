/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;

import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.ResolvedRange;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.physical.ObjectReadLease;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/** One resolved candidate whose physical object remains protected until release completes. */
public final class PinnedResolvedRange implements AutoCloseable {
    private final GenerationReadCandidate candidate;
    private final ObjectReadLease objectLease;
    private final AtomicReference<CompletableFuture<Void>> release = new AtomicReference<>();

    public PinnedResolvedRange(
            GenerationReadCandidate candidate,
            ObjectReadLease objectLease) {
        this.candidate = Objects.requireNonNull(candidate, "candidate");
        this.objectLease = Objects.requireNonNull(objectLease, "objectLease");
        if (!(candidate.resolvedRange().readTarget() instanceof ObjectSliceReadTarget object)) {
            throw new IllegalArgumentException("M2 pinned ranges require an object-slice target");
        }
        if (!ObjectKeyHash.from(object.objectKey()).equals(objectLease.object().objectKeyHash())) {
            throw new IllegalArgumentException("reader lease does not protect the resolved target object");
        }
    }

    public GenerationReadCandidate candidate() {
        return candidate;
    }

    public ResolvedRange resolvedRange() {
        return candidate.resolvedRange();
    }

    public CompletableFuture<Void> release() {
        CompletableFuture<Void> current = release.get();
        if (current != null) {
            return current;
        }
        CompletableFuture<Void> completion = new CompletableFuture<>();
        if (!release.compareAndSet(null, completion)) {
            return release.get();
        }
        try {
            Objects.requireNonNull(objectLease.release(), "object lease release future")
                    .whenComplete((ignored, failure) -> {
                        if (failure == null) {
                            completion.complete(null);
                        } else {
                            completion.completeExceptionally(failure);
                        }
                    });
        } catch (Throwable failure) {
            completion.completeExceptionally(failure);
        }
        return completion;
    }

    public boolean isReleased() {
        return release.get() != null || objectLease.isReleased();
    }

    @Override
    public void close() {
        release();
    }
}
