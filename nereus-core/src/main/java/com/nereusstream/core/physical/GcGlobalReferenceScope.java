/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.physical;

import com.nereusstream.api.StreamId;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Authoritative global stream scope used only for ownerless physical-object reference queries.
 *
 * <p>The implementation must rebuild the scope from durable metadata for every call. It must not
 * cache a process-local query-to-stream mapping.
 */
@FunctionalInterface
public interface GcGlobalReferenceScope {
    CompletableFuture<GcGlobalReferenceScopeSnapshot> snapshot();

    static GcGlobalReferenceScope unsupported() {
        return () -> CompletableFuture.completedFuture(
                GcGlobalReferenceScopeSnapshot.incomplete());
    }

    static CompletableFuture<List<StreamId>> resolveStreams(
            GcReferenceQuery query,
            GcReferenceSnapshotBuilder builder,
            GcGlobalReferenceScope globalScope) {
        Objects.requireNonNull(query, "query");
        Objects.requireNonNull(builder, "builder");
        Objects.requireNonNull(globalScope, "globalScope");
        if (query.kind() != GcReferenceQueryKind.OWNERLESS_ORPHAN_CANDIDATE) {
            return CompletableFuture.completedFuture(query.affectedStreams());
        }
        return globalScope.snapshot().thenApply(snapshot -> {
            snapshot.contributeTo(builder);
            return snapshot.complete() && !builder.limitExceeded()
                    ? snapshot.streams()
                    : List.of();
        });
    }
}
