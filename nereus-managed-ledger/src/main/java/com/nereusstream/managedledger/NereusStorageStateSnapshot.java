/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger;

import com.nereusstream.api.StreamMetadata;
import com.nereusstream.api.StreamState;
import com.nereusstream.managedledger.projection.VirtualLedgerProjection;
import java.util.Objects;
import java.util.Optional;

/** Immutable get-only durable-state inspection result used by the storage-class binding protocol. */
public record NereusStorageStateSnapshot(
        NereusDurableStorageState state,
        Optional<VirtualLedgerProjection> projection,
        Optional<StreamMetadata> streamMetadata) {
    public NereusStorageStateSnapshot {
        Objects.requireNonNull(state, "state");
        projection = Objects.requireNonNull(projection, "projection");
        streamMetadata = Objects.requireNonNull(streamMetadata, "streamMetadata");
        if (state == NereusDurableStorageState.MISSING) {
            if (projection.isPresent() || streamMetadata.isPresent()) {
                throw new IllegalArgumentException("MISSING storage state cannot carry durable records");
            }
        } else {
            VirtualLedgerProjection mapping = projection.orElseThrow(() ->
                    new IllegalArgumentException("present storage state requires a projection"));
            StreamMetadata metadata = streamMetadata.orElseThrow(() ->
                    new IllegalArgumentException("present storage state requires stream metadata"));
            if (!mapping.streamId().equals(metadata.streamId())
                    || state != fromStreamState(metadata.state())) {
                throw new IllegalArgumentException("projection and stream metadata do not match storage state");
            }
        }
    }

    public static NereusStorageStateSnapshot missing() {
        return new NereusStorageStateSnapshot(
                NereusDurableStorageState.MISSING, Optional.empty(), Optional.empty());
    }

    private static NereusDurableStorageState fromStreamState(StreamState state) {
        return switch (state) {
            case ACTIVE -> NereusDurableStorageState.ACTIVE;
            case SEALED -> NereusDurableStorageState.SEALED;
            case DELETING -> NereusDurableStorageState.DELETING;
            case DELETED -> NereusDurableStorageState.DELETED;
            case CREATING -> throw new IllegalArgumentException("CREATING is not a published F2 storage state");
        };
    }
}
