/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.CursorMetadataStore;
import com.nereusstream.metadata.oxia.VersionedCursorState;
import com.nereusstream.metadata.oxia.records.CursorStateRecord;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/** Strict snapshot read plus root-version revalidation shared by storage and retention recovery. */
final class CursorStateHydrator {
    private final String cluster;
    private final CursorMetadataStore metadataStore;
    private final CursorSnapshotStore snapshotStore;
    private final CursorStatePersistencePlanner planner;
    private final CursorStorageConfig config;

    CursorStateHydrator(
            String cluster,
            CursorMetadataStore metadataStore,
            CursorSnapshotStore snapshotStore,
            CursorStatePersistencePlanner planner,
            CursorStorageConfig config) {
        this.cluster = Objects.requireNonNull(cluster, "cluster");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.snapshotStore = Objects.requireNonNull(snapshotStore, "snapshotStore");
        this.planner = Objects.requireNonNull(planner, "planner");
        this.config = Objects.requireNonNull(config, "config");
    }

    CompletableFuture<CursorStatePersistencePlanner.HydratedState> load(
            CursorLedgerIdentity ledger, String cursorName) {
        Objects.requireNonNull(ledger, "ledger");
        Objects.requireNonNull(cursorName, "cursorName");
        return metadataStore
                .getCursor(cluster, new StreamId(ledger.projection().streamId()), cursorName)
                .thenCompose(root -> root
                        .map(value -> hydrate(ledger, value))
                        .orElseGet(() -> CompletableFuture.failedFuture(invariant(
                                "cursor root disappeared during hydration"))));
    }

    CompletableFuture<CursorStatePersistencePlanner.HydratedState> hydrate(
            CursorLedgerIdentity ledger, VersionedCursorState root) {
        Objects.requireNonNull(ledger, "ledger");
        Objects.requireNonNull(root, "root");
        return hydrateAttempt(ledger, root, 0);
    }

    private CompletableFuture<CursorStatePersistencePlanner.HydratedState> hydrateAttempt(
            CursorLedgerIdentity ledger, VersionedCursorState root, int attempt) {
        if (attempt >= config.cursorHydrationMaxAttempts()) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.METADATA_CONDITION_FAILED,
                    true,
                    "cursor root did not stabilize during snapshot hydration"));
        }
        CursorStateRecord value = root.value();
        final CursorIdentity identity;
        try {
            if (!value.projection().equals(ledger.projection())) {
                throw invariant("cursor projection changed during hydration");
            }
            identity = new CursorIdentity(
                    ledger,
                    value.cursorName(),
                    value.cursorNameHash(),
                    value.cursorGeneration());
            if (value.snapshotReference().isEmpty()) {
                return CompletableFuture.completedFuture(
                        planner.hydrate(root, ledger, Optional.empty()));
            }
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }

        CursorSnapshotReference reference;
        try {
            reference = planner.hydrateReference(value.snapshotReference().orElseThrow());
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
        CompletableFuture<CursorAckState> snapshotRead = snapshotStore.read(reference, identity);
        return snapshotRead.handle((snapshot, readError) -> new SnapshotRead(snapshot, unwrapNullable(readError)))
                .thenCompose(read -> metadataStore.getCursor(
                                cluster,
                                new StreamId(ledger.projection().streamId()),
                                value.cursorName())
                        .thenCompose(latest -> {
                            if (latest.isEmpty()) {
                                return CompletableFuture.failedFuture(invariant(
                                        "cursor root disappeared during snapshot revalidation"));
                            }
                            VersionedCursorState current = latest.orElseThrow();
                            if (current.metadataVersion() != root.metadataVersion()) {
                                return hydrateAttempt(ledger, current, attempt + 1);
                            }
                            if (read.error() != null) {
                                return CompletableFuture.failedFuture(
                                        stableSnapshotReadFailure(read.error()));
                            }
                            try {
                                return CompletableFuture.completedFuture(
                                        planner.hydrate(root, ledger, Optional.of(read.snapshot())));
                            } catch (Throwable error) {
                                return CompletableFuture.failedFuture(error);
                            }
                        }));
    }

    private static Throwable unwrapNullable(Throwable error) {
        if (error == null) {
            return null;
        }
        Throwable current = error;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static Throwable stableSnapshotReadFailure(Throwable error) {
        if (error instanceof CursorSnapshotCodecV1.CursorSnapshotCorruptionException
                || (error instanceof NereusException nereus
                        && (nereus.code() == ErrorCode.OBJECT_NOT_FOUND
                                || nereus.code() == ErrorCode.OBJECT_CHECKSUM_MISMATCH))) {
            return new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "stable cursor root references a missing or corrupt snapshot",
                    error);
        }
        return error;
    }

    private record SnapshotRead(CursorAckState snapshot, Throwable error) {
    }
}
