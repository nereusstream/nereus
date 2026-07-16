/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.materialization.MaterializationDeadline;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Closed, unique dispatch table for generic journal removal types. */
public final class GcMetadataRetirementRegistry {
    private final Map<String, GcMetadataRetirementHandler> handlers;

    public GcMetadataRetirementRegistry(List<GcMetadataRetirementHandler> handlers) {
        HashMap<String, GcMetadataRetirementHandler> exact = new HashMap<>();
        for (GcMetadataRetirementHandler handler : List.copyOf(
                Objects.requireNonNull(handlers, "handlers"))) {
            Objects.requireNonNull(handler, "handler");
            String type = canonicalType(handler.removalType());
            if (exact.put(type, handler) != null) {
                throw new IllegalArgumentException(
                        "metadata-retirement removal types must be unique: " + type);
            }
        }
        this.handlers = Map.copyOf(exact);
    }

    public void requireSupports(List<GcPlannedMetadataRemoval> removals) {
        for (GcPlannedMetadataRemoval removal : List.copyOf(
                Objects.requireNonNull(removals, "removals"))) {
            if (!handlers.containsKey(removal.removalType())) {
                throw invariant(
                        "no metadata-retirement handler is registered for "
                                + removal.removalType());
            }
        }
    }

    public CompletableFuture<GcMetadataRetirementOutcome> retire(
            GcMetadataRetirementContext context,
            GcPlannedMetadataRemoval removal,
            MaterializationDeadline deadline) {
        Objects.requireNonNull(context, "context");
        GcPlannedMetadataRemoval exactRemoval = Objects.requireNonNull(removal, "removal");
        Objects.requireNonNull(deadline, "deadline");
        if (!context.journal().plannedMetadataRemovals().contains(exactRemoval)) {
            return CompletableFuture.failedFuture(invariant(
                    "metadata retirement is not present in the authenticated journal"));
        }
        GcMetadataRetirementHandler handler = handlers.get(exactRemoval.removalType());
        if (handler == null) {
            return CompletableFuture.failedFuture(invariant(
                    "no metadata-retirement handler is registered for "
                            + exactRemoval.removalType()));
        }
        return deadline.bound(
                        () -> handler.retire(context, exactRemoval, deadline),
                        "retire journaled metadata type " + exactRemoval.removalType())
                .thenApply(outcome -> Objects.requireNonNull(
                        outcome, "metadata-retirement handler outcome"));
    }

    private static String canonicalType(String value) {
        Objects.requireNonNull(value, "removalType");
        if (value.length() > 128
                || !value.matches("[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?")) {
            throw new IllegalArgumentException("removalType is not canonical");
        }
        return value;
    }

    private static NereusException invariant(String message) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }
}
