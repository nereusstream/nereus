/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.materialization.MaterializationDeadline;
import com.nereusstream.metadata.oxia.retirement.SourceRetirementMetadataStore;
import com.nereusstream.metadata.oxia.retirement.VersionedGenerationZeroMarker;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

/** Exact, restart-safe deletion of one journaled generation-zero committed marker. */
public final class GenerationZeroMarkerRetirementHandler
        implements GcMetadataRetirementHandler {
    public static final String REMOVAL_TYPE = "generation-zero-marker";

    private final GenerationZeroMarkerLoader markerLoader;
    private final GenerationZeroMarkerDelete markerDelete;

    public GenerationZeroMarkerRetirementHandler(
            String cluster,
            SourceRetirementMetadataStore sourceRetirement) {
        this(
                loader(cluster, sourceRetirement),
                delete(cluster, sourceRetirement));
    }

    GenerationZeroMarkerRetirementHandler(
            GenerationZeroMarkerLoader markerLoader,
            GenerationZeroMarkerDelete markerDelete) {
        this.markerLoader = Objects.requireNonNull(markerLoader, "markerLoader");
        this.markerDelete = Objects.requireNonNull(markerDelete, "markerDelete");
    }

    @Override
    public String removalType() {
        return REMOVAL_TYPE;
    }

    @Override
    public CompletableFuture<GcMetadataRetirementOutcome> retire(
            GcMetadataRetirementContext context,
            GcPlannedMetadataRemoval removal,
            MaterializationDeadline deadline) {
        Objects.requireNonNull(context, "context");
        GcPlannedMetadataRemoval exact = Objects.requireNonNull(removal, "removal");
        Objects.requireNonNull(deadline, "deadline");
        if (!REMOVAL_TYPE.equals(exact.removalType())) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "generation-zero marker handler received another removal type"));
        }
        return load(exact.key(), deadline).thenCompose(optional -> {
            if (optional.isEmpty()) {
                return CompletableFuture.completedFuture(
                        GcMetadataRetirementOutcome.ALREADY_ABSENT);
            }
            VersionedGenerationZeroMarker marker = optional.orElseThrow();
            requireExact(marker, exact);
            CompletableFuture<Void> delete = deadline.bound(
                    () -> markerDelete.delete(
                            exact.key(),
                            exact.metadataVersion(),
                            exact.durableValueSha256()),
                    "conditionally delete exact generation-zero committed marker");
            return delete.handle((ignored, failure) -> {
                if (failure == null) {
                    return CompletableFuture.completedFuture(
                            GcMetadataRetirementOutcome.RETIRED);
                }
                Throwable original = unwrap(failure);
                return load(exact.key(), deadline).thenCompose(reloaded -> {
                    if (reloaded.isEmpty()) {
                        return CompletableFuture.completedFuture(
                                GcMetadataRetirementOutcome.ALREADY_ABSENT);
                    }
                    if (matches(reloaded.orElseThrow(), exact)) {
                        return CompletableFuture.failedFuture(original);
                    }
                    return CompletableFuture.failedFuture(invariant(
                            "generation-zero committed marker changed after uncertain delete"));
                });
            }).thenCompose(Function.identity());
        });
    }

    private CompletableFuture<Optional<VersionedGenerationZeroMarker>> load(
            String exactKey,
            MaterializationDeadline deadline) {
        return deadline.bound(
                        () -> markerLoader.load(exactKey),
                        "reload exact generation-zero committed marker")
                .thenApply(value -> Objects.requireNonNull(
                        value, "generation-zero marker lookup result"));
    }

    private static void requireExact(
            VersionedGenerationZeroMarker marker,
            GcPlannedMetadataRemoval removal) {
        if (!matches(marker, removal)) {
            throw invariant("generation-zero marker no longer matches the sealed journal");
        }
    }

    private static boolean matches(
            VersionedGenerationZeroMarker marker,
            GcPlannedMetadataRemoval removal) {
        return marker.key().equals(removal.key())
                && marker.metadataVersion() == removal.metadataVersion()
                && marker.durableValueSha256().equals(removal.durableValueSha256());
    }

    private static GenerationZeroMarkerLoader loader(
            String cluster,
            SourceRetirementMetadataStore sourceRetirement) {
        String exactCluster = requireText(cluster, "cluster");
        SourceRetirementMetadataStore exact = Objects.requireNonNull(
                sourceRetirement, "sourceRetirement");
        return key -> exact.getCommittedMarkerByKey(exactCluster, key);
    }

    private static GenerationZeroMarkerDelete delete(
            String cluster,
            SourceRetirementMetadataStore sourceRetirement) {
        String exactCluster = requireText(cluster, "cluster");
        SourceRetirementMetadataStore exact = Objects.requireNonNull(
                sourceRetirement, "sourceRetirement");
        return (key, expectedVersion, expectedDigest) ->
                exact.deleteCommittedMarkerByKey(
                        exactCluster, key, expectedVersion, expectedDigest);
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must be non-blank");
        }
        return value;
    }

    private static NereusException invariant(String message) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static Throwable unwrap(Throwable supplied) {
        Throwable current = supplied;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
