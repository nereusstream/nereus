/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.materialization.MaterializationDeadline;
import com.nereusstream.metadata.oxia.retirement.SourceRetirementMetadataStore;
import com.nereusstream.metadata.oxia.retirement.VersionedGenerationZeroCommit;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

/** Exact, restart-safe deletion of one journaled generation-zero commit node. */
public final class GenerationZeroCommitRetirementHandler
        implements GcMetadataRetirementHandler {
    public static final String REMOVAL_TYPE = "generation-zero-commit";

    private final GenerationZeroCommitLoader commitLoader;
    private final GenerationZeroCommitDelete commitDelete;

    public GenerationZeroCommitRetirementHandler(
            String cluster,
            SourceRetirementMetadataStore sourceRetirement) {
        this(
                loader(cluster, sourceRetirement),
                delete(cluster, sourceRetirement));
    }

    GenerationZeroCommitRetirementHandler(
            GenerationZeroCommitLoader commitLoader,
            GenerationZeroCommitDelete commitDelete) {
        this.commitLoader = Objects.requireNonNull(commitLoader, "commitLoader");
        this.commitDelete = Objects.requireNonNull(commitDelete, "commitDelete");
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
                    "generation-zero commit handler received another removal type"));
        }
        return load(exact.key(), deadline).thenCompose(optional -> {
            if (optional.isEmpty()) {
                return CompletableFuture.completedFuture(
                        GcMetadataRetirementOutcome.ALREADY_ABSENT);
            }
            VersionedGenerationZeroCommit commit = optional.orElseThrow();
            requireExact(commit, exact);
            CompletableFuture<Void> delete = deadline.bound(
                    () -> commitDelete.delete(
                            exact.key(),
                            exact.metadataVersion(),
                            exact.durableValueSha256()),
                    "conditionally delete exact generation-zero commit node");
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
                            "generation-zero commit node changed after uncertain delete"));
                });
            }).thenCompose(Function.identity());
        });
    }

    private CompletableFuture<Optional<VersionedGenerationZeroCommit>> load(
            String exactKey,
            MaterializationDeadline deadline) {
        return deadline.bound(
                        () -> commitLoader.load(exactKey),
                        "reload exact generation-zero commit node")
                .thenApply(value -> Objects.requireNonNull(
                        value, "generation-zero commit lookup result"));
    }

    private static void requireExact(
            VersionedGenerationZeroCommit commit,
            GcPlannedMetadataRemoval removal) {
        if (!matches(commit, removal)) {
            throw invariant("generation-zero commit no longer matches the sealed journal");
        }
    }

    private static boolean matches(
            VersionedGenerationZeroCommit commit,
            GcPlannedMetadataRemoval removal) {
        return commit.key().equals(removal.key())
                && commit.metadataVersion() == removal.metadataVersion()
                && commit.durableValueSha256().equals(removal.durableValueSha256());
    }

    private static GenerationZeroCommitLoader loader(
            String cluster,
            SourceRetirementMetadataStore sourceRetirement) {
        String exactCluster = requireText(cluster, "cluster");
        SourceRetirementMetadataStore exact = Objects.requireNonNull(
                sourceRetirement, "sourceRetirement");
        return key -> exact.getCommitNodeByKey(exactCluster, key);
    }

    private static GenerationZeroCommitDelete delete(
            String cluster,
            SourceRetirementMetadataStore sourceRetirement) {
        String exactCluster = requireText(cluster, "cluster");
        SourceRetirementMetadataStore exact = Objects.requireNonNull(
                sourceRetirement, "sourceRetirement");
        return (key, expectedVersion, expectedDigest) ->
                exact.deleteCommitNodeByKey(
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
