/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.materialization.MaterializationDeadline;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.GenerationCandidateKeyIdentity;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedGenerationZeroIndex;
import com.nereusstream.metadata.oxia.retirement.SourceRetirementMetadataStore;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

/** Exact, restart-safe deletion of one journaled generation-zero index. */
public final class GenerationZeroIndexRetirementHandler
        implements GcMetadataRetirementHandler {
    public static final String REMOVAL_TYPE = "generation-zero-index";

    private final F4Keyspace keys;
    private final GenerationCandidateLoader candidateLoader;
    private final GenerationZeroIndexDelete indexDelete;

    public GenerationZeroIndexRetirementHandler(
            String cluster,
            GenerationMetadataStore generations,
            SourceRetirementMetadataStore sourceRetirement) {
        this(
                cluster,
                loader(cluster, generations),
                delete(cluster, sourceRetirement));
    }

    GenerationZeroIndexRetirementHandler(
            String cluster,
            GenerationCandidateLoader candidateLoader,
            GenerationZeroIndexDelete indexDelete) {
        this.keys = new F4Keyspace(requireText(cluster, "cluster"));
        this.candidateLoader = Objects.requireNonNull(candidateLoader, "candidateLoader");
        this.indexDelete = Objects.requireNonNull(indexDelete, "indexDelete");
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
        final GenerationCandidateKeyIdentity identity;
        try {
            requireType(exact);
            identity = keys.parseGenerationIndexKey(exact.key());
            if (!identity.generationZero()) {
                throw invariant("generation-zero removal names a higher-generation key");
            }
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return load(identity, exact.key(), deadline).thenCompose(optional -> {
            if (optional.isEmpty()) {
                return CompletableFuture.completedFuture(
                        GcMetadataRetirementOutcome.ALREADY_ABSENT);
            }
            VersionedGenerationCandidate candidate = optional.orElseThrow();
            requireExactPlanned(candidate, exact);
            if (!(candidate instanceof VersionedGenerationZeroIndex zero)) {
                return CompletableFuture.failedFuture(invariant(
                        "generation-zero key decoded as a higher-generation record"));
            }
            if (zero.value().tombstoned()) {
                return CompletableFuture.failedFuture(invariant(
                        "journal planned a tombstoned generation-zero index"));
            }
            CompletableFuture<Void> delete = deadline.bound(
                    () -> indexDelete.delete(
                            identity.streamId(),
                            identity.offsetEnd(),
                            exact.metadataVersion(),
                            exact.durableValueSha256()),
                    "conditionally delete exact generation-zero index");
            return delete.handle((ignored, failure) -> {
                if (failure == null) {
                    return CompletableFuture.completedFuture(
                            GcMetadataRetirementOutcome.RETIRED);
                }
                Throwable original = unwrap(failure);
                return load(identity, exact.key(), deadline).thenCompose(reloaded -> {
                    if (reloaded.isEmpty()) {
                        return CompletableFuture.completedFuture(
                                GcMetadataRetirementOutcome.ALREADY_ABSENT);
                    }
                    VersionedGenerationCandidate current = reloaded.orElseThrow();
                    if (matchesPlanned(current, exact)) {
                        return CompletableFuture.failedFuture(original);
                    }
                    return CompletableFuture.failedFuture(invariant(
                            "generation-zero index changed after uncertain delete"));
                });
            }).thenCompose(Function.identity());
        });
    }

    private CompletableFuture<Optional<VersionedGenerationCandidate>> load(
            GenerationCandidateKeyIdentity identity,
            String key,
            MaterializationDeadline deadline) {
        return deadline.bound(
                () -> candidateLoader.load(identity.streamId(), identity.view(), key),
                "reload exact generation-zero index")
                .thenApply(value -> Objects.requireNonNull(
                        value, "generation-zero candidate lookup result"));
    }

    private static GenerationCandidateLoader loader(
            String cluster,
            GenerationMetadataStore generations) {
        GenerationMetadataStore exact = Objects.requireNonNull(generations, "generations");
        return (streamId, view, key) -> exact.getCandidateByKey(
                cluster, streamId, view, key);
    }

    private static GenerationZeroIndexDelete delete(
            String cluster,
            SourceRetirementMetadataStore sourceRetirement) {
        SourceRetirementMetadataStore exact = Objects.requireNonNull(
                sourceRetirement, "sourceRetirement");
        return (streamId, offsetEnd, expectedVersion, expectedDigest) ->
                exact.deleteGenerationZeroIndex(
                        cluster,
                        streamId,
                        offsetEnd,
                        expectedVersion,
                        expectedDigest);
    }

    private static void requireType(GcPlannedMetadataRemoval removal) {
        if (!REMOVAL_TYPE.equals(removal.removalType())) {
            throw new IllegalArgumentException(
                    "generation-zero handler received another removal type");
        }
    }

    private static void requireExactPlanned(
            VersionedGenerationCandidate candidate,
            GcPlannedMetadataRemoval removal) {
        if (!matchesPlanned(candidate, removal)) {
            throw invariant("generation-zero index no longer matches the sealed journal");
        }
    }

    private static boolean matchesPlanned(
            VersionedGenerationCandidate candidate,
            GcPlannedMetadataRemoval removal) {
        return candidate.key().equals(removal.key())
                && candidate.metadataVersion() == removal.metadataVersion()
                && candidate.durableValueSha256().equals(removal.durableValueSha256());
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
