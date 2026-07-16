/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.materialization.MaterializationDeadline;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.GenerationCandidateKeyIdentity;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

/** Exact DRAINING-to-RETIRED transition for one journaled higher-generation index. */
public final class HigherGenerationIndexRetirementHandler
        implements GcMetadataRetirementHandler {
    public static final String REMOVAL_TYPE = "generation-index";
    private static final String REASON_PREFIX = "physical-gc:";

    private final F4Keyspace keys;
    private final GenerationCandidateLoader candidateLoader;
    private final HigherGenerationIndexCas indexCas;

    public HigherGenerationIndexRetirementHandler(
            String cluster,
            GenerationMetadataStore generations) {
        this(
                cluster,
                loader(cluster, generations),
                cas(cluster, generations));
    }

    HigherGenerationIndexRetirementHandler(
            String cluster,
            GenerationCandidateLoader candidateLoader,
            HigherGenerationIndexCas indexCas) {
        this.keys = new F4Keyspace(requireText(cluster, "cluster"));
        this.candidateLoader = Objects.requireNonNull(candidateLoader, "candidateLoader");
        this.indexCas = Objects.requireNonNull(indexCas, "indexCas");
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
        GcMetadataRetirementContext exactContext = Objects.requireNonNull(context, "context");
        GcPlannedMetadataRemoval exactRemoval = Objects.requireNonNull(removal, "removal");
        Objects.requireNonNull(deadline, "deadline");
        final GenerationCandidateKeyIdentity identity;
        try {
            requireType(exactRemoval);
            identity = keys.parseGenerationIndexKey(exactRemoval.key());
            if (identity.generationZero()) {
                throw invariant("higher-generation removal names a generation-zero key");
            }
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return load(identity, exactRemoval.key(), deadline).thenCompose(optional -> {
            if (optional.isEmpty()) {
                return CompletableFuture.completedFuture(
                        GcMetadataRetirementOutcome.ALREADY_ABSENT);
            }
            VersionedGenerationCandidate candidate = optional.orElseThrow();
            if (!(candidate instanceof VersionedGenerationIndex higher)) {
                return CompletableFuture.failedFuture(invariant(
                        "higher-generation key decoded as a generation-zero record"));
            }
            if (isExactRetiredProgress(higher, exactContext, exactRemoval)) {
                return CompletableFuture.completedFuture(
                        GcMetadataRetirementOutcome.ALREADY_ABSENT);
            }
            requireExactDraining(higher, exactRemoval, exactContext);
            GenerationIndexRecord replacement = retired(
                    higher.value(), exactContext);
            CompletableFuture<VersionedGenerationIndex> cas = deadline.bound(
                    () -> indexCas.compareAndSet(replacement, higher.metadataVersion()),
                    "CAS exact higher-generation index DRAINING to RETIRED");
            return cas.handle((retired, failure) -> {
                if (failure == null) {
                    requireExactReplacement(retired, replacement, exactRemoval.key());
                    return CompletableFuture.completedFuture(
                            GcMetadataRetirementOutcome.RETIRED);
                }
                Throwable original = unwrap(failure);
                return load(identity, exactRemoval.key(), deadline).thenCompose(reloaded -> {
                    if (reloaded.isEmpty()) {
                        return CompletableFuture.completedFuture(
                                GcMetadataRetirementOutcome.ALREADY_ABSENT);
                    }
                    VersionedGenerationCandidate current = reloaded.orElseThrow();
                    if (current instanceof VersionedGenerationIndex currentHigher
                            && isExactRetiredProgress(
                                    currentHigher, exactContext, exactRemoval)) {
                        return CompletableFuture.completedFuture(
                                GcMetadataRetirementOutcome.RETIRED);
                    }
                    if (matchesPlanned(current, exactRemoval)) {
                        return CompletableFuture.failedFuture(original);
                    }
                    return CompletableFuture.failedFuture(invariant(
                            "higher-generation index changed after uncertain retirement CAS"));
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
                "reload exact higher-generation index")
                .thenApply(value -> Objects.requireNonNull(
                        value, "higher-generation candidate lookup result"));
    }

    private static void requireExactDraining(
            VersionedGenerationIndex higher,
            GcPlannedMetadataRemoval removal,
            GcMetadataRetirementContext context) {
        if (!matchesPlanned(higher, removal)) {
            throw invariant("higher-generation index no longer matches the sealed journal");
        }
        if (higher.value().lifecycle() != GenerationLifecycle.DRAINING) {
            throw invariant("journaled higher-generation index is not DRAINING");
        }
        if (higher.value().stateChangedAtMillis()
                > context.deletingRoot().value().deleteStartedAtMillis()) {
            throw invariant("higher-generation DRAINING timestamp follows delete intent");
        }
    }

    private static boolean isExactRetiredProgress(
            VersionedGenerationIndex candidate,
            GcMetadataRetirementContext context,
            GcPlannedMetadataRemoval removal) {
        return candidate.value().lifecycle() == GenerationLifecycle.RETIRED
                && candidate.key().equals(removal.key())
                && candidate.value().stateReason().equals(
                        retirementReason(context))
                && candidate.value().stateChangedAtMillis()
                        == context.deletingRoot().value().deleteStartedAtMillis();
    }

    private static GenerationIndexRecord retired(
            GenerationIndexRecord current,
            GcMetadataRetirementContext context) {
        return new GenerationIndexRecord(
                current.schemaVersion(),
                current.streamId(),
                current.readViewId(),
                current.offsetStart(),
                current.offsetEnd(),
                current.generation(),
                current.publicationId(),
                current.taskId(),
                GenerationLifecycle.RETIRED,
                current.sourceSetSha256(),
                current.policySha256(),
                current.readTarget(),
                current.targetIdentitySha256(),
                current.materializationPolicySha256(),
                current.payloadFormat(),
                current.sourceRecordCount(),
                current.outputRecordCount(),
                current.entryCount(),
                current.logicalBytes(),
                current.cumulativeSizeAtStart(),
                current.cumulativeSizeAtEnd(),
                current.firstCommitVersion(),
                current.lastCommitVersion(),
                current.schemaRefs(),
                current.projectionRef(),
                current.createdAtMillis(),
                current.committedAtMillis(),
                retirementReason(context),
                context.deletingRoot().value().deleteStartedAtMillis(),
                0);
    }

    private static void requireExactReplacement(
            VersionedGenerationIndex actual,
            GenerationIndexRecord expected,
            String expectedKey) {
        if (!actual.key().equals(expectedKey)
                || !actual.value().withMetadataVersion(0).equals(expected)) {
            throw invariant("higher-generation retirement CAS returned another value");
        }
    }

    private static boolean matchesPlanned(
            VersionedGenerationCandidate candidate,
            GcPlannedMetadataRemoval removal) {
        return candidate.key().equals(removal.key())
                && candidate.metadataVersion() == removal.metadataVersion()
                && candidate.durableValueSha256().equals(removal.durableValueSha256());
    }

    private static String retirementReason(GcMetadataRetirementContext context) {
        return REASON_PREFIX
                + context.journal().gcAttemptId()
                + ":"
                + context.journal().referenceSetSha256().value();
    }

    private static GenerationCandidateLoader loader(
            String cluster,
            GenerationMetadataStore generations) {
        GenerationMetadataStore exact = Objects.requireNonNull(generations, "generations");
        return (streamId, view, key) -> exact.getCandidateByKey(
                cluster, streamId, view, key);
    }

    private static HigherGenerationIndexCas cas(
            String cluster,
            GenerationMetadataStore generations) {
        GenerationMetadataStore exact = Objects.requireNonNull(generations, "generations");
        return (replacement, expectedVersion) -> exact.compareAndSetIndex(
                cluster, replacement, expectedVersion);
    }

    private static void requireType(GcPlannedMetadataRemoval removal) {
        if (!REMOVAL_TYPE.equals(removal.removalType())) {
            throw new IllegalArgumentException(
                    "higher-generation handler received another removal type");
        }
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
