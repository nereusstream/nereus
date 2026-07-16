/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.api.target.ReadTarget;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.GcReferenceQueryKind;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.materialization.MaterializationDeadline;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.GenerationIndexIdentity;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationScanPage;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointCodecV1;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

/** Safely fences matching higher-generation readers before physical-root MARK. */
public final class HigherGenerationPreDrainCoordinator {
    private static final List<ReadView> VIEWS = List.of(
            ReadView.COMMITTED, ReadView.TOPIC_COMPACTED);
    private static final ReadTargetCodecRegistry TARGET_CODECS =
            ReadTargetCodecRegistry.phase15();
    private static final String REASON_PREFIX = "physical-gc-pre-drain:";

    private final String cluster;
    private final GenerationMetadataStore generations;
    private final PhysicalObjectMetadataStore physicalObjects;
    private final PhysicalGcConfig config;
    private final Clock clock;
    private final ScheduledExecutorService scheduler;
    private final F4Keyspace keys;
    private final HigherGenerationRecoveryCoverageVerifier coverage;

    public HigherGenerationPreDrainCoordinator(
            String cluster,
            GenerationMetadataStore generations,
            PhysicalObjectMetadataStore physicalObjects,
            RecoveryCheckpointCodecV1 checkpoints,
            PhysicalGcConfig config,
            Clock clock,
            ScheduledExecutorService scheduler) {
        this.cluster = requireText(cluster, "cluster");
        this.generations = Objects.requireNonNull(generations, "generations");
        this.physicalObjects = Objects.requireNonNull(physicalObjects, "physicalObjects");
        Objects.requireNonNull(checkpoints, "checkpoints");
        this.config = Objects.requireNonNull(config, "config");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.keys = new F4Keyspace(cluster);
        RecoveryReplacementVerifier replacements = new RecoveryReplacementVerifier(
                cluster,
                generations,
                physicalObjects,
                checkpoints,
                config);
        this.coverage = new HigherGenerationRecoveryCoverageVerifier(
                cluster,
                generations,
                checkpoints,
                replacements,
                config);
    }

    public CompletableFuture<HigherGenerationPreDrainResult> preDrain(
            GcCandidate candidate) {
        GcCandidate exact = Objects.requireNonNull(candidate, "candidate");
        if (exact.rootState() != GcCandidateRootState.ACTIVE_DISCOVERY) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "higher-generation pre-drain requires an ACTIVE_DISCOVERY candidate"));
        }
        if (exact.referenceQuery().kind()
                == GcReferenceQueryKind.OWNERLESS_ORPHAN_CANDIDATE) {
            return CompletableFuture.failedFuture(condition(
                    "higher-generation pre-drain cannot infer ownerless stream authority"));
        }
        if (!config.mutationsAllowed()) {
            return CompletableFuture.completedFuture(
                    HigherGenerationPreDrainResult.disabled());
        }
        MaterializationDeadline deadline = new MaterializationDeadline(
                config.operationTimeout(), scheduler);
        CompletableFuture<HigherGenerationPreDrainResult> result;
        try {
            result = requireExactCandidateRoot(exact, deadline)
                    .thenCompose(ignored -> scan(
                            exact.referenceQuery(),
                            0,
                            0,
                            Optional.empty(),
                            null,
                            new ArrayList<>(),
                            0,
                            deadline))
                    .thenCompose(indexes -> indexes.isEmpty()
                            ? CompletableFuture.completedFuture(
                                    HigherGenerationPreDrainResult.noMatchingIndex())
                            : drain(
                                    exact,
                                    indexes,
                                    0,
                                    new ArrayList<>(),
                                    0,
                                    0,
                                    deadline));
        } catch (Throwable failure) {
            deadline.close();
            return CompletableFuture.failedFuture(failure);
        }
        return result.whenComplete((ignored, failure) -> deadline.close());
    }

    private CompletableFuture<List<VersionedGenerationIndex>> scan(
            GcReferenceQuery query,
            int streamIndex,
            int viewIndex,
            Optional<F4ScanToken> continuation,
            String previousKey,
            List<VersionedGenerationIndex> matching,
            int observed,
            MaterializationDeadline deadline) {
        if (streamIndex == query.affectedStreams().size()) {
            return CompletableFuture.completedFuture(List.copyOf(matching));
        }
        if (viewIndex == VIEWS.size()) {
            return scan(
                    query,
                    streamIndex + 1,
                    0,
                    Optional.empty(),
                    null,
                    matching,
                    observed,
                    deadline);
        }
        StreamId stream = query.affectedStreams().get(streamIndex);
        ReadView view = VIEWS.get(viewIndex);
        int remaining = config.maxAuthoritiesPerDomainSnapshot() - observed;
        int limit = remaining == 0
                ? 1
                : Math.min(config.metadataScanPageSize(), remaining);
        return deadline.bound(
                        () -> generations.scanIndex(
                                cluster,
                                stream,
                                view,
                                0,
                                Long.MAX_VALUE,
                                continuation,
                                limit),
                        "scan higher-generation pre-drain indexes")
                .thenCompose(page -> {
                    requireProgress(page, previousKey);
                    int nextObserved = Math.addExact(
                            observed, page.values().size());
                    if (nextObserved > config.maxAuthoritiesPerDomainSnapshot()) {
                        return CompletableFuture.failedFuture(invariant(
                                "higher-generation pre-drain scan exceeded its authority bound"));
                    }
                    for (VersionedGenerationCandidate candidate : page.values()) {
                        if (candidate instanceof VersionedGenerationIndex higher) {
                            requireScannedIdentity(stream, view, higher);
                            if (matches(query, higher)) {
                                matching.add(higher);
                            }
                        }
                    }
                    if (page.continuation().isPresent()) {
                        if (nextObserved
                                >= config.maxAuthoritiesPerDomainSnapshot()) {
                            return CompletableFuture.failedFuture(invariant(
                                    "higher-generation pre-drain scan exceeded its authority bound"));
                        }
                        return scan(
                                query,
                                streamIndex,
                                viewIndex,
                                page.continuation(),
                                page.values().get(page.values().size() - 1).key(),
                                matching,
                                nextObserved,
                                deadline);
                    }
                    return scan(
                            query,
                            streamIndex,
                            viewIndex + 1,
                            Optional.empty(),
                            null,
                            matching,
                            nextObserved,
                            deadline);
                });
    }

    private CompletableFuture<HigherGenerationPreDrainResult> drain(
            GcCandidate candidate,
            List<VersionedGenerationIndex> indexes,
            int index,
            List<VersionedGenerationIndex> draining,
            int transitioned,
            int alreadyDraining,
            MaterializationDeadline deadline) {
        if (index == indexes.size()) {
            return CompletableFuture.completedFuture(
                    new HigherGenerationPreDrainResult(
                            HigherGenerationPreDrainStatus.DRAINING_READY,
                            draining,
                            transitioned,
                            alreadyDraining));
        }
        VersionedGenerationIndex source = indexes.get(index);
        GenerationLifecycle lifecycle = source.value().lifecycle();
        if (lifecycle == GenerationLifecycle.PREPARED) {
            return CompletableFuture.failedFuture(condition(
                    "matching higher-generation PREPARED index vetoes pre-drain"));
        }
        if (lifecycle != GenerationLifecycle.COMMITTED
                && lifecycle != GenerationLifecycle.QUARANTINED
                && lifecycle != GenerationLifecycle.DRAINING) {
            return CompletableFuture.failedFuture(invariant(
                    "matching higher-generation index has an unsupported pre-drain lifecycle"));
        }
        return deadline.bound(
                        () -> coverage.prove(candidate.referenceQuery(), source),
                        "prove higher-generation recovery replacement coverage")
                .thenCompose(proof -> {
                    if (lifecycle == GenerationLifecycle.DRAINING) {
                        draining.add(source);
                        return drain(
                                candidate,
                                indexes,
                                index + 1,
                                draining,
                                transitioned,
                                Math.addExact(alreadyDraining, 1),
                                deadline);
                    }
                    return transition(candidate, proof, deadline).thenCompose(result -> {
                        draining.add(result.index());
                        return drain(
                                candidate,
                                indexes,
                                index + 1,
                                draining,
                                result.transitioned()
                                        ? Math.addExact(transitioned, 1)
                                        : transitioned,
                                result.transitioned()
                                        ? alreadyDraining
                                        : Math.addExact(alreadyDraining, 1),
                                deadline);
                    });
                });
    }

    private CompletableFuture<TransitionResult> transition(
            GcCandidate candidate,
            HigherGenerationRecoveryCoverageVerifier.CoverageProof proof,
            MaterializationDeadline deadline) {
        VersionedGenerationIndex source = proof.source();
        GenerationIndexRecord replacement = draining(
                source.value(), candidate.candidateId(), nonNegativeNow());
        return requireExactCandidateRoot(candidate, deadline)
                .thenCompose(ignored -> deadline.bound(
                        () -> generations.compareAndSetIndex(
                                cluster, replacement, source.metadataVersion()),
                        "CAS higher-generation source to DRAINING"))
                .handle((drained, failure) -> {
                    if (failure == null) {
                        requireExactReplacement(drained, source.key(), replacement);
                        return CompletableFuture.completedFuture(
                                new TransitionResult(drained, true));
                    }
                    Throwable original = unwrap(failure);
                    return reload(source, deadline).thenCompose(reloaded -> {
                        if (reloaded.isEmpty()) {
                            return CompletableFuture.failedFuture(condition(
                                    "higher-generation source disappeared after uncertain pre-drain CAS"));
                        }
                        VersionedGenerationIndex current = reloaded.orElseThrow();
                        if (exactReplacement(current, source.key(), replacement)) {
                            return CompletableFuture.completedFuture(
                                    new TransitionResult(current, true));
                        }
                        if (current.equals(source)) {
                            return CompletableFuture.failedFuture(original);
                        }
                        if (current.value().lifecycle() == GenerationLifecycle.DRAINING
                                && sameImmutablePublication(
                                        source.value(), current.value())) {
                            return CompletableFuture.completedFuture(
                                    new TransitionResult(current, false));
                        }
                        return CompletableFuture.failedFuture(invariant(
                                "higher-generation source changed after uncertain pre-drain CAS"));
                    });
                })
                .thenCompose(Function.identity());
    }

    private CompletableFuture<Optional<VersionedGenerationIndex>> reload(
            VersionedGenerationIndex source,
            MaterializationDeadline deadline) {
        return deadline.bound(
                () -> generations.getIndex(cluster, identity(source.value())),
                "reload higher-generation source after uncertain pre-drain CAS");
    }

    private CompletableFuture<Void> requireExactCandidateRoot(
            GcCandidate candidate,
            MaterializationDeadline deadline) {
        ObjectKeyHash object = candidate.object().objectKeyHash();
        return deadline.bound(
                        () -> physicalObjects.getRoot(cluster, object),
                        "reload ACTIVE candidate root before higher-generation pre-drain")
                .thenAccept(optional -> {
                    VersionedPhysicalObjectRoot root = optional.orElseThrow(() -> condition(
                            "candidate physical root is absent before higher-generation pre-drain"));
                    if (!root.key().equals(keys.physicalRootKey(object))
                            || root.value().lifecycle()
                                    != PhysicalObjectLifecycle.ACTIVE
                            || root.metadataVersion()
                                    != candidate.rootMetadataVersion()
                            || root.value().lifecycleEpoch()
                                    != candidate.rootLifecycleEpoch()
                            || !PhysicalObjectIdentity.from(root.value())
                                    .equals(candidate.object())) {
                        throw condition(
                                "candidate physical root changed before higher-generation pre-drain");
                    }
                });
    }

    private static boolean matches(
            GcReferenceQuery query,
            VersionedGenerationIndex candidate) {
        GenerationLifecycle lifecycle = candidate.value().lifecycle();
        if (lifecycle == GenerationLifecycle.RETIRED
                || lifecycle == GenerationLifecycle.ABORTED) {
            return false;
        }
        ReadTarget decoded;
        try {
            decoded = TARGET_CODECS.decode(candidate.value().readTarget());
        } catch (RuntimeException failure) {
            throw invariant("higher-generation pre-drain target cannot be decoded");
        }
        if (!(decoded instanceof ObjectSliceReadTarget target)
                || !target.objectKey().equals(query.object().objectKey())) {
            return false;
        }
        return query.object().objectId().isEmpty()
                || query.object().objectId().orElseThrow().equals(target.objectId());
    }

    private void requireScannedIdentity(
            StreamId stream,
            ReadView view,
            VersionedGenerationIndex candidate) {
        GenerationIndexRecord value = candidate.value();
        String expectedKey = keys.generationIndexKey(
                stream, view, value.offsetEnd(), value.generation());
        if (!value.streamId().equals(stream.value())
                || value.readViewId() != view.wireId()
                || !candidate.key().equals(expectedKey)) {
            throw invariant(
                    "higher-generation pre-drain scan returned a foreign index");
        }
    }

    private static GenerationIndexRecord draining(
            GenerationIndexRecord current,
            String candidateId,
            long nowMillis) {
        long changedAt = Math.max(nowMillis, current.stateChangedAtMillis());
        return new GenerationIndexRecord(
                current.schemaVersion(),
                current.streamId(),
                current.readViewId(),
                current.offsetStart(),
                current.offsetEnd(),
                current.generation(),
                current.publicationId(),
                current.taskId(),
                GenerationLifecycle.DRAINING,
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
                REASON_PREFIX + candidateId,
                changedAt,
                0);
    }

    private static boolean sameImmutablePublication(
            GenerationIndexRecord left,
            GenerationIndexRecord right) {
        GenerationIndexRecord normalized = draining(
                left,
                "a".repeat(52),
                Math.max(left.stateChangedAtMillis(), right.stateChangedAtMillis()));
        GenerationIndexRecord actual = new GenerationIndexRecord(
                right.schemaVersion(),
                right.streamId(),
                right.readViewId(),
                right.offsetStart(),
                right.offsetEnd(),
                right.generation(),
                right.publicationId(),
                right.taskId(),
                GenerationLifecycle.DRAINING,
                right.sourceSetSha256(),
                right.policySha256(),
                right.readTarget(),
                right.targetIdentitySha256(),
                right.materializationPolicySha256(),
                right.payloadFormat(),
                right.sourceRecordCount(),
                right.outputRecordCount(),
                right.entryCount(),
                right.logicalBytes(),
                right.cumulativeSizeAtStart(),
                right.cumulativeSizeAtEnd(),
                right.firstCommitVersion(),
                right.lastCommitVersion(),
                right.schemaRefs(),
                right.projectionRef(),
                right.createdAtMillis(),
                right.committedAtMillis(),
                normalized.stateReason(),
                normalized.stateChangedAtMillis(),
                0);
        return normalized.equals(actual);
    }

    private static void requireExactReplacement(
            VersionedGenerationIndex actual,
            String expectedKey,
            GenerationIndexRecord expected) {
        if (!exactReplacement(actual, expectedKey, expected)) {
            throw invariant("higher-generation pre-drain CAS returned another value");
        }
    }

    private static boolean exactReplacement(
            VersionedGenerationIndex actual,
            String expectedKey,
            GenerationIndexRecord expected) {
        return actual.key().equals(expectedKey)
                && actual.value().withMetadataVersion(0).equals(expected);
    }

    private static GenerationIndexIdentity identity(GenerationIndexRecord value) {
        return new GenerationIndexIdentity(
                new StreamId(value.streamId()),
                ReadView.fromWireId(value.readViewId()),
                value.offsetEnd(),
                value.generation());
    }

    private static void requireProgress(
            GenerationScanPage page,
            String previousKey) {
        if (page.continuation().isPresent() && page.values().isEmpty()) {
            throw invariant(
                    "higher-generation pre-drain scan returned an empty continuation page");
        }
        if (previousKey != null
                && !page.values().isEmpty()
                && page.values().get(0).key().compareTo(previousKey) <= 0) {
            throw invariant("higher-generation pre-drain scan did not advance");
        }
    }

    private long nonNegativeNow() {
        long value = clock.millis();
        if (value < 0) {
            throw new IllegalStateException(
                    "clock returned a negative epoch millisecond");
        }
        return value;
    }

    private static Throwable unwrap(Throwable supplied) {
        Throwable current = supplied;
        while ((current instanceof CompletionException
                        || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
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

    private static NereusException condition(String message) {
        return new NereusException(ErrorCode.METADATA_CONDITION_FAILED, true, message);
    }

    private record TransitionResult(
            VersionedGenerationIndex index,
            boolean transitioned) {
        private TransitionResult {
            Objects.requireNonNull(index, "index");
            if (index.value().lifecycle() != GenerationLifecycle.DRAINING) {
                throw new IllegalArgumentException(
                        "pre-drain transition result is not DRAINING");
            }
        }
    }
}
