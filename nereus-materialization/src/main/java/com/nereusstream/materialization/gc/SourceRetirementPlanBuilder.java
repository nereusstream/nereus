/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.api.target.ReadTarget;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.GcReferenceQueryKind;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.GenerationCandidateKeyIdentity;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationScanPage;
import com.nereusstream.metadata.oxia.OxiaKeyspace;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.OffsetIndexEntry;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.ProjectionIdentity;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedGenerationZeroIndex;
import com.nereusstream.metadata.oxia.VersionedRecoveryCheckpointRoot;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointReferenceRecord;
import com.nereusstream.metadata.oxia.records.StreamCommitTargetRecord;
import com.nereusstream.metadata.oxia.retirement.SourceRetirementMetadataStore;
import com.nereusstream.metadata.oxia.retirement.VersionedGenerationZeroCommit;
import com.nereusstream.metadata.oxia.retirement.VersionedGenerationZeroMarker;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointCodecV1;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointEntry;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Freezes typed source-removal facts and replays the same exact-key reads before MARK/DRAIN.
 *
 * <p>This builder never turns a visible higher generation into DRAINING. It can freeze a higher
 * index only after a separate eligibility transition has already made it non-readable. For
 * generation zero it additionally proves that the recovery root selected an NRC1 entry carrying
 * the exact source commit, then requires either completed trim or at least one current
 * COMMITTED/ACTIVE replacement before admitting index, marker, and commit-node removals.
 */
public final class SourceRetirementPlanBuilder implements GcPlanMetadataRevalidator {
    private static final List<ReadView> VIEWS = List.of(
            ReadView.COMMITTED, ReadView.TOPIC_COMPACTED);
    private static final ReadTargetCodecRegistry TARGET_CODECS =
            ReadTargetCodecRegistry.phase15();

    private final String cluster;
    private final GenerationMetadataStore generations;
    private final SourceRetirementMetadataStore sources;
    private final RecoveryCheckpointCodecV1 checkpoints;
    private final PhysicalGcConfig config;
    private final F4Keyspace f4Keys;
    private final OxiaKeyspace l0Keys;
    private final RecoveryReplacementVerifier replacements;
    private final CompletedTrimRetirementVerifier completedTrim;
    private final HigherGenerationRetirementEligibilityVerifier higherEligibility;

    public SourceRetirementPlanBuilder(
            String cluster,
            OxiaMetadataStore l0,
            GenerationMetadataStore generations,
            PhysicalObjectMetadataStore physicalObjects,
            SourceRetirementMetadataStore sources,
            RecoveryCheckpointCodecV1 checkpoints,
            PhysicalGcConfig config) {
        this.cluster = requireText(cluster, "cluster");
        Objects.requireNonNull(l0, "l0");
        this.generations = Objects.requireNonNull(generations, "generations");
        Objects.requireNonNull(physicalObjects, "physicalObjects");
        this.sources = Objects.requireNonNull(sources, "sources");
        this.checkpoints = Objects.requireNonNull(checkpoints, "checkpoints");
        this.config = Objects.requireNonNull(config, "config");
        this.f4Keys = new F4Keyspace(cluster);
        this.l0Keys = new OxiaKeyspace(cluster);
        this.replacements = new RecoveryReplacementVerifier(
                cluster,
                generations,
                physicalObjects,
                checkpoints,
                config);
        this.completedTrim = new CompletedTrimRetirementVerifier(
                cluster,
                l0,
                generations);
        HigherGenerationRecoveryCoverageVerifier committed =
                new HigherGenerationRecoveryCoverageVerifier(
                        cluster,
                        generations,
                        checkpoints,
                        replacements,
                        config);
        TopicCompactedReplacementVerifier topicCompacted =
                new TopicCompactedReplacementVerifier(
                        cluster,
                        generations,
                        replacements,
                        config);
        this.higherEligibility = new HigherGenerationRetirementEligibilityVerifier(
                completedTrim,
                committed,
                topicCompacted);
    }

    /** Builds one canonical process-local removal set for an ACTIVE affected-stream candidate. */
    public CompletableFuture<List<GcPlannedMetadataRemoval>> build(GcCandidate candidate) {
        GcCandidate exact = Objects.requireNonNull(candidate, "candidate");
        if (exact.rootState() != GcCandidateRootState.ACTIVE_DISCOVERY) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "source retirement planning requires an ACTIVE_DISCOVERY candidate"));
        }
        GcReferenceQuery query = exact.referenceQuery();
        if (query.kind() == GcReferenceQueryKind.OWNERLESS_ORPHAN_CANDIDATE) {
            return CompletableFuture.failedFuture(invariant(
                    "source retirement cannot infer affected streams for an ownerless candidate"));
        }
        return scan(query, 0, 0, Optional.empty(), null, new ArrayList<>(), 0)
                .thenCompose(candidates -> freeze(query, candidates, 0, new ArrayList<>()))
                .thenApply(this::canonicalRemovals)
                .thenCompose(removals -> reload(exact, removals).thenApply(reloaded -> {
                    if (!reloaded.equals(removals)) {
                        throw condition(
                                "source-retirement metadata changed while the plan was frozen");
                    }
                    return removals;
                }));
    }

    @Override
    public CompletableFuture<List<GcPlannedMetadataRemoval>> reload(
            GcCandidate candidate,
            List<GcPlannedMetadataRemoval> expectedRemovals) {
        Objects.requireNonNull(candidate, "candidate");
        List<GcPlannedMetadataRemoval> expected = GcPlanValidation.canonicalAllowEmpty(
                expectedRemovals,
                GcPlanValidation.METADATA_ORDER,
                config.maxReferencesPerDomainSnapshot(),
                "expectedRemovals");
        return reloadOne(expected, 0, new ArrayList<>())
                .thenApply(this::canonicalRemovals)
                .thenCompose(actual -> {
                    if (!actual.equals(expected)) {
                        return CompletableFuture.completedFuture(actual);
                    }
                    return reproveCandidateBindings(
                                    candidate.referenceQuery(),
                                    expected,
                                    0,
                                    new ArrayList<>())
                            .thenApply(this::canonicalRemovals)
                            .thenApply(reproved -> {
                                if (!reproved.equals(expected)) {
                                    throw condition(
                                            "source-retirement removals are not exactly bound to the candidate");
                                }
                                return actual;
                            });
                });
    }

    private CompletableFuture<List<VersionedGenerationCandidate>> scan(
            GcReferenceQuery query,
            int streamIndex,
            int viewIndex,
            Optional<F4ScanToken> continuation,
            String previousKey,
            List<VersionedGenerationCandidate> matching,
            int observed) {
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
                    observed);
        }
        StreamId stream = query.affectedStreams().get(streamIndex);
        ReadView view = VIEWS.get(viewIndex);
        int remaining = config.maxAuthoritiesPerDomainSnapshot() - observed;
        int limit = remaining == 0
                ? 1
                : Math.min(config.metadataScanPageSize(), remaining);
        return generations.scanIndex(
                        cluster,
                        stream,
                        view,
                        0,
                        Long.MAX_VALUE,
                        continuation,
                        limit)
                .thenCompose(page -> {
                    requireProgress(page, previousKey);
                    int nextObserved = Math.addExact(observed, page.values().size());
                    if (nextObserved > config.maxAuthoritiesPerDomainSnapshot()) {
                        return CompletableFuture.failedFuture(invariant(
                                "source-retirement generation scan exceeded its authority bound"));
                    }
                    for (VersionedGenerationCandidate candidate : page.values()) {
                        if (matches(query, candidate)) {
                            matching.add(candidate);
                        }
                    }
                    if (page.continuation().isPresent()) {
                        if (nextObserved >= config.maxAuthoritiesPerDomainSnapshot()) {
                            return CompletableFuture.failedFuture(invariant(
                                    "source-retirement generation scan exceeded its authority bound"));
                        }
                        return scan(
                                query,
                                streamIndex,
                                viewIndex,
                                page.continuation(),
                                page.values().get(page.values().size() - 1).key(),
                                matching,
                                nextObserved);
                    }
                    return scan(
                            query,
                            streamIndex,
                            viewIndex + 1,
                            Optional.empty(),
                            null,
                            matching,
                            nextObserved);
                });
    }

    private CompletableFuture<List<GcPlannedMetadataRemoval>> freeze(
            GcReferenceQuery query,
            List<VersionedGenerationCandidate> candidates,
            int index,
            List<GcPlannedMetadataRemoval> removals) {
        if (index == candidates.size()) {
            return CompletableFuture.completedFuture(List.copyOf(removals));
        }
        if (removals.size() >= config.maxReferencesPerDomainSnapshot()) {
            return CompletableFuture.failedFuture(invariant(
                    "source-retirement removal plan exceeded its configured bound"));
        }
        VersionedGenerationCandidate candidate = candidates.get(index);
        CompletableFuture<Void> add;
        if (candidate instanceof VersionedGenerationZeroIndex zero) {
            add = freezeGenerationZero(query, zero).thenAccept(removals::addAll);
        } else if (candidate instanceof VersionedGenerationIndex higher
                && higher.value().lifecycle() == GenerationLifecycle.DRAINING) {
            add = higherEligibility.prove(query, higher).thenAccept(ignored -> removals.add(
                    removal(
                            HigherGenerationIndexRetirementHandler.REMOVAL_TYPE,
                            higher)));
        } else {
            // The generation reference domain will veto every still-readable matching record.
            add = CompletableFuture.completedFuture(null);
        }
        return add.thenCompose(ignored -> freeze(
                query, candidates, index + 1, removals));
    }

    private CompletableFuture<List<GcPlannedMetadataRemoval>> freezeGenerationZero(
            GcReferenceQuery query,
            VersionedGenerationZeroIndex zero) {
        if (zero.value().tombstoned()) {
            return CompletableFuture.completedFuture(List.of());
        }
        StreamId stream = zero.value().streamId();
        return generations.getRecoveryRoot(cluster, stream).thenCompose(optionalRoot -> {
            VersionedRecoveryCheckpointRoot root = optionalRoot.orElseThrow(() -> condition(
                    "generation-zero source has no recovery root"));
            RecoveryCheckpointReferenceRecord reference = coveringCheckpoint(
                    root, zero.value());
            Checksum content = new Checksum(ChecksumType.SHA256, reference.contentSha256());
            return checkpoints.openAndVerify(
                            new ObjectKey(reference.objectKey()),
                            reference.objectLength(),
                            content,
                            config.operationTimeout())
                    .thenCompose(checkpoint -> {
                        replacements.requireCheckpointIdentity(
                                stream, reference, checkpoint);
                        return checkpoints.findCommitCoveringOffset(
                                        checkpoint,
                                        zero.value().offsetStart(),
                                        config.operationTimeout())
                                .thenApply(optionalEntry -> new CheckpointCommitEvidence(
                                        checkpoint,
                                        optionalEntry.orElseThrow(() ->
                                                condition(
                                                        "recovery checkpoint does not contain the generation-zero commit"))));
                    })
                    .thenCompose(evidence -> {
                        RecoveryCheckpointEntry entry = evidence.entry();
                        requireEntryMatchesIndex(entry, zero.value());
                        return proveGenerationZeroEligibility(
                                        query,
                                        stream,
                                        zero,
                                        evidence)
                                .thenCompose(eligibility -> {
                                    String commitKey = l0Keys.streamCommitKey(
                                            stream,
                                            entry.commitId());
                                    return sources.getCommitNodeByKey(cluster, commitKey)
                                            .thenCompose(optionalCommit -> {
                                                VersionedGenerationZeroCommit commit =
                                                        optionalCommit.orElseThrow(() -> condition(
                                                                "checkpoint-replaced source commit is absent before planning"));
                                                requireCommitMatches(
                                                        entry, zero.value(), commit);
                                                return sources.getCommittedMarker(
                                                                cluster,
                                                                stream,
                                                                commit.markerIdentity())
                                                        .thenCompose(optionalMarker -> {
                                                            VersionedGenerationZeroMarker marker =
                                                                    optionalMarker.orElseThrow(() -> condition(
                                                                            "generation-zero committed marker is absent before planning"));
                                                            requireMarkerMatches(
                                                                    commit, marker);
                                                            return revalidateGenerationZeroEligibility(
                                                                            eligibility)
                                                                    .thenCompose(ignored ->
                                                                            generations.getRecoveryRoot(
                                                                                    cluster,
                                                                                    stream))
                                                                    .thenApply(reloadedRoot -> {
                                                                        if (!reloadedRoot.equals(
                                                                                Optional.of(root))) {
                                                                            throw condition(
                                                                                    "recovery root changed while source facts were frozen");
                                                                        }
                                                                        return List.of(
                                                                                removal(
                                                                                        GenerationZeroIndexRetirementHandler.REMOVAL_TYPE,
                                                                                        zero),
                                                                                removal(
                                                                                        GenerationZeroMarkerRetirementHandler.REMOVAL_TYPE,
                                                                                        marker.key(),
                                                                                        marker.metadataVersion(),
                                                                                        marker.durableValueSha256()),
                                                                                removal(
                                                                                        GenerationZeroCommitRetirementHandler.REMOVAL_TYPE,
                                                                                        commit.key(),
                                                                                        commit.metadataVersion(),
                                                                                        commit.durableValueSha256()));
                                                                    });
                                                        });
                                            });
                                });
                    });
        });
    }

    private CompletableFuture<GenerationZeroEligibility> proveGenerationZeroEligibility(
            GcReferenceQuery query,
            StreamId stream,
            VersionedGenerationZeroIndex zero,
            CheckpointCommitEvidence evidence) {
        return completedTrim.proveIfCompleted(zero).thenCompose(trim -> {
            if (trim.isPresent()) {
                return CompletableFuture.completedFuture(
                        GenerationZeroEligibility.trim(trim.orElseThrow()));
            }
            return replacements.select(
                            query,
                            stream,
                            evidence.checkpoint(),
                            evidence.entry(),
                            RecoveryReplacementVerifier.ReplacementRequirement
                                    .generationZero(zero.value()))
                    .thenApply(GenerationZeroEligibility::replacement);
        });
    }

    private CompletableFuture<Void> revalidateGenerationZeroEligibility(
            GenerationZeroEligibility eligibility) {
        if (eligibility.trim().isPresent()) {
            return completedTrim.revalidate(eligibility.trim().orElseThrow());
        }
        return replacements.revalidate(eligibility.replacement().orElseThrow());
    }

    private CompletableFuture<List<GcPlannedMetadataRemoval>> reloadOne(
            List<GcPlannedMetadataRemoval> expected,
            int index,
            List<GcPlannedMetadataRemoval> actual) {
        if (index == expected.size()) {
            return CompletableFuture.completedFuture(List.copyOf(actual));
        }
        GcPlannedMetadataRemoval removal = expected.get(index);
        return reloadRemoval(removal).thenCompose(optional -> {
            optional.ifPresent(actual::add);
            return reloadOne(expected, index + 1, actual);
        });
    }

    private CompletableFuture<List<GcPlannedMetadataRemoval>> reproveCandidateBindings(
            GcReferenceQuery query,
            List<GcPlannedMetadataRemoval> expected,
            int index,
            List<GcPlannedMetadataRemoval> reproved) {
        if (index == expected.size()) {
            return CompletableFuture.completedFuture(List.copyOf(reproved));
        }
        GcPlannedMetadataRemoval removal = expected.get(index);
        if (!removal.removalType().equals(
                        GenerationZeroIndexRetirementHandler.REMOVAL_TYPE)
                && !removal.removalType().equals(
                        HigherGenerationIndexRetirementHandler.REMOVAL_TYPE)) {
            return reproveCandidateBindings(
                    query, expected, index + 1, reproved);
        }
        boolean generationZero = removal.removalType().equals(
                GenerationZeroIndexRetirementHandler.REMOVAL_TYPE);
        GenerationCandidateKeyIdentity identity;
        try {
            identity = f4Keys.parseGenerationIndexKey(removal.key());
            if (identity.generationZero() != generationZero) {
                throw invariant("generation removal type and key generation disagree");
            }
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return generations.getCandidateByKey(
                        cluster, identity.streamId(), identity.view(), removal.key())
                .thenCompose(optional -> {
                    VersionedGenerationCandidate candidate = optional.orElseThrow(() -> condition(
                            "source-retirement generation disappeared during candidate reproof"));
                    if (!matches(query, candidate)) {
                        return CompletableFuture.failedFuture(condition(
                                "source-retirement generation belongs to another physical candidate"));
                    }
                    if (generationZero) {
                        if (!(candidate instanceof VersionedGenerationZeroIndex zero)) {
                            return CompletableFuture.failedFuture(invariant(
                                    "generation-zero key decoded as a higher index"));
                        }
                        return freezeGenerationZero(query, zero).thenCompose(triple -> {
                            reproved.addAll(triple);
                            return reproveCandidateBindings(
                                    query, expected, index + 1, reproved);
                        });
                    }
                    if (!(candidate instanceof VersionedGenerationIndex higher)
                            || higher.value().lifecycle() != GenerationLifecycle.DRAINING) {
                        return CompletableFuture.failedFuture(condition(
                                "higher-generation candidate is no longer DRAINING"));
                    }
                    return higherEligibility.prove(query, higher).thenCompose(ignored -> {
                        reproved.add(removal(
                                HigherGenerationIndexRetirementHandler.REMOVAL_TYPE,
                                higher));
                        return reproveCandidateBindings(
                                query, expected, index + 1, reproved);
                    });
                });
    }

    private CompletableFuture<Optional<GcPlannedMetadataRemoval>> reloadRemoval(
            GcPlannedMetadataRemoval expected) {
        return switch (expected.removalType()) {
            case GenerationZeroIndexRetirementHandler.REMOVAL_TYPE ->
                    reloadGeneration(expected, true);
            case HigherGenerationIndexRetirementHandler.REMOVAL_TYPE ->
                    reloadGeneration(expected, false);
            case GenerationZeroMarkerRetirementHandler.REMOVAL_TYPE ->
                    sources.getCommittedMarkerByKey(cluster, expected.key())
                            .thenApply(optional -> optional.map(marker -> removal(
                                    expected.removalType(),
                                    marker.key(),
                                    marker.metadataVersion(),
                                    marker.durableValueSha256())));
            case GenerationZeroCommitRetirementHandler.REMOVAL_TYPE ->
                    sources.getCommitNodeByKey(cluster, expected.key())
                            .thenApply(optional -> optional.map(commit -> removal(
                                    expected.removalType(),
                                    commit.key(),
                                    commit.metadataVersion(),
                                    commit.durableValueSha256())));
            default -> CompletableFuture.failedFuture(invariant(
                    "source-retirement revalidator does not own removal type "
                            + expected.removalType()));
        };
    }

    private CompletableFuture<Optional<GcPlannedMetadataRemoval>> reloadGeneration(
            GcPlannedMetadataRemoval expected,
            boolean requireGenerationZero) {
        final GenerationCandidateKeyIdentity identity;
        try {
            identity = f4Keys.parseGenerationIndexKey(expected.key());
            if (identity.generationZero() != requireGenerationZero) {
                throw invariant("generation removal type and key generation disagree");
            }
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return generations.getCandidateByKey(
                        cluster, identity.streamId(), identity.view(), expected.key())
                .thenApply(optional -> optional.map(candidate -> {
                    if (requireGenerationZero
                            && !(candidate instanceof VersionedGenerationZeroIndex)) {
                        throw invariant("generation-zero key decoded as a higher index");
                    }
                    if (!requireGenerationZero
                            && !(candidate instanceof VersionedGenerationIndex)) {
                        throw invariant("higher-generation key decoded as generation zero");
                    }
                    return removal(expected.removalType(), candidate);
                }));
    }

    private List<GcPlannedMetadataRemoval> canonicalRemovals(
            List<GcPlannedMetadataRemoval> supplied) {
        List<GcPlannedMetadataRemoval> ordered = supplied.stream()
                .sorted(GcPlanValidation.METADATA_ORDER)
                .toList();
        return GcPlanValidation.canonicalAllowEmpty(
                ordered,
                GcPlanValidation.METADATA_ORDER,
                config.maxReferencesPerDomainSnapshot(),
                "sourceRetirementRemovals");
    }

    private static boolean matches(
            GcReferenceQuery query,
            VersionedGenerationCandidate candidate) {
        ReadTarget target;
        if (candidate instanceof VersionedGenerationZeroIndex zero) {
            if (zero.value().tombstoned()) {
                return false;
            }
            target = zero.value().readTarget();
        } else if (candidate instanceof VersionedGenerationIndex higher) {
            if (higher.value().lifecycle() == GenerationLifecycle.RETIRED
                    || higher.value().lifecycle() == GenerationLifecycle.ABORTED) {
                return false;
            }
            target = TARGET_CODECS.decode(higher.value().readTarget());
        } else {
            throw invariant("unknown generation candidate type");
        }
        if (!(target instanceof ObjectSliceReadTarget objectTarget)
                || !objectTarget.objectKey().equals(query.object().objectKey())) {
            return false;
        }
        return query.object().objectId().isEmpty()
                || query.object().objectId().orElseThrow().equals(objectTarget.objectId());
    }

    private static RecoveryCheckpointReferenceRecord coveringCheckpoint(
            VersionedRecoveryCheckpointRoot root,
            OffsetIndexEntry index) {
        List<RecoveryCheckpointReferenceRecord> matching = root.value().checkpoints().stream()
                .filter(reference -> reference.coveredStartOffset() <= index.offsetStart()
                        && reference.coveredEndOffset() >= index.offsetEnd()
                        && reference.firstCommitVersion() <= index.commitVersion()
                        && reference.lastCommitVersion() >= index.commitVersion())
                .toList();
        if (matching.size() != 1) {
            throw condition(
                    "generation-zero source is not covered by one exact recovery checkpoint");
        }
        return matching.get(0);
    }

    private static void requireEntryMatchesIndex(
            RecoveryCheckpointEntry entry,
            OffsetIndexEntry index) {
        if (entry.range().startOffset() != index.offsetStart()
                || entry.range().endOffset() != index.offsetEnd()
                || entry.commitVersion() != index.commitVersion()
                || entry.cumulativeSizeAtEnd() != index.cumulativeSize()) {
            throw invariant("recovery checkpoint entry does not match generation-zero index");
        }
    }

    private static void requireCommitMatches(
            RecoveryCheckpointEntry entry,
            OffsetIndexEntry index,
            VersionedGenerationZeroCommit commit) {
        StreamCommitTargetRecord value = commit.canonicalCommit();
        if (!commit.commitId().equals(entry.commitId())
                || !commit.canonicalCommitRecordSha256().equals(
                        entry.canonicalCommitRecordSha256())
                || value.offsetStart() != index.offsetStart()
                || value.offsetEnd() != index.offsetEnd()
                || value.cumulativeSize() != index.cumulativeSize()
                || value.commitVersion() != index.commitVersion()
                || !value.readTarget().equals(TARGET_CODECS.encode(index.readTarget()))
                || !value.payloadFormat().equals(index.payloadFormat().name())
                || value.recordCount() != index.recordCount()
                || value.entryCount() != index.entryCount()
                || value.logicalBytes() != index.logicalBytes()
                || !value.schemaRefs().equals(index.schemaRefs())
                || !ProjectionIdentity.decode(value.projectionRef()).equals(
                        index.projectionRef())) {
            throw invariant(
                    "checkpoint-replaced commit does not match generation-zero index facts");
        }
    }

    private static void requireMarkerMatches(
            VersionedGenerationZeroCommit commit,
            VersionedGenerationZeroMarker marker) {
        if (!marker.streamId().equals(commit.streamId())
                || !marker.identity().equals(commit.markerIdentity())
                || marker.offsetStart() != commit.offsetStart()
                || marker.offsetEnd() != commit.offsetEnd()
                || marker.commitVersion() != commit.commitVersion()
                || marker.readTargetIdentitySha256()
                        .map(value -> !value.value().equals(
                                commit.canonicalCommit().readTarget()
                                        .identityChecksumValue()))
                        .orElse(false)) {
            throw invariant("generation-zero marker does not match its source commit");
        }
    }

    private static GcPlannedMetadataRemoval removal(
            String type,
            VersionedGenerationCandidate candidate) {
        return removal(
                type,
                candidate.key(),
                candidate.metadataVersion(),
                candidate.durableValueSha256());
    }

    private static GcPlannedMetadataRemoval removal(
            String type,
            String key,
            long version,
            Checksum digest) {
        return new GcPlannedMetadataRemoval(type, key, version, digest);
    }

    private static void requireProgress(GenerationScanPage page, String previousKey) {
        if (page.continuation().isPresent() && page.values().isEmpty()) {
            throw invariant("generation scan returned an empty continuation page");
        }
        if (previousKey != null
                && !page.values().isEmpty()
                && page.values().get(0).key().compareTo(previousKey) <= 0) {
            throw invariant("source-retirement generation scan did not advance");
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

    private static NereusException condition(String message) {
        return new NereusException(ErrorCode.METADATA_CONDITION_FAILED, true, message);
    }

    private record CheckpointCommitEvidence(
            RecoveryCheckpointObject checkpoint,
            RecoveryCheckpointEntry entry) {
        private CheckpointCommitEvidence {
            Objects.requireNonNull(checkpoint, "checkpoint");
            Objects.requireNonNull(entry, "entry");
        }
    }

    private record GenerationZeroEligibility(
            Optional<CompletedTrimRetirementVerifier.CompletedTrimProof> trim,
            Optional<RecoveryReplacementVerifier.HealthyReplacement> replacement) {
        private GenerationZeroEligibility {
            trim = Objects.requireNonNull(trim, "trim");
            replacement = Objects.requireNonNull(replacement, "replacement");
            if (trim.isPresent() == replacement.isPresent()) {
                throw new IllegalArgumentException(
                        "generation-zero retirement requires exactly one eligibility proof");
            }
        }

        private static GenerationZeroEligibility trim(
                CompletedTrimRetirementVerifier.CompletedTrimProof proof) {
            return new GenerationZeroEligibility(
                    Optional.of(Objects.requireNonNull(proof, "proof")),
                    Optional.empty());
        }

        private static GenerationZeroEligibility replacement(
                RecoveryReplacementVerifier.HealthyReplacement proof) {
            return new GenerationZeroEligibility(
                    Optional.empty(),
                    Optional.of(Objects.requireNonNull(proof, "proof")));
        }
    }

}
