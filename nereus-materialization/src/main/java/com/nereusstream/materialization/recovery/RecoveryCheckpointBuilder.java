/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.recovery;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PublicationId;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamState;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.recovery.AnchorAwareCommitWalk;
import com.nereusstream.core.recovery.AnchorAwareCommitWalker;
import com.nereusstream.materialization.MaterializationConfig;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationIndexIdentity;
import com.nereusstream.metadata.oxia.GenerationScanPage;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.ProjectionIdentity;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedMaterializationStreamRegistration;
import com.nereusstream.metadata.oxia.VersionedRecoveryCheckpointRoot;
import com.nereusstream.metadata.oxia.codec.GenerationIndexRecordCodecV1;
import com.nereusstream.metadata.oxia.codec.MetadataRecordCodecFactory;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointReferenceRecord;
import com.nereusstream.metadata.oxia.records.StreamCommitTargetRecord;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointEntry;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointFormatV1;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointPublication;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointWriteRequest;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Builds one exact checkpointable commit prefix without mutating metadata or object storage. */
public final class RecoveryCheckpointBuilder {
    private static final int MAX_GENERATION_CANDIDATES = 4_096;
    private static final int MAX_ROOT_REFERENCES = 32;

    private final String cluster;
    private final OxiaMetadataStore l0Store;
    private final GenerationMetadataStore generationStore;
    private final AnchorAwareCommitWalker commitWalker;
    private final MaterializationConfig config;
    private final Clock clock;
    private final GenerationIndexRecordCodecV1 generationCodec = new GenerationIndexRecordCodecV1();

    public RecoveryCheckpointBuilder(
            String cluster,
            OxiaMetadataStore l0Store,
            GenerationMetadataStore generationStore,
            AnchorAwareCommitWalker commitWalker,
            MaterializationConfig config,
            Clock clock) {
        this.cluster = requireText(cluster, "cluster");
        this.l0Store = Objects.requireNonNull(l0Store, "l0Store");
        this.generationStore = Objects.requireNonNull(generationStore, "generationStore");
        this.commitWalker = Objects.requireNonNull(commitWalker, "commitWalker");
        this.config = Objects.requireNonNull(config, "config");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public CompletableFuture<RecoveryCheckpointBuildResult> build(
            StreamId streamId,
            VersionedRecoveryCheckpointRoot baseRoot,
            VersionedMaterializationStreamRegistration registration,
            String checkpointAttemptId) {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(baseRoot, "baseRoot");
        Objects.requireNonNull(registration, "registration");
        String attemptId = requireText(checkpointAttemptId, "checkpointAttemptId");
        validateStableInputs(streamId, baseRoot, registration);
        int pageSize = Math.min(config.plannerPageSize(), 1_000);
        CompletableFuture<AnchorAwareCommitWalk> walk = commitWalker.walk(
                streamId, config.recoveryCheckpointMaxEntries(), pageSize);
        CompletableFuture<StreamMetadataSnapshot> snapshot = l0Store.getStreamSnapshot(cluster, streamId);
        return walk.thenCombine(snapshot, BuildObservation::new)
                .thenCompose(observation -> buildFromObservation(
                        streamId, baseRoot, registration, attemptId, observation));
    }

    /** Re-proves the exact selected prefix/index facts immediately before root publication. */
    public CompletableFuture<Void> revalidate(RecoveryCheckpointPlan plan) {
        Objects.requireNonNull(plan, "plan");
        StreamId streamId = plan.writeRequest().streamId();
        int pageSize = Math.min(config.plannerPageSize(), 1_000);
        CompletableFuture<Optional<VersionedRecoveryCheckpointRoot>> root =
                generationStore.getRecoveryRoot(cluster, streamId);
        CompletableFuture<Optional<VersionedMaterializationStreamRegistration>> registration =
                generationStore.getStreamRegistration(cluster, streamId);
        CompletableFuture<AnchorAwareCommitWalk> walk = commitWalker.walk(
                streamId, config.recoveryCheckpointMaxEntries(), pageSize);
        return CompletableFuture.allOf(root, registration, walk)
                .thenCompose(ignored -> {
                    if (!root.join().equals(Optional.of(plan.baseRoot()))
                            || !registration.join().equals(Optional.of(plan.registration()))) {
                        return CompletableFuture.failedFuture(condition(
                                "recovery checkpoint root/registration changed before publication"));
                    }
                    AnchorAwareCommitWalk current = walk.join();
                    requireSelectedCommitsUnchanged(plan, current);
                    List<CompletableFuture<Void>> indexChecks = plan.targets().stream()
                            .map(target -> revalidateIndex(target.index()))
                            .toList();
                    return CompletableFuture.allOf(indexChecks.toArray(CompletableFuture[]::new));
                });
    }

    private CompletableFuture<Void> revalidateIndex(VersionedGenerationIndex expected) {
        GenerationIndexRecord value = expected.value();
        GenerationIndexIdentity identity = new GenerationIndexIdentity(
                new StreamId(value.streamId()),
                ReadView.fromWireId(value.readViewId()),
                value.offsetEnd(),
                value.generation());
        return generationStore.getIndex(cluster, identity).thenApply(actual -> {
            if (!actual.equals(Optional.of(expected))) {
                throw condition("recovery checkpoint target index changed before publication");
            }
            return null;
        });
    }

    private static void requireSelectedCommitsUnchanged(
            RecoveryCheckpointPlan plan,
            AnchorAwareCommitWalk current) {
        if (!current.recoveryRoot().equals(Optional.of(plan.baseRoot()))
                || !current.anchorReached()) {
            throw condition("recovery root/live tail changed before checkpoint publication");
        }
        Map<String, com.nereusstream.metadata.oxia.AppendRecoveryCommit> expected = new HashMap<>();
        for (var evidence : plan.commitWalk().commitsNewestFirst()) {
            expected.put(evidence.canonicalCommit().commitId(), evidence);
        }
        Map<String, com.nereusstream.metadata.oxia.AppendRecoveryCommit> actual = new HashMap<>();
        for (var evidence : current.commitsNewestFirst()) {
            actual.put(evidence.canonicalCommit().commitId(), evidence);
        }
        for (RecoveryCheckpointEntry entry : plan.entries()) {
            var expectedEvidence = expected.get(entry.commitId());
            var actualEvidence = actual.get(entry.commitId());
            if (expectedEvidence == null
                    || actualEvidence == null
                    || !sameCommitEvidence(expectedEvidence, actualEvidence)) {
                throw condition("selected recovery commit changed or lost reachability");
            }
        }
    }

    private static boolean sameCommitEvidence(
            com.nereusstream.metadata.oxia.AppendRecoveryCommit left,
            com.nereusstream.metadata.oxia.AppendRecoveryCommit right) {
        return left.key().equals(right.key())
                && left.sourceEncoding() == right.sourceEncoding()
                && left.canonicalCommit().equals(right.canonicalCommit())
                && left.sourceMetadataVersion() == right.sourceMetadataVersion()
                && left.sourceRecordSha256().equals(right.sourceRecordSha256())
                && left.canonicalCommitRecordSha256().equals(right.canonicalCommitRecordSha256());
    }

    private CompletableFuture<RecoveryCheckpointBuildResult> buildFromObservation(
            StreamId streamId,
            VersionedRecoveryCheckpointRoot baseRoot,
            VersionedMaterializationStreamRegistration registration,
            String attemptId,
            BuildObservation observation) {
        AnchorAwareCommitWalk walk = observation.walk();
        StreamMetadataSnapshot snapshot = observation.snapshot();
        validateObservation(streamId, baseRoot, registration, walk, snapshot);
        if (!walk.anchorReached()) {
            return completed(RecoveryCheckpointBuildStatus.TAIL_SCAN_LIMIT);
        }
        if (walk.commitsNewestFirst().isEmpty()) {
            return completed(RecoveryCheckpointBuildStatus.NO_LIVE_TAIL);
        }

        RetainedRoot retained = retainedRoot(baseRoot, snapshot.trim().trimOffset());
        if (retained.status().isPresent()) {
            return completed(retained.status().orElseThrow());
        }
        if (retained.references().size() >= MAX_ROOT_REFERENCES) {
            return completed(RecoveryCheckpointBuildStatus.REFERENCE_LIMIT);
        }

        List<com.nereusstream.metadata.oxia.AppendRecoveryCommit> oldestFirst = new ArrayList<>(
                walk.commitsNewestFirst());
        java.util.Collections.reverse(oldestFirst);
        List<com.nereusstream.metadata.oxia.AppendRecoveryCommit> eligible = eligiblePrefix(
                oldestFirst, retained.nextCoverageOffset());
        if (eligible.isEmpty()) {
            return completed(RecoveryCheckpointBuildStatus.NO_ELIGIBLE_PREFIX);
        }
        long maxOffsetEnd = eligible.get(eligible.size() - 1).canonicalCommit().offsetEnd();
        long minOffsetEnd = Math.addExact(retained.nextCoverageOffset(), 1);
        return scanGenerationCandidates(
                        streamId,
                        minOffsetEnd,
                        maxOffsetEnd,
                        Optional.empty(),
                        new ArrayList<>(),
                        0)
                .thenApply(candidates -> assemble(
                        baseRoot,
                        registration,
                        walk,
                        attemptId,
                        retained.references(),
                        eligible,
                        candidates));
    }

    private CompletableFuture<List<VersionedGenerationIndex>> scanGenerationCandidates(
            StreamId streamId,
            long minOffsetEnd,
            long maxOffsetEnd,
            Optional<F4ScanToken> continuation,
            List<VersionedGenerationIndex> accumulator,
            int scannedCandidates) {
        int remaining = MAX_GENERATION_CANDIDATES - scannedCandidates;
        if (remaining <= 0) {
            return CompletableFuture.failedFuture(invariant(
                    "recovery checkpoint generation scan exceeded 4096 candidates"));
        }
        int limit = Math.min(config.plannerPageSize(), remaining);
        return generationStore.scanIndex(
                        cluster,
                        streamId,
                        ReadView.COMMITTED,
                        minOffsetEnd,
                        maxOffsetEnd,
                        continuation,
                        limit)
                .thenCompose(page -> appendCandidatePage(
                        streamId,
                        minOffsetEnd,
                        maxOffsetEnd,
                        page,
                        accumulator,
                        scannedCandidates));
    }

    private CompletableFuture<List<VersionedGenerationIndex>> appendCandidatePage(
            StreamId streamId,
            long minOffsetEnd,
            long maxOffsetEnd,
            GenerationScanPage page,
            List<VersionedGenerationIndex> accumulator,
            int scannedCandidates) {
        for (VersionedGenerationCandidate candidate : page.values()) {
            if (candidate instanceof VersionedGenerationIndex index) {
                accumulator.add(index);
            }
        }
        int observed = Math.addExact(scannedCandidates, page.values().size());
        if (page.continuation().isEmpty()) {
            return CompletableFuture.completedFuture(List.copyOf(accumulator));
        }
        if (observed >= MAX_GENERATION_CANDIDATES) {
            return CompletableFuture.failedFuture(invariant(
                    "recovery checkpoint generation scan has more than 4096 candidates"));
        }
        return scanGenerationCandidates(
                streamId,
                minOffsetEnd,
                maxOffsetEnd,
                page.continuation(),
                accumulator,
                observed);
    }

    private RecoveryCheckpointBuildResult assemble(
            VersionedRecoveryCheckpointRoot baseRoot,
            VersionedMaterializationStreamRegistration registration,
            AnchorAwareCommitWalk walk,
            String attemptId,
            List<RecoveryCheckpointReferenceRecord> retainedReferences,
            List<com.nereusstream.metadata.oxia.AppendRecoveryCommit> eligible,
            List<VersionedGenerationIndex> candidates) {
        List<VersionedGenerationIndex> valid = candidates.stream()
                .filter(index -> validCandidate(index, registration.value(), eligible))
                .sorted(candidatePreference())
                .toList();

        List<com.nereusstream.metadata.oxia.AppendRecoveryCommit> selectedEntries = new ArrayList<>();
        Map<String, VersionedGenerationIndex> selectedTargets = new LinkedHashMap<>();
        for (com.nereusstream.metadata.oxia.AppendRecoveryCommit commit : eligible) {
            Optional<VersionedGenerationIndex> covering = valid.stream()
                    .filter(index -> covers(index.value(), commit.canonicalCommit()))
                    .findFirst();
            if (covering.isEmpty()) {
                break;
            }
            selectedEntries.add(commit);
            selectedTargets.putIfAbsent(covering.orElseThrow().key(), covering.orElseThrow());
        }
        if (selectedEntries.isEmpty()) {
            return RecoveryCheckpointBuildResult.skipped(
                    RecoveryCheckpointBuildStatus.NO_ELIGIBLE_PREFIX);
        }

        long selectedEnd = selectedEntries.get(selectedEntries.size() - 1)
                .canonicalCommit().offsetEnd();
        long initialSelectedEnd = selectedEnd;
        selectedTargets.values().removeIf(index -> index.value().offsetEnd() > initialSelectedEnd);
        boolean everySelectedEntryCovered = selectedEntries.stream().allMatch(commit ->
                selectedTargets.values().stream().anyMatch(index ->
                        covers(index.value(), commit.canonicalCommit())));
        if (!everySelectedEntryCovered) {
            int safeCount = 0;
            for (com.nereusstream.metadata.oxia.AppendRecoveryCommit commit : selectedEntries) {
                if (selectedTargets.values().stream().noneMatch(index ->
                        covers(index.value(), commit.canonicalCommit()))) {
                    break;
                }
                safeCount++;
            }
            selectedEntries = new ArrayList<>(selectedEntries.subList(0, safeCount));
            if (selectedEntries.isEmpty()) {
                return RecoveryCheckpointBuildResult.skipped(
                        RecoveryCheckpointBuildStatus.NO_ELIGIBLE_PREFIX);
            }
            selectedEnd = selectedEntries.get(selectedEntries.size() - 1)
                    .canonicalCommit().offsetEnd();
            long finalSelectedEnd = selectedEnd;
            selectedTargets.values().removeIf(index -> index.value().offsetEnd() > finalSelectedEnd);
        }

        List<RecoveryCheckpointTarget> targets = selectedTargets.values().stream()
                .map(this::target)
                .sorted(Comparator
                        .comparingLong((RecoveryCheckpointTarget value) -> value.publication().generation())
                        .thenComparing(value -> value.publication().publicationId().value()))
                .toList();
        Map<String, Integer> targetIndexes = new HashMap<>();
        for (int index = 0; index < targets.size(); index++) {
            targetIndexes.put(targets.get(index).index().key(), index);
        }
        List<RecoveryCheckpointEntry> entries = new ArrayList<>(selectedEntries.size());
        for (com.nereusstream.metadata.oxia.AppendRecoveryCommit evidence : selectedEntries) {
            StreamCommitTargetRecord commit = evidence.canonicalCommit();
            List<Integer> covering = selectedTargets.values().stream()
                    .filter(index -> covers(index.value(), commit))
                    .sorted(candidatePreference().reversed())
                    .limit(RecoveryCheckpointFormatV1.MAX_PUBLICATION_REFS_PER_ENTRY)
                    .map(index -> targetIndexes.get(index.key()))
                    .sorted()
                    .toList();
            if (covering.isEmpty()) {
                throw invariant("selected recovery commit lost every publication-table target");
            }
            entries.add(new RecoveryCheckpointEntry(
                    commit.commitVersion(),
                    new OffsetRange(commit.offsetStart(), commit.offsetEnd()),
                    commit.cumulativeSize(),
                    commit.commitId(),
                    commit.previousCommitId(),
                    evidence.canonicalCommitRecord(),
                    evidence.canonicalCommitRecordSha256(),
                    covering));
        }

        StreamCommitTargetRecord first = selectedEntries.get(0).canonicalCommit();
        StreamCommitTargetRecord last = selectedEntries.get(selectedEntries.size() - 1).canonicalCommit();
        long cumulativeStart = Math.subtractExact(first.cumulativeSize(), first.logicalBytes());
        long sequence = Math.addExact(baseRoot.value().checkpointSequence(), 1);
        MaterializationStreamRegistrationRecord registrationValue = registration.value();
        RecoveryCheckpointWriteRequest request = new RecoveryCheckpointWriteRequest(
                cluster,
                new StreamId(baseRoot.value().streamId()),
                sequence,
                attemptId,
                new OffsetRange(first.offsetStart(), last.offsetEnd()),
                first.commitVersion(),
                last.commitVersion(),
                cumulativeStart,
                last.cumulativeSize(),
                first.commitId(),
                last.commitId(),
                walk.observedHead().lastCommitId(),
                walk.observedHead().commitVersion(),
                new Checksum(ChecksumType.SHA256, registrationValue.projectionIdentitySha256()),
                entries.size(),
                targets.size());
        return RecoveryCheckpointBuildResult.ready(new RecoveryCheckpointPlan(
                baseRoot,
                registration,
                walk,
                request,
                retainedReferences,
                targets,
                entries,
                config.recoveryCheckpointMaxBytes()));
    }

    private List<com.nereusstream.metadata.oxia.AppendRecoveryCommit> eligiblePrefix(
            List<com.nereusstream.metadata.oxia.AppendRecoveryCommit> oldestFirst,
            long coverageStart) {
        int start = 0;
        while (start < oldestFirst.size()
                && oldestFirst.get(start).canonicalCommit().offsetEnd() <= coverageStart) {
            start++;
        }
        if (start == oldestFirst.size()
                || oldestFirst.get(start).canonicalCommit().offsetStart() != coverageStart) {
            return List.of();
        }
        long now = clock.millis();
        long grace = config.appendReplayGrace().toMillis();
        List<com.nereusstream.metadata.oxia.AppendRecoveryCommit> result = new ArrayList<>();
        for (int index = start;
                index < oldestFirst.size() && result.size() < config.recoveryCheckpointMaxEntries();
                index++) {
            StreamCommitTargetRecord commit = oldestFirst.get(index).canonicalCommit();
            long eligibleAt;
            try {
                eligibleAt = Math.addExact(commit.preparedAtMillis(), grace);
            } catch (ArithmeticException overflow) {
                break;
            }
            if (eligibleAt >= now) {
                break;
            }
            result.add(oldestFirst.get(index));
        }
        return List.copyOf(result);
    }

    private RetainedRoot retainedRoot(
            VersionedRecoveryCheckpointRoot baseRoot,
            long trimOffset) {
        var root = baseRoot.value();
        if (root.checkpoints().isEmpty()) {
            return new RetainedRoot(List.of(), trimOffset, Optional.empty());
        }
        if (trimOffset <= root.coveredStartOffset()) {
            return new RetainedRoot(root.checkpoints(), root.coveredEndOffset(), Optional.empty());
        }
        if (trimOffset >= root.coveredEndOffset()) {
            return new RetainedRoot(List.of(), trimOffset, Optional.empty());
        }
        return new RetainedRoot(
                List.of(),
                trimOffset,
                Optional.of(RecoveryCheckpointBuildStatus.MERGE_REQUIRED));
    }

    private boolean validCandidate(
            VersionedGenerationIndex index,
            MaterializationStreamRegistrationRecord registration,
            List<com.nereusstream.metadata.oxia.AppendRecoveryCommit> commits) {
        GenerationIndexRecord value = index.value();
        Object decodedTarget;
        try {
            decodedTarget = ReadTargetCodecRegistry.phase15().decode(value.readTarget());
        } catch (RuntimeException failure) {
            throw invariant("recovery checkpoint candidate has a malformed read target", failure);
        }
        if (value.lifecycle() != GenerationLifecycle.COMMITTED
                || value.readViewId() != ReadView.COMMITTED.wireId()
                || !value.streamId().equals(registration.streamId())
                || !value.projectionRef().equals(registration.projectionRef())
                || !(decodedTarget instanceof ObjectSliceReadTarget)
                || !durableIndexSha256(value).equals(index.durableValueSha256())) {
            return false;
        }
        int first = -1;
        int last = -1;
        for (int indexValue = 0; indexValue < commits.size(); indexValue++) {
            StreamCommitTargetRecord commit = commits.get(indexValue).canonicalCommit();
            if (commit.offsetStart() == value.offsetStart()
                    && commit.commitVersion() == value.firstCommitVersion()) {
                first = indexValue;
            }
            if (commit.offsetEnd() == value.offsetEnd()
                    && commit.commitVersion() == value.lastCommitVersion()) {
                last = indexValue;
            }
        }
        if (first < 0 || last < first) {
            return false;
        }
        StreamCommitTargetRecord firstCommit = commits.get(first).canonicalCommit();
        StreamCommitTargetRecord lastCommit = commits.get(last).canonicalCommit();
        return value.cumulativeSizeAtStart()
                        == Math.subtractExact(firstCommit.cumulativeSize(), firstCommit.logicalBytes())
                && value.cumulativeSizeAtEnd() == lastCommit.cumulativeSize()
                && value.sourceRecordCount() == Math.subtractExact(value.offsetEnd(), value.offsetStart())
                && value.logicalBytes()
                        == Math.subtractExact(value.cumulativeSizeAtEnd(), value.cumulativeSizeAtStart());
    }

    private RecoveryCheckpointTarget target(VersionedGenerationIndex index) {
        GenerationIndexRecord value = index.value();
        byte[] canonical = generationCodec.encode(value.withMetadataVersion(0));
        Checksum digest = sha256(canonical);
        return new RecoveryCheckpointTarget(
                new RecoveryCheckpointPublication(
                        value.generation(),
                        new PublicationId(value.publicationId()),
                        new OffsetRange(value.offsetStart(), value.offsetEnd()),
                        ByteBuffer.wrap(canonical),
                        digest),
                index);
    }

    private static boolean covers(
            GenerationIndexRecord index,
            StreamCommitTargetRecord commit) {
        return index.offsetStart() <= commit.offsetStart()
                && index.offsetEnd() >= commit.offsetEnd()
                && index.firstCommitVersion() <= commit.commitVersion()
                && index.lastCommitVersion() >= commit.commitVersion()
                && index.cumulativeSizeAtStart()
                        <= Math.subtractExact(commit.cumulativeSize(), commit.logicalBytes())
                && index.cumulativeSizeAtEnd() >= commit.cumulativeSize();
    }

    private static Comparator<VersionedGenerationIndex> candidatePreference() {
        return Comparator
                .comparingLong((VersionedGenerationIndex value) -> value.value().generation())
                .reversed()
                .thenComparingLong(value ->
                        Math.subtractExact(value.value().offsetEnd(), value.value().offsetStart()))
                .thenComparing(VersionedGenerationIndex::key);
    }

    private void validateStableInputs(
            StreamId streamId,
            VersionedRecoveryCheckpointRoot root,
            VersionedMaterializationStreamRegistration registration) {
        F4Keyspace keys = new F4Keyspace(cluster);
        if (!root.key().equals(keys.recoveryRootKey(streamId))
                || !root.value().streamId().equals(streamId.value())
                || !registration.key().equals(keys.materializationRegistryKey(streamId))
                || !registration.value().streamId().equals(streamId.value())) {
            throw invariant("recovery checkpoint root/registration key identity is inconsistent");
        }
        if (ProjectionIdentity.decode(registration.value().projectionRef()).isEmpty()) {
            throw invariant("recovery checkpoint registration has no live projection identity");
        }
    }

    private void validateObservation(
            StreamId streamId,
            VersionedRecoveryCheckpointRoot baseRoot,
            VersionedMaterializationStreamRegistration registration,
            AnchorAwareCommitWalk walk,
            StreamMetadataSnapshot snapshot) {
        if (!walk.recoveryRoot().equals(Optional.of(baseRoot))
                || !snapshot.metadata().streamId().equals(streamId.value())
                || walk.observedHead().commitVersion() > snapshot.committedEnd().commitVersion()
                || walk.observedHead().offsetEnd() > snapshot.committedEnd().committedEndOffset()
                || walk.observedHead().cumulativeSize() > snapshot.committedEnd().cumulativeSize()) {
            throw condition("recovery checkpoint observation changed its root/head identity");
        }
        StreamState state;
        StorageProfile profile;
        StorageProfile registered;
        try {
            state = StreamState.valueOf(snapshot.metadata().state());
            profile = StorageProfile.valueOf(snapshot.metadata().profile()).canonical();
            registered = StorageProfile.valueOf(registration.value().storageProfile()).canonical();
        } catch (IllegalArgumentException failure) {
            throw invariant("recovery checkpoint stream state/profile is unsupported", failure);
        }
        if ((state != StreamState.ACTIVE && state != StreamState.SEALED)
                || profile != registered
                || (profile != StorageProfile.OBJECT_WAL_SYNC_OBJECT
                        && profile != StorageProfile.OBJECT_WAL_ASYNC_OBJECT)) {
            throw condition("stream is not eligible for Object-WAL recovery checkpointing");
        }
        Optional<ProjectionRef> registeredProjection = decodeProjection(
                registration.value().projectionRef(),
                "recovery registration projection cannot be decoded");
        if (registeredProjection.isEmpty()) {
            throw invariant("recovery registration has no live projection identity");
        }
        for (var commit : walk.commitsNewestFirst()) {
            Optional<ProjectionRef> sourceProjection = decodeProjection(
                    commit.canonicalCommit().projectionRef(),
                    "live commit projection cannot be decoded");
            if (sourceProjection.isPresent()
                    && !sourceProjection.equals(registeredProjection)) {
                throw invariant("live commit projection differs from the recovery registration");
            }
        }
    }

    private static Optional<ProjectionRef> decodeProjection(
            String encoded, String failureMessage) {
        try {
            return ProjectionIdentity.decode(encoded);
        } catch (RuntimeException failure) {
            throw invariant(failureMessage, failure);
        }
    }

    private Checksum durableIndexSha256(GenerationIndexRecord value) {
        return sha256(MetadataRecordCodecFactory.encodeEnvelope(
                value.withMetadataVersion(0), GenerationIndexRecord.class));
    }

    private static CompletableFuture<RecoveryCheckpointBuildResult> completed(
            RecoveryCheckpointBuildStatus status) {
        return CompletableFuture.completedFuture(RecoveryCheckpointBuildResult.skipped(status));
    }

    private static Checksum sha256(byte[] bytes) {
        try {
            return new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static NereusException invariant(String message, Throwable cause) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION, false, message, cause);
    }

    private static NereusException condition(String message) {
        return new NereusException(ErrorCode.METADATA_CONDITION_FAILED, true, message);
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }

    private record BuildObservation(
            AnchorAwareCommitWalk walk,
            StreamMetadataSnapshot snapshot) {
    }

    private record RetainedRoot(
            List<RecoveryCheckpointReferenceRecord> references,
            long nextCoverageOffset,
            Optional<RecoveryCheckpointBuildStatus> status) {
        private RetainedRoot {
            references = List.copyOf(Objects.requireNonNull(references, "references"));
            Objects.requireNonNull(status, "status");
            if (nextCoverageOffset < 0) {
                throw new IllegalArgumentException("nextCoverageOffset must be non-negative");
            }
        }
    }
}
