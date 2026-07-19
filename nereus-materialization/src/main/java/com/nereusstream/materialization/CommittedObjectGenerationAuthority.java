/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.api.target.ReadTarget;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.GenerationIndexDigests;
import com.nereusstream.metadata.oxia.GenerationIndexIdentity;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedMaterializationCheckpoint;
import com.nereusstream.metadata.oxia.VersionedObjectProtection;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/** Reconstructs and revalidates the exact F4 metadata proof used before a primary-WAL source is released. */
public final class CommittedObjectGenerationAuthority implements CommittedGenerationRetirementAuthority {
    private static final int MAX_SCAN_ENTRIES = 4_096;
    private static final ReadTargetCodecRegistry TARGET_CODECS = ReadTargetCodecRegistry.phase15();

    private final String cluster;
    private final GenerationMetadataStore generations;
    private final PhysicalObjectMetadataStore physical;
    private final int pageSize;
    private final Duration operationTimeout;
    private final ScheduledExecutorService scheduler;
    private final F4Keyspace keys;

    public CommittedObjectGenerationAuthority(
            String cluster,
            GenerationMetadataStore generations,
            PhysicalObjectMetadataStore physical,
            int pageSize,
            Duration operationTimeout,
            ScheduledExecutorService scheduler) {
        this.cluster = text(cluster, "cluster");
        this.generations = Objects.requireNonNull(generations, "generations");
        this.physical = Objects.requireNonNull(physical, "physical");
        if (pageSize <= 0 || pageSize > 1_000) {
            throw new IllegalArgumentException("pageSize must be in [1, 1000]");
        }
        this.pageSize = pageSize;
        this.operationTimeout = positive(operationTimeout, "operationTimeout");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.keys = new F4Keyspace(this.cluster);
    }

    public CompletableFuture<Optional<CommittedObjectGenerationProof>> prove(
            StreamId streamId,
            OffsetRange sourceRange,
            long sourceCommitVersion) {
        try {
            StreamId stream = Objects.requireNonNull(streamId, "streamId");
            OffsetRange range = Objects.requireNonNull(sourceRange, "sourceRange");
            if (range.isEmpty() || sourceCommitVersion <= 0) {
                throw new IllegalArgumentException("committed Object replacement source identity is invalid");
            }
            MaterializationDeadline deadline = new MaterializationDeadline(operationTimeout, scheduler);
            CompletableFuture<Optional<CommittedObjectGenerationProof>> result = scanIndexes(
                            stream, range, Optional.empty(), new ArrayList<>(), 0, deadline)
                    .thenCompose(candidates -> proveCandidate(
                            stream, range, sourceCommitVersion, candidates, 0, deadline));
            result.whenComplete((ignored, failure) -> deadline.close());
            return result;
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    public CompletableFuture<Void> revalidate(CommittedObjectGenerationProof expected) {
        try {
            CommittedObjectGenerationProof proof = Objects.requireNonNull(expected, "expected");
            MaterializationDeadline deadline = new MaterializationDeadline(operationTimeout, scheduler);
            GenerationIndexRecord value = proof.index().value();
            GenerationIndexIdentity identity = new GenerationIndexIdentity(
                    proof.streamId(), ReadView.COMMITTED, value.offsetEnd(), value.generation());
            CompletableFuture<Optional<VersionedGenerationIndex>> index = deadline.bound(
                    () -> generations.getIndex(cluster, identity), "revalidate committed replacement index");
            CompletableFuture<Optional<VersionedPhysicalObjectRoot>> root = deadline.bound(
                    () -> physical.getRoot(cluster, ObjectKeyHash.from(proof.target().objectKey())),
                    "revalidate committed replacement root");
            CompletableFuture<Optional<VersionedMaterializationCheckpoint>> checkpoint = deadline.bound(
                    () -> generations.getMaterializationCheckpoint(
                            cluster,
                            proof.streamId(),
                            proof.checkpoint().value().policyId(),
                            proof.checkpoint().value().policyVersion()),
                    "revalidate committed replacement checkpoint");
            CompletableFuture<Optional<VersionedObjectProtection>> protection = findVisibleProtection(
                    proof.index(), proof.root(), Optional.empty(), new ArrayList<>(), 0, deadline);
            CompletableFuture<Void> result = CompletableFuture.allOf(index, root, checkpoint, protection)
                    .thenAccept(ignored -> {
                        if (!index.join().equals(Optional.of(proof.index()))
                                || !root.join().equals(Optional.of(proof.root()))
                                || !checkpoint.join().equals(Optional.of(proof.checkpoint()))
                                || !protection.join().equals(Optional.of(proof.visibleProtection()))) {
                            throw condition("committed Object replacement authority changed");
                        }
                    });
            result.whenComplete((ignored, failure) -> deadline.close());
            return result;
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    public CompletableFuture<Optional<CommittedObjectGenerationProof>> proveExact(
            StreamId streamId,
            OffsetRange sourceRange,
            long sourceCommitVersion,
            String indexKey,
            long indexMetadataVersion,
            Checksum indexSha256) {
        try {
            StreamId stream = Objects.requireNonNull(streamId, "streamId");
            OffsetRange range = Objects.requireNonNull(sourceRange, "sourceRange");
            String exactKey = text(indexKey, "indexKey");
            if (range.isEmpty() || sourceCommitVersion <= 0 || indexMetadataVersion < 0) {
                throw new IllegalArgumentException("exact committed Object replacement identity is invalid");
            }
            var decoded = keys.parseGenerationIndexKey(exactKey);
            if (!decoded.streamId().equals(stream)
                    || decoded.view() != ReadView.COMMITTED
                    || decoded.generation() <= 0
                    || decoded.offsetEnd() < range.endOffset()) {
                throw condition("committed Object replacement index key does not cover the source");
            }
            MaterializationDeadline deadline = new MaterializationDeadline(operationTimeout, scheduler);
            GenerationIndexIdentity identity = new GenerationIndexIdentity(
                    stream, decoded.view(), decoded.offsetEnd(), decoded.generation());
            CompletableFuture<Optional<CommittedObjectGenerationProof>> result = deadline.bound(
                            () -> generations.getIndex(cluster, identity),
                            "load exact committed Object replacement index")
                    .thenCompose(optional -> {
                        if (optional.isEmpty()) {
                            return CompletableFuture.completedFuture(Optional.empty());
                        }
                        VersionedGenerationIndex index = optional.orElseThrow();
                        if (!index.key().equals(exactKey)
                                || index.metadataVersion() != indexMetadataVersion
                                || !index.durableValueSha256().equals(indexSha256)
                                || !covers(index.value(), stream, range)) {
                            return CompletableFuture.completedFuture(Optional.empty());
                        }
                        return proveCandidate(
                                stream,
                                range,
                                sourceCommitVersion,
                                List.of(index),
                                0,
                                deadline);
                    });
            result.whenComplete((ignored, failure) -> deadline.close());
            return result;
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    @Override
    public CompletableFuture<Optional<CommittedGenerationRetirementProof>> proveRetirement(
            StreamId streamId,
            OffsetRange sourceRange,
            long sourceCommitVersion) {
        return prove(streamId, sourceRange, sourceCommitVersion)
                .thenApply(optional -> optional.map(CommittedGenerationRetirementProof::from));
    }

    @Override
    public CompletableFuture<Optional<CommittedGenerationRetirementProof>> proveExactRetirement(
            StreamId streamId,
            OffsetRange sourceRange,
            long sourceCommitVersion,
            String indexKey,
            long indexMetadataVersion,
            Checksum indexSha256) {
        return proveExact(
                        streamId,
                        sourceRange,
                        sourceCommitVersion,
                        indexKey,
                        indexMetadataVersion,
                        indexSha256)
                .thenApply(optional -> optional.map(CommittedGenerationRetirementProof::from));
    }

    @Override
    public CompletableFuture<Void> revalidateRetirement(
            CommittedGenerationRetirementProof expected) {
        CommittedGenerationRetirementProof proof = Objects.requireNonNull(expected, "expected");
        return proveExactRetirement(
                        proof.streamId(),
                        proof.sourceRange(),
                        proof.sourceCommitVersion(),
                        proof.indexKey(),
                        proof.indexMetadataVersion(),
                        proof.indexSha256())
                .thenAccept(optional -> {
                    if (!optional.equals(Optional.of(proof))) {
                        throw condition("committed Object retirement authority changed");
                    }
                });
    }

    private CompletableFuture<List<VersionedGenerationIndex>> scanIndexes(
            StreamId stream,
            OffsetRange range,
            Optional<F4ScanToken> continuation,
            List<VersionedGenerationIndex> values,
            int observed,
            MaterializationDeadline deadline) {
        return deadline.bound(
                        () -> generations.scanIndex(
                                cluster,
                                stream,
                                ReadView.COMMITTED,
                                range.endOffset(),
                                Long.MAX_VALUE,
                                continuation,
                                pageSize),
                        "scan committed Object replacement indexes")
                .thenCompose(page -> {
                    int nextObserved = Math.addExact(observed, page.values().size());
                    if (nextObserved > MAX_SCAN_ENTRIES) {
                        return CompletableFuture.failedFuture(invariant(
                                "committed Object replacement scan exceeded its hard bound"));
                    }
                    for (VersionedGenerationCandidate candidate : page.values()) {
                        if (candidate instanceof VersionedGenerationIndex higher
                                && covers(higher.value(), stream, range)) {
                            values.add(higher);
                        }
                    }
                    if (page.continuation().isPresent()) {
                        return scanIndexes(
                                stream, range, page.continuation(), values, nextObserved, deadline);
                    }
                    values.sort(Comparator
                            .comparingLong((VersionedGenerationIndex value) -> value.value().generation())
                            .reversed()
                            .thenComparingLong(value -> value.value().offsetEnd()));
                    return CompletableFuture.completedFuture(List.copyOf(values));
                });
    }

    private CompletableFuture<Optional<CommittedObjectGenerationProof>> proveCandidate(
            StreamId stream,
            OffsetRange range,
            long commitVersion,
            List<VersionedGenerationIndex> candidates,
            int index,
            MaterializationDeadline deadline) {
        if (index == candidates.size()) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        VersionedGenerationIndex candidate = candidates.get(index);
        GenerationIndexRecord value = candidate.value();
        if (value.firstCommitVersion() > commitVersion || value.lastCommitVersion() < commitVersion) {
            return proveCandidate(stream, range, commitVersion, candidates, index + 1, deadline);
        }
        Optional<ObjectSliceReadTarget> decoded = canonicalTarget(candidate, stream);
        if (decoded.isEmpty()) {
            return proveCandidate(stream, range, commitVersion, candidates, index + 1, deadline);
        }
        ObjectSliceReadTarget target = decoded.orElseThrow();
        return loadHealthyRoot(candidate, target, deadline).thenCompose(root -> {
            if (root.isEmpty()) {
                return proveCandidate(stream, range, commitVersion, candidates, index + 1, deadline);
            }
            VersionedPhysicalObjectRoot exactRoot = root.orElseThrow();
            return findVisibleProtection(
                            candidate, exactRoot, Optional.empty(), new ArrayList<>(), 0, deadline)
                    .thenCompose(protection -> {
                        if (protection.isEmpty()) {
                            return proveCandidate(
                                    stream, range, commitVersion, candidates, index + 1, deadline);
                        }
                        return findCheckpoint(
                                        stream,
                                        range,
                                        commitVersion,
                                        candidate,
                                        Optional.empty(),
                                        new ArrayList<>(),
                                        0,
                                        deadline)
                                .thenCompose(checkpoint -> checkpoint.isEmpty()
                                        ? proveCandidate(
                                                stream,
                                                range,
                                                commitVersion,
                                                candidates,
                                                index + 1,
                                                deadline)
                                        : CompletableFuture.completedFuture(Optional.of(
                                                new CommittedObjectGenerationProof(
                                                        stream,
                                                        range,
                                                        commitVersion,
                                                        candidate,
                                                        target,
                                                        exactRoot,
                                                        protection.orElseThrow(),
                                                        checkpoint.orElseThrow()))));
                    });
        });
    }

    private Optional<ObjectSliceReadTarget> canonicalTarget(
            VersionedGenerationIndex candidate,
            StreamId stream) {
        GenerationIndexRecord value = candidate.value();
        try {
            ReadTarget decoded = TARGET_CODECS.decode(value.readTarget());
            if (!(decoded instanceof ObjectSliceReadTarget target)) {
                return Optional.empty();
            }
            String expectedKey = keys.generationIndexKey(
                    stream, ReadView.COMMITTED, value.offsetEnd(), value.generation());
            Checksum expectedDigest = GenerationIndexDigests.durableValueSha256(
                    value.withMetadataVersion(0));
            String targetIdentity = TARGET_CODECS.encode(target).identityChecksumValue();
            if (!candidate.key().equals(expectedKey)
                    || !candidate.durableValueSha256().equals(expectedDigest)
                    || !value.targetIdentitySha256().equals(targetIdentity)) {
                throw invariant("committed Object replacement index is non-canonical");
            }
            return Optional.of(target);
        } catch (NereusException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw invariant("committed Object replacement target cannot be decoded");
        }
    }

    private CompletableFuture<Optional<VersionedPhysicalObjectRoot>> loadHealthyRoot(
            VersionedGenerationIndex index,
            ObjectSliceReadTarget target,
            MaterializationDeadline deadline) {
        ObjectKeyHash object = ObjectKeyHash.from(target.objectKey());
        return deadline.bound(
                        () -> physical.getRoot(cluster, object),
                        "load committed Object replacement root")
                .thenApply(optional -> optional.filter(root -> healthyRoot(index, target, root)));
    }

    private boolean healthyRoot(
            VersionedGenerationIndex index,
            ObjectSliceReadTarget target,
            VersionedPhysicalObjectRoot root) {
        PhysicalObjectIdentity identity;
        try {
            identity = PhysicalObjectIdentity.from(root.value());
        } catch (RuntimeException failure) {
            throw invariant("committed Object replacement root is malformed");
        }
        PhysicalObjectKind expectedKind = switch (target.objectType()) {
            case MULTI_STREAM_WAL_OBJECT -> PhysicalObjectKind.OBJECT_WAL;
            case STREAM_COMPACTED_OBJECT -> PhysicalObjectKind.COMMITTED_COMPACTED;
            default -> throw invariant("committed Object replacement type is unsupported");
        };
        long requiredEnd = Math.addExact(target.objectOffset(), target.objectLength());
        if (target.objectType() == ObjectType.STREAM_COMPACTED_OBJECT
                && !target.physicalFormat().equals(MaterializationPolicy.COMMITTED_FORMAT)) {
            return false;
        }
        return root.key().equals(keys.physicalRootKey(identity.objectKeyHash()))
                && root.value().lifecycle() == PhysicalObjectLifecycle.ACTIVE
                && identity.objectKey().equals(target.objectKey())
                && identity.objectId().filter(target.objectId()::equals).isPresent()
                && identity.kind() == expectedKind
                && requiredEnd <= identity.objectLength()
                && index.value().lifecycle() == GenerationLifecycle.COMMITTED;
    }

    private CompletableFuture<Optional<VersionedObjectProtection>> findVisibleProtection(
            VersionedGenerationIndex index,
            VersionedPhysicalObjectRoot root,
            Optional<F4ScanToken> continuation,
            List<VersionedObjectProtection> matches,
            int observed,
            MaterializationDeadline deadline) {
        ObjectKeyHash object = new ObjectKeyHash(root.value().objectKeyHash());
        return deadline.bound(
                        () -> physical.scanProtections(cluster, object, continuation, pageSize),
                        "scan committed Object replacement protections")
                .thenCompose(page -> {
                    int nextObserved = Math.addExact(observed, page.values().size());
                    if (nextObserved > MAX_SCAN_ENTRIES) {
                        return CompletableFuture.failedFuture(invariant(
                                "committed Object replacement protection scan exceeded its hard bound"));
                    }
                    for (VersionedObjectProtection protection : page.values()) {
                        var value = protection.value();
                        if (value.protectionTypeId() == ObjectProtectionType.VISIBLE_GENERATION.wireId()
                                && value.ownerKey().equals(index.key())
                                && value.ownerMetadataVersion() == index.metadataVersion()
                                && value.ownerIdentitySha256().equals(
                                        index.durableValueSha256().value())
                                && value.rootLifecycleEpoch() == root.value().lifecycleEpoch()) {
                            matches.add(protection);
                        }
                    }
                    if (matches.size() > 1) {
                        return CompletableFuture.failedFuture(invariant(
                                "committed Object replacement visible protection is ambiguous"));
                    }
                    return page.continuation().isPresent()
                            ? findVisibleProtection(
                                    index,
                                    root,
                                    page.continuation(),
                                    matches,
                                    nextObserved,
                                    deadline)
                            : CompletableFuture.completedFuture(matches.stream().findFirst());
                });
    }

    private CompletableFuture<Optional<VersionedMaterializationCheckpoint>> findCheckpoint(
            StreamId stream,
            OffsetRange range,
            long commitVersion,
            VersionedGenerationIndex index,
            Optional<F4ScanToken> continuation,
            List<VersionedMaterializationCheckpoint> matches,
            int observed,
            MaterializationDeadline deadline) {
        return deadline.bound(
                        () -> generations.scanMaterializationCheckpoints(
                                cluster, stream, continuation, pageSize),
                        "scan committed Object replacement checkpoints")
                .thenCompose(page -> {
                    int nextObserved = Math.addExact(observed, page.values().size());
                    if (nextObserved > MAX_SCAN_ENTRIES) {
                        return CompletableFuture.failedFuture(invariant(
                                "committed Object replacement checkpoint scan exceeded its hard bound"));
                    }
                    for (VersionedMaterializationCheckpoint checkpoint : page.values()) {
                        var value = checkpoint.value();
                        if (value.policySha256().equals(index.value().policySha256())
                                && value.contiguousCoveredOffset() >= range.endOffset()
                                && value.observedCommitVersion() >= commitVersion) {
                            matches.add(checkpoint);
                        }
                    }
                    if (page.continuation().isPresent()) {
                        return findCheckpoint(
                                stream,
                                range,
                                commitVersion,
                                index,
                                page.continuation(),
                                matches,
                                nextObserved,
                                deadline);
                    }
                    matches.sort(Comparator
                            .comparingLong((VersionedMaterializationCheckpoint value) ->
                                    value.value().policyVersion())
                            .reversed()
                            .thenComparing(VersionedMaterializationCheckpoint::key));
                    return CompletableFuture.completedFuture(matches.stream().findFirst());
                });
    }

    private static boolean covers(
            GenerationIndexRecord value,
            StreamId stream,
            OffsetRange range) {
        return value.streamId().equals(stream.value())
                && value.readViewId() == ReadView.COMMITTED.wireId()
                && value.lifecycle() == GenerationLifecycle.COMMITTED
                && value.offsetStart() <= range.startOffset()
                && value.offsetEnd() >= range.endOffset();
    }

    private static Duration positive(Duration value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(field + " must be positive");
        }
        return value;
    }

    private static String text(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static NereusException condition(String message) {
        return new NereusException(ErrorCode.METADATA_CONDITION_FAILED, true, message);
    }
}
