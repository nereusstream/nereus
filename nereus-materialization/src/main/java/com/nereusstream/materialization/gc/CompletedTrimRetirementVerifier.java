/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedGenerationZeroIndex;
import com.nereusstream.metadata.oxia.VersionedRecoveryCheckpointRoot;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Exact completed-L0-trim proof for source retirement in either read view. */
final class CompletedTrimRetirementVerifier {
    private final String cluster;
    private final OxiaMetadataStore l0;
    private final GenerationMetadataStore generations;
    private final F4Keyspace keys;

    CompletedTrimRetirementVerifier(
            String cluster,
            OxiaMetadataStore l0,
            GenerationMetadataStore generations) {
        this.cluster = requireText(cluster, "cluster");
        this.l0 = Objects.requireNonNull(l0, "l0");
        this.generations = Objects.requireNonNull(generations, "generations");
        this.keys = new F4Keyspace(cluster);
    }

    CompletableFuture<Optional<CompletedTrimProof>> proveIfCompleted(
            VersionedGenerationCandidate source) {
        SourceIdentity identity = identify(Objects.requireNonNull(source, "source"));
        return l0.getStreamSnapshot(cluster, identity.stream()).thenCompose(snapshot -> {
            requireSnapshotIdentity(identity, snapshot);
            if (snapshot.trim().trimOffset() < identity.offsetEnd()) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            return generations.getRecoveryRoot(cluster, identity.stream())
                    .thenCompose(root -> {
                        requireRootIdentity(identity.stream(), root);
                        CompletedTrimProof proof = new CompletedTrimProof(
                                source, identity, snapshot, root);
                        return revalidate(proof).thenApply(ignored -> Optional.of(proof));
                    });
        });
    }

    CompletableFuture<Void> revalidate(CompletedTrimProof expected) {
        Objects.requireNonNull(expected, "expected");
        SourceIdentity identity = expected.identity();
        return generations.getCandidateByKey(
                        cluster,
                        identity.stream(),
                        identity.view(),
                        expected.source().key())
                .thenCompose(source -> {
                    if (!source.equals(Optional.of(expected.source()))) {
                        return CompletableFuture.failedFuture(condition(
                                "below-trim source changed while retirement facts were frozen"));
                    }
                    return l0.getStreamSnapshot(cluster, identity.stream());
                })
                .thenCompose(snapshot -> {
                    if (!snapshot.sameVersionedAuthority(
                            expected.snapshot())) {
                        return CompletableFuture.failedFuture(condition(
                                "completed trim changed while retirement facts were frozen"));
                    }
                    requireSnapshotIdentity(identity, snapshot);
                    if (snapshot.trim().trimOffset() < identity.offsetEnd()) {
                        return CompletableFuture.failedFuture(condition(
                                "source range is no longer below completed trim"));
                    }
                    return generations.getRecoveryRoot(cluster, identity.stream());
                })
                .thenAccept(root -> {
                    requireRootIdentity(identity.stream(), root);
                    if (!root.equals(expected.recoveryRoot())) {
                        throw condition(
                                "recovery root changed while below-trim facts were frozen");
                    }
                });
    }

    private SourceIdentity identify(VersionedGenerationCandidate source) {
        if (source instanceof VersionedGenerationZeroIndex zero) {
            StreamId stream = zero.value().streamId();
            String expectedKey = keys.generationIndexKey(
                    stream,
                    ReadView.COMMITTED,
                    zero.value().offsetEnd(),
                    0);
            if (!source.key().equals(expectedKey)) {
                throw invariant("generation-zero below-trim source key is non-canonical");
            }
            return new SourceIdentity(
                    stream,
                    ReadView.COMMITTED,
                    zero.value().offsetStart(),
                    zero.value().offsetEnd(),
                    0);
        }
        if (source instanceof VersionedGenerationIndex higher) {
            ReadView view;
            try {
                view = ReadView.fromWireId(higher.value().readViewId());
            } catch (RuntimeException failure) {
                throw invariant("higher-generation below-trim source view is invalid");
            }
            StreamId stream = new StreamId(higher.value().streamId());
            String expectedKey = keys.generationIndexKey(
                    stream,
                    view,
                    higher.value().offsetEnd(),
                    higher.value().generation());
            if (!source.key().equals(expectedKey)) {
                throw invariant("higher-generation below-trim source key is non-canonical");
            }
            return new SourceIdentity(
                    stream,
                    view,
                    higher.value().offsetStart(),
                    higher.value().offsetEnd(),
                    higher.value().generation());
        }
        throw invariant("unknown generation source type for completed-trim proof");
    }

    private static void requireSnapshotIdentity(
            SourceIdentity identity,
            StreamMetadataSnapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        String stream = identity.stream().value();
        if (!snapshot.metadata().streamId().equals(stream)
                || !snapshot.committedEnd().streamId().equals(stream)
                || !snapshot.trim().streamId().equals(stream)
                || snapshot.trim().trimOffset()
                        > snapshot.committedEnd().committedEndOffset()) {
            throw invariant("completed-trim snapshot belongs to another stream or is contradictory");
        }
    }

    private void requireRootIdentity(
            StreamId stream,
            Optional<VersionedRecoveryCheckpointRoot> root) {
        if (root.isPresent()) {
            VersionedRecoveryCheckpointRoot exact = root.orElseThrow();
            if (!exact.key().equals(keys.recoveryRootKey(stream))
                    || !exact.value().streamId().equals(stream.value())) {
                throw invariant("below-trim recovery root identity is non-canonical");
            }
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

    record CompletedTrimProof(
            VersionedGenerationCandidate source,
            SourceIdentity identity,
            StreamMetadataSnapshot snapshot,
            Optional<VersionedRecoveryCheckpointRoot> recoveryRoot) {
        CompletedTrimProof {
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(identity, "identity");
            Objects.requireNonNull(snapshot, "snapshot");
            recoveryRoot = Objects.requireNonNull(recoveryRoot, "recoveryRoot");
            if (snapshot.trim().trimOffset() < identity.offsetEnd()) {
                throw new IllegalArgumentException(
                        "completed-trim proof does not cover its source");
            }
        }
    }

    record SourceIdentity(
            StreamId stream,
            ReadView view,
            long offsetStart,
            long offsetEnd,
            long generation) {
        SourceIdentity {
            Objects.requireNonNull(stream, "stream");
            Objects.requireNonNull(view, "view");
            if (offsetStart < 0
                    || offsetEnd <= offsetStart
                    || generation < 0
                    || (generation == 0 && view != ReadView.COMMITTED)) {
                throw new IllegalArgumentException(
                        "completed-trim source identity is invalid");
            }
        }
    }
}
