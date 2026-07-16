/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.GenerationIndexDigests;
import com.nereusstream.metadata.oxia.GenerationIndexIdentity;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationScanPage;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Proves that a healthier current TOPIC_COMPACTED generation supersedes one source range. */
final class TopicCompactedReplacementVerifier {
    private final String cluster;
    private final GenerationMetadataStore generations;
    private final RecoveryReplacementVerifier targets;
    private final PhysicalGcConfig config;
    private final F4Keyspace keys;

    TopicCompactedReplacementVerifier(
            String cluster,
            GenerationMetadataStore generations,
            RecoveryReplacementVerifier targets,
            PhysicalGcConfig config) {
        this.cluster = requireText(cluster, "cluster");
        this.generations = Objects.requireNonNull(generations, "generations");
        this.targets = Objects.requireNonNull(targets, "targets");
        this.config = Objects.requireNonNull(config, "config");
        this.keys = new F4Keyspace(cluster);
    }

    CompletableFuture<ReplacementProof> prove(
            GcReferenceQuery query,
            VersionedGenerationIndex source) {
        Objects.requireNonNull(query, "query");
        Objects.requireNonNull(source, "source");
        requireSource(source);
        return scan(
                        source,
                        Optional.empty(),
                        null,
                        0,
                        new ArrayList<>())
                .thenCompose(candidates -> {
                    List<VersionedGenerationIndex> preferred = candidates.stream()
                            .sorted(Comparator
                                    .comparingLong((VersionedGenerationIndex value) ->
                                            value.value().generation())
                                    .reversed()
                                    .thenComparingLong(value ->
                                            Math.subtractExact(
                                                    value.value().offsetEnd(),
                                                    value.value().offsetStart()))
                                    .thenComparing(VersionedGenerationIndex::key))
                            .toList();
                    return select(query, source, preferred, 0);
                });
    }

    private CompletableFuture<List<VersionedGenerationIndex>> scan(
            VersionedGenerationIndex source,
            Optional<F4ScanToken> continuation,
            String previousKey,
            int observed,
            List<VersionedGenerationIndex> candidates) {
        int remaining = config.maxAuthoritiesPerDomainSnapshot() - observed;
        int limit = remaining == 0
                ? 1
                : Math.min(config.metadataScanPageSize(), remaining);
        StreamId stream = new StreamId(source.value().streamId());
        return generations.scanIndex(
                        cluster,
                        stream,
                        ReadView.TOPIC_COMPACTED,
                        source.value().offsetEnd(),
                        Long.MAX_VALUE,
                        continuation,
                        limit)
                .thenCompose(page -> {
                    requireProgress(page, previousKey);
                    int nextObserved = Math.addExact(observed, page.values().size());
                    if (nextObserved > config.maxAuthoritiesPerDomainSnapshot()) {
                        return CompletableFuture.failedFuture(invariant(
                                "TOPIC_COMPACTED replacement scan exceeded its authority bound"));
                    }
                    for (VersionedGenerationCandidate candidate : page.values()) {
                        if (!(candidate instanceof VersionedGenerationIndex higher)) {
                            return CompletableFuture.failedFuture(invariant(
                                    "TOPIC_COMPACTED namespace returned a generation-zero index"));
                        }
                        requireScannedIdentity(stream, higher);
                        if (covers(source.value(), higher.value())) {
                            candidates.add(higher);
                        }
                    }
                    if (page.continuation().isEmpty()) {
                        return CompletableFuture.completedFuture(List.copyOf(candidates));
                    }
                    if (nextObserved >= config.maxAuthoritiesPerDomainSnapshot()) {
                        return CompletableFuture.failedFuture(invariant(
                                "TOPIC_COMPACTED replacement scan exceeded its authority bound"));
                    }
                    return scan(
                            source,
                            page.continuation(),
                            page.values().get(page.values().size() - 1).key(),
                            nextObserved,
                            candidates);
                });
    }

    private CompletableFuture<ReplacementProof> select(
            GcReferenceQuery query,
            VersionedGenerationIndex source,
            List<VersionedGenerationIndex> candidates,
            int index) {
        if (index == candidates.size()) {
            return CompletableFuture.failedFuture(condition(
                    "TOPIC_COMPACTED source has no current healthy same-view replacement"));
        }
        VersionedGenerationIndex candidate = candidates.get(index);
        return targets.loadCurrentHealthy(query, candidate).thenCompose(optional -> {
            if (optional.isEmpty()) {
                return select(query, source, candidates, index + 1);
            }
            RecoveryReplacementVerifier.HealthyReplacement replacement =
                    optional.orElseThrow();
            return targets.revalidateSameView(replacement)
                    .thenCompose(ignored -> reloadSource(source))
                    .thenApply(ignored -> new ReplacementProof(source, replacement));
        });
    }

    private CompletableFuture<Void> reloadSource(
            VersionedGenerationIndex source) {
        GenerationIndexRecord value = source.value();
        GenerationIndexIdentity identity = new GenerationIndexIdentity(
                new StreamId(value.streamId()),
                ReadView.TOPIC_COMPACTED,
                value.offsetEnd(),
                value.generation());
        return generations.getIndex(cluster, identity).thenAccept(reloaded -> {
            if (!reloaded.equals(Optional.of(source))) {
                throw condition(
                        "TOPIC_COMPACTED source changed while replacement facts were frozen");
            }
        });
    }

    private void requireSource(VersionedGenerationIndex source) {
        GenerationIndexRecord value = source.value();
        StreamId stream = new StreamId(value.streamId());
        String expectedKey = keys.generationIndexKey(
                stream,
                ReadView.TOPIC_COMPACTED,
                value.offsetEnd(),
                value.generation());
        if (value.readViewId() != ReadView.TOPIC_COMPACTED.wireId()
                || !source.key().equals(expectedKey)
                || !source.durableValueSha256().equals(
                        GenerationIndexDigests.durableValueSha256(
                                value.withMetadataVersion(0)))
                || (value.lifecycle() != GenerationLifecycle.COMMITTED
                        && value.lifecycle() != GenerationLifecycle.QUARANTINED
                        && value.lifecycle() != GenerationLifecycle.DRAINING)) {
            throw invariant("TOPIC_COMPACTED retirement source is non-canonical");
        }
    }

    private void requireScannedIdentity(
            StreamId stream,
            VersionedGenerationIndex candidate) {
        GenerationIndexRecord value = candidate.value();
        String expectedKey = keys.generationIndexKey(
                stream,
                ReadView.TOPIC_COMPACTED,
                value.offsetEnd(),
                value.generation());
        if (!value.streamId().equals(stream.value())
                || value.readViewId() != ReadView.TOPIC_COMPACTED.wireId()
                || !candidate.key().equals(expectedKey)
                || !candidate.durableValueSha256().equals(
                        GenerationIndexDigests.durableValueSha256(
                                value.withMetadataVersion(0)))) {
            throw invariant("TOPIC_COMPACTED replacement scan returned a non-canonical index");
        }
    }

    private static boolean covers(
            GenerationIndexRecord source,
            GenerationIndexRecord candidate) {
        return candidate.lifecycle() == GenerationLifecycle.COMMITTED
                && candidate.generation() > source.generation()
                && candidate.offsetStart() <= source.offsetStart()
                && candidate.offsetEnd() >= source.offsetEnd()
                && candidate.firstCommitVersion() <= source.firstCommitVersion()
                && candidate.lastCommitVersion() >= source.lastCommitVersion()
                && candidate.cumulativeSizeAtStart()
                        <= source.cumulativeSizeAtStart()
                && candidate.cumulativeSizeAtEnd()
                        >= source.cumulativeSizeAtEnd()
                && candidate.payloadFormat().equals(source.payloadFormat())
                && candidate.projectionRef().equals(source.projectionRef());
    }

    private static void requireProgress(
            GenerationScanPage page,
            String previousKey) {
        if (page.continuation().isPresent() && page.values().isEmpty()) {
            throw invariant(
                    "TOPIC_COMPACTED replacement scan returned an empty continuation page");
        }
        if (previousKey != null
                && !page.values().isEmpty()
                && page.values().get(0).key().compareTo(previousKey) <= 0) {
            throw invariant("TOPIC_COMPACTED replacement scan did not advance");
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

    record ReplacementProof(
            VersionedGenerationIndex source,
            RecoveryReplacementVerifier.HealthyReplacement replacement) {
        ReplacementProof {
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(replacement, "replacement");
        }
    }
}
