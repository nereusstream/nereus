/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.physical.GcAuthorityToken;
import com.nereusstream.core.physical.GcGlobalReferenceScope;
import com.nereusstream.core.physical.GcGlobalReferenceScopeSnapshot;
import com.nereusstream.core.physical.GcReferenceDomainConfig;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationProtocolActivationStore;
import com.nereusstream.metadata.oxia.StreamRegistrationScanPage;
import com.nereusstream.metadata.oxia.VersionedGenerationProtocolActivation;
import com.nereusstream.metadata.oxia.VersionedMaterializationStreamRegistration;
import com.nereusstream.metadata.oxia.records.ReferenceDomainVersionRecord;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Activation/backfill-gated full 64-shard registration scope for ownerless reference domains.
 *
 * <p>Registration records remain work hints unless the exact activation record proves registration,
 * physical-root and cursor backfill coverage for the installed domain set.
 */
public final class RegisteredStreamGcGlobalReferenceScope
        implements GcGlobalReferenceScope {
    private static final Comparator<GcAuthorityToken> AUTHORITY_ORDER = Comparator
            .comparing(GcAuthorityToken::authorityKey)
            .thenComparingLong(GcAuthorityToken::metadataVersion)
            .thenComparing(value -> value.identitySha256().value());

    private final String cluster;
    private final GenerationProtocolActivationStore activationStore;
    private final GenerationMetadataStore generationStore;
    private final List<ReferenceDomainVersionRecord> installedDomains;
    private final int pageSize;
    private final int maxAuthorities;
    private final F4Keyspace keys;

    public RegisteredStreamGcGlobalReferenceScope(
            String cluster,
            GenerationProtocolActivationStore activationStore,
            GenerationMetadataStore generationStore,
            List<GcReferenceDomainVersion> installedDomains,
            GcReferenceDomainConfig config) {
        this.cluster = requireText(cluster, "cluster");
        this.activationStore = Objects.requireNonNull(
                activationStore, "activationStore");
        this.generationStore = Objects.requireNonNull(
                generationStore, "generationStore");
        this.installedDomains = GenerationProtocolDomainSets.canonicalInstalled(
                installedDomains);
        GcReferenceDomainConfig exactConfig = Objects.requireNonNull(config, "config");
        this.pageSize = exactConfig.metadataScanPageSize();
        this.maxAuthorities = exactConfig.maxAuthoritiesPerSnapshot();
        this.keys = new F4Keyspace(cluster);
    }

    @Override
    public CompletableFuture<GcGlobalReferenceScopeSnapshot> snapshot() {
        return activationStore.get(cluster).thenCompose(optional -> {
            Accumulator accumulator = new Accumulator(maxAuthorities);
            if (optional.isEmpty()) {
                String key = keys.generationProtocolActivationKey();
                accumulator.addActivation(new GcAuthorityToken(
                        key,
                        0,
                        GlobalReferenceScopeIdentityDigests.activationAbsence(key)));
                accumulator.incomplete = true;
                return CompletableFuture.completedFuture(accumulator.snapshot());
            }
            VersionedGenerationProtocolActivation activation = optional.orElseThrow();
            accumulator.addActivation(authority(activation));
            if (!GenerationProtocolDomainSets.deletionReady(
                    activation.value(), installedDomains)) {
                accumulator.incomplete = true;
                return CompletableFuture.completedFuture(accumulator.snapshot());
            }
            return scanShard(activation, accumulator, 0);
        });
    }

    private CompletableFuture<GcGlobalReferenceScopeSnapshot> scanShard(
            VersionedGenerationProtocolActivation activation,
            Accumulator accumulator,
            int shard) {
        if (accumulator.incomplete
                || shard == F4Keyspace.MATERIALIZATION_REGISTRY_SHARDS) {
            return finish(activation, accumulator);
        }
        return scanPage(
                        accumulator,
                        shard,
                        Optional.empty(),
                        null)
                .thenCompose(ignored -> scanShard(activation, accumulator, shard + 1));
    }

    private CompletableFuture<Void> scanPage(
            Accumulator accumulator,
            int shard,
            Optional<F4ScanToken> continuation,
            String previousKey) {
        if (accumulator.incomplete) {
            return CompletableFuture.completedFuture(null);
        }
        return generationStore.scanStreamRegistrations(
                        cluster, shard, continuation, pageSize)
                .thenCompose(page -> {
                    requireProgress(page, previousKey);
                    for (VersionedMaterializationStreamRegistration registration :
                            page.values()) {
                        if (!accumulator.addRegistration(registration)) {
                            return CompletableFuture.completedFuture(null);
                        }
                    }
                    if (page.continuation().isEmpty()) {
                        return CompletableFuture.completedFuture(null);
                    }
                    String lastKey = page.values().get(page.values().size() - 1).key();
                    return scanPage(
                            accumulator,
                            shard,
                            page.continuation(),
                            lastKey);
                });
    }

    private CompletableFuture<GcGlobalReferenceScopeSnapshot> finish(
            VersionedGenerationProtocolActivation expected,
            Accumulator accumulator) {
        if (accumulator.incomplete) {
            return CompletableFuture.completedFuture(accumulator.snapshot());
        }
        return activationStore.get(cluster).thenApply(current -> {
            if (current.isEmpty() || !current.orElseThrow().equals(expected)) {
                accumulator.incomplete = true;
            }
            return accumulator.snapshot();
        });
    }

    private static GcAuthorityToken authority(
            VersionedGenerationProtocolActivation activation) {
        return new GcAuthorityToken(
                activation.key(),
                activation.metadataVersion(),
                activation.durableValueSha256());
    }

    private static void requireProgress(
            StreamRegistrationScanPage page, String previousKey) {
        if (previousKey != null
                && !page.values().isEmpty()
                && page.values().get(0).key().compareTo(previousKey) <= 0) {
            throw invariant("global registration scan did not advance");
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

    private static final class Accumulator {
        private final int maxAuthorities;
        private final ArrayList<StreamId> streams = new ArrayList<>();
        private final ArrayList<GcAuthorityToken> authorities = new ArrayList<>();
        private final HashSet<String> streamIds = new HashSet<>();
        private long streamCount;
        private long authorityCount;
        private boolean incomplete;

        private Accumulator(int maxAuthorities) {
            this.maxAuthorities = maxAuthorities;
        }

        private void addActivation(GcAuthorityToken activation) {
            authorityCount = Math.addExact(authorityCount, 1);
            if (authorityCount <= maxAuthorities) {
                authorities.add(activation);
            } else {
                incomplete = true;
            }
        }

        private boolean addRegistration(
                VersionedMaterializationStreamRegistration registration) {
            StreamId streamId = new StreamId(registration.value().streamId());
            if (!streamIds.add(streamId.value())) {
                throw invariant("global registration scan returned a duplicate stream");
            }
            streamCount = Math.addExact(streamCount, 1);
            authorityCount = Math.addExact(authorityCount, 1);
            if (authorityCount > maxAuthorities) {
                incomplete = true;
                return false;
            }
            streams.add(streamId);
            authorities.add(new GcAuthorityToken(
                    registration.key(),
                    registration.metadataVersion(),
                    registration.durableValueSha256()));
            return true;
        }

        private GcGlobalReferenceScopeSnapshot snapshot() {
            streams.sort(Comparator.comparing(StreamId::value));
            authorities.sort(AUTHORITY_ORDER);
            return new GcGlobalReferenceScopeSnapshot(
                    !incomplete,
                    streamCount,
                    authorityCount,
                    List.copyOf(streams),
                    List.copyOf(authorities));
        }
    }
}
