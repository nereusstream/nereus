/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.core.physical.GcAuthorityToken;
import com.nereusstream.core.physical.GcReferenceDomain;
import com.nereusstream.core.physical.GcReferenceDomainConfig;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.GcReferenceSnapshot;
import com.nereusstream.core.physical.GcReferenceSnapshotBuilder;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.GenerationProtocolActivationStore;
import com.nereusstream.metadata.oxia.VersionedGenerationProtocolActivation;
import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationLifecycle;
import com.nereusstream.metadata.oxia.records.ReferenceDomainVersionRecord;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Cluster capability/domain-set sentinel that fails deletion closed for unknown future owners. */
public final class FutureCatalogSentinelDomain implements GcReferenceDomain {
    public static final String DOMAIN_ID = "future-catalog-sentinel-v1";
    public static final int PROTOCOL_VERSION = 1;

    private final String cluster;
    private final GenerationProtocolActivationStore activationStore;
    private final GcReferenceDomainConfig config;
    private final List<ReferenceDomainVersionRecord> installedDomains;
    private final F4Keyspace keys;

    public FutureCatalogSentinelDomain(
            String cluster,
            GenerationProtocolActivationStore activationStore,
            GcReferenceDomainConfig config,
            List<GcReferenceDomainVersion> installedDomains) {
        this.cluster = requireText(cluster, "cluster");
        this.activationStore = Objects.requireNonNull(
                activationStore, "activationStore");
        this.config = Objects.requireNonNull(config, "config");
        this.installedDomains = GenerationProtocolDomainSets.canonicalInstalled(
                installedDomains);
        this.keys = new F4Keyspace(cluster);
    }

    @Override
    public String domainId() {
        return DOMAIN_ID;
    }

    @Override
    public int protocolVersion() {
        return PROTOCOL_VERSION;
    }

    @Override
    public CompletableFuture<GcReferenceSnapshot> snapshot(GcReferenceQuery query) {
        Objects.requireNonNull(query, "query");
        GcReferenceSnapshotBuilder builder = new GcReferenceSnapshotBuilder(
                DOMAIN_ID, PROTOCOL_VERSION, query, config);
        return activationStore.get(cluster).thenApply(optional -> {
            if (optional.isEmpty()) {
                String key = keys.generationProtocolActivationKey();
                builder.addAuthority(new GcAuthorityToken(
                        key,
                        0,
                        GlobalReferenceScopeIdentityDigests.activationAbsence(key)));
                builder.markIncomplete();
                return builder.build();
            }
            VersionedGenerationProtocolActivation activation = optional.orElseThrow();
            builder.addAuthority(new GcAuthorityToken(
                    activation.key(),
                    activation.metadataVersion(),
                    activation.durableValueSha256()));
            if (activation.value().lifecycle()
                    != GenerationProtocolActivationLifecycle.ACTIVE) {
                builder.markIncomplete();
            } else if (!GenerationProtocolDomainSets.exactMatch(
                    activation.value(), installedDomains)) {
                builder.veto();
            } else if (!activation.value().physicalDeleteEnabled()
                    || !activation.value().cursorSnapshotDeleteEnabled()) {
                builder.veto();
            } else if (!GenerationProtocolDomainSets.deletionReady(
                    activation.value(), installedDomains)) {
                builder.markIncomplete();
            }
            return builder.build();
        });
    }

    @Override
    public CompletableFuture<Boolean> stillMatches(
            GcReferenceQuery query, GcReferenceSnapshot snapshot) {
        Objects.requireNonNull(query, "query");
        Objects.requireNonNull(snapshot, "snapshot");
        if (!snapshot.domainId().equals(DOMAIN_ID)
                || snapshot.protocolVersion() != PROTOCOL_VERSION
                || !snapshot.queryIdentitySha256().equals(query.queryIdentitySha256())) {
            return CompletableFuture.completedFuture(false);
        }
        return snapshot(query).thenApply(snapshot::equals);
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must be non-blank");
        }
        return value;
    }
}
