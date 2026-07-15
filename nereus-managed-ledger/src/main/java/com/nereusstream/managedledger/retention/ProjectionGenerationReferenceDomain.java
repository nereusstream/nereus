/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.retention;

import com.nereusstream.api.StreamId;
import com.nereusstream.core.physical.GcAuthorityToken;
import com.nereusstream.core.physical.GcReferenceDomain;
import com.nereusstream.core.physical.GcReferenceDomainConfig;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.GcReferenceQueryKind;
import com.nereusstream.core.physical.GcReferenceSnapshot;
import com.nereusstream.core.physical.GcReferenceSnapshotBuilder;
import com.nereusstream.metadata.oxia.ManagedLedgerFacadeState;
import com.nereusstream.metadata.oxia.ManagedLedgerGenerationProtocol;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionKeyspace;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionMetadataStore;
import com.nereusstream.metadata.oxia.ManagedLedgerStreamProjection;
import com.nereusstream.metadata.oxia.VersionedTopicProjection;
import com.nereusstream.metadata.oxia.VersionedVirtualLedgerProjection;
import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** F2 compatibility authority for every affected historical stream incarnation. */
public final class ProjectionGenerationReferenceDomain implements GcReferenceDomain {
    public static final String DOMAIN_ID = "projection-generation-v1";
    public static final int PROTOCOL_VERSION = 1;

    private final String cluster;
    private final ManagedLedgerProjectionMetadataStore metadataStore;
    private final GcReferenceDomainConfig config;
    private final ManagedLedgerProjectionKeyspace keys;

    public ProjectionGenerationReferenceDomain(
            String cluster,
            ManagedLedgerProjectionMetadataStore metadataStore,
            GcReferenceDomainConfig config) {
        this.cluster = requireText(cluster, "cluster");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.config = Objects.requireNonNull(config, "config");
        this.keys = new ManagedLedgerProjectionKeyspace(cluster);
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
        if (query.kind() == GcReferenceQueryKind.OWNERLESS_ORPHAN_CANDIDATE) {
            return CompletableFuture.completedFuture(
                    GcReferenceSnapshotBuilder.unsupportedOwnerless(
                            DOMAIN_ID, PROTOCOL_VERSION, query));
        }
        GcReferenceSnapshotBuilder builder = new GcReferenceSnapshotBuilder(
                DOMAIN_ID, PROTOCOL_VERSION, query, config);
        return scan(query, builder, 0);
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

    private CompletableFuture<GcReferenceSnapshot> scan(
            GcReferenceQuery query,
            GcReferenceSnapshotBuilder builder,
            int streamIndex) {
        if (builder.limitExceeded()
                || streamIndex == query.affectedStreams().size()) {
            return CompletableFuture.completedFuture(builder.build());
        }
        StreamId streamId = query.affectedStreams().get(streamIndex);
        return metadataStore.getProjectionByStream(cluster, streamId).thenCompose(view -> {
            addProjectionAuthorities(builder, streamId, view);
            if (builder.limitExceeded()) {
                return CompletableFuture.completedFuture(builder.build());
            }
            return scan(query, builder, streamIndex + 1);
        });
    }

    private void addProjectionAuthorities(
            GcReferenceSnapshotBuilder builder,
            StreamId streamId,
            ManagedLedgerStreamProjection view) {
        if (!view.streamId().equals(streamId)) {
            throw new IllegalArgumentException("projection lookup returned another stream");
        }
        String bindingKey = keys.virtualLedgerProjectionKey(streamId);
        if (view.streamBinding().isEmpty()) {
            builder.addAuthority(new GcAuthorityToken(
                    bindingKey,
                    0,
                    ReferenceDomainIdentityDigests.absence(DOMAIN_ID, bindingKey)));
            builder.veto();
            return;
        }
        VersionedVirtualLedgerProjection binding = view.streamBinding().orElseThrow();
        builder.addAuthority(new GcAuthorityToken(
                binding.key(),
                binding.metadataVersion(),
                binding.durableValueSha256()));

        String topicKey = keys.topicProjectionKey(binding.value().managedLedgerName());
        if (view.currentTopic().isEmpty()) {
            builder.addAuthority(new GcAuthorityToken(
                    topicKey,
                    0,
                    ReferenceDomainIdentityDigests.absence(DOMAIN_ID, topicKey)));
            builder.veto();
            return;
        }
        VersionedTopicProjection topic = view.currentTopic().orElseThrow();
        builder.addAuthority(new GcAuthorityToken(
                topic.key(), topic.metadataVersion(), topic.durableValueSha256()));
        if (!allowsGenerationAwareDeletion(binding.value().identity(), topic)) {
            builder.veto();
        }
    }

    private static boolean allowsGenerationAwareDeletion(
            ManagedLedgerProjectionIdentity historical,
            VersionedTopicProjection current) {
        ManagedLedgerProjectionIdentity currentIdentity = current.value().projectionIdentity();
        if (historical.equals(currentIdentity)) {
            ManagedLedgerFacadeState state = current.value().parsedFacadeState();
            if (state == ManagedLedgerFacadeState.DELETED) {
                return true;
            }
            if (state == ManagedLedgerFacadeState.DELETING) {
                return false;
            }
            return ManagedLedgerGenerationProtocol.isActivated(current.value());
        }
        return currentIdentity.incarnation() > historical.incarnation()
                && currentIdentity.storageClassBindingGeneration()
                        > historical.storageClassBindingGeneration();
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must be non-blank");
        }
        return value;
    }
}
