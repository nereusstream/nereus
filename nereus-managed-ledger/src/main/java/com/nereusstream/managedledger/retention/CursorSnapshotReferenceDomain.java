/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.retention;

import com.nereusstream.api.StreamId;
import com.nereusstream.core.physical.GcAuthorityToken;
import com.nereusstream.core.physical.GcReference;
import com.nereusstream.core.physical.GcReferenceDomain;
import com.nereusstream.core.physical.GcReferenceDomainConfig;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.GcReferenceQueryKind;
import com.nereusstream.core.physical.GcReferenceSnapshot;
import com.nereusstream.core.physical.GcReferenceSnapshotBuilder;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.metadata.oxia.CursorKeyspace;
import com.nereusstream.metadata.oxia.CursorMetadataDigests;
import com.nereusstream.metadata.oxia.CursorMetadataStore;
import com.nereusstream.metadata.oxia.CursorScanPage;
import com.nereusstream.metadata.oxia.CursorScanToken;
import com.nereusstream.metadata.oxia.VersionedCursorRetention;
import com.nereusstream.metadata.oxia.VersionedCursorState;
import com.nereusstream.metadata.oxia.records.CursorRecordLifecycle;
import com.nereusstream.metadata.oxia.records.CursorRetentionLifecycle;
import com.nereusstream.metadata.oxia.records.CursorSnapshotReferenceRecord;
import com.nereusstream.metadata.oxia.records.CursorStateRecord;
import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Complete F3 retention/root authority for affected streams and cursor snapshot objects. */
public final class CursorSnapshotReferenceDomain implements GcReferenceDomain {
    public static final String DOMAIN_ID = "cursor-snapshot-v1";
    public static final int PROTOCOL_VERSION = 1;

    private final String cluster;
    private final CursorMetadataStore metadataStore;
    private final GcReferenceDomainConfig config;
    private final CursorKeyspace keys;

    public CursorSnapshotReferenceDomain(
            String cluster,
            CursorMetadataStore metadataStore,
            GcReferenceDomainConfig config) {
        this.cluster = requireText(cluster, "cluster");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.config = Objects.requireNonNull(config, "config");
        this.keys = new CursorKeyspace(cluster);
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
        if (query.kind() == GcReferenceQueryKind.CURSOR_SNAPSHOT_CANDIDATE
                && query.object().kind() != PhysicalObjectKind.CURSOR_SNAPSHOT) {
            builder.veto();
        }
        return scanStream(query, builder, 0);
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

    private CompletableFuture<GcReferenceSnapshot> scanStream(
            GcReferenceQuery query,
            GcReferenceSnapshotBuilder builder,
            int streamIndex) {
        if (builder.limitExceeded()
                || streamIndex == query.affectedStreams().size()) {
            return CompletableFuture.completedFuture(builder.build());
        }
        StreamId streamId = query.affectedStreams().get(streamIndex);
        return metadataStore.getRetention(cluster, streamId).thenCompose(retention -> {
            addRetention(builder, streamId, retention);
            if (builder.limitExceeded()) {
                return CompletableFuture.completedFuture(builder.build());
            }
            return scanCursors(
                    query,
                    builder,
                    streamIndex,
                    retention.map(value -> value.value().projection()),
                    false,
                    Optional.empty(),
                    null);
        });
    }

    private CompletableFuture<GcReferenceSnapshot> scanCursors(
            GcReferenceQuery query,
            GcReferenceSnapshotBuilder builder,
            int streamIndex,
            Optional<ManagedLedgerProjectionIdentity> retentionProjection,
            boolean cursorSeen,
            Optional<CursorScanToken> continuation,
            String previousKey) {
        StreamId streamId = query.affectedStreams().get(streamIndex);
        return metadataStore.scanCursors(
                        cluster,
                        streamId,
                        continuation,
                        config.metadataScanPageSize())
                .thenCompose(page -> {
                    requireProgress(streamId, page, previousKey);
                    boolean sawCursor = cursorSeen || !page.records().isEmpty();
                    for (VersionedCursorState cursor : page.records()) {
                        addCursor(
                                query,
                                builder,
                                streamId,
                                retentionProjection,
                                cursor);
                        if (builder.limitExceeded()) {
                            return CompletableFuture.completedFuture(builder.build());
                        }
                    }
                    if (page.continuation().isPresent()) {
                        String lastKey = cursorKey(
                                streamId,
                                page.records().get(page.records().size() - 1).value());
                        return scanCursors(
                                query,
                                builder,
                                streamIndex,
                                retentionProjection,
                                sawCursor,
                                page.continuation(),
                                lastKey);
                    }
                    if (retentionProjection.isEmpty() && sawCursor) {
                        builder.veto();
                    }
                    return scanStream(query, builder, streamIndex + 1);
                });
    }

    private void addRetention(
            GcReferenceSnapshotBuilder builder,
            StreamId streamId,
            Optional<VersionedCursorRetention> retention) {
        String key = keys.retentionKey(streamId);
        if (retention.isEmpty()) {
            builder.addAuthority(new GcAuthorityToken(
                    key,
                    0,
                    ReferenceDomainIdentityDigests.absence(DOMAIN_ID, key)));
            return;
        }
        VersionedCursorRetention value = retention.orElseThrow();
        builder.addAuthority(new GcAuthorityToken(
                key,
                value.metadataVersion(),
                CursorMetadataDigests.durableValueSha256(value.value())));
        if (!value.value().projection().streamId().equals(streamId.value())
                || value.value().lifecycle() != CursorRetentionLifecycle.ACTIVE) {
            builder.veto();
        }
    }

    private void addCursor(
            GcReferenceQuery query,
            GcReferenceSnapshotBuilder builder,
            StreamId streamId,
            Optional<ManagedLedgerProjectionIdentity> retentionProjection,
            VersionedCursorState versioned) {
        CursorStateRecord cursor = versioned.value();
        String key = cursorKey(streamId, cursor);
        var digest = CursorMetadataDigests.durableValueSha256(cursor);
        builder.addAuthority(new GcAuthorityToken(
                key, versioned.metadataVersion(), digest));
        if (!cursor.projection().streamId().equals(streamId.value())
                || retentionProjection.isEmpty()
                || !retentionProjection.orElseThrow().equals(cursor.projection())) {
            builder.veto();
        }
        if (cursor.lifecycle() != CursorRecordLifecycle.ACTIVE
                || cursor.snapshotReference().isEmpty()) {
            return;
        }
        CursorSnapshotReferenceRecord reference = cursor.snapshotReference().orElseThrow();
        if (!reference.objectKey().equals(query.object().objectKey().value())) {
            return;
        }
        builder.addReference(new GcReference(
                "cursor-snapshot-root",
                streamId.value() + "/" + cursor.cursorNameHash() + "/" + reference.snapshotId(),
                key,
                versioned.metadataVersion(),
                digest));
        builder.veto();
    }

    private void requireProgress(
            StreamId streamId, CursorScanPage page, String previousKey) {
        if (previousKey == null || page.records().isEmpty()) {
            return;
        }
        String firstKey = cursorKey(streamId, page.records().get(0).value());
        if (firstKey.compareTo(previousKey) <= 0) {
            throw new IllegalStateException("cursor reference scan did not advance");
        }
    }

    private String cursorKey(StreamId streamId, CursorStateRecord cursor) {
        return keys.cursorStateKey(streamId, cursor.cursorName());
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must be non-blank");
        }
        return value;
    }
}
