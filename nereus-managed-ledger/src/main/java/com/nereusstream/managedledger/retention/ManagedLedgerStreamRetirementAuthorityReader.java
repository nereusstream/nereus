/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.retention;

import com.nereusstream.core.capability.LiveProjectionSubject;
import com.nereusstream.core.capability.StreamRetirementReferenceAuthorityReader;
import com.nereusstream.core.capability.StreamRetirementReferenceAuthoritySnapshot;
import com.nereusstream.core.physical.GcAuthorityToken;
import com.nereusstream.core.physical.GcReferenceDomainConfig;
import com.nereusstream.managedledger.generation.ManagedLedgerGenerationProjectionRefV1;
import com.nereusstream.metadata.oxia.CursorKeyspace;
import com.nereusstream.metadata.oxia.CursorMetadataDigests;
import com.nereusstream.metadata.oxia.CursorMetadataStore;
import com.nereusstream.metadata.oxia.CursorScanPage;
import com.nereusstream.metadata.oxia.CursorScanToken;
import com.nereusstream.metadata.oxia.VersionedCursorRetention;
import com.nereusstream.metadata.oxia.VersionedCursorState;
import com.nereusstream.metadata.oxia.records.CursorRecordLifecycle;
import com.nereusstream.metadata.oxia.records.CursorRetentionLifecycle;
import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** F3 cursor/retention authority that must be reference-free before stream registration removal. */
public final class ManagedLedgerStreamRetirementAuthorityReader
        implements StreamRetirementReferenceAuthorityReader {
    private static final String AUTHORITY_DOMAIN = "stream-retirement-cursor-v1";

    private final String cluster;
    private final CursorMetadataStore metadata;
    private final int pageSize;
    private final int maxAuthorities;
    private final CursorKeyspace keys;

    public ManagedLedgerStreamRetirementAuthorityReader(
            String cluster,
            CursorMetadataStore metadata,
            GcReferenceDomainConfig config) {
        this.cluster = requireText(cluster, "cluster");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        GcReferenceDomainConfig exactConfig = Objects.requireNonNull(config, "config");
        this.pageSize = exactConfig.metadataScanPageSize();
        this.maxAuthorities = exactConfig.maxAuthoritiesPerSnapshot();
        this.keys = new CursorKeyspace(cluster);
    }

    @Override
    public CompletableFuture<StreamRetirementReferenceAuthoritySnapshot> capture(
            LiveProjectionSubject subject) {
        LiveProjectionSubject exact = Objects.requireNonNull(subject, "subject");
        ManagedLedgerGenerationProjectionRefV1 decoded =
                ManagedLedgerGenerationProjectionRefV1.from(exact.projectionRef());
        if (!decoded.identity().streamId().equals(exact.streamId().value())
                || !decoded.projectionIdentitySha256()
                        .equals(exact.projectionIdentitySha256())) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "stream-retirement subject does not match its NPR1 identity"));
        }
        return metadata.getRetention(cluster, exact.streamId())
                .thenCompose(first -> {
                    Accumulator accumulator = new Accumulator(exact, decoded.identity());
                    accumulator.addRetention(first);
                    return scan(
                                    accumulator,
                                    first,
                                    Optional.empty(),
                                    null)
                            .thenCompose(ignored -> metadata.getRetention(
                                    cluster, exact.streamId()))
                            .thenApply(second -> {
                                if (!second.equals(first)) {
                                    accumulator.incomplete = true;
                                }
                                return accumulator.snapshot();
                            });
                });
    }

    private CompletableFuture<Void> scan(
            Accumulator accumulator,
            Optional<VersionedCursorRetention> retention,
            Optional<CursorScanToken> continuation,
            String previousKey) {
        if (accumulator.incomplete) {
            return CompletableFuture.completedFuture(null);
        }
        return metadata.scanCursors(
                        cluster,
                        accumulator.subject.streamId(),
                        continuation,
                        pageSize)
                .thenCompose(page -> {
                    requireProgress(accumulator.subject, page, previousKey);
                    for (VersionedCursorState cursor : page.records()) {
                        if (!accumulator.addCursor(retention, cursor)) {
                            return CompletableFuture.completedFuture(null);
                        }
                    }
                    if (page.continuation().isEmpty()) {
                        return CompletableFuture.completedFuture(null);
                    }
                    if (accumulator.authorityCount >= maxAuthorities) {
                        accumulator.incomplete = true;
                        return CompletableFuture.completedFuture(null);
                    }
                    VersionedCursorState last = page.records().get(page.records().size() - 1);
                    return scan(
                            accumulator,
                            retention,
                            page.continuation(),
                            keys.cursorStateKey(
                                    accumulator.subject.streamId(),
                                    last.value().cursorName()));
                });
    }

    private void requireProgress(
            LiveProjectionSubject subject,
            CursorScanPage page,
            String previousKey) {
        if (previousKey == null || page.records().isEmpty()) {
            return;
        }
        String firstKey = keys.cursorStateKey(
                subject.streamId(), page.records().get(0).value().cursorName());
        if (firstKey.compareTo(previousKey) <= 0) {
            throw new IllegalStateException("stream-retirement cursor scan did not advance");
        }
    }

    private final class Accumulator {
        private final LiveProjectionSubject subject;
        private final ManagedLedgerProjectionIdentity projection;
        private final ArrayList<GcAuthorityToken> authorities = new ArrayList<>();
        private long authorityCount;
        private long liveReferenceCount;
        private boolean incomplete;

        private Accumulator(
                LiveProjectionSubject subject,
                ManagedLedgerProjectionIdentity projection) {
            this.subject = subject;
            this.projection = projection;
        }

        private void addRetention(Optional<VersionedCursorRetention> retention) {
            String key = keys.retentionKey(subject.streamId());
            if (retention.isEmpty()) {
                addAuthority(new GcAuthorityToken(
                        key,
                        0,
                        ReferenceDomainIdentityDigests.absence(
                                AUTHORITY_DOMAIN, key)));
                return;
            }
            VersionedCursorRetention exact = retention.orElseThrow();
            addAuthority(new GcAuthorityToken(
                    key,
                    exact.metadataVersion(),
                    CursorMetadataDigests.durableValueSha256(exact.value())));
            if (!exact.value().projection().equals(projection)
                    || exact.value().lifecycle() != CursorRetentionLifecycle.ACTIVE) {
                liveReferenceCount = Math.addExact(liveReferenceCount, 1);
            }
        }

        private boolean addCursor(
                Optional<VersionedCursorRetention> retention,
                VersionedCursorState cursor) {
            String key = keys.cursorStateKey(
                    subject.streamId(), cursor.value().cursorName());
            addAuthority(new GcAuthorityToken(
                    key,
                    cursor.metadataVersion(),
                    CursorMetadataDigests.durableValueSha256(cursor.value())));
            if (incomplete) {
                return false;
            }
            boolean exactProjection = cursor.value().projection().equals(projection);
            boolean terminal = cursor.value().lifecycle() == CursorRecordLifecycle.DELETED;
            if (retention.isEmpty() || !exactProjection || !terminal) {
                liveReferenceCount = Math.addExact(liveReferenceCount, 1);
            }
            return true;
        }

        private void addAuthority(GcAuthorityToken authority) {
            authorityCount = Math.addExact(authorityCount, 1);
            if (authorityCount <= maxAuthorities) {
                authorities.add(authority);
            } else {
                incomplete = true;
            }
        }

        private StreamRetirementReferenceAuthoritySnapshot snapshot() {
            boolean complete = !incomplete;
            return new StreamRetirementReferenceAuthoritySnapshot(
                    subject,
                    complete,
                    complete && liveReferenceCount == 0,
                    authorityCount,
                    liveReferenceCount,
                    List.copyOf(authorities));
        }
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must be non-blank");
        }
        return value;
    }
}
