/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.api.target.ReadTarget;
import com.nereusstream.core.physical.GcAuthorityToken;
import com.nereusstream.core.physical.GcGlobalReferenceScope;
import com.nereusstream.core.physical.GcReference;
import com.nereusstream.core.physical.GcReferenceDomain;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.GcReferenceSnapshot;
import com.nereusstream.core.physical.GcReferenceSnapshotBuilder;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationScanPage;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedGenerationZeroIndex;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Exact generation-index reference domain for a query's complete affected-stream set. */
public final class GenerationReferenceDomain implements GcReferenceDomain {
    public static final String DOMAIN_ID = "generation-v1";
    public static final int PROTOCOL_VERSION = 1;

    private static final List<ReadView> VIEWS = List.of(
            ReadView.COMMITTED, ReadView.TOPIC_COMPACTED);
    private static final ReadTargetCodecRegistry TARGET_CODECS = ReadTargetCodecRegistry.phase15();

    private final String cluster;
    private final GenerationMetadataStore metadataStore;
    private final PhysicalGcConfig config;
    private final GcGlobalReferenceScope globalScope;

    public GenerationReferenceDomain(
            String cluster,
            GenerationMetadataStore metadataStore,
            PhysicalGcConfig config) {
        this(
                cluster,
                metadataStore,
                config,
                GcGlobalReferenceScope.unsupported());
    }

    public GenerationReferenceDomain(
            String cluster,
            GenerationMetadataStore metadataStore,
            PhysicalGcConfig config,
            GcGlobalReferenceScope globalScope) {
        this.cluster = requireText(cluster, "cluster");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.config = Objects.requireNonNull(config, "config");
        this.globalScope = Objects.requireNonNull(globalScope, "globalScope");
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
        GcReferenceSnapshotBuilder accumulator = new GcReferenceSnapshotBuilder(
                DOMAIN_ID, PROTOCOL_VERSION, query, config.referenceDomainConfig());
        return GcGlobalReferenceScope.resolveStreams(
                        query, accumulator, globalScope)
                .thenCompose(streams -> scan(
                        query,
                        streams,
                        accumulator,
                        0,
                        0,
                        Optional.empty(),
                        null));
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
            List<StreamId> streams,
            GcReferenceSnapshotBuilder accumulator,
            int streamIndex,
            int viewIndex,
            Optional<F4ScanToken> continuation,
            String previousKey) {
        if (accumulator.limitExceeded()) {
            return CompletableFuture.completedFuture(accumulator.build());
        }
        if (streamIndex == streams.size()) {
            return CompletableFuture.completedFuture(accumulator.build());
        }
        if (viewIndex == VIEWS.size()) {
            return scan(
                    query,
                    streams,
                    accumulator,
                    streamIndex + 1,
                    0,
                    Optional.empty(),
                    null);
        }
        StreamId streamId = streams.get(streamIndex);
        ReadView view = VIEWS.get(viewIndex);
        return metadataStore.scanIndex(
                        cluster,
                        streamId,
                        view,
                        0,
                        Long.MAX_VALUE,
                        continuation,
                        config.metadataScanPageSize())
                .thenCompose(page -> {
                    requireProgress(page, previousKey);
                    for (VersionedGenerationCandidate candidate : page.values()) {
                        accumulator.addAuthority(new GcAuthorityToken(
                                candidate.key(),
                                candidate.metadataVersion(),
                                candidate.durableValueSha256()));
                        ReferenceDisposition disposition = disposition(query, candidate);
                        if (disposition != ReferenceDisposition.NONE) {
                            accumulator.addReference(new GcReference(
                                    referenceType(candidate),
                                    candidate.key(),
                                    candidate.key(),
                                    candidate.metadataVersion(),
                                    candidate.durableValueSha256()));
                            if (disposition == ReferenceDisposition.VETO) {
                                accumulator.veto();
                            }
                        }
                        if (accumulator.limitExceeded()) {
                            return CompletableFuture.completedFuture(accumulator.build());
                        }
                    }
                    if (page.continuation().isPresent()) {
                        return scan(
                                query,
                                streams,
                                accumulator,
                                streamIndex,
                                viewIndex,
                                page.continuation(),
                                page.values().get(page.values().size() - 1).key());
                    }
                    return scan(
                            query,
                            streams,
                            accumulator,
                            streamIndex,
                            viewIndex + 1,
                            Optional.empty(),
                            null);
                });
    }

    private static ReferenceDisposition disposition(
            GcReferenceQuery query, VersionedGenerationCandidate candidate) {
        ReadTarget target;
        ReferenceDisposition matchingDisposition;
        if (candidate instanceof VersionedGenerationZeroIndex zero) {
            if (zero.value().tombstoned()) {
                return ReferenceDisposition.NONE;
            }
            target = zero.value().readTarget();
            matchingDisposition = ReferenceDisposition.REMOVABLE;
        } else if (candidate instanceof VersionedGenerationIndex higher) {
            GenerationLifecycle lifecycle = higher.value().lifecycle();
            if (lifecycle == GenerationLifecycle.RETIRED
                    || lifecycle == GenerationLifecycle.ABORTED) {
                return ReferenceDisposition.NONE;
            }
            target = TARGET_CODECS.decode(higher.value().readTarget());
            matchingDisposition = lifecycle == GenerationLifecycle.DRAINING
                    ? ReferenceDisposition.REMOVABLE
                    : ReferenceDisposition.VETO;
        } else {
            throw invariant("unknown generation candidate type");
        }
        if (!(target instanceof ObjectSliceReadTarget objectTarget)
                || !objectTarget.objectKey().equals(query.object().objectKey())) {
            return ReferenceDisposition.NONE;
        }
        boolean objectIdMatches = query.object().objectId().isEmpty()
                || query.object().objectId().orElseThrow().equals(objectTarget.objectId());
        return objectIdMatches ? matchingDisposition : ReferenceDisposition.NONE;
    }

    private static String referenceType(VersionedGenerationCandidate candidate) {
        return candidate instanceof VersionedGenerationZeroIndex
                ? "generation-zero-index"
                : "generation-index";
    }

    private static void requireProgress(GenerationScanPage page, String previousKey) {
        if (previousKey != null
                && !page.values().isEmpty()
                && page.values().get(0).key().compareTo(previousKey) <= 0) {
            throw invariant("generation reference scan did not advance");
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

    private enum ReferenceDisposition {
        NONE,
        REMOVABLE,
        VETO
    }
}
