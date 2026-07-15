/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.api.target.ReadTarget;
import com.nereusstream.core.physical.GcAuthorityToken;
import com.nereusstream.core.physical.GcReference;
import com.nereusstream.core.physical.GcReferenceDomain;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.GcReferenceQueryKind;
import com.nereusstream.core.physical.GcReferenceSnapshot;
import com.nereusstream.core.physical.GcReferenceSnapshotBuilder;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.TaskScanPage;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.MaterializationOutputRecord;
import com.nereusstream.metadata.oxia.records.SourceGenerationRecord;
import com.nereusstream.metadata.oxia.records.TaskLifecycle;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Exact durable-task source/output reference domain for a query's affected streams. */
public final class MaterializationReferenceDomain implements GcReferenceDomain {
    public static final String DOMAIN_ID = "materialization-v1";
    public static final int PROTOCOL_VERSION = 1;

    private static final ReadTargetCodecRegistry TARGET_CODECS = ReadTargetCodecRegistry.phase15();

    private final String cluster;
    private final GenerationMetadataStore metadataStore;
    private final PhysicalGcConfig config;

    public MaterializationReferenceDomain(
            String cluster,
            GenerationMetadataStore metadataStore,
            PhysicalGcConfig config) {
        this.cluster = requireText(cluster, "cluster");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.config = Objects.requireNonNull(config, "config");
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
        GcReferenceSnapshotBuilder accumulator = new GcReferenceSnapshotBuilder(
                DOMAIN_ID, PROTOCOL_VERSION, query, config.referenceDomainConfig());
        return scan(query, accumulator, 0, Optional.empty(), null);
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
            GcReferenceSnapshotBuilder accumulator,
            int streamIndex,
            Optional<F4ScanToken> continuation,
            String previousKey) {
        if (accumulator.limitExceeded()) {
            return CompletableFuture.completedFuture(accumulator.build());
        }
        if (streamIndex == query.affectedStreams().size()) {
            return CompletableFuture.completedFuture(accumulator.build());
        }
        StreamId streamId = query.affectedStreams().get(streamIndex);
        return metadataStore.scanTasks(
                        cluster,
                        streamId,
                        continuation,
                        config.metadataScanPageSize())
                .thenCompose(page -> {
                    requireProgress(page, previousKey);
                    for (VersionedMaterializationTask task : page.values()) {
                        addTask(query, accumulator, task);
                        if (accumulator.limitExceeded()) {
                            return CompletableFuture.completedFuture(accumulator.build());
                        }
                    }
                    if (page.continuation().isPresent()) {
                        return scan(
                                query,
                                accumulator,
                                streamIndex,
                                page.continuation(),
                                page.values().get(page.values().size() - 1).key());
                    }
                    return scan(
                            query,
                            accumulator,
                            streamIndex + 1,
                            Optional.empty(),
                            null);
                });
    }

    private static void addTask(
            GcReferenceQuery query,
            GcReferenceSnapshotBuilder accumulator,
            VersionedMaterializationTask task) {
        accumulator.addAuthority(new GcAuthorityToken(
                task.key(), task.metadataVersion(), task.durableValueSha256()));
        if (!retainsPhysicalReferences(task.value().lifecycle())) {
            return;
        }
        for (int index = 0; index < task.value().sources().size(); index++) {
            SourceGenerationRecord source = task.value().sources().get(index);
            if (matches(query, TARGET_CODECS.decode(source.readTarget()))) {
                accumulator.addReference(new GcReference(
                        "materialization-source",
                        task.value().taskId() + "/source/" + index,
                        task.key(),
                        task.metadataVersion(),
                        task.durableValueSha256()));
                accumulator.veto();
            }
        }
        task.value().output().ifPresent(output -> {
            if (matches(query, output)) {
                accumulator.addReference(new GcReference(
                        "materialization-output",
                        task.value().taskId() + "/output/" + output.outputAttemptId(),
                        task.key(),
                        task.metadataVersion(),
                        task.durableValueSha256()));
                accumulator.veto();
            }
        });
    }

    private static boolean matches(GcReferenceQuery query, ReadTarget target) {
        if (!(target instanceof ObjectSliceReadTarget objectTarget)
                || !objectTarget.objectKey().equals(query.object().objectKey())) {
            return false;
        }
        return query.object().objectId().isEmpty()
                || query.object().objectId().orElseThrow().equals(objectTarget.objectId());
    }

    private static boolean matches(
            GcReferenceQuery query, MaterializationOutputRecord output) {
        if (!output.objectKey().equals(query.object().objectKey().value())
                || !output.objectKeyHash().equals(query.object().objectKeyHash().value())) {
            return false;
        }
        return query.object().objectId().isEmpty()
                || query.object().objectId().orElseThrow().value().equals(output.objectId());
    }

    private static boolean retainsPhysicalReferences(TaskLifecycle lifecycle) {
        return lifecycle != TaskLifecycle.PUBLISHED
                && lifecycle != TaskLifecycle.CANCELLED
                && lifecycle != TaskLifecycle.TERMINAL_FAILED;
    }

    private static void requireProgress(TaskScanPage page, String previousKey) {
        if (previousKey != null
                && !page.values().isEmpty()
                && page.values().get(0).key().compareTo(previousKey) <= 0) {
            throw invariant("materialization reference scan did not advance");
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
}
