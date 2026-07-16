/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.generation;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.core.append.AppendAdmissionGuard;
import com.nereusstream.core.append.AppendAdmissionRequest;
import com.nereusstream.core.backpressure.MaterializationLagGate;
import com.nereusstream.core.capability.GenerationActivationProof;
import com.nereusstream.core.capability.GenerationOperation;
import com.nereusstream.core.capability.GenerationProtocolActivationGuard;
import com.nereusstream.core.capability.LiveProjectionSubject;
import com.nereusstream.metadata.oxia.ManagedLedgerFacadeState;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionMetadataStore;
import com.nereusstream.metadata.oxia.ManagedLedgerStreamProjection;
import com.nereusstream.metadata.oxia.VersionedTopicProjection;
import com.nereusstream.metadata.oxia.VersionedVirtualLedgerProjection;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Resolves the exact F2 projection, activates/revalidates its F4 marker, and applies lag admission before async WAL IO.
 */
public final class ManagedLedgerAsyncAppendAdmissionGuard
        implements AppendAdmissionGuard {
    private final String cluster;
    private final ManagedLedgerProjectionMetadataStore projections;
    private final GenerationProtocolActivationGuard activationGuard;
    private final MaterializationLagGate lagGate;

    public ManagedLedgerAsyncAppendAdmissionGuard(
            String cluster,
            ManagedLedgerProjectionMetadataStore projections,
            GenerationProtocolActivationGuard activationGuard,
            MaterializationLagGate lagGate) {
        this.cluster = requireText(cluster, "cluster");
        this.projections = Objects.requireNonNull(
                projections, "projections");
        this.activationGuard = Objects.requireNonNull(
                activationGuard, "activationGuard");
        this.lagGate = Objects.requireNonNull(lagGate, "lagGate");
    }

    @Override
    public CompletableFuture<Void> admit(
            AppendAdmissionRequest request) {
        final AppendAdmissionRequest exact;
        try {
            exact = Objects.requireNonNull(request, "request");
            if (exact.storageProfile()
                    != StorageProfile.OBJECT_WAL_ASYNC_OBJECT) {
                return CompletableFuture.completedFuture(null);
            }
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return projections.getProjectionByStream(
                        cluster, exact.streamId())
                .thenApply(ManagedLedgerAsyncAppendAdmissionGuard::subject)
                .thenCompose(subject -> activationGuard.requireReady(
                                GenerationOperation.GENERATION_PUBLISH,
                                subject,
                                true)
                        .thenCompose(proof -> lagGate.admit(
                                        exact.streamId(),
                                        exact.timeout())
                                .thenCompose(ignored ->
                                        revalidate(proof))));
    }

    private CompletableFuture<Void> revalidate(
            GenerationActivationProof proof) {
        return activationGuard.revalidate(proof);
    }

    private static LiveProjectionSubject subject(
            ManagedLedgerStreamProjection projection) {
        VersionedVirtualLedgerProjection binding =
                projection.streamBinding().orElseThrow(() ->
                        notReady(
                                "async stream has no managed-ledger projection binding"));
        VersionedTopicProjection topic =
                projection.currentTopic().orElseThrow(() ->
                        notReady(
                                "async stream has no current topic projection"));
        TopicProjectionRecord value = topic.value();
        ManagedLedgerFacadeState state =
                value.parsedFacadeState();
        if (!binding.value().identity()
                        .equals(value.projectionIdentity())
                || !binding.value().managedLedgerName()
                        .equals(value.managedLedgerName())
                || !value.streamId()
                        .equals(projection.streamId().value())) {
            throw invariant(
                    "managed-ledger binding and topic projection disagree for async admission");
        }
        if (state != ManagedLedgerFacadeState.OPEN
                && state != ManagedLedgerFacadeState.SEALED) {
            throw notReady(
                    "topic projection is not live for async admission");
        }
        final StorageProfile profile;
        try {
            profile = StorageProfile.valueOf(
                    value.storageProfile());
        } catch (IllegalArgumentException failure) {
            throw invariant(
                    "topic projection has an unknown async storage profile",
                    failure);
        }
        if (profile != StorageProfile.OBJECT_WAL_ASYNC_OBJECT) {
            throw invariant(
                    "async L0 stream is bound to a non-async topic projection");
        }
        ManagedLedgerGenerationProjectionRefV1 reference =
                new ManagedLedgerGenerationProjectionRefV1(
                        value.managedLedgerName(),
                        value.projectionIdentity());
        return new LiveProjectionSubject(
                projection.streamId(),
                reference.toProjectionRef(),
                reference.projectionIdentitySha256());
    }

    private static NereusException notReady(String message) {
        return new NereusException(
                ErrorCode.METADATA_CONDITION_FAILED,
                true,
                message);
    }

    private static NereusException invariant(String message) {
        return invariant(message, null);
    }

    private static NereusException invariant(
            String message,
            Throwable cause) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION,
                false,
                message,
                cause);
    }

    private static String requireText(
            String value,
            String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(
                    field + " cannot be blank");
        }
        return value;
    }
}
