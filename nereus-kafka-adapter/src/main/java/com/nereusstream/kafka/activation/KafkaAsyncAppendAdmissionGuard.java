/* Licensed under the Apache License, Version 2.0 */

package com.nereusstream.kafka.activation;

import com.nereusstream.api.StorageProfile;
import com.nereusstream.core.append.AppendAdmissionGuard;
import com.nereusstream.core.append.AppendAdmissionRequest;
import com.nereusstream.core.backpressure.MaterializationLagGate;
import com.nereusstream.core.capability.GenerationActivationProof;
import com.nereusstream.core.capability.GenerationOperation;
import com.nereusstream.core.capability.LiveStreamSubject;
import com.nereusstream.materialization.DirectMaterializationStreamAuthority;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Requires direct-stream publication authority and bounded materialization lag before either Kafka
 * async Object profile may enter primary-WAL IO.
 */
public final class KafkaAsyncAppendAdmissionGuard implements AppendAdmissionGuard {
    private final KafkaGenerationProtocolActivationGuard activation;
    private final MaterializationLagGate lagGate;

    public KafkaAsyncAppendAdmissionGuard(
            KafkaGenerationProtocolActivationGuard activation, MaterializationLagGate lagGate) {
        this.activation = Objects.requireNonNull(activation, "activation");
        this.lagGate = Objects.requireNonNull(lagGate, "lagGate");
    }

    @Override
    public CompletableFuture<Void> admit(AppendAdmissionRequest request) {
        AppendAdmissionRequest exact;
        try {
            exact = Objects.requireNonNull(request, "request");
            StorageProfile profile = exact.storageProfile().canonical();
            if (profile != StorageProfile.OBJECT_WAL_ASYNC_OBJECT
                    && profile != StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT) {
                return CompletableFuture.completedFuture(null);
            }
            LiveStreamSubject subject = new LiveStreamSubject(
                    exact.streamId(), DirectMaterializationStreamAuthority.identitySha256(exact.streamId(), profile));
            return activation
                    .requireReady(GenerationOperation.GENERATION_PUBLISH, subject, false)
                    .thenCompose(proof ->
                            lagGate.admit(exact.streamId(), exact.timeout()).thenCompose(ignored -> revalidate(proof)));
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private CompletableFuture<Void> revalidate(GenerationActivationProof proof) {
        return activation.revalidate(proof);
    }
}
