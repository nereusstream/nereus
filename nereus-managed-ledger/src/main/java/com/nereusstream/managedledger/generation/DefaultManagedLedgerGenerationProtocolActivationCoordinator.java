/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.generation;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.core.capability.GenerationCapabilityReadiness;
import com.nereusstream.core.capability.GenerationCapabilityReadinessProvider;
import com.nereusstream.metadata.oxia.F4MetadataConditionFailedException;
import com.nereusstream.metadata.oxia.GenerationProtocolActivationStore;
import com.nereusstream.metadata.oxia.OxiaKeyspace;
import com.nereusstream.metadata.oxia.VersionedGenerationProtocolActivation;
import com.nereusstream.metadata.oxia.records.GenerationBackfillProofRecord;
import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationLifecycle;
import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationRecord;
import com.nereusstream.metadata.oxia.records.ReferenceDomainVersionRecord;
import java.time.Clock;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

/**
 * Performs the monotonic publication-only cluster activation after the exact
 * broker readiness and registration coverage proof have converged.
 */
public final class DefaultManagedLedgerGenerationProtocolActivationCoordinator
        implements ManagedLedgerGenerationProtocolActivationCoordinator {
    private static final int MAX_CAS_ATTEMPTS = 32;

    private final String cluster;
    private final boolean firstActivationEnabled;
    private final GenerationCapabilityReadinessProvider readinessProvider;
    private final GenerationProtocolActivationStore activations;
    private final List<ReferenceDomainVersionRecord> requiredDomains;
    private final Clock clock;

    public DefaultManagedLedgerGenerationProtocolActivationCoordinator(
            String cluster,
            boolean firstActivationEnabled,
            GenerationCapabilityReadinessProvider readinessProvider,
            GenerationProtocolActivationStore activations,
            List<ReferenceDomainVersionRecord> requiredDomains,
            Clock clock) {
        this.cluster = new OxiaKeyspace(cluster).cluster();
        this.firstActivationEnabled = firstActivationEnabled;
        this.readinessProvider = Objects.requireNonNull(
                readinessProvider, "readinessProvider");
        this.activations = Objects.requireNonNull(
                activations, "activations");
        this.requiredDomains = canonicalDomains(requiredDomains);
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public CompletableFuture<Void> activatePublication() {
        return activations.get(cluster)
                .thenCompose(existing -> {
                    if (!firstActivationEnabled
                            && (existing.isEmpty()
                                    || existing.orElseThrow()
                                                    .value()
                                                    .lifecycle()
                                            != GenerationProtocolActivationLifecycle
                                                    .ACTIVE)) {
                        return CompletableFuture.failedFuture(notReady(
                                "first generation publication activation is disabled"));
                    }
                    return readinessProvider
                            .requireGenerationCapabilityReadiness()
                            .thenCompose(readiness -> authority(
                                            existing,
                                            readiness)
                                    .thenCompose(current -> activate(
                                            readiness,
                                            current,
                                            0))
                                    .thenCompose(installed ->
                                            finalRevalidate(
                                                    readiness,
                                                    installed)));
                });
    }

    private CompletableFuture<VersionedGenerationProtocolActivation>
            authority(
                    Optional<VersionedGenerationProtocolActivation>
                            existing,
                    GenerationCapabilityReadiness readiness) {
        if (existing.isPresent()) {
            return CompletableFuture.completedFuture(
                    existing.orElseThrow());
        }
        return activations.getOrCreate(cluster)
                .thenApply(created -> {
                    requireAuthority(readiness, created.value());
                    return created;
                });
    }

    private CompletableFuture<VersionedGenerationProtocolActivation>
            activate(
                    GenerationCapabilityReadiness readiness,
                    VersionedGenerationProtocolActivation current,
                    int attempt) {
        if (attempt >= MAX_CAS_ATTEMPTS) {
            return CompletableFuture.failedFuture(notReady(
                    "generation publication activation CAS retry budget exhausted"));
        }
        GenerationProtocolActivationRecord value = current.value();
        requireAuthority(readiness, value);
        if (value.lifecycle()
                == GenerationProtocolActivationLifecycle.ACTIVE) {
            return CompletableFuture.completedFuture(current);
        }
        long activatedAt = Math.max(
                Math.max(value.preparedAtMillis(), value.updatedAtMillis()),
                Math.max(1, clock.millis()));
        GenerationProtocolActivationRecord replacement =
                new GenerationProtocolActivationRecord(
                        value.schemaVersion(),
                        value.protocolVersion(),
                        GenerationProtocolActivationLifecycle.ACTIVE,
                        true,
                        false,
                        false,
                        value.brokerCapabilityReadinessEpoch(),
                        value.requiredReferenceDomains(),
                        value.streamRegistrationBackfill(),
                        value.physicalRootBackfill(),
                        value.cursorSnapshotBackfill(),
                        value.objectStoreCapabilitySha256(),
                        value.activatingBrokerRunId(),
                        value.preparedAtMillis(),
                        activatedAt,
                        activatedAt,
                        0);
        return activations.compareAndSet(
                        cluster,
                        replacement,
                        current.metadataVersion())
                .handle((updated, failure) -> {
                    if (failure == null) {
                        return CompletableFuture.completedFuture(
                                updated);
                    }
                    Throwable cause = unwrap(failure);
                    return activations.get(cluster)
                            .thenCompose(optional -> {
                                VersionedGenerationProtocolActivation
                                        reloaded =
                                                optional.orElseThrow(() ->
                                                        notReady(
                                                                "generation activation disappeared after publication CAS"));
                                requireAuthority(
                                        readiness, reloaded.value());
                                if (reloaded.value().lifecycle()
                                        == GenerationProtocolActivationLifecycle
                                                .ACTIVE) {
                                    return CompletableFuture.completedFuture(
                                            reloaded);
                                }
                                if (cause
                                        instanceof
                                        F4MetadataConditionFailedException) {
                                    return activate(
                                            readiness,
                                            reloaded,
                                            attempt + 1);
                                }
                                return CompletableFuture.failedFuture(
                                        cause);
                            });
                })
                .thenCompose(Function.identity());
    }

    private CompletableFuture<Void> finalRevalidate(
            GenerationCapabilityReadiness expected,
            VersionedGenerationProtocolActivation installed) {
        Optional<GenerationCapabilityReadiness> cached =
                readinessProvider
                        .currentGenerationCapabilityReadiness();
        if (cached.isEmpty()
                || !cached.orElseThrow().equals(expected)) {
            return CompletableFuture.failedFuture(notReady(
                    "generation readiness was invalidated after publication activation"));
        }
        try {
            requireActiveAuthority(expected, installed.value());
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return activations.get(cluster)
                .thenAccept(optional -> {
                    VersionedGenerationProtocolActivation current =
                            optional.orElseThrow(() -> notReady(
                                    "generation activation disappeared during final publication read"));
                    requireActiveAuthority(expected, current.value());
                    GenerationCapabilityReadiness finalReadiness =
                            readinessProvider
                                    .currentGenerationCapabilityReadiness()
                                    .orElseThrow(() -> notReady(
                                            "generation readiness was invalidated during final publication read"));
                    if (!finalReadiness.equals(expected)) {
                        throw notReady(
                                "generation readiness changed during final publication read");
                    }
                });
    }

    private void requireActiveAuthority(
            GenerationCapabilityReadiness readiness,
            GenerationProtocolActivationRecord activation) {
        requireAuthority(readiness, activation);
        if (activation.lifecycle()
                        != GenerationProtocolActivationLifecycle.ACTIVE
                || !activation.publicationEnabled()) {
            throw notReady(
                    "generation publication activation is not ACTIVE");
        }
    }

    private void requireAuthority(
            GenerationCapabilityReadiness readiness,
            GenerationProtocolActivationRecord activation) {
        if (!activation.requiredReferenceDomains()
                .equals(requiredDomains)) {
            throw invariant(
                    "durable generation reference-domain set differs from the local runtime");
        }
        if (activation.brokerCapabilityReadinessEpoch()
                != readiness.brokerReadinessEpoch()) {
            throw notReady(
                    "generation registration proof belongs to another broker readiness epoch");
        }
        GenerationBackfillProofRecord registration =
                activation.streamRegistrationBackfill();
        if (!registration.complete()
                || registration.brokerReadinessEpoch()
                        != readiness.brokerReadinessEpoch()) {
            throw notReady(
                    "generation registration backfill is incomplete for the current readiness epoch");
        }
    }

    private static List<ReferenceDomainVersionRecord> canonicalDomains(
            List<ReferenceDomainVersionRecord> supplied) {
        List<ReferenceDomainVersionRecord> domains =
                Objects.requireNonNull(
                                supplied, "requiredDomains")
                        .stream()
                        .sorted(Comparator.naturalOrder())
                        .toList();
        if (domains.isEmpty()
                || domains.size()
                        > GenerationProtocolActivationRecord
                                .MAX_REFERENCE_DOMAINS) {
            throw new IllegalArgumentException(
                    "requiredDomains must be non-empty and bounded");
        }
        for (int index = 1; index < domains.size(); index++) {
            if (domains.get(index - 1)
                            .compareTo(domains.get(index))
                    >= 0
                    || domains.get(index - 1)
                            .domainId()
                            .equals(domains.get(index).domainId())) {
                throw new IllegalArgumentException(
                        "requiredDomains must be unique");
            }
        }
        return domains;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static NereusException notReady(String message) {
        return new NereusException(
                ErrorCode.METADATA_CONDITION_FAILED,
                true,
                message);
    }

    private static NereusException invariant(String message) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION,
                false,
                message);
    }
}
