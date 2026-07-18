/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.generation;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.core.capability.GenerationCapabilityReadiness;
import com.nereusstream.core.capability.GenerationCapabilityReadinessProvider;
import com.nereusstream.core.capability.GenerationRegistrationBackfillCompletion;
import com.nereusstream.metadata.oxia.F4MetadataConditionFailedException;
import com.nereusstream.metadata.oxia.GenerationProtocolActivationStore;
import com.nereusstream.metadata.oxia.OxiaKeyspace;
import com.nereusstream.metadata.oxia.VersionedGenerationProtocolActivation;
import com.nereusstream.metadata.oxia.records.GenerationBackfillProofRecord;
import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationRecord;
import com.nereusstream.metadata.oxia.records.ReferenceDomainVersionRecord;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

/**
 * Installs only the exact zero-failure registration coverage proof under the
 * broker readiness value that enclosed the whole traversal.
 */
public final class DefaultManagedLedgerGenerationRegistrationBackfillProofCoordinator
        implements ManagedLedgerGenerationRegistrationBackfillProofCoordinator {
    private static final int MAX_CAS_ATTEMPTS = 32;

    private final String cluster;
    private final GenerationProtocolActivationStore activations;
    private final GenerationCapabilityReadinessProvider readinessProvider;
    private final List<ReferenceDomainVersionRecord> requiredDomains;
    private final ManagedLedgerGenerationReadinessRolloverCoordinator
            readinessRollover;
    private final Clock clock;

    public DefaultManagedLedgerGenerationRegistrationBackfillProofCoordinator(
            String cluster,
            GenerationProtocolActivationStore activations,
            GenerationCapabilityReadinessProvider readinessProvider,
            List<ReferenceDomainVersionRecord> requiredDomains,
            Clock clock) {
        this(
                cluster,
                activations,
                readinessProvider,
                requiredDomains,
                (completion, maxConcurrentStreams, timeout, current) ->
                        CompletableFuture.failedFuture(notReady(
                                "registration proof cannot advance readiness while deletion is enabled")),
                clock);
    }

    public DefaultManagedLedgerGenerationRegistrationBackfillProofCoordinator(
            String cluster,
            GenerationProtocolActivationStore activations,
            GenerationCapabilityReadinessProvider readinessProvider,
            List<ReferenceDomainVersionRecord> requiredDomains,
            ManagedLedgerGenerationReadinessRolloverCoordinator readinessRollover,
            Clock clock) {
        this.cluster = new OxiaKeyspace(cluster).cluster();
        this.activations = Objects.requireNonNull(
                activations, "activations");
        this.readinessProvider = Objects.requireNonNull(
                readinessProvider, "readinessProvider");
        this.requiredDomains = canonicalDomains(requiredDomains);
        this.readinessRollover = Objects.requireNonNull(
                readinessRollover, "readinessRollover");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public CompletableFuture<Void> complete(
            GenerationRegistrationBackfillCompletion completion) {
        return complete(completion, 1, Duration.ofHours(1));
    }

    @Override
    public CompletableFuture<Void> complete(
            GenerationRegistrationBackfillCompletion completion,
            int maxConcurrentStreams,
            Duration timeout) {
        final GenerationRegistrationBackfillCompletion exact;
        try {
            exact = Objects.requireNonNull(completion, "completion");
            if (exact.failureCount() != 0) {
                throw notReady(
                        "registration backfill contains failures");
            }
            if (maxConcurrentStreams <= 0
                    || maxConcurrentStreams
                            > ManagedLedgerPhysicalDeletionActivationRequest
                                    .MAX_CONCURRENT_STREAMS) {
                throw new IllegalArgumentException(
                        "maxConcurrentStreams must be in [1, 1024]");
            }
            timeout = requireTimeout(timeout);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        final Duration exactTimeout = timeout;
        return readinessProvider
                .requireGenerationCapabilityReadiness()
                .thenCompose(current -> {
                    requireExactReadiness(exact.readiness(), current);
                    return activations.getOrCreate(cluster);
                })
                .thenCompose(current -> install(
                        exact,
                        maxConcurrentStreams,
                        exactTimeout,
                        current,
                        0))
                .thenCompose(installed -> finalRevalidate(
                        exact, installed));
    }

    private CompletableFuture<VersionedGenerationProtocolActivation> install(
            GenerationRegistrationBackfillCompletion completion,
            int maxConcurrentStreams,
            Duration timeout,
            VersionedGenerationProtocolActivation current,
            int attempt) {
        if (attempt >= MAX_CAS_ATTEMPTS) {
            return CompletableFuture.failedFuture(notReady(
                    "registration backfill proof CAS retry budget exhausted"));
        }
        GenerationProtocolActivationRecord value = current.value();
        requireDomainSet(value);
        long expectedEpoch = completion.readiness().brokerReadinessEpoch();
        if (value.brokerCapabilityReadinessEpoch() > expectedEpoch) {
            return CompletableFuture.failedFuture(notReady(
                    "registration backfill readiness is older than durable activation"));
        }
        GenerationBackfillProofRecord proof =
                value.streamRegistrationBackfill();
        if (proof.complete()
                && proof.brokerReadinessEpoch() == expectedEpoch) {
            if (!proof.coverageSha256()
                    .equals(completion.coverageSha256().value())) {
                return CompletableFuture.failedFuture(invariant(
                        "completed registration backfill has another coverage digest"));
            }
            return CompletableFuture.completedFuture(current);
        }
        if (value.physicalDeleteEnabled()
                || value.cursorSnapshotDeleteEnabled()) {
            return readinessRollover.rollover(
                    completion,
                    maxConcurrentStreams,
                    timeout,
                    current);
        }

        long completedAt = Math.max(1, clock.millis());
        boolean newerEpoch =
                expectedEpoch > value.brokerCapabilityReadinessEpoch();
        GenerationBackfillProofRecord replacementProof =
                new GenerationBackfillProofRecord(
                        completion.runId(),
                        expectedEpoch,
                        completion.coverageSha256().value(),
                        true,
                        completedAt);
        GenerationProtocolActivationRecord replacement =
                new GenerationProtocolActivationRecord(
                        value.schemaVersion(),
                        value.protocolVersion(),
                        value.lifecycle(),
                        value.publicationEnabled(),
                        value.physicalDeleteEnabled(),
                        value.cursorSnapshotDeleteEnabled(),
                        expectedEpoch,
                        value.requiredReferenceDomains(),
                        replacementProof,
                        newerEpoch
                                ? GenerationBackfillProofRecord.incomplete(
                                        expectedEpoch)
                                : value.physicalRootBackfill(),
                        newerEpoch
                                ? GenerationBackfillProofRecord.incomplete(
                                        expectedEpoch)
                                : value.cursorSnapshotBackfill(),
                        newerEpoch
                                ? ""
                                : value.objectStoreCapabilitySha256(),
                        value.activatingBrokerRunId(),
                        value.preparedAtMillis(),
                        value.activatedAtMillis(),
                        Math.max(value.updatedAtMillis(), completedAt),
                        0);
        return activations.compareAndSet(
                        cluster,
                        replacement,
                        current.metadataVersion())
                .handle((updated, failure) -> {
                    if (failure == null) {
                        return CompletableFuture.completedFuture(updated);
                    }
                    Throwable cause = unwrap(failure);
                    return activations.get(cluster)
                            .thenCompose(optional -> {
                                VersionedGenerationProtocolActivation reloaded =
                                        optional.orElseThrow(() -> notReady(
                                                "generation activation disappeared after proof CAS"));
                                requireDomainSet(reloaded.value());
                                if (sameCoverage(
                                        reloaded.value(),
                                        completion)) {
                                    return CompletableFuture.completedFuture(
                                            reloaded);
                                }
                                if (cause
                                        instanceof
                                        F4MetadataConditionFailedException) {
                                    return install(
                                            completion,
                                            maxConcurrentStreams,
                                            timeout,
                                            reloaded,
                                            attempt + 1);
                                }
                                return CompletableFuture.failedFuture(cause);
                            });
                })
                .thenCompose(Function.identity());
    }

    private CompletableFuture<Void> finalRevalidate(
            GenerationRegistrationBackfillCompletion expected,
            VersionedGenerationProtocolActivation installed) {
        Optional<GenerationCapabilityReadiness> readiness =
                readinessProvider.currentGenerationCapabilityReadiness();
        if (readiness.isEmpty()) {
            return CompletableFuture.failedFuture(notReady(
                    "generation readiness was invalidated after proof CAS"));
        }
        try {
            requireExactReadiness(
                    expected.readiness(), readiness.orElseThrow());
            requireDomainSet(installed.value());
            if (!sameCoverage(installed.value(), expected)) {
                throw notReady(
                        "installed registration backfill proof does not match completion");
            }
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return activations.get(cluster)
                .thenAccept(optional -> {
                    VersionedGenerationProtocolActivation current =
                            optional.orElseThrow(() -> notReady(
                                    "generation activation disappeared after proof completion"));
                    requireDomainSet(current.value());
                    if (!sameCoverage(current.value(), expected)) {
                        throw notReady(
                                "registration backfill proof changed after completion");
                    }
                    requireExactReadiness(
                            expected.readiness(),
                            readinessProvider
                                    .currentGenerationCapabilityReadiness()
                                    .orElseThrow(() -> notReady(
                                            "generation readiness was invalidated during final proof read")));
                });
    }

    private void requireDomainSet(
            GenerationProtocolActivationRecord activation) {
        if (!activation.requiredReferenceDomains()
                .equals(requiredDomains)) {
            throw invariant(
                    "durable generation reference-domain set differs from the local runtime");
        }
    }

    private static boolean sameCoverage(
            GenerationProtocolActivationRecord activation,
            GenerationRegistrationBackfillCompletion completion) {
        GenerationBackfillProofRecord proof =
                activation.streamRegistrationBackfill();
        return activation.brokerCapabilityReadinessEpoch()
                        == completion.readiness().brokerReadinessEpoch()
                && proof.complete()
                && proof.brokerReadinessEpoch()
                        == completion.readiness().brokerReadinessEpoch()
                && proof.coverageSha256()
                        .equals(completion.coverageSha256().value());
    }

    private static void requireExactReadiness(
            GenerationCapabilityReadiness expected,
            GenerationCapabilityReadiness actual) {
        if (!Objects.requireNonNull(expected, "expected")
                .equals(Objects.requireNonNull(actual, "actual"))) {
            throw notReady(
                    "broker generation readiness changed around registration proof");
        }
    }

    private static Duration requireTimeout(Duration value) {
        Objects.requireNonNull(value, "timeout");
        if (value.isZero()
                || value.isNegative()
                || value.toMillis() <= 0) {
            throw new IllegalArgumentException(
                    "timeout must be positive and millisecond-representable");
        }
        return value;
    }

    private static List<ReferenceDomainVersionRecord> canonicalDomains(
            List<ReferenceDomainVersionRecord> supplied) {
        List<ReferenceDomainVersionRecord> domains = List.copyOf(
                Objects.requireNonNull(supplied, "requiredDomains"));
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
                    >= 0) {
                throw new IllegalArgumentException(
                        "requiredDomains must be strictly sorted and unique");
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
