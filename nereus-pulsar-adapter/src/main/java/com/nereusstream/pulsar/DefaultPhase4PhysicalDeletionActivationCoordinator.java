/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.core.capability.GenerationCapabilityReadiness;
import com.nereusstream.core.capability.GenerationCapabilityReadinessProvider;
import com.nereusstream.managedledger.generation.ManagedLedgerPhysicalDeletionActivationCoordinator;
import com.nereusstream.managedledger.generation.ManagedLedgerPhysicalDeletionActivationRequest;
import com.nereusstream.managedledger.generation.ManagedLedgerPhysicalDeletionActivationResult;
import com.nereusstream.managedledger.generation.ManagedLedgerPhysicalDeletionActivationResult.Status;
import com.nereusstream.materialization.gc.PhysicalRootBackfillCoordinator;
import com.nereusstream.materialization.gc.PhysicalRootBackfillReport;
import com.nereusstream.materialization.gc.PhysicalRootBackfillRequest;
import com.nereusstream.metadata.oxia.F4MetadataConditionFailedException;
import com.nereusstream.metadata.oxia.GenerationProtocolActivationStore;
import com.nereusstream.metadata.oxia.OxiaKeyspace;
import com.nereusstream.metadata.oxia.VersionedGenerationProtocolActivation;
import com.nereusstream.metadata.oxia.records.GenerationBackfillProofRecord;
import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationLifecycle;
import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationRecord;
import com.nereusstream.metadata.oxia.records.ReferenceDomainVersionRecord;
import com.nereusstream.objectstore.ObjectStoreDeleteCapabilityProbe;
import com.nereusstream.objectstore.ObjectStoreDeleteCapabilityProof;
import com.nereusstream.objectstore.ObjectStoreDeleteCapabilityRequest;
import java.time.Clock;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Runs the live-reference backfill and configured-scope canary before one monotonic CAS enables both V1 deletion bits.
 */
public final class DefaultPhase4PhysicalDeletionActivationCoordinator
        implements ManagedLedgerPhysicalDeletionActivationCoordinator {
    private static final int MAX_CAS_ATTEMPTS = 32;

    private final String cluster;
    private final boolean activationEnabled;
    private final GenerationCapabilityReadinessProvider readinessProvider;
    private final GenerationProtocolActivationStore activations;
    private final List<ReferenceDomainVersionRecord> requiredDomains;
    private final PhysicalRootBackfillCoordinator backfill;
    private final ObjectStoreDeleteCapabilityProbe capabilityProbe;
    private final String expectedCapabilitySha256;
    private final Clock clock;
    private final LongSupplier nanoTime;

    public DefaultPhase4PhysicalDeletionActivationCoordinator(
            String cluster,
            boolean activationEnabled,
            GenerationCapabilityReadinessProvider readinessProvider,
            GenerationProtocolActivationStore activations,
            List<ReferenceDomainVersionRecord> requiredDomains,
            PhysicalRootBackfillCoordinator backfill,
            ObjectStoreDeleteCapabilityProbe capabilityProbe,
            Clock clock) {
        this(
                cluster,
                activationEnabled,
                readinessProvider,
                activations,
                requiredDomains,
                backfill,
                capabilityProbe,
                clock,
                System::nanoTime);
    }

    DefaultPhase4PhysicalDeletionActivationCoordinator(
            String cluster,
            boolean activationEnabled,
            GenerationCapabilityReadinessProvider readinessProvider,
            GenerationProtocolActivationStore activations,
            List<ReferenceDomainVersionRecord> requiredDomains,
            PhysicalRootBackfillCoordinator backfill,
            ObjectStoreDeleteCapabilityProbe capabilityProbe,
            Clock clock,
            LongSupplier nanoTime) {
        this.cluster = new OxiaKeyspace(cluster).cluster();
        this.activationEnabled = activationEnabled;
        this.readinessProvider = Objects.requireNonNull(
                readinessProvider, "readinessProvider");
        this.activations = Objects.requireNonNull(activations, "activations");
        this.requiredDomains = canonicalDomains(requiredDomains);
        this.backfill = Objects.requireNonNull(backfill, "backfill");
        this.capabilityProbe = Objects.requireNonNull(
                capabilityProbe, "capabilityProbe");
        this.expectedCapabilitySha256 = requireSha256(
                capabilityProbe.expectedCapabilitySha256(),
                "expectedCapabilitySha256");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
    }

    public String expectedCapabilitySha256() {
        return expectedCapabilitySha256;
    }

    @Override
    public CompletableFuture<ManagedLedgerPhysicalDeletionActivationResult> activate(
            ManagedLedgerPhysicalDeletionActivationRequest request) {
        final ManagedLedgerPhysicalDeletionActivationRequest exact;
        final Deadline deadline;
        try {
            exact = Objects.requireNonNull(request, "request");
            if (!activationEnabled) {
                throw notReady(
                        "physical deletion activation requires enabled non-dry-run configuration");
            }
            deadline = Deadline.start(exact.timeout(), nanoTime);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return deadline.call(
                        readinessProvider::requireGenerationCapabilityReadiness)
                .thenCompose(readiness -> deadline
                        .call(() -> activations.get(cluster))
                        .thenCompose(optional -> start(
                                exact,
                                readiness,
                                requireActivation(optional),
                                deadline)));
    }

    private CompletableFuture<ManagedLedgerPhysicalDeletionActivationResult> start(
            ManagedLedgerPhysicalDeletionActivationRequest request,
            GenerationCapabilityReadiness readiness,
            VersionedGenerationProtocolActivation current,
            Deadline deadline) {
        final ActivationBasis basis;
        try {
            basis = captureBasis(readiness, current.value());
            if (deletionEnabled(current.value())) {
                Coverage coverage = coverage(current.value());
                requireDeletionAuthority(basis, coverage, current.value());
                return finalRevalidate(
                        request.runId(),
                        basis,
                        coverage,
                        Status.ALREADY_ACTIVE,
                        deadline);
            }
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }

        final PhysicalRootBackfillRequest backfillRequest;
        try {
            backfillRequest = new PhysicalRootBackfillRequest(
                    request.runId(),
                    readiness.brokerReadinessEpoch(),
                    request.maxConcurrentStreams(),
                    deadline.remaining());
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return deadline.call(() -> backfill.run(backfillRequest))
                .thenCompose(report -> afterBackfill(
                        request, basis, report, deadline));
    }

    private CompletableFuture<ManagedLedgerPhysicalDeletionActivationResult> afterBackfill(
            ManagedLedgerPhysicalDeletionActivationRequest request,
            ActivationBasis basis,
            PhysicalRootBackfillReport report,
            Deadline deadline) {
        final Coverage coverage;
        try {
            coverage = requireSuccessfulReport(request, basis, report);
            requireCurrentReadiness(basis.readiness());
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return deadline.call(() -> activations.get(cluster))
                .thenCompose(optional -> {
                    VersionedGenerationProtocolActivation current =
                            requireActivation(optional);
                    requireCoverageAuthority(basis, coverage, current.value());
                    if (deletionEnabled(current.value())) {
                        requireDeletionAuthority(
                                basis, coverage, current.value());
                        return finalRevalidate(
                                request.runId(),
                                basis,
                                coverage,
                                Status.ALREADY_ACTIVE,
                                deadline);
                    }
                    return probeAndActivate(
                            request,
                            basis,
                            coverage,
                            current,
                            deadline);
                });
    }

    private CompletableFuture<ManagedLedgerPhysicalDeletionActivationResult> probeAndActivate(
            ManagedLedgerPhysicalDeletionActivationRequest request,
            ActivationBasis basis,
            Coverage coverage,
            VersionedGenerationProtocolActivation current,
            Deadline deadline) {
        final ObjectStoreDeleteCapabilityRequest probeRequest;
        try {
            requireCurrentReadiness(basis.readiness());
            probeRequest = new ObjectStoreDeleteCapabilityRequest(
                    request.runId(), deadline.remaining());
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return deadline.call(() -> capabilityProbe.probe(probeRequest))
                .thenCompose(proof -> {
                    requireCapabilityProof(proof);
                    requireCurrentReadiness(basis.readiness());
                    return install(
                            basis,
                            coverage,
                            proof,
                            current,
                            0,
                            deadline);
                })
                .thenCompose(installed -> finalRevalidate(
                        request.runId(),
                        basis,
                        coverage,
                        installed.status(),
                        deadline));
    }

    private CompletableFuture<InstallResult> install(
            ActivationBasis basis,
            Coverage coverage,
            ObjectStoreDeleteCapabilityProof proof,
            VersionedGenerationProtocolActivation current,
            int attempt,
            Deadline deadline) {
        if (attempt >= MAX_CAS_ATTEMPTS) {
            return CompletableFuture.failedFuture(notReady(
                    "physical deletion activation CAS retry budget exhausted"));
        }
        try {
            requireCurrentReadiness(basis.readiness());
            requireCoverageAuthority(basis, coverage, current.value());
            if (deletionEnabled(current.value())) {
                requireDeletionAuthority(basis, coverage, current.value());
                return CompletableFuture.completedFuture(
                        new InstallResult(current, Status.ALREADY_ACTIVE));
            }
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }

        GenerationProtocolActivationRecord replacement = deletionReplacement(
                current.value(), proof.completedAtMillis());
        return deadline.call(() -> activations.compareAndSet(
                        cluster, replacement, current.metadataVersion()))
                .handle((updated, failure) -> {
                    if (failure == null) {
                        try {
                            requireDeletionAuthority(
                                    basis, coverage, updated.value());
                            return CompletableFuture.completedFuture(
                                    new InstallResult(updated, Status.ACTIVATED));
                        } catch (Throwable invalid) {
                            return CompletableFuture
                                    .<InstallResult>failedFuture(invalid);
                        }
                    }
                    Throwable cause = unwrap(failure);
                    return deadline.call(() -> activations.get(cluster))
                            .thenCompose(optional -> {
                                VersionedGenerationProtocolActivation reloaded =
                                        requireActivation(optional);
                                requireCoverageAuthority(
                                        basis, coverage, reloaded.value());
                                if (deletionEnabled(reloaded.value())) {
                                    requireDeletionAuthority(
                                            basis, coverage, reloaded.value());
                                    Status status = cause
                                                    instanceof
                                                    F4MetadataConditionFailedException
                                            ? Status.ALREADY_ACTIVE
                                            : Status.ACTIVATED;
                                    return CompletableFuture.completedFuture(
                                            new InstallResult(reloaded, status));
                                }
                                if (cause
                                        instanceof
                                        F4MetadataConditionFailedException) {
                                    return install(
                                            basis,
                                            coverage,
                                            proof,
                                            reloaded,
                                            attempt + 1,
                                            deadline);
                                }
                                return CompletableFuture.failedFuture(cause);
                            });
                })
                .thenCompose(Function.identity());
    }

    private CompletableFuture<ManagedLedgerPhysicalDeletionActivationResult> finalRevalidate(
            String runId,
            ActivationBasis basis,
            Coverage coverage,
            Status status,
            Deadline deadline) {
        try {
            requireCurrentReadiness(basis.readiness());
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return deadline.call(() -> activations.get(cluster))
                .thenApply(optional -> {
                    VersionedGenerationProtocolActivation current =
                            requireActivation(optional);
                    requireDeletionAuthority(
                            basis, coverage, current.value());
                    requireCurrentReadiness(basis.readiness());
                    return new ManagedLedgerPhysicalDeletionActivationResult(
                            runId,
                            basis.readiness().brokerReadinessEpoch(),
                            coverage.physicalRootSha256(),
                            coverage.cursorSnapshotSha256(),
                            expectedCapabilitySha256,
                            current.metadataVersion(),
                            status);
                });
    }

    private ActivationBasis captureBasis(
            GenerationCapabilityReadiness readiness,
            GenerationProtocolActivationRecord activation) {
        requireBaseAuthority(readiness, activation);
        return new ActivationBasis(
                readiness, activation.streamRegistrationBackfill());
    }

    private void requireBaseAuthority(
            GenerationCapabilityReadiness readiness,
            GenerationProtocolActivationRecord activation) {
        if (activation.lifecycle()
                        != GenerationProtocolActivationLifecycle.ACTIVE
                || !activation.publicationEnabled()) {
            throw notReady(
                    "generation publication must be ACTIVE before physical deletion activation");
        }
        if (!activation.requiredReferenceDomains().equals(requiredDomains)) {
            throw invariant(
                    "durable generation reference-domain set differs from the local runtime");
        }
        if (activation.brokerCapabilityReadinessEpoch()
                != readiness.brokerReadinessEpoch()) {
            throw notReady(
                    "generation activation belongs to another broker readiness epoch");
        }
        GenerationBackfillProofRecord registration =
                activation.streamRegistrationBackfill();
        if (!registration.complete()
                || registration.brokerReadinessEpoch()
                        != readiness.brokerReadinessEpoch()) {
            throw notReady(
                    "registration backfill is incomplete for the current readiness epoch");
        }
        requireCapabilityCompatible(activation);
    }

    private void requireCoverageAuthority(
            ActivationBasis basis,
            Coverage coverage,
            GenerationProtocolActivationRecord activation) {
        requireBaseAuthority(basis.readiness(), activation);
        if (!activation.streamRegistrationBackfill()
                .equals(basis.registrationBackfill())) {
            throw notReady(
                    "registration backfill changed during physical deletion activation");
        }
        requireCoverageProof(
                activation.physicalRootBackfill(),
                basis.readiness().brokerReadinessEpoch(),
                coverage.physicalRootSha256(),
                "physical-root");
        requireCoverageProof(
                activation.cursorSnapshotBackfill(),
                basis.readiness().brokerReadinessEpoch(),
                coverage.cursorSnapshotSha256(),
                "cursor-snapshot");
    }

    private void requireDeletionAuthority(
            ActivationBasis basis,
            Coverage coverage,
            GenerationProtocolActivationRecord activation) {
        requireCoverageAuthority(basis, coverage, activation);
        if (!deletionEnabled(activation)
                || !activation.objectStoreCapabilitySha256()
                        .equals(expectedCapabilitySha256)) {
            throw notReady(
                    "physical deletion activation is not installed for the configured object-store scope");
        }
    }

    private void requireCapabilityCompatible(
            GenerationProtocolActivationRecord activation) {
        if (!activation.objectStoreCapabilitySha256().isEmpty()
                && !activation.objectStoreCapabilitySha256()
                        .equals(expectedCapabilitySha256)) {
            throw invariant(
                    "object-store capability differs within the same broker readiness epoch");
        }
    }

    private void requireCurrentReadiness(
            GenerationCapabilityReadiness expected) {
        Optional<GenerationCapabilityReadiness> current =
                readinessProvider.currentGenerationCapabilityReadiness();
        if (current.isEmpty()
                || !current.orElseThrow().equals(expected)) {
            throw notReady(
                    "broker generation readiness changed during physical deletion activation");
        }
    }

    private Coverage requireSuccessfulReport(
            ManagedLedgerPhysicalDeletionActivationRequest request,
            ActivationBasis basis,
            PhysicalRootBackfillReport report) {
        PhysicalRootBackfillReport exact = Objects.requireNonNull(
                report, "backfill report");
        if (!exact.runId().equals(request.runId())
                || exact.brokerReadinessEpoch()
                        != basis.readiness().brokerReadinessEpoch()) {
            throw invariant(
                    "physical-root backfill report does not match its activation request");
        }
        if (exact.failureCount() != 0
                || !exact.boundedFailures().isEmpty()) {
            throw notReady(
                    "physical-root/cursor-root backfill contains failures");
        }
        return new Coverage(
                exact.dataCoverageSha256().value(),
                exact.cursorCoverageSha256().value());
    }

    private void requireCapabilityProof(
            ObjectStoreDeleteCapabilityProof proof) {
        ObjectStoreDeleteCapabilityProof exact = Objects.requireNonNull(
                proof, "capability proof");
        if (exact.protocolVersion()
                        != ObjectStoreDeleteCapabilityProof.PROTOCOL_VERSION
                || !exact.capabilitySha256()
                        .equals(expectedCapabilitySha256)) {
            throw invariant(
                    "object-store capability proof does not match the configured V1 scope");
        }
    }

    private GenerationProtocolActivationRecord deletionReplacement(
            GenerationProtocolActivationRecord current,
            long capabilityCompletedAtMillis) {
        long updatedAt = Math.max(
                Math.max(current.updatedAtMillis(), capabilityCompletedAtMillis),
                Math.max(1, clock.millis()));
        return new GenerationProtocolActivationRecord(
                current.schemaVersion(),
                current.protocolVersion(),
                current.lifecycle(),
                current.publicationEnabled(),
                true,
                true,
                current.brokerCapabilityReadinessEpoch(),
                current.requiredReferenceDomains(),
                current.streamRegistrationBackfill(),
                current.physicalRootBackfill(),
                current.cursorSnapshotBackfill(),
                expectedCapabilitySha256,
                current.activatingBrokerRunId(),
                current.preparedAtMillis(),
                current.activatedAtMillis(),
                updatedAt,
                0);
    }

    private static Coverage coverage(
            GenerationProtocolActivationRecord activation) {
        return new Coverage(
                activation.physicalRootBackfill().coverageSha256(),
                activation.cursorSnapshotBackfill().coverageSha256());
    }

    private static void requireCoverageProof(
            GenerationBackfillProofRecord proof,
            long readinessEpoch,
            String expectedCoverageSha256,
            String name) {
        if (!proof.complete()
                || proof.brokerReadinessEpoch() != readinessEpoch
                || !proof.coverageSha256().equals(expectedCoverageSha256)) {
            throw notReady(
                    name + " backfill proof is absent or changed");
        }
    }

    private static boolean deletionEnabled(
            GenerationProtocolActivationRecord activation) {
        return activation.physicalDeleteEnabled()
                && activation.cursorSnapshotDeleteEnabled();
    }

    private static VersionedGenerationProtocolActivation requireActivation(
            Optional<VersionedGenerationProtocolActivation> optional) {
        return Objects.requireNonNull(optional, "activation result")
                .orElseThrow(() -> notReady(
                        "generation activation record is absent"));
    }

    private static List<ReferenceDomainVersionRecord> canonicalDomains(
            List<ReferenceDomainVersionRecord> supplied) {
        List<ReferenceDomainVersionRecord> domains = Objects.requireNonNull(
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
            if (domains.get(index - 1).compareTo(domains.get(index)) >= 0
                    || domains.get(index - 1)
                            .domainId()
                            .equals(domains.get(index).domainId())) {
                throw new IllegalArgumentException(
                        "requiredDomains must be unique");
            }
        }
        return domains;
    }

    private static String requireSha256(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.length() != 64) {
            throw new IllegalArgumentException(
                    field + " must be lowercase SHA-256");
        }
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (!((character >= '0' && character <= '9')
                    || (character >= 'a' && character <= 'f'))) {
                throw new IllegalArgumentException(
                        field + " must be lowercase SHA-256");
            }
        }
        return value;
    }

    private static Throwable unwrap(Throwable supplied) {
        Throwable current = supplied;
        while ((current instanceof CompletionException
                        || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static NereusException notReady(String message) {
        return new NereusException(
                ErrorCode.METADATA_CONDITION_FAILED, true, message);
    }

    private static NereusException invariant(String message) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private record ActivationBasis(
            GenerationCapabilityReadiness readiness,
            GenerationBackfillProofRecord registrationBackfill) {
        private ActivationBasis {
            Objects.requireNonNull(readiness, "readiness");
            Objects.requireNonNull(
                    registrationBackfill, "registrationBackfill");
        }
    }

    private record Coverage(
            String physicalRootSha256,
            String cursorSnapshotSha256) {
        private Coverage {
            physicalRootSha256 = requireSha256(
                    physicalRootSha256, "physicalRootSha256");
            cursorSnapshotSha256 = requireSha256(
                    cursorSnapshotSha256, "cursorSnapshotSha256");
        }
    }

    private record InstallResult(
            VersionedGenerationProtocolActivation activation,
            Status status) {
        private InstallResult {
            Objects.requireNonNull(activation, "activation");
            Objects.requireNonNull(status, "status");
        }
    }

    private static final class Deadline {
        private final long deadlineNanos;
        private final LongSupplier nanoTime;

        private Deadline(long deadlineNanos, LongSupplier nanoTime) {
            this.deadlineNanos = deadlineNanos;
            this.nanoTime = nanoTime;
        }

        private static Deadline start(
                Duration timeout, LongSupplier nanoTime) {
            Objects.requireNonNull(timeout, "timeout");
            Objects.requireNonNull(nanoTime, "nanoTime");
            try {
                return new Deadline(
                        Math.addExact(
                                nanoTime.getAsLong(), timeout.toNanos()),
                        nanoTime);
            } catch (ArithmeticException failure) {
                throw new IllegalArgumentException(
                        "physical deletion activation deadline overflows",
                        failure);
            }
        }

        private Duration remaining() {
            long nanos = deadlineNanos - nanoTime.getAsLong();
            if (nanos <= 0) {
                throw new NereusException(
                        ErrorCode.TIMEOUT,
                        true,
                        "physical deletion activation timed out");
            }
            long millis = Math.max(
                    1, (nanos + 999_999L) / 1_000_000L);
            return Duration.ofMillis(millis);
        }

        private <T> CompletableFuture<T> call(
                Supplier<CompletableFuture<T>> operation) {
            Duration bounded = remaining();
            final CompletableFuture<T> future;
            try {
                future = Objects.requireNonNull(
                        operation.get(), "operation future");
            } catch (Throwable failure) {
                return CompletableFuture.failedFuture(failure);
            }
            return future.orTimeout(
                    bounded.toMillis(), TimeUnit.MILLISECONDS);
        }
    }
}
