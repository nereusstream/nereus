/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.core.capability.GenerationCapabilityReadiness;
import com.nereusstream.core.capability.GenerationCapabilityReadinessProvider;
import com.nereusstream.core.capability.GenerationRegistrationBackfillCompletion;
import com.nereusstream.managedledger.generation.ManagedLedgerPhysicalDeletionActivationRequest;
import com.nereusstream.managedledger.generation.ManagedLedgerPhysicalDeletionActivationResult.Status;
import com.nereusstream.materialization.gc.PhysicalRootBackfillCoordinator;
import com.nereusstream.materialization.gc.PhysicalRootBackfillFailure;
import com.nereusstream.materialization.gc.PhysicalRootBackfillReport;
import com.nereusstream.materialization.gc.PhysicalRootBackfillRequest;
import com.nereusstream.materialization.gc.PhysicalRootBackfillStage;
import com.nereusstream.metadata.oxia.F4MetadataConditionFailedException;
import com.nereusstream.metadata.oxia.F4MetadataTestValues;
import com.nereusstream.metadata.oxia.GenerationProtocolActivationStore;
import com.nereusstream.metadata.oxia.GenerationProtocolActivationStoreTestFactory;
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
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class Phase4PhysicalDeletionActivationCoordinatorTest {
    private static final String CLUSTER = F4MetadataTestValues.CLUSTER;
    private static final String RUN_ID = "r".repeat(26);
    private static final long READINESS_EPOCH = 7;
    private static final String REGISTRATION_COVERAGE = F4MetadataTestValues.HASH_A;
    private static final String PHYSICAL_COVERAGE = F4MetadataTestValues.HASH_B;
    private static final String CURSOR_COVERAGE = F4MetadataTestValues.HASH_C;
    private static final String CAPABILITY = F4MetadataTestValues.HASH_D;
    private static final String PROBE_KEY = F4MetadataTestValues.HASH_E;
    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private static final Clock CLOCK = Clock.fixed(
            Instant.ofEpochMilli(2_000), ZoneOffset.UTC);
    private static final List<ReferenceDomainVersionRecord> DOMAINS =
            F4MetadataTestValues.referenceDomains();
    private static final GenerationCapabilityReadiness READINESS =
            new GenerationCapabilityReadiness(
                    READINESS_EPOCH,
                    sha256(F4MetadataTestValues.HASH_E),
                    2);

    @Test
    void backfillsThenProbesThenAtomicallyEnablesBothDeletionBits() {
        try (GenerationProtocolActivationStore store = store()) {
            seedPublication(store, "");
            MutableReadinessProvider readiness = new MutableReadinessProvider(READINESS);
            List<String> calls = new ArrayList<>();
            PhysicalRootBackfillCoordinator backfill = request -> {
                calls.add("backfill");
                return installCoverage(store, request.runId())
                        .thenApply(ignored -> successfulReport(request.runId()));
            };
            ObjectStoreDeleteCapabilityProbe probe = probe(calls, null);
            var coordinator = coordinator(
                    true, readiness, store, backfill, probe);

            var result = coordinator.activate(request()).join();

            assertThat(calls).containsExactly("backfill", "probe");
            assertThat(result.status()).isEqualTo(Status.ACTIVATED);
            assertThat(result.brokerReadinessEpoch()).isEqualTo(READINESS_EPOCH);
            assertThat(result.physicalRootCoverageSha256())
                    .isEqualTo(PHYSICAL_COVERAGE);
            assertThat(result.cursorSnapshotCoverageSha256())
                    .isEqualTo(CURSOR_COVERAGE);
            assertThat(result.objectStoreCapabilitySha256())
                    .isEqualTo(CAPABILITY);

            GenerationProtocolActivationRecord installed = store.get(CLUSTER)
                    .join()
                    .orElseThrow()
                    .value();
            assertThat(installed.physicalDeleteEnabled()).isTrue();
            assertThat(installed.cursorSnapshotDeleteEnabled()).isTrue();
            assertThat(installed.objectStoreCapabilitySha256())
                    .isEqualTo(CAPABILITY);
            assertThat(installed.physicalRootBackfill().coverageSha256())
                    .isEqualTo(PHYSICAL_COVERAGE);
            assertThat(installed.cursorSnapshotBackfill().coverageSha256())
                    .isEqualTo(CURSOR_COVERAGE);
        }
    }

    @Test
    void alreadyActiveSkipsBackfillAndCapabilityCanary() {
        try (GenerationProtocolActivationStore store = store()) {
            seedDeletion(store, CAPABILITY);
            AtomicInteger backfills = new AtomicInteger();
            AtomicInteger probes = new AtomicInteger();
            var coordinator = coordinator(
                    true,
                    new MutableReadinessProvider(READINESS),
                    store,
                    request -> {
                        backfills.incrementAndGet();
                        return CompletableFuture.completedFuture(
                                successfulReport(request.runId()));
                    },
                    new ObjectStoreDeleteCapabilityProbe() {
                        @Override
                        public String expectedCapabilitySha256() {
                            return CAPABILITY;
                        }

                        @Override
                        public CompletableFuture<ObjectStoreDeleteCapabilityProof> probe(
                                ObjectStoreDeleteCapabilityRequest request) {
                            probes.incrementAndGet();
                            return CompletableFuture.completedFuture(
                                    capabilityProof());
                        }
                    });

            var result = coordinator.activate(request()).join();

            assertThat(result.status()).isEqualTo(Status.ALREADY_ACTIVE);
            assertThat(backfills).hasValue(0);
            assertThat(probes).hasValue(0);
        }
    }

    @Test
    void deletionActiveReadinessRolloverReplacesEveryProofInOneCas() {
        try (GenerationProtocolActivationStore store = store()) {
            seedDeletion(store, CAPABILITY);
            VersionedGenerationProtocolActivation old = store.get(CLUSTER)
                    .join()
                    .orElseThrow();
            GenerationCapabilityReadiness nextReadiness =
                    new GenerationCapabilityReadiness(
                            READINESS_EPOCH - 1,
                            sha256(F4MetadataTestValues.HASH_A),
                            2);
            MutableReadinessProvider readiness =
                    new MutableReadinessProvider(nextReadiness);
            List<String> calls = new ArrayList<>();
            PhysicalRootBackfillCoordinator backfill =
                    new PhysicalRootBackfillCoordinator() {
                        @Override
                        public CompletableFuture<PhysicalRootBackfillReport> run(
                                PhysicalRootBackfillRequest request) {
                            return CompletableFuture.failedFuture(
                                    new AssertionError(
                                            "ordinary activation backfill must not run"));
                        }

                        @Override
                        public CompletableFuture<PhysicalRootBackfillReport>
                                runRollover(
                                        PhysicalRootBackfillRequest request,
                                        VersionedGenerationProtocolActivation
                                                expectedCurrent) {
                            calls.add("rollover-backfill");
                            assertThat(expectedCurrent).isEqualTo(old);
                            assertThat(store.get(CLUSTER).join())
                                    .contains(old);
                            return CompletableFuture.completedFuture(
                                    successfulReport(
                                            request.runId(),
                                            request.expectedBrokerReadinessEpoch()));
                        }
                    };
            var coordinator = coordinator(
                    true,
                    readiness,
                    store,
                    backfill,
                    probe(calls, null));
            GenerationRegistrationBackfillCompletion registration =
                    new GenerationRegistrationBackfillCompletion(
                            RUN_ID,
                            nextReadiness,
                            sha256(F4MetadataTestValues.HASH_E),
                            0);

            VersionedGenerationProtocolActivation installed = coordinator
                    .rollover(registration, 4, TIMEOUT, old)
                    .join();

            assertThat(calls).containsExactly("rollover-backfill", "probe");
            GenerationProtocolActivationRecord value = installed.value();
            assertThat(value.brokerCapabilityReadinessEpoch())
                    .isEqualTo(nextReadiness.brokerReadinessEpoch());
            assertThat(value.physicalDeleteEnabled()).isTrue();
            assertThat(value.cursorSnapshotDeleteEnabled()).isTrue();
            assertThat(value.streamRegistrationBackfill().coverageSha256())
                    .isEqualTo(F4MetadataTestValues.HASH_E);
            assertThat(value.physicalRootBackfill().coverageSha256())
                    .isEqualTo(PHYSICAL_COVERAGE);
            assertThat(value.cursorSnapshotBackfill().coverageSha256())
                    .isEqualTo(CURSOR_COVERAGE);
            assertThat(value.streamRegistrationBackfill()
                            .brokerReadinessEpoch())
                    .isEqualTo(nextReadiness.brokerReadinessEpoch());
            assertThat(value.physicalRootBackfill().brokerReadinessEpoch())
                    .isEqualTo(nextReadiness.brokerReadinessEpoch());
            assertThat(value.cursorSnapshotBackfill().brokerReadinessEpoch())
                    .isEqualTo(nextReadiness.brokerReadinessEpoch());
            assertThat(value.objectStoreCapabilitySha256())
                    .isEqualTo(CAPABILITY);
            assertThat(store.get(CLUSTER).join())
                    .contains(installed);
        }
    }

    @Test
    void failedBackfillNeverRunsCanaryOrChangesCapabilityBits() {
        try (GenerationProtocolActivationStore store = store()) {
            seedPublication(store, "");
            AtomicInteger probes = new AtomicInteger();
            PhysicalRootBackfillCoordinator failed = request ->
                    CompletableFuture.completedFuture(failedReport(request.runId()));
            ObjectStoreDeleteCapabilityProbe probe = new ObjectStoreDeleteCapabilityProbe() {
                @Override
                public String expectedCapabilitySha256() {
                    return CAPABILITY;
                }

                @Override
                public CompletableFuture<ObjectStoreDeleteCapabilityProof> probe(
                        ObjectStoreDeleteCapabilityRequest request) {
                    probes.incrementAndGet();
                    return CompletableFuture.completedFuture(capabilityProof());
                }
            };
            var coordinator = coordinator(
                    true,
                    new MutableReadinessProvider(READINESS),
                    store,
                    failed,
                    probe);

            assertThatThrownBy(() -> coordinator.activate(request()).join())
                    .satisfies(error -> assertThat(unwrap(error).code())
                            .isEqualTo(ErrorCode.METADATA_CONDITION_FAILED));
            assertThat(probes).hasValue(0);
            GenerationProtocolActivationRecord current = store.get(CLUSTER)
                    .join()
                    .orElseThrow()
                    .value();
            assertThat(current.physicalDeleteEnabled()).isFalse();
            assertThat(current.cursorSnapshotDeleteEnabled()).isFalse();
            assertThat(current.objectStoreCapabilitySha256()).isEmpty();
        }
    }

    @Test
    void readinessDriftAfterCanaryPreventsActivationCas() {
        try (GenerationProtocolActivationStore store = store()) {
            seedPublication(store, "");
            MutableReadinessProvider readiness = new MutableReadinessProvider(READINESS);
            PhysicalRootBackfillCoordinator backfill = request ->
                    installCoverage(store, request.runId())
                            .thenApply(ignored -> successfulReport(request.runId()));
            ObjectStoreDeleteCapabilityProbe probe = probe(
                    new ArrayList<>(), readiness::invalidate);
            var coordinator = coordinator(
                    true, readiness, store, backfill, probe);

            assertThatThrownBy(() -> coordinator.activate(request()).join())
                    .satisfies(error -> assertThat(unwrap(error).code())
                            .isEqualTo(ErrorCode.METADATA_CONDITION_FAILED));
            GenerationProtocolActivationRecord current = store.get(CLUSTER)
                    .join()
                    .orElseThrow()
                    .value();
            assertThat(current.physicalDeleteEnabled()).isFalse();
            assertThat(current.cursorSnapshotDeleteEnabled()).isFalse();
            assertThat(current.objectStoreCapabilitySha256()).isEmpty();
        }
    }

    @Test
    void lostActivationCasResponseConvergesFromExactDurableAuthority() {
        try (GenerationProtocolActivationStore delegate = store()) {
            seedPublication(delegate, "");
            PhysicalRootBackfillCoordinator backfill = request ->
                    installCoverage(delegate, request.runId())
                            .thenApply(ignored -> successfulReport(request.runId()));
            LostResponseActivationStore store = new LostResponseActivationStore(delegate);
            var coordinator = coordinator(
                    true,
                    new MutableReadinessProvider(READINESS),
                    store,
                    backfill,
                    probe(new ArrayList<>(), null));

            var result = coordinator.activate(request()).join();

            assertThat(store.lostResponse).isTrue();
            assertThat(result.status()).isEqualTo(Status.ACTIVATED);
            assertThat(delegate.get(CLUSTER)
                            .join()
                            .orElseThrow()
                            .value()
                            .physicalDeleteEnabled())
                    .isTrue();
        }
    }

    @Test
    void conditionConflictRetriesWithoutRepeatingBackfillOrCanary() {
        try (GenerationProtocolActivationStore delegate = store()) {
            seedPublication(delegate, "");
            AtomicInteger backfills = new AtomicInteger();
            AtomicInteger probes = new AtomicInteger();
            PhysicalRootBackfillCoordinator backfill = request -> {
                backfills.incrementAndGet();
                return installCoverage(delegate, request.runId())
                        .thenApply(ignored -> successfulReport(request.runId()));
            };
            ConflictOnceActivationStore store = new ConflictOnceActivationStore(delegate);
            ObjectStoreDeleteCapabilityProbe probe = new ObjectStoreDeleteCapabilityProbe() {
                @Override
                public String expectedCapabilitySha256() {
                    return CAPABILITY;
                }

                @Override
                public CompletableFuture<ObjectStoreDeleteCapabilityProof> probe(
                        ObjectStoreDeleteCapabilityRequest request) {
                    probes.incrementAndGet();
                    return CompletableFuture.completedFuture(capabilityProof());
                }
            };
            var coordinator = coordinator(
                    true,
                    new MutableReadinessProvider(READINESS),
                    store,
                    backfill,
                    probe);

            var result = coordinator.activate(request()).join();

            assertThat(result.status()).isEqualTo(Status.ACTIVATED);
            assertThat(store.compareAttempts).hasValue(2);
            assertThat(backfills).hasValue(1);
            assertThat(probes).hasValue(1);
        }
    }

    @Test
    void disabledActivationAndSameEpochScopeMismatchFailClosed() {
        try (GenerationProtocolActivationStore disabledStore = store();
                GenerationProtocolActivationStore mismatchStore = store()) {
            seedPublication(disabledStore, "");
            seedPublication(mismatchStore, F4MetadataTestValues.HASH_E);
            AtomicInteger work = new AtomicInteger();
            PhysicalRootBackfillCoordinator backfill = request -> {
                work.incrementAndGet();
                return CompletableFuture.completedFuture(
                        successfulReport(request.runId()));
            };

            var disabled = coordinator(
                    false,
                    new MutableReadinessProvider(READINESS),
                    disabledStore,
                    backfill,
                    probe(new ArrayList<>(), null));
            assertThatThrownBy(() -> disabled.activate(request()).join())
                    .satisfies(error -> assertThat(unwrap(error).code())
                            .isEqualTo(ErrorCode.METADATA_CONDITION_FAILED));

            var mismatch = coordinator(
                    true,
                    new MutableReadinessProvider(READINESS),
                    mismatchStore,
                    backfill,
                    probe(new ArrayList<>(), null));
            assertThatThrownBy(() -> mismatch.activate(request()).join())
                    .satisfies(error -> assertThat(unwrap(error).code())
                            .isEqualTo(ErrorCode.METADATA_INVARIANT_VIOLATION));
            assertThat(work).hasValue(0);
        }
    }

    private static DefaultPhase4PhysicalDeletionActivationCoordinator coordinator(
            boolean enabled,
            GenerationCapabilityReadinessProvider readiness,
            GenerationProtocolActivationStore store,
            PhysicalRootBackfillCoordinator backfill,
            ObjectStoreDeleteCapabilityProbe probe) {
        return new DefaultPhase4PhysicalDeletionActivationCoordinator(
                CLUSTER,
                enabled,
                readiness,
                store,
                DOMAINS,
                backfill,
                probe,
                CLOCK);
    }

    private static ManagedLedgerPhysicalDeletionActivationRequest request() {
        return new ManagedLedgerPhysicalDeletionActivationRequest(
                RUN_ID, 4, TIMEOUT);
    }

    private static GenerationProtocolActivationStore store() {
        return GenerationProtocolActivationStoreTestFactory.inMemory(
                CLOCK, F4MetadataTestValues.PROCESS, DOMAINS);
    }

    private static void seedPublication(
            GenerationProtocolActivationStore store,
            String capabilitySha256) {
        VersionedGenerationProtocolActivation current = store
                .getOrCreate(CLUSTER)
                .join();
        store.compareAndSet(
                        CLUSTER,
                        active(
                                current.value(),
                                false,
                                capabilitySha256,
                                GenerationBackfillProofRecord.incomplete(
                                        READINESS_EPOCH),
                                GenerationBackfillProofRecord.incomplete(
                                        READINESS_EPOCH)),
                        current.metadataVersion())
                .join();
    }

    private static void seedDeletion(
            GenerationProtocolActivationStore store,
            String capabilitySha256) {
        VersionedGenerationProtocolActivation current = store
                .getOrCreate(CLUSTER)
                .join();
        store.compareAndSet(
                        CLUSTER,
                        active(
                                current.value(),
                                true,
                                capabilitySha256,
                                complete(PHYSICAL_COVERAGE, 2_103),
                                complete(CURSOR_COVERAGE, 2_104)),
                        current.metadataVersion())
                .join();
    }

    private static CompletableFuture<VersionedGenerationProtocolActivation>
            installCoverage(
                    GenerationProtocolActivationStore store,
                    String runId) {
        return store.get(CLUSTER).thenCompose(optional -> {
            VersionedGenerationProtocolActivation current =
                    optional.orElseThrow();
            GenerationProtocolActivationRecord value = current.value();
            GenerationProtocolActivationRecord replacement =
                    new GenerationProtocolActivationRecord(
                            value.schemaVersion(),
                            value.protocolVersion(),
                            value.lifecycle(),
                            value.publicationEnabled(),
                            value.physicalDeleteEnabled(),
                            value.cursorSnapshotDeleteEnabled(),
                            value.brokerCapabilityReadinessEpoch(),
                            value.requiredReferenceDomains(),
                            value.streamRegistrationBackfill(),
                            new GenerationBackfillProofRecord(
                                    runId,
                                    READINESS_EPOCH,
                                    PHYSICAL_COVERAGE,
                                    true,
                                    2_103),
                            new GenerationBackfillProofRecord(
                                    runId,
                                    READINESS_EPOCH,
                                    CURSOR_COVERAGE,
                                    true,
                                    2_104),
                            value.objectStoreCapabilitySha256(),
                            value.activatingBrokerRunId(),
                            value.preparedAtMillis(),
                            value.activatedAtMillis(),
                            2_106,
                            0);
            return store.compareAndSet(
                    CLUSTER, replacement, current.metadataVersion());
        });
    }

    private static GenerationProtocolActivationRecord active(
            GenerationProtocolActivationRecord current,
            boolean deletion,
            String capabilitySha256,
            GenerationBackfillProofRecord physical,
            GenerationBackfillProofRecord cursor) {
        return new GenerationProtocolActivationRecord(
                current.schemaVersion(),
                current.protocolVersion(),
                GenerationProtocolActivationLifecycle.ACTIVE,
                true,
                deletion,
                deletion,
                READINESS_EPOCH,
                current.requiredReferenceDomains(),
                new GenerationBackfillProofRecord(
                        F4MetadataTestValues.ATTEMPT,
                        READINESS_EPOCH,
                        REGISTRATION_COVERAGE,
                        true,
                        2_102),
                physical,
                cursor,
                capabilitySha256,
                current.activatingBrokerRunId(),
                current.preparedAtMillis(),
                2_101,
                2_105,
                0);
    }

    private static GenerationBackfillProofRecord complete(
            String coverage, long completedAt) {
        return new GenerationBackfillProofRecord(
                RUN_ID,
                READINESS_EPOCH,
                coverage,
                true,
                completedAt);
    }

    private static PhysicalRootBackfillReport successfulReport(String runId) {
        return successfulReport(runId, READINESS_EPOCH);
    }

    private static PhysicalRootBackfillReport successfulReport(
            String runId,
            long readinessEpoch) {
        return new PhysicalRootBackfillReport(
                runId,
                readinessEpoch,
                3,
                4,
                2,
                6,
                6,
                0,
                sha256(PHYSICAL_COVERAGE),
                sha256(CURSOR_COVERAGE),
                List.of());
    }

    private static PhysicalRootBackfillReport failedReport(String runId) {
        return new PhysicalRootBackfillReport(
                runId,
                READINESS_EPOCH,
                1,
                0,
                0,
                0,
                0,
                1,
                sha256(PHYSICAL_COVERAGE),
                sha256(CURSOR_COVERAGE),
                List.of(new PhysicalRootBackfillFailure(
                        F4MetadataTestValues.HASH_E,
                        PhysicalRootBackfillStage.OBJECT_HEAD,
                        "OBJECT_HEAD_FAILED")));
    }

    private static ObjectStoreDeleteCapabilityProbe probe(
            List<String> calls, Runnable afterProbe) {
        return new ObjectStoreDeleteCapabilityProbe() {
            @Override
            public String expectedCapabilitySha256() {
                return CAPABILITY;
            }

            @Override
            public CompletableFuture<ObjectStoreDeleteCapabilityProof> probe(
                    ObjectStoreDeleteCapabilityRequest request) {
                calls.add("probe");
                if (afterProbe != null) {
                    afterProbe.run();
                }
                return CompletableFuture.completedFuture(capabilityProof());
            }
        };
    }

    private static ObjectStoreDeleteCapabilityProof capabilityProof() {
        return new ObjectStoreDeleteCapabilityProof(
                ObjectStoreDeleteCapabilityProof.PROTOCOL_VERSION,
                CAPABILITY,
                PROBE_KEY,
                2_200);
    }

    private static Checksum sha256(String value) {
        return new Checksum(ChecksumType.SHA256, value);
    }

    private static NereusException unwrap(Throwable supplied) {
        Throwable failure = supplied;
        while (failure instanceof CompletionException
                && failure.getCause() != null) {
            failure = failure.getCause();
        }
        assertThat(failure).isInstanceOf(NereusException.class);
        return (NereusException) failure;
    }

    private static final class MutableReadinessProvider
            implements GenerationCapabilityReadinessProvider {
        private final AtomicReference<Optional<GenerationCapabilityReadiness>> current;

        private MutableReadinessProvider(GenerationCapabilityReadiness initial) {
            current = new AtomicReference<>(Optional.of(initial));
        }

        @Override
        public CompletableFuture<GenerationCapabilityReadiness>
                requireGenerationCapabilityReadiness() {
            return current.get()
                    .map(CompletableFuture::completedFuture)
                    .orElseGet(() -> CompletableFuture.failedFuture(
                            new IllegalStateException("readiness unavailable")));
        }

        @Override
        public Optional<GenerationCapabilityReadiness>
                currentGenerationCapabilityReadiness() {
            return current.get();
        }

        private void invalidate() {
            current.set(Optional.empty());
        }
    }

    private abstract static class DelegatingActivationStore
            implements GenerationProtocolActivationStore {
        private final GenerationProtocolActivationStore delegate;

        private DelegatingActivationStore(
                GenerationProtocolActivationStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletableFuture<Optional<VersionedGenerationProtocolActivation>> get(
                String cluster) {
            return delegate.get(cluster);
        }

        @Override
        public CompletableFuture<VersionedGenerationProtocolActivation> getOrCreate(
                String cluster) {
            return delegate.getOrCreate(cluster);
        }

        @Override
        public CompletableFuture<VersionedGenerationProtocolActivation> compareAndSet(
                String cluster,
                GenerationProtocolActivationRecord replacement,
                long expectedVersion) {
            return delegate.compareAndSet(
                    cluster, replacement, expectedVersion);
        }

        @Override
        public void close() {
            // The test owns the delegate.
        }
    }

    private static final class LostResponseActivationStore
            extends DelegatingActivationStore {
        private final AtomicBoolean lostResponse = new AtomicBoolean();

        private LostResponseActivationStore(
                GenerationProtocolActivationStore delegate) {
            super(delegate);
        }

        @Override
        public CompletableFuture<VersionedGenerationProtocolActivation> compareAndSet(
                String cluster,
                GenerationProtocolActivationRecord replacement,
                long expectedVersion) {
            return super.compareAndSet(cluster, replacement, expectedVersion)
                    .thenCompose(updated -> {
                        if (lostResponse.compareAndSet(false, true)) {
                            return CompletableFuture.failedFuture(
                                    new NereusException(
                                            ErrorCode.METADATA_CONDITION_FAILED,
                                            true,
                                            "lost activation CAS response"));
                        }
                        return CompletableFuture.completedFuture(updated);
                    });
        }
    }

    private static final class ConflictOnceActivationStore
            extends DelegatingActivationStore {
        private final AtomicInteger compareAttempts = new AtomicInteger();

        private ConflictOnceActivationStore(
                GenerationProtocolActivationStore delegate) {
            super(delegate);
        }

        @Override
        public CompletableFuture<VersionedGenerationProtocolActivation> compareAndSet(
                String cluster,
                GenerationProtocolActivationRecord replacement,
                long expectedVersion) {
            if (compareAttempts.incrementAndGet() == 1) {
                return CompletableFuture.failedFuture(
                        new F4MetadataConditionFailedException(
                                "synthetic activation CAS conflict"));
            }
            return super.compareAndSet(
                    cluster, replacement, expectedVersion);
        }
    }
}
