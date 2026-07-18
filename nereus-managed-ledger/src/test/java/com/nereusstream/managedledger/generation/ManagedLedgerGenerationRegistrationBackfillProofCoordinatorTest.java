/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.generation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.core.capability.GenerationCapabilityReadiness;
import com.nereusstream.core.capability.GenerationCapabilityReadinessProvider;
import com.nereusstream.core.capability.GenerationRegistrationBackfillCompletion;
import com.nereusstream.metadata.oxia.F4MetadataTestValues;
import com.nereusstream.metadata.oxia.GenerationProtocolActivationStore;
import com.nereusstream.metadata.oxia.GenerationProtocolActivationStoreTestFactory;
import com.nereusstream.metadata.oxia.VersionedGenerationProtocolActivation;
import com.nereusstream.metadata.oxia.records.GenerationBackfillProofRecord;
import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationLifecycle;
import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationRecord;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class ManagedLedgerGenerationRegistrationBackfillProofCoordinatorTest {
    private static final String CLUSTER = F4MetadataTestValues.CLUSTER;
    private static final String RUN_ID = "abcdefghijklmnopqrstuvwxyz";
    private static final Checksum COVERAGE =
            sha("11");

    @Test
    void installsExactProofAndConvergesAnIdempotentRerun() {
        MutableReadinessProvider readiness =
                new MutableReadinessProvider(readiness(7, "22"));
        try (GenerationProtocolActivationStore store = activationStore()) {
            var coordinator = coordinator(store, readiness);

            coordinator.complete(completion(readiness.value(), COVERAGE, 0))
                    .join();
            VersionedGenerationProtocolActivation first =
                    store.get(CLUSTER).join().orElseThrow();

            assertThat(first.value().brokerCapabilityReadinessEpoch())
                    .isEqualTo(7);
            assertThat(first.value().streamRegistrationBackfill().complete())
                    .isTrue();
            assertThat(first.value().streamRegistrationBackfill().runId())
                    .isEqualTo(RUN_ID);
            assertThat(first.value().streamRegistrationBackfill().coverageSha256())
                    .isEqualTo(COVERAGE.value());
            assertThat(first.value().physicalRootBackfill())
                    .isEqualTo(com.nereusstream.metadata.oxia.records
                            .GenerationBackfillProofRecord.incomplete(7));
            assertThat(first.value().cursorSnapshotBackfill())
                    .isEqualTo(com.nereusstream.metadata.oxia.records
                            .GenerationBackfillProofRecord.incomplete(7));

            coordinator.complete(new GenerationRegistrationBackfillCompletion(
                            "bcdefghijklmnopqrstuvwxyza",
                            readiness.value(),
                            COVERAGE,
                            0))
                    .join();
            assertThat(store.get(CLUSTER).join())
                    .contains(first);
        }
    }

    @Test
    void rejectsFailuresAndReadinessDriftBeforeCreatingAuthority() {
        MutableReadinessProvider readiness =
                new MutableReadinessProvider(readiness(7, "33"));
        try (GenerationProtocolActivationStore store = activationStore()) {
            var coordinator = coordinator(store, readiness);

            assertConditionFailure(() -> coordinator
                    .complete(completion(readiness.value(), COVERAGE, 1))
                    .join());
            assertThat(store.get(CLUSTER).join()).isEmpty();

            GenerationCapabilityReadiness stale = readiness.value();
            readiness.value = readiness(7, "44");
            assertConditionFailure(() -> coordinator
                    .complete(completion(stale, COVERAGE, 0))
                    .join());
            assertThat(store.get(CLUSTER).join()).isEmpty();
        }
    }

    @Test
    void completedCoverageIsImmutableWithinOneReadinessEpoch() {
        MutableReadinessProvider readiness =
                new MutableReadinessProvider(readiness(7, "55"));
        try (GenerationProtocolActivationStore store = activationStore()) {
            var coordinator = coordinator(store, readiness);
            coordinator.complete(completion(readiness.value(), COVERAGE, 0))
                    .join();

            assertInvariant(() -> coordinator
                    .complete(new GenerationRegistrationBackfillCompletion(
                            "bcdefghijklmnopqrstuvwxyza",
                            readiness.value(),
                            sha("66"),
                            0))
                    .join());
        }
    }

    @Test
    void newerReadinessRefreshesProofAndInvalidatesOtherEpochFacts() {
        MutableReadinessProvider readiness =
                new MutableReadinessProvider(readiness(7, "77"));
        try (GenerationProtocolActivationStore store = activationStore()) {
            var coordinator = coordinator(store, readiness);
            coordinator.complete(completion(readiness.value(), COVERAGE, 0))
                    .join();

            readiness.value = readiness(8, "88");
            Checksum nextCoverage = sha("99");
            coordinator.complete(new GenerationRegistrationBackfillCompletion(
                            "bcdefghijklmnopqrstuvwxyza",
                            readiness.value,
                            nextCoverage,
                            0))
                    .join();

            GenerationProtocolActivationRecord current =
                    store.get(CLUSTER).join().orElseThrow().value();
            assertThat(current.brokerCapabilityReadinessEpoch()).isEqualTo(8);
            assertThat(current.streamRegistrationBackfill().coverageSha256())
                    .isEqualTo(nextCoverage.value());
            assertThat(current.physicalRootBackfill().complete()).isFalse();
            assertThat(current.physicalRootBackfill().brokerReadinessEpoch())
                    .isEqualTo(8);
            assertThat(current.cursorSnapshotBackfill().complete()).isFalse();
            assertThat(current.cursorSnapshotBackfill().brokerReadinessEpoch())
                    .isEqualTo(8);
            assertThat(current.objectStoreCapabilitySha256()).isEmpty();
        }
    }

    @Test
    void lostCasResponseConvergesFromTheExactDurableCoverage() {
        MutableReadinessProvider readiness =
                new MutableReadinessProvider(readiness(7, "aa"));
        GenerationProtocolActivationStore durable = activationStore();
        AtomicBoolean lost = new AtomicBoolean();
        GenerationProtocolActivationStore lossy =
                loseFirstSuccessfulCasResponse(durable, lost);
        try {
            coordinator(lossy, readiness)
                    .complete(completion(readiness.value(), COVERAGE, 0))
                    .join();

            assertThat(lost).isTrue();
            assertThat(durable.get(CLUSTER).join()
                            .orElseThrow()
                            .value()
                            .streamRegistrationBackfill()
                            .coverageSha256())
                    .isEqualTo(COVERAGE.value());
        } finally {
            durable.close();
        }
    }

    @Test
    void readinessInvalidationAfterCasFailsTheCallerButLeavesSafeOldEpochProof() {
        MutableReadinessProvider readiness =
                new MutableReadinessProvider(readiness(7, "bb"));
        readiness.invalidateAfterRequire = true;
        try (GenerationProtocolActivationStore store = activationStore()) {
            var coordinator = coordinator(store, readiness);

            assertConditionFailure(() -> coordinator
                    .complete(completion(readiness.value(), COVERAGE, 0))
                    .join());

            assertThat(store.get(CLUSTER).join()
                            .orElseThrow()
                            .value()
                            .streamRegistrationBackfill()
                            .complete())
                    .isTrue();
        }
    }

    @Test
    void deletionActiveEpochDelegatesOneBoundedAtomicRollover() {
        MutableReadinessProvider readiness =
                new MutableReadinessProvider(readiness(8, "cc"));
        try (GenerationProtocolActivationStore store = activationStore()) {
            VersionedGenerationProtocolActivation old = seedDeletion(store, 7);
            AtomicInteger concurrency = new AtomicInteger();
            AtomicReference<Duration> timeout = new AtomicReference<>();
            ManagedLedgerGenerationReadinessRolloverCoordinator rollover =
                    (registration, maxConcurrentStreams, suppliedTimeout, current) -> {
                        assertThat(current).isEqualTo(old);
                        concurrency.set(maxConcurrentStreams);
                        timeout.set(suppliedTimeout);
                        GenerationProtocolActivationRecord value = current.value();
                        long epoch = registration.readiness()
                                .brokerReadinessEpoch();
                        GenerationProtocolActivationRecord replacement =
                                new GenerationProtocolActivationRecord(
                                        value.schemaVersion(),
                                        value.protocolVersion(),
                                        value.lifecycle(),
                                        value.publicationEnabled(),
                                        value.physicalDeleteEnabled(),
                                        value.cursorSnapshotDeleteEnabled(),
                                        epoch,
                                        value.requiredReferenceDomains(),
                                        completeProof(
                                                registration.runId(),
                                                epoch,
                                                registration.coverageSha256()
                                                        .value(),
                                                1_100),
                                        completeProof(
                                                registration.runId(),
                                                epoch,
                                                sha("dd").value(),
                                                1_101),
                                        completeProof(
                                                registration.runId(),
                                                epoch,
                                                sha("ee").value(),
                                                1_102),
                                        value.objectStoreCapabilitySha256(),
                                        value.activatingBrokerRunId(),
                                        value.preparedAtMillis(),
                                        value.activatedAtMillis(),
                                        1_103,
                                        0);
                        return store.compareAndSet(
                                CLUSTER,
                                replacement,
                                current.metadataVersion());
                    };
            var coordinator =
                    new DefaultManagedLedgerGenerationRegistrationBackfillProofCoordinator(
                            CLUSTER,
                            store,
                            readiness,
                            F4MetadataTestValues.referenceDomains(),
                            rollover,
                            Clock.fixed(
                                    Instant.ofEpochMilli(1_000),
                                    ZoneOffset.UTC));
            Duration deadline = Duration.ofSeconds(17);

            coordinator.complete(
                            completion(readiness.value(), sha("ff"), 0),
                            23,
                            deadline)
                    .join();

            assertThat(concurrency).hasValue(23);
            assertThat(timeout).hasValue(deadline);
            GenerationProtocolActivationRecord installed = store.get(CLUSTER)
                    .join()
                    .orElseThrow()
                    .value();
            assertThat(installed.brokerCapabilityReadinessEpoch()).isEqualTo(8);
            assertThat(installed.physicalDeleteEnabled()).isTrue();
            assertThat(installed.cursorSnapshotDeleteEnabled()).isTrue();
            assertThat(installed.streamRegistrationBackfill().coverageSha256())
                    .isEqualTo(sha("ff").value());
        }
    }

    private static DefaultManagedLedgerGenerationRegistrationBackfillProofCoordinator
            coordinator(
                    GenerationProtocolActivationStore store,
                    GenerationCapabilityReadinessProvider readiness) {
        return new DefaultManagedLedgerGenerationRegistrationBackfillProofCoordinator(
                CLUSTER,
                store,
                readiness,
                F4MetadataTestValues.referenceDomains(),
                Clock.fixed(
                        Instant.ofEpochMilli(1_000),
                        ZoneOffset.UTC));
    }

    private static GenerationProtocolActivationStore activationStore() {
        return GenerationProtocolActivationStoreTestFactory.inMemory(
                Clock.fixed(
                        Instant.ofEpochMilli(900),
                        ZoneOffset.UTC),
                F4MetadataTestValues.PROCESS,
                F4MetadataTestValues.referenceDomains());
    }

    private static VersionedGenerationProtocolActivation seedDeletion(
            GenerationProtocolActivationStore store,
            long epoch) {
        VersionedGenerationProtocolActivation current =
                store.getOrCreate(CLUSTER).join();
        GenerationProtocolActivationRecord value = current.value();
        GenerationProtocolActivationRecord replacement =
                new GenerationProtocolActivationRecord(
                        value.schemaVersion(),
                        value.protocolVersion(),
                        GenerationProtocolActivationLifecycle.ACTIVE,
                        true,
                        true,
                        true,
                        epoch,
                        value.requiredReferenceDomains(),
                        completeProof(RUN_ID, epoch, sha("10").value(), 950),
                        completeProof(RUN_ID, epoch, sha("20").value(), 951),
                        completeProof(RUN_ID, epoch, sha("30").value(), 952),
                        sha("40").value(),
                        value.activatingBrokerRunId(),
                        value.preparedAtMillis(),
                        940,
                        960,
                        0);
        return store.compareAndSet(
                        CLUSTER,
                        replacement,
                        current.metadataVersion())
                .join();
    }

    private static GenerationBackfillProofRecord completeProof(
            String runId,
            long epoch,
            String coverage,
            long completedAtMillis) {
        return new GenerationBackfillProofRecord(
                runId,
                epoch,
                coverage,
                true,
                completedAtMillis);
    }

    private static GenerationRegistrationBackfillCompletion completion(
            GenerationCapabilityReadiness readiness,
            Checksum coverage,
            long failures) {
        return new GenerationRegistrationBackfillCompletion(
                RUN_ID, readiness, coverage, failures);
    }

    private static GenerationCapabilityReadiness readiness(
            long epoch,
            String pair) {
        return new GenerationCapabilityReadiness(
                epoch, sha(pair), 2);
    }

    private static Checksum sha(String pair) {
        return new Checksum(
                ChecksumType.SHA256, pair.repeat(32));
    }

    private static GenerationProtocolActivationStore
            loseFirstSuccessfulCasResponse(
                    GenerationProtocolActivationStore delegate,
                    AtomicBoolean lost) {
        return new GenerationProtocolActivationStore() {
            @Override
            public CompletableFuture<Optional<VersionedGenerationProtocolActivation>>
                    get(String cluster) {
                return delegate.get(cluster);
            }

            @Override
            public CompletableFuture<VersionedGenerationProtocolActivation>
                    getOrCreate(String cluster) {
                return delegate.getOrCreate(cluster);
            }

            @Override
            public CompletableFuture<VersionedGenerationProtocolActivation>
                    compareAndSet(
                            String cluster,
                            GenerationProtocolActivationRecord replacement,
                            long expectedVersion) {
                return delegate.compareAndSet(
                                cluster, replacement, expectedVersion)
                        .thenCompose(result -> {
                            if (lost.compareAndSet(false, true)) {
                                return CompletableFuture.failedFuture(
                                        new IllegalStateException(
                                                "lost proof CAS response"));
                            }
                            return CompletableFuture.completedFuture(result);
                        });
            }

            @Override
            public void close() {
            }
        };
    }

    private static void assertConditionFailure(Runnable operation) {
        assertThatThrownBy(operation::run)
                .satisfies(failure -> {
                    Throwable exact = unwrap(failure);
                    assertThat(exact).isInstanceOf(NereusException.class);
                    assertThat(((NereusException) exact).code())
                            .isEqualTo(ErrorCode.METADATA_CONDITION_FAILED);
                });
    }

    private static void assertInvariant(Runnable operation) {
        assertThatThrownBy(operation::run)
                .satisfies(failure -> {
                    Throwable exact = unwrap(failure);
                    assertThat(exact).isInstanceOf(NereusException.class);
                    assertThat(((NereusException) exact).code())
                            .isEqualTo(ErrorCode.METADATA_INVARIANT_VIOLATION);
                });
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static final class MutableReadinessProvider
            implements GenerationCapabilityReadinessProvider {
        private GenerationCapabilityReadiness value;
        private boolean invalidateAfterRequire;

        private MutableReadinessProvider(
                GenerationCapabilityReadiness value) {
            this.value = value;
        }

        @Override
        public CompletableFuture<GenerationCapabilityReadiness>
                requireGenerationCapabilityReadiness() {
            GenerationCapabilityReadiness result = value;
            if (invalidateAfterRequire) {
                value = null;
            }
            return CompletableFuture.completedFuture(result);
        }

        @Override
        public Optional<GenerationCapabilityReadiness>
                currentGenerationCapabilityReadiness() {
            return Optional.ofNullable(value);
        }

        private GenerationCapabilityReadiness value() {
            return value;
        }
    }
}
