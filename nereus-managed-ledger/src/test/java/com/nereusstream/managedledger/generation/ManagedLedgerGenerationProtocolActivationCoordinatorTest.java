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
import com.nereusstream.metadata.oxia.F4MetadataConditionFailedException;
import com.nereusstream.metadata.oxia.F4MetadataTestValues;
import com.nereusstream.metadata.oxia.GenerationProtocolActivationStore;
import com.nereusstream.metadata.oxia.GenerationProtocolActivationStoreTestFactory;
import com.nereusstream.metadata.oxia.VersionedGenerationProtocolActivation;
import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationLifecycle;
import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationRecord;
import com.nereusstream.metadata.oxia.records.ReferenceDomainVersionRecord;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class ManagedLedgerGenerationProtocolActivationCoordinatorTest {
    private static final String CLUSTER =
            F4MetadataTestValues.CLUSTER;
    private static final Clock STORE_CLOCK = Clock.fixed(
            Instant.ofEpochMilli(900), ZoneOffset.UTC);
    private static final Clock ACTIVATION_CLOCK = Clock.fixed(
            Instant.ofEpochMilli(1_000), ZoneOffset.UTC);
    private static final Checksum COVERAGE = sha("11");

    @Test
    void disabledFirstActivationDoesNotCreateRolloutAuthority() {
        MutableReadinessProvider readiness =
                new MutableReadinessProvider(readiness(7, "22"));
        try (GenerationProtocolActivationStore store =
                activationStore()) {
            assertConditionFailure(() -> coordinator(
                            store, readiness, false)
                    .activatePublication()
                    .join());
            assertThat(store.get(CLUSTER).join()).isEmpty();
        }
    }

    @Test
    void requiresCurrentCompletedRegistrationProofBeforeActiveCas() {
        MutableReadinessProvider readiness =
                new MutableReadinessProvider(readiness(7, "33"));
        try (GenerationProtocolActivationStore store =
                activationStore()) {
            assertConditionFailure(() -> coordinator(
                            store, readiness, true)
                    .activatePublication()
                    .join());
            assertThat(store.get(CLUSTER).join()
                            .orElseThrow()
                            .value()
                            .lifecycle())
                    .isEqualTo(
                            GenerationProtocolActivationLifecycle
                                    .PREPARED);
        }
    }

    @Test
    void activatesPublicationOnlyAndAnExistingActiveRecordIgnoresTheSwitch() {
        MutableReadinessProvider readiness =
                new MutableReadinessProvider(readiness(7, "44"));
        try (GenerationProtocolActivationStore store =
                activationStore()) {
            installProof(store, readiness);

            coordinator(store, readiness, true)
                    .activatePublication()
                    .join();
            VersionedGenerationProtocolActivation active =
                    store.get(CLUSTER).join().orElseThrow();
            assertThat(active.value().lifecycle())
                    .isEqualTo(
                            GenerationProtocolActivationLifecycle
                                    .ACTIVE);
            assertThat(active.value().publicationEnabled()).isTrue();
            assertThat(active.value().physicalDeleteEnabled())
                    .isFalse();
            assertThat(active.value().cursorSnapshotDeleteEnabled())
                    .isFalse();
            assertThat(active.value().activatedAtMillis())
                    .isEqualTo(1_000);

            coordinator(store, readiness, false)
                    .activatePublication()
                    .join();
            assertThat(store.get(CLUSTER).join())
                    .contains(active);
        }
    }

    @Test
    void lostCasResponseConvergesFromTheDurableActiveRecord() {
        MutableReadinessProvider readiness =
                new MutableReadinessProvider(readiness(7, "55"));
        GenerationProtocolActivationStore durable =
                activationStore();
        AtomicBoolean lost = new AtomicBoolean();
        try {
            installProof(durable, readiness);
            coordinator(
                            loseFirstSuccessfulCasResponse(
                                    durable, lost),
                            readiness,
                            true)
                    .activatePublication()
                    .join();

            assertThat(lost).isTrue();
            assertThat(durable.get(CLUSTER).join()
                            .orElseThrow()
                            .value()
                            .lifecycle())
                    .isEqualTo(
                            GenerationProtocolActivationLifecycle
                                    .ACTIVE);
        } finally {
            durable.close();
        }
    }

    @Test
    void conditionConflictRetriesAgainstTheReloadedPreparedRecord() {
        MutableReadinessProvider readiness =
                new MutableReadinessProvider(readiness(7, "66"));
        GenerationProtocolActivationStore durable =
                activationStore();
        AtomicBoolean conflicted = new AtomicBoolean();
        try {
            installProof(durable, readiness);
            coordinator(
                            failFirstCasWithCondition(
                                    durable, conflicted),
                            readiness,
                            true)
                    .activatePublication()
                    .join();

            assertThat(conflicted).isTrue();
            assertThat(durable.get(CLUSTER).join()
                            .orElseThrow()
                            .value()
                            .publicationEnabled())
                    .isTrue();
        } finally {
            durable.close();
        }
    }

    @Test
    void readinessInvalidationAfterCasFailsCallerButLeavesSafeActiveRecord() {
        MutableReadinessProvider readiness =
                new MutableReadinessProvider(readiness(7, "77"));
        try (GenerationProtocolActivationStore store =
                activationStore()) {
            installProof(store, readiness);
            readiness.invalidateAfterRequire = true;

            assertConditionFailure(() -> coordinator(
                            store, readiness, true)
                    .activatePublication()
                    .join());
            assertThat(store.get(CLUSTER).join()
                            .orElseThrow()
                            .value()
                            .lifecycle())
                    .isEqualTo(
                            GenerationProtocolActivationLifecycle
                                    .ACTIVE);
        }
    }

    @Test
    void readinessOrDomainDriftCannotActivateThePreparedRecord() {
        MutableReadinessProvider readiness =
                new MutableReadinessProvider(readiness(7, "88"));
        try (GenerationProtocolActivationStore store =
                activationStore()) {
            installProof(store, readiness);
            readiness.value = readiness(8, "99");

            assertConditionFailure(() -> coordinator(
                            store, readiness, true)
                    .activatePublication()
                    .join());
            assertThat(store.get(CLUSTER).join()
                            .orElseThrow()
                            .value()
                            .lifecycle())
                    .isEqualTo(
                            GenerationProtocolActivationLifecycle
                                    .PREPARED);
        }

        MutableReadinessProvider exact =
                new MutableReadinessProvider(readiness(7, "aa"));
        try (GenerationProtocolActivationStore store =
                activationStore()) {
            installProof(store, exact);
            List<ReferenceDomainVersionRecord> changed =
                    new ArrayList<>(
                            F4MetadataTestValues.referenceDomains());
            changed.set(
                    0,
                    new ReferenceDomainVersionRecord(
                            "append-recovery-v1", 2));

            assertInvariant(() -> new DefaultManagedLedgerGenerationProtocolActivationCoordinator(
                            CLUSTER,
                            true,
                            exact,
                            store,
                            changed,
                            ACTIVATION_CLOCK)
                    .activatePublication()
                    .join());
        }
    }

    private static DefaultManagedLedgerGenerationProtocolActivationCoordinator
            coordinator(
                    GenerationProtocolActivationStore store,
                    GenerationCapabilityReadinessProvider readiness,
                    boolean enabled) {
        return new DefaultManagedLedgerGenerationProtocolActivationCoordinator(
                CLUSTER,
                enabled,
                readiness,
                store,
                F4MetadataTestValues.referenceDomains(),
                ACTIVATION_CLOCK);
    }

    private static void installProof(
            GenerationProtocolActivationStore store,
            MutableReadinessProvider readiness) {
        new DefaultManagedLedgerGenerationRegistrationBackfillProofCoordinator(
                        CLUSTER,
                        store,
                        readiness,
                        F4MetadataTestValues.referenceDomains(),
                        ACTIVATION_CLOCK)
                .complete(new GenerationRegistrationBackfillCompletion(
                        F4MetadataTestValues.ATTEMPT,
                        readiness.value(),
                        COVERAGE,
                        0))
                .join();
    }

    private static GenerationProtocolActivationStore
            activationStore() {
        return GenerationProtocolActivationStoreTestFactory.inMemory(
                STORE_CLOCK,
                F4MetadataTestValues.PROCESS,
                F4MetadataTestValues.referenceDomains());
    }

    private static GenerationProtocolActivationStore
            loseFirstSuccessfulCasResponse(
                    GenerationProtocolActivationStore delegate,
                    AtomicBoolean lost) {
        return wrappingStore(
                delegate,
                (cluster, replacement, expectedVersion) ->
                        delegate.compareAndSet(
                                        cluster,
                                        replacement,
                                        expectedVersion)
                                .thenCompose(result -> {
                                    if (lost.compareAndSet(
                                            false, true)) {
                                        return CompletableFuture
                                                .failedFuture(
                                                        new IllegalStateException(
                                                                "lost activation CAS response"));
                                    }
                                    return CompletableFuture
                                            .completedFuture(result);
                                }));
    }

    private static GenerationProtocolActivationStore
            failFirstCasWithCondition(
                    GenerationProtocolActivationStore delegate,
                    AtomicBoolean conflicted) {
        return wrappingStore(
                delegate,
                (cluster, replacement, expectedVersion) -> {
                    if (conflicted.compareAndSet(false, true)) {
                        return CompletableFuture.failedFuture(
                                new F4MetadataConditionFailedException(
                                        "injected activation conflict"));
                    }
                    return delegate.compareAndSet(
                            cluster,
                            replacement,
                            expectedVersion);
                });
    }

    private static GenerationProtocolActivationStore wrappingStore(
            GenerationProtocolActivationStore delegate,
            Cas cas) {
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
                return cas.apply(
                        cluster, replacement, expectedVersion);
            }

            @Override
            public void close() {
            }
        };
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

    private static void assertConditionFailure(
            Runnable operation) {
        assertThatThrownBy(operation::run)
                .satisfies(error -> {
                    Throwable exact = unwrap(error);
                    assertThat(exact)
                            .isInstanceOf(NereusException.class);
                    assertThat(((NereusException) exact).code())
                            .isEqualTo(
                                    ErrorCode
                                            .METADATA_CONDITION_FAILED);
                });
    }

    private static void assertInvariant(Runnable operation) {
        assertThatThrownBy(operation::run)
                .satisfies(error -> {
                    Throwable exact = unwrap(error);
                    assertThat(exact)
                            .isInstanceOf(NereusException.class);
                    assertThat(((NereusException) exact).code())
                            .isEqualTo(
                                    ErrorCode
                                            .METADATA_INVARIANT_VIOLATION);
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

    @FunctionalInterface
    private interface Cas {
        CompletableFuture<VersionedGenerationProtocolActivation>
                apply(
                        String cluster,
                        GenerationProtocolActivationRecord replacement,
                        long expectedVersion);
    }

    private static final class MutableReadinessProvider
            implements GenerationCapabilityReadinessProvider {
        private GenerationCapabilityReadiness value;
        private boolean invalidateAfterRequire;

        private MutableReadinessProvider(
                GenerationCapabilityReadiness value) {
            this.value = value;
        }

        private GenerationCapabilityReadiness value() {
            return value;
        }

        @Override
        public CompletableFuture<GenerationCapabilityReadiness>
                requireGenerationCapabilityReadiness() {
            if (value == null) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException(
                                "readiness unavailable"));
            }
            GenerationCapabilityReadiness current = value;
            if (invalidateAfterRequire) {
                value = null;
            }
            return CompletableFuture.completedFuture(current);
        }

        @Override
        public Optional<GenerationCapabilityReadiness>
                currentGenerationCapabilityReadiness() {
            return Optional.ofNullable(value);
        }
    }
}
