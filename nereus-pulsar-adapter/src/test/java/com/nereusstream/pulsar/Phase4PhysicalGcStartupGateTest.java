/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.F4MetadataTestValues;
import com.nereusstream.metadata.oxia.GenerationProtocolActivationStore;
import com.nereusstream.metadata.oxia.GenerationProtocolActivationStoreTestFactory;
import com.nereusstream.metadata.oxia.VersionedGenerationProtocolActivation;
import com.nereusstream.metadata.oxia.records.GenerationBackfillProofRecord;
import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationLifecycle;
import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationRecord;
import com.nereusstream.metadata.oxia.records.ReferenceDomainVersionRecord;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class Phase4PhysicalGcStartupGateTest {
    private static final String CLUSTER = F4MetadataTestValues.CLUSTER;
    private static final String RUN_ID = "abcdefghijklmnopqrstuvwxyz";
    private static final long EPOCH = 7;
    private static final String CAPABILITY = F4MetadataTestValues.HASH_D;
    private static final Clock CLOCK = Clock.fixed(
            Instant.ofEpochMilli(1_000), ZoneOffset.UTC);
    private static final List<ReferenceDomainVersionRecord> DOMAINS =
            F4MetadataTestValues.referenceDomains();

    @Test
    void absentOrPublicationOnlyAuthorityDefersMutatingLifecycle() {
        try (GenerationProtocolActivationStore absent = store();
                GenerationProtocolActivationStore publication = store()) {
            seed(publication, false, "");

            assertThat(gate(absent, DOMAINS, CAPABILITY)
                            .destructiveLifecycleAuthorized()
                            .join())
                    .isFalse();
            assertThat(gate(publication, DOMAINS, CAPABILITY)
                            .destructiveLifecycleAuthorized()
                            .join())
                    .isFalse();
        }
    }

    @Test
    void exactDurableScopeAuthorizesRestartRecovery() {
        try (GenerationProtocolActivationStore store = store()) {
            seed(store, true, CAPABILITY);

            assertThat(gate(store, DOMAINS, CAPABILITY)
                            .destructiveLifecycleAuthorized()
                            .join())
                    .isTrue();
        }
    }

    @Test
    void configuredScopeDriftFailsStartupNonRetryably() {
        try (GenerationProtocolActivationStore store = store()) {
            seed(store, true, CAPABILITY);

            assertInvariant(() -> gate(
                            store,
                            DOMAINS,
                            F4MetadataTestValues.HASH_E)
                    .destructiveLifecycleAuthorized()
                    .join());
        }
    }

    @Test
    void installedReferenceDomainDriftFailsStartupNonRetryably() {
        try (GenerationProtocolActivationStore store = store()) {
            seed(store, true, CAPABILITY);
            ArrayList<ReferenceDomainVersionRecord> changed =
                    new ArrayList<>(DOMAINS);
            ReferenceDomainVersionRecord first = changed.get(0);
            changed.set(
                    0,
                    new ReferenceDomainVersionRecord(
                            first.domainId(), first.protocolVersion() + 1));

            assertInvariant(() -> gate(store, changed, CAPABILITY)
                    .destructiveLifecycleAuthorized()
                    .join());
        }
    }

    private static Phase4PhysicalGcStartupGate gate(
            GenerationProtocolActivationStore store,
            List<ReferenceDomainVersionRecord> domains,
            String capability) {
        return new Phase4PhysicalGcStartupGate(
                CLUSTER, store, domains, capability);
    }

    private static GenerationProtocolActivationStore store() {
        return GenerationProtocolActivationStoreTestFactory.inMemory(
                CLOCK, F4MetadataTestValues.PROCESS, DOMAINS);
    }

    private static void seed(
            GenerationProtocolActivationStore store,
            boolean deletion,
            String capability) {
        VersionedGenerationProtocolActivation current =
                store.getOrCreate(CLUSTER).join();
        GenerationBackfillProofRecord registration = complete(
                F4MetadataTestValues.HASH_A, 1_101);
        GenerationBackfillProofRecord physical = deletion
                ? complete(F4MetadataTestValues.HASH_B, 1_102)
                : GenerationBackfillProofRecord.incomplete(EPOCH);
        GenerationBackfillProofRecord cursor = deletion
                ? complete(F4MetadataTestValues.HASH_C, 1_103)
                : GenerationBackfillProofRecord.incomplete(EPOCH);
        GenerationProtocolActivationRecord replacement =
                new GenerationProtocolActivationRecord(
                        current.value().schemaVersion(),
                        current.value().protocolVersion(),
                        GenerationProtocolActivationLifecycle.ACTIVE,
                        true,
                        deletion,
                        deletion,
                        EPOCH,
                        current.value().requiredReferenceDomains(),
                        registration,
                        physical,
                        cursor,
                        capability,
                        current.value().activatingBrokerRunId(),
                        current.value().preparedAtMillis(),
                        Math.max(current.value().preparedAtMillis(), 1_050),
                        Math.max(current.value().updatedAtMillis(), 1_104),
                        0);
        store.compareAndSet(
                        CLUSTER,
                        replacement,
                        current.metadataVersion())
                .join();
    }

    private static GenerationBackfillProofRecord complete(
            String coverage, long completedAtMillis) {
        return new GenerationBackfillProofRecord(
                RUN_ID, EPOCH, coverage, true, completedAtMillis);
    }

    private static void assertInvariant(Runnable operation) {
        assertThatThrownBy(operation::run)
                .isInstanceOf(CompletionException.class)
                .satisfies(error -> assertThat(error.getCause())
                        .isInstanceOf(NereusException.class)
                        .extracting(value -> ((NereusException) value).code())
                        .isEqualTo(ErrorCode.METADATA_INVARIANT_VIOLATION));
    }
}
