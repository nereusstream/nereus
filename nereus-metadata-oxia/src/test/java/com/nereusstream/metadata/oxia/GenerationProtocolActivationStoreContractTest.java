/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
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

class GenerationProtocolActivationStoreContractTest {
    @Test
    void exactClusterAuthorityBootstrapsAndAdvancesCapabilitiesAcrossRuntimes() {
        InMemoryPartitionedOxiaBackend backend = new InMemoryPartitionedOxiaBackend();
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC);
        GenerationProtocolActivationStore first = store(
                backend, clock, F4MetadataTestValues.PROCESS, F4MetadataTestValues.referenceDomains());
        GenerationProtocolActivationStore second = store(
                backend, clock, "e".repeat(26), F4MetadataTestValues.referenceDomains());
        try {
            assertThat(first.get(F4MetadataTestValues.CLUSTER).join()).isEmpty();

            VersionedGenerationProtocolActivation prepared = first
                    .getOrCreate(F4MetadataTestValues.CLUSTER)
                    .join();
            assertThat(prepared.key()).isEqualTo(new F4Keyspace(F4MetadataTestValues.CLUSTER)
                    .generationProtocolActivationKey());
            assertThat(prepared.value().lifecycle())
                    .isEqualTo(GenerationProtocolActivationLifecycle.PREPARED);
            assertThat(prepared.value().requiredReferenceDomains())
                    .containsExactlyElementsOf(F4MetadataTestValues.referenceDomains());
            assertThat(prepared.value().preparedAtMillis()).isEqualTo(1_000);
            assertThat(second.getOrCreate(F4MetadataTestValues.CLUSTER).join())
                    .isEqualTo(prepared);

            VersionedGenerationProtocolActivation publication = first.compareAndSet(
                    F4MetadataTestValues.CLUSTER,
                    publication(prepared.value()),
                    prepared.metadataVersion()).join();
            assertThat(publication.value().publicationEnabled()).isTrue();
            assertThat(publication.value().physicalDeleteEnabled()).isFalse();

            VersionedGenerationProtocolActivation deletion = second.compareAndSet(
                    F4MetadataTestValues.CLUSTER,
                    deletion(publication.value()),
                    publication.metadataVersion()).join();
            assertThat(deletion.value().physicalDeleteEnabled()).isTrue();
            assertThat(deletion.value().cursorSnapshotDeleteEnabled()).isTrue();
            assertThat(first.get(F4MetadataTestValues.CLUSTER).join())
                    .contains(deletion);

            assertConditionFailure(() -> first.compareAndSet(
                    F4MetadataTestValues.CLUSTER,
                    deletion.value().withMetadataVersion(0),
                    publication.metadataVersion()).join());
            assertInvariant(() -> first.compareAndSet(
                    F4MetadataTestValues.CLUSTER,
                    publication(deletion.value()),
                    deletion.metadataVersion()).join());
        } finally {
            first.close();
            second.close();
        }
    }

    @Test
    void activeDomainSetCannotDriftInsideTheSameProtocolActivation() {
        GenerationProtocolActivationRecord active = F4MetadataTestValues.publicationActivation();
        List<ReferenceDomainVersionRecord> changed = new ArrayList<>(active.requiredReferenceDomains());
        changed.set(0, new ReferenceDomainVersionRecord("append-recovery-v1", 2));
        GenerationProtocolActivationRecord replacement = copy(
                active,
                active.lifecycle(),
                active.publicationEnabled(),
                active.physicalDeleteEnabled(),
                active.cursorSnapshotDeleteEnabled(),
                active.brokerCapabilityReadinessEpoch(),
                List.copyOf(changed),
                active.streamRegistrationBackfill(),
                active.physicalRootBackfill(),
                active.cursorSnapshotBackfill(),
                active.objectStoreCapabilitySha256(),
                active.activatedAtMillis(),
                active.updatedAtMillis() + 1);

        assertInvariant(() -> GenerationProtocolActivationTransitions.requireValidReplacement(
                active, replacement));
    }

    private static GenerationProtocolActivationStore store(
            InMemoryPartitionedOxiaBackend backend,
            Clock clock,
            String brokerRunId,
            List<ReferenceDomainVersionRecord> domains) {
        return new OxiaJavaGenerationProtocolActivationStore(
                new PartitionedOxiaClient(backend), clock, brokerRunId, domains);
    }

    private static GenerationProtocolActivationRecord publication(
            GenerationProtocolActivationRecord current) {
        return copy(
                current,
                GenerationProtocolActivationLifecycle.ACTIVE,
                true,
                false,
                false,
                7,
                current.requiredReferenceDomains(),
                GenerationBackfillProofRecord.incomplete(7),
                GenerationBackfillProofRecord.incomplete(7),
                GenerationBackfillProofRecord.incomplete(7),
                "",
                current.lifecycle() == GenerationProtocolActivationLifecycle.ACTIVE
                        ? current.activatedAtMillis()
                        : 1_100,
                Math.max(current.updatedAtMillis() + 1, 1_100));
    }

    private static GenerationProtocolActivationRecord deletion(
            GenerationProtocolActivationRecord current) {
        return copy(
                current,
                GenerationProtocolActivationLifecycle.ACTIVE,
                true,
                true,
                true,
                6,
                current.requiredReferenceDomains(),
                complete(F4MetadataTestValues.ATTEMPT, F4MetadataTestValues.HASH_A, 1_200),
                complete(F4MetadataTestValues.CLAIM, F4MetadataTestValues.HASH_B, 1_201),
                complete(F4MetadataTestValues.PUBLICATION, F4MetadataTestValues.HASH_C, 1_202),
                F4MetadataTestValues.HASH_D,
                current.activatedAtMillis(),
                1_203);
    }

    private static GenerationBackfillProofRecord complete(
            String runId,
            String coverageSha256,
            long completedAtMillis) {
        return new GenerationBackfillProofRecord(
                runId, 6, coverageSha256, true, completedAtMillis);
    }

    private static GenerationProtocolActivationRecord copy(
            GenerationProtocolActivationRecord current,
            GenerationProtocolActivationLifecycle lifecycle,
            boolean publicationEnabled,
            boolean physicalDeleteEnabled,
            boolean cursorSnapshotDeleteEnabled,
            long readinessEpoch,
            List<ReferenceDomainVersionRecord> domains,
            GenerationBackfillProofRecord streamRegistrationBackfill,
            GenerationBackfillProofRecord physicalRootBackfill,
            GenerationBackfillProofRecord cursorSnapshotBackfill,
            String objectStoreCapabilitySha256,
            long activatedAtMillis,
            long updatedAtMillis) {
        return new GenerationProtocolActivationRecord(
                current.schemaVersion(),
                current.protocolVersion(),
                lifecycle,
                publicationEnabled,
                physicalDeleteEnabled,
                cursorSnapshotDeleteEnabled,
                readinessEpoch,
                domains,
                streamRegistrationBackfill,
                physicalRootBackfill,
                cursorSnapshotBackfill,
                objectStoreCapabilitySha256,
                current.activatingBrokerRunId(),
                current.preparedAtMillis(),
                activatedAtMillis,
                updatedAtMillis,
                0);
    }

    private static void assertConditionFailure(Runnable operation) {
        assertThatThrownBy(operation::run)
                .satisfies(failure -> assertThat(unwrap(failure))
                        .isInstanceOf(F4MetadataConditionFailedException.class));
    }

    private static void assertInvariant(Runnable operation) {
        assertThatThrownBy(operation::run).satisfies(failure -> {
            Throwable exact = unwrap(failure);
            assertThat(exact).isInstanceOf(NereusException.class);
            assertThat(((NereusException) exact).code())
                    .isEqualTo(ErrorCode.METADATA_INVARIANT_VIOLATION);
        });
    }

    private static Throwable unwrap(Throwable supplied) {
        Throwable current = supplied;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
