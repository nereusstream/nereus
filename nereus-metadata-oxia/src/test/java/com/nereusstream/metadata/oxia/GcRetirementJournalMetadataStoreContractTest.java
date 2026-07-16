/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.metadata.oxia.records.GcRetirementManifestRecord;
import com.nereusstream.metadata.oxia.records.GcRetirementProtectionRecord;
import com.nereusstream.metadata.oxia.records.GcRetirementRemovalRecord;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class GcRetirementJournalMetadataStoreContractTest {
    @Test
    void productionAdapterImplementsExactAttemptScopedJournalContract() {
        runContract(new OxiaJavaPhysicalObjectMetadataStore(
                new PartitionedOxiaClient(new InMemoryPartitionedOxiaBackend()),
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC)));
    }

    @Test
    void fakeAdapterImplementsExactAttemptScopedJournalContract() {
        runContract(new FakePhysicalObjectMetadataStore());
    }

    private static void runContract(PhysicalObjectMetadataStore store) {
        try {
            GcRetirementManifestRecord manifest = F4MetadataTestValues.gcRetirementManifest();
            GcRetirementProtectionRecord protection = F4MetadataTestValues.gcRetirementProtection();
            GcRetirementRemovalRecord removal = F4MetadataTestValues.gcRetirementRemoval();
            ObjectKeyHash object = new ObjectKeyHash(manifest.objectKeyHash());

            VersionedGcRetirementProtection createdProtection =
                    store.createRetirementProtection(F4MetadataTestValues.CLUSTER, protection).join();
            VersionedGcRetirementRemoval createdRemoval =
                    store.createRetirementRemoval(F4MetadataTestValues.CLUSTER, removal).join();
            assertThat(store.createRetirementProtection(
                            F4MetadataTestValues.CLUSTER, protection).join())
                    .isEqualTo(createdProtection);
            assertThat(store.createRetirementRemoval(
                            F4MetadataTestValues.CLUSTER, removal).join())
                    .isEqualTo(createdRemoval);

            GcRetirementProtectionScanPage protections = store.scanRetirementProtections(
                    F4MetadataTestValues.CLUSTER,
                    object,
                    manifest.gcAttemptId(),
                    Optional.empty(),
                    1).join();
            assertThat(protections.values()).containsExactly(createdProtection);
            assertThat(protections.continuation()).isPresent();
            assertThat(store.scanRetirementProtections(
                            F4MetadataTestValues.CLUSTER,
                            object,
                            manifest.gcAttemptId(),
                            protections.continuation(),
                            1).join().values())
                    .isEmpty();

            GcRetirementRemovalScanPage removals = store.scanRetirementRemovals(
                    F4MetadataTestValues.CLUSTER,
                    object,
                    manifest.gcAttemptId(),
                    Optional.empty(),
                    1).join();
            assertThat(removals.values()).containsExactly(createdRemoval);
            assertThat(removals.continuation()).isPresent();
            assertThat(store.scanRetirementRemovals(
                            F4MetadataTestValues.CLUSTER,
                            object,
                            manifest.gcAttemptId(),
                            removals.continuation(),
                            1).join().values())
                    .isEmpty();

            VersionedGcRetirementManifest createdManifest =
                    store.createRetirementManifest(F4MetadataTestValues.CLUSTER, manifest).join();
            assertThat(store.createRetirementManifest(
                            F4MetadataTestValues.CLUSTER, manifest).join())
                    .isEqualTo(createdManifest);
            assertThat(store.getRetirementManifest(
                            F4MetadataTestValues.CLUSTER, object, manifest.gcAttemptId()).join())
                    .contains(createdManifest);

            assertInvariant(() -> store.createRetirementManifest(
                    F4MetadataTestValues.CLUSTER, conflictingManifest(manifest)).join());
            assertInvariant(() -> store.createRetirementRemoval(
                    F4MetadataTestValues.CLUSTER, conflictingRemoval(removal)).join());
            assertThatThrownBy(() -> store.scanRetirementRemovals(
                            F4MetadataTestValues.CLUSTER,
                            object,
                            manifest.gcAttemptId(),
                            protections.continuation(),
                            1).join())
                    .isInstanceOf(IllegalArgumentException.class);

            String otherAttempt = "b".repeat(26);
            store.createRetirementRemoval(
                    F4MetadataTestValues.CLUSTER, withAttempt(removal, otherAttempt)).join();
            assertThat(store.scanRetirementRemovals(
                            F4MetadataTestValues.CLUSTER,
                            object,
                            otherAttempt,
                            Optional.empty(),
                            10).join().values())
                    .singleElement()
                    .extracting(value -> value.value().gcAttemptId())
                    .isEqualTo(otherAttempt);
        } finally {
            store.close();
        }
    }

    private static GcRetirementManifestRecord conflictingManifest(
            GcRetirementManifestRecord value) {
        return new GcRetirementManifestRecord(
                value.schemaVersion(),
                value.objectKeyHash(),
                value.gcAttemptId(),
                value.referenceSetProtocolVersion(),
                value.queryIdentitySha256(),
                value.domainProofs(),
                value.protectionCount(),
                value.metadataRemovalCount(),
                F4MetadataTestValues.HASH_D,
                value.createdAtMillis(),
                0);
    }

    private static GcRetirementRemovalRecord conflictingRemoval(
            GcRetirementRemovalRecord value) {
        return new GcRetirementRemovalRecord(
                value.schemaVersion(),
                value.objectKeyHash(),
                value.gcAttemptId(),
                value.removalType(),
                value.removalKey(),
                value.removalMetadataVersion(),
                F4MetadataTestValues.HASH_A,
                0);
    }

    private static GcRetirementRemovalRecord withAttempt(
            GcRetirementRemovalRecord value, String gcAttemptId) {
        return new GcRetirementRemovalRecord(
                value.schemaVersion(),
                value.objectKeyHash(),
                gcAttemptId,
                value.removalType(),
                value.removalKey(),
                value.removalMetadataVersion(),
                value.removalDurableValueSha256(),
                0);
    }

    private static void assertInvariant(Runnable action) {
        assertThatThrownBy(action::run).satisfies(failure -> {
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
