/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import static com.nereusstream.metadata.oxia.F4MetadataTestValues.CLUSTER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class ConditionalDeleteContractTest {
    @Test
    void rootDeleteRequiresExactVersionDigestDeletedStateAndDurableTombstoneObservation() {
        OxiaJavaPhysicalObjectMetadataStore store = store();
        PhysicalObjectRootRecord active = F4MetadataTestValues.physicalRoot(PhysicalObjectLifecycle.ACTIVE);
        ObjectKeyHash object = new ObjectKeyHash(active.objectKeyHash());
        VersionedPhysicalObjectRoot activeVersion = store.createRoot(CLUSTER, active).join();

        assertThatThrownBy(() -> store.deleteRoot(
                        CLUSTER,
                        object,
                        activeVersion.metadataVersion(),
                        activeVersion.durableValueSha256()).join())
                .hasCauseInstanceOf(F4MetadataConditionFailedException.class);

        VersionedPhysicalObjectRoot marked = store.compareAndSetRoot(
                CLUSTER,
                PhysicalObjectMetadataStoreContractTest.rootState(
                        active, PhysicalObjectLifecycle.MARKED, 2, false),
                activeVersion.metadataVersion()).join();
        VersionedPhysicalObjectRoot deleting = store.compareAndSetRoot(
                CLUSTER,
                PhysicalObjectMetadataStoreContractTest.rootState(
                        active, PhysicalObjectLifecycle.DELETING, 3, false),
                marked.metadataVersion()).join();
        VersionedPhysicalObjectRoot deleted = store.compareAndSetRoot(
                CLUSTER,
                PhysicalObjectMetadataStoreContractTest.rootState(
                        active, PhysicalObjectLifecycle.DELETED, 4, false),
                deleting.metadataVersion()).join();

        assertThatThrownBy(() -> store.deleteRoot(
                        CLUSTER,
                        object,
                        deleted.metadataVersion(),
                        deleted.durableValueSha256()).join())
                .hasCauseInstanceOf(F4MetadataConditionFailedException.class);

        VersionedPhysicalObjectRoot audited = store.compareAndSetRoot(
                CLUSTER,
                PhysicalObjectMetadataStoreContractTest.rootState(
                        active, PhysicalObjectLifecycle.DELETED, 4, true),
                deleted.metadataVersion()).join();
        assertThatThrownBy(() -> store.deleteRoot(
                        CLUSTER,
                        object,
                        deleted.metadataVersion(),
                        audited.durableValueSha256()).join())
                .hasCauseInstanceOf(F4MetadataConditionFailedException.class);
        assertThatThrownBy(() -> store.deleteRoot(
                        CLUSTER,
                        object,
                        audited.metadataVersion(),
                        new Checksum(ChecksumType.SHA256, "f".repeat(64))).join())
                .hasCauseInstanceOf(F4MetadataConditionFailedException.class);

        store.deleteRoot(
                CLUSTER, object, audited.metadataVersion(), audited.durableValueSha256()).join();
        assertThat(store.getRoot(CLUSTER, object).join()).isEmpty();
    }

    @Test
    void childDeletesNeverTreatMissingOrStaleVersionAsSuccess() {
        OxiaJavaPhysicalObjectMetadataStore store = store();
        PhysicalObjectRootRecord active = F4MetadataTestValues.physicalRoot(PhysicalObjectLifecycle.ACTIVE);
        ObjectKeyHash object = new ObjectKeyHash(active.objectKeyHash());
        store.createRoot(CLUSTER, active).join();
        VersionedReaderLease lease = store.createOrCompareReaderLease(
                CLUSTER, F4MetadataTestValues.readerLease()).join();
        VersionedObjectProtection protection = store.createProtection(
                CLUSTER,
                F4MetadataTestValues.protection(ObjectProtectionType.VISIBLE_GENERATION)).join();
        ObjectProtectionIdentity identity = new ObjectProtectionIdentity(
                object,
                ObjectProtectionType.VISIBLE_GENERATION,
                protection.value().referenceId());

        assertThatThrownBy(() -> store.deleteReaderLease(
                        CLUSTER,
                        object,
                        lease.value().processRunId(),
                        lease.metadataVersion() + 1).join())
                .hasCauseInstanceOf(F4MetadataConditionFailedException.class);
        assertThatThrownBy(() -> store.deleteProtection(
                        CLUSTER, identity, protection.metadataVersion() + 1).join())
                .hasCauseInstanceOf(F4MetadataConditionFailedException.class);

        store.deleteReaderLease(
                CLUSTER, object, lease.value().processRunId(), lease.metadataVersion()).join();
        store.deleteProtection(CLUSTER, identity, protection.metadataVersion()).join();
        assertThatThrownBy(() -> store.deleteReaderLease(
                        CLUSTER, object, lease.value().processRunId(), lease.metadataVersion()).join())
                .hasCauseInstanceOf(F4MetadataConditionFailedException.class);
        assertThatThrownBy(() -> store.deleteProtection(
                        CLUSTER, identity, protection.metadataVersion()).join())
                .hasCauseInstanceOf(F4MetadataConditionFailedException.class);
    }

    private static OxiaJavaPhysicalObjectMetadataStore store() {
        return new OxiaJavaPhysicalObjectMetadataStore(
                new PartitionedOxiaClient(new InMemoryPartitionedOxiaBackend()),
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC));
    }
}
