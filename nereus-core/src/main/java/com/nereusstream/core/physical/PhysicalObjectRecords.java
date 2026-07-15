/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.physical;

import com.nereusstream.api.ChecksumType;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;

final class PhysicalObjectRecords {
    private PhysicalObjectRecords() {
    }

    static PhysicalObjectRootRecord active(
            PhysicalObjectIdentity identity,
            long createdAtMillis,
            long orphanNotBeforeMillis) {
        return new PhysicalObjectRootRecord(
                1,
                identity.objectKeyHash().value(),
                identity.objectKey().value(),
                identity.objectId().map(value -> value.value()).orElse(""),
                identity.kind().wireId(),
                identity.objectLength(),
                ChecksumType.CRC32C.name(),
                identity.storageChecksum().value(),
                identity.contentSha256().map(value -> value.value()).orElse(""),
                identity.etag().orElse(""),
                PhysicalObjectLifecycle.ACTIVE,
                1,
                createdAtMillis,
                orphanNotBeforeMillis,
                "",
                "",
                0,
                0,
                0,
                0,
                0,
                "",
                "",
                0);
    }

    static boolean exactIdentity(PhysicalObjectIdentity identity, PhysicalObjectRootRecord root) {
        return root.objectKeyHash().equals(identity.objectKeyHash().value())
                && root.objectKey().equals(identity.objectKey().value())
                && root.objectId().equals(identity.objectId().map(value -> value.value()).orElse(""))
                && root.objectKindId() == identity.kind().wireId()
                && root.objectLength() == identity.objectLength()
                && root.storageChecksumType().equals(identity.storageChecksum().type().name())
                && root.storageChecksumValue().equals(identity.storageChecksum().value())
                && root.contentSha256().equals(identity.contentSha256().map(value -> value.value()).orElse(""))
                && root.etag().equals(identity.etag().orElse(""));
    }

    static boolean sameActiveRoot(
            VersionedPhysicalObjectRoot expected,
            VersionedPhysicalObjectRoot actual,
            PhysicalObjectIdentity identity) {
        return actual.value().lifecycle() == PhysicalObjectLifecycle.ACTIVE
                && exactIdentity(identity, actual.value())
                && actual.metadataVersion() == expected.metadataVersion()
                && actual.value().lifecycleEpoch() == expected.value().lifecycleEpoch()
                && actual.durableValueSha256().equals(expected.durableValueSha256());
    }
}
