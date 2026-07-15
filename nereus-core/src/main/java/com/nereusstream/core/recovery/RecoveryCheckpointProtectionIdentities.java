/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.recovery;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.keys.DeterministicIds;
import com.nereusstream.core.physical.ObjectProtectionOwner;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedRecoveryCheckpointRoot;
import java.util.Objects;

/** Stable root-owner and target-reference identities shared by publication and repair. */
public final class RecoveryCheckpointProtectionIdentities {
    private RecoveryCheckpointProtectionIdentities() {
    }

    public static ObjectProtectionOwner rootOwner(
            VersionedRecoveryCheckpointRoot root) {
        VersionedRecoveryCheckpointRoot value = Objects.requireNonNull(root, "root");
        return new ObjectProtectionOwner(
                value.key(), value.metadataVersion(), value.durableValueSha256());
    }

    public static String checkpointObjectReferenceId(
            VersionedRecoveryCheckpointRoot root,
            PhysicalObjectIdentity object) {
        VersionedRecoveryCheckpointRoot exactRoot = Objects.requireNonNull(root, "root");
        PhysicalObjectIdentity exactObject = Objects.requireNonNull(object, "object");
        return "rco1-" + stable(exactRoot.value().streamId()
                + '\0' + exactRoot.value().checkpointSequence()
                + '\0' + exactObject.objectKeyHash().value());
    }

    public static String checkpointTargetReferenceId(
            VersionedRecoveryCheckpointRoot root,
            VersionedGenerationIndex target,
            PhysicalObjectIdentity object) {
        VersionedGenerationIndex exactTarget =
                Objects.requireNonNull(target, "target");
        return checkpointTargetReferenceId(
                root,
                exactTarget.key(),
                exactTarget.durableValueSha256(),
                object);
    }

    public static String checkpointTargetReferenceId(
            VersionedRecoveryCheckpointRoot root,
            String indexKey,
            Checksum durableIndexSha256,
            PhysicalObjectIdentity object) {
        VersionedRecoveryCheckpointRoot exactRoot = Objects.requireNonNull(root, "root");
        String exactIndexKey = requireText(indexKey, "indexKey");
        Checksum exactDigest = Objects.requireNonNull(
                durableIndexSha256, "durableIndexSha256");
        PhysicalObjectIdentity exactObject = Objects.requireNonNull(object, "object");
        return "rct1-" + stable(exactRoot.value().streamId()
                + '\0' + exactRoot.value().checkpointSequence()
                + '\0' + exactIndexKey
                + '\0' + exactDigest.value()
                + '\0' + exactObject.objectKeyHash().value());
    }

    private static String stable(String value) {
        return DeterministicIds.stableHashComponent(value);
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }
}
