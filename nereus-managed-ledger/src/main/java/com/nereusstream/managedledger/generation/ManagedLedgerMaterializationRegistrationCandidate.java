/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.generation;

import com.nereusstream.api.Checksum;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import java.util.Objects;

/**
 * Broker-safe immutable projection identity captured before an unloaded-topic
 * materialization-registration backfill write.
 */
public record ManagedLedgerMaterializationRegistrationCandidate(
        String managedLedgerName,
        long storageClassBindingGeneration,
        ManagedLedgerProjectionIdentity projectionIdentity,
        Checksum projectionIdentitySha256) {
    public ManagedLedgerMaterializationRegistrationCandidate {
        managedLedgerName = ManagedLedgerProjectionNames.requireManagedLedgerName(
                managedLedgerName);
        if (storageClassBindingGeneration < 1) {
            throw new IllegalArgumentException(
                    "storageClassBindingGeneration must be positive");
        }
        projectionIdentity = Objects.requireNonNull(
                projectionIdentity, "projectionIdentity");
        if (projectionIdentity.storageClassBindingGeneration()
                != storageClassBindingGeneration) {
            throw new IllegalArgumentException(
                    "projection identity binding generation mismatch");
        }
        Checksum expected = new ManagedLedgerGenerationProjectionRefV1(
                        managedLedgerName, projectionIdentity)
                .projectionIdentitySha256();
        if (!expected.equals(projectionIdentitySha256)) {
            throw new IllegalArgumentException(
                    "projectionIdentitySha256 does not match the exact NPR1 identity");
        }
    }

    public static ManagedLedgerMaterializationRegistrationCandidate from(
            TopicProjectionRecord projection) {
        Objects.requireNonNull(projection, "projection");
        ManagedLedgerProjectionIdentity identity =
                projection.projectionIdentity();
        return new ManagedLedgerMaterializationRegistrationCandidate(
                projection.managedLedgerName(),
                projection.storageClassBindingGeneration(),
                identity,
                new ManagedLedgerGenerationProjectionRefV1(
                                projection.managedLedgerName(), identity)
                        .projectionIdentitySha256());
    }
}
