/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.capability;

import com.nereusstream.api.Checksum;
import com.nereusstream.core.physical.GcReferenceQuery;

/** Exact broker-set capability identity shared with the product activation boundary. */
public record GenerationCapabilityReadiness(
        long brokerReadinessEpoch,
        Checksum brokerSetSha256,
        int persistentBrokerCount) {
    public GenerationCapabilityReadiness {
        if (brokerReadinessEpoch < 0) {
            throw new IllegalArgumentException(
                    "brokerReadinessEpoch must be non-negative");
        }
        brokerSetSha256 = GcReferenceQuery.requireSha256(
                brokerSetSha256, "brokerSetSha256");
        if (persistentBrokerCount < 1) {
            throw new IllegalArgumentException(
                    "persistentBrokerCount must be positive");
        }
    }
}
