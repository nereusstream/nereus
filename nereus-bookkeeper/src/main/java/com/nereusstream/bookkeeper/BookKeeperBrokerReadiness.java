/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import java.util.Objects;

/** Exact live persistent-broker set that currently advertises the BookKeeper protocol binding. */
public record BookKeeperBrokerReadiness(
        long brokerReadinessEpoch,
        Checksum brokerSetSha256,
        int persistentBrokerCount) {
    public BookKeeperBrokerReadiness {
        if (brokerReadinessEpoch <= 0) {
            throw new IllegalArgumentException("brokerReadinessEpoch must be positive");
        }
        brokerSetSha256 = Objects.requireNonNull(brokerSetSha256, "brokerSetSha256");
        if (brokerSetSha256.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException("brokerSetSha256 must use SHA256");
        }
        if (persistentBrokerCount < 1) {
            throw new IllegalArgumentException("persistentBrokerCount must be positive");
        }
    }
}
