/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

import com.nereusstream.bookkeeper.BookKeeperWalConfiguration;
import com.nereusstream.metadata.oxia.BookKeeperMetadataStoreConfig;
import java.util.Objects;

/** Exact provider binding required to install the Kafka BookKeeper generation-zero WAL. */
public record NereusKafkaBookKeeperWalRuntimeConfiguration(
        String deploymentId,
        BookKeeperWalConfiguration wal) {
    public NereusKafkaBookKeeperWalRuntimeConfiguration {
        deploymentId = nonblank(deploymentId, "deploymentId");
        Objects.requireNonNull(wal, "wal");
    }

    public BookKeeperMetadataStoreConfig metadataStore() {
        return new BookKeeperMetadataStoreConfig(
                wal.maxAppendRangesPerLedger(),
                wal.protectionSlotsPerRange(),
                wal.maxReaderLeasesPerLedger(),
                wal.maxUncertainAllocations());
    }

    private static String nonblank(String value, String name) {
        String exact = Objects.requireNonNull(value, name);
        if (exact.isBlank()) {
            throw new IllegalArgumentException(name + " cannot be blank");
        }
        return exact;
    }
}
