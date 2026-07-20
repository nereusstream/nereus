/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import com.nereusstream.bookkeeper.BookKeeperLedgerGcConfiguration;
import com.nereusstream.bookkeeper.BookKeeperWalConfiguration;
import com.nereusstream.metadata.oxia.BookKeeperMetadataStoreConfig;
import java.util.Objects;

/** Production composition values for the optional BookKeeper primary-WAL provider. */
public record NereusBookKeeperRuntimeConfiguration(
        String deploymentId,
        BookKeeperWalConfiguration wal,
        BookKeeperLedgerGcConfiguration ledgerGc) {
    public NereusBookKeeperRuntimeConfiguration {
        Objects.requireNonNull(deploymentId, "deploymentId");
        if (deploymentId.isBlank()) {
            throw new IllegalArgumentException("deploymentId cannot be blank");
        }
        Objects.requireNonNull(wal, "wal");
        ledgerGc = Objects.requireNonNull(ledgerGc, "ledgerGc");
        ledgerGc.validateAgainst(wal);
    }

    public BookKeeperMetadataStoreConfig metadataStore() {
        return new BookKeeperMetadataStoreConfig(
                wal.maxAppendRangesPerLedger(),
                wal.protectionSlotsPerRange(),
                wal.maxReaderLeasesPerLedger(),
                wal.maxUncertainAllocations());
    }
}
