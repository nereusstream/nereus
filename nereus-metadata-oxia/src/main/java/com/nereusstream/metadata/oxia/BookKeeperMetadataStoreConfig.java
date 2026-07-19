/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

/** Immutable bounds that are also part of the BookKeeper profile binding. */
public record BookKeeperMetadataStoreConfig(
        int maxAppendRangesPerLedger,
        int protectionSlotsPerRange,
        int maxReaderLeasesPerLedger,
        int maxUncertainAllocations) {
    public BookKeeperMetadataStoreConfig {
        if (maxAppendRangesPerLedger <= 0 || maxAppendRangesPerLedger > 65_536) {
            throw new IllegalArgumentException("maxAppendRangesPerLedger must be in [1,65536]");
        }
        if (protectionSlotsPerRange < 4 || protectionSlotsPerRange > 64) {
            throw new IllegalArgumentException("protectionSlotsPerRange must be in [4,64]");
        }
        if (maxReaderLeasesPerLedger <= 0 || maxReaderLeasesPerLedger > 65_536) {
            throw new IllegalArgumentException("maxReaderLeasesPerLedger must be in [1,65536]");
        }
        if (maxUncertainAllocations <= 0 || maxUncertainAllocations > 65_536) {
            throw new IllegalArgumentException("maxUncertainAllocations must be in [1,65536]");
        }
        if (Math.multiplyExact(maxAppendRangesPerLedger, protectionSlotsPerRange) > 65_536) {
            throw new IllegalArgumentException("range/protection Cartesian product exceeds 65536");
        }
    }

    public BookKeeperKeyspace keyspace(String cluster) {
        return new BookKeeperKeyspace(cluster, maxAppendRangesPerLedger, protectionSlotsPerRange,
                maxReaderLeasesPerLedger, maxUncertainAllocations);
    }
}
