/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import com.nereusstream.metadata.oxia.records.ReferenceDomainVersionRecord;
import java.util.List;

/** Exact currently installed F4 V1 reference-domain protocol set. */
final class NereusGenerationProtocolReferenceDomains {
    private static final List<ReferenceDomainVersionRecord> V1 = List.of(
            new ReferenceDomainVersionRecord("append-recovery-v1", 1),
            new ReferenceDomainVersionRecord("cursor-snapshot-v1", 1),
            new ReferenceDomainVersionRecord("future-catalog-sentinel-v1", 1),
            new ReferenceDomainVersionRecord("generation-v1", 1),
            new ReferenceDomainVersionRecord("materialization-v1", 1),
            new ReferenceDomainVersionRecord("projection-generation-v1", 1));

    private NereusGenerationProtocolReferenceDomains() {
    }

    static List<ReferenceDomainVersionRecord> currentV1() {
        return V1;
    }
}
