/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.append;

import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.keys.DeterministicIds;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.metadata.oxia.CommittedAppend;
import com.nereusstream.metadata.oxia.MaterializedGenerationZero;
import com.nereusstream.metadata.oxia.PreparedStableAppend;

/** Canonical V1 protection identities shared by validation and publication. */
final class GenerationZeroProtectionIdentities {
    private GenerationZeroProtectionIdentities() {
    }

    static String reachableAppendReferenceId(PreparedStableAppend prepared) {
        return "ra1-" + DeterministicIds.stableHashComponent(
                prepared.request().streamId().value()
                        + prepared.commitId()
                        + prepared.objectKeyHash().value());
    }

    static String visibleGenerationReferenceId(MaterializedGenerationZero materialized) {
        return "vg0-" + DeterministicIds.stableHashComponent(
                materialized.committedAppend().streamId().value()
                        + materialized.indexKey()
                        + materialized.indexRecordSha256().value());
    }

    static ObjectKeyHash objectKeyHash(CommittedAppend append) {
        if (!(append.readTarget() instanceof ObjectSliceReadTarget target)) {
            throw new IllegalArgumentException("generation-zero append target must be an object slice");
        }
        return ObjectKeyHash.from(target.objectKey());
    }
}
