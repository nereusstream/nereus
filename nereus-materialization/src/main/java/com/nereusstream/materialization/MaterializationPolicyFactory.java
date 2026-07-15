/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.ReadView;
import java.util.Optional;

/** Constructs versioned built-in policies from validated operator-controlled semantic fields. */
public final class MaterializationPolicyFactory {
    public static final String LOSSLESS_COMMITTED_POLICY_ID = "nereus-committed-default";

    private MaterializationPolicyFactory() {
    }

    public static MaterializationPolicy losslessCommitted(
            int minMergeSourceRanges,
            int maxSourceRanges,
            long maxRangeRecords,
            long targetObjectBytes,
            int targetRowGroupRecords,
            String compression) {
        long policyVersion = MaterializationCanonical.operatorPolicyVersion(
                minMergeSourceRanges,
                maxSourceRanges,
                maxRangeRecords,
                targetObjectBytes,
                targetRowGroupRecords,
                compression);
        return new MaterializationPolicy(
                LOSSLESS_COMMITTED_POLICY_ID,
                policyVersion,
                ReadView.COMMITTED,
                TaskKind.LOSSLESS_REWRITE,
                MaterializationPolicy.COMMITTED_FORMAT,
                minMergeSourceRanges,
                maxSourceRanges,
                maxRangeRecords,
                targetObjectBytes,
                targetRowGroupRecords,
                compression,
                Optional.empty());
    }
}
