/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;

import com.nereusstream.api.ReadResult;
import com.nereusstream.api.ReadView;
import java.util.Objects;

/** Read result plus the exclusive dense source coverage consumed by a semantic view. */
public record ViewReadResult(
        ReadView view,
        ReadResult result,
        long sourceCoverageEndOffset) {
    public ViewReadResult {
        Objects.requireNonNull(view, "view");
        Objects.requireNonNull(result, "result");
        if (sourceCoverageEndOffset < result.nextOffset()) {
            throw new IllegalArgumentException("source coverage cannot precede the returned cursor");
        }
        if (view == ReadView.COMMITTED && sourceCoverageEndOffset != result.nextOffset()) {
            throw new IllegalArgumentException("COMMITTED source coverage must equal result.nextOffset");
        }
    }
}
