/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger;

import com.nereusstream.api.AppendAttemptId;
import java.util.Objects;

public record NereusWriteFenceSnapshot(
        long generation,
        AppendAttemptId attemptId) {
    public NereusWriteFenceSnapshot {
        if (generation < 1) {
            throw new IllegalArgumentException("write-fence generation must be positive");
        }
        Objects.requireNonNull(attemptId, "attemptId");
    }
}
