/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.recovery;

/** Durable authority that proved one exact append replay result. */
public enum AppendReplayEvidenceSource {
    LIVE_COMMIT,
    RECOVERY_CHECKPOINT
}
