/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;

/** Authoritative evidence domain that decided one missing generation-index repair. */
public enum GenerationIndexRepairSource {
    TRIMMED,
    LIVE_COMMIT,
    RECOVERY_CHECKPOINT
}
