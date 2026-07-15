/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

/** Durable L0 commit encoding from which one canonical generic recovery envelope was derived. */
public enum AppendRecoveryCommitEncoding {
    LEGACY_STREAM_COMMIT_V1,
    GENERIC_STREAM_COMMIT_TARGET_V1
}
