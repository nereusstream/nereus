/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;

/** Provider-neutral physical-read failure category consumed by retry/fallback/quarantine policy. */
public enum PhysicalReadFailureKind {
    NOT_FOUND,
    CHECKSUM_MISMATCH,
    TRANSIENT_IO,
    FENCED_OR_CLOSED,
    AUTHENTICATION,
    METADATA_INVARIANT,
    UNSUPPORTED_TARGET
}
