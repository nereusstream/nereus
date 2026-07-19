/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */
package com.nereusstream.api;

import com.nereusstream.api.target.ReadTarget;
import java.util.Objects;

/** Exact resolved physical source that produced one or more logical read batches. */
public record ReadSourceRef(
        OffsetRange resolvedRange,
        long generation,
        long commitVersion,
        ReadTarget target,
        Checksum targetIdentity) {
    public ReadSourceRef {
        Objects.requireNonNull(resolvedRange, "resolvedRange");
        Objects.requireNonNull(target, "target");
        Objects.requireNonNull(targetIdentity, "targetIdentity");
        if (resolvedRange.isEmpty() || generation < 0 || commitVersion <= 0
                || targetIdentity.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException("invalid read source reference");
        }
    }
}
