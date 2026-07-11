/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */
package com.nereusstream.api;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Opaque process-local identity for recovering one exact append attempt. */
public record AppendAttemptId(String value) {
    public AppendAttemptId {
        Objects.requireNonNull(value, "value");
        int length = value.getBytes(StandardCharsets.UTF_8).length;
        if (value.isBlank() || length > ApiLimits.MAX_APPEND_ATTEMPT_ID_ENCODED_BYTES) {
            throw new IllegalArgumentException("append attempt ID must be nonblank and bounded");
        }
    }
}
