/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.api;

import java.time.Duration;

/** Options for logically deleting a stream. */
public record DeleteOptions(Duration timeout, String reason) {
    public DeleteOptions {
        SealOptions.validate(timeout, reason);
    }
}
