/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

import com.nereusstream.api.ObjectKey;
import java.util.Objects;

public record DeleteObjectResult(ObjectKey key, Status status) {
    public DeleteObjectResult {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(status, "status");
    }

    public enum Status {
        DELETED,
        ALREADY_ABSENT
    }
}
