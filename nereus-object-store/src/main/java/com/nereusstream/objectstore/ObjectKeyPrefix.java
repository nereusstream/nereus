/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

import java.util.Objects;

/** Canonical relative object-key prefix for bounded provider listing. */
public record ObjectKeyPrefix(String value) {
    public ObjectKeyPrefix {
        Objects.requireNonNull(value, "value");
        if (value.isBlank() || value.startsWith("/") || value.contains("\\") || value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("object key prefix must be canonical and relative");
        }
        for (String segment : value.split("/", -1)) {
            if (segment.equals(".") || segment.equals("..")) {
                throw new IllegalArgumentException("object key prefix contains traversal");
            }
        }
    }
}
