/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

import com.nereusstream.api.ObjectKey;
import java.util.Objects;

/** Strict one-prefix mapping from protocol-neutral keys to S3 keys. */
public final class S3ObjectKeyMapper {
    private final String prefix;
    public S3ObjectKeyMapper(String prefix) { this.prefix = requirePath(prefix, "prefix"); }
    public String map(ObjectKey key) { return prefix + "/" + requirePath(Objects.requireNonNull(key, "key").value(), "object key"); }
    private static String requirePath(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank() || value.startsWith("/") || value.endsWith("/")
                || value.chars().anyMatch(c -> c == 0 || Character.isISOControl(c))) throw new IllegalArgumentException(name + " is not canonical");
        for (String part : value.split("/", -1)) if (part.isEmpty() || part.equals(".") || part.equals("..")) throw new IllegalArgumentException(name + " is not canonical");
        return value;
    }
}
