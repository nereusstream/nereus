/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Unambiguous cursor-property mutation shapes used by the storage state machine. */
public sealed interface CursorPropertyMutation {
    record Put(String key, String value) implements CursorPropertyMutation {
        public Put {
            key = requireKey(key);
            Objects.requireNonNull(value, "value");
        }
    }

    record Remove(String key) implements CursorPropertyMutation {
        public Remove {
            key = requireKey(key);
        }
    }

    record ReplaceExternal(Map<String, String> properties) implements CursorPropertyMutation {
        public ReplaceExternal {
            Objects.requireNonNull(properties, "properties");
            LinkedHashMap<String, String> copy = new LinkedHashMap<>();
            properties.forEach((key, value) -> {
                String exactKey = requireKey(key);
                if (exactKey.startsWith("#pulsar.internal.")) {
                    throw new IllegalArgumentException("external replacement cannot contain internal cursor keys");
                }
                copy.put(exactKey, Objects.requireNonNull(value, "properties contains null value"));
            });
            properties = Collections.unmodifiableMap(copy);
        }
    }

    private static String requireKey(String key) {
        Objects.requireNonNull(key, "key");
        if (key.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("cursor property key cannot contain NUL");
        }
        return key;
    }
}
