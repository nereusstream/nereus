/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.core.read.ReadTargetReader;
import java.util.Objects;

/** Runtime pair that makes one primary target both readable and durably protectable by F4. */
public record MaterializationSourceProvider(
        ReadTargetReader reader,
        MaterializationSourceProtectionAdapter<?> protectionAdapter) {
    public MaterializationSourceProvider {
        reader = Objects.requireNonNull(reader, "reader");
        protectionAdapter = Objects.requireNonNull(
                protectionAdapter, "protectionAdapter");
        if (reader.key().targetType() != protectionAdapter.targetType()) {
            throw new IllegalArgumentException(
                    "materialization source reader and protection adapter target types differ");
        }
    }
}
