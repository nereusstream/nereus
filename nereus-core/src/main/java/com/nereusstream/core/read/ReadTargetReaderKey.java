/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;

import com.nereusstream.api.ObjectType;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.api.target.ReadTarget;
import com.nereusstream.api.target.ReadTargetType;
import java.util.Objects;
import java.util.Optional;

/** Exact physical reader discriminator; target type alone is intentionally insufficient. */
public record ReadTargetReaderKey(
        ReadTargetType targetType,
        int targetVersion,
        Optional<ObjectType> objectType,
        Optional<String> physicalFormat,
        Optional<String> logicalFormat) {
    public ReadTargetReaderKey {
        Objects.requireNonNull(targetType, "targetType");
        objectType = Objects.requireNonNull(objectType, "objectType");
        physicalFormat = Objects.requireNonNull(physicalFormat, "physicalFormat")
                .map(value -> requireText(value, "physicalFormat"));
        logicalFormat = Objects.requireNonNull(logicalFormat, "logicalFormat")
                .map(value -> requireText(value, "logicalFormat"));
        if (targetVersion <= 0) {
            throw new IllegalArgumentException("targetVersion must be positive");
        }
        switch (targetType) {
            case OBJECT_SLICE -> {
                if (objectType.isEmpty() || physicalFormat.isEmpty() || logicalFormat.isEmpty()) {
                    throw new IllegalArgumentException(
                            "object-slice reader key requires objectType, physicalFormat, and logicalFormat");
                }
            }
            case BOOKKEEPER_ENTRY_RANGE -> {
                if (objectType.isPresent() || physicalFormat.isPresent() || logicalFormat.isPresent()) {
                    throw new IllegalArgumentException(
                            "BookKeeper reader key cannot carry object fields");
                }
            }
        }
    }

    /** Compatibility constructor for non-object target keys. */
    public ReadTargetReaderKey(
            ReadTargetType targetType,
            int targetVersion,
            Optional<ObjectType> objectType,
            Optional<String> physicalFormat) {
        this(targetType, targetVersion, objectType, physicalFormat, Optional.empty());
    }

    public static ReadTargetReaderKey from(ReadTarget target) {
        Objects.requireNonNull(target, "target");
        if (target instanceof ObjectSliceReadTarget object) {
            return new ReadTargetReaderKey(
                    object.type(), object.version(), Optional.of(object.objectType()),
                    Optional.of(object.physicalFormat()), Optional.of(object.logicalFormat()));
        }
        if (target instanceof BookKeeperEntryRangeReadTarget bookKeeper) {
            return new ReadTargetReaderKey(
                    bookKeeper.type(), bookKeeper.version(), Optional.empty(), Optional.empty());
        }
        throw new IllegalArgumentException("unknown read target Java type: " + target.getClass().getName());
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank() || value.length() > 16_384) {
            throw new IllegalArgumentException(field + " must be non-blank and bounded");
        }
        return value;
    }
}
