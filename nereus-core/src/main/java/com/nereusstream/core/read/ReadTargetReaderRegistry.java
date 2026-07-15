/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.target.ReadTarget;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Immutable exact-key reader registry with fail-closed lookup. */
public final class ReadTargetReaderRegistry {
    private final Map<ReadTargetReaderKey, ReadTargetReader> readers;

    public ReadTargetReaderRegistry(Collection<? extends ReadTargetReader> readers) {
        Objects.requireNonNull(readers, "readers");
        try {
            this.readers = readers.stream()
                    .map(reader -> Objects.requireNonNull(reader, "reader"))
                    .collect(Collectors.toUnmodifiableMap(
                            ReadTargetReader::key, Function.identity(), (left, right) -> {
                                throw new IllegalArgumentException(
                                        "duplicate read target reader key: " + left.key());
                            }));
        } catch (IllegalStateException duplicate) {
            throw new IllegalArgumentException("duplicate read target reader key", duplicate);
        }
    }

    public ReadTargetReader require(ReadTarget target) {
        return require(ReadTargetReaderKey.from(target));
    }

    public ReadTargetReader require(ReadTargetReaderKey key) {
        ReadTargetReader reader = readers.get(Objects.requireNonNull(key, "key"));
        if (reader == null) {
            throw new NereusException(
                    ErrorCode.UNSUPPORTED_READ_TARGET,
                    false,
                    "no physical reader is registered for " + key);
        }
        return reader;
    }

    public boolean contains(ReadTargetReaderKey key) {
        return readers.containsKey(Objects.requireNonNull(key, "key"));
    }

    public int size() {
        return readers.size();
    }
}
