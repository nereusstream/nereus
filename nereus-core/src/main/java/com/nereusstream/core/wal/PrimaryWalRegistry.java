/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.wal;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.target.ReadTargetType;
import com.nereusstream.core.read.ReadTargetReaderRegistry;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
public final class PrimaryWalRegistry {
    private final Map<ReadTargetType, PrimaryWalAppender<?>> appenders;
    private final ReadTargetReaderRegistry readers;
    private final Set<ReadTargetType> readerTypes;
    public PrimaryWalRegistry(List<PrimaryWalAppender<?>> appenders, List<PrimaryWalReader> readers) {
        Objects.requireNonNull(appenders, "appenders");
        Objects.requireNonNull(readers, "readers");
        EnumMap<ReadTargetType, PrimaryWalAppender<?>> appenderMap = new EnumMap<>(ReadTargetType.class);
        appenders.forEach(value -> { Objects.requireNonNull(value, "appender");
            if (value.preparedClass() == null) throw new IllegalArgumentException("primary appender prepared class");
            if (appenderMap.put(value.targetType(), value) != null)
            throw new IllegalArgumentException("duplicate primary appender"); });
        this.appenders = Map.copyOf(appenderMap);
        this.readers = new ReadTargetReaderRegistry(readers);
        this.readerTypes = readers.stream()
                .map(reader -> Objects.requireNonNull(reader, "reader").key().targetType())
                .collect(Collectors.toUnmodifiableSet());
    }
    public PrimaryWalAppender<?> requireAppender(ReadTargetType type) { return Objects.requireNonNullElseGet(
            appenders.get(type), () -> { throw new NereusException(ErrorCode.UNSUPPORTED_READ_TARGET, false,
                    "no primary WAL appender is registered for " + type); }); }
    public boolean hasAppender(ReadTargetType type) { return appenders.containsKey(type); }
    public boolean hasReader(ReadTargetType type) { return readerTypes.contains(type); }
    public ReadTargetReaderRegistry readerRegistry() { return readers; }
}
