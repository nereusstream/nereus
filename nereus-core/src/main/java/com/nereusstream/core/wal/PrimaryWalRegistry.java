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
public final class PrimaryWalRegistry {
    private final Map<ReadTargetType, PrimaryWalAppender<?>> appenders;
    private final ReadTargetReaderRegistry readers;
    public PrimaryWalRegistry(List<PrimaryWalAppender<?>> appenders, List<PrimaryWalReader> readers) {
        EnumMap<ReadTargetType, PrimaryWalAppender<?>> appenderMap = new EnumMap<>(ReadTargetType.class);
        appenders.forEach(value -> { if (appenderMap.put(value.targetType(), value) != null)
            throw new IllegalArgumentException("duplicate primary appender"); });
        this.appenders = Map.copyOf(appenderMap);
        this.readers = new ReadTargetReaderRegistry(readers);
    }
    public PrimaryWalAppender<?> requireAppender(ReadTargetType type) { return Objects.requireNonNullElseGet(
            appenders.get(type), () -> { throw new NereusException(ErrorCode.UNSUPPORTED_READ_TARGET, false,
                    "no primary WAL appender is registered for " + type); }); }
    public boolean hasAppender(ReadTargetType type) { return appenders.containsKey(type); }
    public ReadTargetReaderRegistry readerRegistry() { return readers; }
}
