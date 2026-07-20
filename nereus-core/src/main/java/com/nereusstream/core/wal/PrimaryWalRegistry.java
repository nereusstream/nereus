/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.wal;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.target.ReadTargetType;
import com.nereusstream.core.read.ReadTargetReaderRegistry;
import java.util.ArrayList;
import java.util.Collection;
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
    private final List<PrimaryWalAppender<?>> installedAppenders;
    private final List<PrimaryWalReader> installedReaders;
    public PrimaryWalRegistry(List<PrimaryWalAppender<?>> appenders, List<PrimaryWalReader> readers) {
        Objects.requireNonNull(appenders, "appenders");
        Objects.requireNonNull(readers, "readers");
        EnumMap<ReadTargetType, PrimaryWalAppender<?>> appenderMap = new EnumMap<>(ReadTargetType.class);
        appenders.forEach(value -> { Objects.requireNonNull(value, "appender");
            if (value.preparedClass() == null) throw new IllegalArgumentException("primary appender prepared class");
            if (appenderMap.put(value.targetType(), value) != null)
            throw new IllegalArgumentException("duplicate primary appender"); });
        this.installedAppenders = List.copyOf(appenders);
        this.installedReaders = List.copyOf(readers);
        this.appenders = Map.copyOf(appenderMap);
        this.readers = new ReadTargetReaderRegistry(installedReaders);
        this.readerTypes = installedReaders.stream()
                .map(reader -> Objects.requireNonNull(reader, "reader").key().targetType())
                .collect(Collectors.toUnmodifiableSet());
    }

    /** Merges independently owned provider adapters while retaining constructor duplicate rejection. */
    public static PrimaryWalRegistry combine(Collection<PrimaryWalRegistry> registries) {
        Objects.requireNonNull(registries, "registries");
        ArrayList<PrimaryWalAppender<?>> appenders = new ArrayList<>();
        ArrayList<PrimaryWalReader> readers = new ArrayList<>();
        registries.forEach(registry -> {
            PrimaryWalRegistry exact = Objects.requireNonNull(registry, "registry");
            appenders.addAll(exact.installedAppenders);
            readers.addAll(exact.installedReaders);
        });
        return new PrimaryWalRegistry(appenders, readers);
    }
    public PrimaryWalAppender<?> requireAppender(ReadTargetType type) { return Objects.requireNonNullElseGet(
            appenders.get(type), () -> { throw new NereusException(ErrorCode.UNSUPPORTED_READ_TARGET, false,
                    "no primary WAL appender is registered for " + type); }); }
    public boolean hasAppender(ReadTargetType type) { return appenders.containsKey(type); }
    public boolean hasReader(ReadTargetType type) { return readerTypes.contains(type); }
    public ReadTargetReaderRegistry readerRegistry() { return readers; }
}
