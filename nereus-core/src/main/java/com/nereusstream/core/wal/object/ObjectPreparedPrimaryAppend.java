/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.wal.object;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.wal.PreparedPrimaryAppend;
import com.nereusstream.core.wal.PrimaryAppendRequest;
import com.nereusstream.objectstore.wal.PreparedWalObject;
import java.util.Objects;
public record ObjectPreparedPrimaryAppend(StreamId streamId, int recordCount, int entryCount,
        long logicalBytes, long reservedBytes, PreparedWalObject preparedObject,
        PrimaryAppendRequest request) implements PreparedPrimaryAppend {
    public ObjectPreparedPrimaryAppend { Objects.requireNonNull(streamId); Objects.requireNonNull(preparedObject);
        Objects.requireNonNull(request); if (!request.streamId().equals(streamId))
            throw new IllegalArgumentException("prepared Object WAL request stream does not match");
        if (recordCount <= 0 || entryCount <= 0 || logicalBytes < 0 || reservedBytes <= 0)
            throw new IllegalArgumentException("invalid prepared Object WAL append"); }
}
