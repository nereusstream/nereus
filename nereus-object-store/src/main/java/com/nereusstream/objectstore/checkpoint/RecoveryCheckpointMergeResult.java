/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.checkpoint;

import java.util.Objects;

/** Canonical merged NRC1 header plus its close-owned staged object. */
public record RecoveryCheckpointMergeResult(
        RecoveryCheckpointWriteRequest request,
        RecoveryCheckpointWriteResult object,
        int sourceCount) implements AutoCloseable {
    public RecoveryCheckpointMergeResult {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(object, "object");
        if (sourceCount < 2
                || !object.objectKey().equals(
                        RecoveryCheckpointFormatV1.objectKey(request, object.contentSha256()))) {
            throw new IllegalArgumentException("merged checkpoint result identity is inconsistent");
        }
    }

    @Override
    public void close() {
        object.close();
    }
}
