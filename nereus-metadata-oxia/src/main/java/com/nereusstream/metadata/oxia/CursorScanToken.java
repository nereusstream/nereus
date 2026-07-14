/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.StreamId;
import java.util.Objects;

/** Opaque scope-bound continuation token for one cursor-state scan. */
public final class CursorScanToken {
    private final String cluster;
    private final StreamId streamId;
    private final String scanPrefix;
    private final String exclusiveLastKey;

    CursorScanToken(String cluster, StreamId streamId, String scanPrefix, String exclusiveLastKey) {
        this.cluster = Objects.requireNonNull(cluster, "cluster");
        this.streamId = Objects.requireNonNull(streamId, "streamId");
        this.scanPrefix = Objects.requireNonNull(scanPrefix, "scanPrefix");
        this.exclusiveLastKey = Objects.requireNonNull(exclusiveLastKey, "exclusiveLastKey");
    }

    boolean matches(String expectedCluster, StreamId expectedStreamId, String expectedPrefix) {
        return cluster.equals(expectedCluster)
                && streamId.equals(expectedStreamId)
                && scanPrefix.equals(expectedPrefix)
                && exclusiveLastKey.startsWith(scanPrefix);
    }

    String exclusiveLastKey() {
        return exclusiveLastKey;
    }
}
