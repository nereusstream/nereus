/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.StreamId;
import com.nereusstream.api.keys.KeyComponentCodec;
import java.util.Objects;

/** The only durable key and partition-key constructor for F3 cursor metadata. */
public final class CursorKeyspace {
    private final OxiaKeyspace l0;

    public CursorKeyspace(String cluster) {
        this.l0 = new OxiaKeyspace(cluster);
    }

    public String cursorStateKey(StreamId streamId, String cursorName) {
        return cursorStatePrefix(streamId)
                + CursorNames.cursorNameHash(cursorName)
                + "/state";
    }

    public String cursorStateScanFrom(StreamId streamId) {
        return cursorStatePrefix(streamId);
    }

    public String cursorStateScanToExclusive(StreamId streamId) {
        return lexicalSuccessor(cursorStatePrefix(streamId));
    }

    public String retentionKey(StreamId streamId) {
        return cursorRootPrefix(streamId) + "retention";
    }

    public String cursorWatchPrefix(StreamId streamId) {
        return cursorRootPrefix(streamId);
    }

    public PartitionKey streamPartitionKey(StreamId streamId) {
        return l0.streamPartitionKey(Objects.requireNonNull(streamId, "streamId"));
    }

    private String cursorStatePrefix(StreamId streamId) {
        return cursorRootPrefix(streamId) + "by-hash/";
    }

    private String cursorRootPrefix(StreamId streamId) {
        Objects.requireNonNull(streamId, "streamId");
        return l0.prefix()
                + "/streams/"
                + KeyComponentCodec.encodeComponent(streamId.value())
                + "/facade/managed-ledger/cursors/v1/";
    }

    private static String lexicalSuccessor(String prefix) {
        char last = prefix.charAt(prefix.length() - 1);
        if (last == Character.MAX_VALUE) {
            throw new IllegalArgumentException("cursor scan prefix has no lexical successor");
        }
        return prefix.substring(0, prefix.length() - 1) + (char) (last + 1);
    }
}
