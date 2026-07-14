/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.StreamId;
import com.nereusstream.api.keys.KeyComponentCodec;
import org.junit.jupiter.api.Test;

class CursorKeyspaceTest {
    @Test
    void constructsExactHashedCursorAndRetentionPathsOnTheStreamPartition() {
        String cluster = "cluster/a";
        StreamId streamId = ManagedLedgerProjectionNames.streamId(
                "persistent://tenant/ns/topic", 1);
        String cursor = "sub/one";
        CursorKeyspace keyspace = new CursorKeyspace(cluster);
        String prefix = "/nereus/clusters/"
                + KeyComponentCodec.encodeComponent(cluster)
                + "/streams/"
                + KeyComponentCodec.encodeComponent(streamId.value())
                + "/facade/managed-ledger/cursors/v1/";

        assertThat(keyspace.cursorStateKey(streamId, cursor))
                .isEqualTo(prefix + "by-hash/" + CursorNames.cursorNameHash(cursor) + "/state");
        assertThat(keyspace.cursorStateScanFrom(streamId)).isEqualTo(prefix + "by-hash/0/");
        assertThat(keyspace.cursorStateScanToExclusive(streamId))
                .isEqualTo(prefix + "by-hash/~/");
        assertThat(keyspace.retentionKey(streamId)).isEqualTo(prefix + "retention");
        assertThat(keyspace.cursorWatchPrefix(streamId)).isEqualTo(prefix);
        assertThat(keyspace.streamPartitionKey(streamId).value()).isEqualTo(streamId.value());
    }

    @Test
    void cursorNamesUseStrictUtf8AndTheLockedHashDomain() {
        assertThat(CursorNames.cursorNameHash("subscription"))
                .isEqualTo(com.nereusstream.api.keys.DeterministicIds.stableHashComponent(
                        "nereus-cursor-v1\0subscription"));
        assertThatThrownBy(() -> CursorNames.requireCursorName("\0"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CursorNames.requireCursorName("\ud800"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("UTF-8");
    }
}
