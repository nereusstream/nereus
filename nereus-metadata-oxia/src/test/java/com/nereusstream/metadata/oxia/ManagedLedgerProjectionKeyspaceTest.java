/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.StreamId;
import com.nereusstream.api.keys.KeyComponentCodec;
import org.junit.jupiter.api.Test;

class ManagedLedgerProjectionKeyspaceTest {
    @Test
    void buildsEveryLockedProjectionKeyAndPartitionKey() {
        String cluster = "cluster/a";
        String name = "tenant/ns/persistent/topic";
        StreamId streamId = ManagedLedgerProjectionNames.streamId(name, 1);
        ManagedLedgerProjectionKeyspace keyspace = new ManagedLedgerProjectionKeyspace(cluster);
        String prefix = "/nereus/clusters/" + KeyComponentCodec.encodeComponent(cluster);
        String streamPrefix = prefix + "/streams/" + KeyComponentCodec.encodeComponent(streamId.value());
        String hash = ManagedLedgerProjectionNames.managedLedgerNameHash(name);

        assertThat(keyspace.ledgerIdAllocatorKey())
                .isEqualTo(prefix + "/facade/managed-ledger/ledger-id-allocator");
        assertThat(keyspace.ledgerIdAllocatorPartitionKey().value())
                .isEqualTo("managed-ledger-ledger-id-allocator");
        assertThat(keyspace.topicProjectionKey(name))
                .isEqualTo(prefix + "/facade/managed-ledger/topics/" + hash);
        assertThat(keyspace.topicProjectionPartitionKey(name).value())
                .isEqualTo("managed-ledger-topic/" + hash);
        assertThat(keyspace.virtualLedgerProjectionKey(streamId))
                .isEqualTo(streamPrefix + "/facade/managed-ledger/virtual-ledger");
        assertThat(keyspace.positionIndexKey(streamId))
                .isEqualTo(streamPrefix + "/facade/managed-ledger/position-index");
        assertThat(keyspace.streamPartitionKey(streamId).value()).isEqualTo(streamId.value());
    }

    @Test
    void keepsTopicPartitionStableAcrossIncarnations() {
        ManagedLedgerProjectionKeyspace keyspace = new ManagedLedgerProjectionKeyspace("cluster");
        String name = "tenant/ns/topic";

        assertThat(keyspace.topicProjectionPartitionKey(name))
                .isEqualTo(keyspace.topicProjectionPartitionKey(name));
        assertThat(keyspace.streamPartitionKey(ManagedLedgerProjectionNames.streamId(name, 1)))
                .isNotEqualTo(keyspace.streamPartitionKey(ManagedLedgerProjectionNames.streamId(name, 2)));
    }
}
