/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.StreamId;
import com.nereusstream.api.keys.KeyComponentCodec;
import java.util.Objects;

/** The only durable key/partition-key constructor for F2 projection metadata. */
public final class ManagedLedgerProjectionKeyspace {
    private static final PartitionKey ALLOCATOR_PARTITION_KEY =
            new PartitionKey("managed-ledger-ledger-id-allocator");

    private final OxiaKeyspace l0;
    private final String projectionPrefix;

    public ManagedLedgerProjectionKeyspace(String cluster) {
        this.l0 = new OxiaKeyspace(cluster);
        this.projectionPrefix = l0.prefix() + "/facade/managed-ledger";
    }

    public String ledgerIdAllocatorKey() {
        return projectionPrefix + "/ledger-id-allocator";
    }

    public PartitionKey ledgerIdAllocatorPartitionKey() {
        return ALLOCATOR_PARTITION_KEY;
    }

    public String topicProjectionKey(String managedLedgerName) {
        return projectionPrefix + "/topics/" + managedLedgerNameHash(managedLedgerName);
    }

    public PartitionKey topicProjectionPartitionKey(String managedLedgerName) {
        return new PartitionKey("managed-ledger-topic/" + managedLedgerNameHash(managedLedgerName));
    }

    public String virtualLedgerProjectionKey(StreamId streamId) {
        return streamProjectionPrefix(streamId) + "/virtual-ledger";
    }

    public String positionIndexKey(StreamId streamId) {
        return streamProjectionPrefix(streamId) + "/position-index";
    }

    public PartitionKey streamPartitionKey(StreamId streamId) {
        return l0.streamPartitionKey(Objects.requireNonNull(streamId, "streamId"));
    }

    public static String managedLedgerNameHash(String managedLedgerName) {
        return ManagedLedgerProjectionNames.managedLedgerNameHash(managedLedgerName);
    }

    private String streamProjectionPrefix(StreamId streamId) {
        Objects.requireNonNull(streamId, "streamId");
        return l0.prefix()
                + "/streams/"
                + KeyComponentCodec.encodeComponent(streamId.value())
                + "/facade/managed-ledger";
    }
}
