/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ReadView;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.managedledger.cursor.CursorSnapshotKeys;
import com.nereusstream.materialization.gc.ObjectInventoryFamily;
import com.nereusstream.materialization.gc.ObjectInventoryKey;
import com.nereusstream.objectstore.ObjectKeyPrefix;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointFormatV1;
import com.nereusstream.objectstore.compacted.CompactedObjectFormatV1;
import com.nereusstream.objectstore.wal.WalObjectKeys;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/** Exact V1 object-key families written by the currently installed Nereus runtime. */
final class Phase4ObjectInventoryFamilies {
    private Phase4ObjectInventoryFamilies() {
    }

    static List<ObjectInventoryFamily> currentV1(String cluster) {
        String exactCluster = requireText(cluster, "cluster");
        return List.of(
                family(
                        "wal-object-v1",
                        WalObjectKeys.prefix(exactCluster),
                        key -> {
                            var parsed = WalObjectKeys.parse(exactCluster, key);
                            return new ObjectInventoryKey(
                                    key,
                                    Optional.of(parsed.objectId()),
                                    PhysicalObjectKind.OBJECT_WAL,
                                    Optional.empty());
                        }),
                compacted(exactCluster, ReadView.COMMITTED),
                compacted(exactCluster, ReadView.TOPIC_COMPACTED),
                family(
                        "recovery-checkpoint-v1",
                        RecoveryCheckpointFormatV1.prefix(exactCluster),
                        key -> {
                            var parsed = RecoveryCheckpointFormatV1.parseObjectKey(
                                    exactCluster, key);
                            return new ObjectInventoryKey(
                                    key,
                                    Optional.of(parsed.objectId()),
                                    PhysicalObjectKind.RECOVERY_CHECKPOINT,
                                    Optional.of(parsed.contentSha256()));
                        }),
                family(
                        "cursor-snapshot-v1",
                        CursorSnapshotKeys.clusterPrefix(exactCluster),
                        key -> {
                            CursorSnapshotKeys.parseOwnerless(exactCluster, key);
                            return new ObjectInventoryKey(
                                    key,
                                    Optional.empty(),
                                    PhysicalObjectKind.CURSOR_SNAPSHOT,
                                    Optional.empty());
                        }));
    }

    private static ObjectInventoryFamily compacted(
            String cluster, ReadView view) {
        String id = view == ReadView.COMMITTED
                ? "committed-compacted-v1"
                : "topic-compacted-v1";
        PhysicalObjectKind kind = view == ReadView.COMMITTED
                ? PhysicalObjectKind.COMMITTED_COMPACTED
                : PhysicalObjectKind.TOPIC_COMPACTED;
        return family(
                id,
                CompactedObjectFormatV1.prefix(cluster, view),
                key -> {
                    var parsed = CompactedObjectFormatV1.parseObjectKey(
                            cluster, view, key);
                    return new ObjectInventoryKey(
                            key,
                            Optional.of(parsed.objectId()),
                            kind,
                            Optional.of(parsed.contentSha256()));
                });
    }

    private static ObjectInventoryFamily family(
            String id,
            ObjectKeyPrefix prefix,
            Function<ObjectKey, ObjectInventoryKey> parser) {
        return new Family(id, prefix, parser);
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }

    private record Family(
            String familyId,
            ObjectKeyPrefix prefix,
            Function<ObjectKey, ObjectInventoryKey> parser)
            implements ObjectInventoryFamily {
        private Family {
            familyId = requireText(familyId, "familyId");
            Objects.requireNonNull(prefix, "prefix");
            Objects.requireNonNull(parser, "parser");
        }

        @Override
        public ObjectInventoryKey parse(ObjectKey key) {
            return parser.apply(Objects.requireNonNull(key, "key"));
        }
    }
}
