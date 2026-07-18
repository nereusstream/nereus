/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.keys.KeyComponentCodec;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.managedledger.cursor.CursorSnapshotKeys;
import com.nereusstream.materialization.gc.ObjectInventoryFamily;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointFormatV1;
import com.nereusstream.objectstore.compacted.CompactedObjectFormatV1;
import com.nereusstream.objectstore.wal.WalObjectKeys;
import java.time.Instant;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class Phase4ObjectInventoryFamiliesTest {
    private static final String CLUSTER = "cluster/a";
    private static final String STREAM = "stream-a";
    private static final String CONTENT = "c".repeat(64);
    private static final String ATTEMPT = "b".repeat(26);

    @Test
    void currentFamiliesStrictlyInvertEveryInstalledWriterKey() {
        Map<String, ObjectInventoryFamily> families = Phase4ObjectInventoryFamilies
                .currentV1(CLUSTER)
                .stream()
                .collect(Collectors.toUnmodifiableMap(
                        ObjectInventoryFamily::familyId, Function.identity()));

        assertThat(families).containsOnlyKeys(
                "wal-object-v1",
                "committed-compacted-v1",
                "topic-compacted-v1",
                "recovery-checkpoint-v1",
                "cursor-snapshot-v1");

        String writer = "a".repeat(52);
        String run = "run-hash";
        ObjectId walId = new ObjectId(
                "wo-20260718120000-" + writer + "-" + run + "-0000000000000000001");
        ObjectKey wal = WalObjectKeys.objectKey(
                CLUSTER, writer, run, walId, Instant.parse("2026-07-18T12:00:00Z"));
        var walParsed = families.get("wal-object-v1").parse(wal);
        assertThat(walParsed.kind()).isEqualTo(PhysicalObjectKind.OBJECT_WAL);
        assertThat(walParsed.objectId()).contains(walId);
        assertThat(walParsed.contentSha256()).isEmpty();

        ObjectKey committed = compacted(ReadView.COMMITTED);
        var committedParsed = families.get("committed-compacted-v1").parse(committed);
        assertThat(committedParsed.kind()).isEqualTo(PhysicalObjectKind.COMMITTED_COMPACTED);
        assertThat(committedParsed.objectId())
                .contains(CompactedObjectFormatV1.objectId(committed));
        assertThat(committedParsed.contentSha256().orElseThrow().value()).isEqualTo(CONTENT);

        ObjectKey topicCompacted = compacted(ReadView.TOPIC_COMPACTED);
        assertThat(families.get("topic-compacted-v1").parse(topicCompacted).kind())
                .isEqualTo(PhysicalObjectKind.TOPIC_COMPACTED);

        ObjectKey recovery = new ObjectKey(
                RecoveryCheckpointFormatV1.prefix(CLUSTER).value()
                        + KeyComponentCodec.encodeComponent(STREAM)
                        + "/0000000000000000001-"
                        + CONTENT
                        + "-"
                        + ATTEMPT
                        + ".nrc");
        var recoveryParsed = families.get("recovery-checkpoint-v1").parse(recovery);
        assertThat(recoveryParsed.kind()).isEqualTo(PhysicalObjectKind.RECOVERY_CHECKPOINT);
        assertThat(recoveryParsed.objectId())
                .contains(RecoveryCheckpointFormatV1.objectId(recovery));

        ObjectKey cursor = new ObjectKey(
                CursorSnapshotKeys.clusterPrefix(CLUSTER).value()
                        + KeyComponentCodec.encodeComponent(STREAM)
                        + "/"
                        + "d".repeat(52)
                        + "/0000000000000000001/"
                        + "1".repeat(32)
                        + ".ncs");
        var cursorParsed = families.get("cursor-snapshot-v1").parse(cursor);
        assertThat(cursorParsed.kind()).isEqualTo(PhysicalObjectKind.CURSOR_SNAPSHOT);
        assertThat(cursorParsed.objectId()).isEmpty();
        assertThat(cursorParsed.contentSha256()).isEmpty();
    }

    @Test
    void malformedKeysCannotBePromotedByPrefixMembership() {
        Map<String, ObjectInventoryFamily> families = Phase4ObjectInventoryFamilies
                .currentV1(CLUSTER)
                .stream()
                .collect(Collectors.toMap(
                        ObjectInventoryFamily::familyId, Function.identity()));

        ObjectKey compacted = compacted(ReadView.COMMITTED);
        assertThatThrownBy(() -> families.get("committed-compacted-v1").parse(
                        new ObjectKey(compacted.value().replace(CONTENT, CONTENT.toUpperCase()))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> families.get("recovery-checkpoint-v1").parse(new ObjectKey(
                        RecoveryCheckpointFormatV1.prefix(CLUSTER).value()
                                + KeyComponentCodec.encodeComponent(STREAM)
                                + "/0000000000000000000-"
                                + CONTENT
                                + "-"
                                + ATTEMPT
                                + ".nrc")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> families.get("cursor-snapshot-v1").parse(new ObjectKey(
                        CursorSnapshotKeys.clusterPrefix(CLUSTER).value()
                                + KeyComponentCodec.encodeComponent(STREAM)
                                + "/"
                                + "d".repeat(52)
                                + "/0000000000000000001/"
                                + "1".repeat(32)
                                + ".ncs/extra")))
                .isInstanceOf(IllegalArgumentException.class);
        String writer = "a".repeat(52);
        ObjectId walId = new ObjectId(
                "wo-20260718120000-" + writer + "-run-hash-0000000000000000001");
        ObjectKey wal = WalObjectKeys.objectKey(
                CLUSTER, writer, "run-hash", walId, Instant.parse("2026-07-18T12:00:00Z"));
        assertThatThrownBy(() -> families.get("wal-object-v1").parse(new ObjectKey(
                        wal.value().replace("/2026/07/18/", "/2026/07/17/"))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static ObjectKey compacted(ReadView view) {
        return new ObjectKey(
                CompactedObjectFormatV1.prefix(CLUSTER, view).value()
                        + KeyComponentCodec.encodeComponent(STREAM)
                        + "/0000000000000000000-0000000000000000002/"
                        + CONTENT
                        + "-"
                        + ATTEMPT
                        + ".parquet");
    }
}
