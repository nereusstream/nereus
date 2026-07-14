/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class CursorSnapshotInventoryTest {
    @Test
    void trimPendingVetoesDryRunDeletionAndAnyAuthorityChangeInvalidatesTheInventory() {
        try (CursorStorageTestSupport.Context context =
                new CursorStorageTestSupport.Context(0, 20)) {
            CursorOwnerSession owner = context.owner(CursorStorageTestSupport.OWNER_1);
            context.storage.claimAndLoadActiveCursors(owner).join();
            StreamId streamId = new StreamId(context.ledger.projection().streamId());
            var activeRetention = context.metadataStore.getRetention(
                            CursorStorageTestSupport.CLUSTER, streamId)
                    .join()
                    .orElseThrow();
            CursorSnapshotInventory active = CursorSnapshotInventory.classify(
                    CursorStorageTestSupport.CLUSTER,
                    context.ledger,
                    activeRetention,
                    List.of(),
                    Set.of());
            assertThat(active.deletionVetoed()).isFalse();

            context.streamStorage.failNextTrimBeforeApply(new NereusException(
                    ErrorCode.METADATA_UNAVAILABLE,
                    true,
                    "trim transport unavailable after pending publication"));
            assertThatThrownBy(() -> context.retention.requestTrim(owner, 10, "inventory-veto").join())
                    .hasCauseInstanceOf(NereusException.class);

            var trimPending = context.metadataStore.getRetention(
                            CursorStorageTestSupport.CLUSTER, streamId)
                    .join()
                    .orElseThrow();
            CursorSnapshotInventory pending = CursorSnapshotInventory.classify(
                    CursorStorageTestSupport.CLUSTER,
                    context.ledger,
                    trimPending,
                    List.of(),
                    Set.of());
            assertThat(pending.authority().retentionLifecycle())
                    .isEqualTo(com.nereusstream.metadata.oxia.records.CursorRetentionLifecycle.TRIM_PENDING);
            assertThat(pending.deletionVetoed()).isTrue();
            assertThat(active.stillMatches(trimPending, List.of())).isFalse();
        }
    }
}
