/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class CursorStorageLimitExhaustionTest {
    @Test
    void marginAdjustedRootExhaustionFailsAckBeforeUploadAndLeavesTruthUnchanged() {
        CursorStorageConfig config = constrainedConfig(65_000, 4_096);
        try (CursorStorageTestSupport.Context context =
                new CursorStorageTestSupport.Context(0, 100, config)) {
            CursorOwnerSession owner = context.owner(CursorStorageTestSupport.OWNER_1);
            CursorHandle handle = context.storage.open(
                            owner,
                            "sub",
                            new CursorOpenRequest(
                                    new InitialCursorPosition.Earliest(),
                                    Map.of(),
                                    Map.of(),
                                    0,
                                    100))
                    .join();
            CursorState before = handle.state();

            assertThatThrownBy(() -> context.storage.individualAck(
                            handle,
                            List.of(whole(1), whole(3)))
                    .join())
                    .hasRootCauseMessage(
                            "cursor root cannot fit the margin-adjusted metadata bound after snapshot spill");

            assertThat(handle.state()).isEqualTo(before);
            assertThat(context.snapshotStore.objectCount()).isZero();
            assertThat(context.storage.claimAndLoadActiveCursors(owner).join())
                    .singleElement()
                    .satisfies(reloaded -> assertThat(reloaded.state()).isEqualTo(before));
        }
    }

    @Test
    void snapshotCapExhaustionFailsAckWithoutTruncatingTheLastAdmittedState() {
        CursorStorageConfig config = constrainedConfig(4_096, 2_048);
        try (CursorStorageTestSupport.Context context =
                new CursorStorageTestSupport.Context(0, 2_000, config)) {
            CursorOwnerSession owner = context.owner(CursorStorageTestSupport.OWNER_1);
            CursorHandle handle = context.storage.open(
                            owner,
                            "sub",
                            new CursorOpenRequest(
                                    new InitialCursorPosition.Earliest(),
                                    Map.of(),
                                    Map.of(),
                                    0,
                                    2_000))
                    .join();
            CursorMutationResult admitted = context.storage.individualAck(
                            handle, disjoint(1, 4, 80))
                    .join();
            assertThat(admitted.state().snapshotReference()).isPresent();
            assertThat(context.snapshotStore.objectCount()).isEqualTo(1);

            assertThatThrownBy(() -> context.storage.individualAck(
                            handle, disjoint(3, 4, 80))
                    .join())
                    .hasRootCauseMessage("cursor snapshot exceeds the configured object bound");

            assertThat(handle.state()).isEqualTo(admitted.state());
            assertThat(context.snapshotStore.objectCount()).isEqualTo(1);
            CursorState durable = context.storage
                    .claimAndLoadActiveCursors(owner)
                    .join()
                    .getFirst()
                    .state();
            assertThat(durable).isEqualTo(admitted.state());
            disjoint(1, 4, 80).forEach(request -> assertThat(durable
                            .acknowledgements()
                            .isWholeEntryAcknowledged(request.entryOffset()))
                    .isTrue());
            disjoint(3, 4, 80).forEach(request -> assertThat(durable
                            .acknowledgements()
                            .isWholeEntryAcknowledged(request.entryOffset()))
                    .isFalse());
        }
    }

    private static CursorAckRequest whole(long offset) {
        return new CursorAckRequest(offset, Optional.empty(), Map.of());
    }

    private static List<CursorAckRequest> disjoint(long first, long step, int count) {
        List<CursorAckRequest> requests = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            requests.add(whole(first + step * index));
        }
        return List.copyOf(requests);
    }

    private static CursorStorageConfig constrainedConfig(
            int metadataSafetyMargin, long snapshotMax) {
        CursorStorageConfig source = CursorStorageConfig.defaults();
        return new CursorStorageConfig(
                source.cursorMetadataValueMaxBytes(),
                metadataSafetyMargin,
                64,
                1,
                8,
                8,
                8,
                snapshotMax,
                source.cursorAckPositionsPerRequestMax(),
                64,
                2_048,
                source.cursorTrimReasonMaxUtf8Bytes(),
                source.cursorScanPageSize(),
                source.cursorRecordsPerStreamMax(),
                source.cursorOwnerClaimConcurrency(),
                source.cursorMutationQueueMax(),
                source.cursorMaxCasAttempts(),
                source.cursorHydrationMaxAttempts(),
                source.cursorSnapshotIdMaxAttempts(),
                source.cursorMetadataOperationTimeout(),
                source.cursorSnapshotOperationTimeout());
    }
}
