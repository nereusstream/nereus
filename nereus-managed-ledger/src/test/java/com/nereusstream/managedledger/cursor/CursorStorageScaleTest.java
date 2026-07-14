/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.CursorNames;
import com.nereusstream.metadata.oxia.CursorScanPage;
import com.nereusstream.metadata.oxia.CursorScanToken;
import com.nereusstream.metadata.oxia.ManagedLedgerCursorProtocol;
import com.nereusstream.metadata.oxia.records.CursorRecordLifecycle;
import com.nereusstream.metadata.oxia.records.CursorStateRecord;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.junit.jupiter.api.Test;

class CursorStorageScaleTest {
    private static final int RECORD_LIMIT = 10_000;
    private static final int PAGE_SIZE = 256;

    @Test
    void hydratesTenThousandRootsAcrossBoundedPagesAndRejectsTheNextCreate() {
        try (CursorStorageTestSupport.Context context =
                new CursorStorageTestSupport.Context(0, 20)) {
            CursorOwnerSession owner = prepareActivatedOwner(context);
            seed(context, owner, RECORD_LIMIT);

            ScanResult scan = scanAll(context);
            assertThat(scan.records()).hasSize(RECORD_LIMIT);
            assertThat(scan.pageSizes()).hasSize(40);
            assertThat(scan.pageSizes().subList(0, 39)).containsOnly(PAGE_SIZE);
            assertThat(scan.pageSizes().getLast()).isEqualTo(16);
            assertThat(scan.records())
                    .extracting(record -> record.value().cursorName())
                    .doesNotHaveDuplicates();

            List<CursorHandle> hydrated = context.storage
                    .claimAndLoadActiveCursors(owner)
                    .join();
            assertThat(hydrated).hasSize(RECORD_LIMIT);
            assertThat(hydrated)
                    .extracting(handle -> handle.identity().cursorName())
                    .doesNotHaveDuplicates();
            assertThat(hydrated)
                    .allSatisfy(handle -> assertThat(handle.owner()).isEqualTo(owner));

            assertThatThrownBy(() -> context.storage
                            .open(
                                    owner,
                                    "subscription-overflow",
                                    new CursorOpenRequest(
                                            new InitialCursorPosition.Earliest(),
                                            Map.of(),
                                            Map.of(),
                                            0,
                                            20))
                            .join())
                    .hasCauseInstanceOf(ManagedLedgerException.TooManyRequestsException.class);
            assertThat(context.metadataBackend
                            .getCursor(
                                    CursorStorageTestSupport.CLUSTER,
                                    new StreamId(context.ledger.projection().streamId()),
                                    "subscription-overflow")
                            .join())
                    .isEmpty();
        }
    }

    @Test
    void failsClosedWhenMetadataContainsTenThousandAndOneRoots() {
        try (CursorStorageTestSupport.Context context =
                new CursorStorageTestSupport.Context(0, 20)) {
            CursorOwnerSession owner = prepareActivatedOwner(context);
            seed(context, owner, RECORD_LIMIT + 1);

            assertThatThrownBy(() -> context.storage
                            .claimAndLoadActiveCursors(owner)
                            .join())
                    .hasRootCauseInstanceOf(NereusException.class)
                    .hasRootCauseMessage("cursor scan exceeds cursorRecordsPerStreamMax");
            assertThat(scanAll(context).records()).hasSize(RECORD_LIMIT + 1);
        }
    }

    private static CursorOwnerSession prepareActivatedOwner(
            CursorStorageTestSupport.Context context) {
        CursorOwnerSession owner = context.owner(CursorStorageTestSupport.OWNER_1);
        context.retention.claimAndRecover(owner).join();
        var current = context.projectionStore
                .getProjection(
                        CursorStorageTestSupport.CLUSTER,
                        CursorStorageTestSupport.TOPIC)
                .join()
                .orElseThrow();
        assertThat(ManagedLedgerCursorProtocol.isActivated(current)).isFalse();
        context.projectionStore.activateCursorProtocol(
                        CursorStorageTestSupport.CLUSTER,
                        CursorStorageTestSupport.TOPIC,
                        context.ledger.projection(),
                        current.metadataVersion())
                .join();
        return owner;
    }

    private static void seed(
            CursorStorageTestSupport.Context context,
            CursorOwnerSession owner,
            int count) {
        for (int index = 0; index < count; index++) {
            context.metadataBackend
                    .createCursor(
                            CursorStorageTestSupport.CLUSTER,
                            activeRecord(context, owner, index))
                    .join();
        }
    }

    private static CursorStateRecord activeRecord(
            CursorStorageTestSupport.Context context,
            CursorOwnerSession owner,
            int index) {
        String cursorName = String.format("subscription-%05d", index);
        return new CursorStateRecord(
                0,
                context.ledger.projection(),
                owner.ownerSessionId(),
                cursorName,
                CursorNames.cursorNameHash(cursorName),
                1,
                CursorRecordLifecycle.ACTIVE,
                1,
                1,
                String.format("%032x", index + 1),
                0,
                Optional.empty(),
                List.of(),
                List.of(),
                Map.of(),
                Map.of(),
                1_000,
                1_000,
                OptionalLong.empty());
    }

    private static ScanResult scanAll(CursorStorageTestSupport.Context context) {
        List<com.nereusstream.metadata.oxia.VersionedCursorState> records = new ArrayList<>();
        List<Integer> pageSizes = new ArrayList<>();
        Set<CursorScanToken> seenTokens = new HashSet<>();
        Optional<CursorScanToken> continuation = Optional.empty();
        do {
            CursorScanPage page = context.metadataBackend
                    .scanCursors(
                            CursorStorageTestSupport.CLUSTER,
                            new StreamId(context.ledger.projection().streamId()),
                            continuation,
                            PAGE_SIZE)
                    .join();
            assertThat(page.records()).hasSizeLessThanOrEqualTo(PAGE_SIZE);
            records.addAll(page.records());
            pageSizes.add(page.records().size());
            continuation = page.continuation();
            continuation.ifPresent(token -> assertThat(seenTokens.add(token)).isTrue());
        } while (continuation.isPresent());
        return new ScanResult(List.copyOf(records), List.copyOf(pageSizes));
    }

    private record ScanResult(
            List<com.nereusstream.metadata.oxia.VersionedCursorState> records,
            List<Integer> pageSizes) {
    }
}
