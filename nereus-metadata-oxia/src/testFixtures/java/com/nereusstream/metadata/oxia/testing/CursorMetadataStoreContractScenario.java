/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.testing;

import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.CursorMetadataConditionFailedException;
import com.nereusstream.metadata.oxia.CursorMetadataStore;
import com.nereusstream.metadata.oxia.CursorNames;
import com.nereusstream.metadata.oxia.CursorScanPage;
import com.nereusstream.metadata.oxia.CursorScanToken;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import com.nereusstream.metadata.oxia.VersionedCursorRetention;
import com.nereusstream.metadata.oxia.VersionedCursorState;
import com.nereusstream.metadata.oxia.records.CursorRecordLifecycle;
import com.nereusstream.metadata.oxia.records.CursorRetentionLifecycle;
import com.nereusstream.metadata.oxia.records.CursorRetentionRecord;
import com.nereusstream.metadata.oxia.records.CursorStateRecord;
import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletionException;

/** Store-independent F3 metadata operation sequence shared by fake and real Oxia gates. */
public final class CursorMetadataStoreContractScenario {
    private static final String OWNER = "00112233445566778899aabbccddeeff";
    private static final String ATTEMPT = "102132435465768798a9bacbdcedfe0f";

    private CursorMetadataStoreContractScenario() {
    }

    public static Result run(
            CursorMetadataStore store, String cluster, String managedLedgerName) {
        ManagedLedgerProjectionIdentity projection = projection(managedLedgerName);
        StreamId streamId = new StreamId(projection.streamId());
        CursorStateRecord first = cursor(projection, "sub-a", 1);
        VersionedCursorState created = store.createCursor(cluster, first).join();
        Class<? extends Throwable> duplicateCreateFailure = conditionFailure(
                () -> store.createCursor(cluster, first).join());

        CursorStateRecord updated = cursor(projection, "sub-a", 2);
        VersionedCursorState replaced = store.compareAndSetCursor(
                cluster, updated, created.metadataVersion()).join();
        Class<? extends Throwable> staleCursorFailure = conditionFailure(
                () -> store.compareAndSetCursor(cluster, cursor(projection, "sub-a", 3),
                        created.metadataVersion()).join());

        CursorRetentionRecord initialRetention = retention(projection, 1, 0);
        VersionedCursorRetention createdRetention = store.createRetention(
                cluster, initialRetention).join();
        CursorRetentionRecord updatedRetention = retention(projection, 2, 1);
        VersionedCursorRetention replacedRetention = store.compareAndSetRetention(
                cluster, updatedRetention, createdRetention.metadataVersion()).join();
        Class<? extends Throwable> staleRetentionFailure = conditionFailure(
                () -> store.compareAndSetRetention(
                        cluster, retention(projection, 3, 2), createdRetention.metadataVersion()).join());

        store.createCursor(cluster, cursor(projection, "sub-b", 1)).join();
        List<CursorStateRecord> scanned = new ArrayList<>();
        Optional<CursorScanToken> continuation = Optional.empty();
        do {
            CursorScanPage page = store.scanCursors(cluster, streamId, continuation, 1).join();
            scanned.addAll(page.records().stream().map(VersionedCursorState::value).toList());
            continuation = page.continuation();
        } while (continuation.isPresent());

        return new Result(
                replaced.value(),
                replacedRetention.value(),
                List.copyOf(scanned),
                duplicateCreateFailure,
                staleCursorFailure,
                staleRetentionFailure);
    }

    public record Result(
            CursorStateRecord updatedCursor,
            CursorRetentionRecord updatedRetention,
            List<CursorStateRecord> scannedCursors,
            Class<? extends Throwable> duplicateCreateFailure,
            Class<? extends Throwable> staleCursorFailure,
            Class<? extends Throwable> staleRetentionFailure) {
        public Result {
            scannedCursors = List.copyOf(scannedCursors);
        }
    }

    public static ManagedLedgerProjectionIdentity projection(String managedLedgerName) {
        return new ManagedLedgerProjectionIdentity(
                3,
                1,
                ManagedLedgerProjectionNames.streamId(managedLedgerName, 1).value(),
                ManagedLedgerProjectionNames.MIN_VIRTUAL_LEDGER_ID + 9);
    }

    public static CursorStateRecord cursor(
            ManagedLedgerProjectionIdentity projection, String name, long sequence) {
        return new CursorStateRecord(
                0,
                projection,
                OWNER,
                name,
                CursorNames.cursorNameHash(name),
                1,
                CursorRecordLifecycle.ACTIVE,
                sequence,
                1,
                ATTEMPT,
                0,
                Optional.empty(),
                List.of(),
                List.of(),
                Map.of(),
                Map.of(),
                100,
                100 + sequence,
                OptionalLong.empty());
    }

    public static CursorRetentionRecord retention(
            ManagedLedgerProjectionIdentity projection, long sequence, long floor) {
        return new CursorRetentionRecord(
                0,
                projection,
                OWNER,
                CursorRetentionLifecycle.ACTIVE,
                sequence,
                floor,
                0,
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                100 + sequence);
    }

    private static Class<? extends Throwable> conditionFailure(Runnable operation) {
        try {
            operation.run();
            throw new AssertionError("metadata condition unexpectedly succeeded");
        } catch (CompletionException error) {
            Throwable cause = rootCause(error);
            if (!(cause instanceof CursorMetadataConditionFailedException)) {
                throw error;
            }
            return cause.getClass();
        }
    }

    private static Throwable rootCause(Throwable error) {
        Throwable current = error;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
