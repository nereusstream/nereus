/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger;

import com.nereusstream.managedledger.cursor.CursorHandle;
import com.nereusstream.managedledger.cursor.CursorLedgerIdentity;
import com.nereusstream.managedledger.cursor.CursorLifecycle;
import com.nereusstream.managedledger.cursor.CursorOwnerSession;
import com.nereusstream.managedledger.cursor.CursorRetentionView;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Fully stabilized writable-open result published only after durable cursor hydration. */
public record NereusWritableLedgerOpenResult(
        NereusLedgerOpenResult ledger,
        CursorOwnerSession cursorOwnerSession,
        List<CursorHandle> durableCursors,
        CursorRetentionView cursorRetention) {
    public NereusWritableLedgerOpenResult {
        Objects.requireNonNull(ledger, "ledger");
        Objects.requireNonNull(cursorOwnerSession, "cursorOwnerSession");
        durableCursors = List.copyOf(Objects.requireNonNull(durableCursors, "durableCursors"));
        Objects.requireNonNull(cursorRetention, "cursorRetention");

        CursorLedgerIdentity expectedLedger = new CursorLedgerIdentity(
                ledger.topicProjection().managedLedgerName(),
                ManagedLedgerProjectionNames.managedLedgerNameHash(
                        ledger.topicProjection().managedLedgerName()),
                ledger.topicProjection().projectionIdentity());
        if (!cursorOwnerSession.ledger().equals(expectedLedger)) {
            throw new IllegalArgumentException("cursor owner does not match the writable ledger projection");
        }
        if (!cursorRetention.ledger().equals(expectedLedger)
                || !cursorRetention.ownerSessionId().equals(cursorOwnerSession.ownerSessionId())) {
            throw new IllegalArgumentException("cursor retention does not match the writable owner session");
        }
        if (cursorRetention.lifecycle() != CursorRetentionView.Lifecycle.ACTIVE) {
            throw new IllegalArgumentException("writable open cannot publish pending cursor retention");
        }

        Set<String> exactNames = new HashSet<>();
        for (CursorHandle handle : durableCursors) {
            Objects.requireNonNull(handle, "durableCursors contains null handle");
            if (!handle.owner().equals(cursorOwnerSession)
                    || !handle.identity().ledger().equals(expectedLedger)
                    || !handle.state().ownerSessionId().equals(cursorOwnerSession.ownerSessionId())
                    || handle.state().lifecycle() != CursorLifecycle.ACTIVE) {
                throw new IllegalArgumentException(
                        "durable cursor handle does not match the writable owner session");
            }
            if (!exactNames.add(handle.identity().cursorName())) {
                throw new IllegalArgumentException("durable cursor names must be unique");
            }
        }
    }
}
