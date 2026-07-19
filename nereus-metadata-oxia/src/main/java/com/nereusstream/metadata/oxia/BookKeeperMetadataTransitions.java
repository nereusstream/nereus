/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.metadata.oxia.records.AllocationSlotLifecycle;
import com.nereusstream.metadata.oxia.records.AppendReservationLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperAllocationSlotRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperAppendReservationRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerProtectionRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerReaderLeaseRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperWriterLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperWriterStateRecord;
import com.nereusstream.metadata.oxia.records.LedgerAllocationIntentRecord;
import com.nereusstream.metadata.oxia.records.LedgerAllocationLifecycle;
import com.nereusstream.metadata.oxia.records.ProtectionLifecycle;

/** Fail-closed immutable-identity and monotonic replacement checks for BK metadata CAS. */
public final class BookKeeperMetadataTransitions {
    private BookKeeperMetadataTransitions() { }

    public static void writer(BookKeeperWriterStateRecord before, BookKeeperWriterStateRecord after) {
        require(before.streamId().equals(after.streamId())
                && before.clusterAlias().equals(after.clusterAlias())
                && before.configurationBindingSha256().equals(after.configurationBindingSha256()), "writer identity drift");
        require(after.writerStateEpoch() == before.writerStateEpoch() + 1,
                "writerStateEpoch must increment exactly once");
        require(after.nextSegmentSequence() >= before.nextSegmentSequence(), "segment sequence moved backward");
        require(writerEdge(before.lifecycle(), after.lifecycle()), "illegal writer lifecycle replacement");
        if (before.activeLedgerId() == after.activeLedgerId() && before.activeLedgerId() != 0) {
            require(after.nextEntryId() >= before.nextEntryId()
                    && after.activePhysicalBytes() >= before.activePhysicalBytes()
                    && after.activeAppendRangeCount() >= before.activeAppendRangeCount(), "active writer counters moved backward");
        }
    }

    public static void allocation(LedgerAllocationIntentRecord before, LedgerAllocationIntentRecord after) {
        require(before.allocationId().equals(after.allocationId()) && before.streamId().equals(after.streamId())
                && before.candidateLedgerId() == after.candidateLedgerId()
                && before.allocationSlot() == after.allocationSlot()
                && before.configurationBindingSha256().equals(after.configurationBindingSha256()), "allocation identity drift");
        require(allocationEdge(before.lifecycle(), after.lifecycle()), "illegal allocation lifecycle replacement");
        require(!before.lateCreateHazard() || after.lateCreateHazard(), "lateCreateHazard cleared");
        require(after.updatedAtMillis() >= before.updatedAtMillis(), "allocation update time moved backward");
    }

    public static void allocationSlot(BookKeeperAllocationSlotRecord before, BookKeeperAllocationSlotRecord after) {
        require(before.slot() == after.slot() && before.allocationId().equals(after.allocationId())
                && before.candidateLedgerId() == after.candidateLedgerId()
                && before.ledgerIdentitySha256().equals(after.ledgerIdentitySha256()), "allocation slot identity drift");
        require(allocationSlotEdge(before.lifecycle(), after.lifecycle()),
                "illegal allocation slot lifecycle replacement");
        require(after.updatedAtMillis() >= before.updatedAtMillis(), "allocation slot update time moved backward");
    }

    public static void root(BookKeeperLedgerRootRecord before, BookKeeperLedgerRootRecord after) {
        require(before.ledgerIdentitySha256().equals(after.ledgerIdentitySha256())
                && before.providerScopeSha256().equals(after.providerScopeSha256())
                && before.ledgerId() == after.ledgerId() && before.streamId().equals(after.streamId())
                && before.segmentSequence() == after.segmentSequence()
                && before.allocationId().equals(after.allocationId())
                && before.configurationBindingSha256().equals(after.configurationBindingSha256())
                && before.customMetadataSha256().equals(after.customMetadataSha256()), "ledger root identity drift");
        require(after.lifecycleEpoch() == before.lifecycleEpoch() + 1, "ledger lifecycleEpoch must increment exactly once");
        require(!before.lateCreateHazard() || after.lateCreateHazard(), "ledger lateCreateHazard cleared");
        require(rootEdge(before.lifecycle(), after.lifecycle()), "illegal ledger lifecycle replacement");
        if (before.lifecycle() == after.lifecycle()) {
            require(before.lifecycle() == BookKeeperLedgerLifecycle.ALLOCATING
                    && !before.lateCreateHazard() && after.lateCreateHazard(),
                    "same-state root CAS is reserved for ALLOCATING late-create hazard escalation");
        }
    }

    public static void reservation(BookKeeperAppendReservationRecord before, BookKeeperAppendReservationRecord after) {
        require(before.reservationId().equals(after.reservationId())
                && before.appendAttemptId().equals(after.appendAttemptId())
                && before.ledgerId() == after.ledgerId() && before.firstEntryId() == after.firstEntryId()
                && before.entryCount() == after.entryCount()
                && before.rangeChecksumSha256().equals(after.rangeChecksumSha256()), "reservation identity drift");
        require(reservationEdge(before.lifecycle(), after.lifecycle()),
                "illegal reservation lifecycle replacement");
        require(after.updatedAtMillis() >= before.updatedAtMillis(), "reservation update time moved backward");
    }

    public static void protection(BookKeeperLedgerProtectionRecord before, BookKeeperLedgerProtectionRecord after) {
        require(before.ledgerIdentitySha256().equals(after.ledgerIdentitySha256())
                && before.ledgerRangeSlot() == after.ledgerRangeSlot()
                && before.protectionSlot() == after.protectionSlot()
                && before.referenceId().equals(after.referenceId())
                && before.protectionTypeId() == after.protectionTypeId(), "protection identity drift");
        require(before.lifecycle() == ProtectionLifecycle.RESERVED && after.lifecycle() == ProtectionLifecycle.ACTIVE,
                "only RESERVED -> ACTIVE protection replacement is legal");
    }

    public static void readerLease(BookKeeperLedgerReaderLeaseRecord before, BookKeeperLedgerReaderLeaseRecord after) {
        require(before.ledgerIdentitySha256().equals(after.ledgerIdentitySha256())
                && before.readerSlot() == after.readerSlot()
                && before.processRunId().equals(after.processRunId())
                && before.rootLifecycleEpoch() == after.rootLifecycleEpoch(), "reader lease identity drift");
        require(after.leaseEpoch() == before.leaseEpoch() + 1 && after.expiresAtMillis() > before.expiresAtMillis(),
                "reader lease renewal must increment epoch and expiry");
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalArgumentException(message);
    }

    private static boolean writerEdge(BookKeeperWriterLifecycle before, BookKeeperWriterLifecycle after) {
        if (before == after) return before != BookKeeperWriterLifecycle.CLOSED;
        return switch (before) {
            case IDLE -> after == BookKeeperWriterLifecycle.ALLOCATING
                    || after == BookKeeperWriterLifecycle.CLOSED;
            case ALLOCATING -> after == BookKeeperWriterLifecycle.ACTIVE
                    || after == BookKeeperWriterLifecycle.IDLE
                    || after == BookKeeperWriterLifecycle.RECOVERING;
            case ACTIVE -> after == BookKeeperWriterLifecycle.RECOVERING;
            case RECOVERING -> after == BookKeeperWriterLifecycle.IDLE;
            case CLOSED -> false;
        };
    }

    private static boolean allocationEdge(LedgerAllocationLifecycle before, LedgerAllocationLifecycle after) {
        return switch (before) {
            case PREPARED -> after == LedgerAllocationLifecycle.ROOT_RESERVED
                    || after == LedgerAllocationLifecycle.ABORTED;
            case ROOT_RESERVED -> after == LedgerAllocationLifecycle.CREATE_UNCERTAIN
                    || after == LedgerAllocationLifecycle.PHYSICAL_CREATED
                    || after == LedgerAllocationLifecycle.FOREIGN_COLLISION
                    || after == LedgerAllocationLifecycle.ABORTED;
            case CREATE_UNCERTAIN -> after == LedgerAllocationLifecycle.PHYSICAL_CREATED
                    || after == LedgerAllocationLifecycle.FOREIGN_COLLISION;
            case PHYSICAL_CREATED -> after == LedgerAllocationLifecycle.ACTIVATED
                    || after == LedgerAllocationLifecycle.FOREIGN_COLLISION;
            case ACTIVATED, FOREIGN_COLLISION, ABORTED -> false;
        };
    }

    private static boolean allocationSlotEdge(AllocationSlotLifecycle before, AllocationSlotLifecycle after) {
        return (before == AllocationSlotLifecycle.CLAIMED && after == AllocationSlotLifecycle.CREATE_STARTED)
                || (before == AllocationSlotLifecycle.CREATE_STARTED
                && after == AllocationSlotLifecycle.CREATE_UNCERTAIN);
    }

    private static boolean rootEdge(BookKeeperLedgerLifecycle before, BookKeeperLedgerLifecycle after) {
        if (before == BookKeeperLedgerLifecycle.ALLOCATING && after == BookKeeperLedgerLifecycle.ALLOCATING) {
            return true;
        }
        if (after == BookKeeperLedgerLifecycle.QUARANTINED) {
            return before != BookKeeperLedgerLifecycle.QUARANTINED;
        }
        return switch (before) {
            case ALLOCATING -> after == BookKeeperLedgerLifecycle.ACTIVE
                    || after == BookKeeperLedgerLifecycle.SEALING
                    || after == BookKeeperLedgerLifecycle.ABORTED;
            case ACTIVE -> after == BookKeeperLedgerLifecycle.SEALING;
            case SEALING -> after == BookKeeperLedgerLifecycle.SEALED;
            case SEALED -> after == BookKeeperLedgerLifecycle.MARKED;
            case MARKED -> after == BookKeeperLedgerLifecycle.DELETING
                    || after == BookKeeperLedgerLifecycle.SEALED;
            case DELETING -> after == BookKeeperLedgerLifecycle.DELETED;
            case DELETED, ABORTED, QUARANTINED -> false;
        };
    }

    private static boolean reservationEdge(
            AppendReservationLifecycle before, AppendReservationLifecycle after) {
        return switch (before) {
            case RESERVED -> after == AppendReservationLifecycle.WRITING
                    || after == AppendReservationLifecycle.ABANDONED;
            case WRITING -> after == AppendReservationLifecycle.DURABLE
                    || after == AppendReservationLifecycle.ABANDONED;
            case DURABLE -> after == AppendReservationLifecycle.COMMIT_PREPARED
                    || after == AppendReservationLifecycle.HEAD_COMMITTED
                    || after == AppendReservationLifecycle.ABANDONED;
            case COMMIT_PREPARED -> after == AppendReservationLifecycle.HEAD_COMMITTED;
            case HEAD_COMMITTED, ABANDONED -> false;
        };
    }
}
