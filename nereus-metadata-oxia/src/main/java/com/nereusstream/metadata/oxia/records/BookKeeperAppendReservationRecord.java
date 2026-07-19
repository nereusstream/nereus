/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import com.nereusstream.api.SchemaRef;
import java.util.List;

/** Durable, payload-free reservation for one exact BookKeeper entry range. */
public record BookKeeperAppendReservationRecord(
        int schemaVersion, String reservationId, String appendAttemptId, String streamId, String writerId,
        String writerRunIdHash, long appendSessionEpoch, String fencingTokenHash, long writerStateEpoch,
        long ledgerId, long ledgerRootEpoch, int ledgerRangeSlot, long firstEntryId, int entryCount,
        String rangeChecksumSha256, long expectedStartOffset, String payloadFormat, int recordCount,
        long logicalBytes, long physicalBytes, List<SchemaRef> schemaRefs, String projectionIdentity,
        long minEventTimeMillis, long maxEventTimeMillis, AppendReservationLifecycle lifecycle,
        String commitId, String commitKey, long commitMetadataVersion, String commitRecordSha256,
        long createdAtMillis, long updatedAtMillis, String stateReason, long metadataVersion) {
    public BookKeeperAppendReservationRecord {
        BookKeeperRecordValidation.version(schemaVersion);
        reservationId = BookKeeperRecordValidation.text(reservationId, "reservationId");
        appendAttemptId = BookKeeperRecordValidation.text(appendAttemptId, "appendAttemptId");
        streamId = BookKeeperRecordValidation.text(streamId, "streamId");
        writerId = BookKeeperRecordValidation.text(writerId, "writerId");
        writerRunIdHash = BookKeeperRecordValidation.sha256(writerRunIdHash, "writerRunIdHash");
        BookKeeperRecordValidation.nonNegative(appendSessionEpoch, "appendSessionEpoch");
        fencingTokenHash = BookKeeperRecordValidation.sha256(fencingTokenHash, "fencingTokenHash");
        BookKeeperRecordValidation.positive(writerStateEpoch, "writerStateEpoch");
        BookKeeperRecordValidation.positive(ledgerId, "ledgerId");
        BookKeeperRecordValidation.positive(ledgerRootEpoch, "ledgerRootEpoch");
        BookKeeperRecordValidation.nonNegative(ledgerRangeSlot, "ledgerRangeSlot");
        BookKeeperRecordValidation.nonNegative(firstEntryId, "firstEntryId");
        BookKeeperRecordValidation.positive(entryCount, "entryCount");
        Math.addExact(firstEntryId, entryCount);
        rangeChecksumSha256 = BookKeeperRecordValidation.sha256(rangeChecksumSha256, "rangeChecksumSha256");
        BookKeeperRecordValidation.nonNegative(expectedStartOffset, "expectedStartOffset");
        payloadFormat = BookKeeperRecordValidation.text(payloadFormat, "payloadFormat");
        BookKeeperRecordValidation.positive(recordCount, "recordCount");
        BookKeeperRecordValidation.nonNegative(logicalBytes, "logicalBytes");
        BookKeeperRecordValidation.positive(physicalBytes, "physicalBytes");
        schemaRefs = BookKeeperRecordValidation.schemaRefs(schemaRefs);
        projectionIdentity = BookKeeperRecordValidation.text(projectionIdentity, "projectionIdentity");
        BookKeeperRecordValidation.nonNegative(minEventTimeMillis, "minEventTimeMillis");
        if (maxEventTimeMillis < minEventTimeMillis) throw new IllegalArgumentException("event time range is invalid");
        if (lifecycle == null) throw new NullPointerException("lifecycle");
        commitId = BookKeeperRecordValidation.optional(commitId, "commitId");
        commitKey = BookKeeperRecordValidation.optional(commitKey, "commitKey");
        BookKeeperRecordValidation.nonNegative(commitMetadataVersion, "commitMetadataVersion");
        commitRecordSha256 = BookKeeperRecordValidation.optionalSha256(commitRecordSha256, "commitRecordSha256");
        BookKeeperRecordValidation.times(createdAtMillis, updatedAtMillis);
        stateReason = BookKeeperRecordValidation.optional(stateReason, "stateReason");
        BookKeeperRecordValidation.metadataVersion(metadataVersion);
        boolean committedFacts = !commitId.isEmpty() && !commitKey.isEmpty() && commitMetadataVersion > 0
                && !commitRecordSha256.isEmpty();
        boolean committedLifecycle = lifecycle == AppendReservationLifecycle.COMMIT_PREPARED
                || lifecycle == AppendReservationLifecycle.HEAD_COMMITTED;
        if (committedFacts != committedLifecycle) {
            throw new IllegalArgumentException("commit facts must match reservation lifecycle");
        }
        if ((lifecycle == AppendReservationLifecycle.ABANDONED) != !stateReason.isEmpty()) {
            throw new IllegalArgumentException("only ABANDONED reservation carries stateReason");
        }
    }

    public BookKeeperAppendReservationRecord withMetadataVersion(long version) {
        return new BookKeeperAppendReservationRecord(schemaVersion, reservationId, appendAttemptId, streamId,
                writerId, writerRunIdHash, appendSessionEpoch, fencingTokenHash, writerStateEpoch, ledgerId,
                ledgerRootEpoch, ledgerRangeSlot, firstEntryId, entryCount, rangeChecksumSha256, expectedStartOffset,
                payloadFormat, recordCount, logicalBytes, physicalBytes, schemaRefs, projectionIdentity,
                minEventTimeMillis, maxEventTimeMillis, lifecycle, commitId, commitKey, commitMetadataVersion,
                commitRecordSha256, createdAtMillis, updatedAtMillis, stateReason, version);
    }
}
