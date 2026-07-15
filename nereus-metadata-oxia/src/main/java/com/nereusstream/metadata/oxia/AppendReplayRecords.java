/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.StreamCommitTargetRecord;
import java.util.Objects;
import java.util.Optional;

/** Shared exact validation and hydration for live and NRC1 generic append records. */
public final class AppendReplayRecords {
    private AppendReplayRecords() {
    }

    public static CommittedAppend validateAndHydrate(
            CommitAppendRequest request,
            StreamCommitTargetRecord record,
            AppendOutcome mismatchOutcome) {
        requireMatches(request, record, mismatchOutcome);
        return hydrate(record, request.projectionRef());
    }

    public static void requireMatches(
            CommitAppendRequest request,
            StreamCommitTargetRecord record,
            AppendOutcome mismatchOutcome) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(record, "record");
        Objects.requireNonNull(mismatchOutcome, "mismatchOutcome");
        long expectedEnd;
        try {
            expectedEnd = Math.addExact(
                    request.expectedStartOffset(), request.recordCount());
        } catch (ArithmeticException failure) {
            throw new NereusException(
                    ErrorCode.INVALID_ARGUMENT,
                    false,
                    "generic append end offset overflows",
                    failure,
                    mismatchOutcome);
        }
        if (!record.commitId().equals(request.commitId())
                || !record.streamId().equals(request.streamId().value())
                || !record.writerId().equals(request.writerId())
                || !record.writerRunIdHash().equals(request.writerRunIdHash())
                || record.writerEpoch() != request.epoch()
                || !record.fencingTokenHash().equals(request.fencingTokenHash())
                || record.offsetStart() != request.expectedStartOffset()
                || record.offsetEnd() != expectedEnd
                || record.generation() != 0
                || record.recordCount() != request.recordCount()
                || record.entryCount() != request.entryCount()
                || record.logicalBytes() != request.logicalBytes()
                || !record.readTarget().equals(request.readTargetRecord())
                || !record.payloadFormat().equals(request.payloadFormat().name())
                || !record.schemaRefs().equals(request.schemaRefs())
                || !record.projectionRef().equals(request.projectionIdentity())
                || record.minEventTimeMillis() != request.minEventTimeMillis()
                || record.maxEventTimeMillis() != request.maxEventTimeMillis()) {
            throw new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "replayed generic commit does not match request",
                    mismatchOutcome);
        }
    }

    public static CommittedAppend hydrate(
            StreamCommitTargetRecord record,
            Optional<ProjectionRef> projectionRef) {
        Objects.requireNonNull(record, "record");
        Objects.requireNonNull(projectionRef, "projectionRef");
        return new CommittedAppend(
                new StreamId(record.streamId()),
                record.commitId(),
                record.previousCommitId(),
                ReadTargetCodecRegistry.phase15().decode(record.readTarget()),
                new OffsetRange(record.offsetStart(), record.offsetEnd()),
                record.generation(),
                record.cumulativeSize(),
                record.commitVersion(),
                PayloadFormat.valueOf(record.payloadFormat()),
                record.recordCount(),
                record.entryCount(),
                record.logicalBytes(),
                record.schemaRefs(),
                projectionRef,
                record.minEventTimeMillis(),
                record.maxEventTimeMillis());
    }
}
