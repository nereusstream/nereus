/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ReadTarget;
import com.nereusstream.metadata.oxia.OffsetIndexEntry;
import com.nereusstream.metadata.oxia.ProjectionIdentity;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedGenerationZeroIndex;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import java.util.Objects;
import java.util.Optional;

/** Strict conversion of an authoritative generation candidate into one immutable planner source edge. */
final class MaterializationSourceMapper {
    private static final ReadTargetCodecRegistry TARGET_CODECS = ReadTargetCodecRegistry.phase15();

    private MaterializationSourceMapper() {
    }

    static Optional<SourceGeneration> committedSource(
            VersionedGenerationCandidate candidate,
            StreamId expectedStream,
            ReadView expectedView,
            long committedEndOffset,
            long headCommitVersion,
            Optional<ProjectionRef> effectiveProjection) {
        Objects.requireNonNull(candidate, "candidate");
        Objects.requireNonNull(expectedStream, "expectedStream");
        Objects.requireNonNull(expectedView, "expectedView");
        effectiveProjection = Objects.requireNonNull(effectiveProjection, "effectiveProjection");
        if (committedEndOffset < 0 || headCommitVersion < 0) {
            throw new IllegalArgumentException("head bounds must be non-negative");
        }
        if (candidate instanceof VersionedGenerationZeroIndex zero) {
            return generationZero(
                    zero,
                    expectedStream,
                    expectedView,
                    committedEndOffset,
                    headCommitVersion,
                    effectiveProjection);
        }
        return higherGeneration(
                (VersionedGenerationIndex) candidate,
                expectedStream,
                expectedView,
                committedEndOffset,
                headCommitVersion,
                effectiveProjection);
    }

    static boolean matchesExactSource(
            VersionedGenerationCandidate candidate,
            StreamId streamId,
            SourceGeneration expected) {
        Optional<SourceGeneration> actual = committedSource(
                candidate,
                Objects.requireNonNull(streamId, "streamId"),
                expected.view(),
                expected.range().endOffset(),
                expected.commitVersion(),
                expected.projectionRef());
        return actual.isPresent() && actual.orElseThrow().equals(expected);
    }

    private static Optional<SourceGeneration> generationZero(
            VersionedGenerationZeroIndex wrapper,
            StreamId expectedStream,
            ReadView expectedView,
            long committedEndOffset,
            long headCommitVersion,
            Optional<ProjectionRef> effectiveProjection) {
        OffsetIndexEntry entry = wrapper.value();
        if (expectedView != ReadView.COMMITTED) {
            throw invariant("generation zero appeared outside the COMMITTED view");
        }
        if (!entry.streamId().equals(expectedStream)
                || entry.generation() != 0
                || entry.offsetEnd() > committedEndOffset
                || entry.commitVersion() > headCommitVersion) {
            throw invariant("generation-zero source exceeds or contradicts authoritative stream truth");
        }
        if (entry.tombstoned()) {
            return Optional.empty();
        }
        Optional<ProjectionRef> projection = effectiveProjection(entry.projectionRef(), effectiveProjection, false);
        ReadTarget target = entry.readTarget();
        String identity = TARGET_CODECS.encode(target).identityChecksumValue();
        long cumulativeStart;
        try {
            cumulativeStart = Math.subtractExact(entry.cumulativeSize(), entry.logicalBytes());
        } catch (ArithmeticException failure) {
            throw invariant("generation-zero cumulative accounting underflow", failure);
        }
        return Optional.of(new SourceGeneration(
                ReadView.COMMITTED,
                entry.range(),
                0,
                entry.commitVersion(),
                wrapper.key(),
                wrapper.metadataVersion(),
                wrapper.durableValueSha256(),
                target,
                new Checksum(ChecksumType.SHA256, identity),
                Optional.empty(),
                entry.payloadFormat(),
                projection,
                entry.recordCount(),
                entry.entryCount(),
                entry.logicalBytes(),
                entry.schemaRefs(),
                cumulativeStart,
                entry.cumulativeSize()));
    }

    private static Optional<SourceGeneration> higherGeneration(
            VersionedGenerationIndex wrapper,
            StreamId expectedStream,
            ReadView expectedView,
            long committedEndOffset,
            long headCommitVersion,
            Optional<ProjectionRef> effectiveProjection) {
        GenerationIndexRecord record = wrapper.value();
        if (record.lifecycle() != GenerationLifecycle.COMMITTED) {
            return Optional.empty();
        }
        ReadView actualView;
        ReadTarget target;
        PayloadFormat payloadFormat;
        Optional<ProjectionRef> storedProjection;
        try {
            actualView = ReadView.fromWireId(record.readViewId());
            target = TARGET_CODECS.decode(record.readTarget());
            payloadFormat = PayloadFormat.valueOf(record.payloadFormat());
            storedProjection = ProjectionIdentity.decode(record.projectionRef());
        } catch (RuntimeException failure) {
            throw invariant("higher-generation source contains an unsupported encoded identity", failure);
        }
        if (!record.streamId().equals(expectedStream.value())
                || actualView != expectedView
                || record.offsetEnd() > committedEndOffset
                || record.lastCommitVersion() > headCommitVersion
                || !record.targetIdentitySha256().equals(record.readTarget().identityChecksumValue())) {
            throw invariant("higher-generation source exceeds or contradicts authoritative stream truth");
        }
        Optional<ProjectionRef> projection = effectiveProjection(storedProjection, effectiveProjection, true);
        return Optional.of(new SourceGeneration(
                actualView,
                new OffsetRange(record.offsetStart(), record.offsetEnd()),
                record.generation(),
                record.lastCommitVersion(),
                wrapper.key(),
                wrapper.metadataVersion(),
                wrapper.durableValueSha256(),
                target,
                new Checksum(ChecksumType.SHA256, record.targetIdentitySha256()),
                Optional.of(new Checksum(ChecksumType.SHA256, record.materializationPolicySha256())),
                payloadFormat,
                projection,
                record.sourceRecordCount(),
                record.entryCount(),
                record.logicalBytes(),
                record.schemaRefs(),
                record.cumulativeSizeAtStart(),
                record.cumulativeSizeAtEnd()));
    }

    private static Optional<ProjectionRef> effectiveProjection(
            Optional<ProjectionRef> stored,
            Optional<ProjectionRef> effective,
            boolean higherGeneration) {
        if (stored.isPresent() && effective.isPresent()
                && !stored.orElseThrow().equals(effective.orElseThrow())) {
            throw invariant("source projection conflicts with the registered effective projection");
        }
        if (higherGeneration && effective.isPresent() && stored.isEmpty()) {
            throw invariant("higher generation omitted the registered effective projection");
        }
        return stored.isPresent() ? stored : effective;
    }

    private static NereusException invariant(String message) {
        return invariant(message, null);
    }

    private static NereusException invariant(String message, Throwable cause) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message, cause);
    }
}
