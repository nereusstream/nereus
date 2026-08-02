/* Licensed under the Apache License, Version 2.0 */

package com.nereusstream.metadata.oxia;

import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.ResolvedRange;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ReadTarget;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import java.util.Objects;

/**
 * Strict conversion from a durable F4 index wrapper to a committed resolver candidate.
 */
public final class GenerationIndexValidator {
    private final ReadTargetCodecRegistry targetCodecs;

    public GenerationIndexValidator(ReadTargetCodecRegistry targetCodecs) {
        this.targetCodecs = Objects.requireNonNull(targetCodecs, "targetCodecs");
    }

    public static GenerationIndexValidator phase15Targets() {
        return new GenerationIndexValidator(ReadTargetCodecRegistry.phase15());
    }

    public OffsetIndexEntry requireCommitted(
            VersionedGenerationIndex wrapper,
            StreamId expectedStream,
            ReadView expectedView,
            long committedEndOffset,
            long headCommitVersion) {
        ResolvedRange resolved =
                requireSemantic(wrapper, expectedStream, expectedView, committedEndOffset, headCommitVersion);
        if (expectedView != ReadView.COMMITTED) {
            throw F4MetadataStoreSupport.invariant(
                    "sparse semantic-view indexes cannot be represented by the committed range adapter");
        }
        GenerationIndexRecord record = wrapper.value();
        return new OffsetIndexEntry(
                expectedStream,
                resolved.offsetRange(),
                resolved.generation(),
                record.cumulativeSizeAtEnd(),
                resolved.readTarget(),
                resolved.payloadFormat(),
                resolved.recordCount(),
                resolved.entryCount(),
                resolved.logicalBytes(),
                resolved.schemaRefs(),
                resolved.projectionRef(),
                resolved.commitVersion(),
                false,
                wrapper.metadataVersion());
    }

    /**
     * Validates either dense COMMITTED or sparse semantic-view metadata without losing source coverage.
     */
    public ResolvedRange requireSemantic(
            VersionedGenerationIndex wrapper,
            StreamId expectedStream,
            ReadView expectedView,
            long committedEndOffset,
            long headCommitVersion) {
        Objects.requireNonNull(wrapper, "wrapper");
        Objects.requireNonNull(expectedStream, "expectedStream");
        Objects.requireNonNull(expectedView, "expectedView");
        if (committedEndOffset < 0 || headCommitVersion < 0) {
            throw new IllegalArgumentException("head bounds must be non-negative");
        }
        GenerationIndexRecord record = wrapper.value();
        if (record.lifecycle() != GenerationLifecycle.COMMITTED) {
            throw F4MetadataStoreSupport.invariant("non-COMMITTED generation entered the read path");
        }
        ReadView actualView = ReadView.fromWireId(record.readViewId());
        if (!record.streamId().equals(expectedStream.value()) || actualView != expectedView) {
            throw F4MetadataStoreSupport.invariant("generation index belongs to another stream or view");
        }
        if (record.offsetEnd() > committedEndOffset || record.lastCommitVersion() > headCommitVersion) {
            throw F4MetadataStoreSupport.invariant("generation index exceeds current committed head truth");
        }
        if (!record.targetIdentitySha256().equals(record.readTarget().identityChecksumValue())) {
            throw F4MetadataStoreSupport.invariant("generation target identity does not match durable target bytes");
        }
        ReadTarget target;
        PayloadFormat payloadFormat;
        try {
            target = targetCodecs.decode(record.readTarget());
            payloadFormat = PayloadFormat.valueOf(record.payloadFormat());
        } catch (RuntimeException failure) {
            throw F4MetadataStoreSupport.invariant(
                    "generation index contains an unsupported target or payload format", failure);
        }
        return new ResolvedRange(
                new OffsetRange(record.offsetStart(), record.offsetEnd()),
                record.generation(),
                target,
                payloadFormat,
                record.sourceRecordCount(),
                record.entryCount(),
                record.logicalBytes(),
                record.schemaRefs(),
                ProjectionIdentity.decode(record.projectionRef()),
                record.lastCommitVersion());
    }
}
