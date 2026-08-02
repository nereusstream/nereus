/* Licensed under the Apache License, Version 2.0 */

package com.nereusstream.materialization;

import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadSourceRef;
import java.util.Objects;

/**
 * Streaming proof that decoded batches came from every frozen source exactly once and in order.
 */
public final class ExactSourceSetVerifier {
    private final ExactSourceSet sourceSet;
    private int sourceIndex;
    private long nextOffset;
    private int sourceEntries;
    private long sourceRecords;
    private long sourceBytes;
    private boolean finished;

    public ExactSourceSetVerifier(ExactSourceSet sourceSet) {
        this.sourceSet = Objects.requireNonNull(sourceSet, "sourceSet");
        this.nextOffset = sourceSet.coverage().startOffset();
    }

    public void accept(ReadBatch batch) {
        Objects.requireNonNull(batch, "batch");
        if (finished) {
            throw new IllegalStateException("exact source-set verification is already finished");
        }
        if (sourceIndex >= sourceSet.sources().size()) {
            throw new IllegalArgumentException("exact source set emitted trailing batches");
        }
        SourceGeneration expected = sourceSet.sources().get(sourceIndex);
        ReadSourceRef actual = batch.source();
        if (batch.range().startOffset() != nextOffset
                || batch.range().endOffset() > expected.range().endOffset()
                || !actual.resolvedRange().equals(expected.range())
                || actual.generation() != expected.generation()
                || actual.commitVersion() != expected.commitVersion()
                || !actual.target().equals(expected.readTarget())
                || !actual.targetIdentity().equals(expected.targetIdentitySha256())
                || batch.payloadFormat() != expected.payloadFormat()
                || !batch.projectionRef().equals(expected.projectionRef())
                || !batch.schemaRefs().equals(expected.schemaRefs())) {
            throw new IllegalArgumentException("read batch does not match the frozen exact source identity");
        }
        nextOffset = batch.range().endOffset();
        sourceEntries = Math.addExact(sourceEntries, 1);
        sourceRecords = Math.addExact(sourceRecords, batch.range().recordCount());
        sourceBytes = Math.addExact(sourceBytes, batch.payload().length);
        if (nextOffset == expected.range().endOffset()) {
            if (sourceEntries != expected.entryCount()
                    || sourceRecords != expected.recordCount()
                    || sourceBytes != expected.logicalBytes()) {
                throw new IllegalArgumentException("read batches do not match frozen exact source accounting");
            }
            sourceIndex++;
            sourceEntries = 0;
            sourceRecords = 0;
            sourceBytes = 0;
        }
    }

    public void finish() {
        if (finished) {
            throw new IllegalStateException("exact source-set verification is already finished");
        }
        finished = true;
        if (sourceIndex != sourceSet.sources().size()
                || nextOffset != sourceSet.coverage().endOffset()
                || sourceEntries != 0
                || sourceRecords != 0
                || sourceBytes != 0) {
            throw new IllegalArgumentException("exact source-set verification ended before frozen coverage");
        }
    }
}
