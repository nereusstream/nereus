/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;

import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import java.util.Objects;
import java.util.Optional;

/** Terminal, evidence-labelled result of one bounded generation-index repair. */
public record GenerationIndexRepairResult(
        StreamId streamId,
        long targetOffset,
        GenerationIndexRepairSource source,
        int scannedRecords,
        Optional<VersionedGenerationIndex> restoredIndex) {
    public GenerationIndexRepairResult {
        Objects.requireNonNull(streamId, "streamId");
        if (targetOffset < 0 || scannedRecords < 0) {
            throw new IllegalArgumentException(
                    "repair target and scan count must be non-negative");
        }
        Objects.requireNonNull(source, "source");
        restoredIndex = Objects.requireNonNull(restoredIndex, "restoredIndex");
        if ((source == GenerationIndexRepairSource.RECOVERY_CHECKPOINT)
                != restoredIndex.isPresent()) {
            throw new IllegalArgumentException(
                    "only checkpoint repair carries a restored generation index");
        }
        restoredIndex.ifPresent(index -> {
            var value = index.value();
            if (!value.streamId().equals(streamId.value())
                    || value.lifecycle() != GenerationLifecycle.COMMITTED
                    || value.offsetStart() > targetOffset
                    || targetOffset >= value.offsetEnd()) {
                throw new IllegalArgumentException(
                        "restored generation index does not cover the repair target");
            }
        });
    }

    public static GenerationIndexRepairResult trimmed(
            StreamId streamId, long targetOffset) {
        return new GenerationIndexRepairResult(
                streamId,
                targetOffset,
                GenerationIndexRepairSource.TRIMMED,
                0,
                Optional.empty());
    }

    public static GenerationIndexRepairResult live(
            StreamId streamId, long targetOffset, int scannedRecords) {
        return new GenerationIndexRepairResult(
                streamId,
                targetOffset,
                GenerationIndexRepairSource.LIVE_COMMIT,
                scannedRecords,
                Optional.empty());
    }

    public static GenerationIndexRepairResult checkpoint(
            StreamId streamId,
            long targetOffset,
            int scannedRecords,
            VersionedGenerationIndex restoredIndex) {
        return new GenerationIndexRepairResult(
                streamId,
                targetOffset,
                GenerationIndexRepairSource.RECOVERY_CHECKPOINT,
                scannedRecords,
                Optional.of(Objects.requireNonNull(restoredIndex, "restoredIndex")));
    }
}
