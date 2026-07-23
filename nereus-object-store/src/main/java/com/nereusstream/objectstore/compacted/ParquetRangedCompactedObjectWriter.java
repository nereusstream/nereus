/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import com.nereusstream.api.ReadView;
import com.nereusstream.objectstore.staging.StagingFileManager;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;

/** Closed NCP2 Parquet writer; V1 schemas and validation are not reused or reinterpreted. */
public final class ParquetRangedCompactedObjectWriter implements RangedCompactedObjectWriter {
    private final ParquetV2WriterSupport support;

    public ParquetRangedCompactedObjectWriter(StagingFileManager stagingFiles, Executor writerExecutor) {
        support = new ParquetV2WriterSupport(stagingFiles, writerExecutor);
    }

    @Override
    public CompletableFuture<RangedCompactedObjectWriteResult> write(
            RangedCompactedObjectWriteRequest request,
            Flow.Publisher<RangedCompactedObjectRow> rows) {
        Objects.requireNonNull(request, "request");
        return support.write(
                new ParquetV2WriterSupport.WriteSpec<>(
                        ReadView.COMMITTED,
                        request.cluster(),
                        request.streamId(),
                        request.sourceCoverage(),
                        request.outputAttemptId(),
                        request.entryCount(),
                        request.sourceRecordCount(),
                        request.logicalBytes(),
                        request.targetRowGroupRecords(),
                        request.compression(),
                        CompactedObjectFormatV2.COMMITTED_SCHEMA,
                        CompactedObjectFormatV2.metadata(request),
                        ParquetV2WriterSupport.ncp2Codec(request)),
                rows);
    }
}
