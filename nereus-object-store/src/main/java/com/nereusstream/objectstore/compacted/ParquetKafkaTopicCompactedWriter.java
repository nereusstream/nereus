/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import com.nereusstream.api.ReadView;
import com.nereusstream.objectstore.staging.StagingFileManager;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;

/** Closed NTC2 Parquet writer with Kafka-specific row validation. */
public final class ParquetKafkaTopicCompactedWriter implements KafkaTopicCompactedObjectWriter {
    private final ParquetV2WriterSupport support;

    public ParquetKafkaTopicCompactedWriter(StagingFileManager stagingFiles, Executor writerExecutor) {
        support = new ParquetV2WriterSupport(stagingFiles, writerExecutor);
    }

    @Override
    public CompletableFuture<RangedCompactedObjectWriteResult> write(
            KafkaTopicCompactedObjectWriteRequest request,
            Flow.Publisher<KafkaTopicCompactedObjectRow> rows) {
        Objects.requireNonNull(request, "request");
        return support.write(
                new ParquetV2WriterSupport.WriteSpec<>(
                        ReadView.TOPIC_COMPACTED,
                        request.cluster(),
                        request.streamId(),
                        request.sourceCoverage(),
                        request.outputAttemptId(),
                        request.entryCount(),
                        request.outputRecordCount(),
                        request.logicalBytes(),
                        request.targetRowGroupRecords(),
                        request.compression(),
                        CompactedObjectFormatV2.TOPIC_COMPACTED_SCHEMA,
                        CompactedObjectFormatV2.metadata(request),
                        ParquetV2WriterSupport.ntc2Codec(request)),
                rows);
    }
}
