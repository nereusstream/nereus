/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.recovery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.FirstEntryPolicy;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadBoundaryMode;
import com.nereusstream.api.ReadRequest;
import com.nereusstream.api.ReadResult;
import com.nereusstream.api.ReadSourceRef;
import com.nereusstream.api.ReadTargetIdentities;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.SemanticReadResult;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.kafka.testing.TestStreamStorage;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class DefaultKafkaRecoveryBatchSourceTest {
    private static final StreamId STREAM_ID = new StreamId("recovery-stream");

    @Test
    void mapsOneBoundedExactCommittedPageWithoutReadingPastTheFrozenEnd() {
        TestStreamStorage streams = new TestStreamStorage();
        AtomicReference<ReadRequest> captured = new AtomicReference<>();
        streams.semanticReader((streamId, request) -> {
            assertThat(streamId).isEqualTo(STREAM_ID);
            captured.set(request);
            ReadResult result = new ReadResult(
                    STREAM_ID,
                    2,
                    4,
                    List.of(batch(new OffsetRange(2, 4), PayloadFormat.KAFKA_RECORD_BATCH)),
                    false);
            return CompletableFuture.completedFuture(
                    SemanticReadResult.forRequest(request, result, 4));
        });
        DefaultKafkaRecoveryBatchSource source =
                new DefaultKafkaRecoveryBatchSource(streams, STREAM_ID, 17, 4_096);

        KafkaRecoveryBatchPage page =
                source.readCommittedPage(2, 5, Duration.ofSeconds(3)).join();

        assertThat(page.requestedOffset()).isEqualTo(2);
        assertThat(page.nextOffset()).isEqualTo(4);
        assertThat(page.batches()).singleElement().satisfies(value -> {
            assertThat(value.baseOffset()).isEqualTo(2);
            assertThat(value.lastOffset()).isEqualTo(3);
            assertThat(value.encodedBatch()).containsExactly(2, 3, 4);
        });
        assertThat(captured.get().view()).isEqualTo(ReadView.COMMITTED);
        assertThat(captured.get().boundaryMode()).isEqualTo(ReadBoundaryMode.EXACT_START);
        assertThat(captured.get().firstEntryPolicy())
                .isEqualTo(FirstEntryPolicy.ALLOW_FIRST_ENTRY_OVERFLOW);
        assertThat(captured.get().options().maxRecords()).isEqualTo(17);
        assertThat(captured.get().options().maxBytes()).isEqualTo(4_096);
    }

    @Test
    void failsClosedOnAnEmptyOrNonKafkaPageBeforeTheFrozenEnd() {
        TestStreamStorage empty = new TestStreamStorage();
        empty.semanticReader((streamId, request) -> {
            ReadResult result = new ReadResult(STREAM_ID, 2, 2, List.of(), false);
            return CompletableFuture.completedFuture(
                    SemanticReadResult.forRequest(request, result, 2));
        });
        DefaultKafkaRecoveryBatchSource emptySource =
                new DefaultKafkaRecoveryBatchSource(empty, STREAM_ID, 10, 1_024);

        assertThatThrownBy(() -> emptySource
                        .readCommittedPage(2, 5, Duration.ofSeconds(1))
                        .join())
                .hasRootCauseInstanceOf(NereusException.class)
                .hasRootCauseMessage("Kafka recovery read made no progress before the frozen end");

        TestStreamStorage wrongFormat = new TestStreamStorage();
        wrongFormat.semanticReader((streamId, request) -> {
            ReadResult result = new ReadResult(
                    STREAM_ID,
                    2,
                    4,
                    List.of(batch(new OffsetRange(2, 4), PayloadFormat.OPAQUE_RECORD_BATCH)),
                    false);
            return CompletableFuture.completedFuture(
                    SemanticReadResult.forRequest(request, result, 4));
        });
        DefaultKafkaRecoveryBatchSource wrongFormatSource =
                new DefaultKafkaRecoveryBatchSource(wrongFormat, STREAM_ID, 10, 1_024);

        assertThatThrownBy(() -> wrongFormatSource
                        .readCommittedPage(2, 5, Duration.ofSeconds(1))
                        .join())
                .hasRootCauseInstanceOf(NereusException.class)
                .hasRootCauseMessage(
                        "Kafka recovery read returned a non-Kafka or out-of-range batch");
    }

    private static ReadBatch batch(OffsetRange range, PayloadFormat format) {
        byte[] payload = new byte[] {2, 3, 4};
        Checksum checksum = new Checksum(ChecksumType.CRC32C, "00000000");
        EntryIndexRef index = new EntryIndexRef(
                EntryIndexLocation.OBJECT_FOOTER,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                0,
                1,
                checksum);
        ObjectSliceReadTarget target = new ObjectSliceReadTarget(
                1,
                new ObjectId("recovery-object"),
                new ObjectKey("f9/recovery-object"),
                ObjectType.MULTI_STREAM_WAL_OBJECT,
                "WAL_OBJECT_V1",
                "KAFKA_RECORD_BATCH_V1",
                "recovery-slice",
                0,
                payload.length,
                checksum,
                index);
        return new ReadBatch(
                range,
                format,
                payload,
                List.of(),
                Optional.empty(),
                new ReadSourceRef(
                        range,
                        0,
                        1,
                        target,
                        ReadTargetIdentities.sha256(target)));
    }
}
