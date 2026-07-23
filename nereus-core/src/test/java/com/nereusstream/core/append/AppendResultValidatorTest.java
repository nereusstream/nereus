/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.core.append;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.AppendBatch;
import com.nereusstream.api.AppendEntry;
import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.AppendPrecondition;
import com.nereusstream.api.AppendResult;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.SchemaRef;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.BookKeeperEntryMapping;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class AppendResultValidatorTest {
    private static final StreamId STREAM_ID = new StreamId("stream-a");
    private static final SchemaRef SCHEMA = new SchemaRef("kafka", "record-batch", 1);

    @Test
    void acceptsOnlyTheExactCommittedRequest() {
        AppendBatch request = request();
        AppendResult result = result(
                STREAM_ID,
                new OffsetRange(4, 7),
                PayloadFormat.KAFKA_RECORD_BATCH,
                3,
                1,
                3,
                List.of(SCHEMA));

        assertThat(AppendResultValidator.requireExactRequest(
                STREAM_ID,
                request,
                AppendPrecondition.expectedStartOffset(4),
                result)).isSameAs(result);
    }

    @Test
    void rejectsEveryCallerVisibleLogicalMismatchAsKnownCommitted() {
        AppendBatch request = request();
        List<AppendResult> mismatches = List.of(
                result(new StreamId("stream-b"), new OffsetRange(4, 7),
                        PayloadFormat.KAFKA_RECORD_BATCH, 3, 1, 3, List.of(SCHEMA)),
                result(STREAM_ID, new OffsetRange(4, 7),
                        PayloadFormat.PULSAR_ENTRY_BATCH, 3, 1, 3, List.of(SCHEMA)),
                result(STREAM_ID, new OffsetRange(4, 6),
                        PayloadFormat.KAFKA_RECORD_BATCH, 2, 1, 3, List.of(SCHEMA)),
                result(STREAM_ID, new OffsetRange(4, 7),
                        PayloadFormat.KAFKA_RECORD_BATCH, 3, 2, 3, List.of(SCHEMA)),
                result(STREAM_ID, new OffsetRange(4, 7),
                        PayloadFormat.KAFKA_RECORD_BATCH, 3, 1, 4, List.of(SCHEMA)),
                result(STREAM_ID, new OffsetRange(4, 7),
                        PayloadFormat.KAFKA_RECORD_BATCH, 3, 1, 3, List.of()));

        for (AppendResult mismatch : mismatches) {
            assertInvariantViolation(() -> AppendResultValidator.requireExactRequest(
                    STREAM_ID,
                    request,
                    AppendPrecondition.expectedStartOffset(4),
                    mismatch));
        }
        assertInvariantViolation(() -> AppendResultValidator.requireExactRequest(
                STREAM_ID,
                request,
                AppendPrecondition.expectedStartOffset(5),
                result(STREAM_ID, new OffsetRange(4, 7),
                        PayloadFormat.KAFKA_RECORD_BATCH, 3, 1, 3, List.of(SCHEMA))));
    }

    private static void assertInvariantViolation(Runnable operation) {
        assertThatThrownBy(operation::run)
                .isInstanceOfSatisfying(NereusException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(ErrorCode.METADATA_INVARIANT_VIOLATION);
                    assertThat(failure.appendOutcome()).contains(AppendOutcome.KNOWN_COMMITTED);
                });
    }

    private static AppendBatch request() {
        byte[] payload = "abc".getBytes(StandardCharsets.UTF_8);
        return new AppendBatch(
                PayloadFormat.KAFKA_RECORD_BATCH,
                List.of(new AppendEntry(payload, 3, 1, Map.of())),
                3,
                1,
                1,
                1,
                List.of(SCHEMA),
                Map.of(),
                Optional.empty());
    }

    private static AppendResult result(
            StreamId streamId,
            OffsetRange range,
            PayloadFormat payloadFormat,
            int recordCount,
            int entryCount,
            long logicalBytes,
            List<SchemaRef> schemaRefs) {
        return new AppendResult(
                streamId,
                range,
                range.endOffset(),
                logicalBytes,
                0,
                new BookKeeperEntryRangeReadTarget(
                        1,
                        "cluster-a",
                        1,
                        0,
                        1,
                        BookKeeperEntryMapping.ONE_NEREUS_ENTRY_PER_BOOKKEEPER_ENTRY,
                        new Checksum(ChecksumType.SHA256, "0".repeat(64))),
                payloadFormat,
                recordCount,
                entryCount,
                logicalBytes,
                schemaRefs,
                Optional.empty(),
                1);
    }
}
