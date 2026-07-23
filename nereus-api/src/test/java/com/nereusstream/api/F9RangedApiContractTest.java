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

package com.nereusstream.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.target.ObjectSliceReadTarget;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.Test;

class F9RangedApiContractTest {
    private static final StreamId STREAM_ID = new StreamId("f9-api-stream");
    private static final ReadOptions READ_OPTIONS = new ReadOptions(
            100, 1024, ReadIsolation.COMMITTED, Duration.ofSeconds(1));

    @Test
    void kafkaBatchAcceptsRangedEntriesAndPreservesCallerOrder() {
        AppendEntry first = new AppendEntry(new byte[] {1, 2}, 3, 10, Map.of());
        AppendEntry second = new AppendEntry(new byte[] {3}, 2, 12, Map.of());

        AppendBatch batch = new AppendBatch(
                PayloadFormat.KAFKA_RECORD_BATCH,
                List.of(first, second),
                5,
                2,
                10,
                12,
                List.of(),
                Map.of(),
                Optional.empty());

        assertThat(batch.entries()).containsExactly(first, second);
        assertThat(batch.recordCount()).isEqualTo(5);
        assertThat(batch.entryCount()).isEqualTo(2);
    }

    @Test
    void kafkaBatchRejectsRecordCountOverflowAndEntryLimit() {
        AppendEntry maximum = new AppendEntry(new byte[0], Integer.MAX_VALUE, 1, Map.of());
        AppendEntry one = new AppendEntry(new byte[0], 1, 1, Map.of());

        assertThatThrownBy(() -> kafkaBatch(List.of(maximum, one), Integer.MAX_VALUE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("overflows int");
        assertThatThrownBy(() -> kafkaBatch(
                Collections.nCopies(ApiLimits.MAX_APPEND_ENTRIES + 1, one),
                ApiLimits.MAX_APPEND_ENTRIES + 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maximum append entry count");
    }

    @Test
    void kafkaBatchChecksumCoversExactConcatenatedPayloadOrder() {
        AppendEntry first = new AppendEntry(new byte[] {1, 2}, 2, 1, Map.of());
        AppendEntry second = new AppendEntry(new byte[] {3, 4}, 3, 1, Map.of());
        Checksum checksum = crc32c(first, second);

        AppendBatch batch = new AppendBatch(
                PayloadFormat.KAFKA_RECORD_BATCH,
                List.of(first, second),
                5,
                2,
                1,
                1,
                List.of(),
                Map.of(),
                Optional.of(checksum));

        assertThat(batch.checksum()).contains(checksum);
        AppendEntry changed = new AppendEntry(new byte[] {4, 3}, 3, 1, Map.of());
        assertThatThrownBy(() -> new AppendBatch(
                PayloadFormat.KAFKA_RECORD_BATCH,
                List.of(first, changed),
                5,
                2,
                1,
                1,
                List.of(),
                Map.of(),
                Optional.of(checksum)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("checksum mismatch");
    }

    @Test
    void opaqueBatchRetainsOneEntryPerOffsetRule() {
        AppendEntry ranged = new AppendEntry(new byte[] {1}, 2, 1, Map.of());

        assertThatThrownBy(() -> new AppendBatch(
                PayloadFormat.OPAQUE_RECORD_BATCH,
                List.of(ranged),
                2,
                1,
                1,
                1,
                List.of(),
                Map.of(),
                Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("recordCount == 1");
    }

    @Test
    void appendPreconditionIsClosedAndNonNegative() {
        assertThat(AppendPrecondition.none().expectedStartOffset()).isEmpty();
        assertThat(AppendPrecondition.expectedStartOffset(0).expectedStartOffset()).hasValue(0);
        assertThatThrownBy(() -> AppendPrecondition.expectedStartOffset(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void legacyProviderDelegatesOnlyEmptyAppendPrecondition() {
        LegacyStorage storage = new LegacyStorage();
        AppendBatch batch = kafkaBatch(
                List.of(new AppendEntry(new byte[] {1}, 2, 1, Map.of())), 2);
        AppendOptions options = appendOptions();

        assertThat(storage.append(STREAM_ID, batch, options, AppendPrecondition.none()))
                .isSameAs(storage.appendResult);
        assertThat(storage.appendCalls).hasValue(1);

        NereusException failure = failure(storage.append(
                STREAM_ID, batch, options, AppendPrecondition.expectedStartOffset(2)));
        assertThat(failure.code()).isEqualTo(ErrorCode.UNSUPPORTED_APPEND_PRECONDITION);
        assertThat(failure.retriable()).isFalse();
        assertThat(storage.appendCalls).hasValue(1);
    }

    @Test
    void legacyProviderDelegatesOnlyEmptyAppendAuthority() {
        LegacyStorage storage = new LegacyStorage();
        AppendSessionOptions options = new AppendSessionOptions("writer", Duration.ofSeconds(1), false);

        AcquiredAppendSession acquired = storage.acquireAppendSession(
                STREAM_ID, AppendSessionRequest.legacy(options)).join();

        assertThat(acquired.session()).isEqualTo(storage.appendSession);
        assertThat(acquired.authority()).isEmpty();
        assertThat(storage.sessionCalls).hasValue(1);
        NereusException failure = failure(storage.acquireAppendSession(
                STREAM_ID,
                AppendSessionRequest.authoritative(
                        options,
                        new AppendAuthority("authority", "id", 1, "owner", 2))));
        assertThat(failure.code()).isEqualTo(ErrorCode.UNSUPPORTED_APPEND_AUTHORITY);
        assertThat(storage.sessionCalls).hasValue(1);
    }

    @Test
    void legacyProviderRejectsExplicitSessionRenewalWithoutThrowingFromValidation() {
        LegacyStorage storage = new LegacyStorage();

        assertThat(failure(storage.renewAppendSession(storage.appendSession, Duration.ofSeconds(1))).code())
                .isEqualTo(ErrorCode.UNSUPPORTED_APPEND_AUTHORITY);
        assertThat(failure(storage.renewAppendSession(storage.appendSession, Duration.ofNanos(1))).code())
                .isEqualTo(ErrorCode.INVALID_ARGUMENT);
        assertThat(failure(storage.renewAppendSession(
                        storage.appendSession, Duration.ofSeconds(Long.MAX_VALUE))).code())
                .isEqualTo(ErrorCode.INVALID_ARGUMENT);
    }

    @Test
    void legacyProviderWrapsOnlyLegacyEquivalentReadRequest() {
        LegacyStorage storage = new LegacyStorage();
        ReadRequest legacy = new ReadRequest(
                7,
                ReadView.COMMITTED,
                ReadBoundaryMode.EXACT_START,
                FirstEntryPolicy.LEGACY_STRICT_LIMIT,
                READ_OPTIONS);

        SemanticReadResult result = storage.read(STREAM_ID, legacy).join();

        assertThat(result.view()).isEqualTo(ReadView.COMMITTED);
        assertThat(result.result().requestedOffset()).isEqualTo(7);
        assertThat(result.sourceCoverageEndOffset()).isEqualTo(7);
        assertThat(storage.readCalls).hasValue(1);

        ReadRequest containing = new ReadRequest(
                7,
                ReadView.COMMITTED,
                ReadBoundaryMode.CONTAINING_ENTRY,
                FirstEntryPolicy.ALLOW_FIRST_ENTRY_OVERFLOW,
                READ_OPTIONS);
        assertThat(failure(storage.read(STREAM_ID, containing)).code())
                .isEqualTo(ErrorCode.UNSUPPORTED_READ_SEMANTICS);
        assertThat(storage.readCalls).hasValue(1);
    }

    @Test
    void semanticResultEnforcesRequestBoundaryDensityAndCoverage() {
        ReadRequest containing = new ReadRequest(
                12,
                ReadView.COMMITTED,
                ReadBoundaryMode.CONTAINING_ENTRY,
                FirstEntryPolicy.ALLOW_FIRST_ENTRY_OVERFLOW,
                READ_OPTIONS);
        ReadResult valid = new ReadResult(
                STREAM_ID,
                12,
                15,
                List.of(readBatch(new OffsetRange(10, 15), new OffsetRange(10, 15))),
                false);

        assertThat(SemanticReadResult.forRequest(containing, valid, 15).result()).isEqualTo(valid);
        assertThatThrownBy(() -> SemanticReadResult.forRequest(
                new ReadRequest(
                        12,
                        ReadView.COMMITTED,
                        ReadBoundaryMode.EXACT_START,
                        FirstEntryPolicy.LEGACY_STRICT_LIMIT,
                        READ_OPTIONS),
                valid,
                15))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("EXACT_START");
        assertThatThrownBy(() -> new SemanticReadResult(ReadView.COMMITTED, valid, 16))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("COMMITTED");

        ReadResult sparse = new ReadResult(STREAM_ID, 12, 12, List.of(), false);
        SemanticReadResult sparseResult = SemanticReadResult.forRequest(
                new ReadRequest(
                        12,
                        ReadView.TOPIC_COMPACTED,
                        ReadBoundaryMode.CONTAINING_ENTRY,
                        FirstEntryPolicy.ALLOW_FIRST_ENTRY_OVERFLOW,
                        READ_OPTIONS),
                sparse,
                20);
        assertThat(sparseResult.sourceCoverageEndOffset()).isEqualTo(20);
    }

    @Test
    void newStreamStorageMethodsRemainBinarySafeDefaults() throws NoSuchMethodException {
        assertThat(StreamStorage.class.getMethod(
                "append",
                StreamId.class,
                AppendBatch.class,
                AppendOptions.class,
                AppendPrecondition.class).isDefault()).isTrue();
        assertThat(StreamStorage.class.getMethod(
                "read", StreamId.class, ReadRequest.class).isDefault()).isTrue();
        assertThat(StreamStorage.class.getMethod(
                "acquireAppendSession", StreamId.class, AppendSessionRequest.class).isDefault()).isTrue();
        assertThat(StreamStorage.class.getMethod(
                "renewAppendSession", AppendSession.class, Duration.class).isDefault()).isTrue();
        assertThat(StreamStorage.class.getMethod(
                "append", StreamId.class, AppendBatch.class, AppendOptions.class).isDefault()).isFalse();
        assertThat(StreamStorage.class.getMethod(
                "read", StreamId.class, long.class, ReadOptions.class).isDefault()).isFalse();
    }

    private static AppendBatch kafkaBatch(List<AppendEntry> entries, int recordCount) {
        return new AppendBatch(
                PayloadFormat.KAFKA_RECORD_BATCH,
                entries,
                recordCount,
                entries.size(),
                1,
                1,
                List.of(),
                Map.of(),
                Optional.empty());
    }

    private static AppendOptions appendOptions() {
        return new AppendOptions(
                Optional.empty(),
                DurabilityLevel.WAL_DURABLE,
                Duration.ofSeconds(1),
                true,
                Map.of());
    }

    private static ReadBatch readBatch(OffsetRange range, OffsetRange resolvedRange) {
        EntryIndexRef index = new EntryIndexRef(
                EntryIndexLocation.OBJECT_FOOTER,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                0,
                1,
                crc32c());
        ObjectSliceReadTarget target = new ObjectSliceReadTarget(
                1,
                new ObjectId("f9-read-object"),
                new ObjectKey("f9/read/object"),
                ObjectType.MULTI_STREAM_WAL_OBJECT,
                "WAL_OBJECT_V1",
                "KAFKA_RECORD_BATCH_V1",
                "slice",
                0,
                1,
                crc32c(),
                index);
        ReadSourceRef source = new ReadSourceRef(
                resolvedRange,
                0,
                1,
                target,
                ReadTargetIdentities.sha256(target));
        return new ReadBatch(
                range,
                PayloadFormat.KAFKA_RECORD_BATCH,
                new byte[] {1},
                List.of(),
                Optional.empty(),
                source);
    }

    private static Checksum crc32c() {
        return new Checksum(ChecksumType.CRC32C, "00000000");
    }

    private static Checksum crc32c(AppendEntry... entries) {
        CRC32C crc32c = new CRC32C();
        for (AppendEntry entry : entries) {
            byte[] payload = entry.payload();
            crc32c.update(payload, 0, payload.length);
        }
        return new Checksum(
                ChecksumType.CRC32C,
                String.format(Locale.ROOT, "%08x", crc32c.getValue()));
    }

    private static NereusException failure(CompletableFuture<?> future) {
        try {
            future.join();
            throw new AssertionError("future unexpectedly completed successfully");
        } catch (CompletionException failure) {
            assertThat(failure.getCause()).isInstanceOf(NereusException.class);
            return (NereusException) failure.getCause();
        }
    }

    private static final class LegacyStorage implements StreamStorage {
        private final CompletableFuture<AppendResult> appendResult = new CompletableFuture<>();
        private final AppendSession appendSession =
                new AppendSession(STREAM_ID, "writer", 1, "token", 1, 1_000);
        private final AtomicInteger appendCalls = new AtomicInteger();
        private final AtomicInteger sessionCalls = new AtomicInteger();
        private final AtomicInteger readCalls = new AtomicInteger();

        @Override
        public CompletableFuture<StreamMetadata> createOrGetStream(
                StreamName streamName, StreamCreateOptions options) {
            return unsupported();
        }

        @Override
        public CompletableFuture<AppendSession> acquireAppendSession(
                StreamId streamId, AppendSessionOptions options) {
            sessionCalls.incrementAndGet();
            return CompletableFuture.completedFuture(appendSession);
        }

        @Override
        public CompletableFuture<AppendResult> append(
                StreamId streamId, AppendBatch batch, AppendOptions options) {
            appendCalls.incrementAndGet();
            return appendResult;
        }

        @Override
        public CompletableFuture<AppendResult> recoverAppend(
                StreamId streamId, AppendAttemptId attemptId, AppendRecoveryOptions options) {
            return unsupported();
        }

        @Override
        public CompletableFuture<ReadResult> read(
                StreamId streamId, long startOffset, ReadOptions options) {
            readCalls.incrementAndGet();
            return CompletableFuture.completedFuture(
                    new ReadResult(streamId, startOffset, startOffset, List.of(), true));
        }

        @Override
        public CompletableFuture<ResolveResult> resolve(
                StreamId streamId, long startOffset, ResolveOptions options) {
            return unsupported();
        }

        @Override
        public CompletableFuture<Void> trim(
                StreamId streamId, long beforeOffset, TrimOptions options) {
            return unsupported();
        }

        @Override
        public CompletableFuture<StreamMetadata> getStreamMetadata(StreamId streamId) {
            return unsupported();
        }

        @Override
        public CompletableFuture<StreamMetadata> seal(StreamId streamId, SealOptions options) {
            return unsupported();
        }

        @Override
        public CompletableFuture<StreamMetadata> delete(StreamId streamId, DeleteOptions options) {
            return unsupported();
        }

        @Override
        public void close() {
        }

        private static <T> CompletableFuture<T> unsupported() {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }
    }
}
