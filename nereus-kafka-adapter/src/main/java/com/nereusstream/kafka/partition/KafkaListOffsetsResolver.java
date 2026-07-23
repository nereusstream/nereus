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

package com.nereusstream.kafka.partition;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.kafka.codec.KafkaFetchAssembly;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

/**
 * Resolves Kafka ListOffsets against one frozen stable partition boundary while delegating record iteration to the fork.
 * Timestamp scans are bounded by records, bytes, read operations, and the request deadline; exhaustion never returns an
 * approximate offset.
 */
public final class KafkaListOffsetsResolver {
    private final KafkaPartitionStorage storage;
    private final KafkaRecordTimestampInspector timestampInspector;

    public KafkaListOffsetsResolver(
            KafkaPartitionStorage storage,
            KafkaRecordTimestampInspector timestampInspector) {
        this.storage = Objects.requireNonNull(storage, "storage");
        this.timestampInspector = Objects.requireNonNull(timestampInspector, "timestampInspector");
    }

    /** Resolves one request, returning empty only when an exact timestamp/max-timestamp record does not exist. */
    public CompletableFuture<Optional<KafkaListOffsetResult>> resolve(KafkaListOffsetsRequest request) {
        Objects.requireNonNull(request, "request");
        Optional<NereusException> authorityFailure = authorityFailure(request);
        if (authorityFailure.isPresent()) {
            return CompletableFuture.failedFuture(authorityFailure.orElseThrow());
        }

        KafkaStableSnapshot snapshot = storage.stableSnapshot();
        authorityFailure = authorityFailure(request);
        if (authorityFailure.isPresent()) {
            return CompletableFuture.failedFuture(authorityFailure.orElseThrow());
        }
        if (request.query() == KafkaListOffsetQuery.EARLIEST) {
            return CompletableFuture.completedFuture(Optional.of(specialResult(
                    request.query(), snapshot.logStartOffset(), snapshot)));
        }
        if (request.query() == KafkaListOffsetQuery.LATEST) {
            return CompletableFuture.completedFuture(Optional.of(specialResult(
                    request.query(), snapshot.stableEndOffset(), snapshot)));
        }
        if (snapshot.logStartOffset() == snapshot.stableEndOffset()) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        ScanState initial = new ScanState(
                request,
                snapshot,
                new ScanDeadline(request.timeout()),
                snapshot.logStartOffset(),
                0,
                0,
                0,
                Optional.empty());
        return scan(initial);
    }

    private CompletableFuture<Optional<KafkaListOffsetResult>> scan(ScanState state) {
        Optional<NereusException> authorityFailure = authorityFailure(state.request);
        if (authorityFailure.isPresent()) {
            return CompletableFuture.failedFuture(authorityFailure.orElseThrow());
        }
        if (state.cursor >= state.snapshot.stableEndOffset()) {
            return CompletableFuture.completedFuture(finalResult(state));
        }
        if (state.readOperations >= state.request.maxReadOperations()) {
            return scanLimit("Kafka ListOffsets exceeded its read-operation budget");
        }
        long remainingRecords = state.request.maxScanRecords() - state.scannedRecords;
        long remainingBytes = state.request.maxScanBytes() - state.scannedBytes;
        if (remainingRecords <= 0 || remainingBytes <= 0) {
            return scanLimit("Kafka ListOffsets exhausted its committed-tail scan budget");
        }

        Duration remaining;
        try {
            remaining = state.deadline.remaining();
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        int readRecords = (int) Math.min(Integer.MAX_VALUE, remainingRecords);
        int targetBytes = (int) Math.min(state.request.readTargetBytes(), remainingBytes);
        KafkaStorageReadRequest readRequest = new KafkaStorageReadRequest(
                state.cursor,
                state.snapshot.stableEndOffset(),
                readRecords,
                Math.max(1, targetBytes),
                state.request.hardMaxReadBytes(),
                true,
                state.snapshot.logStartOffset(),
                0,
                remaining);
        CompletableFuture<KafkaStorageReadResult> read;
        try {
            read = storage.read(readRequest);
            if (read == null) {
                throw new IllegalStateException("KafkaPartitionStorage returned a null read future");
            }
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return read.thenCompose(value -> processPage(state, value));
    }

    private CompletableFuture<Optional<KafkaListOffsetResult>> processPage(
            ScanState state,
            KafkaStorageReadResult readResult) {
        try {
            Optional<NereusException> authorityFailure = authorityFailure(state.request);
            if (authorityFailure.isPresent()) {
                return CompletableFuture.failedFuture(authorityFailure.orElseThrow());
            }
            KafkaFetchAssembly page = Objects.requireNonNull(readResult, "readResult").fetchAssembly();
            if (readResult.stableSnapshot().logStartOffset() > state.cursor) {
                return failed(
                        ErrorCode.OFFSET_TRIMMED,
                        false,
                        "Kafka ListOffsets source was trimmed during the lookup");
            }
            if (page.sizeInBytes() == 0) {
                return failed(
                        ErrorCode.READ_LIMIT_TOO_SMALL,
                        false,
                        "Kafka ListOffsets could not make progress within its record/entry limits");
            }
            if (page.actualFirstBatchBaseOffset().isEmpty()
                    || page.actualFirstBatchBaseOffset().orElseThrow() > state.cursor
                    || page.nextLogicalOffset() <= state.cursor
                    || page.nextLogicalOffset() > state.snapshot.stableEndOffset()) {
                return invariant("Kafka ListOffsets read returned a non-contiguous committed page", null);
            }

            long pageRecords = page.nextLogicalOffset() - state.cursor;
            long scannedRecords = Math.addExact(state.scannedRecords, pageRecords);
            long scannedBytes = Math.addExact(state.scannedBytes, page.sizeInBytes());
            if (scannedRecords > state.request.maxScanRecords()
                    || scannedBytes > state.request.maxScanBytes()) {
                return scanLimit("Kafka ListOffsets page exceeded its remaining scan budget");
            }

            Optional<KafkaTimestampAndOffset> inspected = inspect(state, page);
            if (inspected.isPresent()) {
                KafkaTimestampAndOffset exact = inspected.orElseThrow();
                validateInspected(state, page.nextLogicalOffset(), exact);
                authorityFailure = authorityFailure(state.request);
                if (authorityFailure.isPresent()) {
                    return CompletableFuture.failedFuture(authorityFailure.orElseThrow());
                }
                if (state.request.query() == KafkaListOffsetQuery.TIMESTAMP) {
                    return CompletableFuture.completedFuture(Optional.of(result(state, exact)));
                }
            }

            Optional<KafkaTimestampAndOffset> best = state.bestMaximum;
            if (state.request.query() == KafkaListOffsetQuery.MAX_TIMESTAMP && inspected.isPresent()) {
                KafkaTimestampAndOffset candidate = inspected.orElseThrow();
                if (best.isEmpty()
                        || candidate.timestampMillis() > best.orElseThrow().timestampMillis()
                        || (candidate.timestampMillis() == best.orElseThrow().timestampMillis()
                                && candidate.offset() < best.orElseThrow().offset())) {
                    best = Optional.of(candidate);
                }
            }
            ScanState next = new ScanState(
                    state.request,
                    state.snapshot,
                    state.deadline,
                    page.nextLogicalOffset(),
                    scannedRecords,
                    scannedBytes,
                    state.readOperations + 1,
                    best);
            return scan(next);
        } catch (NereusException failure) {
            return CompletableFuture.failedFuture(failure);
        } catch (Throwable failure) {
            return invariant("Kafka ListOffsets inspector rejected committed Kafka bytes", failure);
        }
    }

    private Optional<KafkaTimestampAndOffset> inspect(ScanState state, KafkaFetchAssembly page) {
        if (state.request.query() == KafkaListOffsetQuery.TIMESTAMP) {
            return timestampInspector.firstAtOrAfter(
                    page.recordsBuffer(),
                    state.cursor,
                    state.request.targetTimestampMillis().orElseThrow());
        }
        return timestampInspector.maximum(page.recordsBuffer(), state.cursor);
    }

    private static void validateInspected(
            ScanState state,
            long pageEndOffset,
            KafkaTimestampAndOffset exact) {
        if (exact.offset() < state.cursor || exact.offset() >= pageEndOffset) {
            throw new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "Kafka timestamp inspector returned an offset outside the loaded page");
        }
        if (state.request.query() == KafkaListOffsetQuery.TIMESTAMP
                && exact.timestampMillis() < state.request.targetTimestampMillis().orElseThrow()) {
            throw new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "Kafka timestamp inspector returned a timestamp below the requested target");
        }
    }

    private Optional<NereusException> authorityFailure(KafkaListOffsetsRequest request) {
        if (request.expectedLeaderEpoch() != storage.leaderEpoch()) {
            return Optional.of(new NereusException(
                    ErrorCode.FENCED_APPEND,
                    false,
                    "Kafka ListOffsets request carries a stale leader epoch"));
        }
        KafkaPartitionState currentState = storage.state();
        if (currentState != KafkaPartitionState.LEADER_WRITABLE) {
            return Optional.of(new NereusException(
                    ErrorCode.FENCED_APPEND,
                    false,
                    "Kafka partition is not a writable current leader: " + currentState));
        }
        return Optional.empty();
    }

    private Optional<KafkaListOffsetResult> finalResult(ScanState state) {
        return state.bestMaximum.map(value -> result(state, value));
    }

    private static KafkaListOffsetResult result(ScanState state, KafkaTimestampAndOffset value) {
        return new KafkaListOffsetResult(
                state.request.query(),
                OptionalLong.of(value.timestampMillis()),
                value.offset(),
                value.leaderEpoch(),
                state.snapshot);
    }

    private static KafkaListOffsetResult specialResult(
            KafkaListOffsetQuery query,
            long offset,
            KafkaStableSnapshot snapshot) {
        return new KafkaListOffsetResult(
                query, OptionalLong.empty(), offset, OptionalInt.empty(), snapshot);
    }

    private static <T> CompletableFuture<T> scanLimit(String message) {
        return failed(ErrorCode.METADATA_LIMIT_EXCEEDED, false, message);
    }

    private static <T> CompletableFuture<T> invariant(String message, Throwable cause) {
        return NereusException.failedFuture(
                ErrorCode.METADATA_INVARIANT_VIOLATION, false, message, cause);
    }

    private static <T> CompletableFuture<T> failed(
            ErrorCode code,
            boolean retriable,
            String message) {
        return NereusException.failedFuture(code, retriable, message);
    }

    private record ScanState(
            KafkaListOffsetsRequest request,
            KafkaStableSnapshot snapshot,
            ScanDeadline deadline,
            long cursor,
            long scannedRecords,
            long scannedBytes,
            int readOperations,
            Optional<KafkaTimestampAndOffset> bestMaximum) {
        private ScanState {
            Objects.requireNonNull(request, "request");
            Objects.requireNonNull(snapshot, "snapshot");
            Objects.requireNonNull(deadline, "deadline");
            Objects.requireNonNull(bestMaximum, "bestMaximum");
        }
    }

    private static final class ScanDeadline {
        private final long startedNanos = System.nanoTime();
        private final long timeoutNanos;

        private ScanDeadline(Duration timeout) {
            timeoutNanos = timeout.toNanos();
        }

        private Duration remaining() {
            long remaining = timeoutNanos - (System.nanoTime() - startedNanos);
            if (remaining <= 0) {
                throw new NereusException(
                        ErrorCode.TIMEOUT, true, "Kafka ListOffsets committed-tail scan timed out");
            }
            return Duration.ofNanos(remaining);
        }
    }
}
