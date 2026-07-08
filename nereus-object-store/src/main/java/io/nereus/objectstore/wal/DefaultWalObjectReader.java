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

package io.nereus.objectstore.wal;

import io.nereus.api.EntryIndexLocation;
import io.nereus.api.ErrorCode;
import io.nereus.api.NereusException;
import io.nereus.api.ObjectKey;
import io.nereus.api.ObjectType;
import io.nereus.api.OffsetRange;
import io.nereus.api.PayloadFormat;
import io.nereus.api.ReadBatch;
import io.nereus.api.ReadOptions;
import io.nereus.api.ResolvedObjectRange;
import io.nereus.objectstore.Crc32cChecksums;
import io.nereus.objectstore.ObjectStore;
import io.nereus.objectstore.RangeReadOptions;
import io.nereus.objectstore.RangeReadResult;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

public final class DefaultWalObjectReader implements WalObjectReader {
    private final ObjectStore objectStore;
    private final ReadResourceGuard resourceGuard;
    private final WalReadObserver readObserver;

    public DefaultWalObjectReader(ObjectStore objectStore) {
        this(objectStore, ReadResourceGuard.unbounded(), WalReadObserver.noop());
    }

    public DefaultWalObjectReader(
            ObjectStore objectStore,
            ReadResourceGuard resourceGuard,
            WalReadObserver readObserver) {
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
        this.resourceGuard = Objects.requireNonNull(resourceGuard, "resourceGuard");
        this.readObserver = Objects.requireNonNull(readObserver, "readObserver");
    }

    @Override
    public CompletableFuture<List<ReadBatch>> read(
            long startOffset,
            List<ResolvedObjectRange> ranges,
            ReadOptions options) {
        Objects.requireNonNull(ranges, "ranges");
        Objects.requireNonNull(options, "options");
        if (startOffset < 0) {
            return NereusException.failedFuture(ErrorCode.INVALID_ARGUMENT, false, "startOffset must be non-negative");
        }
        try {
            List<ReadBatch> batches = new ArrayList<>();
            int remainingRecords = options.maxRecords();
            int remainingBytes = options.maxBytes();
            for (ResolvedObjectRange range : ranges) {
                if (remainingRecords <= 0) {
                    break;
                }
                SliceRead sliceRead = readSlice(range, options);
                int returnedBefore = batches.stream().mapToInt(batch -> batch.payload().length).sum();
                ClipResult clipped = clip(
                        startOffset,
                        range,
                        sliceRead.payload(),
                        sliceRead.entryIndex(),
                        remainingRecords,
                        remainingBytes,
                        !batches.isEmpty());
                batches.addAll(clipped.batches());
                remainingRecords -= clipped.recordsReturned();
                remainingBytes -= clipped.bytesReturned();
                int returnedAfter = batches.stream().mapToInt(batch -> batch.payload().length).sum();
                readObserver.onSliceRead(
                        range.objectLength(),
                        range.entryIndexRef().length(),
                        returnedAfter - returnedBefore);
                if (clipped.limitReached()) {
                    break;
                }
            }
            return CompletableFuture.completedFuture(List.copyOf(batches));
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private SliceRead readSlice(ResolvedObjectRange range, ReadOptions options) {
        validateRange(range);
        long bytesToReserve = checkedAdd(range.objectLength(), range.entryIndexRef().length());
        try (ReadResourceGuard.Reservation ignored = resourceGuard.reserve(bytesToReserve)) {
            byte[] payload = readRangeBytes(
                    range.objectKey(),
                    range.objectOffset(),
                    range.objectLength(),
                    options);
            ObjectKey indexObjectKey = range.entryIndexRef().objectKey().orElse(range.objectKey());
            byte[] entryIndexBytes = readRangeBytes(
                    indexObjectKey,
                    range.entryIndexRef().offset(),
                    range.entryIndexRef().length(),
                    options);
            if (!Crc32cChecksums.checksum(payload, entryIndexBytes).equals(range.sliceChecksum())) {
                throw failure(ErrorCode.OBJECT_CHECKSUM_MISMATCH, false, "slice checksum mismatch");
            }
            if (!Crc32cChecksums.checksum(entryIndexBytes).equals(range.entryIndexRef().checksum())) {
                throw failure(ErrorCode.OBJECT_CHECKSUM_MISMATCH, false, "entry index checksum mismatch");
            }
            EntryIndex entryIndex = EntryIndexDecoder.decode(
                    entryIndexBytes,
                    payload.length,
                    minEventTime(range),
                    maxEventTime(range));
            return new SliceRead(payload, entryIndex);
        }
    }

    private ClipResult clip(
            long startOffset,
            ResolvedObjectRange range,
            byte[] payload,
            EntryIndex entryIndex,
            int maxRecords,
            int maxBytes,
            boolean returnedRecordBeforeRange) {
        List<ReadBatch> batches = new ArrayList<>();
        int recordsReturned = 0;
        int bytesReturned = 0;
        boolean selectedAny = false;
        for (EntryIndexItem item : entryIndex.entries()) {
            long absoluteOffset = Math.addExact(range.offsetRange().startOffset(), item.relativeBaseOffset());
            if (absoluteOffset < startOffset || absoluteOffset >= range.offsetRange().endOffset()) {
                continue;
            }
            selectedAny = true;
            if (recordsReturned + item.recordCount() > maxRecords) {
                return new ClipResult(batches, recordsReturned, bytesReturned, true);
            }
            if (item.payloadLength() > maxBytes - bytesReturned) {
                if (!returnedRecordBeforeRange && batches.isEmpty()) {
                    throw failure(ErrorCode.READ_LIMIT_TOO_SMALL, true, "first readable entry exceeds maxBytes");
                }
                return new ClipResult(batches, recordsReturned, bytesReturned, true);
            }
            byte[] entryPayload = new byte[Math.toIntExact(item.payloadLength())];
            System.arraycopy(payload, Math.toIntExact(item.payloadOffset()), entryPayload, 0, entryPayload.length);
            batches.add(new ReadBatch(
                    new OffsetRange(absoluteOffset, absoluteOffset + item.recordCount()),
                    range.payloadFormat(),
                    entryPayload,
                    range.schemaRefs(),
                    range.entryIndexRef(),
                    range.projectionRef(),
                    range.objectId(),
                    range.objectOffset() + item.payloadOffset(),
                    item.payloadLength()));
            recordsReturned += item.recordCount();
            bytesReturned += entryPayload.length;
        }
        return new ClipResult(batches, recordsReturned, bytesReturned, selectedAny && recordsReturned >= maxRecords);
    }

    private byte[] readRangeBytes(
            ObjectKey objectKey,
            long offset,
            long length,
            ReadOptions options) {
        RangeReadResult result;
        try {
            result = objectStore.readRange(
                            objectKey,
                            offset,
                            length,
                            new RangeReadOptions(Optional.empty(), options.timeout()))
                    .join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw e;
        }
        ByteBuffer buffer = result.payload().asReadOnlyBuffer();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    private void validateRange(ResolvedObjectRange range) {
        Objects.requireNonNull(range, "range");
        if (range.objectType() != ObjectType.MULTI_STREAM_WAL_OBJECT) {
            throw failure(ErrorCode.UNSUPPORTED_FORMAT, false, "unsupported object type");
        }
        if (range.payloadFormat() != PayloadFormat.OPAQUE_RECORD_BATCH) {
            throw failure(ErrorCode.UNSUPPORTED_FORMAT, false, "unsupported payload format");
        }
        if (range.entryIndexRef().location() != EntryIndexLocation.OBJECT_FOOTER) {
            throw failure(ErrorCode.UNSUPPORTED_FORMAT, false, "unsupported entry index location");
        }
        if (range.entryIndexRef().objectId().isPresent() != range.entryIndexRef().objectKey().isPresent()) {
            throw failure(ErrorCode.UNSUPPORTED_FORMAT, false, "entry index object identity is incomplete");
        }
    }

    private long checkedAdd(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException e) {
            throw failure(ErrorCode.INVALID_ARGUMENT, false, "read reservation size overflow", e);
        }
    }

    private long minEventTime(ResolvedObjectRange ignored) {
        return 0;
    }

    private long maxEventTime(ResolvedObjectRange ignored) {
        return Long.MAX_VALUE;
    }

    private static NereusException failure(ErrorCode code, boolean retriable, String message) {
        return new NereusException(code, retriable, message);
    }

    private static NereusException failure(ErrorCode code, boolean retriable, String message, Throwable cause) {
        return new NereusException(code, retriable, message, cause);
    }

    private record SliceRead(byte[] payload, EntryIndex entryIndex) {
    }

    private record ClipResult(
            List<ReadBatch> batches,
            int recordsReturned,
            int bytesReturned,
            boolean limitReached) {
        private ClipResult {
            batches = List.copyOf(batches);
        }
    }
}
