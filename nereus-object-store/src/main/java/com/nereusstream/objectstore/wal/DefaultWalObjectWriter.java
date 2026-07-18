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

package com.nereusstream.objectstore.wal;

import com.nereusstream.api.AppendBatch;
import com.nereusstream.api.AppendEntry;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.SchemaRef;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.keys.DeterministicIds;
import com.nereusstream.api.keys.KeyComponentCodec;
import com.nereusstream.objectstore.ByteBufferObjectUpload;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.PutObjectAttemptGuard;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.objectstore.PutObjectResult;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicLong;

public final class DefaultWalObjectWriter implements WalObjectWriter {
    private static final DateTimeFormatter OBJECT_TIME =
            DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneOffset.UTC);
    private static final String CONTENT_TYPE = "application/vnd.nereus.wal-object";

    private final ObjectStore objectStore;
    private final String writerVersion;
    private final Clock clock;
    private final AtomicLong nextSequence = new AtomicLong();

    public DefaultWalObjectWriter(ObjectStore objectStore, String writerVersion, Clock clock) {
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
        this.writerVersion = requireNonBlank(writerVersion, "writerVersion");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public PreparedWalObject prepare(WalWriteRequest request) {
        Objects.requireNonNull(request, "request");
        try {
            LayoutPlan plan = plan(request);
            WalObjectLayout.EncodedObject encoded = WalObjectLayout.encodeObject(
                    plan.sections(),
                    plan.footerOffset(),
                    plan.footerLength());
            WalWriteResult result = new WalWriteResult(
                    plan.objectId(),
                    plan.objectKey(),
                    plan.objectLength(),
                    encoded.objectChecksum(),
                    encoded.storageChecksum(),
                    WalObjectLayout.FORMAT_MAJOR,
                    WalObjectLayout.FORMAT_MINOR,
                    writerVersion,
                    plan.createdAtMillis(),
                    plan.writtenSlices());
            return new PreparedWalObject(result, encoded.bytes(), request.options().uploadTimeout());
        } catch (NereusException e) {
            throw e;
        } catch (RuntimeException e) {
            throw failure(ErrorCode.INVALID_ARGUMENT, false, e.getMessage(), e);
        }
    }

    @Override
    public CompletableFuture<WalWriteResult> upload(PreparedWalObject preparedObject) {
        return upload(
                preparedObject,
                (ignored, attempt) -> CompletableFuture.completedFuture(null));
    }

    @Override
    public CompletableFuture<WalWriteResult> upload(
            PreparedWalObject preparedObject,
            PutObjectAttemptGuard attemptGuard) {
        Objects.requireNonNull(preparedObject, "preparedObject");
        Objects.requireNonNull(attemptGuard, "attemptGuard");
        WalWriteResult result = preparedObject.result();
        PutObjectOptions options = new PutObjectOptions(
                CONTENT_TYPE,
                result.storageChecksum(),
                true,
                Map.of("objectChecksum", result.objectChecksum().value()),
                preparedObject.uploadTimeout());
        ByteBufferObjectUpload source = new ByteBufferObjectUpload(
                preparedObject.payload());
        CompletableFuture<PutObjectResult> upload;
        try {
            upload = objectStore.putObject(
                    result.objectKey(), source, options, attemptGuard);
        } catch (Throwable failure) {
            source.close();
            return CompletableFuture.failedFuture(failure);
        }
        upload.whenComplete((ignored, failure) -> source.close());
        return upload
                .thenApply(putResult -> verifyPutResult(result, putResult))
                .exceptionally(DefaultWalObjectWriter::unwrapCompletionException);
    }

    private LayoutPlan plan(WalWriteRequest request) {
        if (request.options().compression() != CompressionType.NONE) {
            throw failure(ErrorCode.UNSUPPORTED_FORMAT, false, "Phase 1 supports only CompressionType.NONE");
        }
        if (request.options().forceSingleStreamObject()
                && request.slices().stream().map(WalStreamSliceInput::streamId).distinct().count() > 1) {
            throw failure(ErrorCode.INVALID_ARGUMENT, false, "forceSingleStreamObject rejects multiple stream ids");
        }
        Instant now = clock.instant();
        long sequence = nextSequence.getAndIncrement();
        String writerIdHash = DeterministicIds.stableHashComponent(request.writerId());
        ObjectId objectId = new ObjectId("wo-" + OBJECT_TIME.format(now) + "-"
                + writerIdHash + "-" + request.writerRunIdHash() + "-"
                + KeyComponentCodec.encodeNonNegativeLong(sequence));
        ObjectKey objectKey = WalObjectKeys.objectKey(
                request.cluster(), writerIdHash, request.writerRunIdHash(), objectId, now);
        List<WalStreamSliceInput> sortedInputs = request.slices().stream()
                .sorted(Comparator.comparing((WalStreamSliceInput input) -> input.streamId().value()))
                .toList();
        List<PreparedSlice> preparedSlices = new ArrayList<>();
        for (int ordinal = 0; ordinal < sortedInputs.size(); ordinal++) {
            preparedSlices.add(prepareSlice(objectId, request.writerEpoch(), ordinal, sortedInputs.get(ordinal)));
        }
        long minEventTime = preparedSlices.stream()
                .mapToLong(slice -> slice.batch().minEventTimeMillis())
                .min()
                .orElse(0);
        long maxEventTime = preparedSlices.stream()
                .mapToLong(slice -> slice.batch().maxEventTimeMillis())
                .max()
                .orElse(0);

        byte[] headerPayload = WalObjectLayout.encodeWalObjectHeader(
                objectId,
                request.cluster(),
                request.writerId(),
                request.writerRunIdHash(),
                request.writerEpoch(),
                writerVersion,
                now.toEpochMilli(),
                request.options().compression(),
                preparedSlices.size(),
                preparedSlices.size(),
                minEventTime,
                maxEventTime);
        List<StreamSliceDescriptor> placeholderDescriptors = preparedSlices.stream()
                .map(slice -> slice.descriptor(0, 0))
                .toList();
        int directoryLength = WalObjectLayout.sectionEncodedLength(
                WalObjectLayout.encodeSliceDirectory(placeholderDescriptors));
        long offset = WalObjectLayout.COMMON_HEADER_LENGTH;
        offset = checkedAdd(offset, WalObjectLayout.sectionEncodedLength(headerPayload));
        offset = checkedAdd(offset, directoryLength);

        List<StreamSliceDescriptor> descriptors = new ArrayList<>();
        for (PreparedSlice slice : preparedSlices) {
            long payloadOffset = checkedAdd(offset, WalObjectLayout.SECTION_HEADER_LENGTH);
            offset = checkedAdd(offset, WalObjectLayout.sectionEncodedLength(slice.payload()));
            long entryIndexOffset = checkedAdd(offset, WalObjectLayout.SECTION_HEADER_LENGTH);
            offset = checkedAdd(offset, WalObjectLayout.sectionEncodedLength(slice.entryIndexBytes()));
            descriptors.add(slice.descriptor(payloadOffset, entryIndexOffset));
        }
        byte[] directoryPayload = WalObjectLayout.encodeSliceDirectory(descriptors);
        if (WalObjectLayout.sectionEncodedLength(directoryPayload) != directoryLength) {
            throw failure(ErrorCode.INVALID_ARGUMENT, false, "slice directory length changed after layout");
        }
        byte[] footerPayload = WalObjectLayout.encodeFooter(objectId, descriptors);
        long footerOffset = offset;
        int footerLength = WalObjectLayout.sectionEncodedLength(footerPayload);
        long finalObjectLength = checkedAdd(footerOffset, footerLength);
        if (finalObjectLength > request.options().maxObjectBytes()) {
            throw failure(ErrorCode.INVALID_ARGUMENT, false, "encoded WAL object exceeds maxObjectBytes");
        }
        if (finalObjectLength > Integer.MAX_VALUE) {
            throw failure(ErrorCode.INVALID_ARGUMENT, false, "encoded WAL object exceeds in-memory limit");
        }

        List<WalObjectLayout.Section> sections = new ArrayList<>();
        sections.add(new WalObjectLayout.Section(WalObjectLayout.SECTION_WAL_OBJECT_HEADER, headerPayload));
        sections.add(new WalObjectLayout.Section(WalObjectLayout.SECTION_STREAM_SLICE_DIRECTORY, directoryPayload));
        for (PreparedSlice slice : preparedSlices) {
            sections.add(new WalObjectLayout.Section(WalObjectLayout.SECTION_PAYLOAD_BLOCK, slice.payload()));
            sections.add(new WalObjectLayout.Section(WalObjectLayout.SECTION_ENTRY_INDEX, slice.entryIndexBytes()));
        }
        sections.add(new WalObjectLayout.Section(WalObjectLayout.SECTION_FOOTER, footerPayload));

        List<WrittenStreamSlice> writtenSlices = new ArrayList<>();
        for (int i = 0; i < descriptors.size(); i++) {
            StreamSliceDescriptor descriptor = descriptors.get(i);
            writtenSlices.add(new WrittenStreamSlice(
                    descriptor.streamId(),
                    descriptor.sliceId(),
                    descriptor.payloadOffset(),
                    descriptor.payloadLength(),
                    descriptor.recordCount(),
                    descriptor.entryCount(),
                    descriptor.logicalBytes(),
                    descriptor.schemaRefs(),
                    descriptor.payloadFormat(),
                    new EntryIndexRef(
                            EntryIndexLocation.OBJECT_FOOTER,
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty(),
                            descriptor.entryIndexOffset(),
                            descriptor.entryIndexLength(),
                            preparedSlices.get(i).entryIndexChecksum()),
                    descriptor.checksum(),
                    descriptor.minEventTimeMillis(),
                    descriptor.maxEventTimeMillis()));
        }
        return new LayoutPlan(
                objectId,
                objectKey,
                finalObjectLength,
                now.toEpochMilli(),
                footerOffset,
                footerLength,
                sections,
                writtenSlices);
    }

    private PreparedSlice prepareSlice(
            ObjectId objectId,
            long writerEpoch,
            int ordinal,
            WalStreamSliceInput input) {
        AppendBatch batch = input.batch();
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        List<EntryIndexItem> items = new ArrayList<>();
        long relativeBaseOffset = 0;
        long payloadOffset = 0;
        for (int i = 0; i < batch.entries().size(); i++) {
            AppendEntry entry = batch.entries().get(i);
            byte[] bytes = entry.payload();
            payload.writeBytes(bytes);
            items.add(new EntryIndexItem(
                    i,
                    relativeBaseOffset,
                    entry.recordCount(),
                    payloadOffset,
                    bytes.length,
                    entry.eventTimeMillis(),
                    entry.attributes()));
            relativeBaseOffset = Math.addExact(relativeBaseOffset, entry.recordCount());
            payloadOffset = Math.addExact(payloadOffset, bytes.length);
        }
        byte[] payloadBytes = payload.toByteArray();
        EntryIndex entryIndex = new EntryIndex(batch.entryCount(), batch.recordCount(), items);
        byte[] entryIndexBytes = EntryIndexEncoder.encode(entryIndex);
        Checksum entryIndexChecksum = com.nereusstream.objectstore.Crc32cChecksums.checksum(entryIndexBytes);
        Checksum sliceChecksum = com.nereusstream.objectstore.Crc32cChecksums.checksum(payloadBytes, entryIndexBytes);
        String sliceId = objectId.value() + "/" + KeyComponentCodec.encodeNonNegativeLong(ordinal);
        return new PreparedSlice(
                input.streamId(),
                sliceId,
                writerEpoch,
                ordinal,
                batch,
                payloadBytes,
                entryIndexBytes,
                entryIndexChecksum,
                sliceChecksum);
    }

    private WalWriteResult verifyPutResult(WalWriteResult result, PutObjectResult putResult) {
        if (!putResult.checksum().equals(result.storageChecksum())) {
            throw failure(ErrorCode.OBJECT_CHECKSUM_MISMATCH, false, "object store returned mismatched storage checksum");
        }
        if (putResult.objectLength() != result.objectLength()) {
            throw failure(ErrorCode.OBJECT_CHECKSUM_MISMATCH, false, "object store returned mismatched object length");
        }
        return result;
    }

    private static long checkedAdd(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException e) {
            throw failure(ErrorCode.INVALID_ARGUMENT, false, "WAL layout size overflow", e);
        }
    }

    private static NereusException failure(ErrorCode code, boolean retriable, String message) {
        return new NereusException(code, retriable, message);
    }

    private static NereusException failure(ErrorCode code, boolean retriable, String message, Throwable cause) {
        return new NereusException(code, retriable, message, cause);
    }

    private static <T> T unwrapCompletionException(Throwable throwable) {
        if (throwable instanceof CompletionException completionException
                && completionException.getCause() instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        if (throwable instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        throw new CompletionException(throwable);
    }

    private static String requireNonBlank(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName);
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " cannot be blank");
        }
        return value;
    }

    private record PreparedSlice(
            StreamId streamId,
            String sliceId,
            long writerEpoch,
            int ordinal,
            AppendBatch batch,
            byte[] payload,
            byte[] entryIndexBytes,
            Checksum entryIndexChecksum,
            Checksum sliceChecksum) {
        StreamSliceDescriptor descriptor(long payloadOffset, long entryIndexOffset) {
            return new StreamSliceDescriptor(
                    ordinal,
                    streamId,
                    sliceId,
                    writerEpoch,
                    0,
                    batch.entryCount(),
                    batch.recordCount(),
                    payload.length,
                    payloadOffset,
                    payload.length,
                    entryIndexOffset,
                    entryIndexBytes.length,
                    sliceChecksum,
                    batch.payloadFormat(),
                    batch.minEventTimeMillis(),
                    batch.maxEventTimeMillis(),
                    batch.schemaRefs());
        }
    }

    private record LayoutPlan(
            ObjectId objectId,
            ObjectKey objectKey,
            long objectLength,
            long createdAtMillis,
            long footerOffset,
            int footerLength,
            List<WalObjectLayout.Section> sections,
            List<WrittenStreamSlice> writtenSlices) {
        LayoutPlan {
            sections = List.copyOf(sections);
            writtenSlices = List.copyOf(writtenSlices);
        }
    }
}
