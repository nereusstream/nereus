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

import io.nereus.api.AppendBatch;
import io.nereus.api.AppendEntry;
import io.nereus.api.Checksum;
import io.nereus.api.EntryIndexLocation;
import io.nereus.api.EntryIndexRef;
import io.nereus.api.ErrorCode;
import io.nereus.api.NereusException;
import io.nereus.api.ObjectId;
import io.nereus.api.ObjectKey;
import io.nereus.api.PayloadFormat;
import io.nereus.api.SchemaRef;
import io.nereus.api.StreamId;
import io.nereus.api.keys.DeterministicIds;
import io.nereus.api.keys.KeyComponentCodec;
import io.nereus.objectstore.ObjectStore;
import io.nereus.objectstore.PutObjectOptions;
import io.nereus.objectstore.PutObjectResult;
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
    private static final DateTimeFormatter YEAR =
            DateTimeFormatter.ofPattern("yyyy").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter MONTH =
            DateTimeFormatter.ofPattern("MM").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter DAY =
            DateTimeFormatter.ofPattern("dd").withZone(ZoneOffset.UTC);
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
    public CompletableFuture<WalWriteResult> write(WalWriteRequest request) {
        Objects.requireNonNull(request, "request");
        LayoutPlan plan;
        try {
            plan = plan(request);
        } catch (NereusException e) {
            return CompletableFuture.failedFuture(e);
        } catch (RuntimeException e) {
            return NereusException.failedFuture(ErrorCode.INVALID_ARGUMENT, false, e.getMessage(), e);
        }
        WalObjectLayout.EncodedObject encoded = WalObjectLayout.encodeObject(
                plan.sections(),
                plan.footerOffset(),
                plan.footerLength());
        PutObjectOptions options = new PutObjectOptions(
                CONTENT_TYPE,
                encoded.storageChecksum(),
                true,
                Map.of("objectChecksum", encoded.objectChecksum().value()),
                request.options().uploadTimeout());
        return objectStore.putObject(
                        plan.objectKey(),
                        ByteBuffer.wrap(encoded.bytes()).asReadOnlyBuffer(),
                        options)
                .thenApply(result -> verifyPutResult(plan, encoded, result))
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
        ObjectKey objectKey = objectKey(request.cluster(), writerIdHash, request.writerRunIdHash(), objectId, now);
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
        Checksum entryIndexChecksum = io.nereus.objectstore.Crc32cChecksums.checksum(entryIndexBytes);
        Checksum sliceChecksum = io.nereus.objectstore.Crc32cChecksums.checksum(payloadBytes, entryIndexBytes);
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

    private WalWriteResult verifyPutResult(
            LayoutPlan plan,
            WalObjectLayout.EncodedObject encoded,
            PutObjectResult putResult) {
        if (!putResult.checksum().equals(encoded.storageChecksum())) {
            throw failure(ErrorCode.OBJECT_CHECKSUM_MISMATCH, false, "object store returned mismatched storage checksum");
        }
        if (putResult.objectLength() != plan.objectLength()) {
            throw failure(ErrorCode.OBJECT_CHECKSUM_MISMATCH, false, "object store returned mismatched object length");
        }
        return new WalWriteResult(
                plan.objectId(),
                plan.objectKey(),
                plan.objectLength(),
                encoded.objectChecksum(),
                encoded.storageChecksum(),
                plan.writtenSlices());
    }

    private ObjectKey objectKey(
            String cluster,
            String writerIdHash,
            String writerRunIdHash,
            ObjectId objectId,
            Instant now) {
        String clusterComponent = KeyComponentCodec.encodeComponent(cluster);
        return new ObjectKey(clusterComponent + "/wal/"
                + YEAR.format(now) + "/"
                + MONTH.format(now) + "/"
                + DAY.format(now) + "/"
                + writerIdHash + "/"
                + writerRunIdHash + "/"
                + objectId.value() + ".nrs");
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
