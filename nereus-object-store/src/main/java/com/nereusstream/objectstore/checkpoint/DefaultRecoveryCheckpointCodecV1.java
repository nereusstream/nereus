/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.checkpoint;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.PublicationId;
import com.nereusstream.objectstore.Crc32cChecksums;
import com.nereusstream.objectstore.HeadObjectOptions;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.RangeReadOptions;
import com.nereusstream.objectstore.RangeReadResult;
import com.nereusstream.objectstore.staging.PrivateStagedObjectFile;
import com.nereusstream.objectstore.staging.PrivateStagingSpillFile;
import com.nereusstream.objectstore.staging.StagingFileManager;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Private-staging NRC1 writer and exact object-store range reader. */
public final class DefaultRecoveryCheckpointCodecV1 implements RecoveryCheckpointCodecV1 {
    private static final int PUBLICATION_DIRECTORY_ENTRY_BYTES = Integer.BYTES + Long.BYTES;
    private static final int COMMIT_DIRECTORY_ENTRY_BYTES = Long.BYTES * 3;
    private static final int PUBLICATION_FACT_BYTES = Integer.BYTES + Long.BYTES * 2 + Integer.BYTES;

    private final ObjectStore objectStore;
    private final StagingFileManager stagingFiles;
    private final Executor codecExecutor;
    private final RecoveryCheckpointVerifier verifier;

    public DefaultRecoveryCheckpointCodecV1(
            ObjectStore objectStore,
            StagingFileManager stagingFiles,
            Executor codecExecutor,
            RecoveryCheckpointVerifier verifier) {
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
        this.stagingFiles = Objects.requireNonNull(stagingFiles, "stagingFiles");
        this.codecExecutor = Objects.requireNonNull(codecExecutor, "codecExecutor");
        this.verifier = Objects.requireNonNull(verifier, "verifier");
    }

    @Override
    public CompletableFuture<RecoveryCheckpointWriteResult> write(
            RecoveryCheckpointWriteRequest request,
            Flow.Publisher<RecoveryCheckpointPublication> publications,
            Flow.Publisher<RecoveryCheckpointEntry> entries) {
        CompletableFuture<RecoveryCheckpointWriteResult> result = new CompletableFuture<>();
        if (request == null || publications == null || entries == null) {
            result.completeExceptionally(new NereusException(
                    ErrorCode.INVALID_ARGUMENT,
                    false,
                    "recovery checkpoint request and both publishers are required"));
            return result;
        }
        SerialExecutor serial = new SerialExecutor(codecExecutor);
        AtomicReference<WriteCoordinator> admitted = new AtomicReference<>();
        try {
            serial.execute(() -> {
                if (result.isCancelled()) {
                    return;
                }
                try {
                    WriteCoordinator coordinator = new WriteCoordinator(request, entries, result, serial);
                    admitted.set(coordinator);
                    coordinator.start(publications);
                } catch (Throwable failure) {
                    result.completeExceptionally(mapWriteFailure("initialize recovery checkpoint writer", failure));
                }
            });
        } catch (RejectedExecutionException failure) {
            result.completeExceptionally(new NereusException(
                    ErrorCode.STORAGE_CLOSED,
                    false,
                    "recovery checkpoint executor rejected the operation",
                    failure));
        }
        result.whenComplete((ignored, failure) -> {
            if (result.isCancelled()) {
                WriteCoordinator coordinator = admitted.get();
                if (coordinator != null) {
                    coordinator.cancel();
                }
            }
        });
        return result;
    }

    @Override
    public CompletableFuture<RecoveryCheckpointObject> openAndVerify(
            ObjectKey key,
            long expectedLength,
            Checksum expectedContentSha256,
            Duration timeout) {
        try {
            Objects.requireNonNull(key, "key");
            requireObjectLength(expectedLength);
            RecoveryCheckpointValidation.requireSha256(expectedContentSha256, "expectedContentSha256");
            requireTimeout(timeout);
        } catch (RuntimeException failure) {
            return failedRead("invalid recovery checkpoint open request", failure);
        }
        CheckpointReadDeadline deadline = new CheckpointReadDeadline(timeout);
        long footerOffset = expectedLength - RecoveryCheckpointFormatV1.FOOTER_BYTES;
        CompletableFuture<Void> head = objectStore.headObject(key, new HeadObjectOptions(deadline.remaining()))
                .thenAccept(value -> {
                    if (!value.key().equals(key) || value.objectLength() != expectedLength) {
                        throw corrupt("recovery checkpoint HEAD identity or length mismatch");
                    }
                });
        CompletableFuture<RecoveryCheckpointBinary.Footer> footer = readExact(
                        key,
                        footerOffset,
                        RecoveryCheckpointFormatV1.FOOTER_BYTES,
                        deadline)
                .thenApply(RecoveryCheckpointBinary::decodeFooter)
                .thenApply(value -> {
                    validateFooterBounds(value.directory(), expectedLength);
                    return value;
                });
        return head.thenCombine(footer, (ignored, value) -> value)
                .thenCompose(value -> {
                    long headerReadLength = Math.min(
                            RecoveryCheckpointFormatV1.MAX_HEADER_BYTES,
                            value.directory().publicationDirectoryOffset());
                    CompletableFuture<RecoveryCheckpointBinary.Decoded<RecoveryCheckpointWriteRequest>> header =
                            readExact(key, 0, headerReadLength, deadline)
                                    .thenApply(RecoveryCheckpointBinary::decodeHeader);
                    CompletableFuture<PublicationOffsets> publications = readPublicationDirectory(
                            key, value.directory(), deadline);
                    CompletableFuture<CommitOffsets> commits = readCommitDirectory(
                            key, value.directory(), deadline);
                    CompletableFuture<Void> digests = verifyDigests(
                            key,
                            expectedLength,
                            value.bodySha256(),
                            expectedContentSha256,
                            deadline);
                    return header.thenCombine(publications, HeaderAndPublications::new)
                            .thenCombine(commits, HeaderPublicationsAndCommits::new)
                            .thenCombine(digests, (state, ignored) -> {
                                validateDirectories(
                                        state.header(),
                                        state.publications(),
                                        state.commits(),
                                        value.directory());
                                RecoveryCheckpointWriteRequest request = state.header().value();
                                ObjectKey expectedKey = RecoveryCheckpointFormatV1.objectKey(
                                        request, expectedContentSha256);
                                if (!expectedKey.equals(key)) {
                                    throw corrupt("NRC1 header does not reproduce the exact object key");
                                }
                                return new RecoveryCheckpointObject(
                                        request,
                                        RecoveryCheckpointFormatV1.objectId(key),
                                        key,
                                        expectedLength,
                                        value.bodySha256(),
                                        expectedContentSha256,
                                        value.directory());
                            });
                })
                .handle((value, failure) -> {
                    if (failure != null) {
                        throw new CompletionException(mapReadFailure("open and verify recovery checkpoint", failure));
                    }
                    return value;
                });
    }

    @Override
    public CompletableFuture<Optional<RecoveryCheckpointPublication>> findPublication(
            RecoveryCheckpointObject object,
            long generation,
            PublicationId publicationId,
            Duration timeout) {
        try {
            Objects.requireNonNull(object, "object");
            if (generation <= 0) {
                throw new IllegalArgumentException("generation must be positive");
            }
            Objects.requireNonNull(publicationId, "publicationId");
            requireTimeout(timeout);
        } catch (RuntimeException failure) {
            return failedRead("invalid recovery checkpoint publication lookup", failure);
        }
        CheckpointReadDeadline deadline = new CheckpointReadDeadline(timeout);
        CompletableFuture<PublicationOffsets> publications = readPublicationDirectory(
                object.objectKey(), object.directory(), deadline);
        CompletableFuture<CommitOffsets> commits = readCommitDirectory(
                object.objectKey(), object.directory(), deadline);
        return publications.thenCombine(commits, DirectoryState::new)
                .thenCompose(state -> findPublication(
                        object,
                        state.publications(),
                        state.commits().fileOffsets()[0],
                        generation,
                        publicationId,
                        0,
                        state.publications().fileOffsets().length - 1,
                        deadline))
                .handle((value, failure) -> {
                    if (failure != null) {
                        throw new CompletionException(mapReadFailure("find recovery checkpoint publication", failure));
                    }
                    return value;
                });
    }

    @Override
    public CompletableFuture<Optional<RecoveryCheckpointEntry>> findCommit(
            RecoveryCheckpointObject object,
            long commitVersion,
            String commitId,
            Duration timeout) {
        try {
            Objects.requireNonNull(object, "object");
            RecoveryCheckpointValidation.requireText(commitId, "commitId");
            requireTimeout(timeout);
        } catch (RuntimeException failure) {
            return failedRead("invalid recovery checkpoint commit lookup", failure);
        }
        RecoveryCheckpointWriteRequest header = object.header();
        if (commitVersion < header.firstCommitVersion() || commitVersion > header.lastCommitVersion()) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        CheckpointReadDeadline deadline = new CheckpointReadDeadline(timeout);
        CompletableFuture<PublicationOffsets> publications = readPublicationDirectory(
                object.objectKey(), object.directory(), deadline);
        CompletableFuture<CommitOffsets> commits = readCommitDirectory(
                object.objectKey(), object.directory(), deadline);
        return publications.thenCombine(commits, DirectoryState::new)
                .thenCompose(state -> {
                    int block = Math.toIntExact(
                            (commitVersion - header.firstCommitVersion())
                                    / RecoveryCheckpointFormatV1.COMMIT_DIRECTORY_STRIDE);
                    long start = state.commits().fileOffsets()[block];
                    long end = block + 1 < state.commits().fileOffsets().length
                            ? state.commits().fileOffsets()[block + 1]
                            : object.directory().publicationDirectoryOffset();
                    long length = end - start;
                    long maximumBlock = (long) RecoveryCheckpointFormatV1.COMMIT_DIRECTORY_STRIDE
                            * RecoveryCheckpointFormatV1.MAX_RECORD_BYTES;
                    if (length <= 0 || length > maximumBlock) {
                        throw corrupt("NRC1 commit directory points to an invalid block length");
                    }
                    return readExact(object.objectKey(), start, length, deadline)
                            .thenCompose(bytes -> findCommitInBlock(
                                    object,
                                    state.publications(),
                                    state.commits().fileOffsets()[0],
                                    bytes,
                                    commitVersion,
                                    commitId,
                                    deadline));
                })
                .handle((value, failure) -> {
                    if (failure != null) {
                        throw new CompletionException(mapReadFailure("find recovery checkpoint commit", failure));
                    }
                    return value;
                });
    }

    private CompletableFuture<Optional<RecoveryCheckpointPublication>> findPublication(
            RecoveryCheckpointObject object,
            PublicationOffsets offsets,
            long publicationRecordsEnd,
            long generation,
            PublicationId publicationId,
            int low,
            int high,
            CheckpointReadDeadline deadline) {
        if (low > high) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        int midpoint = low + ((high - low) >>> 1);
        return readPublicationAt(object, offsets, publicationRecordsEnd, midpoint, deadline)
                .thenCompose(value -> {
                    int comparison = comparePublication(value, generation, publicationId);
                    if (comparison == 0) {
                        return CompletableFuture.completedFuture(Optional.of(value));
                    }
                    return comparison < 0
                            ? findPublication(
                                    object,
                                    offsets,
                                    publicationRecordsEnd,
                                    generation,
                                    publicationId,
                                    midpoint + 1,
                                    high,
                                    deadline)
                            : findPublication(
                                    object,
                                    offsets,
                                    publicationRecordsEnd,
                                    generation,
                                    publicationId,
                                    low,
                                    midpoint - 1,
                                    deadline);
                });
    }

    private CompletableFuture<RecoveryCheckpointPublication> readPublicationAt(
            RecoveryCheckpointObject object,
            PublicationOffsets offsets,
            long publicationRecordsEnd,
            int index,
            CheckpointReadDeadline deadline) {
        long start = offsets.fileOffsets()[index];
        long end = index + 1 < offsets.fileOffsets().length
                ? offsets.fileOffsets()[index + 1]
                : publicationRecordsEnd;
        long length = end - start;
        if (length <= 0 || length > RecoveryCheckpointFormatV1.MAX_RECORD_BYTES) {
            return CompletableFuture.failedFuture(corrupt(
                    "NRC1 publication directory points to an invalid record length"));
        }
        return readExact(object.objectKey(), start, length, deadline).thenApply(bytes -> {
            RecoveryCheckpointBinary.Decoded<RecoveryCheckpointPublication> decoded =
                    RecoveryCheckpointBinary.decodePublication(bytes);
            if (decoded.bytesConsumed() != length) {
                throw corrupt("NRC1 publication directory does not delimit one exact record");
            }
            validatePublication(object.header(), decoded.value());
            return decoded.value();
        });
    }

    private CompletableFuture<Optional<RecoveryCheckpointEntry>> findCommitInBlock(
            RecoveryCheckpointObject object,
            PublicationOffsets publicationOffsets,
            long publicationRecordsEnd,
            ByteBuffer bytes,
            long commitVersion,
            String commitId,
            CheckpointReadDeadline deadline) {
        ByteBuffer cursor = bytes.asReadOnlyBuffer();
        while (cursor.hasRemaining()) {
            RecoveryCheckpointBinary.Decoded<RecoveryCheckpointEntry> decoded =
                    RecoveryCheckpointBinary.decodeEntry(cursor);
            RecoveryCheckpointEntry entry = decoded.value();
            cursor.position(cursor.position() + decoded.bytesConsumed());
            validateEntryBounds(object.header(), entry);
            verifier.verifyEntry(object.header(), entry);
            if (entry.commitVersion() == commitVersion) {
                if (!entry.commitId().equals(commitId)) {
                    return CompletableFuture.completedFuture(Optional.empty());
                }
                return verifyPublicationReferences(
                                object,
                                publicationOffsets,
                                publicationRecordsEnd,
                                entry,
                                deadline)
                        .thenApply(ignored -> Optional.of(entry));
            }
            if (entry.commitVersion() > commitVersion) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
        }
        return CompletableFuture.completedFuture(Optional.empty());
    }

    private CompletableFuture<Void> verifyPublicationReferences(
            RecoveryCheckpointObject object,
            PublicationOffsets publicationOffsets,
            long publicationRecordsEnd,
            RecoveryCheckpointEntry entry,
            CheckpointReadDeadline deadline) {
        if (entry.coveringPublicationIndexes().isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        List<CompletableFuture<RecoveryCheckpointPublication>> reads = new ArrayList<>();
        for (int index : entry.coveringPublicationIndexes()) {
            if (index >= publicationOffsets.fileOffsets().length) {
                return CompletableFuture.failedFuture(corrupt(
                        "NRC1 commit entry references a publication outside the table"));
            }
            reads.add(readPublicationAt(object, publicationOffsets, publicationRecordsEnd, index, deadline));
        }
        return CompletableFuture.allOf(reads.toArray(CompletableFuture[]::new)).thenRun(() -> {
            List<Coverage> coverages = reads.stream()
                    .map(CompletableFuture::join)
                    .map(value -> new Coverage(value.coverage().startOffset(), value.coverage().endOffset()))
                    .toList();
            requireCovered(entry, coverages);
        });
    }

    private CompletableFuture<PublicationOffsets> readPublicationDirectory(
            ObjectKey key,
            RecoveryCheckpointDirectory directory,
            CheckpointReadDeadline deadline) {
        return readExact(
                        key,
                        directory.publicationDirectoryOffset(),
                        directory.publicationDirectoryLength(),
                        deadline)
                .thenApply(DefaultRecoveryCheckpointCodecV1::decodePublicationDirectory);
    }

    private CompletableFuture<CommitOffsets> readCommitDirectory(
            ObjectKey key,
            RecoveryCheckpointDirectory directory,
            CheckpointReadDeadline deadline) {
        return readExact(
                        key,
                        directory.commitDirectoryOffset(),
                        directory.commitDirectoryLength(),
                        deadline)
                .thenApply(DefaultRecoveryCheckpointCodecV1::decodeCommitDirectory);
    }

    private CompletableFuture<Void> verifyDigests(
            ObjectKey key,
            long objectLength,
            Checksum expectedBodySha256,
            Checksum expectedContentSha256,
            CheckpointReadDeadline deadline) {
        MessageDigest body = RecoveryCheckpointFormatV1.newSha256();
        MessageDigest content = RecoveryCheckpointFormatV1.newSha256();
        long bodyLength = objectLength - RecoveryCheckpointFormatV1.FOOTER_BYTES;
        return hashNextRange(key, 0, objectLength, bodyLength, body, content, deadline)
                .thenRun(() -> {
                    Checksum actualBody = checksum(body.digest());
                    Checksum actualContent = checksum(content.digest());
                    if (!actualBody.equals(expectedBodySha256)
                            || !actualContent.equals(expectedContentSha256)) {
                        throw corrupt("NRC1 body or complete-object SHA256 mismatch");
                    }
                });
    }

    private CompletableFuture<Void> hashNextRange(
            ObjectKey key,
            long offset,
            long objectLength,
            long bodyLength,
            MessageDigest body,
            MessageDigest content,
            CheckpointReadDeadline deadline) {
        if (offset == objectLength) {
            return CompletableFuture.completedFuture(null);
        }
        long remaining = objectLength - offset;
        long length = Math.min(RecoveryCheckpointFormatV1.HASH_READ_CHUNK_BYTES, remaining);
        if (offset < bodyLength) {
            length = Math.min(length, bodyLength - offset);
        }
        long next = offset + length;
        return readExact(key, offset, length, deadline).thenCompose(bytes -> {
            ByteBuffer contentBytes = bytes.asReadOnlyBuffer();
            content.update(contentBytes);
            if (offset < bodyLength) {
                body.update(bytes.asReadOnlyBuffer());
            }
            return hashNextRange(key, next, objectLength, bodyLength, body, content, deadline);
        });
    }

    private CompletableFuture<ByteBuffer> readExact(
            ObjectKey key,
            long offset,
            long length,
            CheckpointReadDeadline deadline) {
        if (length < 0 || length > Integer.MAX_VALUE) {
            return CompletableFuture.failedFuture(corrupt("NRC1 range read exceeds the bounded buffer limit"));
        }
        return objectStore.readRange(
                        key,
                        offset,
                        length,
                        new RangeReadOptions(Optional.empty(), deadline.remaining()))
                .thenApply(result -> validateRange(result, key, offset, length));
    }

    private static ByteBuffer validateRange(
            RangeReadResult result,
            ObjectKey key,
            long offset,
            long length) {
        if (!result.key().equals(key)
                || result.offset() != offset
                || result.length() != length
                || result.payload().remaining() != length) {
            throw corrupt("object store returned a mismatched NRC1 range");
        }
        return result.payload().asReadOnlyBuffer();
    }

    private static PublicationOffsets decodePublicationDirectory(ByteBuffer bytes) {
        ByteBuffer reader = bytes.asReadOnlyBuffer().order(ByteOrder.BIG_ENDIAN);
        requireRemaining(reader, Integer.BYTES, "publication directory count");
        int count = reader.getInt();
        if (count <= 0 || count > RecoveryCheckpointFormatV1.MAX_PUBLICATION_COUNT) {
            throw corrupt("NRC1 publication directory count is outside its hard limit");
        }
        long expectedLength = Integer.BYTES + (long) count * PUBLICATION_DIRECTORY_ENTRY_BYTES;
        if (expectedLength != bytes.remaining()) {
            throw corrupt("NRC1 publication directory length does not match its count");
        }
        long[] offsets = new long[count];
        long previous = -1;
        for (int expectedIndex = 0; expectedIndex < count; expectedIndex++) {
            int index = reader.getInt();
            long offset = reader.getLong();
            if (index != expectedIndex || offset <= previous) {
                throw corrupt("NRC1 publication directory is not canonical and strictly ordered");
            }
            offsets[index] = offset;
            previous = offset;
        }
        if (reader.hasRemaining()) {
            throw corrupt("NRC1 publication directory has trailing bytes");
        }
        return new PublicationOffsets(offsets);
    }

    private static CommitOffsets decodeCommitDirectory(ByteBuffer bytes) {
        ByteBuffer reader = bytes.asReadOnlyBuffer().order(ByteOrder.BIG_ENDIAN);
        requireRemaining(reader, Integer.BYTES * 2, "commit directory header");
        int stride = reader.getInt();
        int count = reader.getInt();
        int maximumCount = (RecoveryCheckpointFormatV1.MAX_ENTRY_COUNT
                        + RecoveryCheckpointFormatV1.COMMIT_DIRECTORY_STRIDE - 1)
                / RecoveryCheckpointFormatV1.COMMIT_DIRECTORY_STRIDE;
        if (stride != RecoveryCheckpointFormatV1.COMMIT_DIRECTORY_STRIDE
                || count <= 0
                || count > maximumCount) {
            throw corrupt("NRC1 commit directory stride or count is invalid");
        }
        long expectedLength = Integer.BYTES * 2L + (long) count * COMMIT_DIRECTORY_ENTRY_BYTES;
        if (expectedLength != bytes.remaining()) {
            throw corrupt("NRC1 commit directory length does not match its count");
        }
        long[] versions = new long[count];
        long[] offsetStarts = new long[count];
        long[] fileOffsets = new long[count];
        long previousVersion = -1;
        long previousOffsetStart = -1;
        long previousFileOffset = -1;
        for (int index = 0; index < count; index++) {
            long version = reader.getLong();
            long offsetStart = reader.getLong();
            long fileOffset = reader.getLong();
            if (version <= previousVersion
                    || offsetStart <= previousOffsetStart
                    || fileOffset <= previousFileOffset) {
                throw corrupt("NRC1 commit directory is not strictly ordered");
            }
            versions[index] = version;
            offsetStarts[index] = offsetStart;
            fileOffsets[index] = fileOffset;
            previousVersion = version;
            previousOffsetStart = offsetStart;
            previousFileOffset = fileOffset;
        }
        if (reader.hasRemaining()) {
            throw corrupt("NRC1 commit directory has trailing bytes");
        }
        return new CommitOffsets(versions, offsetStarts, fileOffsets);
    }

    private static void validateDirectories(
            RecoveryCheckpointBinary.Decoded<RecoveryCheckpointWriteRequest> header,
            PublicationOffsets publications,
            CommitOffsets commits,
            RecoveryCheckpointDirectory directory) {
        RecoveryCheckpointWriteRequest request = header.value();
        if (publications.fileOffsets().length != request.expectedPublicationCount()) {
            throw corrupt("NRC1 publication directory count differs from the header");
        }
        int expectedCommitDirectoryCount = Math.toIntExact(
                (request.expectedEntryCount() + (long) RecoveryCheckpointFormatV1.COMMIT_DIRECTORY_STRIDE - 1)
                        / RecoveryCheckpointFormatV1.COMMIT_DIRECTORY_STRIDE);
        if (commits.fileOffsets().length != expectedCommitDirectoryCount) {
            throw corrupt("NRC1 commit directory count differs from the header");
        }
        if (publications.fileOffsets()[0] != header.bytesConsumed()
                || publications.fileOffsets()[publications.fileOffsets().length - 1]
                        >= commits.fileOffsets()[0]
                || commits.fileOffsets()[0] <= header.bytesConsumed()
                || commits.fileOffsets()[commits.fileOffsets().length - 1]
                        >= directory.publicationDirectoryOffset()
                || commits.versions()[0] != request.firstCommitVersion()
                || commits.offsetStarts()[0] != request.coverage().startOffset()) {
            throw corrupt("NRC1 directory record bounds do not match header/body sections");
        }
        for (int index = 0; index < commits.versions().length; index++) {
            long expectedVersion = request.firstCommitVersion()
                    + (long) index * RecoveryCheckpointFormatV1.COMMIT_DIRECTORY_STRIDE;
            if (commits.versions()[index] != expectedVersion) {
                throw corrupt("NRC1 sparse commit directory version is non-canonical");
            }
        }
    }

    private static void validateFooterBounds(
            RecoveryCheckpointDirectory directory,
            long objectLength) {
        long footerOffset = objectLength - RecoveryCheckpointFormatV1.FOOTER_BYTES;
        if (directory.commitDirectoryOffset() + directory.commitDirectoryLength() != footerOffset
                || directory.publicationDirectoryOffset() >= footerOffset) {
            throw corrupt("NRC1 footer directory ranges are outside the object body");
        }
    }

    private void validatePublication(
            RecoveryCheckpointWriteRequest header,
            RecoveryCheckpointPublication publication) {
        if (publication.coverage().startOffset() < header.coverage().startOffset()
                || publication.coverage().endOffset() > header.coverage().endOffset()) {
            throw corrupt("NRC1 publication coverage is outside checkpoint coverage");
        }
        verifier.verifyPublication(header, publication);
    }

    private static void validateEntryBounds(
            RecoveryCheckpointWriteRequest header,
            RecoveryCheckpointEntry entry) {
        if (entry.commitVersion() < header.firstCommitVersion()
                || entry.commitVersion() > header.lastCommitVersion()
                || entry.range().startOffset() < header.coverage().startOffset()
                || entry.range().endOffset() > header.coverage().endOffset()) {
            throw corrupt("NRC1 commit entry is outside checkpoint header coverage");
        }
        for (int index : entry.coveringPublicationIndexes()) {
            if (index >= header.expectedPublicationCount()) {
                throw corrupt("NRC1 commit entry references a publication outside the table");
            }
        }
    }

    private static int comparePublication(
            RecoveryCheckpointPublication actual,
            long generation,
            PublicationId publicationId) {
        int generationComparison = Long.compare(actual.generation(), generation);
        return generationComparison != 0
                ? generationComparison
                : actual.publicationId().value().compareTo(publicationId.value());
    }

    private static void requireObjectLength(long value) {
        if (value <= RecoveryCheckpointFormatV1.FOOTER_BYTES
                || value > RecoveryCheckpointFormatV1.MAX_OBJECT_BYTES) {
            throw new IllegalArgumentException("expectedLength is outside the NRC1 object limit");
        }
    }

    private static void requireTimeout(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
    }

    private static void requireRemaining(ByteBuffer value, int bytes, String field) {
        if (value.remaining() < bytes) {
            throw corrupt("NRC1 " + field + " is truncated");
        }
    }

    private static Checksum checksum(byte[] digest) {
        return new Checksum(ChecksumType.SHA256, java.util.HexFormat.of().formatHex(digest));
    }

    private static RecoveryCheckpointFormatException corrupt(String message) {
        return new RecoveryCheckpointFormatException(message);
    }

    private static <T> CompletableFuture<T> failedRead(String action, RuntimeException failure) {
        return CompletableFuture.failedFuture(mapReadFailure(action, failure));
    }

    private static Throwable mapWriteFailure(String action, Throwable failure) {
        Throwable current = unwrap(failure);
        if (current instanceof NereusException) {
            return current;
        }
        if (current instanceof IllegalArgumentException || current instanceof NullPointerException) {
            return new NereusException(ErrorCode.INVALID_ARGUMENT, false, action + " failed", current);
        }
        return new NereusException(ErrorCode.OBJECT_UPLOAD_FAILED, true, action + " failed", current);
    }

    private static RuntimeException mapReadFailure(String action, Throwable failure) {
        Throwable current = unwrap(failure);
        if (current instanceof NereusException nereus) {
            return nereus;
        }
        if (current instanceof IllegalArgumentException || current instanceof NullPointerException) {
            return new NereusException(ErrorCode.INVALID_ARGUMENT, false, action + " failed", current);
        }
        return new NereusException(ErrorCode.OBJECT_READ_FAILED, true, action + " failed", current);
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = Objects.requireNonNull(failure, "failure");
        while ((current instanceof CompletionException
                        || current instanceof java.util.concurrent.ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private final class WriteCoordinator {
        private final RecoveryCheckpointWriteRequest request;
        private final Flow.Publisher<RecoveryCheckpointEntry> entries;
        private final CompletableFuture<RecoveryCheckpointWriteResult> result;
        private final SerialExecutor serial;
        private final CheckpointSink sink;
        private final AtomicBoolean terminal = new AtomicBoolean();
        private volatile PhaseSubscriber<?> active;

        private WriteCoordinator(
                RecoveryCheckpointWriteRequest request,
                Flow.Publisher<RecoveryCheckpointEntry> entries,
                CompletableFuture<RecoveryCheckpointWriteResult> result,
                SerialExecutor serial) throws IOException {
            this.request = request;
            this.entries = entries;
            this.result = result;
            this.serial = serial;
            this.sink = new CheckpointSink(request);
        }

        private void start(Flow.Publisher<RecoveryCheckpointPublication> publications) {
            PhaseSubscriber<RecoveryCheckpointPublication> subscriber = new PhaseSubscriber<>(
                    this,
                    sink::writePublication,
                    this::publicationsComplete);
            active = subscriber;
            publications.subscribe(subscriber);
        }

        private void publicationsComplete() {
            sink.finishPublications();
            PhaseSubscriber<RecoveryCheckpointEntry> subscriber = new PhaseSubscriber<>(
                    this,
                    sink::writeEntry,
                    this::entriesComplete);
            active = subscriber;
            entries.subscribe(subscriber);
        }

        private void entriesComplete() {
            if (!terminal.compareAndSet(false, true)) {
                return;
            }
            try {
                RecoveryCheckpointWriteResult completed = sink.finish();
                if (!result.complete(completed)) {
                    completed.close();
                }
            } catch (Throwable failure) {
                sink.close();
                result.completeExceptionally(mapWriteFailure("finish recovery checkpoint object", failure));
            }
        }

        private void submit(Runnable action) {
            try {
                serial.execute(action);
            } catch (RejectedExecutionException failure) {
                fail(failure);
            }
        }

        private void fail(Throwable failure) {
            if (!terminal.compareAndSet(false, true)) {
                return;
            }
            PhaseSubscriber<?> subscriber = active;
            if (subscriber != null) {
                subscriber.cancelSubscription();
            }
            sink.close();
            result.completeExceptionally(mapWriteFailure("write recovery checkpoint object", failure));
        }

        private void cancel() {
            submit(() -> {
                if (terminal.compareAndSet(false, true)) {
                    PhaseSubscriber<?> subscriber = active;
                    if (subscriber != null) {
                        subscriber.cancelSubscription();
                    }
                    sink.close();
                }
            });
        }
    }

    private static final class PhaseSubscriber<T> implements Flow.Subscriber<T> {
        private final WriteCoordinator coordinator;
        private final ThrowingConsumer<T> consumer;
        private final ThrowingRunnable completion;
        private final AtomicBoolean awaitingItem = new AtomicBoolean();
        private final AtomicBoolean terminated = new AtomicBoolean();
        private volatile Flow.Subscription subscription;

        private PhaseSubscriber(
                WriteCoordinator coordinator,
                ThrowingConsumer<T> consumer,
                ThrowingRunnable completion) {
            this.coordinator = coordinator;
            this.consumer = consumer;
            this.completion = completion;
        }

        @Override
        public void onSubscribe(Flow.Subscription value) {
            Objects.requireNonNull(value, "subscription");
            if (subscription != null || terminated.get()) {
                value.cancel();
                coordinator.fail(new IllegalStateException("NRC1 publisher subscribed more than once"));
                return;
            }
            subscription = value;
            requestOne();
        }

        @Override
        public void onNext(T value) {
            if (!awaitingItem.compareAndSet(true, false)) {
                coordinator.fail(new IllegalStateException("NRC1 publisher emitted without demand"));
                return;
            }
            coordinator.submit(() -> {
                try {
                    consumer.accept(Objects.requireNonNull(value, "publisher value"));
                    requestOne();
                } catch (Throwable failure) {
                    coordinator.fail(failure);
                }
            });
        }

        @Override
        public void onError(Throwable failure) {
            if (terminated.compareAndSet(false, true)) {
                coordinator.submit(() -> coordinator.fail(Objects.requireNonNull(failure, "failure")));
            }
        }

        @Override
        public void onComplete() {
            if (terminated.compareAndSet(false, true)) {
                coordinator.submit(() -> {
                    try {
                        completion.run();
                    } catch (Throwable failure) {
                        coordinator.fail(failure);
                    }
                });
            }
        }

        private void requestOne() {
            coordinator.submit(() -> {
                if (terminated.get()) {
                    return;
                }
                Flow.Subscription value = subscription;
                if (value == null) {
                    coordinator.fail(new IllegalStateException("NRC1 publisher omitted its subscription"));
                    return;
                }
                awaitingItem.set(true);
                try {
                    value.request(1);
                } catch (Throwable failure) {
                    awaitingItem.set(false);
                    coordinator.fail(failure);
                }
            });
        }

        private void cancelSubscription() {
            terminated.set(true);
            Flow.Subscription value = subscription;
            if (value != null) {
                value.cancel();
            }
        }
    }

    private final class CheckpointSink implements AutoCloseable {
        private final RecoveryCheckpointWriteRequest request;
        private final PrivateStagedObjectFile stagingFile;
        private final PrivateStagingSpillFile publicationDirectorySpill;
        private final PrivateStagingSpillFile commitDirectorySpill;
        private final PrivateStagingSpillFile publicationFactsSpill;
        private final OutputStream stagingOutput;
        private final DataOutputStream publicationDirectoryOutput;
        private final DataOutputStream commitDirectoryOutput;
        private final OutputStream publicationFactsOutput;
        private final MessageDigest bodySha256 = RecoveryCheckpointFormatV1.newSha256();
        private PrivateStagingSpillFile.RandomAccessReader publicationFactsReader;
        private long position;
        private int publicationCount;
        private int entryCount;
        private int commitDirectoryCount;
        private long previousGeneration = -1;
        private String previousPublicationId = "";
        private long previousCommitVersion = -1;
        private long previousRangeEnd = -1;
        private long previousCumulativeEnd = -1;
        private String previousCommitId = "";
        private boolean publicationsFinished;
        private boolean transferred;
        private boolean closed;

        private CheckpointSink(RecoveryCheckpointWriteRequest request) throws IOException {
            this.request = request;
            this.stagingFile = stagingFiles.create("checkpoint");
            PrivateStagingSpillFile publicationDirectory = null;
            PrivateStagingSpillFile commitDirectory = null;
            PrivateStagingSpillFile publicationFacts = null;
            try {
                publicationDirectory = stagingFiles.createSpill("checkpoint-pubdir");
                commitDirectory = stagingFiles.createSpill("checkpoint-commitdir");
                publicationFacts = stagingFiles.createSpill("checkpoint-pubfacts");
                this.publicationDirectorySpill = publicationDirectory;
                this.commitDirectorySpill = commitDirectory;
                this.publicationFactsSpill = publicationFacts;
                this.stagingOutput = stagingFile.outputStream();
                this.publicationDirectoryOutput = new DataOutputStream(publicationDirectory.outputStream());
                this.commitDirectoryOutput = new DataOutputStream(commitDirectory.outputStream());
                this.publicationFactsOutput = publicationFacts.outputStream();
                writeBody(RecoveryCheckpointBinary.encodeHeader(request));
            } catch (Throwable failure) {
                stagingFile.close();
                if (publicationDirectory != null) {
                    publicationDirectory.close();
                }
                if (commitDirectory != null) {
                    commitDirectory.close();
                }
                if (publicationFacts != null) {
                    publicationFacts.close();
                }
                throw failure;
            }
        }

        private void writePublication(RecoveryCheckpointPublication publication) throws IOException {
            if (publicationsFinished || publicationCount >= request.expectedPublicationCount()) {
                throw new RecoveryCheckpointFormatException("NRC1 publication publisher exceeded its declared count");
            }
            validatePublication(request, publication);
            if (publicationCount > 0
                    && (publication.generation() < previousGeneration
                            || (publication.generation() == previousGeneration
                                    && publication.publicationId().value().compareTo(previousPublicationId) <= 0))) {
                throw new RecoveryCheckpointFormatException(
                        "NRC1 publications must be unique and sorted by generation/publication id");
            }
            long recordOffset = position;
            byte[] encoded = RecoveryCheckpointBinary.encodePublication(publication);
            writeBody(encoded);
            publicationDirectoryOutput.writeInt(publicationCount);
            publicationDirectoryOutput.writeLong(recordOffset);
            writePublicationFact(publicationCount, publication);
            previousGeneration = publication.generation();
            previousPublicationId = publication.publicationId().value();
            publicationCount++;
        }

        private void finishPublications() {
            if (publicationsFinished) {
                throw new IllegalStateException("NRC1 publication phase was already completed");
            }
            if (publicationCount != request.expectedPublicationCount()) {
                throw new RecoveryCheckpointFormatException(
                        "NRC1 publication publisher completed before its declared count");
            }
            try {
                publicationDirectoryOutput.close();
                publicationFactsOutput.close();
                publicationDirectorySpill.seal();
                publicationFactsSpill.seal();
                publicationFactsReader = publicationFactsSpill.openRandomAccessReader();
                publicationsFinished = true;
            } catch (IOException failure) {
                throw new NereusException(
                        ErrorCode.OBJECT_UPLOAD_FAILED,
                        true,
                        "failed to seal NRC1 publication runs",
                        failure);
            }
        }

        private void writeEntry(RecoveryCheckpointEntry entry) throws IOException {
            if (!publicationsFinished || entryCount >= request.expectedEntryCount()) {
                throw new RecoveryCheckpointFormatException("NRC1 commit publisher exceeded its declared count");
            }
            validateEntryBounds(request, entry);
            verifier.verifyEntry(request, entry);
            if (entryCount == 0) {
                if (entry.commitVersion() != request.firstCommitVersion()
                        || entry.range().startOffset() != request.coverage().startOffset()
                        || !entry.commitId().equals(request.firstCommitId())
                        || entry.cumulativeSizeAtEnd() < request.cumulativeSizeAtStart()) {
                    throw new RecoveryCheckpointFormatException(
                            "first NRC1 commit entry does not bridge the header start");
                }
            } else if (entry.commitVersion() != previousCommitVersion + 1
                    || entry.range().startOffset() != previousRangeEnd
                    || !entry.previousCommitId().equals(previousCommitId)
                    || entry.cumulativeSizeAtEnd() < previousCumulativeEnd) {
                throw new RecoveryCheckpointFormatException(
                        "NRC1 commit entries are not version/range/predecessor/size contiguous");
            }
            List<Coverage> publicationCoverages = new ArrayList<>();
            for (int index : entry.coveringPublicationIndexes()) {
                if (index >= publicationCount) {
                    throw new RecoveryCheckpointFormatException(
                            "NRC1 commit entry references a publication outside the table");
                }
                PublicationFact fact = readPublicationFact(index);
                publicationCoverages.add(new Coverage(fact.startOffset(), fact.endOffset()));
            }
            if (!publicationCoverages.isEmpty()) {
                requireCovered(entry, publicationCoverages);
            }
            if (entryCount % RecoveryCheckpointFormatV1.COMMIT_DIRECTORY_STRIDE == 0) {
                commitDirectoryOutput.writeLong(entry.commitVersion());
                commitDirectoryOutput.writeLong(entry.range().startOffset());
                commitDirectoryOutput.writeLong(position);
                commitDirectoryCount++;
            }
            writeBody(RecoveryCheckpointBinary.encodeEntry(entry));
            previousCommitVersion = entry.commitVersion();
            previousRangeEnd = entry.range().endOffset();
            previousCumulativeEnd = entry.cumulativeSizeAtEnd();
            previousCommitId = entry.commitId();
            entryCount++;
        }

        private RecoveryCheckpointWriteResult finish() throws IOException {
            if (!publicationsFinished
                    || entryCount != request.expectedEntryCount()
                    || previousCommitVersion != request.lastCommitVersion()
                    || previousRangeEnd != request.coverage().endOffset()
                    || previousCumulativeEnd != request.cumulativeSizeAtEnd()
                    || !previousCommitId.equals(request.lastCommitId())) {
                throw new RecoveryCheckpointFormatException(
                        "NRC1 commit publisher did not exactly close the declared header coverage");
            }
            commitDirectoryOutput.close();
            commitDirectorySpill.seal();
            long publicationDirectoryLength = Math.addExact(
                    Integer.BYTES, publicationDirectorySpill.sealedLength());
            long commitDirectoryLength = Math.addExact(
                    Integer.BYTES * 2L, commitDirectorySpill.sealedLength());
            if (publicationDirectoryLength + commitDirectoryLength
                    > RecoveryCheckpointFormatV1.MAX_DIRECTORY_BYTES) {
                throw new RecoveryCheckpointFormatException("NRC1 combined directories exceed their hard limit");
            }
            long publicationDirectoryOffset = position;
            writeBody(int32(publicationCount));
            copySpill(publicationDirectorySpill);
            long commitDirectoryOffset = position;
            writeBody(int32(RecoveryCheckpointFormatV1.COMMIT_DIRECTORY_STRIDE));
            writeBody(int32(commitDirectoryCount));
            copySpill(commitDirectorySpill);
            RecoveryCheckpointDirectory directory = new RecoveryCheckpointDirectory(
                    publicationDirectoryOffset,
                    publicationDirectoryLength,
                    commitDirectoryOffset,
                    commitDirectoryLength,
                    RecoveryCheckpointFormatV1.COMMIT_DIRECTORY_STRIDE);
            Checksum bodyChecksum = checksum(bodySha256.digest());
            byte[] footer = RecoveryCheckpointBinary.encodeFooter(directory, bodyChecksum);
            if (position > RecoveryCheckpointFormatV1.MAX_OBJECT_BYTES - footer.length) {
                throw new RecoveryCheckpointFormatException("NRC1 object exceeds its 1 GiB hard limit");
            }
            stagingOutput.write(footer);
            position = Math.addExact(position, footer.length);
            stagingOutput.close();
            PrivateStagedObjectFile sealed = stagingFile.seal();
            if (sealed.sealedLength() != position) {
                throw new IllegalStateException("sealed NRC1 length differs from the writer position");
            }
            Checksum contentSha256 = sealed.contentSha256();
            ObjectKey key = RecoveryCheckpointFormatV1.objectKey(request, contentSha256);
            RecoveryCheckpointWriteResult result;
            try {
                result = new RecoveryCheckpointWriteResult(
                        sealed,
                        RecoveryCheckpointFormatV1.objectId(key),
                        key,
                        RecoveryCheckpointFormatV1.objectKeyHash(key),
                        position,
                        sealed.storageCrc32c(),
                        bodyChecksum,
                        contentSha256,
                        directory);
            } catch (Throwable failure) {
                sealed.close();
                throw failure;
            }
            transferred = true;
            closeTemporaryFiles();
            return result;
        }

        private void writePublicationFact(
                int index,
                RecoveryCheckpointPublication publication) throws IOException {
            ByteBuffer body = ByteBuffer.allocate(PUBLICATION_FACT_BYTES - Integer.BYTES)
                    .order(ByteOrder.BIG_ENDIAN)
                    .putInt(index)
                    .putLong(publication.coverage().startOffset())
                    .putLong(publication.coverage().endOffset());
            byte[] protectedBytes = body.array();
            ByteBuffer fact = ByteBuffer.allocate(PUBLICATION_FACT_BYTES)
                    .order(ByteOrder.BIG_ENDIAN)
                    .put(protectedBytes)
                    .putInt(Crc32cChecksums.intValue(Crc32cChecksums.checksum(protectedBytes)));
            publicationFactsOutput.write(fact.array());
        }

        private PublicationFact readPublicationFact(int expectedIndex) {
            long offset = (long) expectedIndex * PUBLICATION_FACT_BYTES;
            ByteBuffer value = publicationFactsReader.readRange(offset, PUBLICATION_FACT_BYTES)
                    .order(ByteOrder.BIG_ENDIAN);
            byte[] protectedBytes = new byte[PUBLICATION_FACT_BYTES - Integer.BYTES];
            value.get(protectedBytes);
            int expectedCrc = value.getInt();
            if (Crc32cChecksums.intValue(Crc32cChecksums.checksum(protectedBytes)) != expectedCrc) {
                throw new RecoveryCheckpointFormatException("NRC1 publication fact spill CRC32C mismatch");
            }
            ByteBuffer facts = ByteBuffer.wrap(protectedBytes).order(ByteOrder.BIG_ENDIAN);
            int actualIndex = facts.getInt();
            long start = facts.getLong();
            long end = facts.getLong();
            if (actualIndex != expectedIndex || start < 0 || end <= start) {
                throw new RecoveryCheckpointFormatException("NRC1 publication fact spill identity is invalid");
            }
            return new PublicationFact(actualIndex, start, end);
        }

        private void copySpill(PrivateStagingSpillFile spill) throws IOException {
            long copied = 0;
            byte[] buffer = new byte[64 << 10];
            try (InputStream input = spill.openVerifiedInputStream()) {
                while (true) {
                    int read = input.read(buffer);
                    if (read < 0) {
                        break;
                    }
                    writeBody(buffer, 0, read);
                    copied = Math.addExact(copied, read);
                }
            }
            if (copied != spill.sealedLength()) {
                throw new RecoveryCheckpointFormatException("NRC1 directory spill length changed during copy");
            }
        }

        private void writeBody(byte[] bytes) throws IOException {
            writeBody(bytes, 0, bytes.length);
        }

        private void writeBody(byte[] bytes, int offset, int length) throws IOException {
            requireWritableBodyBytes(length);
            stagingOutput.write(bytes, offset, length);
            bodySha256.update(bytes, offset, length);
            position = Math.addExact(position, length);
        }

        private void requireWritableBodyBytes(long bytes) {
            long maximumBody = RecoveryCheckpointFormatV1.MAX_OBJECT_BYTES
                    - RecoveryCheckpointFormatV1.FOOTER_BYTES;
            if (bytes < 0 || position > maximumBody || bytes > maximumBody - position) {
                throw new RecoveryCheckpointFormatException("NRC1 object exceeds its 1 GiB hard limit");
            }
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            closeTemporaryFiles();
            if (!transferred) {
                stagingFile.close();
            }
        }

        private void closeTemporaryFiles() {
            if (publicationFactsReader != null) {
                publicationFactsReader.close();
                publicationFactsReader = null;
            }
            publicationDirectorySpill.close();
            commitDirectorySpill.close();
            publicationFactsSpill.close();
        }
    }

    private static void requireCovered(
            RecoveryCheckpointEntry entry,
            List<Coverage> publicationCoverages) {
        List<Coverage> ordered = publicationCoverages.stream()
                .sorted(Comparator.comparingLong(Coverage::startOffset)
                        .thenComparingLong(Coverage::endOffset))
                .toList();
        long cursor = entry.range().startOffset();
        for (Coverage coverage : ordered) {
            if (coverage.endOffset() <= cursor) {
                continue;
            }
            if (coverage.startOffset() > cursor) {
                throw new RecoveryCheckpointFormatException(
                        "NRC1 publication references do not losslessly cover the commit entry");
            }
            cursor = Math.max(cursor, coverage.endOffset());
            if (cursor >= entry.range().endOffset()) {
                return;
            }
        }
        throw new RecoveryCheckpointFormatException(
                "NRC1 publication references do not losslessly cover the commit entry");
    }

    private static byte[] int32(int value) {
        return ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN).putInt(value).array();
    }

    private static final class SerialExecutor implements Executor {
        private final Executor delegate;
        private final ArrayDeque<Runnable> queue = new ArrayDeque<>();
        private boolean running;

        private SerialExecutor(Executor delegate) {
            this.delegate = delegate;
        }

        @Override
        public void execute(Runnable command) {
            Objects.requireNonNull(command, "command");
            boolean schedule;
            synchronized (this) {
                queue.addLast(command);
                schedule = !running;
                if (schedule) {
                    running = true;
                }
            }
            if (schedule) {
                try {
                    delegate.execute(this::drain);
                } catch (RuntimeException failure) {
                    synchronized (this) {
                        running = false;
                        queue.clear();
                    }
                    throw failure;
                }
            }
        }

        private void drain() {
            while (true) {
                Runnable next;
                synchronized (this) {
                    next = queue.pollFirst();
                    if (next == null) {
                        running = false;
                        return;
                    }
                }
                try {
                    next.run();
                } catch (Throwable ignored) {
                    // Each submitted action owns its future completion and cleanup.
                }
            }
        }
    }

    private static final class CheckpointReadDeadline {
        private final long deadlineNanos;

        private CheckpointReadDeadline(Duration timeout) {
            long now = System.nanoTime();
            long duration;
            try {
                duration = timeout.toNanos();
            } catch (ArithmeticException failure) {
                duration = Long.MAX_VALUE;
            }
            long candidate;
            try {
                candidate = Math.addExact(now, duration);
            } catch (ArithmeticException failure) {
                candidate = Long.MAX_VALUE;
            }
            deadlineNanos = candidate;
        }

        private Duration remaining() {
            long remaining = deadlineNanos - System.nanoTime();
            if (remaining <= 0) {
                throw new NereusException(
                        ErrorCode.TIMEOUT,
                        true,
                        "recovery checkpoint read deadline expired");
            }
            return Duration.ofNanos(remaining);
        }
    }

    @FunctionalInterface
    private interface ThrowingConsumer<T> {
        void accept(T value) throws Exception;
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private record Coverage(long startOffset, long endOffset) {
    }

    private record PublicationFact(int index, long startOffset, long endOffset) {
    }

    private record PublicationOffsets(long[] fileOffsets) {
    }

    private record CommitOffsets(long[] versions, long[] offsetStarts, long[] fileOffsets) {
    }

    private record DirectoryState(PublicationOffsets publications, CommitOffsets commits) {
    }

    private record HeaderAndPublications(
            RecoveryCheckpointBinary.Decoded<RecoveryCheckpointWriteRequest> header,
            PublicationOffsets publications) {
    }

    private record HeaderPublicationsAndCommits(
            RecoveryCheckpointBinary.Decoded<RecoveryCheckpointWriteRequest> header,
            PublicationOffsets publications,
            CommitOffsets commits) {
        private HeaderPublicationsAndCommits(HeaderAndPublications state, CommitOffsets commits) {
            this(state.header(), state.publications(), commits);
        }
    }
}
