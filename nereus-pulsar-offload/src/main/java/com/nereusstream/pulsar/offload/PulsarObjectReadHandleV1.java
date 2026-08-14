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

package com.nereusstream.pulsar.offload;

import com.nereusstream.pulsar.offload.PulsarOffloadObjectStoreV1.ImmutableObject;
import com.nereusstream.pulsar.offload.PulsarOffloadObjectStoreV1.ObjectStoreException;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.EntryPayload;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.SparseBlock;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.Root;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import javax.crypto.SecretKey;

/** Bounded NPO1/NPD1 Object-side read handle for one immutable sealed-ledger attempt. */
public final class PulsarObjectReadHandleV1 {
    public enum ReadFailureKind {
        NOT_FOUND,
        TIMEOUT,
        UNAVAILABLE,
        SHORT_READ,
        INTEGRITY,
        FORMAT,
        INVALID_RANGE,
        CANCELLED,
        CLOSED
    }

    public static final class ObjectReadException extends IllegalStateException {
        private final ReadFailureKind kind;

        public ObjectReadException(ReadFailureKind kind, String message) {
            super(message);
            this.kind = Objects.requireNonNull(kind, "kind");
        }

        public ObjectReadException(ReadFailureKind kind, String message, Throwable cause) {
            super(message, cause);
            this.kind = Objects.requireNonNull(kind, "kind");
        }

        public ReadFailureKind kind() {
            return kind;
        }
    }

    public record VerificationReport(long entryCount, long logicalLength, int blockCount, String dataSha256) {
        public VerificationReport {
            Objects.requireNonNull(dataSha256, "dataSha256");
        }
    }

    private final PulsarOffloadObjectStoreV1 objectStore;
    private final PulsarOffloadLimitCandidateV1 limits;
    private final PulsarSealedLedgerAttemptV1 attempt;
    private final SecretKey attemptKey;
    private final Root root;
    private final byte[] dataHeader;
    private final AtomicBoolean closed = new AtomicBoolean();

    private PulsarObjectReadHandleV1(
            PulsarOffloadObjectStoreV1 objectStore,
            PulsarOffloadLimitCandidateV1 limits,
            PulsarSealedLedgerAttemptV1 attempt,
            SecretKey attemptKey,
            Root root,
            byte[] dataHeader) {
        this.objectStore = objectStore;
        this.limits = limits;
        this.attempt = attempt;
        this.attemptKey = attemptKey;
        this.root = root;
        this.dataHeader = dataHeader.clone();
    }

    public static CompletionStage<PulsarObjectReadHandleV1> open(
            PulsarOffloadObjectStoreV1 objectStore,
            PulsarOffloadLimitCandidateV1 limits,
            PulsarSealedLedgerAttemptV1 attempt,
            SecretKey attemptKey) {
        Objects.requireNonNull(objectStore, "objectStore");
        Objects.requireNonNull(limits, "limits");
        Objects.requireNonNull(attempt, "attempt");
        Objects.requireNonNull(attemptKey, "attemptKey");
        PulsarOffloadKeysV1 keys = attempt.keys();
        return provider(() -> objectStore.head(keys.rootKey()), "root HEAD")
                .thenCompose(rootProof -> {
                    if (rootProof.bytes() > Npo1CodecV1.MAX_ROOT_BYTES || rootProof.bytes() > Integer.MAX_VALUE) {
                        return failed(ReadFailureKind.FORMAT, "provider NPO1 length exceeds the hard cap");
                    }
                    return readExact(objectStore, keys.rootKey(), 0, Math.toIntExact(rootProof.bytes()), "root GET")
                            .thenApply(rootBytes -> parseRoot(rootBytes, rootProof, limits, attempt));
                })
                .thenCompose(root -> provider(() -> objectStore.head(keys.dataKey()), "data HEAD")
                        .thenCompose(dataProof -> validateDataProof(root, dataProof)
                                .thenCompose(ignored -> readExact(
                                        objectStore,
                                        keys.dataKey(),
                                        0,
                                        Npd1CodecV1.DATA_HEADER_BYTES,
                                        "data header GET"))
                                .thenApply(header -> {
                                    validateDataHeader(root, header);
                                    return new PulsarObjectReadHandleV1(
                                            objectStore, limits, attempt, attemptKey, root, header);
                                })));
    }

    public Root root() {
        return root;
    }

    public CompletionStage<List<EntryPayload>> read(long firstEntryId, long lastEntryId) {
        if (closed.get()) {
            return failed(ReadFailureKind.CLOSED, "Object read handle is closed");
        }
        if (firstEntryId < 0
                || lastEntryId < firstEntryId
                || lastEntryId > root.sealedLedger().lastAddConfirmed()) {
            return failed(ReadFailureKind.INVALID_RANGE, "requested entry range is outside the sealed ledger");
        }
        List<EntryPayload> result = new ArrayList<>();
        CompletionStage<Void> chain = CompletableFuture.completedFuture(null);
        for (SparseBlock block : root.sparseIndex()) {
            if (block.lastEntryId() < firstEntryId || block.firstEntryId() > lastEntryId) {
                continue;
            }
            chain = chain.thenCompose(ignored -> readBlock(block).thenAccept(entries -> {
                for (EntryPayload entry : entries) {
                    if (entry.entryId() >= firstEntryId && entry.entryId() <= lastEntryId) {
                        result.add(entry);
                    }
                }
            }));
        }
        return chain.thenApply(ignored -> {
            validateRange(result, firstEntryId, lastEntryId);
            return List.copyOf(result);
        });
    }

    public CompletionStage<VerificationReport> verifyCompleteLedger() {
        if (closed.get()) {
            return failed(ReadFailureKind.CLOSED, "Object read handle is closed");
        }
        VerificationAccumulator accumulator = new VerificationAccumulator(dataHeader);
        CompletionStage<Void> chain = CompletableFuture.completedFuture(null);
        for (SparseBlock block : root.sparseIndex()) {
            chain = chain.thenCompose(ignored -> readEncodedBlock(block).thenAccept(encoded -> {
                List<EntryPayload> entries = decodeBlock(encoded, block);
                accumulator.accept(block, encoded, entries);
            }));
        }
        return chain.thenApply(ignored -> accumulator.finish(root));
    }

    public CompletionStage<Void> close() {
        closed.set(true);
        return CompletableFuture.completedFuture(null);
    }

    private CompletionStage<List<EntryPayload>> readBlock(SparseBlock block) {
        return readEncodedBlock(block).thenApply(encoded -> decodeBlock(encoded, block));
    }

    private CompletionStage<byte[]> readEncodedBlock(SparseBlock block) {
        if (closed.get()) {
            return failed(ReadFailureKind.CLOSED, "Object read handle is closed");
        }
        return readExact(
                objectStore,
                attempt.keys().dataKey(),
                block.blockOffset(),
                block.encodedBlockBytes(),
                "NPD1 block range GET");
    }

    private List<EntryPayload> decodeBlock(byte[] encoded, SparseBlock block) {
        try {
            return Npd1CodecV1.decodeBlock(encoded, block, attemptKey, attempt.attemptUuid(), limits);
        } catch (RuntimeException failure) {
            throw new ObjectReadException(ReadFailureKind.INTEGRITY, "NPD1 block verification failed", failure);
        }
    }

    private static Root parseRoot(
            byte[] rootBytes,
            ImmutableObject rootProof,
            PulsarOffloadLimitCandidateV1 limits,
            PulsarSealedLedgerAttemptV1 attempt) {
        String actualSha = sha256(rootBytes);
        if (rootBytes.length != rootProof.bytes() || !actualSha.equals(rootProof.sha256())) {
            throw new ObjectReadException(ReadFailureKind.INTEGRITY, "root Object proof differs from exact GET");
        }
        Root root;
        try {
            root = Npo1CodecV1.parseCanonical(rootBytes, limits);
        } catch (RuntimeException failure) {
            ReadFailureKind kind =
                    failure.getMessage() != null && failure.getMessage().contains("self-digest")
                            ? ReadFailureKind.INTEGRITY
                            : ReadFailureKind.FORMAT;
            throw new ObjectReadException(kind, "NPO1 root validation failed", failure);
        }
        if (root.attempt().ledgerId() != attempt.ledgerId()
                || !root.attempt().attemptUuid().equals(attempt.attemptUuid())
                || !root.attempt().providerScopePrefix().equals(attempt.providerScopePrefix())
                || root.attempt().retentionClass() != attempt.retentionClass()
                || root.sealedLedger().lastAddConfirmed() != attempt.lastAddConfirmed()
                || root.sealedLedger().entryCount() != attempt.entryCount()
                || root.sealedLedger().logicalLength() != attempt.logicalLength()) {
            throw new ObjectReadException(ReadFailureKind.FORMAT, "NPO1 identity or sealed-ledger facts differ");
        }
        return root;
    }

    private static CompletionStage<Void> validateDataProof(Root root, ImmutableObject proof) {
        if (proof.bytes() != root.dataExtent().dataBytes()
                || !proof.sha256().equals(root.dataExtent().dataSha256())
                || !proof.immutableVersion().equals(root.dataExtent().immutableVersion())) {
            return failed(ReadFailureKind.INTEGRITY, "data Object proof differs from NPO1");
        }
        return CompletableFuture.completedFuture(null);
    }

    private static void validateDataHeader(Root root, byte[] headerBytes) {
        try {
            Npd1CodecV1.DataHeader header = Npd1CodecV1.parseDataHeader(headerBytes);
            if (header.blockCount() != root.sparseIndex().size()
                    || header.totalBytes() != root.dataExtent().dataBytes()) {
                throw new ObjectReadException(ReadFailureKind.FORMAT, "NPD1 header differs from NPO1");
            }
        } catch (ObjectReadException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw new ObjectReadException(ReadFailureKind.FORMAT, "NPD1 header validation failed", failure);
        }
    }

    private static void validateRange(List<EntryPayload> entries, long first, long last) {
        long expected = first;
        for (EntryPayload entry : entries) {
            if (entry.entryId() != expected) {
                throw new ObjectReadException(ReadFailureKind.INTEGRITY, "Object range is not exact and contiguous");
            }
            expected = Math.addExact(expected, 1);
        }
        if (expected != Math.addExact(last, 1)) {
            throw new ObjectReadException(ReadFailureKind.SHORT_READ, "Object range ended before the requested entry");
        }
    }

    private static CompletionStage<byte[]> readExact(
            PulsarOffloadObjectStoreV1 objectStore, String key, long offset, int length, String operation) {
        return provider(() -> objectStore.readRange(key, offset, length), operation)
                .thenApply(bytes -> {
                    if (bytes.length != length) {
                        throw new ObjectReadException(
                                ReadFailureKind.SHORT_READ, operation + " returned a short range");
                    }
                    return bytes;
                });
    }

    private static <T> CompletionStage<T> provider(Supplier<CompletionStage<T>> call, String operation) {
        CompletionStage<T> stage;
        try {
            stage = call.get();
            if (stage == null) {
                return failed(ReadFailureKind.UNAVAILABLE, operation + " returned a null stage");
            }
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(mapProviderFailure(operation, failure));
        }
        CompletableFuture<T> result = new CompletableFuture<>();
        stage.whenComplete((value, failure) -> {
            if (failure == null) {
                result.complete(value);
            } else {
                result.completeExceptionally(mapProviderFailure(operation, failure));
            }
        });
        return result;
    }

    private static ObjectReadException mapProviderFailure(String operation, Throwable failure) {
        Throwable unwrapped = unwrap(failure);
        if (unwrapped instanceof ObjectReadException readFailure) {
            return readFailure;
        }
        if (unwrapped instanceof ObjectStoreException providerFailure) {
            ReadFailureKind kind =
                    switch (providerFailure.kind()) {
                        case NOT_FOUND -> ReadFailureKind.NOT_FOUND;
                        case TIMEOUT -> ReadFailureKind.TIMEOUT;
                        case SHORT_READ -> ReadFailureKind.SHORT_READ;
                        case INTEGRITY, CONFLICT -> ReadFailureKind.INTEGRITY;
                        case CANCELLED -> ReadFailureKind.CANCELLED;
                        case UNAVAILABLE -> ReadFailureKind.UNAVAILABLE;
                    };
            return new ObjectReadException(kind, operation + " failed", providerFailure);
        }
        return new ObjectReadException(ReadFailureKind.UNAVAILABLE, operation + " failed", unwrapped);
    }

    private static Throwable unwrap(Throwable failure) {
        if (failure instanceof CompletionException && failure.getCause() != null) {
            return failure.getCause();
        }
        return failure;
    }

    private static <T> CompletionStage<T> failed(ReadFailureKind kind, String message) {
        return CompletableFuture.failedFuture(new ObjectReadException(kind, message));
    }

    private static String sha256(byte[] bytes) {
        return HexFormat.of().formatHex(digest().digest(bytes));
    }

    private static MessageDigest digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("JDK lacks SHA-256", failure);
        }
    }

    private static final class VerificationAccumulator {
        private final MessageDigest digest = digest();
        private long nextEntryId;
        private long entryCount;
        private long logicalLength;
        private long objectBytes;
        private int blockCount;

        private VerificationAccumulator(byte[] header) {
            digest.update(header);
            objectBytes = header.length;
        }

        private void accept(SparseBlock block, byte[] encoded, List<EntryPayload> entries) {
            if (block.blockOrdinal() != blockCount || block.firstEntryId() != nextEntryId) {
                throw new ObjectReadException(ReadFailureKind.INTEGRITY, "verification block coverage differs");
            }
            digest.update(encoded);
            objectBytes = Math.addExact(objectBytes, encoded.length);
            for (EntryPayload entry : entries) {
                if (entry.entryId() != nextEntryId) {
                    throw new ObjectReadException(ReadFailureKind.INTEGRITY, "verification entry coverage differs");
                }
                nextEntryId = Math.addExact(nextEntryId, 1);
                entryCount = Math.addExact(entryCount, 1);
                logicalLength = Math.addExact(logicalLength, entry.payloadBytes());
            }
            blockCount++;
        }

        private VerificationReport finish(Root root) {
            String actualSha = HexFormat.of().formatHex(digest.digest());
            if (entryCount != root.sealedLedger().entryCount()
                    || logicalLength != root.sealedLedger().logicalLength()
                    || blockCount != root.sparseIndex().size()
                    || objectBytes != root.dataExtent().dataBytes()
                    || !actualSha.equals(root.dataExtent().dataSha256())) {
                throw new ObjectReadException(ReadFailureKind.INTEGRITY, "complete Object verification differs");
            }
            return new VerificationReport(entryCount, logicalLength, blockCount, actualSha);
        }
    }
}
