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

package com.nereusstream.storage.bookkeeper;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperDigestTypeV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import org.apache.bookkeeper.client.api.BKException;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;

/**
 * Fail-closed M5 adapter for deleting one exact, sealed BookKeeper ledger and reconciling the result.
 *
 * <p>This adapter is deliberately not a delete-authority API. Its caller must first persist and revalidate the M5-D
 * intent/fences. A delete response is never treated as success without an authoritative metadata read proving the
 * ledger absent.
 */
public final class M5BookKeeperDeleteAdapterV1 {
    private static final byte[] TARGET_DOMAIN = "NEREUS-M5-BK-DELETE-TARGET-V1".getBytes(StandardCharsets.US_ASCII);

    private final DeleteClient client;
    private final BookKeeperCapabilitySnapshotV1 capability;
    private final byte[] password;

    public M5BookKeeperDeleteAdapterV1(
            BookKeeper borrowedClient, BookKeeperCapabilitySnapshotV1 capability, byte[] password) {
        this(new RealDeleteClient(borrowedClient), capability, password);
    }

    M5BookKeeperDeleteAdapterV1(DeleteClient client, BookKeeperCapabilitySnapshotV1 capability, byte[] password) {
        this.client = Objects.requireNonNull(client, "client");
        this.capability = Objects.requireNonNull(capability, "capability");
        this.password = Objects.requireNonNull(password, "password").clone();
    }

    /** Captures the immutable target only from an exact, closed, currently readable ledger metadata record. */
    public CompletionStage<CaptureResult> captureExactTarget(RunLedgerHandleV1 handle) {
        requireHandle(handle);
        return read(handle).thenApply(read -> switch (read.outcome()) {
            case PRESENT ->
                targetFrom(read.metadata().orElseThrow(), handle)
                        .map(CaptureResult::exact)
                        .orElseGet(CaptureResult::differentOrUnsealed);
            case DEFINITIVELY_ABSENT -> CaptureResult.definitivelyAbsent();
            case OUTCOME_UNKNOWN -> CaptureResult.outcomeUnknown();
        });
    }

    /**
     * Revalidates the exact target, dispatches one delete, and always reconciles from BookKeeper metadata.
     *
     * <p>The caller may retry only {@link DeleteOutcome#EXACT_LEDGER_REMAINS}. Unknown and changed metadata remain
     * fail-closed.
     */
    public CompletionStage<DeleteResult> deleteAndReconcile(BookKeeperDeleteTargetV1 target) {
        Objects.requireNonNull(target, "target");
        requireHandle(target.handle());
        return read(target.handle()).thenCompose(preRead -> {
            DeleteResult precondition = classify(preRead, target);
            if (precondition.outcome() != DeleteOutcome.EXACT_LEDGER_REMAINS) {
                return CompletableFuture.completedFuture(precondition);
            }
            return deleteThenReconcile(target);
        });
    }

    private CompletionStage<DeleteResult> deleteThenReconcile(BookKeeperDeleteTargetV1 target) {
        CompletionStage<Void> dispatch;
        try {
            dispatch = client.delete(target.handle().ledgerIdentity().ledgerId());
        } catch (RuntimeException failure) {
            dispatch = CompletableFuture.failedFuture(failure);
        }
        return dispatch.handle((ignored, failure) -> null)
                .thenCompose(ignored -> read(target.handle()))
                .thenApply(read -> classify(read, target));
    }

    private DeleteResult classify(MetadataRead read, BookKeeperDeleteTargetV1 expected) {
        return switch (read.outcome()) {
            case DEFINITIVELY_ABSENT -> DeleteResult.authoritativelyAbsent();
            case OUTCOME_UNKNOWN -> DeleteResult.outcomeUnknown();
            case PRESENT ->
                targetFrom(read.metadata().orElseThrow(), expected.handle())
                        .map(actual -> actual.equals(expected)
                                ? DeleteResult.exactLedgerRemains()
                                : DeleteResult.differentLedgerOrMetadata())
                        .orElseGet(DeleteResult::differentLedgerOrMetadata);
        };
    }

    private CompletionStage<MetadataRead> read(RunLedgerHandleV1 handle) {
        CompletionStage<LedgerMetadata> read;
        try {
            read = client.read(handle.ledgerIdentity().ledgerId());
        } catch (RuntimeException failure) {
            return CompletableFuture.completedFuture(classifyReadFailure(failure));
        }
        return read.handle((metadata, failure) -> failure == null
                ? MetadataRead.present(Objects.requireNonNull(metadata, "metadata"))
                : classifyReadFailure(failure));
    }

    private static MetadataRead classifyReadFailure(Throwable failure) {
        Throwable cause = unwrap(failure);
        if (cause instanceof BKException bookKeeperFailure
                && (bookKeeperFailure.getCode() == BKException.Code.NoSuchLedgerExistsException
                        || bookKeeperFailure.getCode()
                                == BKException.Code.NoSuchLedgerExistsOnMetadataServerException)) {
            return MetadataRead.definitivelyAbsent();
        }
        return MetadataRead.outcomeUnknown();
    }

    private Optional<BookKeeperDeleteTargetV1> targetFrom(LedgerMetadata metadata, RunLedgerHandleV1 handle) {
        if (!metadata.isClosed()
                || !RealBookKeeperCellSessionV1.metadataMatches(metadata, handle)
                || metadata.getEnsembleSize() != capability.ensembleSize()
                || metadata.getWriteQuorumSize() != capability.writeQuorumSize()
                || metadata.getAckQuorumSize() != capability.ackQuorumSize()
                || !Arrays.equals(metadata.getPassword(), password)) {
            return Optional.empty();
        }
        Optional<BookKeeperDigestTypeV1> digestType = digestType(metadata.getDigestType());
        if (digestType.isEmpty() || digestType.orElseThrow() != capability.digestType()) {
            return Optional.empty();
        }
        Sha256Digest passwordSha256 = Sha256Digest.hash(CanonicalBytes.copyOf(password));
        Sha256Digest metadataSha256 = metadataSha256(metadata, handle, passwordSha256);
        return Optional.of(new BookKeeperDeleteTargetV1(
                handle,
                metadata.getLastEntryId(),
                metadata.getLength(),
                metadata.getEnsembleSize(),
                metadata.getWriteQuorumSize(),
                metadata.getAckQuorumSize(),
                digestType.orElseThrow(),
                capability.credentialIdentityVersion(),
                passwordSha256,
                metadata.getMetadataFormatVersion(),
                metadata.getCToken(),
                metadataSha256));
    }

    private static Sha256Digest metadataSha256(
            LedgerMetadata metadata, RunLedgerHandleV1 handle, Sha256Digest passwordSha256) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                writeBytes(output, TARGET_DOMAIN);
                output.writeLong(handle.ledgerIdentity().ledgerId());
                writeBytes(output, handle.providerScopeId().digest().bytes().toByteArray());
                writeBytes(output, handle.runId().value().bytes().toByteArray());
                writeBytes(output, handle.configurationDigest().bytes().toByteArray());
                output.writeBoolean(metadata.isClosed());
                output.writeLong(metadata.getLastEntryId());
                output.writeLong(metadata.getLength());
                output.writeInt(metadata.getEnsembleSize());
                output.writeInt(metadata.getWriteQuorumSize());
                output.writeInt(metadata.getAckQuorumSize());
                writeString(output, metadata.getDigestType().name());
                output.writeBoolean(metadata.hasPassword());
                writeBytes(output, passwordSha256.bytes().toByteArray());
                output.writeLong(metadata.getCtime());
                output.writeInt(metadata.getMetadataFormatVersion());
                output.writeLong(metadata.getCToken());
                writeString(output, metadata.getState().name());
                writeCustomMetadata(output, metadata.getCustomMetadata());
                output.writeInt(metadata.getAllEnsembles().size());
                for (Map.Entry<Long, ? extends java.util.List<org.apache.bookkeeper.net.BookieId>> entry :
                        metadata.getAllEnsembles().entrySet()) {
                    output.writeLong(entry.getKey());
                    output.writeInt(entry.getValue().size());
                    for (org.apache.bookkeeper.net.BookieId bookie : entry.getValue()) {
                        writeString(output, bookie.toString());
                    }
                }
            }
            return Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray()));
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory BookKeeper target encoding failed", impossible);
        }
    }

    private static void writeCustomMetadata(DataOutputStream output, Map<String, byte[]> metadata) throws IOException {
        output.writeInt(metadata.size());
        for (Map.Entry<String, byte[]> entry : metadata.entrySet().stream()
                .sorted(Map.Entry.comparingByKey(Comparator.naturalOrder()))
                .toList()) {
            writeString(output, entry.getKey());
            writeBytes(output, entry.getValue());
        }
    }

    private static void writeString(DataOutputStream output, String value) throws IOException {
        writeBytes(output, value.getBytes(StandardCharsets.UTF_8));
    }

    private static void writeBytes(DataOutputStream output, byte[] value) throws IOException {
        output.writeInt(value.length);
        output.write(value);
    }

    private static Optional<BookKeeperDigestTypeV1> digestType(DigestType value) {
        return switch (value) {
            case MAC -> Optional.of(BookKeeperDigestTypeV1.MAC);
            case CRC32 -> Optional.of(BookKeeperDigestTypeV1.CRC32);
            case CRC32C -> Optional.of(BookKeeperDigestTypeV1.CRC32C);
            case DUMMY -> Optional.empty();
        };
    }

    private void requireHandle(RunLedgerHandleV1 handle) {
        Objects.requireNonNull(handle, "handle");
        if (!handle.providerScopeId().equals(capability.providerScopeId())
                || !handle.configurationDigest().equals(capability.configurationDigest())) {
            throw new IllegalArgumentException("delete target differs from the admitted BookKeeper capability");
        }
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    public enum CaptureOutcome {
        EXACT_TARGET,
        DEFINITIVELY_ABSENT,
        DIFFERENT_OR_UNSEALED,
        OUTCOME_UNKNOWN
    }

    public enum DeleteOutcome {
        AUTHORITATIVELY_ABSENT,
        EXACT_LEDGER_REMAINS,
        DIFFERENT_LEDGER_OR_METADATA,
        OUTCOME_UNKNOWN
    }

    public record CaptureResult(CaptureOutcome outcome, Optional<BookKeeperDeleteTargetV1> exactTarget) {
        public CaptureResult {
            Objects.requireNonNull(outcome, "outcome");
            Objects.requireNonNull(exactTarget, "exactTarget");
            if ((outcome == CaptureOutcome.EXACT_TARGET) != exactTarget.isPresent()) {
                throw new IllegalArgumentException("only EXACT_TARGET carries a BookKeeper delete target");
            }
        }

        static CaptureResult exact(BookKeeperDeleteTargetV1 target) {
            return new CaptureResult(CaptureOutcome.EXACT_TARGET, Optional.of(target));
        }

        static CaptureResult definitivelyAbsent() {
            return new CaptureResult(CaptureOutcome.DEFINITIVELY_ABSENT, Optional.empty());
        }

        static CaptureResult differentOrUnsealed() {
            return new CaptureResult(CaptureOutcome.DIFFERENT_OR_UNSEALED, Optional.empty());
        }

        static CaptureResult outcomeUnknown() {
            return new CaptureResult(CaptureOutcome.OUTCOME_UNKNOWN, Optional.empty());
        }
    }

    public record DeleteResult(DeleteOutcome outcome) {
        public DeleteResult {
            Objects.requireNonNull(outcome, "outcome");
        }

        static DeleteResult authoritativelyAbsent() {
            return new DeleteResult(DeleteOutcome.AUTHORITATIVELY_ABSENT);
        }

        static DeleteResult exactLedgerRemains() {
            return new DeleteResult(DeleteOutcome.EXACT_LEDGER_REMAINS);
        }

        static DeleteResult differentLedgerOrMetadata() {
            return new DeleteResult(DeleteOutcome.DIFFERENT_LEDGER_OR_METADATA);
        }

        static DeleteResult outcomeUnknown() {
            return new DeleteResult(DeleteOutcome.OUTCOME_UNKNOWN);
        }
    }

    public record BookKeeperDeleteTargetV1(
            RunLedgerHandleV1 handle,
            long sealedLastEntryId,
            long sealedLength,
            int ensembleSize,
            int writeQuorumSize,
            int ackQuorumSize,
            BookKeeperDigestTypeV1 digestType,
            String passwordCredentialIdentityVersion,
            Sha256Digest passwordSha256,
            int metadataFormatVersion,
            long metadataCToken,
            Sha256Digest metadataSha256) {
        public BookKeeperDeleteTargetV1 {
            Objects.requireNonNull(handle, "handle");
            Objects.requireNonNull(digestType, "digestType");
            Objects.requireNonNull(passwordCredentialIdentityVersion, "passwordCredentialIdentityVersion");
            Objects.requireNonNull(passwordSha256, "passwordSha256");
            Objects.requireNonNull(metadataSha256, "metadataSha256");
            if (sealedLastEntryId < -1
                    || sealedLength < 0
                    || ensembleSize <= 0
                    || writeQuorumSize <= 0
                    || ackQuorumSize <= 0
                    || ackQuorumSize > writeQuorumSize
                    || writeQuorumSize > ensembleSize
                    || passwordCredentialIdentityVersion.isBlank()
                    || metadataFormatVersion < 0) {
                throw new IllegalArgumentException("invalid exact sealed BookKeeper delete target");
            }
        }
    }

    interface DeleteClient {
        CompletionStage<LedgerMetadata> read(long ledgerId);

        CompletionStage<Void> delete(long ledgerId);
    }

    private enum ReadOutcome {
        PRESENT,
        DEFINITIVELY_ABSENT,
        OUTCOME_UNKNOWN
    }

    private record MetadataRead(ReadOutcome outcome, Optional<LedgerMetadata> metadata) {
        private MetadataRead {
            Objects.requireNonNull(outcome, "outcome");
            Objects.requireNonNull(metadata, "metadata");
        }

        static MetadataRead present(LedgerMetadata metadata) {
            return new MetadataRead(ReadOutcome.PRESENT, Optional.of(metadata));
        }

        static MetadataRead definitivelyAbsent() {
            return new MetadataRead(ReadOutcome.DEFINITIVELY_ABSENT, Optional.empty());
        }

        static MetadataRead outcomeUnknown() {
            return new MetadataRead(ReadOutcome.OUTCOME_UNKNOWN, Optional.empty());
        }
    }

    private record RealDeleteClient(BookKeeper client) implements DeleteClient {
        private RealDeleteClient {
            Objects.requireNonNull(client, "client");
        }

        @Override
        public CompletionStage<LedgerMetadata> read(long ledgerId) {
            return client.getLedgerMetadata(ledgerId);
        }

        @Override
        public CompletionStage<Void> delete(long ledgerId) {
            return client.newDeleteLedgerOp().withLedgerId(ledgerId).execute();
        }
    }
}
