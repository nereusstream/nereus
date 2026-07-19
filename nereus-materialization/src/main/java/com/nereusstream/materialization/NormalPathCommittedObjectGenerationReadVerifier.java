/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.PhysicalReadResult;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ReadTargetIdentities;
import com.nereusstream.api.ReadView;
import com.nereusstream.core.read.GenerationReadCandidate;
import com.nereusstream.core.read.GenerationReadResolver;
import com.nereusstream.core.read.PinnedResolvedRange;
import com.nereusstream.core.read.ProviderNeutralReadAccounting;
import com.nereusstream.core.read.ReadTargetDispatcher;
import com.nereusstream.core.read.ReadTargetReaderRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;

/** Acquires the normal durable Object pin and reads every record in the exact retirement source range. */
public final class NormalPathCommittedObjectGenerationReadVerifier
        implements CommittedObjectGenerationReadVerifier {
    private final GenerationReadResolver resolver;
    private final ReadTargetDispatcher dispatcher;
    private final int pageRecords;
    private final int pageBytes;
    private final ScheduledExecutorService scheduler;

    public NormalPathCommittedObjectGenerationReadVerifier(
            GenerationReadResolver resolver,
            ReadTargetReaderRegistry readers,
            int pageRecords,
            int pageBytes,
            ScheduledExecutorService scheduler) {
        this.resolver = Objects.requireNonNull(resolver, "resolver");
        this.dispatcher = new ReadTargetDispatcher(Objects.requireNonNull(readers, "readers"));
        if (pageRecords <= 0 || pageBytes <= 0) {
            throw new IllegalArgumentException("read verification page limits must be positive");
        }
        this.pageRecords = pageRecords;
        this.pageBytes = pageBytes;
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    }

    @Override
    public CompletableFuture<Boolean> verify(
            CommittedObjectGenerationProof proof,
            Duration timeout) {
        try {
            CommittedObjectGenerationProof expected = Objects.requireNonNull(proof, "proof");
            MaterializationDeadline deadline = new MaterializationDeadline(timeout, scheduler);
            CompletableFuture<Boolean> result = verifyPage(
                    expected, expected.sourceRange().startOffset(), deadline);
            result.whenComplete((ignored, failure) -> deadline.close());
            return result;
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private CompletableFuture<Boolean> verifyPage(
            CommittedObjectGenerationProof proof,
            long offset,
            MaterializationDeadline deadline) {
        if (offset == proof.sourceRange().endOffset()) {
            return CompletableFuture.completedFuture(true);
        }
        return deadline.bound(
                        () -> resolver.resolve(
                                proof.streamId(),
                                offset,
                                ReadView.COMMITTED,
                                deadline.remaining()),
                        "resolve committed Object retirement read")
                .thenCompose(optional -> {
                    if (optional.isEmpty()) {
                        return CompletableFuture.completedFuture(false);
                    }
                    PinnedResolvedRange pinned = optional.orElseThrow();
                    if (!isExact(proof, pinned.candidate())) {
                        return pinned.release().thenApply(ignored -> false);
                    }
                    int records = Math.toIntExact(Math.min(
                            pageRecords,
                            proof.sourceRange().endOffset() - offset));
                    ReadOptions options = new ReadOptions(
                            records,
                            pageBytes,
                            ReadIsolation.COMMITTED,
                            deadline.remaining());
                    CompletableFuture<PhysicalReadResult> read = deadline.bound(
                            () -> dispatcher.read(
                                    proof.streamId(),
                                    offset,
                                    List.of(pinned.resolvedRange()),
                                    options),
                            "read committed Object retirement range");
                    return releaseAfter(read, pinned).thenCompose(result -> {
                        long next = validatePage(proof, offset, records, pinned.candidate(), result);
                        return verifyPage(proof, next, deadline);
                    });
                });
    }

    private static boolean isExact(
            CommittedObjectGenerationProof proof,
            GenerationReadCandidate candidate) {
        var value = proof.index().value();
        return !candidate.generationZero()
                && candidate.view() == ReadView.COMMITTED
                && candidate.indexKey().equals(proof.index().key())
                && candidate.indexMetadataVersion() == proof.index().metadataVersion()
                && candidate.indexRecordSha256().equals(proof.index().durableValueSha256())
                && candidate.resolvedRange().generation() == value.generation()
                && candidate.resolvedRange().commitVersion() == value.lastCommitVersion()
                && candidate.resolvedRange().readTarget().equals(proof.target())
                && candidate.resolvedRange().offsetRange().startOffset() <= proof.sourceRange().startOffset()
                && candidate.resolvedRange().offsetRange().endOffset() >= proof.sourceRange().endOffset();
    }

    private static long validatePage(
            CommittedObjectGenerationProof proof,
            long offset,
            int maximumRecords,
            GenerationReadCandidate candidate,
            PhysicalReadResult result) {
        ProviderNeutralReadAccounting.validate(List.of(candidate.resolvedRange()), result);
        if (result.batches().isEmpty()) {
            throw invariant("committed Object retirement verification returned no records");
        }
        Checksum targetIdentity = ReadTargetIdentities.sha256(proof.target());
        long next = offset;
        long records = 0;
        for (ReadBatch batch : result.batches()) {
            if (batch.range().startOffset() != next
                    || batch.range().endOffset() > proof.sourceRange().endOffset()
                    || !batch.source().resolvedRange().equals(candidate.resolvedRange().offsetRange())
                    || batch.source().generation() != candidate.resolvedRange().generation()
                    || batch.source().commitVersion() != candidate.resolvedRange().commitVersion()
                    || !batch.source().target().equals(proof.target())
                    || !batch.source().targetIdentity().equals(targetIdentity)) {
                throw invariant("committed Object retirement read escaped its exact generation identity");
            }
            next = batch.range().endOffset();
            records = Math.addExact(records, batch.range().recordCount());
        }
        if (next <= offset || records > maximumRecords) {
            throw invariant("committed Object retirement read made invalid bounded progress");
        }
        return next;
    }

    private static <T> CompletableFuture<T> releaseAfter(
            CompletableFuture<T> operation,
            PinnedResolvedRange pinned) {
        return operation.handle((value, failure) -> pinned.release().handle((ignored, releaseFailure) -> {
            if (failure == null && releaseFailure == null) {
                return value;
            }
            Throwable cause = failure == null ? unwrap(releaseFailure) : unwrap(failure);
            if (failure != null && releaseFailure != null) {
                cause.addSuppressed(unwrap(releaseFailure));
            }
            throw new CompletionException(cause);
        })).thenCompose(value -> value);
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }
}
