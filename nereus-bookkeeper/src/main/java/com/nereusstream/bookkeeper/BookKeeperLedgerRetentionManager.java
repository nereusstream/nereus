/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.BookKeeperLedgerMetadataStore;
import com.nereusstream.metadata.oxia.BookKeeperVersionedValue;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import org.apache.bookkeeper.client.api.LedgerMetadata;

/** Mark/drain/delete/dual-absence convergence for exact Nereus-owned BookKeeper ledgers. */
public final class BookKeeperLedgerRetentionManager {
    private final String cluster;
    private final BookKeeperWalConfiguration configuration;
    private final BookKeeperLedgerGcConfiguration gcConfiguration;
    private final BookKeeperLedgerMetadataStore metadata;
    private final BookKeeperLedgerIdNamespaceReservationVerifier namespaceVerifier;
    private final BookKeeperProtocolActivationVerifier activationVerifier;
    private final BookKeeperClientOperations client;
    private final BookKeeperWalRetentionGate gate;
    private final Clock clock;
    private final Semaphore deletePermits;

    public BookKeeperLedgerRetentionManager(
            String cluster,
            BookKeeperWalConfiguration configuration,
            BookKeeperLedgerGcConfiguration gcConfiguration,
            BookKeeperLedgerMetadataStore metadata,
            BookKeeperLedgerIdNamespaceReservationVerifier namespaceVerifier,
            BookKeeperProtocolActivationVerifier activationVerifier,
            BookKeeperClientOperations client,
            BookKeeperWalRetentionGate gate,
            Clock clock) {
        this.cluster = text(cluster, "cluster");
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.gcConfiguration = Objects.requireNonNull(gcConfiguration, "gcConfiguration");
        gcConfiguration.validateAgainst(configuration);
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.namespaceVerifier = Objects.requireNonNull(namespaceVerifier, "namespaceVerifier");
        this.activationVerifier = Objects.requireNonNull(activationVerifier, "activationVerifier");
        this.client = Objects.requireNonNull(client, "client");
        this.gate = Objects.requireNonNull(gate, "gate");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.deletePermits = new Semaphore(gcConfiguration.maxConcurrentDeletes());
    }

    /** Conditionally freezes one twice-validated SEALED candidate. */
    public CompletableFuture<BookKeeperLedgerGcResult> mark(
            BookKeeperLedgerRetirementCandidate candidate,
            Duration timeout) {
        BookKeeperLedgerRetirementCandidate expected = Objects.requireNonNull(candidate, "candidate");
        if (!gcConfiguration.enabled()) {
            return CompletableFuture.completedFuture(BookKeeperLedgerGcResult.of(
                    BookKeeperLedgerGcAction.DISABLED, expected.root()));
        }
        return gate.evaluate(expected.root(), bounded(timeout)).thenCompose(reloaded -> {
            if (reloaded.candidate().isEmpty()) {
                return CompletableFuture.completedFuture(
                        BookKeeperLedgerGcResult.blocked(expected.root(), reloaded.blockers()));
            }
            BookKeeperLedgerRetirementCandidate exact = reloaded.candidate().orElseThrow();
            if (!sameCandidate(expected, exact)) {
                return CompletableFuture.completedFuture(BookKeeperLedgerGcResult.blocked(
                        exact.root(), java.util.Set.of(BookKeeperRetentionBlocker.ROOT_CHANGED_OR_INELIGIBLE)));
            }
            if (gcConfiguration.dryRun()) {
                return CompletableFuture.completedFuture(BookKeeperLedgerGcResult.of(
                        BookKeeperLedgerGcAction.DRY_RUN_ADMITTED, exact.root()));
            }
            long now = clock.millis();
            long deleteNotBefore = add(now, gcConfiguration.drainGrace());
            BookKeeperLedgerRootRecord marked = root(
                    exact.root().value(),
                    BookKeeperLedgerLifecycle.MARKED,
                    exact.root().value().lifecycleEpoch() + 1,
                    attemptId(exact),
                    exact.referenceSetSha256().value(),
                    now,
                    deleteNotBefore,
                    0,
                    0,
                    0,
                    "");
            return metadata.compareAndSetRoot(cluster, marked, exact.root().metadataVersion())
                    .thenApply(value -> BookKeeperLedgerGcResult.of(BookKeeperLedgerGcAction.MARKED, value));
        });
    }

    /** Advances at most one durable/provider step; waits are represented as results, never sleeps. */
    public CompletableFuture<BookKeeperLedgerGcResult> converge(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> observed,
            Duration timeout) {
        BookKeeperVersionedValue<BookKeeperLedgerRootRecord> expected = Objects.requireNonNull(observed, "observed");
        if (!gcConfiguration.enabled()) {
            return CompletableFuture.completedFuture(
                    BookKeeperLedgerGcResult.of(BookKeeperLedgerGcAction.DISABLED, expected));
        }
        if (gcConfiguration.dryRun()) {
            return CompletableFuture.completedFuture(
                    BookKeeperLedgerGcResult.of(BookKeeperLedgerGcAction.DRY_RUN_ADMITTED, expected));
        }
        return reloadExact(expected).thenCompose(root -> switch (root.value().lifecycle()) {
            case MARKED -> advanceMarked(root, bounded(timeout));
            case DELETING -> advanceDeleting(root, bounded(timeout));
            case DELETED, ABORTED, QUARANTINED -> CompletableFuture.completedFuture(
                    BookKeeperLedgerGcResult.of(BookKeeperLedgerGcAction.ALREADY_TERMINAL, root));
            default -> CompletableFuture.completedFuture(BookKeeperLedgerGcResult.blocked(
                    root, java.util.Set.of(BookKeeperRetentionBlocker.ROOT_CHANGED_OR_INELIGIBLE)));
        });
    }

    private CompletableFuture<BookKeeperLedgerGcResult> advanceMarked(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            Duration timeout) {
        if (clock.millis() < root.value().deleteNotBeforeMillis()) {
            return CompletableFuture.completedFuture(
                    BookKeeperLedgerGcResult.of(BookKeeperLedgerGcAction.WAITING_DRAIN, root));
        }
        return gate.evaluateMarked(root, timeout).thenCompose(evaluation -> {
            if (evaluation.candidate().isEmpty()
                    || !evaluation.candidate().orElseThrow().referenceSetSha256().value()
                            .equals(root.value().referenceSetSha256())) {
                BookKeeperLedgerRootRecord sealed = root(
                        root.value(), BookKeeperLedgerLifecycle.SEALED, root.value().lifecycleEpoch() + 1,
                        "", "", 0, 0, 0, 0, 0, "");
                return metadata.compareAndSetRoot(cluster, sealed, root.metadataVersion())
                        .thenApply(value -> BookKeeperLedgerGcResult.of(BookKeeperLedgerGcAction.UNMARKED, value));
            }
            BookKeeperLedgerRootRecord deleting = root(
                    root.value(), BookKeeperLedgerLifecycle.DELETING, root.value().lifecycleEpoch() + 1,
                    root.value().gcAttemptId(), root.value().referenceSetSha256(),
                    root.value().markedAtMillis(), root.value().deleteNotBeforeMillis(), clock.millis(), 0, 0, "");
            return metadata.compareAndSetRoot(cluster, deleting, root.metadataVersion())
                    .thenApply(value -> BookKeeperLedgerGcResult.of(BookKeeperLedgerGcAction.DELETING, value));
        });
    }

    private CompletableFuture<BookKeeperLedgerGcResult> advanceDeleting(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            Duration timeout) {
        if (root.value().firstAbsentAtMillis() > 0) {
            if (clock.millis() < add(root.value().firstAbsentAtMillis(), gcConfiguration.lateCreateAuditGrace())) {
                return CompletableFuture.completedFuture(
                        BookKeeperLedgerGcResult.of(BookKeeperLedgerGcAction.WAITING_SECOND_ABSENCE, root));
            }
            return requireDeletionAuthority(root, timeout)
                    .thenCompose(ignored -> observeMetadata(root, timeout))
                    .thenCompose(metadataValue -> {
                        if (metadataValue.isPresent()) {
                            try {
                                requireProviderExact(root.value(), metadataValue.orElseThrow());
                            } catch (RuntimeException mismatch) {
                                return quarantine(root, "foreign ledger appeared after first absence");
                            }
                            return deletePresent(root, timeout);
                        }
                        long deletedAt = Math.max(clock.millis(), root.value().firstAbsentAtMillis() + 1);
                        BookKeeperLedgerRootRecord deleted = root(
                                root.value(), BookKeeperLedgerLifecycle.DELETED,
                                root.value().lifecycleEpoch() + 1, root.value().gcAttemptId(),
                                root.value().referenceSetSha256(), root.value().markedAtMillis(),
                                root.value().deleteNotBeforeMillis(), root.value().deleteStartedAtMillis(),
                                root.value().firstAbsentAtMillis(), deletedAt, "");
                        return metadata.compareAndSetRoot(cluster, deleted, root.metadataVersion())
                                .thenApply(value -> BookKeeperLedgerGcResult.of(BookKeeperLedgerGcAction.DELETED, value));
                    });
        }
        return requireDeletionAuthority(root, timeout)
                .thenCompose(ignored -> observeMetadata(root, timeout))
                .thenCompose(provider -> {
                    if (provider.isEmpty()) return recordFirstAbsence(root);
                    try {
                        requireProviderExact(root.value(), provider.orElseThrow());
                    } catch (RuntimeException mismatch) {
                        return quarantine(root, "provider metadata changed before physical delete");
                    }
                    return deletePresent(root, timeout);
                });
    }

    private CompletableFuture<BookKeeperLedgerGcResult> deletePresent(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            Duration timeout) {
        return gate.evaluateDeleting(root, timeout).handle((evaluation, failure) -> {
            if (failure == null) return CompletableFuture.completedFuture(evaluation);
            if (isNotFound(failure)) return CompletableFuture.<BookKeeperRetentionEvaluation>completedFuture(null);
            return CompletableFuture.<BookKeeperRetentionEvaluation>failedFuture(unwrap(failure));
        }).thenCompose(java.util.function.Function.identity()).thenCompose(evaluation -> {
            if (evaluation == null) return recordFirstAbsence(root);
            if (evaluation.candidate().isEmpty()) {
                if (evaluation.blockers().contains(BookKeeperRetentionBlocker.PROVIDER_METADATA_MISMATCH)) {
                    return quarantine(root, "provider metadata changed before physical delete");
                }
                return CompletableFuture.completedFuture(BookKeeperLedgerGcResult.blocked(root, evaluation.blockers()));
            }
            if (!evaluation.candidate().orElseThrow().referenceSetSha256().value()
                    .equals(root.value().referenceSetSha256())) {
                return CompletableFuture.completedFuture(BookKeeperLedgerGcResult.blocked(
                        root, java.util.Set.of(BookKeeperRetentionBlocker.PROTECTION_PRESENT)));
            }
            if (!deletePermits.tryAcquire()) {
                return CompletableFuture.completedFuture(BookKeeperLedgerGcResult.of(
                        BookKeeperLedgerGcAction.DELETE_RETRY_REQUIRED, root));
            }
            CompletableFuture<Void> deletion;
            try {
                deletion = client.delete(root.value().ledgerId(), new BookKeeperOperationDeadline(min(
                        timeout, configuration.deleteTimeout())));
            } catch (Throwable failure) {
                deletion = CompletableFuture.failedFuture(failure);
            }
            return deletion.handle((ignored, deleteFailure) -> {
                        deletePermits.release();
                        return observeMetadata(root, timeout);
                    })
                    .thenCompose(java.util.function.Function.identity())
                    .thenCompose(provider -> {
                        if (provider.isPresent()) {
                            try {
                                requireProviderExact(root.value(), provider.orElseThrow());
                                return CompletableFuture.completedFuture(BookKeeperLedgerGcResult.of(
                                        BookKeeperLedgerGcAction.DELETE_RETRY_REQUIRED, root));
                            } catch (RuntimeException mismatch) {
                                return quarantine(root, "provider metadata changed after delete response loss");
                            }
                        }
                        return recordFirstAbsence(root);
                    });
        });
    }

    private CompletableFuture<BookKeeperLedgerGcResult> recordFirstAbsence(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root) {
        long firstAbsent = Math.max(
                Math.max(clock.millis(), root.value().deleteStartedAtMillis()),
                root.value().firstAbsentAtMillis() + 1);
        BookKeeperLedgerRootRecord absent = root(
                root.value(), BookKeeperLedgerLifecycle.DELETING,
                root.value().lifecycleEpoch() + 1, root.value().gcAttemptId(),
                root.value().referenceSetSha256(), root.value().markedAtMillis(),
                root.value().deleteNotBeforeMillis(), root.value().deleteStartedAtMillis(),
                firstAbsent, 0, "");
        return metadata.compareAndSetRoot(cluster, absent, root.metadataVersion())
                .thenApply(value -> BookKeeperLedgerGcResult.of(
                        BookKeeperLedgerGcAction.FIRST_ABSENCE_RECORDED, value));
    }

    private CompletableFuture<Void> requireDeletionAuthority(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            Duration timeout) {
        BookKeeperOperationDeadline deadline = new BookKeeperOperationDeadline(min(
                Objects.requireNonNull(timeout, "timeout"),
                configuration.operationTimeout()));
        var namespace = deadline.bound(
                namespaceVerifier.requireActive(configuration, deadline.remaining()));
        var activation = deadline.bound(
                activationVerifier.requireActive(deadline.remaining()));
        return CompletableFuture.allOf(namespace, activation).thenApply(ignored -> {
            activation.join().requireExact(configuration, namespace.join());
            if (!namespace.join().ledgerIdNamespaceSha256().value()
                    .equals(root.value().ledgerIdNamespaceSha256())) {
                throw invariant("BookKeeper namespace authority changed during physical deletion");
            }
            return null;
        });
    }

    private CompletableFuture<Optional<LedgerMetadata>> observeMetadata(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            Duration timeout) {
        CompletableFuture<LedgerMetadata> future;
        try {
            future = client.metadata(root.value().ledgerId(), new BookKeeperOperationDeadline(timeout));
        } catch (Throwable failure) {
            future = CompletableFuture.failedFuture(failure);
        }
        return future.handle((value, failure) -> {
            if (failure == null) return Optional.of(value);
            if (isNotFound(failure)) return Optional.empty();
            throw new java.util.concurrent.CompletionException(unwrap(failure));
        });
    }

    private CompletableFuture<BookKeeperLedgerGcResult> quarantine(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            String reason) {
        BookKeeperLedgerRootRecord quarantined = root(
                root.value(), BookKeeperLedgerLifecycle.QUARANTINED,
                root.value().lifecycleEpoch() + 1, "", "", 0, 0, 0, 0, 0, reason);
        return metadata.compareAndSetRoot(cluster, quarantined, root.metadataVersion())
                .thenApply(value -> BookKeeperLedgerGcResult.of(BookKeeperLedgerGcAction.QUARANTINED, value));
    }

    private CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerRootRecord>> reloadExact(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> observed) {
        return metadata.getRoot(cluster, configuration.providerScopeSha256(), observed.value().ledgerId())
                .thenApply(optional -> {
                    var current = optional.orElseThrow(() -> invariant("BookKeeper GC root disappeared"));
                    if (current.metadataVersion() != observed.metadataVersion()
                            || !current.durableValueSha256().equals(observed.durableValueSha256())) {
                        throw invariant("BookKeeper GC root changed before convergence pass");
                    }
                    return current;
                });
    }

    private static boolean sameCandidate(
            BookKeeperLedgerRetirementCandidate left,
            BookKeeperLedgerRetirementCandidate right) {
        return left.root().metadataVersion() == right.root().metadataVersion()
                && left.root().durableValueSha256().equals(right.root().durableValueSha256())
                && left.referenceSetSha256().equals(right.referenceSetSha256())
                && left.activationProof().activationMetadataVersion()
                        == right.activationProof().activationMetadataVersion()
                && left.activationProof().activationRecordSha256()
                        .equals(right.activationProof().activationRecordSha256());
    }

    private void requireProviderExact(BookKeeperLedgerRootRecord root, LedgerMetadata provider) {
        BookKeeperLedgerCustomMetadata.fromRoot(cluster, configuration, root)
                .requireExactImmutableLedgerMetadata(root.ledgerId(), configuration, provider);
        if (!provider.isClosed()
                || provider.getLastEntryId() != root.sealedLastEntryId()
                || provider.getLength() != root.sealedLength()) {
            throw invariant("BookKeeper provider metadata does not match sealed root");
        }
    }

    private static BookKeeperLedgerRootRecord root(
            BookKeeperLedgerRootRecord before,
            BookKeeperLedgerLifecycle lifecycle,
            long lifecycleEpoch,
            String gcAttemptId,
            String referenceSetSha256,
            long markedAtMillis,
            long deleteNotBeforeMillis,
            long deleteStartedAtMillis,
            long firstAbsentAtMillis,
            long deletedAtMillis,
            String stateReason) {
        return new BookKeeperLedgerRootRecord(
                before.schemaVersion(), before.ledgerIdentitySha256(), before.clusterAlias(),
                before.providerScopeSha256(), before.ledgerId(), before.streamId(), before.segmentSequence(),
                before.allocationId(), before.allocationSlot(), before.configurationBindingSha256(),
                before.ledgerIdNamespaceSha256(), before.lateCreateHazard(), before.writerId(),
                before.writerRunIdHash(), before.appendSessionEpoch(), before.fencingTokenHash(),
                before.ensembleSize(), before.writeQuorumSize(), before.ackQuorumSize(), before.digestType(),
                before.customMetadataSha256(), lifecycle, lifecycleEpoch, before.createdAtMillis(),
                before.activatedAtMillis(), before.sealStartedAtMillis(), before.sealedAtMillis(),
                before.sealedLastEntryId(), before.sealedLength(), before.sealReason(), gcAttemptId,
                referenceSetSha256, markedAtMillis, deleteNotBeforeMillis, deleteStartedAtMillis,
                firstAbsentAtMillis, deletedAtMillis, stateReason, 0);
    }

    private static String attemptId(BookKeeperLedgerRetirementCandidate candidate) {
        MessageDigest digest = digest();
        frame(digest, "NBKGC1");
        frame(digest, Long.toString(candidate.root().value().ledgerId()));
        frame(digest, Long.toString(candidate.root().metadataVersion()));
        frame(digest, candidate.root().durableValueSha256().value());
        frame(digest, candidate.referenceSetSha256().value());
        frame(digest, candidate.activationProof().activationRecordSha256().value());
        return HexFormat.of().formatHex(digest.digest());
    }

    private Duration bounded(Duration timeout) {
        return min(Objects.requireNonNull(timeout, "timeout"), configuration.operationTimeout());
    }

    private static long add(long millis, Duration duration) {
        try {
            return Math.addExact(millis, duration.toMillis());
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    private static Duration min(Duration left, Duration right) {
        return left.compareTo(right) <= 0 ? left : right;
    }

    private static boolean isNotFound(Throwable failure) {
        Throwable current = unwrap(failure);
        return current instanceof NereusException nereus
                && nereus.code() == ErrorCode.PRIMARY_WAL_TARGET_NOT_FOUND;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof java.util.concurrent.CompletionException
                        || current instanceof java.util.concurrent.ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static MessageDigest digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static void frame(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static String text(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) throw new IllegalArgumentException(name + " cannot be blank");
        return value;
    }
}
