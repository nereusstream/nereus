/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.metadata.oxia.BookKeeperLedgerMetadataStore;
import com.nereusstream.metadata.oxia.BookKeeperScanToken;
import com.nereusstream.metadata.oxia.BookKeeperVersionedValue;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

/**
 * Complete, binding-scoped root scanner that retires references before advancing whole-ledger GC.
 *
 * <p>The scanner is deliberately sequential. It owns no scheduler and consumes one bounded metadata/provider
 * operation at a time; provider delete concurrency remains enforced by {@link BookKeeperLedgerRetentionManager}.
 */
public final class BookKeeperLedgerRetentionScanner {
    public static final int ROOT_SHARDS = 256;

    private final String cluster;
    private final BookKeeperWalConfiguration configuration;
    private final BookKeeperLedgerGcConfiguration gcConfiguration;
    private final String ledgerIdNamespaceSha256;
    private final BookKeeperLedgerMetadataStore metadata;
    private final BiFunction<BookKeeperVersionedValue<BookKeeperLedgerRootRecord>, Duration, CompletableFuture<Void>>
            materializationTrigger;
    private final BiFunction<
                    BookKeeperVersionedValue<BookKeeperLedgerRootRecord>,
                    Duration,
                    CompletableFuture<BookKeeperWalReferenceRetirementResult>>
            referenceRetirement;
    private final BiFunction<
                    BookKeeperVersionedValue<BookKeeperLedgerRootRecord>,
                    Duration,
                    CompletableFuture<BookKeeperRetentionEvaluation>>
            retentionEvaluation;
    private final BiFunction<
                    BookKeeperLedgerRetirementCandidate,
                    Duration,
                    CompletableFuture<BookKeeperLedgerGcResult>>
            mark;
    private final BiFunction<
                    BookKeeperVersionedValue<BookKeeperLedgerRootRecord>,
                    Duration,
                    CompletableFuture<BookKeeperLedgerGcResult>>
            converge;

    public BookKeeperLedgerRetentionScanner(
            String cluster,
            BookKeeperWalConfiguration configuration,
            BookKeeperLedgerGcConfiguration gcConfiguration,
            String ledgerIdNamespaceSha256,
            BookKeeperLedgerMetadataStore metadata,
            BookKeeperSealedLedgerMaterializationTrigger materializationTrigger,
            BookKeeperWalOnlyReferenceRetirementCoordinator referenceRetirement,
            BookKeeperWalRetentionGate retentionGate,
            BookKeeperLedgerRetentionManager retentionManager) {
        this(
                cluster,
                configuration,
                gcConfiguration,
                ledgerIdNamespaceSha256,
                metadata,
                Objects.requireNonNull(materializationTrigger, "materializationTrigger")::trigger,
                Objects.requireNonNull(referenceRetirement, "referenceRetirement")::retireEligible,
                Objects.requireNonNull(retentionGate, "retentionGate")::evaluate,
                Objects.requireNonNull(retentionManager, "retentionManager")::mark,
                retentionManager::converge);
    }

    BookKeeperLedgerRetentionScanner(
            String cluster,
            BookKeeperWalConfiguration configuration,
            BookKeeperLedgerGcConfiguration gcConfiguration,
            String ledgerIdNamespaceSha256,
            BookKeeperLedgerMetadataStore metadata,
            BiFunction<BookKeeperVersionedValue<BookKeeperLedgerRootRecord>, Duration, CompletableFuture<Void>>
                    materializationTrigger,
            BiFunction<
                            BookKeeperVersionedValue<BookKeeperLedgerRootRecord>,
                            Duration,
                            CompletableFuture<BookKeeperWalReferenceRetirementResult>>
                    referenceRetirement,
            BiFunction<
                            BookKeeperVersionedValue<BookKeeperLedgerRootRecord>,
                            Duration,
                            CompletableFuture<BookKeeperRetentionEvaluation>>
                    retentionEvaluation,
            BiFunction<
                            BookKeeperLedgerRetirementCandidate,
                            Duration,
                            CompletableFuture<BookKeeperLedgerGcResult>>
                    mark,
            BiFunction<
                            BookKeeperVersionedValue<BookKeeperLedgerRootRecord>,
                            Duration,
                            CompletableFuture<BookKeeperLedgerGcResult>>
                    converge) {
        this.cluster = text(cluster, "cluster");
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.gcConfiguration = Objects.requireNonNull(gcConfiguration, "gcConfiguration");
        gcConfiguration.validateAgainst(configuration);
        this.ledgerIdNamespaceSha256 = BookKeeperWalConfiguration.sha256(
                ledgerIdNamespaceSha256, "ledgerIdNamespaceSha256");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.materializationTrigger = Objects.requireNonNull(materializationTrigger, "materializationTrigger");
        this.referenceRetirement = Objects.requireNonNull(referenceRetirement, "referenceRetirement");
        this.retentionEvaluation = Objects.requireNonNull(retentionEvaluation, "retentionEvaluation");
        this.mark = Objects.requireNonNull(mark, "mark");
        this.converge = Objects.requireNonNull(converge, "converge");
    }

    public CompletableFuture<BookKeeperLedgerRetentionScanResult> scanOnce() {
        if (!gcConfiguration.enabled() || gcConfiguration.dryRun()) {
            return CompletableFuture.completedFuture(new BookKeeperLedgerRetentionScanResult(
                    false, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        }
        Accumulator accumulator = new Accumulator();
        return scanShard(0, Optional.empty(), accumulator)
                .thenApply(ignored -> accumulator.result());
    }

    private CompletableFuture<Void> scanShard(
            int shard,
            Optional<BookKeeperScanToken> continuation,
            Accumulator accumulator) {
        if (shard >= ROOT_SHARDS) {
            return CompletableFuture.completedFuture(null);
        }
        BookKeeperOperationDeadline deadline =
                new BookKeeperOperationDeadline(configuration.operationTimeout());
        return deadline.bound(metadata.scanRoots(
                        cluster,
                        shard,
                        continuation,
                        Math.min(configuration.retentionPageSize(), 1_024)))
                .thenCompose(page -> processPage(page.values(), 0, accumulator)
                        .thenCompose(ignored -> {
                            if (page.continuation().isPresent()) {
                                return scanShard(shard, page.continuation(), accumulator);
                            }
                            accumulator.shardsScanned++;
                            return scanShard(shard + 1, Optional.empty(), accumulator);
                        }));
    }

    private CompletableFuture<Void> processPage(
            List<BookKeeperVersionedValue<BookKeeperLedgerRootRecord>> roots,
            int index,
            Accumulator accumulator) {
        CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
        for (int current = index; current < roots.size(); current++) {
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root = roots.get(current);
            chain = chain.thenCompose(ignored -> processRoot(root, accumulator));
        }
        return chain;
    }

    private CompletableFuture<Void> processRoot(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            Accumulator accumulator) {
        accumulator.rootsScanned++;
        if (!matchesBinding(root.value())) {
            return CompletableFuture.completedFuture(null);
        }
        accumulator.matchingRoots++;
        CompletableFuture<Void> processed = switch (root.value().lifecycle()) {
            case SEALED -> processSealed(root, accumulator);
            case MARKED, DELETING -> processInFlight(root, accumulator);
            default -> CompletableFuture.completedFuture(null);
        };
        return processed.handle((ignored, failure) -> {
            if (failure != null) {
                accumulator.rootsFailed++;
            }
            return null;
        });
    }

    private CompletableFuture<Void> processSealed(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            Accumulator accumulator) {
        accumulator.sealedRoots++;
        CompletableFuture<Void> trigger = accumulator.materializationAttempted
                ? CompletableFuture.completedFuture(null)
                : triggerMaterialization(root, accumulator);
        return trigger.thenCompose(ignored -> referenceRetirement
                        .apply(root, configuration.operationTimeout()))
                .thenCompose(retired -> {
                    accumulator.protectionsRetired = Math.addExact(
                            accumulator.protectionsRetired, retired.newlyRetiredProtections());
                    return retentionEvaluation.apply(root, configuration.operationTimeout());
                })
                .thenCompose(evaluation -> {
                    if (evaluation.candidate().isEmpty()) {
                        accumulator.rootsBlocked++;
                        return CompletableFuture.completedFuture(null);
                    }
                    return mark.apply(
                                    evaluation.candidate().orElseThrow(),
                                    configuration.operationTimeout())
                            .thenAccept(result -> account(result, accumulator));
                });
    }

    private CompletableFuture<Void> triggerMaterialization(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            Accumulator accumulator) {
        accumulator.materializationAttempted = true;
        accumulator.materializationTriggers++;
        return materializationTrigger.apply(root, configuration.operationTimeout())
                .handle((ignored, failure) -> {
                    if (failure != null) {
                        accumulator.materializationTriggerFailures++;
                    }
                    return null;
                });
    }

    private CompletableFuture<Void> processInFlight(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            Accumulator accumulator) {
        accumulator.inFlightRoots++;
        return converge.apply(root, configuration.operationTimeout())
                .thenAccept(result -> account(result, accumulator));
    }

    private boolean matchesBinding(BookKeeperLedgerRootRecord root) {
        return root.clusterAlias().equals(configuration.clusterAlias())
                && root.providerScopeSha256().equals(configuration.providerScopeSha256())
                && root.configurationBindingSha256()
                        .equals(configuration.configurationBindingSha256().value())
                && root.ledgerIdNamespaceSha256().equals(ledgerIdNamespaceSha256);
    }

    private static void account(
            BookKeeperLedgerGcResult result,
            Accumulator accumulator) {
        switch (result.action()) {
            case BLOCKED -> accumulator.rootsBlocked++;
            case MARKED -> accumulator.rootsMarked++;
            default -> accumulator.rootsAdvanced++;
        }
    }

    private static final class Accumulator {
        private int shardsScanned;
        private int rootsScanned;
        private int matchingRoots;
        private int sealedRoots;
        private int inFlightRoots;
        private boolean materializationAttempted;
        private int materializationTriggers;
        private int materializationTriggerFailures;
        private int protectionsRetired;
        private int rootsMarked;
        private int rootsAdvanced;
        private int rootsBlocked;
        private int rootsFailed;

        private BookKeeperLedgerRetentionScanResult result() {
            return new BookKeeperLedgerRetentionScanResult(
                    true,
                    shardsScanned,
                    rootsScanned,
                    matchingRoots,
                    sealedRoots,
                    inFlightRoots,
                    materializationTriggers,
                    materializationTriggerFailures,
                    protectionsRetired,
                    rootsMarked,
                    rootsAdvanced,
                    rootsBlocked,
                    rootsFailed);
        }
    }

    private static String text(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }
}
