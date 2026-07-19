/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import com.nereusstream.materialization.MaterializationStreamTrigger;
import com.nereusstream.metadata.oxia.BookKeeperLedgerMetadataStore;
import com.nereusstream.metadata.oxia.BookKeeperVersionedValue;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Revalidates one exact SEALED ledger and hints the single F4 planner/service; it never creates tasks itself. */
public final class BookKeeperSealedLedgerMaterializationTrigger {
    private final String cluster;
    private final BookKeeperWalConfiguration configuration;
    private final BookKeeperLedgerMetadataStore metadata;
    private final MaterializationStreamTrigger materialization;

    public BookKeeperSealedLedgerMaterializationTrigger(
            String cluster,
            BookKeeperWalConfiguration configuration,
            BookKeeperLedgerMetadataStore metadata,
            MaterializationStreamTrigger materialization) {
        this.cluster = text(cluster, "cluster");
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.materialization = Objects.requireNonNull(materialization, "materialization");
    }

    public CompletableFuture<Void> trigger(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> sealedRoot,
            Duration timeout) {
        try {
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> expected =
                    Objects.requireNonNull(sealedRoot, "sealedRoot");
            requireSealed(expected);
            BookKeeperOperationDeadline deadline = new BookKeeperOperationDeadline(min(
                    Objects.requireNonNull(timeout, "timeout"), configuration.operationTimeout()));
            return reload(expected, deadline)
                    .thenCompose(ignored -> deadline.bound(materialization.trigger(
                            new StreamId(expected.value().streamId()))))
                    .thenCompose(ignored -> reload(expected, deadline))
                    .thenApply(ignored -> null);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private CompletableFuture<Void> reload(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> expected,
            BookKeeperOperationDeadline deadline) {
        return deadline.bound(metadata.getRoot(
                        cluster,
                        configuration.providerScopeSha256(),
                        expected.value().ledgerId()))
                .thenAccept(optional -> {
                    BookKeeperVersionedValue<BookKeeperLedgerRootRecord> current = optional.orElseThrow(
                            () -> condition("sealed BookKeeper ledger disappeared before materialization trigger"));
                    if (!current.equals(expected)) {
                        throw condition("sealed BookKeeper ledger changed during materialization trigger");
                    }
                    requireSealed(current);
                });
    }

    private static void requireSealed(BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root) {
        if (root.value().lifecycle() != BookKeeperLedgerLifecycle.SEALED) {
            throw new NereusException(
                    ErrorCode.METADATA_CONDITION_FAILED,
                    true,
                    "BookKeeper materialization trigger requires an exact SEALED root");
        }
    }

    private static Duration min(Duration left, Duration right) {
        return left.compareTo(right) <= 0 ? left : right;
    }

    private static NereusException condition(String message) {
        return new NereusException(ErrorCode.METADATA_CONDITION_FAILED, true, message);
    }

    private static String text(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }
}
