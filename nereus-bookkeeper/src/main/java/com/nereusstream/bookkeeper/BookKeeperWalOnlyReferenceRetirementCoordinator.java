/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.BookKeeperLedgerMetadataStore;
import com.nereusstream.metadata.oxia.BookKeeperScanToken;
import com.nereusstream.metadata.oxia.BookKeeperVersionedValue;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerProtectionRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;
import com.nereusstream.metadata.oxia.records.ProtectionLifecycle;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Retires exact BK_ONLY range protections from completed trim/abandoned facts without choosing a trim offset. */
public final class BookKeeperWalOnlyReferenceRetirementCoordinator {
    private final String cluster;
    private final BookKeeperWalConfiguration configuration;
    private final BookKeeperLedgerMetadataStore metadata;
    private final BookKeeperWalOnlyRetirementAuthority authority;
    private final BookKeeperWalReferenceManager references;

    public BookKeeperWalOnlyReferenceRetirementCoordinator(
            String cluster,
            BookKeeperWalConfiguration configuration,
            BookKeeperLedgerMetadataStore metadata,
            BookKeeperWalOnlyRetirementAuthority authority,
            BookKeeperWalReferenceManager references) {
        this.cluster = text(cluster, "cluster");
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.authority = Objects.requireNonNull(authority, "authority");
        this.references = Objects.requireNonNull(references, "references");
    }

    public CompletableFuture<BookKeeperWalReferenceRetirementResult> retireEligible(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            Duration timeout) {
        BookKeeperVersionedValue<BookKeeperLedgerRootRecord> exact = Objects.requireNonNull(root, "root");
        if (exact.value().lifecycle() != BookKeeperLedgerLifecycle.SEALED) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.METADATA_CONDITION_FAILED,
                    false,
                    "BookKeeper reference retirement requires an exact SEALED root"));
        }
        BookKeeperOperationDeadline deadline = new BookKeeperOperationDeadline(min(
                Objects.requireNonNull(timeout, "timeout"), configuration.operationTimeout()));
        return scan(exact.value().ledgerId(), deadline, Optional.empty(), new ArrayList<>())
                .thenCompose(values -> retire(values, deadline, 0, 0)
                        .thenCompose(newlyRetired -> scan(
                                        exact.value().ledgerId(), deadline, Optional.empty(), new ArrayList<>())
                                .thenApply(reloaded -> {
                                    int remaining = Math.toIntExact(reloaded.stream()
                                            .filter(value -> value.value().lifecycle()
                                                    != ProtectionLifecycle.RETIRED)
                                            .count());
                                    return new BookKeeperWalReferenceRetirementResult(
                                            reloaded.size(), newlyRetired, remaining);
                                })));
    }

    private CompletableFuture<Integer> retire(
            List<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>> protections,
            BookKeeperOperationDeadline deadline,
            int index,
            int newlyRetired) {
        if (index >= protections.size()) return CompletableFuture.completedFuture(newlyRetired);
        BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> protection = protections.get(index);
        if (protection.value().lifecycle() == ProtectionLifecycle.RETIRED) {
            return retire(protections, deadline, index + 1, newlyRetired);
        }
        CompletableFuture<Optional<BookKeeperProtectionRetirementProof>> proof =
                authority.proveAbandonedAppend(protection, deadline.remaining()).thenCompose(abandoned -> {
                    if (abandoned.isPresent()
                            || protection.value().lifecycle() == ProtectionLifecycle.RESERVED) {
                        return CompletableFuture.completedFuture(abandoned);
                    }
                    return authority.proveLogicalTrim(protection, deadline.remaining());
                });
        return proof.thenCompose(optional -> {
            if (optional.isEmpty()) return retire(protections, deadline, index + 1, newlyRetired);
            BookKeeperProtectionRetirementProof exactProof = optional.orElseThrow();
            return references.retire(
                            protection.value().ledgerId(),
                            protection.value().ledgerRangeSlot(),
                            protection.value().protectionSlot(),
                            exactProof,
                            deadline.remaining())
                    .thenCompose(ignored -> retire(protections, deadline, index + 1, newlyRetired + 1));
        });
    }

    private CompletableFuture<List<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>>> scan(
            long ledgerId,
            BookKeeperOperationDeadline deadline,
            Optional<BookKeeperScanToken> continuation,
            List<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>> values) {
        int maximum = Math.multiplyExact(
                configuration.maxAppendRangesPerLedger(), configuration.protectionSlotsPerRange());
        return deadline.bound(metadata.scanProtections(
                        cluster,
                        configuration.providerScopeSha256(),
                        ledgerId,
                        continuation,
                        Math.min(configuration.retentionPageSize(), 1_024)))
                .thenCompose(page -> {
                    values.addAll(page.values());
                    if (values.size() > maximum) {
                        return CompletableFuture.failedFuture(new NereusException(
                                ErrorCode.METADATA_LIMIT_EXCEEDED,
                                false,
                                "BookKeeper protection inventory exceeds its configured Cartesian bound"));
                    }
                    return page.continuation().isPresent()
                            ? scan(ledgerId, deadline, page.continuation(), values)
                            : CompletableFuture.completedFuture(List.copyOf(values));
                });
    }

    private static Duration min(Duration left, Duration right) {
        return left.compareTo(right) <= 0 ? left : right;
    }

    private static String text(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) throw new IllegalArgumentException(name + " cannot be blank");
        return value;
    }
}
