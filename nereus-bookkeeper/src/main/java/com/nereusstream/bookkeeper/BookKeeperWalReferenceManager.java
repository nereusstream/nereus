/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.BookKeeperLedgerMetadataStore;
import com.nereusstream.metadata.oxia.BookKeeperVersionedValue;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerProtectionRecord;
import com.nereusstream.metadata.oxia.records.ProtectionLifecycle;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Converts an exact ACTIVE protection into a permanent RETIRED inventory tombstone after owner retirement. */
public final class BookKeeperWalReferenceManager {
    private final String cluster;
    private final BookKeeperWalConfiguration configuration;
    private final BookKeeperLedgerMetadataStore metadata;
    private final BookKeeperProtectionRetirementVerifier verifier;

    public BookKeeperWalReferenceManager(
            String cluster,
            BookKeeperWalConfiguration configuration,
            BookKeeperLedgerMetadataStore metadata,
            BookKeeperProtectionRetirementVerifier verifier) {
        this.cluster = text(cluster, "cluster");
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.verifier = Objects.requireNonNull(verifier, "verifier");
    }

    public CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>> retire(
            long ledgerId,
            int rangeSlot,
            int protectionSlot,
            BookKeeperProtectionRetirementProof proof,
            Duration timeout) {
        BookKeeperProtectionRetirementProof exact = Objects.requireNonNull(proof, "proof");
        BookKeeperOperationDeadline deadline = new BookKeeperOperationDeadline(min(
                Objects.requireNonNull(timeout, "timeout"), configuration.operationTimeout()));
        return deadline.bound(metadata.getProtection(
                        cluster,
                        configuration.providerScopeSha256(),
                        ledgerId,
                        rangeSlot,
                        protectionSlot))
                .thenApply(optional -> optional.orElseThrow(
                        () -> invariant("BookKeeper protection disappeared before retirement")))
                .thenCompose(current -> {
                    requireProof(current, exact);
                    if (current.value().lifecycle() == ProtectionLifecycle.RESERVED
                            && exact.reason() != BookKeeperProtectionRetirementProof.Reason.ABANDONED_APPEND) {
                        return CompletableFuture.failedFuture(invariant(
                                "BookKeeper RESERVED protection requires exact abandoned-append authority"));
                    }
                    if (current.value().lifecycle() == ProtectionLifecycle.RETIRED) {
                        return verifier.revalidate(exact, current, deadline.remaining()).thenApply(ignored -> current);
                    }
                    return verifier.revalidate(exact, current, deadline.remaining())
                            .thenCompose(ignored -> metadata.compareAndSetProtection(
                                    cluster,
                                    configuration.providerScopeSha256(),
                                    retired(current.value(), exact),
                                    current.metadataVersion()))
                            .thenCompose(retired -> verifier.revalidate(exact, retired, deadline.remaining())
                                    .thenApply(ignored -> retired));
                });
    }

    private static void requireProof(
            BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> current,
            BookKeeperProtectionRetirementProof proof) {
        BookKeeperLedgerProtectionRecord value = current.value();
        if (!current.key().equals(proof.protectionKey())
                || (value.lifecycle() != ProtectionLifecycle.RETIRED
                        && current.metadataVersion() != proof.protectionMetadataVersion())
                || (value.lifecycle() != ProtectionLifecycle.RETIRED
                        && !current.durableValueSha256().equals(proof.protectionRecordSha256()))
                || (value.lifecycle() != ProtectionLifecycle.RESERVED
                        && !value.ownerKey().equals(proof.ownerKey()))
                || (value.lifecycle() != ProtectionLifecycle.RESERVED
                        && value.ownerMetadataVersion() != proof.ownerMetadataVersion())
                || (value.lifecycle() != ProtectionLifecycle.RESERVED
                        && !value.ownerIdentitySha256().equals(proof.ownerIdentitySha256().value()))) {
            throw invariant("BookKeeper retirement proof does not match the exact protection/owner");
        }
    }

    private static BookKeeperLedgerProtectionRecord retired(
            BookKeeperLedgerProtectionRecord before,
            BookKeeperProtectionRetirementProof proof) {
        String ownerKey = before.lifecycle() == ProtectionLifecycle.RESERVED
                ? proof.ownerKey() : before.ownerKey();
        long ownerVersion = before.lifecycle() == ProtectionLifecycle.RESERVED
                ? proof.ownerMetadataVersion() : before.ownerMetadataVersion();
        String ownerIdentity = before.lifecycle() == ProtectionLifecycle.RESERVED
                ? proof.ownerIdentitySha256().value() : before.ownerIdentitySha256();
        return new BookKeeperLedgerProtectionRecord(
                before.schemaVersion(), before.ledgerIdentitySha256(), before.clusterAlias(), before.ledgerId(),
                before.rootLifecycleEpoch(), before.ledgerRangeSlot(), before.protectionSlot(),
                before.protectionTypeId(), before.referenceId(), before.firstEntryId(), before.entryCount(),
                before.rangeChecksumSha256(), before.streamId(), before.offsetStart(), before.offsetEnd(),
                before.commitVersion(), ownerKey, ownerVersion,
                ownerIdentity, ProtectionLifecycle.RETIRED,
                before.createdAtMillis(), before.expiresAtMillis(), 0);
    }

    private static Duration min(Duration left, Duration right) {
        return left.compareTo(right) <= 0 ? left : right;
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
