/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.StreamId;
import com.nereusstream.materialization.CommittedGenerationRetirementAuthority;
import com.nereusstream.materialization.CommittedGenerationRetirementProof;
import com.nereusstream.metadata.oxia.BookKeeperVersionedValue;
import com.nereusstream.metadata.oxia.BookKeeperLedgerMetadataStore;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerProtectionRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperProtectionType;
import com.nereusstream.metadata.oxia.records.ProtectionLifecycle;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Adds exact healthy higher-generation authority to the common trim/abandoned BookKeeper retirement facts. */
public final class BookKeeperAsyncObjectRetirementAuthority implements BookKeeperWalRetirementAuthority {
    private final String cluster;
    private final BookKeeperWalConfiguration configuration;
    private final BookKeeperWalOnlyRetirementAuthority common;
    private final CommittedGenerationRetirementAuthority replacements;
    private final BookKeeperLedgerMetadataStore ledgerMetadata;

    public BookKeeperAsyncObjectRetirementAuthority(
            String cluster,
            BookKeeperWalConfiguration configuration,
            BookKeeperWalOnlyRetirementAuthority common,
            CommittedGenerationRetirementAuthority replacements,
            BookKeeperLedgerMetadataStore ledgerMetadata) {
        this.cluster = text(cluster, "cluster");
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.common = Objects.requireNonNull(common, "common");
        this.replacements = Objects.requireNonNull(replacements, "replacements");
        this.ledgerMetadata = Objects.requireNonNull(ledgerMetadata, "ledgerMetadata");
    }

    @Override
    public CompletableFuture<Optional<BookKeeperProtectionRetirementProof>> proveLogicalTrim(
            BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> protection,
            Duration timeout) {
        return common.proveLogicalTrim(protection, timeout);
    }

    @Override
    public CompletableFuture<Optional<BookKeeperProtectionRetirementProof>> proveAbandonedAppend(
            BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> protection,
            Duration timeout) {
        return common.proveAbandonedAppend(protection, timeout);
    }

    @Override
    public CompletableFuture<Optional<BookKeeperProtectionRetirementProof>> proveHealthyHigherGeneration(
            BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> protection,
            Duration timeout) {
        BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> exact =
                Objects.requireNonNull(protection, "protection");
        Objects.requireNonNull(timeout, "timeout");
        if (!eligible(exact.value())) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        StreamId stream = new StreamId(exact.value().streamId());
        OffsetRange range = new OffsetRange(exact.value().offsetStart(), exact.value().offsetEnd());
        return sourceCommitVersion(exact.value()).thenCompose(commitVersion ->
                replacements.proveRetirement(stream, range, commitVersion)
                        .thenApply(optional -> optional.map(replacement -> proof(exact, replacement))));
    }

    @Override
    public CompletableFuture<Void> revalidate(
            BookKeeperProtectionRetirementProof proof,
            BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> protection,
            Duration timeout) {
        BookKeeperProtectionRetirementProof expected = Objects.requireNonNull(proof, "proof");
        BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> current =
                Objects.requireNonNull(protection, "protection");
        Objects.requireNonNull(timeout, "timeout");
        if (expected.reason() != BookKeeperProtectionRetirementProof.Reason.HEALTHY_HIGHER_GENERATION) {
            return common.revalidate(expected, current, timeout);
        }
        if (!retirementType(current.value())
                || current.value().lifecycle() != ProtectionLifecycle.ACTIVE
                        && current.value().lifecycle() != ProtectionLifecycle.RETIRED) {
            return CompletableFuture.failedFuture(condition(
                    "BookKeeper protection is not eligible for higher-generation retirement"));
        }
        return sourceCommitVersion(current.value()).thenCompose(commitVersion ->
                replacements.proveExactRetirement(
                        new StreamId(current.value().streamId()),
                        new OffsetRange(current.value().offsetStart(), current.value().offsetEnd()),
                        commitVersion,
                        expected.authorityKey(),
                        expected.authorityMetadataVersion(),
                        expected.authorityRecordSha256()))
                .thenAccept(optional -> {
                    if (optional.isEmpty()) {
                        throw condition("healthy higher Object generation authority changed");
                    }
                });
    }

    private CompletableFuture<Long> sourceCommitVersion(BookKeeperLedgerProtectionRecord protection) {
        if (protection.commitVersion() > 0) {
            return CompletableFuture.completedFuture(protection.commitVersion());
        }
        return ledgerMetadata.getProtection(
                        cluster,
                        configuration.providerScopeSha256(),
                        protection.ledgerId(),
                        protection.ledgerRangeSlot(),
                        1)
                .thenApply(optional -> {
                    BookKeeperLedgerProtectionRecord anchor = optional.orElseThrow(() -> condition(
                            "BookKeeper visible-generation anchor is absent for source retirement"))
                            .value();
                    if (anchor.protectionType() != BookKeeperProtectionType.VISIBLE_GENERATION
                            || anchor.lifecycle() != ProtectionLifecycle.ACTIVE
                                    && anchor.lifecycle() != ProtectionLifecycle.RETIRED
                            || anchor.commitVersion() <= 0
                            || !sameRange(protection, anchor)) {
                        throw condition(
                                "BookKeeper visible-generation anchor changed for source retirement");
                    }
                    return anchor.commitVersion();
                });
    }

    private static boolean sameRange(
            BookKeeperLedgerProtectionRecord left,
            BookKeeperLedgerProtectionRecord right) {
        return left.ledgerIdentitySha256().equals(right.ledgerIdentitySha256())
                && left.clusterAlias().equals(right.clusterAlias())
                && left.ledgerId() == right.ledgerId()
                && left.ledgerRangeSlot() == right.ledgerRangeSlot()
                && left.firstEntryId() == right.firstEntryId()
                && left.entryCount() == right.entryCount()
                && left.rangeChecksumSha256().equals(right.rangeChecksumSha256())
                && left.streamId().equals(right.streamId())
                && left.offsetStart() == right.offsetStart()
                && left.offsetEnd() == right.offsetEnd();
    }

    private static BookKeeperProtectionRetirementProof proof(
            BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> protection,
            CommittedGenerationRetirementProof replacement) {
        BookKeeperLedgerProtectionRecord value = protection.value();
        return new BookKeeperProtectionRetirementProof(
                protection.key(),
                protection.metadataVersion(),
                protection.durableValueSha256(),
                value.ownerKey(),
                value.ownerMetadataVersion(),
                new Checksum(ChecksumType.SHA256, value.ownerIdentitySha256()),
                replacement.indexKey(),
                replacement.indexMetadataVersion(),
                replacement.indexSha256(),
                BookKeeperProtectionRetirementProof.Reason.HEALTHY_HIGHER_GENERATION);
    }

    private static boolean eligible(BookKeeperLedgerProtectionRecord value) {
        return value.lifecycle() == ProtectionLifecycle.ACTIVE && retirementType(value);
    }

    private static boolean retirementType(BookKeeperLedgerProtectionRecord value) {
        BookKeeperProtectionType type = value.protectionType();
        return type == BookKeeperProtectionType.REACHABLE_APPEND
                || type == BookKeeperProtectionType.VISIBLE_GENERATION
                || type == BookKeeperProtectionType.APPEND_RECOVERY;
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
