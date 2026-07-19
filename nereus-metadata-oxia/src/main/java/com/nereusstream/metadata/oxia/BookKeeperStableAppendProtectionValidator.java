/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.keys.DeterministicIds;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerProtectionRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperProtectionType;
import com.nereusstream.metadata.oxia.records.ProtectionLifecycle;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Re-reads and validates the exact BookKeeper root/protection proof before stream-head CAS. */
public final class BookKeeperStableAppendProtectionValidator {
    private final BookKeeperLedgerMetadataStore metadata;
    private final BookKeeperMetadataStoreConfig configuration;

    public BookKeeperStableAppendProtectionValidator(
            BookKeeperLedgerMetadataStore metadata,
            BookKeeperMetadataStoreConfig configuration) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.configuration = Objects.requireNonNull(configuration, "configuration");
    }

    public CompletableFuture<Void> validate(
            String cluster,
            PreparedStableAppend prepared,
            BookKeeperPhysicalReferenceProof proof) {
        Objects.requireNonNull(cluster, "cluster");
        Objects.requireNonNull(prepared, "prepared");
        Objects.requireNonNull(proof, "proof");
        if (!(prepared.request().readTarget() instanceof BookKeeperEntryRangeReadTarget target)) {
            return failed("BookKeeper proof was supplied for another primary target type");
        }
        String expectedReferenceId = "ra1-" + DeterministicIds.stableHashComponent(
                prepared.request().streamId().value()
                        + prepared.commitId()
                        + prepared.primaryTargetIdentitySha256().value());
        if (proof.purpose() != PhysicalReferencePurpose.REACHABLE_APPEND
                || !proof.targetIdentitySha256().equals(prepared.primaryTargetIdentitySha256())
                || !proof.referenceId().equals(expectedReferenceId)
                || proof.ledgerId() != target.ledgerId()
                || !proof.clusterAlias().equals(target.clusterAlias())
                || proof.protectionSlot() != 0) {
            return failed("BookKeeper stable append proof is non-canonical");
        }
        BookKeeperKeyspace keys = configuration.keyspace(cluster);
        String expectedLedgerIdentity = keys.ledgerIdentitySha256(
                proof.providerScopeSha256(), target.ledgerId());
        if (!proof.ledgerIdentitySha256().equals(expectedLedgerIdentity)) {
            return failed("BookKeeper proof ledger identity digest is non-canonical");
        }
        CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerRootRecord>> root =
                metadata.getRoot(cluster, proof.providerScopeSha256(), target.ledgerId())
                        .thenApply(optional -> optional.orElseThrow(() -> invariant(
                                "BookKeeper ledger root is absent before head commit")));
        CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>> protection =
                metadata.getProtection(
                                cluster,
                                proof.providerScopeSha256(),
                                target.ledgerId(),
                                proof.ledgerRangeSlot(),
                                proof.protectionSlot())
                        .thenApply(optional -> optional.orElseThrow(() -> invariant(
                                "BookKeeper REACHABLE_APPEND protection is absent")));
        return root.thenCombine(protection, (exactRoot, exactProtection) -> {
            validateRoot(prepared, proof, target, expectedLedgerIdentity, exactRoot);
            validateProtection(
                    prepared,
                    proof,
                    target,
                    expectedReferenceId,
                    expectedLedgerIdentity,
                    exactProtection);
            return null;
        });
    }

    private static void validateRoot(
            PreparedStableAppend prepared,
            BookKeeperPhysicalReferenceProof proof,
            BookKeeperEntryRangeReadTarget target,
            String expectedLedgerIdentity,
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root) {
        BookKeeperLedgerRootRecord value = root.value();
        if (root.metadataVersion() != proof.rootMetadataVersion()
                || !root.durableValueSha256().equals(proof.rootRecordSha256())
                || value.lifecycle() != BookKeeperLedgerLifecycle.ACTIVE
                || value.lifecycleEpoch() != proof.rootLifecycleEpoch()
                || value.ledgerId() != target.ledgerId()
                || !value.ledgerIdentitySha256().equals(expectedLedgerIdentity)
                || !value.providerScopeSha256().equals(proof.providerScopeSha256())
                || !value.clusterAlias().equals(target.clusterAlias())
                || !value.streamId().equals(prepared.request().streamId().value())) {
            throw invariant("BookKeeper ledger root changed before head commit");
        }
    }

    private static void validateProtection(
            PreparedStableAppend prepared,
            BookKeeperPhysicalReferenceProof proof,
            BookKeeperEntryRangeReadTarget target,
            String expectedReferenceId,
            String expectedLedgerIdentity,
            BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> protection) {
        BookKeeperLedgerProtectionRecord value = protection.value();
        long expectedEnd = Math.addExact(
                prepared.request().expectedStartOffset(), prepared.request().recordCount());
        if (protection.metadataVersion() != proof.protectionMetadataVersion()
                || !protection.durableValueSha256().equals(proof.protectionRecordSha256())
                || value.lifecycle() != ProtectionLifecycle.ACTIVE
                || value.protectionType() != BookKeeperProtectionType.REACHABLE_APPEND
                || !value.ledgerIdentitySha256().equals(expectedLedgerIdentity)
                || value.ledgerId() != target.ledgerId()
                || value.rootLifecycleEpoch() != proof.rootLifecycleEpoch()
                || value.ledgerRangeSlot() != proof.ledgerRangeSlot()
                || value.protectionSlot() != 0
                || !value.referenceId().equals(expectedReferenceId)
                || value.firstEntryId() != target.firstEntryId()
                || value.entryCount() != target.entryCount()
                || !value.rangeChecksumSha256().equals(target.rangeChecksum().value())
                || !value.streamId().equals(prepared.request().streamId().value())
                || value.offsetStart() != prepared.request().expectedStartOffset()
                || value.offsetEnd() != expectedEnd
                || !value.ownerKey().equals(prepared.commitKey())
                || value.ownerMetadataVersion() != prepared.commitMetadataVersion()
                || !value.ownerIdentitySha256().equals(prepared.commitRecordSha256().value())
                || value.expiresAtMillis() != 0) {
            throw invariant("BookKeeper REACHABLE_APPEND protection changed before head commit");
        }
    }

    private static CompletableFuture<Void> failed(String message) {
        return CompletableFuture.failedFuture(invariant(message));
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }
}
