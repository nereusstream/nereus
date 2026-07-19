/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import com.nereusstream.api.target.ReadTargetType;
import com.nereusstream.core.physical.ObjectProtectionOwner;
import com.nereusstream.materialization.MaterializationSourceProtection;
import com.nereusstream.materialization.MaterializationSourceProtectionAdapter;
import com.nereusstream.materialization.SourceGeneration;
import com.nereusstream.metadata.oxia.BookKeeperLedgerMetadataStore;
import com.nereusstream.metadata.oxia.BookKeeperMetadataConditionFailedException;
import com.nereusstream.metadata.oxia.BookKeeperScanToken;
import com.nereusstream.metadata.oxia.BookKeeperVersionedValue;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerProtectionRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperProtectionType;
import com.nereusstream.metadata.oxia.records.ProtectionLifecycle;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Fixed dynamic-slot MATERIALIZATION_SOURCE protection for BookKeeper generation-zero ranges. */
public final class BookKeeperMaterializationSourceProtectionAdapter
        implements MaterializationSourceProtectionAdapter<BookKeeperEntryRangeReadTarget> {
    private static final int FIRST_DYNAMIC_SLOT = 3;

    private final String cluster;
    private final BookKeeperWalConfiguration configuration;
    private final BookKeeperLedgerMetadataStore metadata;
    private final Clock clock;

    public BookKeeperMaterializationSourceProtectionAdapter(
            String cluster,
            BookKeeperWalConfiguration configuration,
            BookKeeperLedgerMetadataStore metadata,
            Clock clock) {
        this.cluster = requireText(cluster, "cluster");
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public ReadTargetType targetType() {
        return ReadTargetType.BOOKKEEPER_ENTRY_RANGE;
    }

    @Override
    public Class<BookKeeperEntryRangeReadTarget> targetClass() {
        return BookKeeperEntryRangeReadTarget.class;
    }

    @Override
    public CompletableFuture<MaterializationSourceProtection> acquireOrTransfer(
            StreamId streamId,
            SourceGeneration source,
            String referenceId,
            ObjectProtectionOwner owner,
            OwnerRevalidator ownerRevalidator) {
        try {
            SourceGeneration exactSource = Objects.requireNonNull(source, "source");
            String exactStream = Objects.requireNonNull(streamId, "streamId").value();
            BookKeeperEntryRangeReadTarget target = requireTarget(exactSource);
            String exactReference = requireText(referenceId, "referenceId");
            ObjectProtectionOwner exactOwner = Objects.requireNonNull(owner, "owner");
            OwnerRevalidator exactRevalidator = Objects.requireNonNull(
                    ownerRevalidator, "ownerRevalidator");
            BookKeeperOperationDeadline deadline = deadline();
            return loadRoot(target, deadline)
                    .thenCompose(root -> loadRangeAnchor(exactStream, exactSource, target, deadline)
                            .thenCompose(anchor -> acquireSlot(
                                    exactStream,
                                    exactSource,
                                    target,
                                    root,
                                    anchor.value().ledgerRangeSlot(),
                                    exactReference,
                                    exactOwner,
                                    exactRevalidator,
                                    firstSlot(exactReference),
                                    0,
                                    deadline)));
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    @Override
    public CompletableFuture<MaterializationSourceProtection> revalidate(
            MaterializationSourceProtection protection,
            OwnerRevalidator ownerRevalidator) {
        try {
            MaterializationSourceProtection exact = requireWrapper(protection);
            OwnerRevalidator revalidator = Objects.requireNonNull(
                    ownerRevalidator, "ownerRevalidator");
            return revalidator.revalidate(exact.owner())
                    .thenCompose(ignored -> reloadExact(exact, deadline()))
                    .thenCompose(current -> revalidator.revalidate(exact.owner())
                            .thenApply(ignored -> wrap(current)));
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    @Override
    public CompletableFuture<MaterializationSourceProtection> transfer(
            MaterializationSourceProtection protection,
            ObjectProtectionOwner newOwner,
            OwnerRevalidator newOwnerRevalidator) {
        try {
            MaterializationSourceProtection exact = requireWrapper(protection);
            ObjectProtectionOwner replacement = Objects.requireNonNull(newOwner, "newOwner");
            OwnerRevalidator revalidator = Objects.requireNonNull(
                    newOwnerRevalidator, "newOwnerRevalidator");
            requireMonotonicOwner(exact.owner(), replacement);
            if (exact.owner().equals(replacement)) {
                return revalidate(exact, revalidator);
            }
            BookKeeperOperationDeadline deadline = deadline();
            return revalidator.revalidate(replacement)
                    .thenCompose(ignored -> reloadExact(exact, deadline))
                    .thenCompose(current -> compareAndSetOwner(
                            current, replacement, deadline))
                    .thenCompose(updated -> revalidator.revalidate(replacement)
                            .thenCompose(ignored -> reloadExact(wrap(updated), deadline)))
                    .thenApply(this::wrap);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    @Override
    public CompletableFuture<Void> release(
            MaterializationSourceProtection protection,
            RemovalAuthorizer removalAuthorizer) {
        try {
            MaterializationSourceProtection exact = requireWrapper(protection);
            RemovalAuthorizer authorizer = Objects.requireNonNull(
                    removalAuthorizer, "removalAuthorizer");
            BookKeeperOperationDeadline deadline = deadline();
            return reloadExact(exact, deadline)
                    .thenCompose(current -> authorizer.authorize(wrap(current))
                            .thenCompose(ignored -> deleteOrReloadAbsent(current, deadline)));
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private CompletableFuture<MaterializationSourceProtection> acquireSlot(
            String streamId,
            SourceGeneration source,
            BookKeeperEntryRangeReadTarget target,
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            int rangeSlot,
            String referenceId,
            ObjectProtectionOwner owner,
            OwnerRevalidator ownerRevalidator,
            int firstSlot,
            int attempt,
            BookKeeperOperationDeadline deadline) {
        int dynamicSlots = dynamicSlots();
        if (attempt >= dynamicSlots) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.BACKPRESSURE_REJECTED,
                    true,
                    "BookKeeper materialization source protection slots are exhausted"));
        }
        int slot = FIRST_DYNAMIC_SLOT
                + Math.floorMod(firstSlot - FIRST_DYNAMIC_SLOT + attempt, dynamicSlots);
        return deadline.bound(metadata.getProtection(
                        cluster,
                        configuration.providerScopeSha256(),
                        target.ledgerId(),
                        rangeSlot,
                        slot))
                .thenCompose(optional -> {
                    if (optional.isPresent()) {
                        BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> current =
                                optional.orElseThrow();
                        if (!sameLogicalReference(
                                current.value(), streamId, source, target, referenceId)) {
                            return acquireSlot(
                                    streamId,
                                    source,
                                    target,
                                    root,
                                    rangeSlot,
                                    referenceId,
                                    owner,
                                    ownerRevalidator,
                                    firstSlot,
                                    attempt + 1,
                                    deadline);
                        }
                        return reconcileOwner(current, owner, ownerRevalidator, deadline)
                                .thenCompose(updated -> finishAcquire(
                                        streamId,
                                        source,
                                        target,
                                        root,
                                        updated,
                                        ownerRevalidator,
                                        deadline));
                    }
                    return ownerRevalidator.revalidate(owner)
                            .thenCompose(ignored -> revalidateRoot(
                                    root, target, streamId, deadline))
                            .thenCompose(ignored -> createOrReload(
                                    streamId,
                                    source,
                                    target,
                                    root,
                                    rangeSlot,
                                    slot,
                                    referenceId,
                                    owner,
                                    deadline))
                            .thenCompose(created -> finishAcquire(
                                    streamId,
                                    source,
                                    target,
                                    root,
                                    created,
                                    ownerRevalidator,
                                    deadline))
                            .exceptionallyCompose(failure -> {
                                Throwable exact = unwrap(failure);
                                if (exact instanceof BookKeeperMetadataConditionFailedException) {
                                    return acquireSlot(
                                            streamId,
                                            source,
                                            target,
                                            root,
                                            rangeSlot,
                                            referenceId,
                                            owner,
                                            ownerRevalidator,
                                            firstSlot,
                                            attempt + 1,
                                            deadline);
                                }
                                return CompletableFuture.failedFuture(exact);
                            });
                });
    }

    private CompletableFuture<MaterializationSourceProtection> finishAcquire(
            String streamId,
            SourceGeneration source,
            BookKeeperEntryRangeReadTarget target,
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> protection,
            OwnerRevalidator ownerRevalidator,
            BookKeeperOperationDeadline deadline) {
        ObjectProtectionOwner owner = owner(protection.value());
        return ownerRevalidator.revalidate(owner)
                .thenCompose(ignored -> revalidateRoot(root, target, streamId, deadline))
                .thenCompose(ignored -> reloadExact(wrap(protection), deadline))
                .thenApply(this::wrap);
    }

    private CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>> createOrReload(
            String streamId,
            SourceGeneration source,
            BookKeeperEntryRangeReadTarget target,
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            int rangeSlot,
            int protectionSlot,
            String referenceId,
            ObjectProtectionOwner owner,
            BookKeeperOperationDeadline deadline) {
        BookKeeperLedgerProtectionRecord value = new BookKeeperLedgerProtectionRecord(
                1,
                root.value().ledgerIdentitySha256(),
                target.clusterAlias(),
                target.ledgerId(),
                root.value().lifecycleEpoch(),
                rangeSlot,
                protectionSlot,
                BookKeeperProtectionType.MATERIALIZATION_SOURCE.wireId(),
                referenceId,
                target.firstEntryId(),
                target.entryCount(),
                target.rangeChecksum().value(),
                streamId,
                source.range().startOffset(),
                source.range().endOffset(),
                source.commitVersion(),
                owner.ownerKey(),
                owner.metadataVersion(),
                owner.identitySha256().value(),
                ProtectionLifecycle.ACTIVE,
                clock.millis(),
                0,
                0);
        return deadline.bound(metadata.createProtection(
                        cluster, configuration.providerScopeSha256(), value))
                .handle((created, failure) -> {
                    if (failure == null) {
                        return CompletableFuture.completedFuture(created);
                    }
                    Throwable original = unwrap(failure);
                    return deadline.bound(metadata.getProtection(
                                    cluster,
                                    configuration.providerScopeSha256(),
                                    target.ledgerId(),
                                    rangeSlot,
                                    protectionSlot))
                            .thenCompose(optional -> {
                                if (optional.isPresent()
                                        && sameCreatedValue(value, optional.orElseThrow().value())) {
                                    return CompletableFuture.completedFuture(optional.orElseThrow());
                                }
                                return CompletableFuture.failedFuture(original);
                            });
                })
                .thenCompose(valueFuture -> valueFuture);
    }

    private CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>> reconcileOwner(
            BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> current,
            ObjectProtectionOwner requested,
            OwnerRevalidator ownerRevalidator,
            BookKeeperOperationDeadline deadline) {
        ObjectProtectionOwner existing = owner(current.value());
        requireMonotonicOwner(existing, requested);
        if (existing.equals(requested)) {
            return CompletableFuture.completedFuture(current);
        }
        return ownerRevalidator.revalidate(requested)
                .thenCompose(ignored -> compareAndSetOwner(current, requested, deadline));
    }

    private CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>> compareAndSetOwner(
            BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> current,
            ObjectProtectionOwner owner,
            BookKeeperOperationDeadline deadline) {
        BookKeeperLedgerProtectionRecord replacement = withOwner(current.value(), owner);
        return deadline.bound(metadata.compareAndSetProtection(
                        cluster,
                        configuration.providerScopeSha256(),
                        replacement,
                        current.metadataVersion()))
                .handle((updated, failure) -> {
                    if (failure == null) {
                        return CompletableFuture.completedFuture(updated);
                    }
                    Throwable original = unwrap(failure);
                    return deadline.bound(metadata.getProtection(
                                    cluster,
                                    configuration.providerScopeSha256(),
                                    current.value().ledgerId(),
                                    current.value().ledgerRangeSlot(),
                                    current.value().protectionSlot()))
                            .thenCompose(optional -> {
                                if (optional.isPresent()
                                        && sameCreatedValue(replacement, optional.orElseThrow().value())) {
                                    return CompletableFuture.completedFuture(optional.orElseThrow());
                                }
                                return CompletableFuture.failedFuture(original);
                            });
                })
                .thenCompose(value -> value);
    }

    private CompletableFuture<Void> deleteOrReloadAbsent(
            BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> current,
            BookKeeperOperationDeadline deadline) {
        return deadline.bound(metadata.deleteProtection(
                        cluster,
                        configuration.providerScopeSha256(),
                        current.value().ledgerId(),
                        current.value().ledgerRangeSlot(),
                        current.value().protectionSlot(),
                        current.metadataVersion()))
                .handle((ignored, failure) -> {
                    if (failure == null) {
                        return CompletableFuture.<Void>completedFuture(null);
                    }
                    Throwable original = unwrap(failure);
                    return deadline.bound(metadata.getProtection(
                                    cluster,
                                    configuration.providerScopeSha256(),
                                    current.value().ledgerId(),
                                    current.value().ledgerRangeSlot(),
                                    current.value().protectionSlot()))
                            .thenCompose(optional -> optional.isEmpty()
                                    ? CompletableFuture.<Void>completedFuture(null)
                                    : CompletableFuture.<Void>failedFuture(original));
                })
                .thenCompose(value -> value);
    }

    private CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>> reloadExact(
            MaterializationSourceProtection protection,
            BookKeeperOperationDeadline deadline) {
        BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> expected = handle(protection).protection();
        return deadline.bound(metadata.getProtection(
                        cluster,
                        configuration.providerScopeSha256(),
                        expected.value().ledgerId(),
                        expected.value().ledgerRangeSlot(),
                        expected.value().protectionSlot()))
                .thenCompose(optional -> {
                    BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> current = optional.orElseThrow(
                            () -> condition("BookKeeper materialization source protection is absent"));
                    if (!sameVersioned(expected, current)) {
                        return CompletableFuture.failedFuture(condition(
                                "BookKeeper materialization source protection changed"));
                    }
                    return deadline.bound(metadata.getRoot(
                                    cluster,
                                    configuration.providerScopeSha256(),
                                    current.value().ledgerId()))
                            .thenApply(root -> {
                                requireReadableRoot(
                                        root.orElseThrow(() -> condition(
                                                "BookKeeper materialization source root is absent"))
                                                .value(),
                                        current.value());
                                return current;
                            });
                });
    }

    private CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerRootRecord>> loadRoot(
            BookKeeperEntryRangeReadTarget target,
            BookKeeperOperationDeadline deadline) {
        return deadline.bound(metadata.getRoot(
                        cluster,
                        configuration.providerScopeSha256(),
                        target.ledgerId()))
                .thenApply(optional -> optional.orElseThrow(
                        () -> condition("BookKeeper materialization source root is absent")))
                .thenApply(root -> {
                    requireReadableRoot(root.value(), target);
                    return root;
                });
    }

    private CompletableFuture<Void> revalidateRoot(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> expected,
            BookKeeperEntryRangeReadTarget target,
            String streamId,
            BookKeeperOperationDeadline deadline) {
        return loadRoot(target, deadline).thenAccept(current -> {
            if (!current.value().ledgerIdentitySha256().equals(expected.value().ledgerIdentitySha256())
                    || current.value().lifecycleEpoch() < expected.value().lifecycleEpoch()
                    || !current.value().streamId().equals(streamId)) {
                throw condition("BookKeeper materialization source root changed");
            }
        });
    }

    private CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>> loadRangeAnchor(
            String streamId,
            SourceGeneration source,
            BookKeeperEntryRangeReadTarget target,
            BookKeeperOperationDeadline deadline) {
        return scanProtections(target.ledgerId(), Optional.empty(), new ArrayList<>(), deadline)
                .thenApply(values -> {
                    List<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>> matches = values.stream()
                            .filter(value -> value.value().protectionSlot() == 1)
                            .filter(value -> value.value().protectionType()
                                    == BookKeeperProtectionType.VISIBLE_GENERATION)
                            .filter(value -> value.value().lifecycle() == ProtectionLifecycle.ACTIVE)
                            .filter(value -> sameSource(value.value(), streamId, source, target))
                            .toList();
                    if (matches.size() != 1) {
                        throw condition(
                                "BookKeeper generation-zero visible protection is absent or ambiguous");
                    }
                    return matches.get(0);
                });
    }

    private CompletableFuture<List<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>>> scanProtections(
            long ledgerId,
            Optional<BookKeeperScanToken> continuation,
            List<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>> values,
            BookKeeperOperationDeadline deadline) {
        int limit = Math.min(configuration.retentionPageSize(), 1_024);
        return deadline.bound(metadata.scanProtections(
                        cluster,
                        configuration.providerScopeSha256(),
                        ledgerId,
                        continuation,
                        limit))
                .thenCompose(page -> {
                    values.addAll(page.values());
                    int maximum = Math.multiplyExact(
                            configuration.maxAppendRangesPerLedger(),
                            configuration.protectionSlotsPerRange());
                    if (values.size() > maximum) {
                        return CompletableFuture.failedFuture(new NereusException(
                                ErrorCode.METADATA_LIMIT_EXCEEDED,
                                false,
                                "BookKeeper materialization source inventory exceeds fixed bounds"));
                    }
                    return page.continuation().isPresent()
                            ? scanProtections(
                                    ledgerId, page.continuation(), values, deadline)
                            : CompletableFuture.completedFuture(List.copyOf(values));
                });
    }

    private MaterializationSourceProtection requireWrapper(
            MaterializationSourceProtection protection) {
        MaterializationSourceProtection exact = Objects.requireNonNull(
                protection, "protection");
        if (exact.targetType() != ReadTargetType.BOOKKEEPER_ENTRY_RANGE) {
            throw new IllegalArgumentException(
                    "BookKeeper source adapter received another protection type");
        }
        BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> value = handle(exact).protection();
        if (!value.value().referenceId().equals(exact.referenceId())
                || !owner(value.value()).equals(exact.owner())
                || value.metadataVersion() != exact.metadataVersion()) {
            throw new IllegalArgumentException(
                    "BookKeeper source protection wrapper is inconsistent");
        }
        return exact;
    }

    private static BookKeeperMaterializationSourceProtectionHandle handle(
            MaterializationSourceProtection protection) {
        return protection.requireProviderHandle(
                BookKeeperMaterializationSourceProtectionHandle.class);
    }

    private MaterializationSourceProtection wrap(
            BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> protection) {
        return new MaterializationSourceProtection(
                ReadTargetType.BOOKKEEPER_ENTRY_RANGE,
                protection.value().referenceId(),
                owner(protection.value()),
                protection.metadataVersion(),
                new BookKeeperMaterializationSourceProtectionHandle(protection));
    }

    private static ObjectProtectionOwner owner(BookKeeperLedgerProtectionRecord value) {
        return new ObjectProtectionOwner(
                value.ownerKey(),
                value.ownerMetadataVersion(),
                new Checksum(ChecksumType.SHA256, value.ownerIdentitySha256()));
    }

    private static void requireMonotonicOwner(
            ObjectProtectionOwner current,
            ObjectProtectionOwner requested) {
        if (!current.ownerKey().equals(requested.ownerKey())
                || requested.metadataVersion() < current.metadataVersion()
                || (requested.metadataVersion() == current.metadataVersion()
                        && !requested.identitySha256().equals(current.identitySha256()))) {
            throw condition("BookKeeper materialization source owner transfer is non-monotonic");
        }
    }

    private static BookKeeperLedgerProtectionRecord withOwner(
            BookKeeperLedgerProtectionRecord value,
            ObjectProtectionOwner owner) {
        return new BookKeeperLedgerProtectionRecord(
                value.schemaVersion(),
                value.ledgerIdentitySha256(),
                value.clusterAlias(),
                value.ledgerId(),
                value.rootLifecycleEpoch(),
                value.ledgerRangeSlot(),
                value.protectionSlot(),
                value.protectionTypeId(),
                value.referenceId(),
                value.firstEntryId(),
                value.entryCount(),
                value.rangeChecksumSha256(),
                value.streamId(),
                value.offsetStart(),
                value.offsetEnd(),
                value.commitVersion(),
                owner.ownerKey(),
                owner.metadataVersion(),
                owner.identitySha256().value(),
                value.lifecycle(),
                value.createdAtMillis(),
                value.expiresAtMillis(),
                0);
    }

    private static boolean sameLogicalReference(
            BookKeeperLedgerProtectionRecord value,
            String streamId,
            SourceGeneration source,
            BookKeeperEntryRangeReadTarget target,
            String referenceId) {
        return value.protectionType() == BookKeeperProtectionType.MATERIALIZATION_SOURCE
                && value.referenceId().equals(referenceId)
                && value.lifecycle() == ProtectionLifecycle.ACTIVE
                && sameSource(value, streamId, source, target);
    }

    private static boolean sameSource(
            BookKeeperLedgerProtectionRecord value,
            String streamId,
            SourceGeneration source,
            BookKeeperEntryRangeReadTarget target) {
        return value.ledgerId() == target.ledgerId()
                && value.clusterAlias().equals(target.clusterAlias())
                && value.firstEntryId() == target.firstEntryId()
                && value.entryCount() == target.entryCount()
                && value.rangeChecksumSha256().equals(target.rangeChecksum().value())
                && value.streamId().equals(streamId)
                && value.offsetStart() == source.range().startOffset()
                && value.offsetEnd() == source.range().endOffset()
                && value.commitVersion() == source.commitVersion();
    }

    private static boolean sameCreatedValue(
            BookKeeperLedgerProtectionRecord expected,
            BookKeeperLedgerProtectionRecord actual) {
        return expected.withMetadataVersion(actual.metadataVersion()).equals(actual);
    }

    private static boolean sameVersioned(
            BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> expected,
            BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> actual) {
        return expected.key().equals(actual.key())
                && expected.metadataVersion() == actual.metadataVersion()
                && expected.durableValueSha256().equals(actual.durableValueSha256())
                && expected.value().equals(actual.value());
    }

    private static void requireReadableRoot(
            BookKeeperLedgerRootRecord root,
            BookKeeperEntryRangeReadTarget target) {
        if (root.ledgerId() != target.ledgerId()
                || !root.clusterAlias().equals(target.clusterAlias())) {
            throw condition("BookKeeper materialization source root identity differs from target");
        }
        if (root.lifecycle() != BookKeeperLedgerLifecycle.ACTIVE
                && root.lifecycle() != BookKeeperLedgerLifecycle.SEALING
                && root.lifecycle() != BookKeeperLedgerLifecycle.SEALED) {
            throw condition("BookKeeper materialization source root is not readable");
        }
    }

    private static void requireReadableRoot(
            BookKeeperLedgerRootRecord root,
            BookKeeperLedgerProtectionRecord protection) {
        if (root.ledgerId() != protection.ledgerId()
                || !root.clusterAlias().equals(protection.clusterAlias())
                || !root.ledgerIdentitySha256().equals(protection.ledgerIdentitySha256())
                || root.lifecycleEpoch() < protection.rootLifecycleEpoch()
                || !root.streamId().equals(protection.streamId())) {
            throw condition("BookKeeper materialization protection root identity changed");
        }
        if (root.lifecycle() != BookKeeperLedgerLifecycle.ACTIVE
                && root.lifecycle() != BookKeeperLedgerLifecycle.SEALING
                && root.lifecycle() != BookKeeperLedgerLifecycle.SEALED) {
            throw condition("BookKeeper materialization protection root is not readable");
        }
    }

    private static BookKeeperEntryRangeReadTarget requireTarget(SourceGeneration source) {
        if (!(source.readTarget() instanceof BookKeeperEntryRangeReadTarget target)) {
            throw new IllegalArgumentException(
                    "BookKeeper source adapter received another target type");
        }
        return target;
    }

    private int firstSlot(String referenceId) {
        byte[] digest;
        try {
            digest = MessageDigest.getInstance("SHA-256")
                    .digest(referenceId.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
        int candidate = ByteBuffer.wrap(digest).getInt();
        return FIRST_DYNAMIC_SLOT + Math.floorMod(candidate, dynamicSlots());
    }

    private int dynamicSlots() {
        return configuration.protectionSlotsPerRange() - FIRST_DYNAMIC_SLOT;
    }

    private BookKeeperOperationDeadline deadline() {
        return new BookKeeperOperationDeadline(configuration.operationTimeout());
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException
                        || current instanceof java.util.concurrent.ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }

    private static NereusException condition(String message) {
        return new NereusException(ErrorCode.METADATA_CONDITION_FAILED, true, message);
    }
}
