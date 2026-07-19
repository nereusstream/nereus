/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.BookKeeperLedgerMetadataStore;
import com.nereusstream.metadata.oxia.BookKeeperScanToken;
import com.nereusstream.metadata.oxia.BookKeeperVersionedValue;
import com.nereusstream.metadata.oxia.BookKeeperWriterMetadataStore;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerProtectionRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerReaderLeaseRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperProtectionType;
import com.nereusstream.metadata.oxia.records.BookKeeperWriterLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperWriterStateRecord;
import com.nereusstream.metadata.oxia.records.ProtectionLifecycle;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.api.LedgerMetadata;

/** Complete, bounded, fail-closed capture for one sealed BookKeeper ledger. */
public final class BookKeeperWalRetentionGate {
    private final String cluster;
    private final BookKeeperWalConfiguration configuration;
    private final BookKeeperLedgerGcConfiguration gcConfiguration;
    private final BookKeeperWriterMetadataStore writerMetadata;
    private final BookKeeperLedgerMetadataStore ledgerMetadata;
    private final BookKeeperLedgerIdNamespaceReservationVerifier namespaceVerifier;
    private final BookKeeperProtocolActivationVerifier activationVerifier;
    private final BookKeeperClientOperations client;
    private final Clock clock;

    public BookKeeperWalRetentionGate(
            String cluster,
            BookKeeperWalConfiguration configuration,
            BookKeeperLedgerGcConfiguration gcConfiguration,
            BookKeeperWriterMetadataStore writerMetadata,
            BookKeeperLedgerMetadataStore ledgerMetadata,
            BookKeeperLedgerIdNamespaceReservationVerifier namespaceVerifier,
            BookKeeperProtocolActivationVerifier activationVerifier,
            BookKeeperClientOperations client,
            Clock clock) {
        this.cluster = text(cluster, "cluster");
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.gcConfiguration = Objects.requireNonNull(gcConfiguration, "gcConfiguration");
        gcConfiguration.validateAgainst(configuration);
        this.writerMetadata = Objects.requireNonNull(writerMetadata, "writerMetadata");
        this.ledgerMetadata = Objects.requireNonNull(ledgerMetadata, "ledgerMetadata");
        this.namespaceVerifier = Objects.requireNonNull(namespaceVerifier, "namespaceVerifier");
        this.activationVerifier = Objects.requireNonNull(activationVerifier, "activationVerifier");
        this.client = Objects.requireNonNull(client, "client");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public CompletableFuture<BookKeeperRetentionEvaluation> evaluate(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> observed,
            Duration timeout) {
        return evaluate(observed, BookKeeperLedgerLifecycle.SEALED, timeout);
    }

    CompletableFuture<BookKeeperRetentionEvaluation> evaluateMarked(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> observed,
            Duration timeout) {
        return evaluate(observed, BookKeeperLedgerLifecycle.MARKED, timeout);
    }

    CompletableFuture<BookKeeperRetentionEvaluation> evaluateDeleting(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> observed,
            Duration timeout) {
        return evaluate(observed, BookKeeperLedgerLifecycle.DELETING, timeout);
    }

    private CompletableFuture<BookKeeperRetentionEvaluation> evaluate(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> observed,
            BookKeeperLedgerLifecycle requiredLifecycle,
            Duration timeout) {
        BookKeeperVersionedValue<BookKeeperLedgerRootRecord> expected = Objects.requireNonNull(observed, "observed");
        BookKeeperOperationDeadline deadline = new BookKeeperOperationDeadline(min(
                Objects.requireNonNull(timeout, "timeout"),
                configuration.operationTimeout()));
        var rootFuture = deadline.bound(ledgerMetadata.getRoot(
                cluster, configuration.providerScopeSha256(), expected.value().ledgerId()));
        var writerFuture = deadline.bound(writerMetadata.getWriter(
                cluster, new StreamId(expected.value().streamId())));
        var protectionsFuture = scanProtections(expected.value().ledgerId(), deadline, Optional.empty(), new ArrayList<>());
        var readersFuture = scanReaders(expected.value().ledgerId(), deadline, Optional.empty(), new ArrayList<>());
        var slotFuture = deadline.bound(writerMetadata.getAllocationSlot(
                cluster, expected.value().allocationSlot()));
        var namespaceFuture = namespaceVerifier.requireActive(configuration, deadline.remaining());
        var activationFuture = activationVerifier.requireActive(deadline.remaining());
        var providerFuture = deadline.bound(client.metadata(expected.value().ledgerId(), deadline));
        return CompletableFuture.allOf(
                        rootFuture,
                        writerFuture,
                        protectionsFuture,
                        readersFuture,
                        slotFuture,
                        namespaceFuture,
                        activationFuture,
                        providerFuture)
                .thenApply(ignored -> evaluate(
                        expected,
                        requiredLifecycle,
                        rootFuture.join(),
                        writerFuture.join(),
                        protectionsFuture.join(),
                        readersFuture.join(),
                        slotFuture.join().isPresent(),
                        namespaceFuture.join(),
                        activationFuture.join(),
                        providerFuture.join()));
    }

    private BookKeeperRetentionEvaluation evaluate(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> expected,
            BookKeeperLedgerLifecycle requiredLifecycle,
            Optional<BookKeeperVersionedValue<BookKeeperLedgerRootRecord>> reloaded,
            Optional<BookKeeperVersionedValue<BookKeeperWriterStateRecord>> writerOptional,
            List<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>> protections,
            List<BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord>> readers,
            boolean allocationSlotPresent,
            BookKeeperLedgerIdNamespaceReservation namespace,
            BookKeeperProtocolActivationProof activation,
            LedgerMetadata providerMetadata) {
        EnumSet<BookKeeperRetentionBlocker> blockers = EnumSet.noneOf(BookKeeperRetentionBlocker.class);
        BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root = reloaded.orElse(null);
        if (root == null
                || root.metadataVersion() != expected.metadataVersion()
                || !root.durableValueSha256().equals(expected.durableValueSha256())
                || root.value().lifecycle() != requiredLifecycle) {
            blockers.add(BookKeeperRetentionBlocker.ROOT_CHANGED_OR_INELIGIBLE);
        }
        BookKeeperLedgerRootRecord rootValue = root == null ? expected.value() : root.value();
        if (rootValue.lateCreateHazard()) blockers.add(BookKeeperRetentionBlocker.LATE_CREATE_HAZARD);
        if (allocationSlotPresent) blockers.add(BookKeeperRetentionBlocker.ALLOCATION_SLOT_PRESENT);
        BookKeeperVersionedValue<BookKeeperWriterStateRecord> writer = writerOptional.orElse(null);
        if (writer == null
                || ((writer.value().lifecycle() == BookKeeperWriterLifecycle.ACTIVE
                                || writer.value().lifecycle() == BookKeeperWriterLifecycle.RECOVERING)
                        && writer.value().activeLedgerId() == rootValue.ledgerId())
                || (writer.value().lifecycle() == BookKeeperWriterLifecycle.ALLOCATING
                        && writer.value().allocationLedgerId() == rootValue.ledgerId())) {
            blockers.add(BookKeeperRetentionBlocker.WRITER_SELECTS_LEDGER);
        }
        if (!completeRetiredInventory(rootValue, protections)) {
            blockers.add(BookKeeperRetentionBlocker.PROTECTION_PRESENT);
        }
        if (!readers.isEmpty()) blockers.add(BookKeeperRetentionBlocker.READER_LEASE_PRESENT);
        if (protections.size() > Math.multiplyExact(
                        configuration.maxAppendRangesPerLedger(),
                        configuration.protectionSlotsPerRange())
                || readers.size() > configuration.maxReaderLeasesPerLedger()) {
            blockers.add(BookKeeperRetentionBlocker.INVENTORY_LIMIT_EXCEEDED);
        }
        try {
            activation.requireExact(configuration, namespace);
            if (!namespace.ledgerIdNamespaceSha256().value().equals(rootValue.ledgerIdNamespaceSha256())) {
                throw new IllegalArgumentException("root namespace digest drifted");
            }
        } catch (IllegalArgumentException mismatch) {
            blockers.add(BookKeeperRetentionBlocker.ACTIVATION_MISMATCH);
        }
        try {
            BookKeeperLedgerCustomMetadata.fromRoot(cluster, configuration, rootValue)
                    .requireExactImmutableLedgerMetadata(rootValue.ledgerId(), configuration, providerMetadata);
            if (!providerMetadata.isClosed()
                    || providerMetadata.getLastEntryId() != rootValue.sealedLastEntryId()
                    || providerMetadata.getLength() != rootValue.sealedLength()) {
                throw new IllegalArgumentException("sealed provider facts changed");
            }
        } catch (RuntimeException mismatch) {
            blockers.add(BookKeeperRetentionBlocker.PROVIDER_METADATA_MISMATCH);
        }
        if (!blockers.isEmpty()) return BookKeeperRetentionEvaluation.blocked(blockers);
        Checksum referenceSet = referenceSet(root, writer, protections, readers, activation);
        return BookKeeperRetentionEvaluation.admitted(new BookKeeperLedgerRetirementCandidate(
                root,
                writer,
                protections,
                referenceSet,
                activation,
                clock.millis()));
    }

    private boolean completeRetiredInventory(
            BookKeeperLedgerRootRecord root,
            List<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>> protections) {
        if (protections.stream().anyMatch(value -> value.value().lifecycle() != ProtectionLifecycle.RETIRED)) {
            return false;
        }
        if (root.sealedLastEntryId() < 0) return protections.isEmpty();
        if (protections.isEmpty()) return false;
        List<BookKeeperLedgerProtectionRecord> mandatory = protections.stream()
                .map(BookKeeperVersionedValue::value)
                .filter(value -> value.protectionSlot() < 3)
                .sorted(Comparator.comparingInt(BookKeeperLedgerProtectionRecord::ledgerRangeSlot)
                        .thenComparingInt(BookKeeperLedgerProtectionRecord::protectionSlot))
                .toList();
        if (mandatory.size() % 3 != 0) return false;
        int ranges = mandatory.size() / 3;
        if (ranges <= 0 || ranges > configuration.maxAppendRangesPerLedger()) return false;
        List<BookKeeperLedgerProtectionRecord> canonicalRanges = new ArrayList<>(ranges);
        long nextEntry = 0;
        for (int rangeSlot = 0; rangeSlot < ranges; rangeSlot++) {
            long rangeFirstEntry = nextEntry;
            int rangeEntryCount = -1;
            String rangeChecksum = "";
            for (int protectionSlot = 0; protectionSlot < 3; protectionSlot++) {
                BookKeeperLedgerProtectionRecord value = mandatory.get(rangeSlot * 3 + protectionSlot);
                if (value.ledgerRangeSlot() != rangeSlot
                        || value.protectionSlot() != protectionSlot
                        || value.protectionType() != switch (protectionSlot) {
                            case 0 -> BookKeeperProtectionType.REACHABLE_APPEND;
                            case 1 -> BookKeeperProtectionType.VISIBLE_GENERATION;
                            default -> BookKeeperProtectionType.APPEND_RECOVERY;
                        }
                        || value.firstEntryId() != rangeFirstEntry) {
                    return false;
                }
                if (protectionSlot == 0) {
                    rangeEntryCount = value.entryCount();
                    rangeChecksum = value.rangeChecksumSha256();
                    canonicalRanges.add(value);
                } else if (value.entryCount() != rangeEntryCount
                        || !value.rangeChecksumSha256().equals(rangeChecksum)
                        || !sameLogicalRange(value, canonicalRanges.get(rangeSlot))) {
                    return false;
                }
            }
            nextEntry = Math.addExact(nextEntry, rangeEntryCount);
        }
        if (nextEntry < Math.addExact(root.sealedLastEntryId(), 1)) return false;
        for (BookKeeperLedgerProtectionRecord value : protections.stream()
                .map(BookKeeperVersionedValue::value).toList()) {
            if (value.ledgerId() != root.ledgerId()
                    || !value.ledgerIdentitySha256().equals(root.ledgerIdentitySha256())
                    || !value.clusterAlias().equals(root.clusterAlias())
                    || !value.streamId().equals(root.streamId())
                    || value.rootLifecycleEpoch() > root.lifecycleEpoch()
                    || value.ledgerRangeSlot() >= ranges
                    || value.protectionSlot() >= configuration.protectionSlotsPerRange()
                    || !sameLogicalRange(value, canonicalRanges.get(value.ledgerRangeSlot()))) {
                return false;
            }
        }
        return true;
    }

    private static boolean sameLogicalRange(
            BookKeeperLedgerProtectionRecord value,
            BookKeeperLedgerProtectionRecord canonical) {
        return value.firstEntryId() == canonical.firstEntryId()
                && value.entryCount() == canonical.entryCount()
                && value.rangeChecksumSha256().equals(canonical.rangeChecksumSha256())
                && value.streamId().equals(canonical.streamId())
                && value.offsetStart() == canonical.offsetStart()
                && value.offsetEnd() == canonical.offsetEnd();
    }

    private CompletableFuture<List<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>>> scanProtections(
            long ledgerId,
            BookKeeperOperationDeadline deadline,
            Optional<BookKeeperScanToken> continuation,
            List<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>> values) {
        int pageSize = Math.min(configuration.retentionPageSize(), 1_024);
        return deadline.bound(ledgerMetadata.scanProtections(
                        cluster,
                        configuration.providerScopeSha256(),
                        ledgerId,
                        continuation,
                        pageSize))
                .thenCompose(page -> {
                    values.addAll(page.values());
                    if (values.size() > Math.multiplyExact(
                            configuration.maxAppendRangesPerLedger(),
                            configuration.protectionSlotsPerRange())) {
                        return CompletableFuture.completedFuture(List.copyOf(values));
                    }
                    return page.continuation().isPresent()
                            ? scanProtections(ledgerId, deadline, page.continuation(), values)
                            : CompletableFuture.completedFuture(List.copyOf(values));
                });
    }

    private CompletableFuture<List<BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord>>> scanReaders(
            long ledgerId,
            BookKeeperOperationDeadline deadline,
            Optional<BookKeeperScanToken> continuation,
            List<BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord>> values) {
        int pageSize = Math.min(configuration.retentionPageSize(), 1_024);
        return deadline.bound(ledgerMetadata.scanReaderLeases(
                        cluster,
                        configuration.providerScopeSha256(),
                        ledgerId,
                        continuation,
                        pageSize))
                .thenCompose(page -> {
                    values.addAll(page.values());
                    if (values.size() > configuration.maxReaderLeasesPerLedger()) {
                        return CompletableFuture.completedFuture(List.copyOf(values));
                    }
                    return page.continuation().isPresent()
                            ? scanReaders(ledgerId, deadline, page.continuation(), values)
                            : CompletableFuture.completedFuture(List.copyOf(values));
                });
    }

    private static Checksum referenceSet(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            BookKeeperVersionedValue<BookKeeperWriterStateRecord> writer,
            List<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>> protections,
            List<BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord>> readers,
            BookKeeperProtocolActivationProof activation) {
        MessageDigest digest = digest();
        frame(digest, "NBKREF1");
        frame(digest, root.key());
        frame(digest, root.value().ledgerIdentitySha256());
        frame(digest, Long.toString(root.value().sealedLastEntryId()));
        frame(digest, Long.toString(root.value().sealedLength()));
        frame(digest, writer.key());
        frame(digest, writer.value().streamId());
        frame(digest, writer.value().configurationBindingSha256());
        protections.stream().sorted(Comparator.comparing(BookKeeperVersionedValue::key))
                .forEach(value -> versioned(digest, value));
        readers.stream().sorted(Comparator.comparing(BookKeeperVersionedValue::key))
                .forEach(value -> versioned(digest, value));
        frame(digest, Long.toString(activation.activationMetadataVersion()));
        frame(digest, activation.activationRecordSha256().value());
        return new Checksum(ChecksumType.SHA256, HexFormat.of().formatHex(digest.digest()));
    }

    private static void versioned(MessageDigest digest, BookKeeperVersionedValue<?> value) {
        frame(digest, value.key());
        frame(digest, Long.toString(value.metadataVersion()));
        frame(digest, value.durableValueSha256().value());
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

    private static Duration min(Duration left, Duration right) {
        return left.compareTo(right) <= 0 ? left : right;
    }

    private static String text(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) throw new IllegalArgumentException(name + " cannot be blank");
        return value;
    }
}
