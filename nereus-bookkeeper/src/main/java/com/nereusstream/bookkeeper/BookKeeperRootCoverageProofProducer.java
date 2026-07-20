/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.BookKeeperKeyspace;
import com.nereusstream.metadata.oxia.BookKeeperLedgerMetadataStore;
import com.nereusstream.metadata.oxia.BookKeeperMetadataStoreConfig;
import com.nereusstream.metadata.oxia.BookKeeperScanToken;
import com.nereusstream.metadata.oxia.BookKeeperVersionedValue;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerProtectionRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerReaderLeaseRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Produces NBKROOT1 only after a strict empty-continuation traversal of all 256 root shards. */
public final class BookKeeperRootCoverageProofProducer {
    private static final String DOMAIN = "NBKROOT1";

    private final String cluster;
    private final BookKeeperWalConfiguration configuration;
    private final String ledgerIdNamespaceSha256;
    private final BookKeeperLedgerMetadataStore metadata;
    private final BookKeeperKeyspace keys;
    private final int pageSize;

    public BookKeeperRootCoverageProofProducer(
            String cluster,
            BookKeeperWalConfiguration configuration,
            String ledgerIdNamespaceSha256,
            BookKeeperLedgerMetadataStore metadata) {
        this.cluster = text(cluster, "cluster");
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.ledgerIdNamespaceSha256 = BookKeeperWalConfiguration.sha256(
                ledgerIdNamespaceSha256, "ledgerIdNamespaceSha256");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        BookKeeperMetadataStoreConfig store = new BookKeeperMetadataStoreConfig(
                configuration.maxAppendRangesPerLedger(),
                configuration.protectionSlotsPerRange(),
                configuration.maxReaderLeasesPerLedger(),
                configuration.maxUncertainAllocations());
        this.keys = store.keyspace(cluster);
        this.pageSize = Math.min(configuration.retentionPageSize(), 1_024);
    }

    public CompletableFuture<BookKeeperRootCoverageProof> produce(
            BookKeeperBrokerReadiness readiness, Duration timeout) {
        final BookKeeperBrokerReadiness exactReadiness;
        final BookKeeperOperationDeadline deadline;
        final Accumulator accumulator;
        try {
            exactReadiness = Objects.requireNonNull(readiness, "readiness");
            deadline = new BookKeeperOperationDeadline(timeout);
            accumulator = new Accumulator(exactReadiness);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return scanShard(0, Optional.empty(), accumulator, deadline)
                .thenApply(ignored -> accumulator.finish());
    }

    private CompletableFuture<Void> scanShard(
            int shard,
            Optional<BookKeeperScanToken> continuation,
            Accumulator accumulator,
            BookKeeperOperationDeadline deadline) {
        if (shard == BookKeeperLedgerRetentionScanner.ROOT_SHARDS) {
            return CompletableFuture.completedFuture(null);
        }
        return deadline.bound(metadata.scanRoots(cluster, shard, continuation, pageSize))
                .thenCompose(page -> processRoots(page.values(), 0, shard, accumulator, deadline)
                        .thenCompose(ignored -> {
                            if (page.continuation().isPresent()) {
                                return scanShard(
                                        shard,
                                        page.continuation(),
                                        accumulator,
                                        deadline);
                            }
                            accumulator.completeShard(shard);
                            return scanShard(
                                    shard + 1,
                                    Optional.empty(),
                                    accumulator,
                                    deadline);
                        }));
    }

    private CompletableFuture<Void> processRoots(
            List<BookKeeperVersionedValue<BookKeeperLedgerRootRecord>> roots,
            int index,
            int shard,
            Accumulator accumulator,
            BookKeeperOperationDeadline deadline) {
        if (index == roots.size()) {
            return CompletableFuture.completedFuture(null);
        }
        BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root = roots.get(index);
        accumulator.observeRootKey(shard, root.key());
        accumulator.rootsScanned = Math.addExact(accumulator.rootsScanned, 1);
        if (!matchesBinding(root.value())) {
            return processRoots(roots, index + 1, shard, accumulator, deadline);
        }
        requireRoot(root, shard);
        accumulator.matchingRoots = Math.addExact(accumulator.matchingRoots, 1);
        accumulator.value("root", root);
        return scanProtections(root, Optional.empty(), null, 0, accumulator, deadline)
                .thenCompose(ignored -> scanReaders(
                        root, Optional.empty(), null, 0, accumulator, deadline))
                .thenCompose(ignored -> processRoots(
                        roots, index + 1, shard, accumulator, deadline));
    }

    private CompletableFuture<Void> scanProtections(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            Optional<BookKeeperScanToken> continuation,
            String previousKey,
            int count,
            Accumulator accumulator,
            BookKeeperOperationDeadline deadline) {
        BookKeeperLedgerRootRecord value = root.value();
        return deadline.bound(metadata.scanProtections(
                        cluster,
                        configuration.providerScopeSha256(),
                        value.ledgerId(),
                        continuation,
                        pageSize))
                .thenCompose(page -> {
                    String last = previousKey;
                    int nextCount = count;
                    for (BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> protection : page.values()) {
                        requireOrdered(last, protection.key(), "protection");
                        requireProtection(root, protection);
                        accumulator.value("protection", protection);
                        accumulator.protectionsScanned = Math.addExact(
                                accumulator.protectionsScanned, 1);
                        nextCount = Math.addExact(nextCount, 1);
                        last = protection.key();
                    }
                    int maximum = Math.multiplyExact(
                            configuration.maxAppendRangesPerLedger(),
                            configuration.protectionSlotsPerRange());
                    if (nextCount > maximum) {
                        throw invariant("BookKeeper protection scan exceeded the configured Cartesian bound");
                    }
                    if (page.continuation().isEmpty()) {
                        return CompletableFuture.completedFuture(null);
                    }
                    return scanProtections(
                            root,
                            page.continuation(),
                            last,
                            nextCount,
                            accumulator,
                            deadline);
                });
    }

    private CompletableFuture<Void> scanReaders(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            Optional<BookKeeperScanToken> continuation,
            String previousKey,
            int count,
            Accumulator accumulator,
            BookKeeperOperationDeadline deadline) {
        BookKeeperLedgerRootRecord value = root.value();
        return deadline.bound(metadata.scanReaderLeases(
                        cluster,
                        configuration.providerScopeSha256(),
                        value.ledgerId(),
                        continuation,
                        pageSize))
                .thenCompose(page -> {
                    String last = previousKey;
                    int nextCount = count;
                    for (BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord> reader : page.values()) {
                        requireOrdered(last, reader.key(), "reader lease");
                        requireReader(root, reader);
                        accumulator.value("reader", reader);
                        accumulator.readerLeasesScanned = Math.addExact(
                                accumulator.readerLeasesScanned, 1);
                        nextCount = Math.addExact(nextCount, 1);
                        last = reader.key();
                    }
                    if (nextCount > configuration.maxReaderLeasesPerLedger()) {
                        throw invariant("BookKeeper reader scan exceeded its configured fixed-slot bound");
                    }
                    if (page.continuation().isEmpty()) {
                        return CompletableFuture.completedFuture(null);
                    }
                    return scanReaders(
                            root,
                            page.continuation(),
                            last,
                            nextCount,
                            accumulator,
                            deadline);
                });
    }

    private boolean matchesBinding(BookKeeperLedgerRootRecord root) {
        return root.clusterAlias().equals(configuration.clusterAlias())
                && root.providerScopeSha256().equals(configuration.providerScopeSha256())
                && root.configurationBindingSha256()
                        .equals(configuration.configurationBindingSha256().value())
                && root.ledgerIdNamespaceSha256().equals(ledgerIdNamespaceSha256);
    }

    private void requireRoot(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> versioned,
            int shard) {
        BookKeeperLedgerRootRecord root = versioned.value();
        BookKeeperKeyspace.RootKeyIdentity identity = keys.parseRootKey(
                versioned.key(), root.providerScopeSha256(), root.ledgerId());
        if (identity.shard() != shard
                || !identity.ledgerIdentitySha256().equals(root.ledgerIdentitySha256())
                || !configuration.ledgerIdNamespace().contains(root.ledgerId())
                || root.metadataVersion() != versioned.metadataVersion()
                || root.ensembleSize() != configuration.ensembleSize()
                || root.writeQuorumSize() != configuration.writeQuorumSize()
                || root.ackQuorumSize() != configuration.ackQuorumSize()
                || !root.digestType().equals(configuration.digestType().name())) {
            throw invariant("BookKeeper root does not match its exact key/configuration authority");
        }
        BookKeeperLedgerCustomMetadata.fromRoot(cluster, configuration, root);
    }

    private void requireProtection(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> versioned) {
        BookKeeperLedgerRootRecord owner = root.value();
        BookKeeperLedgerProtectionRecord value = versioned.value();
        BookKeeperKeyspace.ProtectionKeyIdentity key = keys.parseProtectionKey(
                versioned.key(), configuration.providerScopeSha256(), owner.ledgerId());
        if (value.metadataVersion() != versioned.metadataVersion()
                || value.ledgerId() != owner.ledgerId()
                || !value.ledgerIdentitySha256().equals(owner.ledgerIdentitySha256())
                || !value.clusterAlias().equals(configuration.clusterAlias())
                || !value.streamId().equals(owner.streamId())
                || key.rangeSlot() != value.ledgerRangeSlot()
                || key.protectionSlot() != value.protectionSlot()) {
            throw invariant("BookKeeper protection does not match its root/key authority");
        }
    }

    private void requireReader(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            BookKeeperVersionedValue<BookKeeperLedgerReaderLeaseRecord> versioned) {
        BookKeeperLedgerRootRecord owner = root.value();
        BookKeeperLedgerReaderLeaseRecord value = versioned.value();
        BookKeeperKeyspace.ReaderKeyIdentity key = keys.parseReaderLeaseKey(
                versioned.key(), configuration.providerScopeSha256(), owner.ledgerId());
        if (value.metadataVersion() != versioned.metadataVersion()
                || value.ledgerId() != owner.ledgerId()
                || !value.ledgerIdentitySha256().equals(owner.ledgerIdentitySha256())
                || key.readerSlot() != value.readerSlot()) {
            throw invariant("BookKeeper reader lease does not match its root/key authority");
        }
    }

    private static void requireOrdered(String previous, String current, String kind) {
        if (previous != null && previous.compareTo(current) >= 0) {
            throw invariant("BookKeeper " + kind + " scan is not strictly ordered and unique");
        }
    }

    private final class Accumulator {
        private final BookKeeperBrokerReadiness readiness;
        private final MessageDigest digest = sha256();
        private int shardsScanned;
        private long rootsScanned;
        private long matchingRoots;
        private long protectionsScanned;
        private long readerLeasesScanned;
        private int activeShard = -1;
        private String previousRootKey;

        private Accumulator(BookKeeperBrokerReadiness readiness) {
            this.readiness = readiness;
            frame(digest, DOMAIN);
            frame(digest, cluster);
            frame(digest, configuration.configurationBindingSha256().value());
            frame(digest, ledgerIdNamespaceSha256);
            number(digest, readiness.brokerReadinessEpoch());
            frame(digest, readiness.brokerSetSha256().value());
        }

        private void observeRootKey(int shard, String key) {
            if (activeShard != shard) {
                activeShard = shard;
                previousRootKey = null;
            }
            requireOrdered(previousRootKey, key, "root");
            previousRootKey = key;
        }

        private void completeShard(int shard) {
            if (shard != shardsScanned) {
                throw invariant("BookKeeper root shards were not completed in canonical order");
            }
            shardsScanned++;
            previousRootKey = null;
        }

        private void value(String kind, BookKeeperVersionedValue<?> value) {
            frame(digest, kind);
            frame(digest, value.key());
            number(digest, value.metadataVersion());
            frame(digest, value.durableValueSha256().value());
        }

        private BookKeeperRootCoverageProof finish() {
            number(digest, matchingRoots);
            number(digest, protectionsScanned);
            number(digest, readerLeasesScanned);
            return new BookKeeperRootCoverageProof(
                    readiness.brokerReadinessEpoch(),
                    readiness.brokerSetSha256(),
                    shardsScanned,
                    rootsScanned,
                    matchingRoots,
                    protectionsScanned,
                    readerLeasesScanned,
                    new Checksum(
                            ChecksumType.SHA256,
                            HexFormat.of().formatHex(digest.digest())));
        }
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static void frame(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }

    private static void number(MessageDigest digest, long value) {
        digest.update(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
    }

    private static NereusException invariant(String message) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static String text(String value, String name) {
        String exact = Objects.requireNonNull(value, name);
        if (exact.isBlank()) {
            throw new IllegalArgumentException(name + " cannot be blank");
        }
        return exact;
    }
}
