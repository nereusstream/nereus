/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamState;
import com.nereusstream.bookkeeper.BookKeeperBrokerReadiness;
import com.nereusstream.bookkeeper.BookKeeperOperationDeadline;
import com.nereusstream.bookkeeper.BookKeeperWalConfiguration;
import com.nereusstream.managedledger.generation.ManagedLedgerGenerationProjectionRefV1;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionMetadataStore;
import com.nereusstream.metadata.oxia.ManagedLedgerStreamProjection;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.ProjectionIdentity;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.VersionedMaterializationStreamRegistration;
import com.nereusstream.metadata.oxia.VersionedTopicProjection;
import com.nereusstream.metadata.oxia.VersionedVirtualLedgerProjection;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
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

/** Produces NBKSTREAM1 only after all 64 registrations and each live BK L0/F2 authority agree. */
public final class BookKeeperStreamCoverageProofProducer {
    private static final String DOMAIN = "NBKSTREAM1";

    private final String cluster;
    private final BookKeeperWalConfiguration configuration;
    private final String ledgerIdNamespaceSha256;
    private final GenerationMetadataStore generations;
    private final OxiaMetadataStore l0;
    private final ManagedLedgerProjectionMetadataStore projections;
    private final F4Keyspace keys;
    private final int pageSize;

    public BookKeeperStreamCoverageProofProducer(
            String cluster,
            BookKeeperWalConfiguration configuration,
            String ledgerIdNamespaceSha256,
            GenerationMetadataStore generations,
            OxiaMetadataStore l0,
            ManagedLedgerProjectionMetadataStore projections) {
        this.cluster = text(cluster, "cluster");
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.ledgerIdNamespaceSha256 = new Checksum(
                        ChecksumType.SHA256,
                        Objects.requireNonNull(
                                ledgerIdNamespaceSha256,
                                "ledgerIdNamespaceSha256"))
                .value();
        this.generations = Objects.requireNonNull(generations, "generations");
        this.l0 = Objects.requireNonNull(l0, "l0");
        this.projections = Objects.requireNonNull(projections, "projections");
        this.keys = new F4Keyspace(cluster);
        this.pageSize = Math.min(configuration.retentionPageSize(), 1_000);
    }

    public CompletableFuture<BookKeeperStreamCoverageProof> produce(
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
            Optional<F4ScanToken> continuation,
            Accumulator accumulator,
            BookKeeperOperationDeadline deadline) {
        if (shard == BookKeeperStreamCoverageProof.STREAM_SHARDS) {
            return CompletableFuture.completedFuture(null);
        }
        return deadline.bound(generations.scanStreamRegistrations(
                        cluster, shard, continuation, pageSize))
                .thenCompose(page -> process(
                                page.values(), 0, shard, accumulator, deadline)
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

    private CompletableFuture<Void> process(
            List<VersionedMaterializationStreamRegistration> values,
            int index,
            int shard,
            Accumulator accumulator,
            BookKeeperOperationDeadline deadline) {
        if (index == values.size()) {
            return CompletableFuture.completedFuture(null);
        }
        VersionedMaterializationStreamRegistration registration = values.get(index);
        accumulator.observeKey(shard, registration.key());
        accumulator.registrationsScanned = Math.addExact(
                accumulator.registrationsScanned, 1);
        MaterializationStreamRegistrationRecord value = registration.value();
        StorageProfile profile = parseProfile(value.storageProfile());
        if (!isBookKeeper(profile)) {
            return process(values, index + 1, shard, accumulator, deadline);
        }
        StreamId streamId = new StreamId(value.streamId());
        requireRegistration(registration, streamId, shard);
        return deadline.bound(l0.getStreamSnapshot(cluster, streamId))
                .thenCombine(
                        deadline.bound(projections.getProjectionByStream(cluster, streamId)),
                        (snapshot, projection) -> {
                            requireAuthorities(registration, profile, snapshot, projection);
                            accumulator.stream(registration, snapshot, projection);
                            accumulator.bookKeeperStreamsVerified = Math.addExact(
                                    accumulator.bookKeeperStreamsVerified, 1);
                            return null;
                        })
                .thenCompose(ignored -> process(
                        values, index + 1, shard, accumulator, deadline));
    }

    private void requireRegistration(
            VersionedMaterializationStreamRegistration registration,
            StreamId streamId,
            int shard) {
        if (!registration.key().equals(keys.materializationRegistryKey(streamId))
                || keys.materializationRegistryShard(streamId) != shard
                || registration.value().metadataVersion() != registration.metadataVersion()) {
            throw invariant("BookKeeper stream registration key/version is not canonical");
        }
    }

    private static void requireAuthorities(
            VersionedMaterializationStreamRegistration registration,
            StorageProfile expectedProfile,
            StreamMetadataSnapshot snapshot,
            ManagedLedgerStreamProjection projection) {
        MaterializationStreamRegistrationRecord value = registration.value();
        StreamId streamId = new StreamId(value.streamId());
        if (!snapshot.metadata().streamId().equals(streamId.value())
                || !snapshot.metadata().profile().equals(expectedProfile.name())
                || value.lastHintCommitVersion() > snapshot.committedEnd().commitVersion()) {
            throw invariant("BookKeeper registration and L0 stream authority disagree");
        }
        final StreamState streamState;
        try {
            streamState = StreamState.valueOf(snapshot.metadata().state());
        } catch (IllegalArgumentException failure) {
            throw invariant("BookKeeper stream has an unknown L0 lifecycle", failure);
        }
        if (streamState != StreamState.ACTIVE && streamState != StreamState.SEALED) {
            throw invariant("registered BookKeeper stream is not ACTIVE or SEALED");
        }
        VersionedVirtualLedgerProjection binding = projection.streamBinding()
                .orElseThrow(() -> invariant("BookKeeper stream projection binding is absent"));
        VersionedTopicProjection topic = projection.currentTopic()
                .orElseThrow(() -> invariant("BookKeeper current topic projection is absent"));
        TopicProjectionRecord topicValue = topic.value();
        if (!projection.streamId().equals(streamId)
                || !binding.value().identity().streamId().equals(streamId.value())
                || !topicValue.streamId().equals(streamId.value())
                || !topicValue.storageProfile().equals(expectedProfile.name())
                || !topicValue.streamName().equals(snapshot.metadata().streamName())
                || topicValue.createdAtMillis() != snapshot.metadata().createdAtMillis()) {
            throw invariant("BookKeeper L0 and F2 projection identities disagree");
        }
        ManagedLedgerGenerationProjectionRefV1 expectedRef =
                new ManagedLedgerGenerationProjectionRefV1(
                        topicValue.managedLedgerName(), topicValue.projectionIdentity());
        Optional<ProjectionRef> registrationRef;
        try {
            registrationRef = ProjectionIdentity.decode(value.projectionRef());
        } catch (RuntimeException failure) {
            throw invariant("BookKeeper registration projection reference is malformed", failure);
        }
        if (!registrationRef.equals(Optional.of(expectedRef.toProjectionRef()))
                || !value.projectionIdentitySha256()
                        .equals(expectedRef.projectionIdentitySha256().value())) {
            throw invariant("BookKeeper registration does not identify the exact F2 projection");
        }
    }

    private static StorageProfile parseProfile(String value) {
        try {
            return StorageProfile.valueOf(value);
        } catch (IllegalArgumentException failure) {
            throw invariant("registered stream has an unknown storage profile", failure);
        }
    }

    private static boolean isBookKeeper(StorageProfile profile) {
        return profile == StorageProfile.BOOKKEEPER_WAL_ONLY
                || profile == StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT
                || profile == StorageProfile.BOOKKEEPER_WAL_SYNC_OBJECT;
    }

    private final class Accumulator {
        private final BookKeeperBrokerReadiness readiness;
        private final MessageDigest digest = sha256();
        private int shardsScanned;
        private long registrationsScanned;
        private long bookKeeperStreamsVerified;
        private int activeShard = -1;
        private String previousKey;

        private Accumulator(BookKeeperBrokerReadiness readiness) {
            this.readiness = readiness;
            frame(digest, DOMAIN);
            frame(digest, cluster);
            frame(digest, configuration.configurationBindingSha256().value());
            frame(digest, ledgerIdNamespaceSha256);
            number(digest, readiness.brokerReadinessEpoch());
            frame(digest, readiness.brokerSetSha256().value());
        }

        private void observeKey(int shard, String key) {
            if (activeShard != shard) {
                activeShard = shard;
                previousKey = null;
            }
            if (previousKey != null && previousKey.compareTo(key) >= 0) {
                throw invariant("BookKeeper stream registration scan is not strictly ordered and unique");
            }
            previousKey = key;
        }

        private void completeShard(int shard) {
            if (shard != shardsScanned) {
                throw invariant("BookKeeper stream shards were not completed in canonical order");
            }
            shardsScanned++;
            previousKey = null;
        }

        private void stream(
                VersionedMaterializationStreamRegistration registration,
                StreamMetadataSnapshot snapshot,
                ManagedLedgerStreamProjection projection) {
            VersionedVirtualLedgerProjection binding = projection.streamBinding().orElseThrow();
            VersionedTopicProjection topic = projection.currentTopic().orElseThrow();
            frame(digest, registration.key());
            number(digest, registration.metadataVersion());
            frame(digest, registration.durableValueSha256().value());
            frame(digest, binding.key());
            number(digest, binding.metadataVersion());
            frame(digest, binding.durableValueSha256().value());
            frame(digest, topic.key());
            number(digest, topic.metadataVersion());
            frame(digest, topic.durableValueSha256().value());
            frame(digest, snapshot.metadata().streamId());
            frame(digest, snapshot.metadata().profile());
            frame(digest, snapshot.metadata().state());
            number(digest, snapshot.metadata().policyVersion());
            number(digest, snapshot.committedEnd().commitVersion());
            number(digest, snapshot.committedEnd().committedEndOffset());
            number(digest, snapshot.trim().trimOffset());
        }

        private BookKeeperStreamCoverageProof finish() {
            number(digest, bookKeeperStreamsVerified);
            return new BookKeeperStreamCoverageProof(
                    readiness.brokerReadinessEpoch(),
                    readiness.brokerSetSha256(),
                    shardsScanned,
                    registrationsScanned,
                    bookKeeperStreamsVerified,
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
        return invariant(message, null);
    }

    private static NereusException invariant(String message, Throwable cause) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION, false, message, cause);
    }

    private static String text(String value, String name) {
        String exact = Objects.requireNonNull(value, name);
        if (exact.isBlank()) {
            throw new IllegalArgumentException(name + " cannot be blank");
        }
        return exact;
    }
}
