/* Licensed under the Apache License, Version 2.0 */

package com.nereusstream.kafka.activation;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamState;
import com.nereusstream.bookkeeper.BookKeeperBrokerReadiness;
import com.nereusstream.bookkeeper.BookKeeperOperationDeadline;
import com.nereusstream.bookkeeper.BookKeeperStreamCoverageProof;
import com.nereusstream.bookkeeper.BookKeeperStreamCoverageProofProvider;
import com.nereusstream.bookkeeper.BookKeeperWalConfiguration;
import com.nereusstream.materialization.DirectMaterializationStreamAuthority;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.KafkaPartitionKeyspace;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataStore;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionRegistry;
import com.nereusstream.metadata.oxia.VersionedMaterializationStreamRegistration;
import com.nereusstream.metadata.oxia.records.KafkaPartitionBindingRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionLifecycle;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Produces NBKKAFKASTREAM1 only after all Kafka binding and materialization registry shards agree
 * with the exact L0 authority for every live BookKeeper-backed partition.
 */
public final class KafkaBookKeeperStreamCoverageProofProducer implements BookKeeperStreamCoverageProofProvider {
    private static final String DOMAIN = "NBKKAFKASTREAM1";

    private final String cluster;
    private final BookKeeperWalConfiguration configuration;
    private final String ledgerIdNamespaceSha256;
    private final GenerationMetadataStore generations;
    private final OxiaMetadataStore l0;
    private final KafkaPartitionMetadataStore bindings;
    private final F4Keyspace generationKeys;
    private final KafkaPartitionKeyspace bindingKeys;
    private final int pageSize;

    public KafkaBookKeeperStreamCoverageProofProducer(
            String cluster,
            String kafkaClusterId,
            BookKeeperWalConfiguration configuration,
            String ledgerIdNamespaceSha256,
            GenerationMetadataStore generations,
            OxiaMetadataStore l0,
            KafkaPartitionMetadataStore bindings) {
        this.cluster = text(cluster, "cluster");
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.ledgerIdNamespaceSha256 = new Checksum(
                        ChecksumType.SHA256, Objects.requireNonNull(ledgerIdNamespaceSha256, "ledgerIdNamespaceSha256"))
                .value();
        this.generations = Objects.requireNonNull(generations, "generations");
        this.l0 = Objects.requireNonNull(l0, "l0");
        this.bindings = Objects.requireNonNull(bindings, "bindings");
        this.generationKeys = new F4Keyspace(cluster);
        this.bindingKeys = new KafkaPartitionKeyspace(cluster, kafkaClusterId);
        this.pageSize = Math.min(configuration.retentionPageSize(), 1_000);
    }

    @Override
    public CompletableFuture<BookKeeperStreamCoverageProof> produce(
            BookKeeperBrokerReadiness readiness, Duration timeout) {
        final Accumulator accumulator;
        final BookKeeperOperationDeadline deadline;
        try {
            accumulator = new Accumulator(Objects.requireNonNull(readiness, "readiness"));
            deadline = new BookKeeperOperationDeadline(timeout);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return scanBindingShard(0, Optional.empty(), accumulator, deadline)
                .thenCompose(ignored -> scanMaterializationShard(0, Optional.empty(), accumulator, deadline))
                .thenApply(ignored -> accumulator.finish());
    }

    private CompletableFuture<Void> scanBindingShard(
            int shard, Optional<String> continuation, Accumulator accumulator, BookKeeperOperationDeadline deadline) {
        if (shard == KafkaPartitionKeyspace.REGISTRY_SHARDS) {
            return CompletableFuture.completedFuture(null);
        }
        return deadline.bound(bindings.scanRegistry(shard, continuation, pageSize))
                .thenCompose(page -> processBindingHints(page.values(), 0, shard, accumulator, deadline)
                        .thenCompose(ignored -> {
                            if (page.continuation().isPresent()) {
                                return scanBindingShard(shard, page.continuation(), accumulator, deadline);
                            }
                            accumulator.completeBindingShard(shard);
                            return scanBindingShard(shard + 1, Optional.empty(), accumulator, deadline);
                        }));
    }

    private CompletableFuture<Void> processBindingHints(
            List<VersionedKafkaPartitionRegistry> hints,
            int index,
            int shard,
            Accumulator accumulator,
            BookKeeperOperationDeadline deadline) {
        if (index == hints.size()) {
            return CompletableFuture.completedFuture(null);
        }
        VersionedKafkaPartitionRegistry hint = hints.get(index);
        accumulator.observeBindingHint(shard, hint.key());
        requireCanonicalHint(shard, hint);
        return deadline.bound(bindings.get(hint.value().identity()))
                .thenCompose(optional -> {
                    VersionedKafkaPartitionBinding binding = optional.orElseThrow(
                            () -> invariant("Kafka registry hint has no authoritative binding root"));
                    requireHintBinding(hint, binding);
                    return observeBinding(hint, binding, accumulator, deadline);
                })
                .thenCompose(ignored -> processBindingHints(hints, index + 1, shard, accumulator, deadline));
    }

    private CompletableFuture<Void> observeBinding(
            VersionedKafkaPartitionRegistry hint,
            VersionedKafkaPartitionBinding binding,
            Accumulator accumulator,
            BookKeeperOperationDeadline deadline) {
        KafkaPartitionBindingRecord value = binding.value();
        StorageProfile profile = profile(value.storageProfile());
        if (!isBookKeeper(profile)) {
            accumulator.binding(hint, binding, null);
            return CompletableFuture.completedFuture(null);
        }
        if (value.lifecycle() == KafkaPartitionLifecycle.DELETED) {
            accumulator.binding(hint, binding, null);
            return CompletableFuture.completedFuture(null);
        }
        if (value.lifecycle() != KafkaPartitionLifecycle.ACTIVE) {
            return CompletableFuture.failedFuture(invariant("live BookKeeper Kafka binding is not ACTIVE"));
        }
        StreamId streamId = new StreamId(value.streamId());
        return deadline.bound(l0.getStreamSnapshot(cluster, streamId)).thenAccept(snapshot -> {
            requireL0Binding(binding, profile, snapshot);
            accumulator.binding(hint, binding, snapshot);
        });
    }

    private CompletableFuture<Void> scanMaterializationShard(
            int shard,
            Optional<F4ScanToken> continuation,
            Accumulator accumulator,
            BookKeeperOperationDeadline deadline) {
        if (shard == F4Keyspace.MATERIALIZATION_REGISTRY_SHARDS) {
            return CompletableFuture.completedFuture(null);
        }
        return deadline.bound(generations.scanStreamRegistrations(cluster, shard, continuation, pageSize))
                .thenCompose(page -> processRegistrations(page.values(), 0, shard, accumulator)
                        .thenCompose(ignored -> {
                            if (page.continuation().isPresent()) {
                                return scanMaterializationShard(shard, page.continuation(), accumulator, deadline);
                            }
                            accumulator.completeMaterializationShard(shard);
                            return scanMaterializationShard(shard + 1, Optional.empty(), accumulator, deadline);
                        }));
    }

    private CompletableFuture<Void> processRegistrations(
            List<VersionedMaterializationStreamRegistration> registrations,
            int index,
            int shard,
            Accumulator accumulator) {
        if (index == registrations.size()) {
            return CompletableFuture.completedFuture(null);
        }
        VersionedMaterializationStreamRegistration registration = registrations.get(index);
        accumulator.observeRegistration(shard, registration.key());
        accumulator.registration(registration, shard);
        return processRegistrations(registrations, index + 1, shard, accumulator);
    }

    private void requireCanonicalHint(int shard, VersionedKafkaPartitionRegistry hint) {
        if (!bindingKeys.parseRegistryKey(shard, hint.key()).equals(hint.value().identity())
                || !hint.value()
                        .bindingRootKey()
                        .equals(bindingKeys.bindingRootKey(hint.value().identity()))) {
            throw invariant("Kafka partition registry key/root identity is not canonical");
        }
    }

    private static void requireHintBinding(
            VersionedKafkaPartitionRegistry hint, VersionedKafkaPartitionBinding binding) {
        if (!binding.key().equals(hint.value().bindingRootKey())
                || !binding.value().identity().equals(hint.value().identity())
                || binding.value().bindingEpoch() < hint.value().bindingEpoch()) {
            throw invariant("Kafka partition registry and authoritative binding disagree");
        }
        if (binding.value().bindingEpoch() == hint.value().bindingEpoch()
                && !MessageDigest.isEqual(
                        HexFormat.of().parseHex(binding.durableValueSha256().value()),
                        hint.value().bindingRootSha256())) {
            throw invariant("Kafka partition registry digest disagrees with its authoritative binding");
        }
    }

    private static void requireL0Binding(
            VersionedKafkaPartitionBinding binding, StorageProfile profile, StreamMetadataSnapshot snapshot) {
        KafkaPartitionBindingRecord value = binding.value();
        final StreamState state;
        try {
            state = StreamState.valueOf(snapshot.metadata().state());
        } catch (IllegalArgumentException failure) {
            throw invariant("BookKeeper Kafka stream has an unknown L0 lifecycle", failure);
        }
        if (!snapshot.metadata().streamId().equals(value.streamId())
                || !snapshot.metadata().streamName().equals(value.streamName())
                || !snapshot.metadata().profile().equals(profile.name())
                || (state != StreamState.ACTIVE && state != StreamState.SEALED)
                || value.observedLogStartOffset() != snapshot.trim().trimOffset()
                || value.observedStableEndOffset() != snapshot.committedEnd().committedEndOffset()) {
            throw invariant("Kafka BookKeeper binding and L0 stream authority disagree");
        }
    }

    private final class Accumulator {
        private final BookKeeperBrokerReadiness readiness;
        private final MessageDigest digest = sha256();
        private final Map<String, BindingAuthority> bookKeeperBindings = new LinkedHashMap<>();
        private final Map<String, BindingAuthority> requiredMaterializations = new LinkedHashMap<>();
        private int bindingShardsScanned;
        private int materializationShardsScanned;
        private long bindingRegistrationsScanned;
        private long materializationRegistrationsScanned;
        private int activeBindingShard = -1;
        private int activeMaterializationShard = -1;
        private String previousBindingKey;
        private String previousRegistrationKey;

        private Accumulator(BookKeeperBrokerReadiness readiness) {
            this.readiness = readiness;
            frame(digest, DOMAIN);
            frame(digest, cluster);
            frame(digest, bindingKeys.kafkaClusterId());
            frame(digest, configuration.configurationBindingSha256().value());
            frame(digest, ledgerIdNamespaceSha256);
            number(digest, readiness.brokerReadinessEpoch());
            frame(digest, readiness.brokerSetSha256().value());
        }

        private void observeBindingHint(int shard, String key) {
            if (activeBindingShard != shard) {
                activeBindingShard = shard;
                previousBindingKey = null;
            }
            if (previousBindingKey != null && previousBindingKey.compareTo(key) >= 0) {
                throw invariant("Kafka partition registry scan is not strictly ordered and unique");
            }
            previousBindingKey = key;
        }

        private void completeBindingShard(int shard) {
            if (shard != bindingShardsScanned) {
                throw invariant("Kafka partition registry shards were not completed in canonical order");
            }
            bindingShardsScanned++;
            previousBindingKey = null;
        }

        private void observeRegistration(int shard, String key) {
            if (activeMaterializationShard != shard) {
                activeMaterializationShard = shard;
                previousRegistrationKey = null;
            }
            if (previousRegistrationKey != null && previousRegistrationKey.compareTo(key) >= 0) {
                throw invariant("Kafka materialization registry scan is not strictly ordered and unique");
            }
            previousRegistrationKey = key;
        }

        private void completeMaterializationShard(int shard) {
            if (shard != materializationShardsScanned) {
                throw invariant("Kafka materialization registry shards were not completed in canonical order");
            }
            materializationShardsScanned++;
            previousRegistrationKey = null;
        }

        private void binding(
                VersionedKafkaPartitionRegistry hint,
                VersionedKafkaPartitionBinding binding,
                StreamMetadataSnapshot snapshot) {
            bindingRegistrationsScanned = Math.addExact(bindingRegistrationsScanned, 1);
            frame(digest, hint.key());
            number(digest, hint.metadataVersion());
            frame(digest, hint.durableValueSha256().value());
            frame(digest, binding.key());
            number(digest, binding.metadataVersion());
            frame(digest, binding.durableValueSha256().value());
            KafkaPartitionBindingRecord value = binding.value();
            frame(digest, value.storageProfile());
            number(digest, value.lifecycleId());
            number(digest, value.bindingEpoch());
            if (snapshot == null) {
                return;
            }
            frame(digest, snapshot.metadata().streamId());
            frame(digest, snapshot.metadata().streamName());
            frame(digest, snapshot.metadata().profile());
            frame(digest, snapshot.metadata().state());
            number(digest, snapshot.metadata().policyVersion());
            number(digest, snapshot.committedEnd().commitVersion());
            number(digest, snapshot.committedEnd().committedEndOffset());
            number(digest, snapshot.trim().trimOffset());
            BindingAuthority authority = new BindingAuthority(binding, snapshot);
            if (bookKeeperBindings.putIfAbsent(value.streamId(), authority) != null) {
                throw invariant("multiple live Kafka bindings identify one BookKeeper stream");
            }
            StorageProfile profile = profile(value.storageProfile());
            if (profile.objectMaterializationEnabled()) {
                requiredMaterializations.put(value.streamId(), authority);
            }
        }

        private void registration(VersionedMaterializationStreamRegistration registration, int shard) {
            materializationRegistrationsScanned = Math.addExact(materializationRegistrationsScanned, 1);
            MaterializationStreamRegistrationRecord value = registration.value();
            StorageProfile profile = profile(value.storageProfile());
            if (!isBookKeeper(profile)) {
                return;
            }
            StreamId streamId = new StreamId(value.streamId());
            if (!registration.key().equals(generationKeys.materializationRegistryKey(streamId))
                    || generationKeys.materializationRegistryShard(streamId) != shard
                    || value.metadataVersion() != registration.metadataVersion()) {
                throw invariant("Kafka BookKeeper materialization registration is not canonical");
            }
            if (!profile.objectMaterializationEnabled()) {
                throw invariant(
                        "BookKeeper WAL-only Kafka stream unexpectedly has an object-materialization registration");
            }
            BindingAuthority authority = requiredMaterializations.remove(streamId.value());
            if (authority == null
                    || !authority.binding().value().storageProfile().equals(profile.name())
                    || !value.projectionRef().equals(DirectMaterializationStreamAuthority.encodedProjectionRef())
                    || !value.projectionIdentitySha256()
                            .equals(DirectMaterializationStreamAuthority.identitySha256(streamId, profile)
                                    .value())
                    || value.lastHintCommitVersion()
                            > authority.snapshot().committedEnd().commitVersion()) {
                throw invariant("Kafka BookKeeper materialization registration and direct stream authority disagree");
            }
            frame(digest, registration.key());
            number(digest, registration.metadataVersion());
            frame(digest, registration.durableValueSha256().value());
        }

        private BookKeeperStreamCoverageProof finish() {
            if (bindingShardsScanned != KafkaPartitionKeyspace.REGISTRY_SHARDS
                    || materializationShardsScanned != F4Keyspace.MATERIALIZATION_REGISTRY_SHARDS) {
                throw invariant("Kafka BookKeeper stream coverage did not scan every registry shard");
            }
            if (!requiredMaterializations.isEmpty()) {
                throw invariant("live BookKeeper Kafka binding lacks its direct materialization registration");
            }
            number(digest, bindingRegistrationsScanned);
            number(digest, materializationRegistrationsScanned);
            number(digest, bookKeeperBindings.size());
            return new BookKeeperStreamCoverageProof(
                    readiness.brokerReadinessEpoch(),
                    readiness.brokerSetSha256(),
                    materializationShardsScanned,
                    bindingRegistrationsScanned,
                    bookKeeperBindings.size(),
                    new Checksum(ChecksumType.SHA256, HexFormat.of().formatHex(digest.digest())));
        }
    }

    private static StorageProfile profile(String value) {
        try {
            StorageProfile parsed = StorageProfile.valueOf(value);
            StorageProfile canonical = parsed.canonical();
            if (!canonical.name().equals(value)) {
                throw invariant("Kafka binding uses a non-canonical storage profile");
            }
            return canonical;
        } catch (IllegalArgumentException failure) {
            throw invariant("Kafka binding contains an unknown storage profile", failure);
        }
    }

    private static boolean isBookKeeper(StorageProfile profile) {
        return profile == StorageProfile.BOOKKEEPER_WAL_ONLY
                || profile == StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT
                || profile == StorageProfile.BOOKKEEPER_WAL_SYNC_OBJECT;
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static void frame(MessageDigest digest, String value) {
        byte[] bytes = Objects.requireNonNull(value, "value").getBytes(StandardCharsets.UTF_8);
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
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message, cause);
    }

    private static String text(String value, String name) {
        String exact = Objects.requireNonNull(value, name);
        if (exact.isBlank()) {
            throw new IllegalArgumentException(name + " cannot be blank");
        }
        return exact;
    }

    private record BindingAuthority(VersionedKafkaPartitionBinding binding, StreamMetadataSnapshot snapshot) {
        private BindingAuthority {
            Objects.requireNonNull(binding, "binding");
            Objects.requireNonNull(snapshot, "snapshot");
        }
    }
}
