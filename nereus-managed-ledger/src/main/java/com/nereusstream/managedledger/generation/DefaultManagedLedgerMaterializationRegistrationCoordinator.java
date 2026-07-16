/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.generation;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamState;
import com.nereusstream.metadata.oxia.F4MetadataConditionFailedException;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.ManagedLedgerFacadeState;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionMetadataStore;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import com.nereusstream.metadata.oxia.OxiaKeyspace;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.ProjectionIdentity;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.VersionedMaterializationStreamRegistration;
import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import java.time.Clock;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Creates or refreshes the hint-only 64-shard stream registration before a
 * Nereus topic incarnation is returned to broker code.
 */
public final class DefaultManagedLedgerMaterializationRegistrationCoordinator
        implements ManagedLedgerMaterializationRegistrationCoordinator {
    private static final int MAX_REFRESH_ATTEMPTS = 32;
    private static final Map<String, String> PAYLOAD_ATTRIBUTES = Map.of(
            ManagedLedgerProjectionNames.PAYLOAD_MAPPING_ATTRIBUTE,
            ManagedLedgerProjectionNames.PAYLOAD_MAPPING_V1);

    private final String cluster;
    private final ManagedLedgerProjectionMetadataStore projections;
    private final OxiaMetadataStore l0;
    private final GenerationMetadataStore generations;
    private final Clock clock;

    public DefaultManagedLedgerMaterializationRegistrationCoordinator(
            String cluster,
            ManagedLedgerProjectionMetadataStore projections,
            OxiaMetadataStore l0,
            GenerationMetadataStore generations,
            Clock clock) {
        this.cluster = new OxiaKeyspace(cluster).cluster();
        this.projections = Objects.requireNonNull(projections, "projections");
        this.l0 = Objects.requireNonNull(l0, "l0");
        this.generations = Objects.requireNonNull(generations, "generations");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public CompletableFuture<Void> ensureRegistered(
            String managedLedgerName,
            ManagedLedgerProjectionIdentity expectedProjectionIdentity) {
        final String exactName;
        try {
            exactName = ManagedLedgerProjectionNames.requireManagedLedgerName(
                    managedLedgerName);
            Objects.requireNonNull(
                    expectedProjectionIdentity,
                    "expectedProjectionIdentity");
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return capture(exactName, expectedProjectionIdentity)
                .thenCompose(authority -> createOrRefresh(authority, 0)
                        .thenCompose(registered -> finalRevalidate(
                                authority, registered)));
    }

    private CompletableFuture<RegistrationAuthority> capture(
            String managedLedgerName,
            ManagedLedgerProjectionIdentity expectedProjectionIdentity) {
        return projections.getProjection(cluster, managedLedgerName)
                .thenCompose(optional -> {
                    TopicProjectionRecord projection = optional.orElseThrow(() ->
                            notReady("authoritative topic projection is absent"));
                    requireExpectedProjection(
                            projection, expectedProjectionIdentity);
                    StreamId streamId = new StreamId(projection.streamId());
                    return l0.getStreamSnapshot(cluster, streamId)
                            .thenApply(snapshot -> authority(
                                    projection, snapshot));
                });
    }

    private RegistrationAuthority authority(
            TopicProjectionRecord projection,
            StreamMetadataSnapshot stream) {
        requireLiveAndCompatible(projection, stream);
        ManagedLedgerGenerationProjectionRefV1 reference =
                new ManagedLedgerGenerationProjectionRefV1(
                        projection.managedLedgerName(),
                        projection.projectionIdentity());
        ProjectionRef projectionRef = reference.toProjectionRef();
        Checksum projectionDigest = reference.projectionIdentitySha256();
        return new RegistrationAuthority(
                projection.managedLedgerName(),
                projection.projectionIdentity(),
                new StreamId(projection.streamId()),
                ProjectionIdentity.encode(Optional.of(projectionRef)),
                projectionDigest,
                projection.storageProfile(),
                stream.committedEnd().commitVersion());
    }

    private CompletableFuture<VersionedMaterializationStreamRegistration>
            createOrRefresh(
                    RegistrationAuthority authority,
                    int attempt) {
        if (attempt >= MAX_REFRESH_ATTEMPTS) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.METADATA_CONDITION_FAILED,
                    true,
                    "materialization registration refresh retry budget exhausted"));
        }
        long now = clock.millis();
        MaterializationStreamRegistrationRecord candidate =
                new MaterializationStreamRegistrationRecord(
                        1,
                        authority.streamId().value(),
                        authority.projectionRef(),
                        authority.projectionIdentitySha256().value(),
                        authority.storageProfile(),
                        now,
                        authority.commitVersionHint(),
                        now,
                        0);
        return generations.createOrVerifyStreamRegistration(
                        cluster, candidate)
                .thenCompose(current -> refresh(
                        authority, current, attempt));
    }

    private CompletableFuture<VersionedMaterializationStreamRegistration>
            refresh(
                    RegistrationAuthority authority,
                    VersionedMaterializationStreamRegistration current,
                    int attempt) {
        requireRegistrationIdentity(authority, current);
        if (current.value().lastHintCommitVersion()
                >= authority.commitVersionHint()) {
            return CompletableFuture.completedFuture(current);
        }
        MaterializationStreamRegistrationRecord replacement =
                new MaterializationStreamRegistrationRecord(
                        current.value().schemaVersion(),
                        current.value().streamId(),
                        current.value().projectionRef(),
                        current.value().projectionIdentitySha256(),
                        current.value().storageProfile(),
                        current.value().registeredAtMillis(),
                        authority.commitVersionHint(),
                        Math.max(
                                current.value().updatedAtMillis(),
                                clock.millis()),
                        0);
        CompletableFuture<VersionedMaterializationStreamRegistration> write =
                generations.compareAndSetStreamRegistration(
                        cluster,
                        replacement,
                        current.metadataVersion());
        return write.handle((updated, error) -> {
                    if (error == null) {
                        return CompletableFuture.completedFuture(updated);
                    }
                    Throwable cause = unwrap(error);
                    return generations.getStreamRegistration(
                                    cluster, authority.streamId())
                            .thenCompose(reloaded -> {
                                if (reloaded.isPresent()) {
                                    VersionedMaterializationStreamRegistration
                                            actual = reloaded.orElseThrow();
                                    requireRegistrationIdentity(
                                            authority, actual);
                                    if (actual.value()
                                                    .lastHintCommitVersion()
                                            >= authority
                                                    .commitVersionHint()) {
                                        return CompletableFuture
                                                .completedFuture(actual);
                                    }
                                }
                                if (cause
                                        instanceof
                                        F4MetadataConditionFailedException) {
                                    return createOrRefresh(
                                            authority, attempt + 1);
                                }
                                return CompletableFuture.failedFuture(cause);
                            });
                })
                .thenCompose(value -> value);
    }

    private CompletableFuture<Void> finalRevalidate(
            RegistrationAuthority expected,
            VersionedMaterializationStreamRegistration registered) {
        requireRegistrationIdentity(expected, registered);
        return capture(
                        expected.managedLedgerName(),
                        expected.projectionIdentity())
                .thenCompose(current -> {
                    if (!current.sameImmutableIdentity(expected)) {
                        return CompletableFuture.failedFuture(notReady(
                                "projection or L0 registration authority changed"));
                    }
                    return generations.getStreamRegistration(
                                    cluster, expected.streamId())
                            .thenAccept(optional -> {
                                VersionedMaterializationStreamRegistration
                                        finalRegistration =
                                                optional.orElseThrow(() ->
                                                        notReady(
                                                                "materialization registration disappeared"));
                                requireRegistrationIdentity(
                                        expected, finalRegistration);
                                if (finalRegistration.value()
                                                .lastHintCommitVersion()
                                        < expected.commitVersionHint()) {
                                    throw notReady(
                                            "materialization registration hint moved backward");
                                }
                            });
                });
    }

    private static void requireExpectedProjection(
            TopicProjectionRecord projection,
            ManagedLedgerProjectionIdentity expected) {
        if (!projection.projectionIdentity().equals(expected)) {
            throw notReady(
                    "authoritative topic projection identity changed");
        }
    }

    private static void requireLiveAndCompatible(
            TopicProjectionRecord projection,
            StreamMetadataSnapshot stream) {
        ManagedLedgerFacadeState facade = projection.parsedFacadeState();
        if (facade != ManagedLedgerFacadeState.OPEN
                && facade != ManagedLedgerFacadeState.SEALED) {
            throw notReady("topic projection is not live");
        }
        if (!stream.metadata().streamId().equals(projection.streamId())
                || !stream.metadata().streamName()
                        .equals(projection.streamName())
                || !stream.metadata().profile()
                        .equals(projection.storageProfile())
                || !stream.metadata().attributes()
                        .equals(PAYLOAD_ATTRIBUTES)
                || stream.metadata().createdAtMillis()
                        != projection.createdAtMillis()) {
            throw invariant(
                    "L0 stream identity/profile differs from topic projection");
        }
        final StorageProfile profile;
        final StreamState state;
        try {
            profile = StorageProfile.valueOf(stream.metadata().profile());
            state = StreamState.valueOf(stream.metadata().state());
        } catch (IllegalArgumentException failure) {
            throw invariant(
                    "L0 stream has an unknown profile or lifecycle",
                    failure);
        }
        if (profile.canonical()
                        != StorageProfile.valueOf(
                                        projection.storageProfile())
                                .canonical()
                || (state != StreamState.ACTIVE
                        && state != StreamState.SEALED)) {
            throw notReady(
                    "L0 stream is not live under the projection profile");
        }
        if (facade == ManagedLedgerFacadeState.SEALED
                && state != StreamState.SEALED) {
            throw invariant(
                    "topic projection lifecycle leads L0 stream truth");
        }
    }

    private static void requireRegistrationIdentity(
            RegistrationAuthority expected,
            VersionedMaterializationStreamRegistration actual) {
        MaterializationStreamRegistrationRecord value = actual.value();
        if (!value.streamId().equals(expected.streamId().value())
                || !value.projectionRef()
                        .equals(expected.projectionRef())
                || !value.projectionIdentitySha256()
                        .equals(expected
                                .projectionIdentitySha256()
                                .value())
                || !value.storageProfile()
                        .equals(expected.storageProfile())) {
            throw invariant(
                    "materialization registration identity conflicts with projection");
        }
    }

    private static NereusException notReady(String message) {
        return new NereusException(
                ErrorCode.METADATA_CONDITION_FAILED,
                true,
                message);
    }

    private static NereusException invariant(String message) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION,
                false,
                message);
    }

    private static NereusException invariant(
            String message, Throwable cause) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION,
                false,
                message,
                cause);
    }

    private static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while (current instanceof CompletionException
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private record RegistrationAuthority(
            String managedLedgerName,
            ManagedLedgerProjectionIdentity projectionIdentity,
            StreamId streamId,
            String projectionRef,
            Checksum projectionIdentitySha256,
            String storageProfile,
            long commitVersionHint) {
        private RegistrationAuthority {
            Objects.requireNonNull(
                    managedLedgerName, "managedLedgerName");
            Objects.requireNonNull(
                    projectionIdentity, "projectionIdentity");
            Objects.requireNonNull(streamId, "streamId");
            Objects.requireNonNull(projectionRef, "projectionRef");
            Objects.requireNonNull(
                    projectionIdentitySha256,
                    "projectionIdentitySha256");
            Objects.requireNonNull(storageProfile, "storageProfile");
            if (commitVersionHint < 0) {
                throw new IllegalArgumentException(
                        "commitVersionHint must be non-negative");
            }
        }

        private boolean sameImmutableIdentity(
                RegistrationAuthority other) {
            return managedLedgerName.equals(other.managedLedgerName)
                    && projectionIdentity.equals(
                            other.projectionIdentity)
                    && streamId.equals(other.streamId)
                    && projectionRef.equals(other.projectionRef)
                    && projectionIdentitySha256.equals(
                            other.projectionIdentitySha256)
                    && storageProfile.equals(other.storageProfile);
        }
    }
}
