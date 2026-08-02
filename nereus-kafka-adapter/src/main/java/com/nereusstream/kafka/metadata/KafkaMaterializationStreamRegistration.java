/* Licensed under the Apache License, Version 2.0 */

package com.nereusstream.kafka.metadata;

import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.materialization.DirectMaterializationStreamAuthority;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.VersionedMaterializationStreamRegistration;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Idempotently installs the projection-free F4 registration required by native Kafka compaction.
 */
public final class KafkaMaterializationStreamRegistration {
    private final String cluster;
    private final GenerationMetadataStore generations;
    private final Clock clock;

    public KafkaMaterializationStreamRegistration(String cluster, GenerationMetadataStore generations, Clock clock) {
        this.cluster = requireText(cluster, "cluster");
        this.generations = Objects.requireNonNull(generations, "generations");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public CompletableFuture<VersionedMaterializationStreamRegistration> ensure(
            StreamId streamId, StorageProfile profile) {
        StreamId exactStream = Objects.requireNonNull(streamId, "streamId");
        StorageProfile exactProfile = Objects.requireNonNull(profile, "profile").canonical();
        if (!exactProfile.objectMaterializationEnabled()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "Kafka compaction registration requires an object-materialization" + " profile"));
        }
        long now = Math.max(0, clock.millis());
        MaterializationStreamRegistrationRecord candidate = new MaterializationStreamRegistrationRecord(
                1,
                exactStream.value(),
                DirectMaterializationStreamAuthority.encodedProjectionRef(),
                DirectMaterializationStreamAuthority.identitySha256(exactStream, exactProfile)
                        .value(),
                exactProfile.name(),
                now,
                0,
                now,
                0);
        return generations.createOrVerifyStreamRegistration(cluster, candidate).thenApply(registration -> {
            requireExact(registration, exactStream, exactProfile);
            return registration;
        });
    }

    private static void requireExact(
            VersionedMaterializationStreamRegistration registration, StreamId streamId, StorageProfile profile) {
        VersionedMaterializationStreamRegistration exact = Objects.requireNonNull(registration, "registration");
        MaterializationStreamRegistrationRecord value = exact.value();
        if (!value.streamId().equals(streamId.value())
                || !value.storageProfile().equals(profile.name())
                || !value.projectionRef().equals(DirectMaterializationStreamAuthority.encodedProjectionRef())
                || !value.projectionIdentitySha256()
                        .equals(DirectMaterializationStreamAuthority.identitySha256(streamId, profile)
                                .value())) {
            throw new IllegalStateException("Kafka materialization registration differs from direct-stream authority");
        }
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }
}
