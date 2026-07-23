/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.activation;

import com.nereusstream.api.StorageProfile;
import com.nereusstream.metadata.oxia.records.KafkaStorageProtocolActivationRecord;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Controller-owned immutable policy for the first native-storage activation. */
public record KafkaStorageActivationPolicy(
        String kafkaClusterId,
        List<String> allowedStorageProfiles,
        String defaultStorageProfile,
        Duration readinessTtl) {
    public KafkaStorageActivationPolicy(
            String kafkaClusterId,
            Set<StorageProfile> allowedStorageProfiles,
            StorageProfile defaultStorageProfile,
            Duration readinessTtl) {
        this(
                kafkaClusterId,
                canonicalProfiles(allowedStorageProfiles),
                Objects.requireNonNull(defaultStorageProfile, "defaultStorageProfile").name(),
                readinessTtl);
    }

    public KafkaStorageActivationPolicy {
        Objects.requireNonNull(kafkaClusterId, "kafkaClusterId");
        if (kafkaClusterId.isBlank()) {
            throw new IllegalArgumentException("kafkaClusterId must be nonblank");
        }
        allowedStorageProfiles = List.copyOf(Objects.requireNonNull(
                allowedStorageProfiles, "allowedStorageProfiles"));
        if (allowedStorageProfiles.isEmpty()
                || allowedStorageProfiles.size()
                        > KafkaStorageProtocolActivationRecord.MAX_STORAGE_PROFILES) {
            throw new IllegalArgumentException("allowedStorageProfiles must contain between one and five values");
        }
        String previous = null;
        for (String name : allowedStorageProfiles) {
            StorageProfile profile = parseCanonical(name);
            if (previous != null && previous.compareTo(profile.name()) >= 0) {
                throw new IllegalArgumentException("allowedStorageProfiles must be sorted and unique");
            }
            previous = profile.name();
        }
        StorageProfile exactDefault = parseCanonical(defaultStorageProfile);
        defaultStorageProfile = exactDefault.name();
        if (!allowedStorageProfiles.contains(defaultStorageProfile)) {
            throw new IllegalArgumentException("defaultStorageProfile must be allowed");
        }
        Objects.requireNonNull(readinessTtl, "readinessTtl");
        if (readinessTtl.isZero() || readinessTtl.isNegative() || readinessTtl.toMillis() <= 0) {
            throw new IllegalArgumentException("readinessTtl must be positive and millisecond-representable");
        }
    }

    private static List<String> canonicalProfiles(Set<StorageProfile> supplied) {
        Set<StorageProfile> profiles = Set.copyOf(Objects.requireNonNull(supplied, "allowedStorageProfiles"));
        if (profiles.isEmpty()) {
            throw new IllegalArgumentException("allowedStorageProfiles must be non-empty");
        }
        for (StorageProfile profile : profiles) {
            if (profile.canonical() != profile) {
                throw new IllegalArgumentException("allowedStorageProfiles cannot contain aliases");
            }
        }
        return profiles.stream().map(Enum::name).sorted(Comparator.naturalOrder()).toList();
    }

    private static StorageProfile parseCanonical(String supplied) {
        Objects.requireNonNull(supplied, "storageProfile");
        final StorageProfile profile;
        try {
            profile = StorageProfile.valueOf(supplied);
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException("unknown storage profile " + supplied, failure);
        }
        if (profile.canonical() != profile) {
            throw new IllegalArgumentException("storage profile aliases are not allowed");
        }
        return profile;
    }
}
