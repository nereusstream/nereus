/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.partition;

import com.nereusstream.api.AppendCompletionPolicy;
import com.nereusstream.api.DurabilityLevel;
import com.nereusstream.api.StorageProfile;
import java.util.Objects;
import java.util.Set;

/** Immutable Kafka append policy that preserves each activated Nereus profile's exact success predicate. */
public record KafkaStorageProfilePolicy(
        StorageProfile storageProfile,
        DurabilityLevel durabilityLevel,
        AppendCompletionPolicy completionPolicy) {
    private static final Set<StorageProfile> ACTIVATED_PROFILES = Set.of(
            StorageProfile.OBJECT_WAL_SYNC_OBJECT,
            StorageProfile.OBJECT_WAL_ASYNC_OBJECT,
            StorageProfile.BOOKKEEPER_WAL_ONLY,
            StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT,
            StorageProfile.BOOKKEEPER_WAL_SYNC_OBJECT);

    public KafkaStorageProfilePolicy {
        Objects.requireNonNull(storageProfile, "storageProfile");
        Objects.requireNonNull(durabilityLevel, "durabilityLevel");
        Objects.requireNonNull(completionPolicy, "completionPolicy");
        if (!ACTIVATED_PROFILES.contains(storageProfile)) {
            throw new IllegalArgumentException("Kafka bindings require an explicitly activated storage profile");
        }
        if (durabilityLevel != storageProfile.defaultDurabilityLevel()) {
            throw new IllegalArgumentException("Kafka durability must equal the storage-profile default");
        }
        if (completionPolicy != AppendCompletionPolicy.PROFILE_DEFAULT) {
            throw new IllegalArgumentException(
                    "Kafka Produce must preserve the storage-profile default completion predicate");
        }
    }

    public static KafkaStorageProfilePolicy forProfile(StorageProfile storageProfile) {
        StorageProfile exact = Objects.requireNonNull(storageProfile, "storageProfile");
        return new KafkaStorageProfilePolicy(
                exact, exact.defaultDurabilityLevel(), AppendCompletionPolicy.PROFILE_DEFAULT);
    }

    public static Set<StorageProfile> activatedProfiles() {
        return ACTIVATED_PROFILES;
    }
}
