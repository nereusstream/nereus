/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.partition;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.AppendCompletionPolicy;
import com.nereusstream.api.DurabilityLevel;
import com.nereusstream.api.StorageProfile;
import java.util.Set;
import org.junit.jupiter.api.Test;

class KafkaStorageProfilePolicyTest {
    @Test
    void preservesTheExactDefaultSuccessPredicateForAllFiveActivatedProfiles() {
        assertThat(KafkaStorageProfilePolicy.activatedProfiles()).containsExactlyInAnyOrderElementsOf(Set.of(
                StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                StorageProfile.OBJECT_WAL_ASYNC_OBJECT,
                StorageProfile.BOOKKEEPER_WAL_ONLY,
                StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT,
                StorageProfile.BOOKKEEPER_WAL_SYNC_OBJECT));
        for (StorageProfile profile : new StorageProfile[] {
                StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                StorageProfile.OBJECT_WAL_ASYNC_OBJECT,
                StorageProfile.BOOKKEEPER_WAL_ONLY,
                StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT,
                StorageProfile.BOOKKEEPER_WAL_SYNC_OBJECT
        }) {
            KafkaStorageProfilePolicy policy = KafkaStorageProfilePolicy.forProfile(profile);
            assertThat(policy.storageProfile()).isEqualTo(profile);
            assertThat(policy.durabilityLevel()).isEqualTo(profile.defaultDurabilityLevel());
            assertThat(policy.completionPolicy()).isEqualTo(AppendCompletionPolicy.PROFILE_DEFAULT);
        }
    }

    @SuppressWarnings("removal")
    @Test
    void rejectsAliasesWeakenedCompletionAndNonDefaultDurability() {
        assertThatThrownBy(() -> KafkaStorageProfilePolicy.forProfile(StorageProfile.OBJECT_WAL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("explicitly activated");
        assertThatThrownBy(() -> new KafkaStorageProfilePolicy(
                        StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                        StorageProfile.OBJECT_WAL_SYNC_OBJECT.defaultDurabilityLevel(),
                        AppendCompletionPolicy.STABLE_HEAD))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("default completion");
        assertThatThrownBy(() -> new KafkaStorageProfilePolicy(
                        StorageProfile.BOOKKEEPER_WAL_ONLY,
                        DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED,
                        AppendCompletionPolicy.PROFILE_DEFAULT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("durability");
    }
}
