/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.profile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.AppendCompletionPolicy;
import com.nereusstream.api.DurabilityLevel;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.target.ReadTargetType;
import java.util.Map;
import org.junit.jupiter.api.Test;

class StorageProfileResolverRegistryTest {
    private final Phase4StorageProfileResolver objectResolver = new Phase4StorageProfileResolver();

    @Test
    void dispatchesCanonicalProfilesWithoutChangingDelegateSemantics() {
        StorageProfileResolverRegistry registry = new StorageProfileResolverRegistry(Map.of(
                StorageProfile.OBJECT_WAL_SYNC_OBJECT, objectResolver,
                StorageProfile.OBJECT_WAL_ASYNC_OBJECT, objectResolver));

        assertThat(registry.supports(StorageProfile.OBJECT_WAL)).isTrue();
        assertThat(registry.requireExecutable(
                        StorageProfile.OBJECT_WAL,
                        DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED,
                        AppendCompletionPolicy.GENERATION_ZERO_INDEX,
                        true,
                        true,
                        false)
                .primaryTargetType())
                .isEqualTo(ReadTargetType.OBJECT_SLICE);
        assertThat(registry.requireReadable(
                        StorageProfile.OBJECT_WAL_ASYNC_OBJECT,
                        type -> type == ReadTargetType.OBJECT_SLICE))
                .isEqualTo(ReadTargetType.OBJECT_SLICE);
    }

    @Test
    void rejectsMissingAndDuplicateCanonicalBindings() {
        StorageProfileResolverRegistry registry = new StorageProfileResolverRegistry(Map.of(
                StorageProfile.OBJECT_WAL_SYNC_OBJECT, objectResolver));

        assertThatThrownBy(() -> registry.requireExecutable(
                        StorageProfile.BOOKKEEPER_WAL_ONLY,
                        DurabilityLevel.WAL_DURABLE,
                        true,
                        true))
                .isInstanceOf(NereusException.class)
                .hasMessageContaining("no storage-profile resolver");
        assertThatThrownBy(() -> new StorageProfileResolverRegistry(Map.of(
                        StorageProfile.OBJECT_WAL, objectResolver,
                        StorageProfile.OBJECT_WAL_SYNC_OBJECT, objectResolver)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate");
    }
}
