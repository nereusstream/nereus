/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.core.profile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.DurabilityLevel;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.target.ReadTargetType;
import org.junit.jupiter.api.Test;

class Phase4StorageProfileResolverTest {
    private final Phase4StorageProfileResolver resolver =
            new Phase4StorageProfileResolver();

    @Test
    void acceptsOnlyTheFrozenObjectWalMatrix() {
        StorageExecutionPlan sync = resolver.requireExecutable(
                StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED,
                true,
                true);
        assertThat(sync.profile())
                .isEqualTo(StorageProfile.OBJECT_WAL_SYNC_OBJECT);
        assertThat(sync.primaryTargetType())
                .isEqualTo(ReadTargetType.OBJECT_SLICE);
        assertThat(sync.publicationMode())
                .isEqualTo(ObjectPublicationMode.SYNCHRONOUS);

        for (DurabilityLevel durability : DurabilityLevel.values()) {
            StorageExecutionPlan async = resolver.requireExecutable(
                    StorageProfile.OBJECT_WAL_ASYNC_OBJECT,
                    durability,
                    true,
                    true);
            assertThat(async.profile())
                    .isEqualTo(StorageProfile.OBJECT_WAL_ASYNC_OBJECT);
            assertThat(async.publicationMode())
                    .isEqualTo(ObjectPublicationMode.ASYNCHRONOUS);
            assertThat(async.allowedDurability()).isEqualTo(durability);
        }
    }

    @Test
    @SuppressWarnings("deprecation")
    void canonicalLegacyObjectWalRemainsStrict() {
        StorageExecutionPlan plan = resolver.requireExecutable(
                StorageProfile.OBJECT_WAL,
                DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED,
                true,
                true);

        assertThat(plan.profile())
                .isEqualTo(StorageProfile.OBJECT_WAL_SYNC_OBJECT);
        assertThatThrownBy(() -> resolver.requireExecutable(
                        StorageProfile.OBJECT_WAL,
                        DurabilityLevel.WAL_DURABLE,
                        true,
                        true))
                .isInstanceOfSatisfying(
                        NereusException.class,
                        failure -> assertThat(failure.code())
                                .isEqualTo(
                                        ErrorCode
                                                .UNSUPPORTED_DURABILITY_LEVEL));
    }

    @Test
    void rejectsBookKeeperAndIncompletePrimaryAdaptersBeforeIo() {
        for (StorageProfile profile : new StorageProfile[] {
            StorageProfile.BOOKKEEPER_WAL_ONLY,
            StorageProfile.BOOKKEEPER_WAL_SYNC_OBJECT,
            StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT
        }) {
            assertThatThrownBy(() -> resolver.requireExecutable(
                            profile,
                            profile.defaultDurabilityLevel(),
                            true,
                            true))
                    .isInstanceOfSatisfying(
                            NereusException.class,
                            failure -> assertThat(failure.code())
                                    .isEqualTo(
                                            ErrorCode
                                                    .UNSUPPORTED_STORAGE_PROFILE));
        }

        assertThatThrownBy(() -> resolver.requireExecutable(
                        StorageProfile.OBJECT_WAL_ASYNC_OBJECT,
                        DurabilityLevel.WAL_DURABLE,
                        false,
                        true))
                .isInstanceOfSatisfying(
                        NereusException.class,
                        failure -> assertThat(failure.code())
                                .isEqualTo(
                                        ErrorCode.UNSUPPORTED_STORAGE_PROFILE));
        assertThatThrownBy(() -> resolver.requireExecutable(
                        StorageProfile.OBJECT_WAL_ASYNC_OBJECT,
                        DurabilityLevel.WAL_DURABLE,
                        true,
                        false))
                .isInstanceOfSatisfying(
                        NereusException.class,
                        failure -> assertThat(failure.code())
                                .isEqualTo(
                                        ErrorCode.UNSUPPORTED_STORAGE_PROFILE));
    }
}
