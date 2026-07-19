/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.DurabilityLevel;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.target.ReadTargetType;
import com.nereusstream.core.profile.AppendAckBoundary;
import com.nereusstream.core.profile.ObjectPublicationMode;
import org.junit.jupiter.api.Test;

class BookKeeperStorageProfileResolverTest {
    private final BookKeeperStorageProfileResolver resolver = new BookKeeperStorageProfileResolver();

    @Test
    void admitsBkOnlyWithBothExactAdapters() {
        for (DurabilityLevel durability : DurabilityLevel.values()) {
            var plan = resolver.requireExecutable(
                    StorageProfile.BOOKKEEPER_WAL_ONLY, durability, true, true);
            assertThat(plan.primaryTargetType()).isEqualTo(ReadTargetType.BOOKKEEPER_ENTRY_RANGE);
            assertThat(plan.publicationMode()).isEqualTo(ObjectPublicationMode.DISABLED);
            assertThat(plan.allowedDurability()).isEqualTo(durability);
            assertThat(plan.ackBoundary()).isEqualTo(
                    durability == DurabilityLevel.WAL_DURABLE
                            ? AppendAckBoundary.STABLE_HEAD
                            : AppendAckBoundary.GENERATION_ZERO_VISIBLE);
        }
    }

    @Test
    void admitsAsyncObjectAtStableHeadOnly() {
        var plan = resolver.requireExecutable(
                StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT,
                DurabilityLevel.WAL_DURABLE,
                true,
                true);
        assertThat(plan.primaryTargetType()).isEqualTo(ReadTargetType.BOOKKEEPER_ENTRY_RANGE);
        assertThat(plan.publicationMode()).isEqualTo(ObjectPublicationMode.ASYNCHRONOUS);
        assertThat(plan.ackBoundary()).isEqualTo(AppendAckBoundary.STABLE_HEAD);

        assertThatThrownBy(() -> resolver.requireExecutable(
                        StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT,
                        DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED,
                        true,
                        true))
                .isInstanceOfSatisfying(
                        NereusException.class,
                        failure -> assertThat(failure.code())
                                .isEqualTo(ErrorCode.UNSUPPORTED_DURABILITY_LEVEL));
    }

    @Test
    void rejectsObjectFutureBkProfilesAndMissingAdaptersBeforeIo() {
        for (StorageProfile profile : StorageProfile.values()) {
            if (profile.canonical() == StorageProfile.BOOKKEEPER_WAL_ONLY
                    || profile.canonical() == StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT) {
                continue;
            }
            assertUnsupported(() -> resolver.requireExecutable(
                    profile, profile.defaultDurabilityLevel(), true, true));
        }
        assertUnsupported(() -> resolver.requireExecutable(
                StorageProfile.BOOKKEEPER_WAL_ONLY, DurabilityLevel.WAL_DURABLE, false, true));
        assertUnsupported(() -> resolver.requireExecutable(
                StorageProfile.BOOKKEEPER_WAL_ONLY, DurabilityLevel.WAL_DURABLE, true, false));
    }

    @Test
    void admitsReadsOnlyForBkOnlyWithTheExactReader() {
        assertThat(resolver.requireReadable(
                StorageProfile.BOOKKEEPER_WAL_ONLY,
                type -> type == ReadTargetType.BOOKKEEPER_ENTRY_RANGE))
                .isEqualTo(ReadTargetType.BOOKKEEPER_ENTRY_RANGE);
        assertUnsupported(() -> resolver.requireReadable(
                StorageProfile.BOOKKEEPER_WAL_ONLY,
                type -> false));
        assertThat(resolver.requireReadable(
                StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT,
                type -> type == ReadTargetType.BOOKKEEPER_ENTRY_RANGE))
                .isEqualTo(ReadTargetType.BOOKKEEPER_ENTRY_RANGE);
    }

    private static void assertUnsupported(Runnable operation) {
        assertThatThrownBy(operation::run).isInstanceOfSatisfying(
                NereusException.class,
                failure -> assertThat(failure.code()).isEqualTo(ErrorCode.UNSUPPORTED_STORAGE_PROFILE));
    }
}
