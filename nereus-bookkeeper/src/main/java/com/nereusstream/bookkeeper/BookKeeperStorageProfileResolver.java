/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.DurabilityLevel;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.target.ReadTargetType;
import com.nereusstream.core.profile.AppendAckBoundary;
import com.nereusstream.core.profile.ObjectPublicationMode;
import com.nereusstream.core.profile.StorageExecutionPlan;
import com.nereusstream.core.profile.StorageProfileResolver;
import java.util.Objects;
import java.util.function.Predicate;

/** Executable BK-M2 matrix: BK_ONLY with stable-head or strict generation-zero completion. */
public final class BookKeeperStorageProfileResolver implements StorageProfileResolver {
    @Override
    public StorageExecutionPlan requireExecutable(
            StorageProfile raw,
            DurabilityLevel durability,
            boolean primaryAppenderInstalled,
            boolean primaryReaderInstalled) {
        StorageProfile profile = Objects.requireNonNull(raw, "profile").canonical();
        DurabilityLevel exactDurability = Objects.requireNonNull(durability, "durability");
        if (profile != StorageProfile.BOOKKEEPER_WAL_ONLY
                || !primaryAppenderInstalled
                || !primaryReaderInstalled) {
            throw new NereusException(
                    ErrorCode.UNSUPPORTED_STORAGE_PROFILE,
                    false,
                    "storage profile has no complete BK-M2 execution plan",
                    AppendOutcome.KNOWN_NOT_COMMITTED);
        }
        return new StorageExecutionPlan(
                profile,
                ReadTargetType.BOOKKEEPER_ENTRY_RANGE,
                ObjectPublicationMode.DISABLED,
                exactDurability,
                exactDurability == DurabilityLevel.WAL_DURABLE
                        ? AppendAckBoundary.STABLE_HEAD
                        : AppendAckBoundary.GENERATION_ZERO_VISIBLE);
    }

    @Override
    public ReadTargetType requireReadable(
            StorageProfile raw, Predicate<ReadTargetType> readerInstalled) {
        StorageProfile profile = Objects.requireNonNull(raw, "profile").canonical();
        Predicate<ReadTargetType> installed = Objects.requireNonNull(readerInstalled, "readerInstalled");
        if (profile != StorageProfile.BOOKKEEPER_WAL_ONLY
                || !installed.test(ReadTargetType.BOOKKEEPER_ENTRY_RANGE)) {
            throw new NereusException(ErrorCode.UNSUPPORTED_STORAGE_PROFILE, false,
                    "storage profile has no complete BK-M2 read plan");
        }
        return ReadTargetType.BOOKKEEPER_ENTRY_RANGE;
    }
}
