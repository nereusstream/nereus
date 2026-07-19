/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.profile;
import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.DurabilityLevel;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.target.ReadTargetType;
import java.util.Objects;
import java.util.function.Predicate;

/** Executable Phase 1.5 matrix: strict synchronous Object WAL only. */
public final class Phase15StorageProfileResolver implements StorageProfileResolver {
    @Override
    public StorageExecutionPlan requireExecutable(StorageProfile raw, DurabilityLevel durability,
            boolean appender, boolean reader) {
        StorageProfile profile = raw.canonical();
        if (profile != StorageProfile.OBJECT_WAL_SYNC_OBJECT || !appender || !reader) {
            throw new NereusException(ErrorCode.UNSUPPORTED_STORAGE_PROFILE, false,
                    "storage profile has no complete Phase 1.5 execution plan",
                    AppendOutcome.KNOWN_NOT_COMMITTED);
        }
        if (durability != DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED) {
            throw new NereusException(ErrorCode.UNSUPPORTED_DURABILITY_LEVEL, false,
                    "Phase 1.5 exposes only strict index-confirmed durability",
                    AppendOutcome.KNOWN_NOT_COMMITTED);
        }
        return new StorageExecutionPlan(profile, ReadTargetType.OBJECT_SLICE,
                ObjectPublicationMode.SYNCHRONOUS, durability);
    }

    @Override
    public ReadTargetType requireReadable(
            StorageProfile raw, Predicate<ReadTargetType> readerInstalled) {
        StorageProfile profile = Objects.requireNonNull(raw, "profile").canonical();
        Predicate<ReadTargetType> installed = Objects.requireNonNull(readerInstalled, "readerInstalled");
        if (profile != StorageProfile.OBJECT_WAL_SYNC_OBJECT
                || !installed.test(ReadTargetType.OBJECT_SLICE)) {
            throw new NereusException(ErrorCode.UNSUPPORTED_STORAGE_PROFILE, false,
                    "storage profile has no complete Phase 1.5 read plan");
        }
        return ReadTargetType.OBJECT_SLICE;
    }
}
