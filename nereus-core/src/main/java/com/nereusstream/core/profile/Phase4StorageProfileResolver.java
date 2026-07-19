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

import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.DurabilityLevel;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.target.ReadTargetType;
import java.util.Objects;
import java.util.function.Predicate;

/** Executable Phase 4 Object-WAL matrix; BookKeeper profiles remain closed until their adapter gate. */
public final class Phase4StorageProfileResolver implements StorageProfileResolver {
    @Override
    public StorageExecutionPlan requireExecutable(
            StorageProfile raw,
            DurabilityLevel durability,
            boolean primaryAppenderInstalled,
            boolean primaryReaderInstalled) {
        StorageProfile profile = Objects.requireNonNull(raw, "profile").canonical();
        Objects.requireNonNull(durability, "durability");
        if (!profile.usesObjectWal() || !primaryAppenderInstalled || !primaryReaderInstalled) {
            throw new NereusException(
                    ErrorCode.UNSUPPORTED_STORAGE_PROFILE,
                    false,
                    "storage profile has no complete Phase 4 primary WAL execution plan",
                    AppendOutcome.KNOWN_NOT_COMMITTED);
        }
        if (profile == StorageProfile.OBJECT_WAL_SYNC_OBJECT
                && durability != DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED) {
            throw new NereusException(
                    ErrorCode.UNSUPPORTED_DURABILITY_LEVEL,
                    false,
                    "synchronous Object WAL requires index-confirmed durability",
                    AppendOutcome.KNOWN_NOT_COMMITTED);
        }
        ObjectPublicationMode publicationMode =
                profile == StorageProfile.OBJECT_WAL_ASYNC_OBJECT
                        ? ObjectPublicationMode.ASYNCHRONOUS
                        : ObjectPublicationMode.SYNCHRONOUS;
        return new StorageExecutionPlan(
                profile,
                ReadTargetType.OBJECT_SLICE,
                publicationMode,
                durability);
    }

    @Override
    public ReadTargetType requireReadable(
            StorageProfile raw, Predicate<ReadTargetType> readerInstalled) {
        StorageProfile profile = Objects.requireNonNull(raw, "profile").canonical();
        Predicate<ReadTargetType> installed = Objects.requireNonNull(readerInstalled, "readerInstalled");
        if (!profile.usesObjectWal() || !installed.test(ReadTargetType.OBJECT_SLICE)) {
            throw new NereusException(ErrorCode.UNSUPPORTED_STORAGE_PROFILE, false,
                    "storage profile has no complete Phase 4 read plan");
        }
        return ReadTargetType.OBJECT_SLICE;
    }
}
