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

package com.nereusstream.managedledger.projection;

import com.nereusstream.api.AppendCompletionPolicy;
import com.nereusstream.api.AppendOptions;
import com.nereusstream.api.AppendRecoveryOptions;
import com.nereusstream.api.DeleteOptions;
import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.SealOptions;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamCreateOptions;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Centralizes every F2 request option sent to protocol-neutral L0. */
public final class F2L0RequestFactory {
    public static final String TERMINATE_REASON = "pulsar-managed-ledger-terminate";
    public static final String DELETE_REASON = "pulsar-managed-ledger-delete";
    private final StorageProfile createProfile;

    public F2L0RequestFactory() {
        this(StorageProfile.OBJECT_WAL_SYNC_OBJECT);
    }

    public F2L0RequestFactory(StorageProfile createProfile) {
        this.createProfile = requireManagedLedgerProfile(createProfile);
    }

    public StreamCreateOptions createOptions() {
        return new StreamCreateOptions(
                createProfile,
                Map.of(
                        ManagedLedgerProjectionNames.PAYLOAD_MAPPING_ATTRIBUTE,
                        ManagedLedgerProjectionNames.PAYLOAD_MAPPING_V1));
    }

    public AppendOptions appendOptions(Duration timeout) {
        return appendOptions(
                StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                timeout);
    }

    public AppendOptions appendOptions(
            StorageProfile profile,
            Duration timeout) {
        StorageProfile exact = requireManagedLedgerProfile(profile);
        return new AppendOptions(
                Optional.empty(),
                exact.defaultDurabilityLevel(),
                exact == StorageProfile.BOOKKEEPER_WAL_SYNC_OBJECT
                        ? AppendCompletionPolicy.REQUIRED_OBJECT_GENERATION
                        : AppendCompletionPolicy.PROFILE_DEFAULT,
                timeout,
                true,
                Map.of());
    }

    public AppendRecoveryOptions recoveryOptions(Duration timeout) {
        return new AppendRecoveryOptions(timeout);
    }

    public ReadOptions singleEntryReadOptions(int maxEntryBytes, Duration timeout) {
        return new ReadOptions(1, maxEntryBytes, ReadIsolation.COMMITTED, timeout);
    }

    public SealOptions sealOptions(Duration timeout) {
        return new SealOptions(timeout, TERMINATE_REASON);
    }

    public DeleteOptions deleteOptions(Duration timeout) {
        return new DeleteOptions(timeout, DELETE_REASON);
    }

    private static StorageProfile requireManagedLedgerProfile(
            StorageProfile profile) {
        StorageProfile exact = Objects.requireNonNull(
                        profile, "profile")
                .canonical();
        if (exact != StorageProfile.OBJECT_WAL_SYNC_OBJECT
                && exact
                        != StorageProfile.OBJECT_WAL_ASYNC_OBJECT
                && exact
                        != StorageProfile.BOOKKEEPER_WAL_ONLY
                && exact
                        != StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT
                && exact
                        != StorageProfile.BOOKKEEPER_WAL_SYNC_OBJECT) {
            throw new IllegalArgumentException(
                    "managed-ledger facade has no executable profile mapping");
        }
        return exact;
    }
}
