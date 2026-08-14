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

package com.nereusstream.pulsar.offload;

import java.util.Objects;
import java.util.UUID;

/** Native-authority facts captured before one sealed, non-current ledger offload. */
public record PulsarSealedLedgerAttemptV1(
        long ledgerId,
        UUID attemptUuid,
        long lastAddConfirmed,
        long entryCount,
        long logicalLength,
        long creationTimestampMillis,
        long metadataVersion,
        String providerScopePrefix,
        RetentionClass retentionClass,
        DeleteState deleteState,
        boolean bookkeeperDeleted) {
    public enum RetentionClass {
        RETAIN_BK,
        DELETE_AFTER_VERIFIED
    }

    public enum DeleteState {
        BK_DELETE_NONE,
        BK_DELETE_INTENT,
        BK_DELETE_DONE
    }

    public PulsarSealedLedgerAttemptV1 {
        Objects.requireNonNull(attemptUuid, "attemptUuid");
        Objects.requireNonNull(retentionClass, "retentionClass");
        Objects.requireNonNull(deleteState, "deleteState");
        PulsarOffloadKeysV1.derive(providerScopePrefix, ledgerId, attemptUuid);
        if (lastAddConfirmed < 0
                || entryCount != Math.addExact(lastAddConfirmed, 1)
                || logicalLength < 0
                || creationTimestampMillis < 0
                || metadataVersion < 0) {
            throw new IllegalArgumentException("sealed-ledger facts are empty, negative, or inconsistent");
        }
        boolean expectedDeleted = deleteState != DeleteState.BK_DELETE_NONE;
        if (bookkeeperDeleted != expectedDeleted) {
            throw new IllegalArgumentException("compatibility boolean differs from irreversible delete state");
        }
        if (retentionClass == RetentionClass.RETAIN_BK && deleteState != DeleteState.BK_DELETE_NONE) {
            throw new IllegalArgumentException("RETAIN_BK cannot carry delete intent or done");
        }
    }

    public PulsarOffloadKeysV1 keys() {
        return PulsarOffloadKeysV1.derive(providerScopePrefix, ledgerId, attemptUuid);
    }

    public PulsarSealedLedgerAttemptV1 transitionDeleteState(DeleteState next) {
        Objects.requireNonNull(next, "next");
        if (retentionClass == RetentionClass.RETAIN_BK || next.ordinal() < deleteState.ordinal()) {
            throw new IllegalStateException("delete state cannot advance under this retention/state combination");
        }
        if (next.ordinal() > deleteState.ordinal() + 1) {
            throw new IllegalStateException("delete state cannot skip an irreversible fact");
        }
        return new PulsarSealedLedgerAttemptV1(
                ledgerId,
                attemptUuid,
                lastAddConfirmed,
                entryCount,
                logicalLength,
                creationTimestampMillis,
                Math.addExact(metadataVersion, 1),
                providerScopePrefix,
                retentionClass,
                next,
                next != DeleteState.BK_DELETE_NONE);
    }
}
