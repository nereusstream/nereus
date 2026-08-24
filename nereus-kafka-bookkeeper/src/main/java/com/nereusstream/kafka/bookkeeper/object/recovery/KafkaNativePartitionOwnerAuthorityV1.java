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

package com.nereusstream.kafka.bookkeeper.object.recovery;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.protocol.ProtocolCellIdentity;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.storage.object.control.WalRunObjectSession;
import com.nereusstream.storage.object.control.WalRunReference;
import com.nereusstream.storage.object.nwg1.Nwg1VerificationContextV1;
import com.nereusstream.storage.object.recovery.OwnerOpenRecoveryCoordinator;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Minimal synchronous bridge from Kafka's durable partition/election authority to Object-WAL owner-open recovery.
 *
 * <p>The implementation must be backed by Kafka's process-current KRaft metadata offset, broker epoch and partition
 * leader epoch authority. It must reject a stale or regressing authority before invoking the callback, retain its
 * current-owner exclusion for the callback's complete duration, and prevent a superseded callback from performing a
 * late Provider mutation before a newer owner begins LIST/recovery. It must invoke the callback synchronously on the
 * calling thread exactly once. A proof/token/fence snapshot is not a valid implementation of this interface.
 */
@FunctionalInterface
public interface KafkaNativePartitionOwnerAuthorityV1 {
    WalRunObjectSession executeWhileCurrentOwner(
            KafkaPartitionFenceV1 exactCurrentFence, SynchronousOwnerCallback callback) throws IOException;

    /** Callback authority exists only on the synchronous stack of {@link #executeWhileCurrentOwner}. */
    @FunctionalInterface
    interface SynchronousOwnerCallback {
        WalRunObjectSession execute() throws IOException;
    }

    /**
     * Binds this native authority to common's exact ProtocolCell/Root owner-open SPI. The returned executor has no
     * reusable authority token: its callback is same-thread, one-use, and rejected after native control returns.
     */
    static OwnerOpenRecoveryCoordinator.ProtocolOwnerFenceExecutor protocolOwnerFenceExecutor(
            KafkaNativePartitionOwnerAuthorityV1 nativeAuthority,
            KafkaPartitionFenceV1 currentFence,
            Nbke2RunBindingV1 expectedRunBinding,
            ProtocolCellIdentity expectedProtocolCell,
            WalRunReference expectedRootReference,
            Nwg1VerificationContextV1 exactVerificationContext) {
        Objects.requireNonNull(nativeAuthority, "nativeAuthority");
        requireExactRunFence(currentFence, expectedRunBinding);
        Objects.requireNonNull(expectedProtocolCell, "expectedProtocolCell");
        Objects.requireNonNull(expectedRootReference, "expectedRootReference");
        Objects.requireNonNull(exactVerificationContext, "exactVerificationContext");
        if (!expectedProtocolCell.equals(exactVerificationContext.protocolCell())
                || !Arrays.equals(
                        expectedRootReference.rootSha256().bytes().toByteArray(),
                        exactVerificationContext.walRunRootSha256())) {
            throw new IllegalArgumentException(
                    "Kafka recovery verification context differs from the exact ProtocolCell/WalRun Root");
        }
        return (actualProtocolCell, actualRootReference, actualRootSha256, recoveryCallback) -> {
            requireExactCommonAuthority(
                    expectedProtocolCell,
                    expectedRootReference,
                    actualProtocolCell,
                    actualRootReference,
                    actualRootSha256);
            final class OneUseOwnerCallback implements SynchronousOwnerCallback {
                private final Thread ownerThread = Thread.currentThread();
                private boolean active = true;
                private boolean invoked;
                private WalRunObjectSession exactResult;

                @Override
                public synchronized WalRunObjectSession execute() throws IOException {
                    if (!active || invoked || Thread.currentThread() != ownerThread) {
                        throw new IllegalStateException(
                                "Kafka native owner recovery callback escaped, changed thread, or was repeated");
                    }
                    invoked = true;
                    exactResult = recoveryCallback.recover(exactVerificationContext);
                    return exactResult;
                }

                private synchronized void deactivate() {
                    active = false;
                }

                private synchronized WalRunObjectSession requireExactReturnedSession(WalRunObjectSession returned) {
                    if (!invoked || returned == null || returned != exactResult) {
                        throw new IllegalStateException("Kafka native owner authority omitted or substituted the exact "
                                + "recovery callback result");
                    }
                    return returned;
                }
            }
            OneUseOwnerCallback guarded = new OneUseOwnerCallback();
            WalRunObjectSession returned;
            try {
                returned = nativeAuthority.executeWhileCurrentOwner(currentFence, guarded);
            } finally {
                guarded.deactivate();
            }
            return guarded.requireExactReturnedSession(returned);
        };
    }

    private static void requireExactCommonAuthority(
            ProtocolCellIdentity expectedProtocolCell,
            WalRunReference expectedRootReference,
            ProtocolCellIdentity actualProtocolCell,
            WalRunReference actualRootReference,
            Sha256Digest actualRootSha256) {
        if (!expectedProtocolCell.equals(actualProtocolCell)
                || !expectedRootReference.equals(actualRootReference)
                || !expectedRootReference.rootSha256().equals(actualRootSha256)) {
            throw new IllegalArgumentException(
                    "Kafka native owner authority differs from the exact ProtocolCell/WalRun Root");
        }
    }

    private static void requireExactRunFence(KafkaPartitionFenceV1 currentFence, Nbke2RunBindingV1 expectedRunBinding) {
        Objects.requireNonNull(currentFence, "currentFence");
        Objects.requireNonNull(expectedRunBinding, "expectedRunBinding");
        if (!currentFence.bindingId().equals(expectedRunBinding.bindingId())
                || !currentFence.topicIncarnation().equals(expectedRunBinding.topicIncarnation())
                || currentFence.partitionId() != expectedRunBinding.partitionId()
                || !currentFence.storageEpochId().equals(expectedRunBinding.storageEpochId())
                || currentFence.ownerEpoch() < expectedRunBinding.creatorOwnerEpoch()
                || currentFence.kafkaLeaderEpoch() < expectedRunBinding.kafkaLeaderEpoch()) {
            throw new IllegalArgumentException("Kafka native current fence differs from the exact NBKE2 run binding");
        }
    }
}
