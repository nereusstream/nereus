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

package com.nereusstream.kafka.bookkeeper.run;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.KafkaTopicId;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.KafkaTopicName;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2ProtocolCheckpointV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunFooterV1;
import com.nereusstream.storage.api.bookkeeper.AppendQuorumProofV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
import com.nereusstream.storage.api.bookkeeper.BookKeeperDigestTypeV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity;
import com.nereusstream.storage.api.bookkeeper.BookKeeperProtocolModeV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperTimeoutClassV1;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.bookkeeper.ExactLedgerEntryV1;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationResultV1;
import com.nereusstream.storage.api.bookkeeper.RetainedStoragePayload;
import com.nereusstream.storage.api.bookkeeper.RunLedgerAppendRequestV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerCloseProofV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerConfigurationV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerOpenResultV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerReadOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerReadResultV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerRecoveryProofV1;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import com.nereusstream.storage.api.kafka.KafkaRunRootAuthority;
import com.nereusstream.storage.api.kafka.KafkaRunRootSnapshotV1;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public final class KafkaRunTestFixtures {
    private KafkaRunTestFixtures() {}

    public static Nbke2RunBindingV1 binding(long runId, long ownerEpoch, int leaderEpoch) {
        return new Nbke2RunBindingV1(
                new TopicBindingId(digest(1)),
                new KafkaTopicIncarnationIdentity(new KafkaTopicId(new Id128(0, 2)), new KafkaTopicName("orders")),
                7,
                new StorageEpochId(digest(3)),
                ownerEpoch,
                leaderEpoch,
                new CellProviderScopeId(digest(4)),
                new StorageRunId(new Id128(0, runId)));
    }

    public static Nbke2ProtocolCheckpointV1 checkpoint(Nbke2RunBindingV1 binding, long coveredThrough) {
        return new Nbke2ProtocolCheckpointV1(
                binding,
                coveredThrough,
                coveredThrough,
                coveredThrough,
                coveredThrough,
                CanonicalBytes.copyOf(new byte[] {1}),
                CanonicalBytes.copyOf(new byte[] {2}),
                CanonicalBytes.copyOf(new byte[] {3}));
    }

    public static Nbke2RunFooterV1 footer(KafkaBookKeeperRunSnapshotV1 snapshot, long endOffset) {
        long footerEntryId = snapshot.nextEntryId();
        return new Nbke2RunFooterV1(
                snapshot.runBinding(),
                endOffset,
                footerEntryId + 1,
                -1,
                snapshot.latestProtocolCheckpointEntryId().orElse(-1),
                snapshot.runBinding().creatorOwnerEpoch(),
                java.util.List.of());
    }

    public static Sha256Digest digest(int lastByte) {
        byte[] bytes = new byte[Sha256Digest.LENGTH];
        bytes[bytes.length - 1] = (byte) lastByte;
        return Sha256Digest.copyOf(bytes);
    }

    public static final class FakeSession implements BookKeeperCellSession {
        public final BookKeeperCapabilitySnapshotV1 capability;
        public final Map<Long, CanonicalBytes> entries = new LinkedHashMap<>();
        public final java.util.List<Long> readEntryIds = new java.util.ArrayList<>();
        public final Map<Long, RunLedgerReadResultV1> readOverrides = new LinkedHashMap<>();
        public ProviderMutationResultV1<RunLedgerHandleV1> createOverride;
        public ProviderMutationResultV1<AppendQuorumProofV1> nextAppendOverride;
        public RuntimeException nextAppendFailure;
        public ProviderMutationResultV1<RunLedgerCloseProofV1> closeOverride;
        public RunLedgerOpenResultV1 openOverride;
        public ProviderMutationResultV1<RunLedgerRecoveryProofV1> recoveryOverride;
        public long delayedEntryId = -1;
        public CompletableFuture<ProviderMutationResultV1<AppendQuorumProofV1>> delayedAppend;
        public final Set<Long> delayedEntryIds = new java.util.HashSet<>();
        public final Map<Long, CompletableFuture<ProviderMutationResultV1<AppendQuorumProofV1>>> delayedAppends =
                new LinkedHashMap<>();
        public RunLedgerHandleV1 handle;
        public int drainCalls;
        public int closeCalls;

        public FakeSession() {
            capability = new BookKeeperCapabilitySnapshotV1(
                    binding(6, 11, 5).providerScopeId(),
                    "cd06340851d6d657b7c7546df01df365c18980de",
                    digest(5),
                    "cd06340851d6d657b7c7546df01df365c18980de",
                    digest(6),
                    BookKeeperProtocolModeV1.V3,
                    10_000_000,
                    10_000_000,
                    4_000_000,
                    true,
                    3,
                    3,
                    2,
                    BookKeeperDigestTypeV1.CRC32C,
                    true,
                    true,
                    new BookKeeperTimeoutClassV1(1_000, 2_000, 2_000, 5_000),
                    "bk-credential:v7",
                    digest(7));
        }

        @Override
        public CellProviderScopeId providerScopeId() {
            return capability.providerScopeId();
        }

        @Override
        public BookKeeperCapabilitySnapshotV1 capabilitySnapshot() {
            return capability;
        }

        @Override
        public CompletionStage<ProviderMutationResultV1<RunLedgerHandleV1>> createRunLedger(
                RunLedgerConfigurationV1 configuration) {
            handle = new RunLedgerHandleV1(
                    configuration.providerScopeId(),
                    configuration.runId(),
                    new BookKeeperLedgerIdentity(
                            41 + configuration.runId().value().lowBits()),
                    configuration.configurationDigest());
            return CompletableFuture.completedFuture(
                    createOverride == null ? ProviderMutationResultV1.appliedExact(handle) : createOverride);
        }

        @Override
        public CompletionStage<RunLedgerOpenResultV1> openRunLedger(RunLedgerHandleV1 expectedHandle) {
            return CompletableFuture.completedFuture(
                    openOverride == null ? RunLedgerOpenResultV1.openedExact(expectedHandle) : openOverride);
        }

        @Override
        public CompletionStage<ProviderMutationResultV1<AppendQuorumProofV1>> appendExplicitEntry(
                RunLedgerAppendRequestV1 request) {
            RetainedStoragePayload retained = request.payload().retain();
            CanonicalBytes bytes = bytes(retained.readOnlyBuffer());
            entries.put(request.expectedEntryId(), bytes);
            ProviderMutationResultV1<AppendQuorumProofV1> result = nextAppendOverride;
            nextAppendOverride = null;
            RuntimeException appendFailure = nextAppendFailure;
            nextAppendFailure = null;
            if (appendFailure != null) {
                retained.release();
                return CompletableFuture.failedFuture(appendFailure);
            }
            if (result == null) {
                result = ProviderMutationResultV1.appliedExact(new AppendQuorumProofV1(
                        request.handle(),
                        request.expectedEntryId(),
                        retained.readableBytes(),
                        retained.sha256(),
                        capability.ackQuorumSize()));
            }
            if (request.expectedEntryId() == delayedEntryId || delayedEntryIds.contains(request.expectedEntryId())) {
                CompletableFuture<ProviderMutationResultV1<AppendQuorumProofV1>> pending = new CompletableFuture<>();
                delayedAppends.put(request.expectedEntryId(), pending);
                if (request.expectedEntryId() == delayedEntryId) {
                    delayedAppend = pending;
                }
                ProviderMutationResultV1<AppendQuorumProofV1> delayedResult = result;
                pending.whenComplete((ignored, failure) -> retained.release());
                return pending.thenApply(ignored -> delayedResult);
            }
            retained.release();
            return CompletableFuture.completedFuture(result);
        }

        @Override
        public CompletionStage<RunLedgerReadResultV1> readExactEntry(RunLedgerHandleV1 requested, long entryId) {
            readEntryIds.add(entryId);
            RunLedgerReadResultV1 override = readOverrides.get(entryId);
            if (override != null) {
                return CompletableFuture.completedFuture(override);
            }
            CanonicalBytes payload = entries.get(entryId);
            if (payload == null) {
                return CompletableFuture.completedFuture(
                        RunLedgerReadResultV1.withoutEntry(RunLedgerReadOutcomeV1.DEFINITIVELY_ABSENT));
            }
            return CompletableFuture.completedFuture(RunLedgerReadResultV1.foundExact(
                    new ExactLedgerEntryV1(requested, entryId, payload, Sha256Digest.hash(payload))));
        }

        @Override
        public CompletionStage<ProviderMutationResultV1<RunLedgerRecoveryProofV1>> fenceAndRecoverRunLedger(
                RunLedgerHandleV1 requested) {
            ProviderMutationResultV1<RunLedgerRecoveryProofV1> result = recoveryOverride;
            if (result == null) {
                result = ProviderMutationResultV1.appliedExact(new RunLedgerRecoveryProofV1(
                        requested,
                        entries.keySet().stream()
                                .mapToLong(Long::longValue)
                                .max()
                                .orElse(-1),
                        true,
                        true));
            }
            return CompletableFuture.completedFuture(result);
        }

        @Override
        public CompletionStage<ProviderMutationResultV1<RunLedgerCloseProofV1>> closeRunLedger(
                RunLedgerHandleV1 requested) {
            closeCalls++;
            ProviderMutationResultV1<RunLedgerCloseProofV1> result = closeOverride;
            if (result == null) {
                result = ProviderMutationResultV1.appliedExact(new RunLedgerCloseProofV1(
                        requested,
                        entries.keySet().stream()
                                .mapToLong(Long::longValue)
                                .max()
                                .orElse(-1)));
            }
            return CompletableFuture.completedFuture(result);
        }

        @Override
        public CompletionStage<Void> drain() {
            drainCalls++;
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> closeAsync() {
            closeCalls++;
            return CompletableFuture.completedFuture(null);
        }

        public void completeDelayedAppend() {
            delayedAppend.complete(ProviderMutationResultV1.outcomeUnknown());
        }

        public void completeDelayedEntry(long entryId) {
            delayedAppends.get(entryId).complete(ProviderMutationResultV1.outcomeUnknown());
        }

        private static CanonicalBytes bytes(ByteBuffer buffer) {
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return CanonicalBytes.copyOf(bytes);
        }
    }

    public static final class FakeRootAuthority implements KafkaRunRootAuthority {
        public final Map<StorageRunId, KafkaRunRootSnapshotV1> roots = new LinkedHashMap<>();
        public ProviderMutationResultV1<KafkaRunRootSnapshotV1> nextOverride;

        @Override
        public CompletionStage<ProviderMutationResultV1<KafkaRunRootSnapshotV1>> createRoot(
                KafkaRunRootSnapshotV1 activeCandidate) {
            return mutate(activeCandidate);
        }

        @Override
        public CompletionStage<Optional<KafkaRunRootSnapshotV1>> openRoot(StorageRunId runId) {
            return CompletableFuture.completedFuture(Optional.ofNullable(roots.get(runId)));
        }

        @Override
        public CompletionStage<ProviderMutationResultV1<KafkaRunRootSnapshotV1>> sealRoot(
                KafkaRunRootSnapshotV1 expectedActive, KafkaRunRootSnapshotV1 sealedCandidate) {
            if (!expectedActive.equals(roots.get(expectedActive.runId()))) {
                return CompletableFuture.completedFuture(ProviderMutationResultV1.fencedOrConflict());
            }
            return mutate(sealedCandidate);
        }

        @Override
        public CompletionStage<ProviderMutationResultV1<KafkaRunRootSnapshotV1>> createSuccessor(
                KafkaRunRootSnapshotV1 expectedSealed, KafkaRunRootSnapshotV1 activeSuccessor) {
            if (!expectedSealed.equals(roots.get(expectedSealed.runId()))) {
                return CompletableFuture.completedFuture(ProviderMutationResultV1.fencedOrConflict());
            }
            return mutate(activeSuccessor);
        }

        private CompletionStage<ProviderMutationResultV1<KafkaRunRootSnapshotV1>> mutate(
                KafkaRunRootSnapshotV1 candidate) {
            ProviderMutationResultV1<KafkaRunRootSnapshotV1> result = nextOverride;
            nextOverride = null;
            if (result == null) {
                roots.put(candidate.runId(), candidate);
                result = ProviderMutationResultV1.appliedExact(candidate);
            }
            return CompletableFuture.completedFuture(result);
        }
    }
}
