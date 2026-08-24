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

package com.nereusstream.storage.object.recovery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.KafkaCellId;
import com.nereusstream.domain.protocol.KafkaProtocolCellIdentity;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.control.CurrentWalRunPointer;
import com.nereusstream.storage.object.control.LaneSequenceVector;
import com.nereusstream.storage.object.control.Nwg1RootAdmissionCaps;
import com.nereusstream.storage.object.control.ObjectProviderAccessProfile;
import com.nereusstream.storage.object.control.ObjectProviderRootConfiguration;
import com.nereusstream.storage.object.control.ProviderProofMode;
import com.nereusstream.storage.object.control.TerminalProtocolCheckpointBindingV1;
import com.nereusstream.storage.object.control.TerminalProtocolCheckpointVerifierV1;
import com.nereusstream.storage.object.control.WalCheckpointPolicy;
import com.nereusstream.storage.object.control.WalRunBounds;
import com.nereusstream.storage.object.control.WalRunControlCodec;
import com.nereusstream.storage.object.control.WalRunControlKeys;
import com.nereusstream.storage.object.control.WalRunFormatContractV1;
import com.nereusstream.storage.object.control.WalRunPredecessor;
import com.nereusstream.storage.object.control.WalRunReference;
import com.nereusstream.storage.object.control.WalRunRootRecord;
import com.nereusstream.storage.object.control.WalRunSealRecord;
import com.nereusstream.storage.object.kms.WrappedRunKeyEnvelope;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class WalRunLineageRecoveryTest {
    @Test
    void exactLineageWalkValidatesRootAndPredecessorSeal() {
        MapStore store = new MapStore();
        WalRunRootRecord root1 = root(1, Optional.empty());
        WalRunReference reference1 = reference(WalRunControlKeys.rootKey(7, 1), root1);
        com.nereusstream.storage.object.control.WalCheckpointHeadV1 finalHead =
                com.nereusstream.storage.object.control.WalCheckpointHeadV1.empty(
                        reference1.rootSha256(), reference1.shardRunEpoch(), 1);
        CanonicalBytes finalHeadBytes = WalRunControlCodec.encodeCheckpointHead(finalHead);
        WalRunSealRecord seal1 = new WalRunSealRecord(
                reference1,
                LaneSequenceVector.empty(),
                WalRunControlKeys.checkpointHeadKey(7, 1),
                Sha256Digest.hash(finalHeadBytes),
                0,
                0);
        WalRunRootRecord root2 = root(
                2,
                Optional.of(new WalRunPredecessor(
                        reference1,
                        WalRunControlKeys.sealKey(7, 1),
                        WalRunControlCodec.sealSha256(seal1),
                        Optional.of(terminalBinding(1)))));
        WalRunReference reference2 = reference(WalRunControlKeys.rootKey(7, 2), root2);
        store.values.put(
                WalRunControlKeys.pointerKey(7),
                WalRunControlCodec.encodePointer(new CurrentWalRunPointer(reference2)));
        store.values.put(reference1.rootKey(), WalRunControlCodec.encodeRoot(root1));
        store.values.put(reference2.rootKey(), WalRunControlCodec.encodeRoot(root2));
        store.values.put(WalRunControlKeys.sealKey(7, 1), WalRunControlCodec.encodeSeal(seal1));
        store.values.put(WalRunControlKeys.checkpointHeadKey(7, 1), finalHeadBytes);
        store.values.put(terminalHeadKey(1), terminalHeadBytes(1));

        var recovered = new WalRunLineageRecovery(store, (root, seal, binding, exactValue, context) -> {
                    if (!exactValue.equals(terminalHeadBytes(root.shardRunEpoch()))) {
                        throw new IllegalStateException("test terminal Kafka Head differs");
                    }
                    assertThat(context.budgetOwnerRoot()).isEqualTo(root2);
                    assertThat(context.protocolObjectRoot()).isEqualTo(root1);
                })
                .recover(
                        WalRunControlKeys.pointerKey(7),
                        root2.protocolCellIdentity(),
                        root2.providerScopeId(),
                        Optional.empty(),
                        () -> 0);

        assertThat(recovered.runs())
                .extracting(WalRunLineageRecovery.RecoveredRun::reference)
                .containsExactly(reference2, reference1);
        assertThat(recovered.runs().get(0).predecessorSeal()).contains(seal1);
        recovered.requireConsumableFor(root2);
        assertThatThrownBy(() -> recovered.requireConsumableFor(root1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("differs from the current Root");
        CumulativeRecoveryBudget continuedBudget = recovered.consumeFor(root2);
        long beforeTailCharge = continuedBudget.snapshot().canonicalBodyBytes();
        continuedBudget.chargeRangeGet(1);
        assertThat(continuedBudget.snapshot().canonicalBodyBytes()).isEqualTo(beforeTailCharge + 1);
        assertThatThrownBy(() -> recovered.consumeFor(root2))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already consumed");
        assertThatThrownBy(() -> recovered.requireConsumableFor(root2))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already consumed");
        var substituted = new WalRunLineageRecovery(store, (root, seal, binding, exactValue, context) -> {})
                .recover(
                        WalRunControlKeys.pointerKey(7),
                        root2.protocolCellIdentity(),
                        root2.providerScopeId(),
                        Optional.empty(),
                        () -> 0);
        assertThatThrownBy(() -> substituted.consumeFor(root1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("differs from the current Root");
    }

    @Test
    void rootDigestSubstitutionAndUnreachableFrontierFailClosed() {
        MapStore store = new MapStore();
        WalRunRootRecord root = root(1, Optional.empty());
        WalRunReference reference = reference(WalRunControlKeys.rootKey(7, 1), root);
        store.values.put(
                WalRunControlKeys.pointerKey(7), WalRunControlCodec.encodePointer(new CurrentWalRunPointer(reference)));
        store.values.put(WalRunControlKeys.rootKey(7, 1), WalRunControlCodec.encodeRoot(root(2, Optional.empty())));
        assertThatThrownBy(() -> new WalRunLineageRecovery(store)
                        .recover(
                                WalRunControlKeys.pointerKey(7),
                                root.protocolCellIdentity(),
                                root.providerScopeId(),
                                Optional.empty(),
                                () -> 0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("digest differs");

        store.values.put(WalRunControlKeys.rootKey(7, 1), WalRunControlCodec.encodeRoot(root));
        WalRunReference unrelated = new WalRunReference(WalRunControlKeys.rootKey(7, 0), digest(9), 7, 0);
        assertThatThrownBy(() -> new WalRunLineageRecovery(store)
                        .recover(
                                WalRunControlKeys.pointerKey(7),
                                root.protocolCellIdentity(),
                                root.providerScopeId(),
                                Optional.of(unrelated),
                                () -> 0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("retirement frontier");
    }

    @Test
    void expectedProtocolCellAndProviderScopeCannotBeSubstituted() {
        MapStore store = new MapStore();
        WalRunRootRecord root = root(1, Optional.empty());
        WalRunReference reference = reference(WalRunControlKeys.rootKey(7, 1), root);
        store.values.put(
                WalRunControlKeys.pointerKey(7), WalRunControlCodec.encodePointer(new CurrentWalRunPointer(reference)));
        store.values.put(reference.rootKey(), WalRunControlCodec.encodeRoot(root));
        KafkaProtocolCellIdentity otherCell =
                new KafkaProtocolCellIdentity(new DeploymentId(new Id128(9, 10)), new KafkaCellId(new Id128(11, 12)));

        assertThatThrownBy(() -> new WalRunLineageRecovery(store)
                        .recover(
                                WalRunControlKeys.pointerKey(7),
                                otherCell,
                                root.providerScopeId(),
                                Optional.empty(),
                                () -> 0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Protocol Cell");
    }

    @Test
    void terminalProtocolObjectWorkingSetUnderboundFailsBeforeReaderIo() {
        WalRunRootRecord root = root(1, Optional.empty());
        RecoveryEnvelopeLimits limits = root.recoveryEnvelope();
        CumulativeRecoveryBudget budget = new CumulativeRecoveryBudget(
                new RecoveryEnvelopeLimits(
                        limits.maxLiveRoots(),
                        limits.maxPredecessorRuns(),
                        limits.maxListPages(),
                        limits.maxListedKeys(),
                        limits.maxListedKeyBytes(),
                        limits.maxHeadRequests(),
                        limits.maxRangeGetRequests(),
                        limits.maxFullGetRequests(),
                        limits.maxCanonicalBodyBytes(),
                        limits.maxDecodedContexts(),
                        limits.maxDecodedFrames(),
                        limits.maxDecodedCommitSets(),
                        1,
                        limits.maxConcurrency(),
                        limits.maxRetryAttempts(),
                        limits.maxWallTimeNanos()),
                () -> 0);
        AtomicInteger readerCalls = new AtomicInteger();
        var context = new TerminalProtocolCheckpointVerifierV1.RecoveryContext(
                root, root, budget, ignoredRoot -> identity -> {
                    readerCalls.incrementAndGet();
                    return CanonicalBytes.copyOf(new byte[] {1, 2});
                });
        ObjectIdentity identity =
                new ObjectIdentity("protocol/exact", 2, Sha256Digest.hash(CanonicalBytes.copyOf(new byte[] {1, 2})));

        assertThatThrownBy(() -> context.readVerifiedProtocolObject(identity))
                .isInstanceOf(RecoveryEnvelopeExceededException.class)
                .hasMessageContaining("working memory");
        assertThat(readerCalls).hasValue(0);
        assertThat(budget.snapshot().fullGetRequests()).isZero();
        assertThat(budget.snapshot().canonicalBodyBytes()).isZero();
        assertThat(budget.snapshot().workingMemoryBytes()).isZero();
        assertThat(budget.snapshot().currentConcurrency()).isZero();

        CumulativeRecoveryBudget chargedBudget = new CumulativeRecoveryBudget(root.recoveryEnvelope(), () -> 0);
        var failingContext = new TerminalProtocolCheckpointVerifierV1.RecoveryContext(
                root, root, chargedBudget, ignoredRoot -> ignored -> {
                    throw new java.io.IOException("synthetic reader loss");
                });
        assertThatThrownBy(() -> failingContext.readVerifiedProtocolObject(identity))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("protocol Object recovery failed");
        assertThat(chargedBudget.snapshot().fullGetRequests()).isEqualTo(1);
        assertThat(chargedBudget.snapshot().canonicalBodyBytes()).isEqualTo(2);
        assertThat(chargedBudget.snapshot().workingMemoryBytes()).isZero();
        assertThat(chargedBudget.snapshot().currentConcurrency()).isZero();
    }

    private static WalRunRootRecord root(long epoch, Optional<WalRunPredecessor> predecessor) {
        return new WalRunRootRecord(
                7,
                epoch,
                new Id128(epoch + 1, epoch + 2),
                0,
                new KafkaProtocolCellIdentity(new DeploymentId(new Id128(1, 2)), new KafkaCellId(new Id128(3, 4))),
                new CellProviderScopeId(digest(1)),
                WalRunFormatContractV1.frozen(),
                new Nwg1RootAdmissionCaps(4096, 1024, 752, 16, 32, 32, 1008, 1024, 4096, 4096),
                new WalRunBounds(100, 4096, 1000, 3),
                new WalCheckpointPolicy(0, 10, 4096, 1000, 10, 4096),
                new ObjectProviderRootConfiguration(
                        ObjectProviderAccessProfile.C1_SINGLE_PUT_SINGLE_RANGE_STRONG_LIST,
                        "adapter-v1",
                        "canonical-key-v1",
                        "cell-a/wal/run-" + epoch,
                        ProviderProofMode.NONE,
                        0,
                        4096,
                        4096,
                        1024,
                        1,
                        100,
                        digest(2)),
                rootRecoveryLimits(),
                new WrappedRunKeyEnvelope(
                        "fake-kms", "aes-kw-v1", "kms/a", "v1", CanonicalBytes.copyOf(new byte[] {1, 2, (byte) epoch})),
                predecessor);
    }

    private static RecoveryEnvelopeLimits rootRecoveryLimits() {
        return new RecoveryEnvelopeLimits(
                4, 3, 10, 102, 100_000, 0, 3300, 10, 20L * 1024 * 1024, 1600, 3200, 3200, 64 * 1024, 2, 10, 1_000_000);
    }

    private static String terminalHeadKey(long epoch) {
        return "v2/object-wal/protocol/kafka/run-" + epoch + "/head";
    }

    private static CanonicalBytes terminalHeadBytes(long epoch) {
        return CanonicalBytes.copyOf(new byte[] {1, (byte) epoch});
    }

    private static TerminalProtocolCheckpointBindingV1 terminalBinding(long epoch) {
        return new TerminalProtocolCheckpointBindingV1(
                com.nereusstream.domain.protocol.ProtocolKindV1.KAFKA,
                terminalHeadKey(epoch),
                Sha256Digest.hash(terminalHeadBytes(epoch)));
    }

    private static WalRunReference reference(String key, WalRunRootRecord root) {
        return new WalRunReference(key, WalRunControlCodec.rootSha256(root), root.shardId(), root.shardRunEpoch());
    }

    private static WalRunSealRecord seal(WalRunReference root) {
        return new WalRunSealRecord(
                root,
                LaneSequenceVector.empty(),
                WalRunControlKeys.checkpointHeadKey(root.shardId(), root.shardRunEpoch()),
                digest(7),
                0,
                0);
    }

    private static Sha256Digest digest(int seed) {
        byte[] value = new byte[32];
        for (int index = 0; index < value.length; index++) {
            value[index] = (byte) (seed + index);
        }
        return Sha256Digest.copyOf(value);
    }

    private static final class MapStore implements CanonicalControlMetadataStore {
        private final Map<String, CanonicalBytes> values = new LinkedHashMap<>();

        @Override
        public Optional<CanonicalBytes> get(String key) {
            return Optional.ofNullable(values.get(key));
        }

        @Override
        public ControlMutationOutcome putIfAbsent(String key, CanonicalBytes exactValue) {
            return values.putIfAbsent(key, exactValue) == null
                    ? ControlMutationOutcome.APPLIED
                    : ControlMutationOutcome.DEFINITIVE_CONFLICT;
        }

        @Override
        public ControlMutationOutcome compareAndSet(
                String key, Optional<CanonicalBytes> exactExpected, CanonicalBytes exactCandidate) {
            Optional<CanonicalBytes> actual = Optional.ofNullable(values.get(key));
            if (!actual.equals(exactExpected)) {
                return ControlMutationOutcome.DEFINITIVE_CONFLICT;
            }
            values.put(key, exactCandidate);
            return ControlMutationOutcome.APPLIED;
        }
    }
}
