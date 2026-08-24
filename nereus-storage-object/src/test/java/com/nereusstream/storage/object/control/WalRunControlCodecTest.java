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

package com.nereusstream.storage.object.control;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.storage.object.recovery.RecoveryEnvelopeLimits;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class WalRunControlCodecTest {
    @Test
    void exactProductionWireAndProjectionGoldensAreStable() {
        WalRunRootRecord root = ObjectWalControlTestFixtures.root(1, Optional.empty());
        WalRunReference reference = ObjectWalControlTestFixtures.reference(WalRunControlKeys.rootKey(7, 1), root);
        CurrentWalRunPointer pointer = new CurrentWalRunPointer(reference);
        ProviderResolvedExtentRowV1 row = row(WalLaneId.OBJECT_LATENCY, 0, 3);
        WalRunCheckpointPageV1 page = new WalRunCheckpointPageV1(
                reference.rootSha256(), 0, Optional.empty(), List.of(row), LaneSequenceVector.of(0, -1, -1));
        com.nereusstream.domain.bytes.Sha256Digest pageSha =
                com.nereusstream.domain.bytes.Sha256Digest.hash(WalRunControlCodec.encodeCheckpointPage(page));
        WalCheckpointHeadV1 head = new WalCheckpointHeadV1(
                reference.rootSha256(),
                1,
                9,
                0,
                Optional.of(WalRunControlKeys.checkpointPageKey(7, 1, 0, pageSha)),
                Optional.of(pageSha),
                page.coveredThrough());
        WalRunSealRecord seal = new WalRunSealRecord(
                reference,
                page.coveredThrough(),
                WalRunControlKeys.checkpointHeadKey(7, 1),
                com.nereusstream.domain.bytes.Sha256Digest.hash(WalRunControlCodec.encodeCheckpointHead(head)),
                1,
                row.bodyLength());
        assertGolden(
                WalRunControlCodec.encodeRoot(root),
                541,
                "6cdab8ede9279e4c067a81a70f6ffb98b7433dd3df5ba199124b3f12d9734367");
        assertGolden(
                WalRunControlCodec.encodePointer(pointer),
                116,
                "2f402622fc7abba243aca9ef7e6b1b87aad286ada8e890c59d48c900beb7c3b8");
        assertGolden(
                WalRunControlCodec.encodeSeal(seal),
                263,
                "bf26faa57404ff44b79baf8dab220449d56ff8c0d464d75345949e34a29b0a50");
        assertGolden(
                WalRunControlCodec.encodeCheckpointPage(page),
                131,
                "10ea321efb086c594e7566d51e7b54663d6c45f3e1b627d42c29681187a2fce9");
        assertGolden(
                WalRunControlCodec.encodeCheckpointHead(head),
                283,
                "5fb18c31560579b9f6a85aee828fcb0bf49e227e35ede8464170f8e1f244e952");
        assertGolden(
                WalRunControlWireProjectionV1.canonicalTsv(),
                7855,
                "686468eb8e006bf05f7181885eada3f42825142bf67ac9076b857ef90e4f4b6e");
    }

    @Test
    void rootPointerSealAndCheckpointRecordsRoundTripCanonically() {
        WalRunRootRecord root = ObjectWalControlTestFixtures.root(1, Optional.empty());
        WalRunReference reference = ObjectWalControlTestFixtures.reference(WalRunControlKeys.rootKey(7, 1), root);
        CurrentWalRunPointer pointer = new CurrentWalRunPointer(reference);
        ProviderResolvedExtentRowV1 row = row(WalLaneId.OBJECT_LATENCY, 0, 3);
        WalRunCheckpointPageV1 page = new WalRunCheckpointPageV1(
                reference.rootSha256(),
                0,
                Optional.empty(),
                List.of(row),
                LaneSequenceVector.empty().with(WalLaneId.OBJECT_LATENCY, 0));
        WalCheckpointHeadV1 head = new WalCheckpointHeadV1(
                reference.rootSha256(),
                1,
                9,
                0,
                Optional.of(WalRunControlKeys.checkpointPageKey(7, 1, 0, ObjectWalControlTestFixtures.digest(8))),
                Optional.of(ObjectWalControlTestFixtures.digest(8)),
                page.coveredThrough());
        WalRunSealRecord seal = new WalRunSealRecord(
                reference,
                page.coveredThrough(),
                WalRunControlKeys.checkpointHeadKey(7, 1),
                ObjectWalControlTestFixtures.digest(9),
                1,
                row.bodyLength());

        assertThat(WalRunControlCodec.decodeRoot(WalRunControlCodec.encodeRoot(root)))
                .isEqualTo(root);
        assertThat(WalRunControlCodec.decodePointer(WalRunControlCodec.encodePointer(pointer)))
                .isEqualTo(pointer);
        assertThat(WalRunControlCodec.decodeSeal(WalRunControlCodec.encodeSeal(seal)))
                .isEqualTo(seal);
        assertThat(WalRunControlCodec.decodeCheckpointPage(
                        WalRunControlCodec.encodeCheckpointPage(page), root.providerConfiguration()))
                .isEqualTo(page);
        assertThat(WalRunControlCodec.decodeCheckpointHead(WalRunControlCodec.encodeCheckpointHead(head)))
                .isEqualTo(head);
        assertThat(WalRunControlCodec.rootSha256(root)).isEqualTo(WalRunControlCodec.rootSha256(root));
    }

    @Test
    void strictDecoderRejectsReservedBitsTrailingBytesAndProofModeSubstitution() {
        WalRunRootRecord root = ObjectWalControlTestFixtures.root(1, Optional.empty());
        byte[] rootBytes = WalRunControlCodec.encodeRoot(root).toByteArray();
        rootBytes[5] = 1;
        assertThatThrownBy(() -> WalRunControlCodec.decodeRoot(CanonicalBytes.copyOf(rootBytes)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("magic/version/reserved");

        byte[] pointer = WalRunControlCodec.encodePointer(new CurrentWalRunPointer(
                        ObjectWalControlTestFixtures.reference(WalRunControlKeys.rootKey(7, 1), root)))
                .toByteArray();
        byte[] withTrailing = java.util.Arrays.copyOf(pointer, pointer.length + 1);
        assertThatThrownBy(() -> WalRunControlCodec.decodePointer(CanonicalBytes.copyOf(withTrailing)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("trailing");

        ProviderResolvedExtentRowV1 proofRow = new ProviderResolvedExtentRowV1(
                WalLaneId.OBJECT_COST,
                0,
                256,
                512,
                ObjectWalControlTestFixtures.digest(7),
                new ProviderVersionProof(
                        ProviderProofMode.VERSION_BOUND_FULL_OBJECT_SHA256_V1, CanonicalBytes.copyOf(new byte[] {1})));
        WalRunCheckpointPageV1 page = new WalRunCheckpointPageV1(
                WalRunControlCodec.rootSha256(root),
                0,
                Optional.empty(),
                List.of(proofRow),
                LaneSequenceVector.empty().with(WalLaneId.OBJECT_COST, 0));
        assertThatThrownBy(() -> WalRunControlCodec.decodeCheckpointPage(
                        WalRunControlCodec.encodeCheckpointPage(page), root.providerConfiguration()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("differs from the Root");
    }

    @Test
    void everyControlRecordRejectsMagicVersionReservedTruncationAndTrailingMutations() {
        WalRunRootRecord root = ObjectWalControlTestFixtures.root(1, Optional.empty());
        WalRunReference reference = ObjectWalControlTestFixtures.reference(WalRunControlKeys.rootKey(7, 1), root);
        ProviderResolvedExtentRowV1 row = row(WalLaneId.OBJECT_LATENCY, 0, 3);
        WalRunCheckpointPageV1 page = new WalRunCheckpointPageV1(
                reference.rootSha256(), 0, Optional.empty(), List.of(row), LaneSequenceVector.of(0, -1, -1));
        com.nereusstream.domain.bytes.Sha256Digest pageSha =
                com.nereusstream.domain.bytes.Sha256Digest.hash(WalRunControlCodec.encodeCheckpointPage(page));
        WalCheckpointHeadV1 head = new WalCheckpointHeadV1(
                reference.rootSha256(),
                1,
                9,
                0,
                Optional.of(WalRunControlKeys.checkpointPageKey(7, 1, 0, pageSha)),
                Optional.of(pageSha),
                page.coveredThrough());
        WalRunSealRecord seal = new WalRunSealRecord(
                reference,
                page.coveredThrough(),
                WalRunControlKeys.checkpointHeadKey(7, 1),
                com.nereusstream.domain.bytes.Sha256Digest.hash(WalRunControlCodec.encodeCheckpointHead(head)),
                1,
                row.bodyLength());
        List<WireCase> cases = List.of(
                new WireCase(WalRunControlCodec.encodeRoot(root), WalRunControlCodec::decodeRoot),
                new WireCase(
                        WalRunControlCodec.encodePointer(new CurrentWalRunPointer(reference)),
                        WalRunControlCodec::decodePointer),
                new WireCase(WalRunControlCodec.encodeSeal(seal), WalRunControlCodec::decodeSeal),
                new WireCase(
                        WalRunControlCodec.encodeCheckpointPage(page),
                        value -> WalRunControlCodec.decodeCheckpointPage(value, root.providerConfiguration())),
                new WireCase(WalRunControlCodec.encodeCheckpointHead(head), WalRunControlCodec::decodeCheckpointHead));

        for (WireCase wireCase : cases) {
            for (int offset : new int[] {0, 4, 5, 6}) {
                byte[] mutation = wireCase.encoded().toByteArray();
                mutation[offset] ^= 1;
                assertThatThrownBy(() -> wireCase.decoder().accept(CanonicalBytes.copyOf(mutation)))
                        .isInstanceOf(IllegalArgumentException.class);
            }
            byte[] exact = wireCase.encoded().toByteArray();
            assertThatThrownBy(() -> wireCase.decoder()
                            .accept(CanonicalBytes.copyOf(java.util.Arrays.copyOf(exact, exact.length - 1))))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> wireCase.decoder()
                            .accept(CanonicalBytes.copyOf(java.util.Arrays.copyOf(exact, exact.length + 1))))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void kafkaSuccessorPredecessorBindingRoundTripsAndRejectsMissingOrUnknownProtocolCode() {
        WalRunRootRecord predecessorRoot = ObjectWalControlTestFixtures.root(1, Optional.empty());
        WalRunReference predecessor =
                ObjectWalControlTestFixtures.reference(WalRunControlKeys.rootKey(7, 1), predecessorRoot);
        WalRunSealRecord predecessorSeal = new WalRunSealRecord(
                predecessor,
                LaneSequenceVector.empty(),
                WalRunControlKeys.checkpointHeadKey(7, 1),
                ObjectWalControlTestFixtures.digest(10),
                0,
                0);
        String terminalHeadKey = "v2/object-wal/protocol/kafka/run-1/head";
        WalRunRootRecord successorRoot = ObjectWalControlTestFixtures.root(
                2,
                Optional.of(new WalRunPredecessor(
                        predecessor,
                        WalRunControlKeys.sealKey(7, 1),
                        WalRunControlCodec.sealSha256(predecessorSeal),
                        Optional.of(new TerminalProtocolCheckpointBindingV1(
                                com.nereusstream.domain.protocol.ProtocolKindV1.KAFKA,
                                terminalHeadKey,
                                ObjectWalControlTestFixtures.digest(11))))));
        CanonicalBytes encoded = WalRunControlCodec.encodeRoot(successorRoot);

        assertThat(WalRunControlCodec.decodeRoot(encoded)).isEqualTo(successorRoot);
        int bindingFlagOffset = encoded.length() - (1 + 1 + 2 + terminalHeadKey.length() + 32);
        byte[] missingBinding = encoded.toByteArray();
        missingBinding[bindingFlagOffset] = 0;
        assertThatThrownBy(() -> WalRunControlCodec.decodeRoot(CanonicalBytes.copyOf(missingBinding)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires the exact terminal Kafka");
        byte[] unknownProtocol = encoded.toByteArray();
        unknownProtocol[bindingFlagOffset + 1] = 9;
        assertThatThrownBy(() -> WalRunControlCodec.decodeRoot(CanonicalBytes.copyOf(unknownProtocol)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown ProtocolKindV1");

        assertGolden(encoded, 820, "ff5ae6f5eaf6795addaff09b4a73de3942608fdfa0f0a4cd88a4f890f0ecf3f4");
    }

    @Test
    void checkpointRowsArePhysicalOnlyOrderedAndBounded() {
        assertThat(ProviderResolvedExtentRowV1.class.getRecordComponents())
                .extracting(java.lang.reflect.RecordComponent::getName)
                .containsExactly(
                        "laneId", "laneSequence", "directoryPrefixEnd", "bodyLength", "objectSha256", "providerProof");
        assertThatThrownBy(() -> new WalRunCheckpointPageV1(
                        ObjectWalControlTestFixtures.digest(1),
                        0,
                        Optional.empty(),
                        List.of(row(WalLaneId.OBJECT_COST, 0, 1), row(WalLaneId.OBJECT_LATENCY, 0, 2)),
                        LaneSequenceVector.of(0, -1, 0)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ordered");
    }

    @Test
    void rootRejectsInternallyInconsistentCapsAndUnrecoverableMaximumInventory() {
        assertThatThrownBy(() -> new Nwg1RootAdmissionCaps(4096, 1024, 753, 16, 32, 32, 1008, 1024, 4096, 4096))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("plaintext/header/tag");
        assertThatThrownBy(() -> new Nwg1RootAdmissionCaps(512, 512, 240, 16, 32, 32, 1008, 1024, 4096, 4096))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("stored-frame");

        WalRunRootRecord root = ObjectWalControlTestFixtures.root(1, Optional.empty());
        Nwg1RootAdmissionCaps currentCaps = root.nwg1AdmissionCaps();
        assertThatThrownBy(() -> copyWithCaps(
                        root,
                        new Nwg1RootAdmissionCaps(
                                WalRunRootRecord.IMPLEMENTATION_MAX_CANONICAL_BODY_BYTES + 1,
                                currentCaps.maxDirectoryPrefixBytes(),
                                currentCaps.maxDirectoryPlaintextBytes(),
                                currentCaps.maxBindingContexts(),
                                currentCaps.maxAppendUnits(),
                                currentCaps.maxFrames(),
                                currentCaps.maxDecodedFrameBytes(),
                                currentCaps.maxStoredFrameBytes(),
                                currentCaps.maxDecodedAppendUnitBytes(),
                                currentCaps.maxTotalDecodedPayloadBytes())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("byte-array implementation cap");
        RecoveryEnvelopeLimits current = root.recoveryEnvelope();
        RecoveryEnvelopeLimits insufficientRange = copyRecovery(
                current,
                99,
                current.maxFullGetRequests(),
                current.maxCanonicalBodyBytes(),
                current.maxWorkingMemoryBytes());

        assertThatThrownBy(() -> copyWithRecovery(root, insufficientRange))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("every admitted extent");
        assertThatThrownBy(() -> copyWithRecovery(
                        root, copyListRecovery(current, 3, current.maxListedKeys(), current.maxListedKeyBytes())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("protocol Object");
        assertThatThrownBy(() -> copyWithRecovery(
                        root, copyListRecovery(current, current.maxListPages(), 101, current.maxListedKeyBytes())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("protocol Object");
        long extentKeyBytes = Math.multiplyExact(
                Math.addExact(root.bounds().maxExtentCount(), 1),
                ObjectWalLeafKeyV1.maximumFullKeyBytes(root.providerConfiguration()));
        assertThatThrownBy(() -> copyWithRecovery(
                        root,
                        copyListRecovery(
                                current,
                                current.maxListPages(),
                                current.maxListedKeys(),
                                Math.addExact(extentKeyBytes, 1023))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("listed-key byte");
        assertThatThrownBy(() -> copyWithRecovery(
                        root,
                        copyRecovery(
                                current,
                                current.maxRangeGetRequests(),
                                3,
                                current.maxCanonicalBodyBytes(),
                                current.maxWorkingMemoryBytes())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("protocol Object");
        assertThatThrownBy(() -> copyWithRecovery(
                        root,
                        copyRecovery(
                                current,
                                current.maxRangeGetRequests(),
                                current.maxFullGetRequests(),
                                8L * 1024 * 1024,
                                current.maxWorkingMemoryBytes())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("canonical-byte envelope");
        assertThatThrownBy(() -> copyWithRecovery(root, copyDepthRecovery(current, 3, 4)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lineage bounds");
        assertThatThrownBy(() -> copyWithRecovery(
                        root,
                        copyRecovery(
                                current,
                                current.maxRangeGetRequests(),
                                current.maxFullGetRequests(),
                                current.maxCanonicalBodyBytes(),
                                4095)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("memory/concurrency");
    }

    private static WalRunRootRecord copyWithRecovery(WalRunRootRecord root, RecoveryEnvelopeLimits recovery) {
        return new WalRunRootRecord(
                root.shardId(),
                root.shardRunEpoch(),
                root.walRunSessionId(),
                root.openedAtMillis(),
                root.protocolCellIdentity(),
                root.providerScopeId(),
                root.formatContract(),
                root.nwg1AdmissionCaps(),
                root.bounds(),
                root.checkpointPolicy(),
                root.providerConfiguration(),
                recovery,
                root.wrappedRunKey(),
                root.predecessor());
    }

    private static WalRunRootRecord copyWithCaps(WalRunRootRecord root, Nwg1RootAdmissionCaps caps) {
        return new WalRunRootRecord(
                root.shardId(),
                root.shardRunEpoch(),
                root.walRunSessionId(),
                root.openedAtMillis(),
                root.protocolCellIdentity(),
                root.providerScopeId(),
                root.formatContract(),
                caps,
                root.bounds(),
                root.checkpointPolicy(),
                root.providerConfiguration(),
                root.recoveryEnvelope(),
                root.wrappedRunKey(),
                root.predecessor());
    }

    private static RecoveryEnvelopeLimits copyRecovery(
            RecoveryEnvelopeLimits current,
            int maxRangeGetRequests,
            int maxFullGetRequests,
            long maxCanonicalBodyBytes,
            long maxWorkingMemoryBytes) {
        return new RecoveryEnvelopeLimits(
                current.maxLiveRoots(),
                current.maxPredecessorRuns(),
                current.maxListPages(),
                current.maxListedKeys(),
                current.maxListedKeyBytes(),
                current.maxHeadRequests(),
                maxRangeGetRequests,
                maxFullGetRequests,
                maxCanonicalBodyBytes,
                current.maxDecodedContexts(),
                current.maxDecodedFrames(),
                current.maxDecodedCommitSets(),
                maxWorkingMemoryBytes,
                current.maxConcurrency(),
                current.maxRetryAttempts(),
                current.maxWallTimeNanos());
    }

    private static RecoveryEnvelopeLimits copyListRecovery(
            RecoveryEnvelopeLimits current, int maxListPages, long maxListedKeys, long maxListedKeyBytes) {
        return new RecoveryEnvelopeLimits(
                current.maxLiveRoots(),
                current.maxPredecessorRuns(),
                maxListPages,
                maxListedKeys,
                maxListedKeyBytes,
                current.maxHeadRequests(),
                current.maxRangeGetRequests(),
                current.maxFullGetRequests(),
                current.maxCanonicalBodyBytes(),
                current.maxDecodedContexts(),
                current.maxDecodedFrames(),
                current.maxDecodedCommitSets(),
                current.maxWorkingMemoryBytes(),
                current.maxConcurrency(),
                current.maxRetryAttempts(),
                current.maxWallTimeNanos());
    }

    private static RecoveryEnvelopeLimits copyDepthRecovery(
            RecoveryEnvelopeLimits current, int maxPredecessorRuns, int maxLiveRoots) {
        return new RecoveryEnvelopeLimits(
                maxLiveRoots,
                maxPredecessorRuns,
                current.maxListPages(),
                current.maxListedKeys(),
                current.maxListedKeyBytes(),
                current.maxHeadRequests(),
                current.maxRangeGetRequests(),
                current.maxFullGetRequests(),
                current.maxCanonicalBodyBytes(),
                current.maxDecodedContexts(),
                current.maxDecodedFrames(),
                current.maxDecodedCommitSets(),
                current.maxWorkingMemoryBytes(),
                current.maxConcurrency(),
                current.maxRetryAttempts(),
                current.maxWallTimeNanos());
    }

    private static ProviderResolvedExtentRowV1 row(WalLaneId laneId, long sequence, int seed) {
        return new ProviderResolvedExtentRowV1(
                laneId, sequence, 256, 512, ObjectWalControlTestFixtures.digest(seed), ProviderVersionProof.none());
    }

    private static void assertGolden(CanonicalBytes value, int length, String sha256) {
        assertThat(value).satisfies(bytes -> {
            assertThat(bytes.length()).isEqualTo(length);
            assertThat(com.nereusstream.domain.bytes.Sha256Digest.hash(bytes).toHex())
                    .isEqualTo(sha256);
        });
    }

    private record WireCase(CanonicalBytes encoded, java.util.function.Consumer<CanonicalBytes> decoder) {}
}
