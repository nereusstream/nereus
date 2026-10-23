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

package com.nereusstream.kafka.bookkeeper.object.nwkcp1;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointStateV1;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaRecoveryCheckpointVectorV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectAuthorizationV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectRunRecoveryRetirementV1;
import com.nereusstream.metadata.oxia.v2.objectwal.OxiaCanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.control.TerminalProtocolCheckpointVerifierV1;
import com.nereusstream.storage.object.control.WalRunControlKeys;
import com.nereusstream.storage.object.control.WalRunKafkaProtocolEmptyClosureV1;
import com.nereusstream.storage.object.control.WalRunLifecycleManager;
import com.nereusstream.storage.object.control.WalRunRuntime;
import com.nereusstream.storage.object.control.WalRunTerminalClosureProofV1;
import com.nereusstream.storage.object.recovery.WalRunLineageRecovery;
import io.oxia.client.api.OxiaClientBuilder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Real shared-Run retirement: selected and unselected members, original UNKNOWN and bounded prefix recovery. */
@Timeout(180)
class M5ObjectRunRecoveryRetirementRealTest {
    @Test
    void completeSharedRunRetiresAfterEveryMemberTransfersWhileSelectedObjectsRemainReadable() throws Exception {
        try (var f = new KafkaObjectAuthorizationRealTest.Fixture(8)) {
            var a = f.partition(0, 6, 7);
            var b = f.partition(1, 6, 7);
            var c = f.partition(2, 6, 7);
            var protocolOnly = f.partition(3, 6, 7);
            var credit = f.pipeline.reservePhysicalBeforePosition(4096);
            var group = new ArrayList<>(List.of(a.prepare(f, body(0), credit), b.prepare(f, body(0), credit)));
            group.sort(Comparator.comparing(
                    value -> value.commit().expectedFence().bindingId().digest().toHex()));
            f.writeShared(group, 0, 2);
            assertThat(a.bridge.publishNext(() -> {})).isPresent();
            assertThat(b.bridge.publishNext(() -> {})).isPresent();
            var closedA = checkpointAndClose(f, a);
            // A physical unit rejected after actual PUT must be accounted by its real closed Owner, not a miss.
            f.authority.checkpoint(f.session.rootSha256(), f.run(c.fence), c.coordinator);
            var rejected = c.prepare(f, body(0));
            assertThatThrownBy(() -> c.tracker.reconcileRejectedAfterOwnerClose(rejected.ticket()))
                    .hasMessageContaining("physically resolved rejection");
            var closedC = new AtomicReference<KafkaObjectAuthorizationV1.ClosedHistory>();
            f.provider.afterNwgPut = identity -> closedC.set(f.authority.close(c.fence));
            assertThat(f.writeShared(List.of(rejected), 1, 3).authorizationFailures())
                    .hasSize(1);
            assertThat(c.tracker.pendingUnits()).isOne();
            long resolved = f.session.runtimeRecoveryState().resolvedExtentCount();
            c.tracker.reconcileRejectedAfterOwnerClose(rejected.ticket());
            assertThat(c.tracker.pendingUnits()).isZero();
            assertThat(f.session.runtimeRecoveryState().resolvedExtentCount()).isEqualTo(resolved);
            assertThat(c.snapshot().root().frontiers().highWatermark()).isZero();
            assertThat(closedC.get().head().grants()).isEmpty();
            publishProtocol(f, protocolOnly);
            var closedProtocolOnly = checkpointAndClose(f, protocolOnly);
            var proof = seal(f);
            var retirement = new KafkaObjectRunRecoveryRetirementV1(f.authority);
            String protocolKey =
                    WalRunControlKeys.kafkaProtocolCheckpointHeadKey(f.root.shardId(), f.root.shardRunEpoch());
            f.metadata.afterProtocolHead = () -> f.metadata.failReadKey = protocolKey;
            assertThatThrownBy(() -> retirement.retire(f.session, proof, f.verification, List.of(closedA)))
                    .hasMessageContaining("read unavailable");
            var originalProtocolCandidate = f.metadata.protocolHeadCandidates.getLast();
            assertThatThrownBy(f.session::close).hasMessageContaining("retains Provider operations");
            assertThatThrownBy(() -> retirement.retire(f.session, proof, f.verification, List.of(closedA)))
                    .hasMessageContaining("actual selected closed Owner");
            assertThat(f.metadata.protocolHeadCandidates.getLast()).isEqualTo(originalProtocolCandidate);
            assertThat(f.metadata.retirementCandidates).isEmpty();
            var closedB = checkpointAndClose(f, b);
            String key = proof.root().rootKey() + "/recovery-retirement-v1";
            f.metadata.afterRetirement = () -> f.metadata.failReadKey = key;
            assertThatThrownBy(() -> retirement.retire(
                            f.session,
                            proof,
                            f.verification,
                            List.of(closedA, closedB, closedC.get(), closedProtocolOnly)))
                    .hasMessageContaining("read unavailable");
            assertThat(f.metadata.retirementCandidates).hasSize(1);
            CanonicalBytes candidate = f.metadata.retirementCandidates.getFirst();
            assertThatThrownBy(f.session::close).hasMessageContaining("retains Provider operations");
            // Actual legal Owner advancement must preserve the selected immutable proof through UNKNOWN.
            var nextA = f.takeover(closedA, 7, 8);
            var nextB = f.takeover(closedB, 7, 8);
            var confirmed = retirement.retire(f.session, proof, f.verification, List.of());
            assertThat(confirmed.certificateSha256()).isEqualTo(Sha256Digest.hash(candidate));
            assertThat(f.metadata.retirementCandidates).hasSize(2).containsOnly(candidate);
            assertThat(confirmed.prefixRuns()).isOne();
            int reads = reads(f);
            f.provider.beforeRead = ignored -> {
                throw new AssertionError("retired payload was admitted to recovery");
            };
            f.rolloverRetired(nextA, proof, confirmed, 4);
            coldRecoverWithoutOldPayload(f, List.of(closedA, closedB, closedC.get(), closedProtocolOnly), 1);
            assertThat(reads(f)).isEqualTo(reads);
            f.provider.beforeRead = null;
            assertBody(f, nextA, 1, List.of(body(0)));
            assertBody(f, nextB, 1, List.of(body(0)));
            assertThat(reads(f)).isGreaterThan(reads);
            System.out.println("M5_RUN_RETIREMENT_SAMPLE kind=SHARED_SELECTED_AND_UNSELECTED prefixRuns=1"
                    + " physicalExtents=2 physicalMembers=3 protocolOnlyMembers=1 retainedRuns=1 coldPrefixGets=0"
                    + " selectedOriginalBytes=true originalUnknownCandidate=true wholeM5Authority=false");
        }
    }

    @Test
    void missingDirectPrefixVetoesThenExactContinuousExtensionAvoidsRetiredPayload() throws Exception {
        try (var f = new KafkaObjectAuthorizationRealTest.Fixture(8)) {
            var a = f.partition(0, 6, 7);
            a.append(f, body(0));
            var originalSession = f.session;
            var originalVerification = f.verification;
            var firstProof = f.rollover(a);
            String firstPrefix =
                    originalSession.rootRecord().providerConfiguration().exclusiveNamespacePrefix() + "/";
            a.append(f, body(1));
            var b = f.partition(1, 6, 7);
            b.append(f, body(0));
            var closedA = checkpointAndClose(f, a);
            var closedB = checkpointAndClose(f, b);
            var secondProof = seal(f);
            var second = new KafkaObjectRunRecoveryRetirementV1(f.authority);
            int beforeGap = reads(f);
            assertThatThrownBy(() -> second.retire(f.session, secondProof, f.verification, List.of(closedA, closedB)))
                    .hasMessageContaining("unqualified predecessor prefix");
            assertThat(reads(f)).isEqualTo(beforeGap);
            assertThat(f.metadata.retirementCandidates).isEmpty();
            var first = new KafkaObjectRunRecoveryRetirementV1(f.authority);
            int sourceRanges = f.session.recoverySnapshot().rangeGetRequests();
            int retiringRanges = originalSession.recoverySnapshot().rangeGetRequests();
            var firstCertificate =
                    first.retire(originalSession, firstProof, originalVerification, List.of(closedA), resolver(f));
            assertThat(firstCertificate.prefixRuns()).isOne();
            assertThat(f.session.recoverySnapshot().rangeGetRequests()).isEqualTo(sourceRanges);
            assertThat(originalSession.recoverySnapshot().rangeGetRequests()).isGreaterThan(retiringRanges);
            f.provider.beforeRead = key -> {
                if (key.startsWith(firstPrefix)) {
                    throw new AssertionError("prefix extension rescanned retired predecessor payload");
                }
            };
            var secondCertificate = second.retire(f.session, secondProof, f.verification, List.of(closedA, closedB));
            assertThat(secondCertificate.prefixRuns()).isEqualTo(2);
            int beforeCold = reads(f);
            f.provider.beforeRead = ignored -> {
                throw new AssertionError("retired prefix payload was scanned");
            };
            f.rolloverRetired(a, secondProof, secondCertificate, 5);
            coldRecoverWithoutOldPayload(f, List.of(closedA, closedB), 2);
            assertThat(reads(f)).isEqualTo(beforeCold);
            f.provider.beforeRead = null;
            var nextA = f.takeover(closedA, 7, 8);
            var nextB = f.takeover(closedB, 7, 8);
            assertBody(f, nextA, 2, List.of(body(0), body(1)));
            assertBody(f, nextB, 1, List.of(body(0)));
            System.out.println("M5_RUN_RETIREMENT_SAMPLE kind=CONTIGUOUS_PREFIX prefixRuns=2"
                    + " actualEpochs=1,2,5 retainedRuns=1 coldPrefixGets=0 selectedOriginalBytes=true"
                    + " missingPrefixVeto=true wholeM5Authority=false");
        }
    }

    @Test
    void checkpointOnlyEmptyRunTransfersFullStateAndColdRecoversFromSelectedCommonCheckpoint() throws Exception {
        try (var f = new KafkaObjectAuthorizationRealTest.Fixture(8)) {
            var a = f.partition(0, 6, 7);
            var data = KafkaObjectAuthorizationRealTest.data(0, 7, 0, true);
            var marker = KafkaObjectAuthorizationRealTest.marker(
                    1, 7, org.apache.kafka.common.record.ControlRecordType.COMMIT);
            a.append(f, data);
            a.append(f, marker);
            var firstSession = f.session;
            var firstVerification = f.verification;
            var firstProof = f.rollover(a);
            var selected = protocolState(f, a);
            assertThat(selected.producerState())
                    .isNotEqualTo(com.nereusstream.kafka.bookkeeper.commit.KafkaCommittedProducerStateV1.empty());
            assertThat(selected.transactionState().completedTransactions()).hasSize(1);
            publishProtocol(f, a);
            var closed = checkpointAndClose(f, a);
            var proof = seal(f);
            assertThat(proof.aggregateExtentCount()).isZero();
            var first = new KafkaObjectRunRecoveryRetirementV1(f.authority);
            first.retire(firstSession, firstProof, firstVerification, List.of(closed), resolver(f));
            var retirement = new KafkaObjectRunRecoveryRetirementV1(f.authority);
            assertThatThrownBy(() -> retirement.retire(f.session, proof, f.verification, List.of()))
                    .hasMessageContaining("protocol checkpoint member lacks its actual selected closed Owner");
            var wrong = new KafkaProtocolCheckpointStateV1(
                    selected.vector(),
                    com.nereusstream.kafka.bookkeeper.commit.KafkaCommittedProducerStateV1.empty(),
                    selected.transactionState(),
                    selected.leaderEpochIndex());
            assertThatThrownBy(() -> KafkaObjectAuthorizedRecoveryV1.verifyCheckpointTransfer(
                            f.authority, closed, wrong, resolver(f), f.session))
                    .hasMessageContaining("full protocol state did not transfer");
            var confirmed = retirement.retire(f.session, proof, f.verification, List.of(closed), resolver(f));
            assertThat(confirmed.prefixRuns()).isEqualTo(2);
            int beforeCold = reads(f);
            f.provider.beforeRead = ignored -> {
                throw new AssertionError("cold retirement fetched old payload");
            };
            f.rolloverRetired(a, proof, confirmed, 5);
            coldRecoverWithoutOldPayload(f, List.of(closed), 2);
            assertThat(reads(f)).isEqualTo(beforeCold);
            f.provider.beforeRead = null;
            var next = f.takeover(closed, 7, 8);
            assertBody(f, next, 2, List.of(data, marker));
            System.out.println("M5_RUN_RETIREMENT_SAMPLE kind=CHECKPOINT_ONLY_EMPTY_RUN physicalExtents=0"
                    + " protocolOnlyMembers=1 prefixRuns=2 fullProducerTransactionLeaderState=true"
                    + " sameOffsetWrongStateRejected=true freshClientColdRecovery=true selectedOriginalBytes=true"
                    + " wholeM5Authority=false");
        }
    }

    private static ObjectKafkaProtocolCheckpointStoreV1 publishProtocol(
            KafkaObjectAuthorizationRealTest.Fixture f, KafkaObjectAuthorizationRealTest.Partition partition) {
        String prefix = f.root.providerConfiguration().exclusiveNamespacePrefix() + "/runs/" + f.root.shardRunEpoch();
        var backend = new StorageObjectNwkcp1BackendV1(f.session, f.metadata, prefix);
        var store = new ObjectKafkaProtocolCheckpointStoreV1(
                prefix,
                new KafkaNwkcp1WalRunContextV1(f.session.rootSha256(), f.run(partition.fence)),
                partition.fence.ownerEpoch(),
                backend);
        store.publish(protocolState(f, partition)).toCompletableFuture().join();
        return store;
    }

    private static KafkaObjectAuthorizedRecoveryV1.Resolver resolver(KafkaObjectAuthorizationRealTest.Fixture f) {
        return grant -> {
            var sha = grant.locator().extent().walRunRootSha();
            var session = f.sources.get(sha);
            var root = f.roots.get(sha);
            if (session == null || root == null) {
                throw new IllegalStateException("actual retained source session is absent");
            }
            var context = new com.nereusstream.storage.object.nwg1.Nwg1VerificationContextV1(
                    root.protocolCellIdentity(),
                    root.providerScopeId().digest().bytes().toByteArray(),
                    sha.bytes().toByteArray(),
                    com.nereusstream.storage.object.nwg1.Nwg1EnvelopeV1.decode(
                            root.wrappedRunKey().framedBytes().toByteArray()),
                    (binding, kind, version) -> Sha256Digest.hash(CanonicalBytes.copyOf(
                                    grant.fence().toString().getBytes(java.nio.charset.StandardCharsets.UTF_8)))
                            .bytes()
                            .toByteArray(),
                    f.verification.nativePayloadVerifier(),
                    0,
                    0);
            return new KafkaObjectAuthorizedRecoveryV1.Source(session, context);
        };
    }

    @Test
    void actualHeadCasPinsNativeIoAndOriginalUnknownBeforeTerminalFence() throws Exception {
        try (var f = new KafkaObjectAuthorizationRealTest.Fixture(8)) {
            var partition = f.partition(0, 6, 7);
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var failRead = new AtomicBoolean();
            var holdCas = new AtomicBoolean(true);
            var original = new AtomicReference<CanonicalBytes>();
            var metadata = new CanonicalControlMetadataStore() {
                @Override
                public Optional<CanonicalBytes> get(String key) {
                    if (key.endsWith("/protocol/kafka/nwkcp1-v1/head") && failRead.getAndSet(false)) {
                        throw new IllegalStateException("actual Head confirmation reply unavailable");
                    }
                    return f.metadata.get(key);
                }

                @Override
                public ControlMutationOutcome putIfAbsent(String key, CanonicalBytes value) {
                    return f.metadata.putIfAbsent(key, value);
                }

                @Override
                public ControlMutationOutcome compareAndSet(
                        String key, Optional<CanonicalBytes> expected, CanonicalBytes value) {
                    var outcome = f.metadata.compareAndSet(key, expected, value);
                    if (key.endsWith("/protocol/kafka/nwkcp1-v1/head") && holdCas.getAndSet(false)) {
                        original.set(value);
                        entered.countDown();
                        await(release);
                        failRead.set(true);
                        return ControlMutationOutcome.RESPONSE_UNKNOWN;
                    }
                    return outcome;
                }
            };
            String prefix = f.root.providerConfiguration().exclusiveNamespacePrefix() + "/runs/1";
            var backend = new StorageObjectNwkcp1BackendV1(f.session, metadata, prefix);
            var context = new KafkaNwkcp1WalRunContextV1(f.session.rootSha256(), f.run(partition.fence));
            var store =
                    new ObjectKafkaProtocolCheckpointStoreV1(prefix, context, partition.fence.ownerEpoch(), backend);
            var state = protocolState(f, partition);
            var executor = Executors.newSingleThreadExecutor();
            try {
                var call = executor.submit(
                        () -> store.publish(state).toCompletableFuture().join());
                assertThat(entered.await(15, TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(f.session::close).hasMessageContaining("retains Provider operations");
                release.countDown();
                assertThatThrownBy(() -> call.get(15, TimeUnit.SECONDS))
                        .hasRootCauseMessage("actual Head confirmation reply unavailable");
                assertThatThrownBy(f.session::close).hasMessageContaining("retains Provider operations");
                assertThat(backend.readHead(backend.protocolHeadKey(prefix))
                                .toCompletableFuture()
                                .join())
                        .contains(original.get());
                f.session.sealRuntime();
                var proof = f.pipeline.publishPhysicalSeal(new WalRunLifecycleManager(f.metadata));
                store.terminalize(proof).toCompletableFuture().join();
                var before = KafkaProtocolCheckpointHeadCodecV1.decode(prefix, f.session.rootSha256(), original.get());
                CanonicalBytes late = KafkaProtocolCheckpointHeadCodecV1.encode(
                        prefix, f.session.rootSha256(), before.takeover(before.publisherEpoch() + 1));
                assertThat(f.metadata.compareAndSet(backend.protocolHeadKey(prefix), Optional.of(original.get()), late))
                        .isEqualTo(ControlMutationOutcome.DEFINITIVE_CONFLICT);
                var emptyClose = WalRunKafkaProtocolEmptyClosureV1.begin(f.session, proof);
                assertThat(emptyClose.closeIfAbsent(f.metadata)).isEmpty();
                assertThat(store.currentHead()
                                .toCompletableFuture()
                                .join()
                                .orElseThrow()
                                .state())
                        .isEqualTo(KafkaProtocolCheckpointHeadStateV1.TERMINAL);
            } finally {
                release.countDown();
                executor.shutdownNow();
                assertThat(executor.awaitTermination(15, TimeUnit.SECONDS)).isTrue();
            }
            System.out.println("M5_PROTOCOL_DOMAIN_SAMPLE kind=EXISTING_HEAD_FIRST originalIoPinned=true"
                    + " unknownConfirmationPinned=true lateExactCasRejected=true"
                    + " physicalExtents=0 wholeM5Authority=false");
        }
    }

    @Test
    void absentDomainCloseSurvivesReplyLossAndFencesActualLateFirstHeadCas() throws Exception {
        try (var f = new KafkaObjectAuthorizationRealTest.Fixture(8)) {
            var partition = f.partition(0, 6, 7);
            String prefix = f.root.providerConfiguration().exclusiveNamespacePrefix() + "/runs/1";
            var backend = new StorageObjectNwkcp1BackendV1(f.session, f.metadata, prefix);
            var state = protocolState(f, partition);
            var encoded = Nwkcp1CodecV1.encode(prefix, new Nwkcp1ObjectV1(f.session.rootSha256(), List.of(state)));
            assertThat(backend.conditionalCreateObject(encoded.key(), encoded.body(), encoded.digest())
                            .toCompletableFuture()
                            .join()
                            .createdToken())
                    .isPresent();
            var lateHead = KafkaProtocolCheckpointHeadV1.open(
                    f.session.rootSha256(),
                    partition.fence.ownerEpoch(),
                    null,
                    encoded,
                    List.of(KafkaCheckpointCoverageV1.from(state.vector())));
            CanonicalBytes late = KafkaProtocolCheckpointHeadCodecV1.encode(prefix, f.session.rootSha256(), lateHead);
            var proof = seal(f);
            var attempt = WalRunKafkaProtocolEmptyClosureV1.begin(f.session, proof);
            var loseReply = new AtomicBoolean(true);
            var failRead = new AtomicBoolean();
            var metadata = new CanonicalControlMetadataStore() {
                @Override
                public Optional<CanonicalBytes> get(String key) {
                    if (key.equals(backend.protocolHeadKey(prefix)) && failRead.getAndSet(false)) {
                        throw new IllegalStateException("actual empty-close confirmation reply unavailable");
                    }
                    return f.metadata.get(key);
                }

                @Override
                public ControlMutationOutcome putIfAbsent(String key, CanonicalBytes value) {
                    return f.metadata.putIfAbsent(key, value);
                }

                @Override
                public ControlMutationOutcome compareAndSet(
                        String key, Optional<CanonicalBytes> expected, CanonicalBytes value) {
                    var result = f.metadata.compareAndSet(key, expected, value);
                    if (loseReply.getAndSet(false)) {
                        failRead.set(true);
                        return ControlMutationOutcome.RESPONSE_UNKNOWN;
                    }
                    return result;
                }
            };
            assertThatThrownBy(() -> attempt.closeIfAbsent(metadata)).hasMessageContaining("reply unavailable");
            assertThatThrownBy(f.session::close).hasMessageContaining("retains Provider operations");
            var confirmed = attempt.closeIfAbsent(metadata).orElseThrow();
            assertThat(confirmed.value().seal().root()).isEqualTo(proof.root());
            assertThat(f.metadata.compareAndSet(backend.protocolHeadKey(prefix), Optional.empty(), late))
                    .isEqualTo(ControlMutationOutcome.DEFINITIVE_CONFLICT);
            var context = new KafkaNwkcp1WalRunContextV1(f.session.rootSha256(), f.run(partition.fence));
            assertThatThrownBy(() -> new KafkaObjectCheckpointRecoveryV1(prefix, context, backend, Optional.empty())
                            .recover()
                            .toCompletableFuture()
                            .join())
                    .hasRootCauseMessage("Root-scoped protocol domain is closed without a selected checkpoint");
            assertThat(f.metadata.get(backend.protocolHeadKey(prefix))).contains(confirmed.exactValue());
            System.out.println("M5_PROTOCOL_DOMAIN_SAMPLE kind=ABSENT_CLOSE_FIRST physicalExtents=0"
                    + " actualProtocolObject=true originalUnknownCandidate=true lateFirstCasRejected=true"
                    + " checkpointFallbackRejected=true wholeM5Authority=false");
        }
    }

    private static KafkaProtocolCheckpointStateV1 protocolState(
            KafkaObjectAuthorizationRealTest.Fixture f, KafkaObjectAuthorizationRealTest.Partition partition) {
        var snapshot = partition.snapshot();
        long end = snapshot.root().frontiers().durableEndOffset();
        return new KafkaProtocolCheckpointStateV1(
                new KafkaRecoveryCheckpointVectorV1(f.run(partition.fence), end, end, end, end),
                snapshot.committedProducerState(),
                snapshot.transactionState(),
                snapshot.leaderEpochIndex());
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(15, TimeUnit.SECONDS)) {
                throw new IllegalStateException("actual Head CAS response hold timed out");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(failure);
        }
    }

    private static WalRunTerminalClosureProofV1 seal(KafkaObjectAuthorizationRealTest.Fixture fixture) {
        fixture.session.stopAdmission(WalRunRuntime.StopReason.OWNER_REQUEST);
        fixture.session.sealRuntime();
        return fixture.pipeline.publishPhysicalSeal(new WalRunLifecycleManager(fixture.metadata));
    }

    private static KafkaObjectAuthorizationV1.ClosedHistory checkpointAndClose(
            KafkaObjectAuthorizationRealTest.Fixture fixture, KafkaObjectAuthorizationRealTest.Partition partition) {
        fixture.authority.checkpoint(fixture.session.rootSha256(), fixture.run(partition.fence), partition.coordinator);
        return fixture.authority.close(partition.fence);
    }

    private static void coldRecoverWithoutOldPayload(
            KafkaObjectAuthorizationRealTest.Fixture fixture,
            List<KafkaObjectAuthorizationV1.ClosedHistory> members,
            long retiredRuns)
            throws Exception {
        try (var client = OxiaClientBuilder.create(System.getProperty("nereus.nsip1.object.oxia"))
                .namespace("default")
                .requestTimeout(Duration.ofSeconds(10))
                .asyncClient()
                .get()) {
            var metadata = new OxiaCanonicalControlMetadataStore(client, fixture.authorityRoot, fixture.root.shardId());
            var authority = new KafkaObjectAuthorizationV1(
                    metadata, fixture.root.shardId(), members.getFirst().head().bounds());
            var reader = new KafkaObjectRunRecoveryRetirementV1(authority);
            long started = System.nanoTime();
            var recovered = new WalRunLineageRecovery(
                            metadata,
                            TerminalProtocolCheckpointVerifierV1.failClosed(),
                            TerminalProtocolCheckpointVerifierV1.ProtocolObjectRecoveryReaderFactoryV1.failClosed(),
                            reader)
                    .recover(
                            WalRunControlKeys.pointerKey(fixture.root.shardId()),
                            fixture.root.protocolCellIdentity(),
                            fixture.root.providerScopeId(),
                            Optional.empty(),
                            System::nanoTime);
            assertThat(recovered.runs()).singleElement().satisfies(value -> assertThat(value.root())
                    .isEqualTo(fixture.root));
            for (var member : members) {
                var selected = authority.readClosed(member.head().fence());
                var state = KafkaObjectAuthorizedRecoveryV1.recover(
                                authority,
                                selected,
                                fixture.run(selected.head().fence()),
                                fixture.session.rootSha256(),
                                grant -> {
                                    throw new AssertionError("retired grant was used for protocol restoration");
                                })
                        .state();
                var checkpoint = authority.checkpoint(selected).orElseThrow();
                assertThat(state.vector().recoveryCoveredThrough())
                        .isEqualTo(selected.head().endOffset());
                assertThat(state.producerState()).isEqualTo(checkpoint.producerState());
                assertThat(state.transactionState()).isEqualTo(checkpoint.transactionState());
                assertThat(state.leaderEpochIndex()).isEqualTo(checkpoint.leaderEpochIndex());
            }
            System.out.println("M5_RUN_RETIREMENT_RECOVERY retiredRuns=" + retiredRuns + " nanos="
                    + (System.nanoTime() - started) + " actualFreshOxiaClient=true retainedRuns=1");
        }
    }

    private static int reads(KafkaObjectAuthorizationRealTest.Fixture fixture) {
        return fixture.provider.fullGets + fixture.provider.ranges;
    }

    private static void assertBody(
            KafkaObjectAuthorizationRealTest.Fixture fixture,
            KafkaObjectAuthorizationRealTest.Partition partition,
            long end,
            List<CanonicalBytes> expected) {
        assertThat(fixture.readAtLso(partition, end).ranges())
                .extracting(value -> value.kafkaRecordBatchBytes())
                .containsExactlyElementsOf(expected);
    }

    private static CanonicalBytes body(long offset) {
        return KafkaObjectAuthorizationRealTest.data(offset, 7, Math.toIntExact(offset), false);
    }
}
