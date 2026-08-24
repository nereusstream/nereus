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
import com.nereusstream.domain.codec.DeterministicTopicIdsV1;
import com.nereusstream.domain.codec.TopicIncarnationIdentityCodecV1;
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.KafkaCellId;
import com.nereusstream.domain.protocol.KafkaProtocolCellIdentity;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaNativeAssignedRecordBatchV1;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaRawAssignedRecordBatchFactsV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaCommittedProducerStateV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaLeaderEpochIndexV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaProtocolAppendPlanV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaProtocolBatchDeltaV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaSpeculativeCommitV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionBatchKindV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionStateV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.object.ObjectKafkaTestFixtures;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaNwg1ObjectPipelineV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectCompletionTrackerV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectNativeStateV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectPhysicalFrontiersV1;
import com.nereusstream.kafka.bookkeeper.object.recovery.KafkaNativePartitionOwnerAuthorityV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.control.CurrentWalRunPointer;
import com.nereusstream.storage.object.control.Nwg1RootAdmissionCaps;
import com.nereusstream.storage.object.control.ObjectProviderAccessProfile;
import com.nereusstream.storage.object.control.ObjectProviderRootConfiguration;
import com.nereusstream.storage.object.control.ProviderProofMode;
import com.nereusstream.storage.object.control.ProviderResolvedExtentRowV1;
import com.nereusstream.storage.object.control.WalCheckpointHeadV1;
import com.nereusstream.storage.object.control.WalCheckpointPolicy;
import com.nereusstream.storage.object.control.WalCheckpointPublisher;
import com.nereusstream.storage.object.control.WalRunBounds;
import com.nereusstream.storage.object.control.WalRunControlCodec;
import com.nereusstream.storage.object.control.WalRunControlKeys;
import com.nereusstream.storage.object.control.WalRunFormatContractV1;
import com.nereusstream.storage.object.control.WalRunLifecycleManager;
import com.nereusstream.storage.object.control.WalRunObjectSession;
import com.nereusstream.storage.object.control.WalRunPredecessor;
import com.nereusstream.storage.object.control.WalRunRootRecord;
import com.nereusstream.storage.object.control.WalRunRuntime;
import com.nereusstream.storage.object.control.WalRunSealRecord;
import com.nereusstream.storage.object.kms.KmsCellSession;
import com.nereusstream.storage.object.kms.KmsTransport;
import com.nereusstream.storage.object.kms.WrappedRunKeyEnvelope;
import com.nereusstream.storage.object.nwg1.GroupEncodingPlanV1;
import com.nereusstream.storage.object.nwg1.Nwg1CommitmentsV1;
import com.nereusstream.storage.object.nwg1.Nwg1DirectoryV1;
import com.nereusstream.storage.object.nwg1.Nwg1EnvelopeV1;
import com.nereusstream.storage.object.nwg1.Nwg1IsolationScopeV1;
import com.nereusstream.storage.object.nwg1.Nwg1ObjectReaderV1;
import com.nereusstream.storage.object.nwg1.Nwg1RejectionV1;
import com.nereusstream.storage.object.nwg1.Nwg1ValidationException;
import com.nereusstream.storage.object.nwg1.Nwg1ValidationStageV1;
import com.nereusstream.storage.object.nwg1.Nwg1VerificationContextV1;
import com.nereusstream.storage.object.provider.C1ObjectProviderSession;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.provider.ObjectProviderCapabilities;
import com.nereusstream.storage.object.provider.ObjectProviderTransport;
import com.nereusstream.storage.object.recovery.OwnerOpenRecoveryCoordinator;
import com.nereusstream.storage.object.recovery.RecoveryEnvelopeLimits;
import com.nereusstream.storage.object.recovery.WalRunLineageRecovery;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StorageObjectNwkcp1BackendV1Test {
    private static final String ROOT_PREFIX = "scope/cells/01/shards/0007/runs/0000000000000000001";

    @TempDir
    Path tempDirectory;

    @Test
    void rejectsSyntacticallyValidUnissuedCreatedAndHeadSelectedTokensBeforeProviderIo() {
        FakeTransport transport = new FakeTransport();
        ExactMetadata metadata = new ExactMetadata();
        WalRunRootRecord rootRecord = root();
        var freshRoot = new WalRunLifecycleManager(metadata)
                .createRootAndInitializePointer(
                        WalRunControlKeys.rootKey(rootRecord.shardId(), rootRecord.shardRunEpoch()), rootRecord);
        var objectSession = WalRunObjectSession.openNew(
                freshRoot.ownerAuthority().orElseThrow(),
                new C1ObjectProviderSession(
                        transport,
                        rootRecord.providerScopeId(),
                        rootRecord.providerConfiguration().exclusiveNamespacePrefix(),
                        rootRecord.providerConfiguration().maxObjectBodyBytes(),
                        rootRecord.nwg1AdmissionCaps().maxDirectoryPrefixBytes()),
                new KmsCellSession(
                        new FakeKmsTransport(),
                        rootRecord.providerScopeId(),
                        "kms/cell-a",
                        2,
                        new SecureRandom(new byte[] {1, 2, 3})),
                () -> 0);
        var backend = new StorageObjectNwkcp1BackendV1(objectSession, metadata);
        Sha256Digest digest = ObjectKafkaTestFixtures.digest(55);
        String key = Nwkcp1ObjectKeyV1.objectKey(ROOT_PREFIX, digest);
        Nwkcp1BackendV1.CreatedObjectToken forgedCreated = new Nwkcp1BackendV1.CreatedObjectToken() {
            @Override
            public String key() {
                return key;
            }

            @Override
            public long length() {
                return 64;
            }

            @Override
            public Sha256Digest digest() {
                return digest;
            }
        };
        Nwkcp1BackendV1.SelectedObjectToken forgedSelected = new Nwkcp1BackendV1.SelectedObjectToken() {
            @Override
            public String key() {
                return key;
            }

            @Override
            public long length() {
                return 64;
            }

            @Override
            public Sha256Digest digest() {
                return digest;
            }

            @Override
            public Sha256Digest exactHeadValueSha256() {
                return ObjectKafkaTestFixtures.digest(56);
            }
        };

        assertThatThrownBy(() -> backend.readCreatedObject(forgedCreated)
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("NWKCP1 created-object token was not issued by this backend");
        assertThatThrownBy(() -> backend.readSelectedObject(forgedSelected, false)
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("NWKCP1 selected-object token was not issued by this backend");
        assertThat(transport.fullGets).isZero();
        assertThat(transport.objects).isEmpty();
        objectSession.close();
    }

    @Test
    void mapsC1ProviderAndExactControlCasIncludingResponseLoss() {
        FakeTransport transport = new FakeTransport();
        transport.unknownNextCreate = true;
        ExactMetadata metadata = new ExactMetadata();
        WalRunRootRecord rootRecord = root();
        var provider = new C1ObjectProviderSession(
                transport,
                rootRecord.providerScopeId(),
                rootRecord.providerConfiguration().exclusiveNamespacePrefix(),
                rootRecord.providerConfiguration().maxObjectBodyBytes(),
                rootRecord.nwg1AdmissionCaps().maxDirectoryPrefixBytes());
        var freshRoot = new WalRunLifecycleManager(metadata)
                .createRootAndInitializePointer(
                        WalRunControlKeys.rootKey(rootRecord.shardId(), rootRecord.shardRunEpoch()), rootRecord);
        var objectSession = WalRunObjectSession.openNew(
                freshRoot.ownerAuthority().orElseThrow(),
                provider,
                new KmsCellSession(
                        new FakeKmsTransport(),
                        rootRecord.providerScopeId(),
                        "kms/cell-a",
                        2,
                        new SecureRandom(new byte[] {1, 2, 3})),
                () -> 0);
        metadata.unknownNextCas = true;
        var backend = new StorageObjectNwkcp1BackendV1(objectSession, metadata);
        var root = objectSession.rootSha256();
        var context = new KafkaNwkcp1WalRunContextV1(root, ObjectKafkaTestFixtures.runBinding());
        var store = new ObjectKafkaProtocolCheckpointStoreV1(ROOT_PREFIX, context, 9, backend);

        var state = ObjectKafkaTestFixtures.checkpoint(100);
        assertThat(store.publish(state).toCompletableFuture().join().physicalIdentity())
                .isZero();
        assertThat(store.currentHead()
                        .toCompletableFuture()
                        .join()
                        .orElseThrow()
                        .coveredThroughVector())
                .containsExactly(KafkaCheckpointCoverageV1.from(state.vector()));
        assertThat(transport.fullGets).isGreaterThanOrEqualTo(2);
        assertThat(objectSession.state()).isEqualTo(WalRunObjectSession.State.OPEN);
        assertThat(objectSession.runtimeState()).isEqualTo(WalRunRuntime.State.ADMITTING);
        assertThat(store.publish(state).toCompletableFuture().join().physicalIdentity())
                .isZero();
        var advancedState = ObjectKafkaTestFixtures.checkpoint(101);
        assertThat(store.publish(advancedState).toCompletableFuture().join().physicalIdentity())
                .isEqualTo(1);

        var recovered = new KafkaObjectCheckpointRecoveryV1(ROOT_PREFIX, context, backend, Optional.empty())
                .recover()
                .toCompletableFuture()
                .join();
        assertThat(recovered.state()).isEqualTo(advancedState);
        assertThat(recovered.source()).isEqualTo(KafkaObjectCheckpointRecoveryV1.Source.NWKCP1);
        var budget = objectSession.recoverySnapshot();
        assertThat(budget.fullGetRequests()).isEqualTo(2);
        assertThat(budget.decodedContexts()).isOne();
        assertThat(budget.decodedCommitSets()).isOne();
        assertThat(budget.canonicalBodyBytes()).isGreaterThan(state.vector().recoveryCoveredThrough());
        objectSession.close();
        assertThat(objectSession.state()).isEqualTo(WalRunObjectSession.State.CLOSED);
    }

    @Test
    void terminalHeadRequiresExactPhysicalClosureAndFencesFurtherPublicationAndTakeover() {
        FakeTransport transport = new FakeTransport();
        ExactMetadata metadata = new ExactMetadata();
        WalRunRootRecord rootRecord = root();
        var lifecycle = new WalRunLifecycleManager(metadata);
        var freshRoot = lifecycle.createRootAndInitializePointer(
                WalRunControlKeys.rootKey(rootRecord.shardId(), rootRecord.shardRunEpoch()), rootRecord);
        var rootReference = freshRoot.reference();
        var provider = new C1ObjectProviderSession(
                transport,
                rootRecord.providerScopeId(),
                rootRecord.providerConfiguration().exclusiveNamespacePrefix(),
                rootRecord.providerConfiguration().maxObjectBodyBytes(),
                rootRecord.nwg1AdmissionCaps().maxDirectoryPrefixBytes());
        var objectSession = WalRunObjectSession.openNew(
                freshRoot.ownerAuthority().orElseThrow(),
                provider,
                new KmsCellSession(
                        new FakeKmsTransport(),
                        rootRecord.providerScopeId(),
                        "kms/cell-a",
                        2,
                        new SecureRandom(new byte[] {1, 2, 3})),
                () -> 0);
        var backend = new StorageObjectNwkcp1BackendV1(objectSession, metadata);
        var context = new KafkaNwkcp1WalRunContextV1(objectSession.rootSha256(), ObjectKafkaTestFixtures.runBinding());
        var store = new ObjectKafkaProtocolCheckpointStoreV1(ROOT_PREFIX, context, 9, backend);
        var state = ObjectKafkaTestFixtures.checkpoint(100);
        store.publish(state).toCompletableFuture().join();

        String physicalHeadKey = WalRunControlKeys.checkpointHeadKey(7, 1);
        CanonicalBytes physicalHeadBytes =
                WalRunControlCodec.encodeCheckpointHead(WalCheckpointHeadV1.empty(objectSession.rootSha256(), 1, 1));
        assertThat(metadata.putIfAbsent(physicalHeadKey, physicalHeadBytes)).isEqualTo(ControlMutationOutcome.APPLIED);
        objectSession.stopAdmission(WalRunRuntime.StopReason.OWNER_REQUEST);
        var terminalSequence = objectSession.sealRuntime();
        var seal = new WalRunSealRecord(
                rootReference, terminalSequence, physicalHeadKey, Sha256Digest.hash(physicalHeadBytes), 0, 0);
        objectSession.drain();
        objectSession.requireTerminalClosable();
        var proof = lifecycle.publishSeal(WalRunControlKeys.sealKey(7, 1), seal, objectSession);

        var binding = store.terminalize(proof).toCompletableFuture().join();
        assertThat(store.currentHead()
                        .toCompletableFuture()
                        .join()
                        .orElseThrow()
                        .state())
                .isEqualTo(KafkaProtocolCheckpointHeadStateV1.TERMINAL);
        int objectsAtTerminal = transport.objects.size();
        assertThatThrownBy(() -> store.publish(ObjectKafkaTestFixtures.checkpoint(101))
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("checkpoint Head is terminal or publisher-fenced");
        assertThat(transport.objects).hasSize(objectsAtTerminal);
        assertThatThrownBy(() -> store.takeover(10).toCompletableFuture().join())
                .hasRootCauseMessage("only an OPEN checkpoint Head permits an advancing takeover");

        String pointerKey = WalRunControlKeys.pointerKey(7);
        var currentPointer = new CurrentWalRunPointer(rootReference);
        var successorLifecycle = new WalRunLifecycleManager(metadata, store, protocolObjectRoot -> {
            if (!protocolObjectRoot.equals(rootRecord)) {
                throw new IllegalArgumentException("test protocol reader Root differs");
            }
            return objectSession.terminalLineageProtocolObjectReader();
        });
        successorLifecycle.initializePointer(pointerKey, currentPointer);
        String sealKey = WalRunControlKeys.sealKey(7, 1);
        var predecessor = new WalRunPredecessor(
                rootReference, sealKey, WalRunControlCodec.sealSha256(seal), Optional.of(binding));
        WalRunRootRecord successorRoot = successor(rootRecord, predecessor);
        var successorPublication = successorLifecycle.publishSuccessorAndAdvance(
                pointerKey, currentPointer, sealKey, seal, WalRunControlKeys.rootKey(7, 2), successorRoot);
        var successorReference = successorPublication;
        assertThat(successorLifecycle.readPointer(pointerKey).current()).isEqualTo(successorReference);
        assertThat(successorReference.shardRunEpoch()).isEqualTo(2);
        objectSession.close();
        assertThat(objectSession.state()).isEqualTo(WalRunObjectSession.State.CLOSED);
    }

    @Test
    void unresolvedLaneZeroCandidateDoesNotStopLaneOneAndExactRetryDoesNotRepeatPut() {
        PipelineFixture fixture = pipelineFixture();
        fixture.transport.unknownNextCreate = true;
        var protocolStore = new ObjectKafkaProtocolCheckpointStoreV1(
                ROOT_PREFIX,
                new KafkaNwkcp1WalRunContextV1(fixture.session.rootSha256(), ObjectKafkaTestFixtures.runBinding()),
                9,
                new StorageObjectNwkcp1BackendV1(fixture.session, fixture.metadata));
        protocolStore
                .publish(ObjectKafkaTestFixtures.checkpoint(100))
                .toCompletableFuture()
                .join();
        assertThat(fixture.session.state()).isEqualTo(WalRunObjectSession.State.OPEN);
        assertThat(fixture.session.runtimeState()).isEqualTo(WalRunRuntime.State.ADMITTING);

        KafkaSpeculativeCommitV1 laneZeroCommit = commitForRoot(fixture.root, 0);
        KafkaSpeculativeCommitV1 laneOneCommit = commitForRoot(fixture.root, 1);
        var laneZeroReservation = fixture.tracker.reserveBeforePosition();
        var laneZeroTicket = fixture.tracker.assignPosition(laneZeroReservation, laneZeroCommit);
        var laneOneReservation = fixture.tracker.reserveBeforePosition();
        var laneOneTicket = fixture.tracker.assignPosition(laneOneReservation, laneOneCommit);
        GroupEncodingPlanV1 laneZeroPlan = plan(fixture, 0, List.of(laneZeroTicket), List.of(laneZeroCommit));
        GroupEncodingPlanV1 laneOnePlan = plan(fixture, 1, List.of(laneOneTicket), List.of(laneOneCommit));
        fixture.transport.unknownNextCreate = true;
        fixture.transport.failNextList = true;

        assertThatThrownBy(() -> fixture.pipeline.writeResolveAndInstall(
                        laneZeroPlan, laneZeroTicket, laneZeroCommit, nativeState(laneZeroCommit), 10))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Provider pipeline failed");
        assertThat(fixture.session.state()).isEqualTo(WalRunObjectSession.State.OPEN);
        assertThat(fixture.session.runtimeState()).isEqualTo(WalRunRuntime.State.ADMITTING);
        assertThat(fixture.transport.objects).hasSize(2);

        fixture.pipeline.writeResolveAndInstall(
                laneOnePlan, laneOneTicket, laneOneCommit, nativeState(laneOneCommit), 11);
        assertThat(fixture.physical.resolvedThrough(1)).isZero();
        assertThat(fixture.physical.resolvedThrough(0)).isEqualTo(-1);

        fixture.pipeline.writeResolveAndInstall(
                laneZeroPlan, laneZeroTicket, laneZeroCommit, nativeState(laneZeroCommit), 12);
        assertThat(fixture.physical.resolvedThrough(0)).isZero();
        assertThat(fixture.transport.objects).hasSize(3);
        assertThat(fixture.transport.puts).isEqualTo(3);
        fixture.session.close();
    }

    @Test
    void sharedPhysicalObjectUsesOneFullGetAndIsolatesRealMemberFailureFromSibling() {
        PipelineFixture fixture = pipelineFixture();
        KafkaSpeculativeCommitV1 firstCommit = commitForRoot(fixture.root, 0);
        KafkaSpeculativeCommitV1 secondCommit = commitForRoot(fixture.root, 1);
        var firstReservation = fixture.tracker.reserveBeforePosition();
        var firstTicket = fixture.tracker.assignPosition(firstReservation, firstCommit);
        var secondReservation = fixture.tracker.reserveBeforePosition();
        var secondTicket = fixture.tracker.assignPosition(secondReservation, secondCommit);
        GroupEncodingPlanV1 sharedPlan =
                plan(fixture, 2, List.of(firstTicket, secondTicket), List.of(firstCommit, secondCommit));
        // Plan admission and sealed-body self-verification consume four calls; reject the first publication member.
        fixture.nativeVerifier.rejectCall = 5;

        var result = fixture.pipeline.writeResolveAndInstallShared(
                sharedPlan,
                List.of(
                        new KafkaNwg1ObjectPipelineV1.SharedMember(firstTicket, firstCommit, nativeState(firstCommit)),
                        new KafkaNwg1ObjectPipelineV1.SharedMember(
                                secondTicket, secondCommit, nativeState(secondCommit))),
                20);

        assertThat(result.isolatedFailures())
                .extracting(KafkaNwg1ObjectPipelineV1.IsolatedMemberFailure::ticket)
                .containsExactly(firstTicket);
        assertThat(result.verifiedMembers())
                .extracting(KafkaNwg1ObjectPipelineV1.VerifiedMember::ticket)
                .containsExactly(secondTicket);
        assertThat(fixture.physical.resolvedThrough(2)).isZero();
        assertThat(fixture.publisher.queueDepth()).isOne();
        fixture.pipeline.flushCheckpointForSeal();
        assertThat(fixture.publisher.head().coveredThrough().toArray()).containsExactly(-1, -1, 0);
        var checkpointPage = WalRunControlCodec.decodeCheckpointPage(
                fixture.metadata
                        .get(fixture.publisher.head().pageKey().orElseThrow())
                        .orElseThrow(),
                fixture.root.providerConfiguration());
        assertThat(checkpointPage.extents()).singleElement().satisfies(row -> {
            assertThat(row.providerProof().mode()).isEqualTo(ProviderProofMode.NONE);
            assertThat(row.providerProof().canonicalVersionToken().isEmpty()).isTrue();
        });
        assertThat(fixture.transport.fullGets).isOne();
        assertThat(fixture.transport.rangeGets).isZero();
        assertThat(fixture.kmsTransport.unwrapCalls).isOne();
        fixture.session.close();
    }

    @Test
    void checkpointIoFailureRetainsDebtButCurrentVerifiedMemberReachesM2Tracker() {
        ExactMetadata metadata = new ExactMetadata();
        PipelineFixture fixture =
                pipelineFixture(root(new WalCheckpointPolicy(0, 1, 1024 * 1024, 5_000, 16, 8192)), metadata);
        KafkaSpeculativeCommitV1 firstCommit = commitForRoot(fixture.root, 0);
        var firstReservation = fixture.tracker.reserveBeforePosition();
        var firstTicket = fixture.tracker.assignPosition(firstReservation, firstCommit);
        GroupEncodingPlanV1 firstPlan = plan(fixture, 0, List.of(firstTicket), List.of(firstCommit));
        metadata.failNextCas = true;

        fixture.pipeline.writeResolveAndInstall(firstPlan, firstTicket, firstCommit, nativeState(firstCommit), 30);

        assertThat(fixture.tracker.readyAt(0)).isPresent();
        assertThat(fixture.physical.resolvedThrough(0)).isZero();
        assertThat(fixture.publisher.queueDepth()).isOne();
        assertThat(fixture.transport.puts).isOne();

        KafkaSpeculativeCommitV1 secondCommit = commitForRoot(fixture.root, 1);
        var secondReservation = fixture.tracker.reserveBeforePosition();
        var secondTicket = fixture.tracker.assignPosition(secondReservation, secondCommit);
        GroupEncodingPlanV1 secondPlan = plan(fixture, 1, List.of(secondTicket), List.of(secondCommit));
        fixture.pipeline.writeResolveAndInstall(secondPlan, secondTicket, secondCommit, nativeState(secondCommit), 31);

        assertThat(fixture.physical.resolvedThrough(1)).isZero();
        assertThat(fixture.publisher.queueDepth()).isZero();
        assertThat(fixture.transport.puts).isEqualTo(2);
        fixture.session.close();
    }

    @Test
    void publicationReadFailureRetriesFromProviderExactWithoutSecondPut() {
        PipelineFixture fixture = pipelineFixture();
        KafkaSpeculativeCommitV1 commit = commitForRoot(fixture.root, 0);
        var reservation = fixture.tracker.reserveBeforePosition();
        var ticket = fixture.tracker.assignPosition(reservation, commit);
        GroupEncodingPlanV1 plan = plan(fixture, 0, List.of(ticket), List.of(commit));
        fixture.transport.failNextFullGet = true;

        assertThatThrownBy(() -> fixture.pipeline.writeResolveAndInstall(plan, ticket, commit, nativeState(commit), 35))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Provider pipeline failed");
        assertThat(fixture.transport.puts).isOne();

        fixture.pipeline.writeResolveAndInstall(plan, ticket, commit, nativeState(commit), 36);
        assertThat(fixture.transport.puts).isOne();
        assertThat(fixture.physical.resolvedThrough(0)).isZero();
        fixture.session.close();
    }

    @Test
    void unresolvedProviderCandidateForbidsSealAndCreatesNoSealMetadata() {
        PipelineFixture fixture = pipelineFixture();
        KafkaSpeculativeCommitV1 commit = commitForRoot(fixture.root, 0);
        var reservation = fixture.tracker.reserveBeforePosition();
        var ticket = fixture.tracker.assignPosition(reservation, commit);
        GroupEncodingPlanV1 plan = plan(fixture, 0, List.of(ticket), List.of(commit));
        fixture.transport.unknownNextCreate = true;
        fixture.transport.failNextList = true;
        assertThatThrownBy(() -> fixture.pipeline.writeResolveAndInstall(plan, ticket, commit, nativeState(commit), 40))
                .isInstanceOf(IllegalStateException.class);

        fixture.session.stopAdmission(WalRunRuntime.StopReason.OWNER_REQUEST);
        assertThatThrownBy(fixture.session::sealRuntime)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("unresolved");
        assertThat(fixture.metadata.get(WalRunControlKeys.sealKey(7, 1))).isEmpty();

        fixture.pipeline.writeResolveAndInstall(plan, ticket, commit, nativeState(commit), 41);
        fixture.session.sealRuntime();
        assertThatThrownBy(() -> fixture.pipeline.publishPhysicalSeal(new WalRunLifecycleManager(fixture.metadata)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("M2 completion tickets");
        assertThat(fixture.metadata.get(WalRunControlKeys.sealKey(7, 1))).isEmpty();
        fixture.session.close();
    }

    @Test
    void emptyTerminalClosureUsesRootBoundPublisherAndObjectSession() {
        PipelineFixture fixture = pipelineFixture();
        fixture.session.stopAdmission(WalRunRuntime.StopReason.OWNER_REQUEST);
        fixture.session.sealRuntime();

        var proof = fixture.pipeline.publishPhysicalSeal(new WalRunLifecycleManager(fixture.metadata));

        assertThat(fixture.metadata.get(WalRunControlKeys.sealKey(7, 1)))
                .map(Sha256Digest::hash)
                .contains(proof.sealSha256());
        assertThat(proof.terminalSequence()).isEqualTo(fixture.publisher.head().coveredThrough());
        assertThat(proof.aggregateExtentCount()).isZero();
        assertThat(proof.aggregateCanonicalBodyBytes()).isZero();
        fixture.session.close();
    }

    @Test
    void authenticatedPhysicalSuffixMergesInterleavedLanesAndCleansExactTempSpools() throws IOException {
        PipelineFixture fixture = pipelineFixture();
        KafkaSpeculativeCommitV1 first = commitForRoot(fixture.root, 0);
        KafkaSpeculativeCommitV1 second = commitForRoot(fixture.root, 1);
        KafkaSpeculativeCommitV1 third = commitForRoot(fixture.root, 2);
        var firstTicket = fixture.tracker.assignPosition(fixture.tracker.reserveBeforePosition(), first);
        var secondTicket = fixture.tracker.assignPosition(fixture.tracker.reserveBeforePosition(), second);
        var thirdTicket = fixture.tracker.assignPosition(fixture.tracker.reserveBeforePosition(), third);
        fixture.pipeline.writeResolveAndInstall(
                plan(fixture, 0, List.of(firstTicket), List.of(first)), firstTicket, first, nativeState(first), 45);
        fixture.pipeline.writeResolveAndInstall(
                plan(fixture, 1, List.of(secondTicket), List.of(second)),
                secondTicket,
                second,
                nativeState(second),
                46);
        fixture.pipeline.writeResolveAndInstall(
                plan(fixture, 0, List.of(thirdTicket), List.of(third)), thirdTicket, third, nativeState(third), 47);
        fixture.pipeline.flushCheckpointForSeal();
        WalRunRuntime.RecoveredState recoveredState = fixture.session.runtimeRecoveryState();
        fixture.session.close();
        Nbke2RunBindingV1 expectedRunBinding = runBindingForRoot(fixture.root);
        List<java.nio.file.Path> tempFiles = new ArrayList<>();
        Path spoolBase = tempDirectory.resolve("interleaved-secure-spool");
        Path orphanRootDirectory = spoolBase
                .resolve(fixture.root.providerScopeId().digest().toHex())
                .resolve(fixture.rootReference.rootSha256().toHex());
        Files.createDirectories(orphanRootDirectory);
        Files.write(orphanRootDirectory.resolve("lane-0.spool"), new byte[] {1, 2, 3});
        var replay = KafkaNwg1SuffixReplayV1.ownerOpenHandler(
                fixture.rootReference,
                fixture.root,
                fixture.verificationContext,
                batch -> KafkaProtocolBatchDeltaV1.nonIdempotent(batch.endOffsetExclusive() - batch.baseOffset()),
                expectedRunBinding,
                fenceForRunBinding(expectedRunBinding, 11),
                0,
                new KafkaNwg1SuffixReplayV1.ReplayTempFileHook() {
                    @Override
                    public void beforeMerge(List<Path> ignored) {}

                    @Override
                    public void filesCreated(List<Path> paths) throws IOException {
                        tempFiles.addAll(paths);
                        for (Path path : paths) {
                            assertThat(Files.getPosixFilePermissions(path))
                                    .isEqualTo(PosixFilePermissions.fromString("rw-------"));
                            assertThat(Files.size(path)).isZero();
                        }
                        Path rootDirectory = paths.get(0).getParent();
                        assertThat(Files.getPosixFilePermissions(rootDirectory))
                                .isEqualTo(PosixFilePermissions.fromString("rwx------"));
                        assertThat(Files.getPosixFilePermissions(rootDirectory.getParent()))
                                .isEqualTo(PosixFilePermissions.fromString("rwx------"));
                        assertThat(Files.getPosixFilePermissions(
                                        rootDirectory.getParent().getParent()))
                                .isEqualTo(PosixFilePermissions.fromString("rwx------"));
                    }
                },
                spoolBase);
        WalRunObjectSession recoveredSession = restoreForReplay(
                fixture.root,
                fixture.rootReference,
                fixture.metadata,
                fixture.transport,
                recoveredState,
                fixture.verificationContext,
                replay);

        var result = replay.ownerOpenResult();

        assertThat(result.state().vector().runBinding()).isEqualTo(expectedRunBinding);
        assertThat(result.state().vector().recoveryCoveredThrough()).isEqualTo(3);
        assertThat(result.recoveredTail().activeTail().startOffset()).isZero();
        assertThat(result.recoveredTail().activeTail().endOffsetExclusive()).isEqualTo(3);
        assertThat(result.recoveredTail().activeTail().locators())
                .extracting(locator -> List.of(
                        locator.startOffset(),
                        locator.endOffsetExclusive(),
                        (long) locator.extent().laneId(),
                        locator.extent().laneSequence()))
                .containsExactly(List.of(0L, 1L, 0L, 0L), List.of(1L, 2L, 1L, 0L), List.of(2L, 3L, 0L, 1L));
        var budget = recoveredSession.recoverySnapshot();
        assertThat(budget.workingMemoryBytes()).isZero();
        assertThat(budget.currentConcurrency()).isZero();
        assertThat(budget.decodedContexts()).isEqualTo(3);
        assertThat(budget.decodedFrames()).isEqualTo(3);
        assertThat(budget.decodedCommitSets()).isEqualTo(3);
        assertThat(tempFiles).hasSize(3).allSatisfy(path -> assertThat(path).doesNotExist());
        recoveredSession.close();
    }

    @Test
    void sameLaneNonMonotonicKafkaOffsetsFailAndCleanTempSpools() {
        PipelineFixture fixture = pipelineFixture();
        KafkaSpeculativeCommitV1 later = commitForRoot(fixture.root, 1);
        KafkaSpeculativeCommitV1 earlier = commitForRoot(fixture.root, 0);
        var laterTicket = fixture.tracker.assignPosition(fixture.tracker.reserveBeforePosition(), later);
        var earlierTicket = fixture.tracker.assignPosition(fixture.tracker.reserveBeforePosition(), earlier);
        fixture.pipeline.writeResolveAndInstall(
                plan(fixture, 0, List.of(laterTicket), List.of(later)), laterTicket, later, nativeState(later), 48);
        fixture.pipeline.writeResolveAndInstall(
                plan(fixture, 0, List.of(earlierTicket), List.of(earlier)),
                earlierTicket,
                earlier,
                nativeState(earlier),
                49);
        fixture.pipeline.flushCheckpointForSeal();
        WalRunRuntime.RecoveredState recoveredState = fixture.session.runtimeRecoveryState();
        fixture.session.close();
        WalRunObjectSession recoveredSession = restoreForReplay(
                fixture.root,
                fixture.rootReference,
                fixture.metadata,
                fixture.transport,
                recoveredState,
                fixture.verificationContext);
        List<java.nio.file.Path> tempFiles = new ArrayList<>();
        var replay = replay(fixture, recoveredSession, new KafkaNwg1SuffixReplayV1.ReplayTempFileHook() {
            @Override
            public void beforeMerge(List<java.nio.file.Path> ignored) {}

            @Override
            public void filesCreated(List<java.nio.file.Path> paths) {
                tempFiles.addAll(paths);
            }
        });

        assertThatThrownBy(replay::replay)
                .isInstanceOf(KafkaObjectCheckpointException.class)
                .hasMessageContaining("not strictly monotonic");
        assertThat(recoveredSession.recoverySnapshot().workingMemoryBytes()).isZero();
        assertThat(recoveredSession.recoverySnapshot().currentConcurrency()).isZero();
        assertThat(tempFiles).hasSize(3).allSatisfy(path -> assertThat(path).doesNotExist());
        recoveredSession.close();
    }

    @Test
    void exactRecoveryDiskCapRejectsOverflowAndOneByteOverbound() {
        WalRunRootRecord root = root();
        long exactCommitSets = Math.multiplyExact(
                root.bounds().maxExtentCount(), (long) root.nwg1AdmissionCaps().maxAppendUnits());
        long expectedRootCap =
                Math.addExact(root.bounds().maxCanonicalBodyBytes(), Math.multiplyExact(exactCommitSets, 138L));
        assertThat(KafkaNwg1SuffixReplayV1.recoveryDiskCap(root)).isEqualTo(expectedRootCap);
        assertThat(KafkaNwg1SuffixReplayV1.recoveryCellDiskCap(root, expectedRootCap))
                .isEqualTo(Math.multiplyExact(
                        expectedRootCap, root.recoveryEnvelope().maxLiveRoots()));
        assertThat(KafkaNwg1SuffixReplayV1.requireDiskCapacity(10, 20, 30)).isEqualTo(30);
        assertThatThrownBy(() -> KafkaNwg1SuffixReplayV1.requireDiskCapacity(10, 21, 30))
                .isInstanceOf(KafkaObjectCheckpointException.class)
                .hasMessageContaining("Root-derived cap");
        assertThatThrownBy(() -> KafkaNwg1SuffixReplayV1.requireDiskCapacity(Long.MAX_VALUE - 1, 2, Long.MAX_VALUE))
                .isInstanceOf(KafkaObjectCheckpointException.class)
                .hasMessageContaining("overflow");
    }

    @Test
    void nativeDurableOwnerAuthorityRejectsRegressionEscapeSubstitutionAndReleasesFailure() throws IOException {
        PipelineFixture fixture = pipelineFixture();
        Nbke2RunBindingV1 binding = runBindingForRoot(fixture.root);
        KafkaPartitionFenceV1 exactFence = new KafkaPartitionFenceV1(
                binding.bindingId(),
                binding.topicIncarnation(),
                binding.partitionId(),
                12,
                binding.storageEpochId(),
                binding.creatorOwnerEpoch() + 1,
                binding.kafkaLeaderEpoch() + 1);
        AtomicReference<KafkaNativePartitionOwnerAuthorityV1.SynchronousOwnerCallback> escaped =
                new AtomicReference<>();
        KafkaNativePartitionOwnerAuthorityV1 exactNative = (fence, callback) -> {
            assertThat(fence).isEqualTo(exactFence);
            escaped.set(callback);
            return callback.execute();
        };
        var exactExecutor = KafkaNativePartitionOwnerAuthorityV1.protocolOwnerFenceExecutor(
                exactNative,
                exactFence,
                binding,
                fixture.root.protocolCellIdentity(),
                fixture.rootReference,
                fixture.verificationContext);

        assertThat(exactExecutor.withDurableOwnerFence(
                        fixture.root.protocolCellIdentity(),
                        fixture.rootReference,
                        fixture.rootReference.rootSha256(),
                        exactContext -> {
                            assertThat(exactContext).isSameAs(fixture.verificationContext);
                            return fixture.session;
                        }))
                .isSameAs(fixture.session);
        assertThatThrownBy(() -> escaped.get().execute())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("escaped");

        AtomicInteger oldCallbackCalls = new AtomicInteger();
        KafkaNativePartitionOwnerAuthorityV1 newerNative = (fence, callback) -> {
            if (fence.ownerEpoch() != binding.creatorOwnerEpoch() + 2
                    || fence.kafkaLeaderEpoch() != binding.kafkaLeaderEpoch() + 2) {
                throw new IllegalStateException("stale Kafka native owner epoch");
            }
            oldCallbackCalls.incrementAndGet();
            return callback.execute();
        };
        var staleExecutor = KafkaNativePartitionOwnerAuthorityV1.protocolOwnerFenceExecutor(
                newerNative,
                exactFence,
                binding,
                fixture.root.protocolCellIdentity(),
                fixture.rootReference,
                fixture.verificationContext);
        assertThatThrownBy(() -> staleExecutor.withDurableOwnerFence(
                        fixture.root.protocolCellIdentity(),
                        fixture.rootReference,
                        fixture.rootReference.rootSha256(),
                        ignoredContext -> fixture.session))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("stale Kafka native owner");
        assertThat(oldCallbackCalls).hasValue(0);

        KafkaPartitionFenceV1 regressedOwner = new KafkaPartitionFenceV1(
                binding.bindingId(),
                binding.topicIncarnation(),
                binding.partitionId(),
                12,
                binding.storageEpochId(),
                binding.creatorOwnerEpoch() - 1,
                binding.kafkaLeaderEpoch());
        assertThatThrownBy(() -> KafkaNativePartitionOwnerAuthorityV1.protocolOwnerFenceExecutor(
                        exactNative,
                        regressedOwner,
                        binding,
                        fixture.root.protocolCellIdentity(),
                        fixture.rootReference,
                        fixture.verificationContext))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("NBKE2 run binding");
        KafkaPartitionFenceV1 regressedLeader = new KafkaPartitionFenceV1(
                binding.bindingId(),
                binding.topicIncarnation(),
                binding.partitionId(),
                12,
                binding.storageEpochId(),
                binding.creatorOwnerEpoch(),
                binding.kafkaLeaderEpoch() - 1);
        assertThatThrownBy(() -> KafkaNativePartitionOwnerAuthorityV1.protocolOwnerFenceExecutor(
                        exactNative,
                        regressedLeader,
                        binding,
                        fixture.root.protocolCellIdentity(),
                        fixture.rootReference,
                        fixture.verificationContext))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("NBKE2 run binding");

        AtomicInteger heldAuthority = new AtomicInteger();
        KafkaNativePartitionOwnerAuthorityV1 releasingNative = (fence, callback) -> {
            heldAuthority.incrementAndGet();
            try {
                return callback.execute();
            } finally {
                heldAuthority.decrementAndGet();
            }
        };
        var releasingExecutor = KafkaNativePartitionOwnerAuthorityV1.protocolOwnerFenceExecutor(
                releasingNative,
                exactFence,
                binding,
                fixture.root.protocolCellIdentity(),
                fixture.rootReference,
                fixture.verificationContext);
        assertThatThrownBy(() -> releasingExecutor.withDurableOwnerFence(
                        fixture.root.protocolCellIdentity(),
                        fixture.rootReference,
                        fixture.rootReference.rootSha256(),
                        ignoredContext -> {
                            throw new KafkaObjectCheckpointException("synthetic owner-open recovery failure");
                        }))
                .isInstanceOf(KafkaObjectCheckpointException.class)
                .hasMessageContaining("synthetic owner-open");
        assertThat(heldAuthority).hasValue(0);

        KafkaNativePartitionOwnerAuthorityV1 substitutingNative = (fence, callback) -> fixture.session;
        var substitutingExecutor = KafkaNativePartitionOwnerAuthorityV1.protocolOwnerFenceExecutor(
                substitutingNative,
                exactFence,
                binding,
                fixture.root.protocolCellIdentity(),
                fixture.rootReference,
                fixture.verificationContext);
        assertThatThrownBy(() -> substitutingExecutor.withDurableOwnerFence(
                        fixture.root.protocolCellIdentity(),
                        fixture.rootReference,
                        fixture.rootReference.rootSha256(),
                        ignoredContext -> fixture.session))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("omitted or substituted");
        fixture.session.close();
    }

    @Test
    void truncatedOrTamperedRecoveryLaneSpoolFailsAndAlwaysCleansFiles() {
        assertTempSpoolCorruptionRejected(
                paths -> {
                    java.nio.file.Path used = paths.stream()
                            .filter(path -> {
                                try {
                                    return java.nio.file.Files.size(path) > 0;
                                } catch (IOException failure) {
                                    throw new IllegalStateException(failure);
                                }
                            })
                            .findFirst()
                            .orElseThrow();
                    try (var channel =
                            java.nio.channels.FileChannel.open(used, java.nio.file.StandardOpenOption.WRITE)) {
                        channel.truncate(java.nio.file.Files.size(used) - 1);
                    }
                },
                "truncated");
        assertTempSpoolCorruptionRejected(
                paths -> {
                    java.nio.file.Path used = paths.stream()
                            .filter(path -> {
                                try {
                                    return java.nio.file.Files.size(path) > 0;
                                } catch (IOException failure) {
                                    throw new IllegalStateException(failure);
                                }
                            })
                            .findFirst()
                            .orElseThrow();
                    try (var channel = java.nio.channels.FileChannel.open(
                            used, java.nio.file.StandardOpenOption.READ, java.nio.file.StandardOpenOption.WRITE)) {
                        long tamperPosition = java.nio.file.Files.size(used) - Sha256Digest.LENGTH - 1;
                        ByteBuffer value = ByteBuffer.allocate(1);
                        channel.read(value, tamperPosition);
                        value.flip();
                        byte changed = (byte) (value.get() ^ 0x01);
                        channel.write(ByteBuffer.wrap(new byte[] {changed}), tamperPosition);
                    }
                },
                "SHA-256");
    }

    @Test
    void underboundStreamingCheckpointBudgetFailsBeforeAnyPageMetadataRead() {
        PipelineFixture fixture = pipelineFixture();
        KafkaSpeculativeCommitV1 commit = commitForRoot(fixture.root, 0);
        var reservation = fixture.tracker.reserveBeforePosition();
        var ticket = fixture.tracker.assignPosition(reservation, commit);
        fixture.pipeline.writeResolveAndInstall(
                plan(fixture, 0, List.of(ticket), List.of(commit)), ticket, commit, nativeState(commit), 50);
        fixture.pipeline.flushCheckpointForSeal();
        String pageKey = fixture.publisher.head().pageKey().orElseThrow();
        String headKey = WalRunControlKeys.checkpointHeadKey(7, 1);
        int pageGetsBefore = fixture.metadata.getCount(pageKey);
        int headGetsBefore = fixture.metadata.getCount(headKey);
        WalRunRuntime.RecoveredState recoveredState = fixture.session.runtimeRecoveryState();
        fixture.session.close();
        WalRunObjectSession recoveredSession = restoreForReplay(
                fixture.root,
                fixture.rootReference,
                fixture.metadata,
                fixture.transport,
                recoveredState,
                fixture.verificationContext);
        long usedCanonicalBytes = recoveredSession.recoverySnapshot().canonicalBodyBytes();
        long leaveOneByteUnderPagePrecharge =
                1024L * 1024 + fixture.root.checkpointPolicy().maxCanonicalPageBytes() - 1;
        recoveredSession.chargeRecoveryControlMetadata(
                fixture.root.recoveryEnvelope().maxCanonicalBodyBytes()
                        - usedCanonicalBytes
                        - leaveOneByteUnderPagePrecharge);
        var replay = new KafkaNwg1SuffixReplayV1(
                fixture.rootReference,
                fixture.metadata,
                recoveredSession,
                fixture.verificationContext,
                ignored -> {
                    throw new AssertionError("underbound page budget must fail before native replay");
                },
                ObjectKafkaTestFixtures.runBinding(),
                fenceForRunBinding(ObjectKafkaTestFixtures.runBinding(), 11),
                0);

        assertThatThrownBy(replay::replay)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("recovery");
        assertThat(fixture.metadata.getCount(headKey)).isEqualTo(headGetsBefore + 2);
        assertThat(fixture.metadata.getCount(pageKey)).isEqualTo(pageGetsBefore + 1);
        recoveredSession.close();
    }

    @Test
    void freshSessionCannotReplayAndPerformsNoMetadataOrProviderIo() {
        PipelineFixture fixture = pipelineFixture();
        int metadataGetsBefore = fixture.metadata.totalGets();

        assertThatThrownBy(() -> new KafkaNwg1SuffixReplayV1(
                        fixture.rootReference,
                        fixture.metadata,
                        fixture.session,
                        fixture.verificationContext,
                        ignored -> {
                            throw new AssertionError("fresh session must fail before native replay");
                        },
                        ObjectKafkaTestFixtures.runBinding(),
                        fenceForRunBinding(ObjectKafkaTestFixtures.runBinding(), 11),
                        0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("RecoveredLineage");
        assertThat(fixture.metadata.totalGets()).isEqualTo(metadataGetsBefore);
        assertThat(fixture.transport.puts).isZero();
        assertThat(fixture.transport.fullGets).isZero();
        assertThat(fixture.transport.rangeGets).isZero();
        assertThat(fixture.transport.lists).isZero();
        fixture.session.close();
    }

    @Test
    void authenticatedEmptyPhysicalChainProducesBoundedRecoveryStateAndTail() {
        FakeTransport transport = new FakeTransport();
        ExactMetadata metadata = new ExactMetadata();
        WalRunRootRecord rootRecord = root();
        var lifecycle = new WalRunLifecycleManager(metadata);
        var rootReference = lifecycle.createRoot(
                WalRunControlKeys.rootKey(rootRecord.shardId(), rootRecord.shardRunEpoch()), rootRecord);
        String physicalHeadKey = WalRunControlKeys.checkpointHeadKey(7, 1);
        CanonicalBytes physicalHeadBytes = WalRunControlCodec.encodeCheckpointHead(
                WalCheckpointHeadV1.empty(rootReference.rootSha256(), rootRecord.shardRunEpoch(), 1));
        assertThat(metadata.putIfAbsent(physicalHeadKey, physicalHeadBytes)).isEqualTo(ControlMutationOutcome.APPLIED);
        var verificationContext = new Nwg1VerificationContextV1(
                rootRecord.protocolCellIdentity(),
                rootRecord.providerScopeId().digest().bytes().toByteArray(),
                rootReference.rootSha256().bytes().toByteArray(),
                Nwg1EnvelopeV1.decode(rootRecord.wrappedRunKey().framedBytes().toByteArray()),
                (bindingId, ownerFenceKind, ownerFenceVersion) ->
                        ObjectKafkaTestFixtures.digest(30).bytes().toByteArray(),
                (bytes, partitionId, leaderEpoch, start, end) ->
                        new Nwg1VerificationContextV1.NativeCoverage(start, end),
                0,
                0);
        var objectSession = restoreForReplay(
                rootRecord,
                rootReference,
                metadata,
                transport,
                new WalRunRuntime(rootRecord).recoveryState(),
                verificationContext);
        var replay = new KafkaNwg1SuffixReplayV1(
                rootReference,
                metadata,
                objectSession,
                verificationContext,
                ignored -> {
                    throw new AssertionError("empty physical chain cannot invoke the native batch adapter");
                },
                ObjectKafkaTestFixtures.runBinding(),
                fenceForRunBinding(ObjectKafkaTestFixtures.runBinding(), 11),
                0);

        var result = replay.replay();
        assertThat(result.state().vector().recoveryCoveredThrough()).isZero();
        assertThat(result.recoveredTail().activeTail().startOffset()).isZero();
        assertThat(result.recoveredTail().activeTail().locators()).isEmpty();
        assertThat(result.recoveredTail().walRunRootSha()).isEqualTo(rootReference.rootSha256());
        var budget = objectSession.recoverySnapshot();
        assertThat(budget.liveRoots()).isOne();
        assertThat(budget.headRequests()).isZero();
        assertThat(budget.fullGetRequests()).isZero();
        assertThat(budget.decodedCommitSets()).isZero();
        objectSession.close();
    }

    private static WalRunObjectSession restoreForReplay(
            WalRunRootRecord root,
            com.nereusstream.storage.object.control.WalRunReference rootReference,
            ExactMetadata metadata,
            FakeTransport transport,
            WalRunRuntime.RecoveredState recoveredState,
            Nwg1VerificationContextV1 verificationContext) {
        return restoreForReplay(
                root,
                rootReference,
                metadata,
                transport,
                recoveredState,
                verificationContext,
                new OwnerOpenRecoveryCoordinator.ProtocolRecoveryHandler() {
                    @Override
                    public void stage(
                            ProviderResolvedExtentRowV1 ignoredRow,
                            Nwg1ObjectReaderV1.AuthenticatedPrefix prefix,
                            com.nereusstream.storage.object.recovery.BoundedObjectTailRecovery.SelectedAppendUnitReader
                                    reader)
                            throws IOException {
                        for (Nwg1DirectoryV1.AppendUnit unit :
                                prefix.directory().appendUnits()) {
                            reader.read(unit.firstFrameOrdinal(), (ignoredFrame, ignoredPayload) -> {});
                        }
                    }

                    @Override
                    public void install(WalRunObjectSession session) throws IOException {
                        session.consumeRecoveredPhysicalRows(ignoredRow -> {});
                    }

                    @Override
                    public void abort() {}
                });
    }

    private static WalRunObjectSession restoreForReplay(
            WalRunRootRecord root,
            com.nereusstream.storage.object.control.WalRunReference rootReference,
            ExactMetadata metadata,
            FakeTransport transport,
            WalRunRuntime.RecoveredState recoveredState,
            Nwg1VerificationContextV1 verificationContext,
            OwnerOpenRecoveryCoordinator.ProtocolRecoveryHandler recoveryHandler) {
        String pointerKey = WalRunControlKeys.pointerKey(root.shardId());
        ControlMutationOutcome pointerOutcome = metadata.putIfAbsent(
                pointerKey, WalRunControlCodec.encodePointer(new CurrentWalRunPointer(rootReference)));
        if (pointerOutcome != ControlMutationOutcome.APPLIED) {
            throw new IllegalStateException("test current WalRun pointer differs");
        }
        var recoveredLineage = new WalRunLineageRecovery(metadata)
                .recover(pointerKey, root.protocolCellIdentity(), root.providerScopeId(), Optional.empty(), () -> 0);
        var provider = new C1ObjectProviderSession(
                transport,
                root.providerScopeId(),
                root.providerConfiguration().exclusiveNamespacePrefix(),
                root.providerConfiguration().maxObjectBodyBytes(),
                root.nwg1AdmissionCaps().maxDirectoryPrefixBytes());
        var kms = new KmsCellSession(
                new FakeKmsTransport(), root.providerScopeId(), "kms/cell-a", 2, new SecureRandom(new byte[] {1, 2, 3
                }));
        WalRunObjectSession restored;
        try {
            restored = OwnerOpenRecoveryCoordinator.recoverUnderDurableFence(
                    (exactCell, exactReference, exactSha, callback) -> callback.recover(verificationContext),
                    root.protocolCellIdentity(),
                    rootReference,
                    root,
                    metadata,
                    provider,
                    kms,
                    recoveredLineage,
                    recoveryHandler);
        } catch (IOException failure) {
            throw new IllegalStateException("test owner-open recovery failed", failure);
        }
        WalRunRuntime.RecoveredState actual = restored.runtimeRecoveryState();
        if (actual.resolvedExtentCount() != recoveredState.resolvedExtentCount()
                || actual.resolvedCanonicalBodyBytes() != recoveredState.resolvedCanonicalBodyBytes()) {
            restored.close();
            throw new IllegalStateException("test owner-open recovery state differs from the sealed runtime");
        }
        return restored;
    }

    private static KafkaNwg1SuffixReplayV1 replay(
            PipelineFixture fixture,
            WalRunObjectSession recoveredSession,
            KafkaNwg1SuffixReplayV1.ReplayTempFileHook hook) {
        return new KafkaNwg1SuffixReplayV1(
                fixture.rootReference,
                fixture.metadata,
                recoveredSession,
                fixture.verificationContext,
                batch -> KafkaProtocolBatchDeltaV1.nonIdempotent(batch.endOffsetExclusive() - batch.baseOffset()),
                runBindingForRoot(fixture.root),
                fenceForRunBinding(runBindingForRoot(fixture.root), 11),
                0,
                hook);
    }

    private static void assertTempSpoolCorruptionRejected(
            KafkaNwg1SuffixReplayV1.ReplayTempFileHook corruption, String expectedMessage) {
        PipelineFixture fixture = pipelineFixture();
        KafkaSpeculativeCommitV1 commit = commitForRoot(fixture.root, 0);
        var ticket = fixture.tracker.assignPosition(fixture.tracker.reserveBeforePosition(), commit);
        fixture.pipeline.writeResolveAndInstall(
                plan(fixture, 0, List.of(ticket), List.of(commit)), ticket, commit, nativeState(commit), 44);
        fixture.pipeline.flushCheckpointForSeal();
        WalRunRuntime.RecoveredState recoveredState = fixture.session.runtimeRecoveryState();
        fixture.session.close();
        WalRunObjectSession recoveredSession = restoreForReplay(
                fixture.root,
                fixture.rootReference,
                fixture.metadata,
                fixture.transport,
                recoveredState,
                fixture.verificationContext);
        List<java.nio.file.Path> tempFiles = new ArrayList<>();
        var replay = replay(fixture, recoveredSession, paths -> {
            tempFiles.addAll(paths);
            corruption.beforeMerge(paths);
        });

        assertThatThrownBy(replay::replay)
                .isInstanceOf(KafkaObjectCheckpointException.class)
                .hasMessageContaining(expectedMessage);
        assertThat(recoveredSession.recoverySnapshot().workingMemoryBytes()).isZero();
        assertThat(recoveredSession.recoverySnapshot().currentConcurrency()).isZero();
        assertThat(tempFiles).hasSize(3).allSatisfy(path -> assertThat(path).doesNotExist());
        recoveredSession.close();
    }

    private static PipelineFixture pipelineFixture() {
        return pipelineFixture(root(), new ExactMetadata());
    }

    private static PipelineFixture pipelineFixture(WalRunRootRecord root, ExactMetadata metadata) {
        var freshRoot = new WalRunLifecycleManager(metadata)
                .createRootAndInitializePointer(WalRunControlKeys.rootKey(root.shardId(), root.shardRunEpoch()), root);
        var rootReference = freshRoot.reference();
        FakeTransport transport = new FakeTransport();
        FakeKmsTransport kmsTransport = new FakeKmsTransport();
        var session = WalRunObjectSession.openNew(
                freshRoot.ownerAuthority().orElseThrow(),
                new C1ObjectProviderSession(
                        transport,
                        root.providerScopeId(),
                        root.providerConfiguration().exclusiveNamespacePrefix(),
                        root.providerConfiguration().maxObjectBodyBytes(),
                        root.nwg1AdmissionCaps().maxDirectoryPrefixBytes()),
                new KmsCellSession(
                        kmsTransport, root.providerScopeId(), "kms/cell-a", 2, new SecureRandom(new byte[] {1, 2, 3})),
                () -> 0);
        Nwg1EnvelopeV1 envelope =
                Nwg1EnvelopeV1.decode(root.wrappedRunKey().framedBytes().toByteArray());
        byte[] ownerWitness = ObjectKafkaTestFixtures.digest(30).bytes().toByteArray();
        NativeVerifierControl nativeVerifier = new NativeVerifierControl();
        var verificationContext = new Nwg1VerificationContextV1(
                root.protocolCellIdentity(),
                root.providerScopeId().digest().bytes().toByteArray(),
                session.rootSha256().bytes().toByteArray(),
                envelope,
                (bindingId, ownerFenceKind, ownerFenceVersion) -> ownerWitness,
                nativeVerifier,
                0,
                0);
        var physical = new KafkaObjectPhysicalFrontiersV1(session.rootSha256());
        var tracker = new KafkaObjectCompletionTrackerV1(8, 8L * 1024, 6);
        var publisher = new WalCheckpointPublisher(
                metadata,
                WalRunControlKeys.checkpointHeadKey(root.shardId(), root.shardRunEpoch()),
                WalRunControlKeys.checkpointPagePrefix(root.shardId(), root.shardRunEpoch()),
                root,
                WalCheckpointHeadV1.empty(session.rootSha256(), root.shardRunEpoch(), 1),
                session);
        publisher.initializeHead();
        var pipeline = new KafkaNwg1ObjectPipelineV1(root, session, verificationContext, physical, tracker, publisher);
        return new PipelineFixture(
                root,
                transport,
                kmsTransport,
                session,
                verificationContext,
                physical,
                tracker,
                publisher,
                pipeline,
                metadata,
                rootReference,
                nativeVerifier,
                ownerWitness);
    }

    private static KafkaSpeculativeCommitV1 commitForRoot(WalRunRootRecord root, long startOffset) {
        var incarnation = ObjectKafkaTestFixtures.runBinding().topicIncarnation();
        var binding = DeterministicTopicIdsV1.deriveBindingId(root.protocolCellIdentity(), incarnation);
        var storageEpoch = DeterministicTopicIdsV1.deriveStorageEpochId(binding, 0);
        var fence = new KafkaPartitionFenceV1(binding, incarnation, 4, 11, storageEpoch, 6, 7);
        var delta = new KafkaProtocolBatchDeltaV1(1, Optional.empty(), KafkaTransactionBatchKindV1.NONE, -1, -1);
        return KafkaSpeculativeCommitV1.assign(
                new KafkaProtocolAppendPlanV1(fence, List.of(delta)), startOffset, startOffset + 1);
    }

    private static Nbke2RunBindingV1 runBindingForRoot(WalRunRootRecord root) {
        var incarnation = ObjectKafkaTestFixtures.runBinding().topicIncarnation();
        var binding = DeterministicTopicIdsV1.deriveBindingId(root.protocolCellIdentity(), incarnation);
        var storageEpoch = DeterministicTopicIdsV1.deriveStorageEpochId(binding, 0);
        return new Nbke2RunBindingV1(
                binding,
                incarnation,
                4,
                storageEpoch,
                6,
                7,
                root.providerScopeId(),
                new StorageRunId(root.walRunSessionId()));
    }

    private static KafkaPartitionFenceV1 fenceForRunBinding(Nbke2RunBindingV1 binding, long bindingGeneration) {
        return new KafkaPartitionFenceV1(
                binding.bindingId(),
                binding.topicIncarnation(),
                binding.partitionId(),
                bindingGeneration,
                binding.storageEpochId(),
                Math.incrementExact(binding.creatorOwnerEpoch()),
                Math.incrementExact(binding.kafkaLeaderEpoch()));
    }

    private static KafkaObjectNativeStateV1 nativeState(KafkaSpeculativeCommitV1 commit) {
        return new KafkaObjectNativeStateV1(
                KafkaCommittedProducerStateV1.empty().apply(commit),
                KafkaTransactionStateV1.empty().apply(commit),
                KafkaLeaderEpochIndexV1.empty()
                        .observe(commit.expectedFence().kafkaLeaderEpoch(), commit.startOffset()),
                commit.endOffsetExclusive(),
                commit.endOffsetExclusive());
    }

    private static GroupEncodingPlanV1 plan(
            PipelineFixture fixture,
            int laneId,
            List<KafkaObjectCompletionTrackerV1.AssignedTicket> tickets,
            List<KafkaSpeculativeCommitV1> commits) {
        if (tickets.size() != commits.size() || commits.isEmpty()) {
            throw new IllegalArgumentException("test plan member inventory differs");
        }
        KafkaSpeculativeCommitV1 first = commits.get(0);
        byte[] nti1 = TopicIncarnationIdentityCodecV1.encode(
                        first.expectedFence().topicIncarnation())
                .toByteArray();
        var binding = new Nwg1DirectoryV1.BindingContext(
                first.expectedFence().bindingId().digest().bytes().toByteArray(),
                first.expectedFence().storageEpochId().digest().bytes().toByteArray(),
                Nwg1CommitmentsV1.ownerFence(1, 1, fixture.ownerWitness),
                nti1,
                1,
                1,
                1,
                1,
                1,
                1);
        ArrayList<Nwg1DirectoryV1.KafkaAppendUnit> units = new ArrayList<>();
        ArrayList<GroupEncodingPlanV1.PlannedFrame> frames = new ArrayList<>();
        long totalPayload = 0;
        for (int index = 0; index < commits.size(); index++) {
            KafkaSpeculativeCommitV1 commit = commits.get(index);
            if (!commit.expectedFence().bindingId().equals(first.expectedFence().bindingId())) {
                throw new IllegalArgumentException("test shared plan requires one exact binding");
            }
            byte[] payload =
                    assignedBatch(commit.startOffset(), commit.expectedFence().kafkaLeaderEpoch());
            totalPayload += payload.length;
            units.add(new Nwg1DirectoryV1.KafkaAppendUnit(
                    0,
                    index,
                    1,
                    commit.expectedFence().partitionId(),
                    commit.expectedFence().kafkaLeaderEpoch(),
                    commit.startOffset(),
                    commit.endOffsetExclusive(),
                    KafkaNwg1ObjectPipelineV1.commitSetId(commit),
                    KafkaNwg1ObjectPipelineV1.storageAttemptId(tickets.get(index)),
                    Sha256Digest.hash(CanonicalBytes.copyOf(payload)).bytes().toByteArray()));
            frames.add(new GroupEncodingPlanV1.PlannedFrame(
                    index, payload, payload, commit.startOffset(), commit.endOffsetExclusive(), 0, 0));
        }
        Nwg1EnvelopeV1 envelope = fixture.verificationContext.envelope();
        return new GroupEncodingPlanV1(
                1,
                fixture.root.shardId(),
                fixture.root.shardRunEpoch(),
                laneId,
                fixture.root.formatContract().packingPolicyCatalogVersion(),
                totalPayload,
                0,
                0,
                10,
                Nwg1CommitmentsV1.protocolCell(fixture.verificationContext.exactNpc1()),
                fixture.root.providerScopeId().digest().bytes().toByteArray(),
                fixture.session.rootSha256().bytes().toByteArray(),
                Nwg1CommitmentsV1.wrappedEnvelope(envelope),
                List.of(binding),
                units,
                frames);
    }

    private static byte[] assignedBatch(long baseOffset, int leaderEpoch) {
        byte[] raw = new byte[61];
        ByteBuffer buffer = ByteBuffer.wrap(raw).order(ByteOrder.BIG_ENDIAN);
        buffer.putLong(0, baseOffset);
        buffer.putInt(Long.BYTES, raw.length - 12);
        buffer.putInt(12, leaderEpoch);
        buffer.put(16, (byte) 2);
        buffer.putInt(23, 0);
        buffer.putLong(43, -1L);
        buffer.putShort(51, (short) -1);
        buffer.putInt(53, -1);
        CRC32C crc = new CRC32C();
        crc.update(raw, 21, raw.length - 21);
        buffer.putInt(17, (int) crc.getValue());
        return raw;
    }

    private record PipelineFixture(
            WalRunRootRecord root,
            FakeTransport transport,
            FakeKmsTransport kmsTransport,
            WalRunObjectSession session,
            Nwg1VerificationContextV1 verificationContext,
            KafkaObjectPhysicalFrontiersV1 physical,
            KafkaObjectCompletionTrackerV1 tracker,
            WalCheckpointPublisher publisher,
            KafkaNwg1ObjectPipelineV1 pipeline,
            ExactMetadata metadata,
            com.nereusstream.storage.object.control.WalRunReference rootReference,
            NativeVerifierControl nativeVerifier,
            byte[] ownerWitness) {
        private PipelineFixture {
            ownerWitness = ownerWitness.clone();
        }

        @Override
        public byte[] ownerWitness() {
            return ownerWitness.clone();
        }
    }

    private static final class NativeVerifierControl implements Nwg1VerificationContextV1.NativePayloadVerifier {
        private int calls;
        private int rejectCall = -1;

        @Override
        public Nwg1VerificationContextV1.NativeCoverage validateKafka(
                byte[] exactAssignedBytes,
                long partitionId,
                long kafkaLeaderEpoch,
                long expectedCoverageStart,
                long expectedCoverageEnd) {
            calls++;
            KafkaNativeAssignedRecordBatchV1 assigned = KafkaNativeAssignedRecordBatchV1.validate(
                    KafkaRawAssignedRecordBatchFactsV1.parse(CanonicalBytes.copyOf(exactAssignedBytes)));
            if (assigned.partitionLeaderEpoch() != kafkaLeaderEpoch
                    || assigned.baseOffset() != expectedCoverageStart
                    || assigned.endOffsetExclusive() != expectedCoverageEnd) {
                throw new AssertionError("test native batch differs from expected NWG1 coverage");
            }
            if (calls == rejectCall) {
                throw new Nwg1ValidationException(
                        Nwg1RejectionV1.COVERAGE_MISMATCH,
                        Nwg1ValidationStageV1.NATIVE_FRAME,
                        Nwg1IsolationScopeV1.APPEND_UNIT,
                        "synthetic member-local native coverage rejection");
            }
            return new Nwg1VerificationContextV1.NativeCoverage(expectedCoverageStart, expectedCoverageEnd);
        }
    }

    private static WalRunRootRecord root() {
        return root(new WalCheckpointPolicy(0, 16, 1024 * 1024, 5_000, 16, 8192));
    }

    private static WalRunRootRecord root(WalCheckpointPolicy checkpointPolicy) {
        var binding = ObjectKafkaTestFixtures.runBinding();
        return new WalRunRootRecord(
                7,
                1,
                binding.runId().value(),
                0,
                new KafkaProtocolCellIdentity(new DeploymentId(new Id128(1, 2)), new KafkaCellId(new Id128(3, 4))),
                binding.providerScopeId(),
                WalRunFormatContractV1.frozen(),
                new Nwg1RootAdmissionCaps(1024 * 1024, 4096, 3824, 16, 100, 100, 4080, 4096, 1024 * 1024, 1024 * 1024),
                new WalRunBounds(100, 1024 * 1024, 60_000, 4),
                checkpointPolicy,
                new ObjectProviderRootConfiguration(
                        ObjectProviderAccessProfile.C1_SINGLE_PUT_SINGLE_RANGE_STRONG_LIST,
                        "test-adapter-v1",
                        "canonical-key-v1",
                        "scope",
                        ProviderProofMode.NONE,
                        0,
                        1024 * 1024,
                        1024 * 1024,
                        4096,
                        1,
                        102,
                        ObjectKafkaTestFixtures.digest(2)),
                new RecoveryEnvelopeLimits(
                        5,
                        4,
                        10,
                        102,
                        200_000,
                        0,
                        10_100,
                        10,
                        32L * 1024 * 1024,
                        10_000,
                        10_000,
                        10_000,
                        1024 * 1024,
                        4,
                        10,
                        60_000_000_000L),
                new WrappedRunKeyEnvelope(
                        "fake-kms",
                        "aes-kw-v1",
                        "kms/cell-a",
                        "version-1",
                        ObjectKafkaTestFixtures.digest(31).bytes()),
                Optional.empty());
    }

    private static WalRunRootRecord successor(WalRunRootRecord predecessor, WalRunPredecessor lineage) {
        ObjectProviderRootConfiguration provider = predecessor.providerConfiguration();
        return new WalRunRootRecord(
                predecessor.shardId(),
                2,
                new Id128(11, 12),
                1,
                predecessor.protocolCellIdentity(),
                predecessor.providerScopeId(),
                predecessor.formatContract(),
                predecessor.nwg1AdmissionCaps(),
                predecessor.bounds(),
                predecessor.checkpointPolicy(),
                new ObjectProviderRootConfiguration(
                        provider.accessProfile(),
                        provider.adapterVersion(),
                        provider.canonicalizerVersion(),
                        "scope-successor",
                        provider.proofMode(),
                        provider.proofTokenHardCap(),
                        provider.maxObjectBodyBytes(),
                        provider.maxSinglePutBytes(),
                        provider.maxSingleRangeReadBytes(),
                        provider.maxPrefixSegmentsPerExtent(),
                        provider.maxListPageKeys(),
                        provider.capabilityReceiptSha256()),
                predecessor.recoveryEnvelope(),
                new WrappedRunKeyEnvelope(
                        "fake-kms",
                        "aes-kw-v1",
                        "kms/cell-a",
                        "version-2",
                        ObjectKafkaTestFixtures.digest(32).bytes()),
                Optional.of(lineage));
    }

    private static final class ExactMetadata implements CanonicalControlMetadataStore {
        private final Map<String, CanonicalBytes> values = new HashMap<>();
        private final Map<String, Integer> getCounts = new HashMap<>();
        private boolean unknownNextCas;
        private boolean failNextCas;

        @Override
        public synchronized Optional<CanonicalBytes> get(String key) {
            getCounts.merge(key, 1, Integer::sum);
            return Optional.ofNullable(values.get(key));
        }

        private synchronized int getCount(String key) {
            return getCounts.getOrDefault(key, 0);
        }

        private synchronized int totalGets() {
            return getCounts.values().stream().mapToInt(Integer::intValue).sum();
        }

        @Override
        public synchronized ControlMutationOutcome putIfAbsent(String key, CanonicalBytes exactValue) {
            CanonicalBytes previous = values.putIfAbsent(key, exactValue);
            return previous == null || previous.equals(exactValue)
                    ? ControlMutationOutcome.APPLIED
                    : ControlMutationOutcome.DEFINITIVE_CONFLICT;
        }

        @Override
        public synchronized ControlMutationOutcome compareAndSet(
                String key, Optional<CanonicalBytes> exactExpected, CanonicalBytes exactCandidate) {
            if (failNextCas) {
                failNextCas = false;
                throw new IllegalStateException("synthetic checkpoint CAS I/O failure");
            }
            CanonicalBytes current = values.get(key);
            boolean matches =
                    exactExpected.map(expected -> expected.equals(current)).orElse(current == null);
            if (!matches) {
                return ControlMutationOutcome.DEFINITIVE_CONFLICT;
            }
            values.put(key, exactCandidate);
            if (unknownNextCas) {
                unknownNextCas = false;
                return ControlMutationOutcome.RESPONSE_UNKNOWN;
            }
            return ControlMutationOutcome.APPLIED;
        }
    }

    private static final class FakeKmsTransport implements KmsTransport {
        private int unwrapCalls;

        @Override
        public WrappedRunKeyEnvelope wrap(String keyIdentity, byte[] plaintextRunKey) {
            return new WrappedRunKeyEnvelope(
                    "fake-kms", "aes-kw-v1", keyIdentity, "version-test", CanonicalBytes.copyOf(plaintextRunKey));
        }

        @Override
        public byte[] unwrap(WrappedRunKeyEnvelope envelope) {
            unwrapCalls++;
            return envelope.wrappedKey().toByteArray();
        }
    }

    private static final class FakeTransport implements ObjectProviderTransport {
        private final Map<String, byte[]> objects = new HashMap<>();
        private final CanonicalBytes version = CanonicalBytes.copyOf(new byte[] {1});
        private boolean unknownNextCreate;
        private boolean failNextList;
        private boolean failNextFullGet;
        private int fullGets;
        private int rangeGets;
        private int puts;
        private int lists;

        @Override
        public ObjectProviderCapabilities capabilities() {
            return new ObjectProviderCapabilities(
                    "test-c1", true, true, true, true, true, 64 * 1024 * 1024L, 4 * 1024 * 1024, 1_000);
        }

        @Override
        public synchronized ConditionalCreateResult putIfAbsent(ObjectIdentity identity, InputStream body)
                throws IOException {
            byte[] candidate = body.readAllBytes();
            puts++;
            if (candidate.length != identity.bodyLength()
                    || !Sha256Digest.hash(CanonicalBytes.copyOf(candidate)).equals(identity.bodySha256())) {
                return ConditionalCreateResult.DEFINITIVE_CONFLICT;
            }
            byte[] previous = objects.putIfAbsent(identity.key(), candidate);
            if (previous != null && !java.util.Arrays.equals(previous, candidate)) {
                return ConditionalCreateResult.DEFINITIVE_CONFLICT;
            }
            if (unknownNextCreate) {
                unknownNextCreate = false;
                return ConditionalCreateResult.RESPONSE_UNKNOWN;
            }
            return previous == null ? ConditionalCreateResult.CREATED : ConditionalCreateResult.ALREADY_EXISTS;
        }

        @Override
        public synchronized StreamingObject get(String key, Optional<CanonicalBytes> exactVersionToken)
                throws IOException {
            if (failNextFullGet) {
                failNextFullGet = false;
                throw new IOException("synthetic publication full-GET failure");
            }
            byte[] body = require(key);
            fullGets++;
            return stream(body, 0, body.length);
        }

        @Override
        public synchronized StreamingObject getRange(
                String key, long inclusiveStart, long exclusiveEnd, Optional<CanonicalBytes> exactVersionToken) {
            byte[] body = require(key);
            rangeGets++;
            int start = Math.toIntExact(inclusiveStart);
            int end = Math.toIntExact(exclusiveEnd);
            return stream(body, start, end);
        }

        @Override
        public synchronized ListPage list(String prefix, Optional<CanonicalBytes> continuationToken, int maximumKeys)
                throws IOException {
            lists++;
            if (failNextList) {
                failNextList = false;
                throw new IOException("synthetic list failure");
            }
            List<ListedObject> listed = new ArrayList<>();
            objects.forEach((key, body) -> {
                if (key.startsWith(prefix)) {
                    listed.add(new ListedObject(key, body.length, Optional.empty()));
                }
            });
            return new ListPage(listed, Optional.empty());
        }

        private byte[] require(String key) {
            byte[] body = objects.get(key);
            if (body == null) {
                throw new IllegalArgumentException("missing fake Object");
            }
            return body;
        }

        private StreamingObject stream(byte[] body, int start, int end) {
            return new StreamingObject(
                    body.length,
                    start,
                    end,
                    Optional.of(version),
                    new ByteArrayInputStream(java.util.Arrays.copyOfRange(body, start, end)));
        }
    }
}
