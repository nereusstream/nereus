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
import com.nereusstream.kafka.bookkeeper.adapter.KafkaNativeAssignedRecordBatchV1;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaNativeProtocolBatchAdapterV1;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaRawAssignedRecordBatchFactsV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaBatchDuplicateIdentityV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaCoherentCommitCoordinatorV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaProtocolAppendPlanV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaSpeculativeCommitV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.object.ObjectKafkaTestFixtures;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaNwg1ObjectPipelineV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectAuthorizationV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectCoherentProtocolSnapshotV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectCompletionTrackerV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectNativeStateV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectPhysicalFrontiersV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectPublicationBridgeV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectSourceProtectionTrackerV1;
import com.nereusstream.kafka.bookkeeper.pipeline.KafkaOffsetAssignedAppendV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.metadata.oxia.v2.objectwal.OxiaCanonicalControlMetadataStore;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.control.LaneSequenceVector;
import com.nereusstream.storage.object.control.ObjectProviderAccessProfile;
import com.nereusstream.storage.object.control.ObjectProviderRootConfiguration;
import com.nereusstream.storage.object.control.ObjectWalLeafKeyV1;
import com.nereusstream.storage.object.control.ProviderProofMode;
import com.nereusstream.storage.object.control.WalCheckpointHeadV1;
import com.nereusstream.storage.object.control.WalCheckpointPolicy;
import com.nereusstream.storage.object.control.WalCheckpointPublisher;
import com.nereusstream.storage.object.control.WalRunControlCodec;
import com.nereusstream.storage.object.control.WalRunControlKeys;
import com.nereusstream.storage.object.control.WalRunLifecycleManager;
import com.nereusstream.storage.object.control.WalRunObjectSession;
import com.nereusstream.storage.object.control.WalRunRootRecord;
import com.nereusstream.storage.object.control.WalRunRuntime;
import com.nereusstream.storage.object.kms.KmsCellSession;
import com.nereusstream.storage.object.kms.KmsTransport;
import com.nereusstream.storage.object.kms.RunKeyCacheIdentity;
import com.nereusstream.storage.object.kms.WrappedRunKeyEnvelope;
import com.nereusstream.storage.object.nwg1.GroupEncodingPlanV1;
import com.nereusstream.storage.object.nwg1.Nwg1CommitmentsV1;
import com.nereusstream.storage.object.nwg1.Nwg1DirectoryV1;
import com.nereusstream.storage.object.nwg1.Nwg1EnvelopeV1;
import com.nereusstream.storage.object.nwg1.Nwg1RootAuthorityV1;
import com.nereusstream.storage.object.nwg1.Nwg1VerificationContextV1;
import com.nereusstream.storage.object.nwg1.Nwg1VerificationPathV1;
import com.nereusstream.storage.object.provider.C1ObjectProviderSession;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.provider.ObjectProviderCapabilities;
import com.nereusstream.storage.object.provider.ObjectProviderTransport;
import com.nereusstream.storage.object.provider.ProviderObjectOutcome;
import com.nereusstream.storage.object.recovery.RecoveryEnvelopeLimits;
import com.nereusstream.storage.object.s3.S3C1ObjectProviderTransport;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/** Real MinIO and Oxia, with response-loss hooks around actual backend operations; local deterministic KMS key. */
class KafkaObjectAuthorizationRealTest {
    enum PutPath {
        CREATED_SINGLE,
        CREATED_SHARED,
        CREATED_WITHOUT_EVIDENCE_SHARED,
        UNKNOWN_SHARED,
        EXISTING_SHARED
    }

    @ParameterizedTest
    @EnumSource(PutPath.class)
    void measuresActualPutAndPublicationReads(PutPath path) throws Exception {
        try (Fixture f = new Fixture(8)) {
            Partition a = f.partition(0, 6, 7);
            f.provider.losePut = path == PutPath.UNKNOWN_SHARED;
            f.provider.precreate = path == PutPath.EXISTING_SHARED;
            f.provider.dropCreationEvidence = path == PutPath.CREATED_WITHOUT_EVIDENCE_SHARED;
            if (path == PutPath.CREATED_SINGLE) {
                var candidate = a.prepare(f, data(0, 7, 0, false));
                var single = new KafkaNwg1ObjectPipelineV1(
                        f.root,
                        f.session,
                        f.verification,
                        new KafkaObjectPhysicalFrontiersV1(f.session.rootSha256()),
                        a.tracker,
                        f.initialPublisher,
                        f.authority);
                single.writeResolveAndInstall(
                        f.plan(List.of(candidate), 0), candidate.ticket, candidate.commit, candidate.nativeState, 2);
                assertThat(a.bridge.publishNext(() -> {})).isPresent();
            } else {
                a.append(f, data(0, 7, 0, false));
            }
            long length = f.provider.lastNwgBodyBytes;
            int expectedGets = path == PutPath.CREATED_SINGLE || path == PutPath.CREATED_SHARED ? 0 : 1;
            int expectedPuts = path == PutPath.EXISTING_SHARED ? 2 : 1;
            assertThat(f.http.puts).isEqualTo(expectedPuts);
            assertThat(f.http.heads).isZero();
            assertThat(f.provider.fullGets).isEqualTo(expectedGets);
            // Count SDK transmission attempts separately: a connection retry has no additional consumed body.
            assertThat(f.http.gets).isGreaterThanOrEqualTo(expectedGets);
            if (expectedGets == 0) {
                assertThat(f.http.gets).isZero();
            }
            assertThat(f.http.ranges).isZero();
            assertThat(f.provider.putBytes).isEqualTo(expectedPuts * length);
            assertThat(f.provider.getBytes).isEqualTo(expectedGets * length);
            assertThat(a.snapshot().root().frontiers().highWatermark()).isEqualTo(1);
            System.out.println("NSIP1 PUT path=" + path + " body=" + length
                    + " put=" + f.http.puts + " head=" + f.http.heads + " get=" + f.http.gets
                    + " fullGetCalls=" + f.provider.fullGets + " range=" + f.http.ranges
                    + " putBytes=" + f.provider.putBytes
                    + " getBytes=" + f.provider.getBytes);
        }
    }

    @Test
    void conflictingExistingObjectCannotAuthorizeOrPublish() throws Exception {
        try (Fixture f = new Fixture(8)) {
            Partition a = f.partition(0, 6, 7);
            f.provider.precreateConflict = true;
            assertThatThrownBy(() -> a.append(f, data(0, 7, 0, false)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("conflict");
            assertThat(a.snapshot().root().frontiers().highWatermark()).isZero();
            assertThat(f.authority.read(a.fence).orElseThrow().grants()).isEmpty();
            assertThat(f.provider.fullGets).isOne();
            assertThat(f.provider.getBytes).isEqualTo(f.provider.lastNwgBodyBytes);
        }
    }

    @Test
    void actualVersionedPutReturnsExactVersionAndRejectsWrongVersionRead() throws Exception {
        try (Fixture f = new Fixture(8, true)) {
            var a = f.partition(0, 6, 7);
            a.append(f, data(0, 7, 0, false));
            var response = f.provider.lastResponse;
            var identity = response.createdIdentity().orElseThrow();
            var version = response.immutableVersionToken();
            assertThat(version).isPresent();
            assertThat(f.http.gets).isZero();
            try (var read = f.provider.delegate.get(identity.key(), version)) {
                assertThat(read.immutableVersionToken()).isEqualTo(version);
                assertThat(Sha256Digest.hash(CanonicalBytes.copyOf(read.body().readAllBytes())))
                        .isEqualTo(identity.bodySha256());
            }
            assertThatThrownBy(() -> f.provider.delegate.get(
                            identity.key(),
                            Optional.of(CanonicalBytes.copyOf(
                                    UUID.randomUUID().toString().getBytes(java.nio.charset.StandardCharsets.UTF_8)))))
                    .isInstanceOf(IOException.class);
            assertThat(f.root.providerConfiguration().proofMode()).isEqualTo(ProviderProofMode.NONE);
        }
    }

    @Test
    void lostPutAndGrantReplyThenCheckpointTailSurviveTwoColdPartitionTakeovers() throws Exception {
        try (Fixture f = new Fixture(2)) {
            Partition a = f.partition(0, 6, 7);
            f.provider.losePut = true;
            f.metadata.afterGrant = () -> {};
            a.append(f, data(0, 7, 0, true));
            assertThat(a.snapshot().root().frontiers().highWatermark()).isEqualTo(1);
            assertThat(a.snapshot().root().frontiers().lastStableOffset()).isZero();
            int beforeRead = f.provider.ranges;
            Partition openTxn = a;
            assertThatThrownBy(() -> f.readAtLso(openTxn, 1)).hasMessageContaining("source plan failed closed");
            assertThat(f.provider.ranges).isEqualTo(beforeRead);
            assertThat(f.readAtIsolation(
                                    openTxn,
                                    1,
                                    com.nereusstream.kafka.bookkeeper.protocol.KafkaReadIsolationV1.READ_UNCOMMITTED)
                            .ranges())
                    .hasSize(1);
            f.authority.checkpoint(f.session.rootSha256(), f.run(a.fence), a.coordinator);
            a.append(f, marker(1, 7, ControlRecordType.COMMIT));
            assertThat(a.snapshot().root().frontiers().lastStableOffset()).isEqualTo(2);
            var committedRead = f.readAtLso(a, 2);
            assertThat(committedRead.ranges()).hasSize(2);
            assertThat(committedRead.ranges().get(0).kafkaRecordBatchBytes()).isEqualTo(data(0, 7, 0, true));
            assertThat(committedRead.ranges().get(1).kafkaRecordBatchBytes())
                    .isEqualTo(marker(1, 7, ControlRecordType.COMMIT));

            var first = f.authority.close(a.fence);
            a = f.takeover(first, 7, 8);
            assertThat(a.snapshot().root().frontiers().highWatermark()).isEqualTo(2);
            assertThat(a.snapshot()
                            .committedProducerState()
                            .findDuplicate(new KafkaBatchDuplicateIdentityV1(42, (short) 0, 0, 0)))
                    .hasValueSatisfying(result -> {
                        assertThat(result.startOffset()).isZero();
                        assertThat(result.maxTimestamp()).isEqualTo(100);
                    });
            // A run/Owner change cannot clear the two-unit recovery debt. CP is the only release.
            a.append(f, data(2, 8, 1, true));
            assertThatThrownBy(a.tracker::reserveBeforePosition).hasMessageContaining("capacity");
            f.authority.checkpoint(f.session.rootSha256(), f.run(a.fence), a.coordinator);
            a.append(f, marker(3, 8, ControlRecordType.ABORT));
            var second = f.authority.close(a.fence);
            a = f.takeover(second, 8, 9);
            assertThat(a.snapshot().root().frontiers().highWatermark()).isEqualTo(4);
            assertThat(a.snapshot().root().frontiers().lastStableOffset()).isEqualTo(4);
            assertThat(a.snapshot().transactionState().completedTransactions()).hasSize(2);
            assertThat(a.snapshot().activeTail().locators().stream()
                            .map(locator -> locator.extent())
                            .distinct()
                            .count())
                    .isEqualTo(4);
            int rangesBeforeRead = f.provider.ranges;
            assertThat(f.readAtLso(a, 4).ranges()).hasSize(4);
            assertThat(f.provider.ranges - rangesBeforeRead).isEqualTo(8);
            assertThat(a.snapshot().transactionState().completedTransactions())
                    .filteredOn(txn -> txn.firstOffset() == 2)
                    .singleElement()
                    .satisfies(txn -> {
                        assertThat(txn.markerEndOffsetExclusive()).isEqualTo(4);
                        assertThat(txn.aborted()).isTrue();
                    });
            assertThat(a.snapshot().activeTail().locators()).hasSize(4);
            assertThat(a.snapshot()
                            .committedProducerState()
                            .findDuplicate(new KafkaBatchDuplicateIdentityV1(42, (short) 0, 1, 1)))
                    .hasValueSatisfying(result -> {
                        assertThat(result.startOffset()).isEqualTo(2);
                        assertThat(result.maxTimestamp()).isEqualTo(102);
                    });
            assertThat(f.provider.lists).isEqualTo(1); // Only the lost-PUT exact Provider reconciliation used LIST.
        }
    }

    @Test
    void unknownGrantWithUnavailableReadRetriesTheExactCandidateWithoutAnotherPut() throws Exception {
        try (Fixture f = new Fixture(8)) {
            Partition a = f.partition(0, 6, 7);
            var candidate = a.prepare(f, data(0, 7, 0, false));
            var plan = f.plan(List.of(candidate), 0);
            var members = List.of(new KafkaNwg1ObjectPipelineV1.SharedMember(
                    candidate.ticket, candidate.commit, candidate.nativeState, a.tracker));
            f.metadata.afterGrant = () -> f.metadata.failReadKey = f.authority.headKey(a.fence);
            assertThatThrownBy(() -> f.pipeline.writeResolveAndInstallShared(plan, members, 2))
                    .hasMessageContaining("grant reconciliation read unavailable");
            assertThat(a.bridge.publishNext(() -> {})).isEmpty();
            assertThat(a.snapshot().root().frontiers().highWatermark()).isZero();
            assertThat(a.snapshot().root().frontiers().allocatedEndOffset()).isEqualTo(1);
            assertThat(f.authority.read(a.fence).orElseThrow().grants()).hasSize(1);
            int verifiedRanges = f.provider.ranges;
            var resolved = f.pipeline.writeResolveAndInstallShared(plan, members, 3);
            assertThat(resolved.verifiedMembers()).hasSize(1);
            assertThat(a.bridge.publishNext(() -> {})).isPresent();
            assertThat(a.bridge.publishNext(() -> {})).isEmpty();
            assertThat(a.snapshot().root().frontiers().highWatermark()).isEqualTo(1);
            assertThat(a.snapshot().root().frontiers().allocatedEndOffset()).isEqualTo(1);
            assertThat(f.provider.puts).isEqualTo(1);
            assertThat(f.provider.ranges).isEqualTo(verifiedRanges);
            assertThat(f.provider.lists).isZero();
            assertThat(f.takeover(f.authority.close(a.fence), 7, 8)
                            .snapshot()
                            .root()
                            .frontiers()
                            .highWatermark())
                    .isEqualTo(1);
        }
    }

    @Test
    void closingOneSharedMemberAfterUploadDoesNotFenceSiblingOrClosePhysicalRun() throws Exception {
        try (Fixture f = new Fixture(8)) {
            Partition a = f.partition(0, 6, 7);
            Partition b = f.partition(1, 6, 7); // Equal local ticket values belong to independent trackers.
            var physical = f.pipeline.reservePhysicalBeforePosition(4096);
            var ca = a.prepare(f, data(0, 7, 0, false), physical);
            var cb = b.prepare(f, data(0, 7, 0, false), physical);
            f.metadata.beforeKey = f.authority.headKey(a.fence);
            f.metadata.beforeGrant = () -> f.authority.close(a.fence);
            var candidates = new ArrayList<>(List.of(ca, cb));
            candidates.sort(Comparator.comparing(
                    c -> c.commit.expectedFence().bindingId().digest().toHex()));
            var result = f.pipeline.writeResolveAndInstallShared(
                    f.plan(candidates, 1),
                    candidates.stream()
                            .map(c -> new KafkaNwg1ObjectPipelineV1.SharedMember(
                                    c.ticket, c.commit, c.nativeState, c.partition.tracker))
                            .toList(),
                    1);
            assertThat(result.authorizationFailures()).hasSize(1);
            assertThat(result.verifiedMembers()).hasSize(1);
            assertThat(a.bridge.publishNext(() -> {})).isEmpty();
            assertThat(b.bridge.publishNext(() -> {})).isPresent();
            assertThat(b.snapshot().root().frontiers().highWatermark()).isEqualTo(1);
            assertThat(f.session.runtimeState()).isEqualTo(WalRunRuntime.State.ADMITTING);
            var closed = f.authority.readClosed(a.fence);
            var recovered = f.recover(closed);
            assertThat(recovered.state().vector().recoveryCoveredThrough()).isZero();
            b.append(f, data(1, 7, 1, false));
            assertThat(b.snapshot().root().frontiers().highWatermark()).isEqualTo(2);
            assertThat(f.provider.puts).isEqualTo(2);
            assertThat(f.provider.lists).isZero();
        }
    }

    @Test
    void grantReplyLostAcrossCloseAndTwoOwnerOpensStillNamesTheExactSelectedCommit() throws Exception {
        try (Fixture f = new Fixture(8)) {
            Partition a = f.partition(0, 6, 7);
            f.metadata.afterGrant = () -> f.authority.close(a.fence);
            a.append(f, data(0, 7, 0, false));
            var old = f.authority.readClosed(a.fence);
            assertThat(old.head().endOffset()).isEqualTo(1);
            var next = f.takeover(old, 7, 8);
            var middle = f.authority.close(next.fence);
            var last = f.takeover(middle, 8, 9);
            assertThat(f.authority.readClosed(a.fence).digest()).isEqualTo(old.digest());
            assertThat(last.snapshot().root().frontiers().highWatermark()).isEqualTo(1);
            assertThat(f.provider.puts).isEqualTo(1);
            assertThat(f.provider.lists).isZero();
        }
    }

    @Test
    void checkpointSelectionFailureKeepsDebtAndRejectsBeforeAllocatingAnotherOffset() throws Exception {
        try (Fixture f = new Fixture(1)) {
            Partition a = f.partition(0, 6, 7);
            a.append(f, data(0, 7, 0, false));
            var before = a.snapshot();
            f.metadata.beforeGrant = () -> {
                throw new IllegalStateException("checkpoint selection failed");
            };
            assertThatThrownBy(() -> f.authority.checkpoint(f.session.rootSha256(), f.run(a.fence), a.coordinator))
                    .hasMessageContaining("selection failed");
            assertThatThrownBy(a.tracker::reserveBeforePosition).hasMessageContaining("capacity");
            assertThat(a.snapshot().root().frontiers().allocatedEndOffset()).isEqualTo(1);
            f.authority.checkpoint(f.session.rootSha256(), f.run(a.fence), a.coordinator);
            a.append(f, data(1, 7, 1, false));
            assertThat(a.snapshot().root().frontiers().highWatermark()).isEqualTo(2);
        }
    }

    @Test
    void physicalSuccessorKeepsCumulativeDebtAndColdRecoveryKeepsBothRoots() throws Exception {
        try (Fixture f = new Fixture(1)) {
            Partition a = f.partition(0, 6, 7);
            a.append(f, data(0, 7, 0, false));
            var firstRoot = f.session.rootSha256();
            f.rollover(a);
            assertThat(f.session.rootSha256()).isNotEqualTo(firstRoot);
            assertThatThrownBy(a.tracker::reserveBeforePosition).hasMessageContaining("capacity");
            f.authority.checkpoint(f.session.rootSha256(), f.run(a.fence), a.coordinator);
            a.append(f, data(1, 7, 1, false));
            var successor = f.takeover(f.authority.close(a.fence), 7, 8);
            assertThat(successor.snapshot().root().frontiers().highWatermark()).isEqualTo(2);
            assertThat(successor.snapshot().activeTail().locators())
                    .extracting(l -> l.extent().walRunRootSha())
                    .containsExactly(firstRoot, f.session.rootSha256());
            assertThat(f.provider.lists).isZero();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void crashBeforeOrAfterGrantRecoversOnlyFiniteSelectedPrefix(boolean grantApplied) throws Exception {
        try (Fixture f = new Fixture(8)) {
            Partition a = f.partition(0, 6, 7);
            Runnable crash = () -> {
                throw new IllegalStateException("crash before local publication");
            };
            if (grantApplied) {
                f.metadata.afterGrant = crash;
            } else {
                f.metadata.beforeGrant = crash;
            }
            assertThatThrownBy(() -> a.append(f, data(0, 7, 0, false))).hasMessageContaining("crash");
            assertThat(a.snapshot().root().frontiers().highWatermark()).isZero();
            var closed = f.authority.close(a.fence);
            var successor = f.takeover(closed, 7, 8);
            assertThat(successor.snapshot().root().frontiers().highWatermark()).isEqualTo(grantApplied ? 1 : 0);
            assertThat(f.authority.read(a.fence).orElseThrow().grants()).hasSize(grantApplied ? 1 : 0);
            assertThatThrownBy(() -> a.append(f, data(1, 7, 1, false)))
                    .isInstanceOf(KafkaObjectAuthorizationV1.Fenced.class);
            assertThat(f.provider.puts).isEqualTo(1);
        }
    }

    enum CheckpointBlock {
        PAGE,
        HEAD
    }

    @ParameterizedTest
    @EnumSource(CheckpointBlock.class)
    void blockedPhysicalCheckpointLeavesPublicationAndCapacityIndependentAndSealWaits(CheckpointBlock block)
            throws Exception {
        try (Fixture f = new Fixture(8, new WalCheckpointPolicy(1, 2, 1024 * 1024, 30_000, 16, 8192))) {
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            Runnable pause = () -> {
                entered.countDown();
                await(release);
            };
            if (block == CheckpointBlock.PAGE) {
                f.metadata.beforePhysicalPage = pause;
            } else {
                f.metadata.beforePhysicalHead = pause;
            }
            var executor = Executors.newFixedThreadPool(2);
            try {
                var a = f.partition(0, 6, 7);
                var first = executor.submit(() -> a.append(f, data(0, 7, 0, false)));
                assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
                first.get(5, TimeUnit.SECONDS);
                assertThat(a.snapshot().root().frontiers().highWatermark()).isEqualTo(1);
                var second = executor.submit(() -> a.append(f, data(1, 7, 1, false)));
                second.get(5, TimeUnit.SECONDS);
                assertThat(a.snapshot().root().frontiers().highWatermark()).isEqualTo(2);
                assertThat(f.publisher.uncoveredExtentCount()).isEqualTo(2);
                assertThat(f.publisher.queueDepth()).isEqualTo(2);
                var before = f.session.runtimeRecoveryState();
                assertThatThrownBy(a.tracker::reserveBeforePosition).hasMessageContaining("capacity");
                assertThat(a.snapshot().root().frontiers().allocatedEndOffset()).isEqualTo(2);
                assertThat(f.session.runtimeRecoveryState()).isEqualTo(before);
                assertThatThrownBy(() -> f.publisher.takeover(2)).hasMessageContaining("drained I/O");
                assertThatThrownBy(f.session::close).hasMessageContaining("retains Provider operations");
                assertThat(f.session.state()).isEqualTo(WalRunObjectSession.State.DRAINING);
                f.session.sealRuntime();
                var sealing = new CountDownLatch(1);
                var seal = executor.submit(() -> {
                    sealing.countDown();
                    return f.pipeline.publishPhysicalSeal(new WalRunLifecycleManager(f.metadata));
                });
                assertThat(sealing.await(5, TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(() -> seal.get(100, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
                release.countDown();
                seal.get(10, TimeUnit.SECONDS);
                assertThat(f.publisher.uncoveredExtentCount()).isZero();
                assertThat(f.publisher.uncoveredBodyBytes()).isZero();
                assertThat(f.publisher.head().coveredThrough()).isEqualTo(LaneSequenceVector.of(1, -1, -1));
                assertThatThrownBy(() -> f.publisher.takeover(3)).hasMessageContaining("closed");
                assertThat(f.provider.puts).isEqualTo(2);
                assertThat(f.provider.fullGets).isZero();
            } finally {
                release.countDown();
                executor.shutdownNow();
                assertThat(executor.awaitTermination(15, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    @Test
    void lostPhysicalHeadReplyAndFailedFirstReadRetainExactCandidateAcrossNewEnqueue() throws Exception {
        try (Fixture f = new Fixture(8, new WalCheckpointPolicy(0, 3, 1024 * 1024, 30_000, 16, 8192))) {
            var a = f.partition(0, 6, 7);
            a.append(f, data(0, 7, 0, false));
            f.metadata.afterPhysicalHead =
                    () -> f.metadata.failPhysicalReadKey = WalRunControlKeys.checkpointHeadKey(7, 1);
            assertThatThrownBy(f.publisher::publishNext).hasMessageContaining("physical checkpoint read unavailable");
            assertThat(f.publisher.head().pageOrdinal()).isEqualTo(-1);
            assertThat(f.publisher.uncoveredExtentCount()).isOne();
            var originalPage = f.metadata.physicalPages.get(0);
            var originalCas = f.metadata.physicalHeads.get(0);
            a.append(f, data(1, 7, 1, false));
            assertThat(a.snapshot().root().frontiers().highWatermark()).isEqualTo(2);
            assertThat(f.publisher.uncoveredExtentCount()).isEqualTo(2);
            var recoveredPage = f.publisher.publishNext().orElseThrow();
            assertThat(WalRunControlCodec.encodeCheckpointPage(recoveredPage)).isEqualTo(originalPage.value());
            assertThat(recoveredPage.extents()).hasSize(1);
            assertThat(f.metadata.physicalPages).hasSize(1);
            assertThat(f.metadata.physicalHeads).containsExactly(originalCas);
            assertThat(f.publisher.uncoveredExtentCount()).isOne();
            f.publisher.flush();
            assertThat(f.publisher.head().pageOrdinal()).isEqualTo(1);
            assertThat(f.publisher.head().coveredThrough()).isEqualTo(LaneSequenceVector.of(1, -1, -1));
            assertThat(f.publisher.uncoveredExtentCount()).isZero();
            assertThat(f.publisher.uncoveredBodyBytes()).isZero();
            assertThat(f.metadata.physicalPages).hasSize(2);
            assertThat(WalRunControlCodec.decodeCheckpointPage(
                                    f.metadata.physicalPages.get(1).value(), f.root.providerConfiguration())
                            .extents())
                    .singleElement()
                    .satisfies(row -> assertThat(row.laneSequence()).isEqualTo(1));
            assertThat(f.metadata.physicalHeads).hasSize(2);
            assertThat(f.provider.puts).isEqualTo(2);
            assertThat(f.provider.fullGets).isZero();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void prePositionDebtIncludesBlockedPutAndSharedCandidateWhileLanePrefixesRemainOrdered(int siblingLane)
            throws Exception {
        try (Fixture f = new Fixture(8, new WalCheckpointPolicy(0, 2, 1024 * 1024, 30_000, 16, 8192))) {
            var a = f.partition(0, 6, 7);
            var b = f.partition(1, 6, 7);
            var c = f.partition(2, 6, 7);
            var first = a.prepare(f, data(0, 7, 0, false));
            var shared = f.pipeline.reservePhysicalBeforePosition(4096);
            var siblings = new ArrayList<>(
                    List.of(b.prepare(f, data(0, 7, 0, false), shared), c.prepare(f, data(0, 7, 0, false), shared)));
            siblings.sort(Comparator.comparing(
                    value -> value.commit.expectedFence().bindingId().digest().toHex()));
            var siblingPlan = f.plan(siblings, siblingLane);
            var siblingMembers = siblings.stream()
                    .map(value -> new KafkaNwg1ObjectPipelineV1.SharedMember(
                            value.ticket, value.commit, value.nativeState, value.partition.tracker))
                    .toList();
            assertThat(f.publisher.uncoveredExtentCount()).isEqualTo(2);
            assertThat(f.publisher.uncoveredBodyBytes()).isEqualTo(8192);
            assertThatThrownBy(a.tracker::reserveBeforePosition).hasMessageContaining("capacity");
            assertThat(a.snapshot().root().frontiers().allocatedEndOffset()).isOne();
            assertThat(f.publisher.queueDepth()).isZero();
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            f.provider.beforeNwgPut = () -> {
                entered.countDown();
                await(release);
            };
            var checkpointEntered = new CountDownLatch(1);
            var checkpointRelease = new CountDownLatch(1);
            if (siblingLane == 1) {
                f.metadata.beforePhysicalPage = () -> {
                    checkpointEntered.countDown();
                    await(checkpointRelease);
                };
            }
            var executor = Executors.newFixedThreadPool(2);
            try {
                var firstMembers = List.of(new KafkaNwg1ObjectPipelineV1.SharedMember(
                        first.ticket, first.commit, first.nativeState, a.tracker));
                var firstPlan = f.plan(List.of(first), 0);
                var firstPut =
                        executor.submit(() -> f.pipeline.writeResolveAndInstallShared(firstPlan, firstMembers, 2));
                assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(() -> f.pipeline.writeResolveAndInstallShared(firstPlan, firstMembers, 2))
                        .hasMessageContaining("in-flight work");
                var siblingPut =
                        executor.submit(() -> f.pipeline.writeResolveAndInstallShared(siblingPlan, siblingMembers, 2));
                if (siblingLane == 0) {
                    assertThatThrownBy(() -> siblingPut.get(5, TimeUnit.SECONDS))
                            .hasRootCauseMessage("sealed plan differs from the unresolved lane candidate");
                    assertThat(b.bridge.publishNext(() -> {})).isEmpty();
                    assertThat(c.bridge.publishNext(() -> {})).isEmpty();
                } else {
                    siblingPut.get(5, TimeUnit.SECONDS);
                    assertThat(b.bridge.publishNext(() -> {})).isPresent();
                    assertThat(c.bridge.publishNext(() -> {})).isPresent();
                    assertThat(checkpointEntered.await(5, TimeUnit.SECONDS)).isTrue();
                }
                assertThat(f.publisher.uncoveredExtentCount()).isEqualTo(2);
                if (siblingLane == 0) {
                    assertThat(f.provider.puts).isOne();
                    assertThat(f.authority.read(b.fence).orElseThrow().grants()).isEmpty();
                    assertThat(f.authority.read(c.fence).orElseThrow().grants()).isEmpty();
                } else {
                    assertThatThrownBy(f.session::close).hasMessageContaining("retains Provider operations");
                }
                release.countDown();
                firstPut.get(10, TimeUnit.SECONDS);
                assertThat(a.bridge.publishNext(() -> {})).isPresent();
                if (siblingLane == 0) {
                    f.pipeline.writeResolveAndInstallShared(siblingPlan, siblingMembers, 3);
                    assertThat(b.bridge.publishNext(() -> {})).isPresent();
                    assertThat(c.bridge.publishNext(() -> {})).isPresent();
                }
                checkpointRelease.countDown();
                f.publisher.flush();
                assertThat(f.publisher.uncoveredExtentCount()).isZero();
                assertThat(f.publisher.uncoveredBodyBytes()).isZero();
                assertThat(f.provider.puts).isEqualTo(2);
                assertThat(f.provider.fullGets).isZero();
                assertThat(a.snapshot().root().frontiers().highWatermark()).isOne();
                assertThat(b.snapshot().root().frontiers().highWatermark()).isOne();
                assertThat(c.snapshot().root().frontiers().highWatermark()).isOne();
            } finally {
                release.countDown();
                checkpointRelease.countDown();
                executor.shutdownNow();
                assertThat(executor.awaitTermination(15, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    @Test
    void blockedSealMetadataKeepsSessionResourcesUntilRealIoTerminates() throws Exception {
        try (Fixture f = new Fixture(8)) {
            var a = f.partition(0, 6, 7);
            a.append(f, data(0, 7, 0, false));
            f.session.stopAdmission(WalRunRuntime.StopReason.OWNER_REQUEST);
            f.session.sealRuntime();
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            f.metadata.beforeSeal = () -> {
                entered.countDown();
                await(release);
            };
            f.metadata.afterSeal = () -> f.metadata.failPhysicalReadKey = WalRunControlKeys.sealKey(7, 1);
            var executor = Executors.newSingleThreadExecutor();
            try {
                var seal =
                        executor.submit(() -> f.pipeline.publishPhysicalSeal(new WalRunLifecycleManager(f.metadata)));
                assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(f.session::close).hasMessageContaining("retains Provider operations");
                assertThatThrownBy(() -> f.publisher.takeover(2)).hasMessageContaining("closed");
                assertThat(f.session.state()).isEqualTo(WalRunObjectSession.State.DRAINING);
                release.countDown();
                assertThatThrownBy(() -> seal.get(10, TimeUnit.SECONDS))
                        .hasRootCauseMessage("physical checkpoint read unavailable");
                f.session.requireTerminalClosable();
                // Retry the exact Seal after response loss without reopening the closed physical publisher.
                f.pipeline.publishPhysicalSeal(new WalRunLifecycleManager(f.metadata));
                f.session.requireTerminalClosable();
            } finally {
                release.countDown();
                executor.shutdownNow();
                assertThat(executor.awaitTermination(15, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"false,false", "true,false", "false,true", "true,true"})
    void unknownCandidateSurvivesTemporaryConcurrencyRejectionFromRealProtocolRead(boolean protocol, boolean failedList)
            throws Exception {
        try (Fixture f = new Fixture(8, false, null, 1)) {
            var a = f.partition(0, 6, 7);
            var readObject = f.protocolObject(a);
            var readIdentity = new ObjectIdentity(readObject.key(), readObject.length(), readObject.digest());
            f.session.conditionalCreateKafkaProtocolObject(
                    f.session.validateKafkaProtocolObject(readIdentity, readObject.body()));
            var candidate = protocol ? null : a.prepare(f, data(0, 7, 0, false));
            var plan = protocol ? null : f.plan(List.of(candidate), 0);
            List<KafkaNwg1ObjectPipelineV1.SharedMember> members = protocol
                    ? List.of()
                    : List.of(new KafkaNwg1ObjectPipelineV1.SharedMember(
                            candidate.ticket, candidate.commit, candidate.nativeState, a.tracker));
            var protocolObject = protocol ? f.protocolObject(f.partition(1, 6, 7)) : null;
            var protocolIdentity = protocol
                    ? new ObjectIdentity(protocolObject.key(), protocolObject.length(), protocolObject.digest())
                    : null;
            var initial = f.session.recoverySnapshot();
            f.provider.losePut = true;
            if (failedList) {
                f.provider.loseListResponse = true;
                if (protocol) {
                    assertThat(f.session
                                    .conditionalCreateKafkaProtocolObject(f.session.validateKafkaProtocolObject(
                                            protocolIdentity, protocolObject.body()))
                                    .outcome())
                            .isEqualTo(ProviderObjectOutcome.OUTCOME_UNKNOWN);
                    assertThatThrownBy(() -> f.session.reconcileUnknownProtocolObject(protocolIdentity))
                            .hasMessageContaining("actual LIST reply unavailable");
                } else {
                    assertThatThrownBy(() -> f.pipeline.writeResolveAndInstallShared(plan, members, 1))
                            .hasRootCauseMessage("actual LIST reply unavailable");
                }
                assertThat(f.provider.lists).isOne();
                assertThat(f.provider.puts).isEqualTo(2);
            }
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            f.provider.beforeFullGet = () -> {
                entered.countDown();
                await(release);
            };
            var executor = Executors.newSingleThreadExecutor();
            try {
                var read = executor.submit(() -> f.session.readVerifiedProtocolObject(readIdentity));
                assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
                var before = f.session.recoverySnapshot();
                assertThat(before.currentConcurrency()).isOne();
                if (protocol) {
                    if (!failedList) {
                        assertThat(f.session
                                        .conditionalCreateKafkaProtocolObject(f.session.validateKafkaProtocolObject(
                                                protocolIdentity, protocolObject.body()))
                                        .outcome())
                                .isEqualTo(ProviderObjectOutcome.OUTCOME_UNKNOWN);
                    }
                    assertThatThrownBy(() -> f.session.reconcileUnknownProtocolObject(protocolIdentity))
                            .hasMessageContaining("recovery concurrency");
                } else {
                    assertThatThrownBy(() -> f.pipeline.writeResolveAndInstallShared(plan, members, 2))
                            .hasMessageContaining("recovery concurrency");
                    assertThat(a.snapshot().root().frontiers().allocatedEndOffset())
                            .isOne();
                    assertThat(a.snapshot().root().frontiers().highWatermark()).isZero();
                }
                assertThat(f.provider.lists).isEqualTo(failedList ? 1 : 0);
                assertThat(f.provider.puts).isEqualTo(2);
                var rejected = f.session.recoverySnapshot();
                assertThat(rejected.listPages()).isEqualTo(before.listPages());
                assertThat(rejected.listedKeys()).isEqualTo(before.listedKeys());
                assertThat(rejected.listedKeyBytes()).isEqualTo(before.listedKeyBytes());
                assertThat(rejected.fullGetRequests()).isEqualTo(before.fullGetRequests());
                assertThat(rejected.retryAttempts()).isEqualTo(before.retryAttempts());
                var reserved = f.session.runtimeRecoveryState();
                release.countDown();
                assertThat(read.get(10, TimeUnit.SECONDS)).isEqualTo(readObject.body());
                if (protocol) {
                    assertThat(f.session
                                    .reconcileUnknownProtocolObject(protocolIdentity)
                                    .outcome())
                            .isEqualTo(ProviderObjectOutcome.EXISTING_EXACT);
                    assertThat(f.session.readVerifiedProtocolObject(protocolIdentity))
                            .isEqualTo(protocolObject.body());
                } else {
                    f.pipeline.writeResolveAndInstallShared(plan, members, 3);
                    assertThat(a.bridge.publishNext(() -> {})).isPresent();
                    assertThat(a.snapshot().root().frontiers().allocatedEndOffset())
                            .isOne();
                    assertThat(a.snapshot().root().frontiers().highWatermark()).isOne();
                    assertThat(f.session.runtimeRecoveryState().reservedExtentCount())
                            .isEqualTo(reserved.reservedExtentCount());
                    assertThat(f.provider.fullGets).isOne();
                }
                assertThat(f.provider.puts).isEqualTo(2); // The read object and the exact UNKNOWN candidate.
                assertThat(f.provider.lists).isEqualTo(failedList ? 2 : 1);
                assertThat(f.session.recoverySnapshot().listPages()).isEqualTo(initial.listPages() + 1);
                assertThat(f.session.recoverySnapshot().listedKeys()).isEqualTo(initial.listedKeys() + 1);
                assertThat(f.session.recoverySnapshot().retryAttempts()).isEqualTo(failedList ? 1 : 0);
                assertThat(f.session.recoverySnapshot().fullGetRequests())
                        .isEqualTo((protocol ? 3 : 2) + (failedList ? 1 : 0));
                assertThat(f.session.recoverySnapshot().currentConcurrency()).isZero();
                assertThat(f.session.recoverySnapshot().workingMemoryBytes()).isZero();
            } finally {
                release.countDown();
                executor.shutdownNow();
                assertThat(executor.awaitTermination(15, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void sharedKmsCellAllowsRealSiblingPublicationDuringUnwrapOrSelfValidation(boolean unwrap) throws Exception {
        var delegate = new StorageObjectNwkcp1BackendV1Test.FakeKmsTransport();
        var kmsGate = new AtomicReference<Runnable>();
        var shared = new KmsCellSession(
                new KmsTransport() {
                    @Override
                    public WrappedRunKeyEnvelope wrap(String identity, byte[] key) {
                        return delegate.wrap(identity, key);
                    }

                    @Override
                    public byte[] unwrap(WrappedRunKeyEnvelope envelope) {
                        var gate = kmsGate.getAndSet(null);
                        if (gate != null) {
                            gate.run();
                        }
                        return delegate.unwrap(envelope);
                    }
                },
                StorageObjectNwkcp1BackendV1Test.root().providerScopeId(),
                "kms/cell-a",
                2,
                new SecureRandom());
        try (var a = new Fixture(8, false, null, 4, shared, 1);
                var b = new Fixture(8, false, null, 4, shared, 2)) {
            var pa = a.partition(0, 6, 7);
            var pb = b.partition(0, 6, 7);
            var warm = pb.prepare(b, data(0, 7, 0, false));
            b.writeShared(List.of(warm), 0, 1);
            assertThat(pb.bridge.publishNext(() -> {})).isPresent();
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            Runnable block = () -> {
                entered.countDown();
                await(release);
            };
            if (unwrap) {
                kmsGate.set(block);
            } else {
                // Admission validates the native batch first; block its actual encrypted-body self-verification.
                a.beforeNativeValidation = () -> a.beforeNativeValidation = block;
            }
            var first = pa.prepare(a, data(0, 7, 0, false));
            var second = pb.prepare(b, data(1, 7, 1, false));
            var executor = Executors.newFixedThreadPool(2);
            try {
                var blocked = executor.submit(() -> a.writeShared(List.of(first), 0, 2));
                assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
                var independent = executor.submit(() -> b.writeShared(List.of(second), 0, 3));
                independent.get(10, TimeUnit.SECONDS);
                assertThat(pb.bridge.publishNext(() -> {})).isPresent();
                assertThat(pb.snapshot().root().frontiers().highWatermark()).isEqualTo(2);
                assertThat(a.provider.puts).isZero();
                assertThatThrownBy(shared::close).hasMessageContaining("actual key operations");
                release.countDown();
                blocked.get(10, TimeUnit.SECONDS);
                assertThat(pa.bridge.publishNext(() -> {})).isPresent();
                assertThat(pa.snapshot().root().frontiers().highWatermark()).isOne();
                assertThat(a.provider.puts).isOne();
            } finally {
                release.countDown();
                executor.shutdownNow();
                assertThat(executor.awaitTermination(15, TimeUnit.SECONDS)).isTrue();
            }
        } finally {
            shared.close();
        }
    }

    @Test
    void sharedKmsBudgetSupportsRealRunRotationWithLongLivedRun() throws Exception {
        var transport = new RoundTripKmsTransport();
        var shared = new KmsCellSession(
                transport,
                StorageObjectNwkcp1BackendV1Test.root().providerScopeId(),
                "kms/cell-a",
                2,
                new SecureRandom());
        var wrappedKeys = new HashSet<WrappedRunKeyEnvelope>();
        try (var longRun = new Fixture(8, false, null, 4, shared, 1, true)) {
            assertThat(wrappedKeys.add(longRun.root.wrappedRunKey())).isTrue();
            var longPartition = longRun.partition(0, 6, 7);
            var first = longPartition.prepare(longRun, data(0, 7, 0, false));
            var firstResult = longRun.writeShared(List.of(first), 0, 1);
            assertThat(longPartition.bridge.publishNext(() -> {})).isPresent();
            longRun.verifyFreshUnwrap(transport, firstResult);
            for (long epoch = 2; epoch <= 8; epoch++) {
                try (var next = new Fixture(8, false, null, 4, shared, epoch, true)) {
                    assertThat(wrappedKeys.add(next.root.wrappedRunKey())).isTrue();
                    var p = next.partition(0, 6, 7);
                    var candidate = p.prepare(next, data(0, 7, 0, false));
                    var result = next.writeShared(List.of(candidate), 0, 1);
                    assertThat(p.bridge.publishNext(() -> {})).isPresent();
                    assertThat(p.snapshot().root().frontiers().highWatermark()).isOne();
                    assertThat(next.provider.puts).isOne();
                    next.verifyFreshUnwrap(transport, result);
                }
            }
            var second = longPartition.prepare(longRun, data(1, 7, 1, false));
            var secondResult = longRun.writeShared(List.of(second), 0, 2);
            assertThat(longPartition.bridge.publishNext(() -> {})).isPresent();
            assertThat(longPartition.snapshot().root().frontiers().highWatermark())
                    .isEqualTo(2);
            longRun.verifyFreshUnwrap(transport, secondResult);
            assertThat(transport.wrapCalls).isEqualTo(8);
            assertThat(transport.unwrapCalls).isEqualTo(9);
            assertThat(wrappedKeys).hasSize(8);
            assertThat(shared.state()).isEqualTo(KmsCellSession.State.DRAINING);
            assertThatThrownBy(() -> shared.deriveObjectKey(
                            new RunKeyCacheIdentity(7, 1),
                            longRun.root.wrappedRunKey(),
                            longRun.session.rootSha256(),
                            com.nereusstream.storage.object.control.WalLaneId.OBJECT_LATENCY,
                            99))
                    .hasMessageContaining("no longer accepts operations");
        } finally {
            shared.close();
        }
    }

    private static final class RoundTripKmsTransport implements KmsTransport {
        private int wrapCalls;
        private int unwrapCalls;

        @Override
        public synchronized WrappedRunKeyEnvelope wrap(String identity, byte[] key) {
            wrapCalls++;
            byte[] ciphertext = key.clone();
            for (int index = 0; index < ciphertext.length; index++) {
                ciphertext[index] ^= (byte) 0xa5;
            }
            return new WrappedRunKeyEnvelope(
                    "fake-kms", "xor-test-v1", identity, "v1", CanonicalBytes.copyOf(ciphertext));
        }

        @Override
        public synchronized byte[] unwrap(WrappedRunKeyEnvelope envelope) {
            if (!"kms/cell-a".equals(envelope.wrappingKeyId())) {
                throw new IllegalArgumentException("unexpected KMS identity");
            }
            unwrapCalls++;
            byte[] plaintext = envelope.wrappedKey().toByteArray();
            for (int index = 0; index < plaintext.length; index++) {
                plaintext[index] ^= (byte) 0xa5;
            }
            return plaintext;
        }
    }

    static void await(CountDownLatch latch) {
        try {
            if (!latch.await(15, TimeUnit.SECONDS)) {
                throw new IllegalStateException("fault gate timed out");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("fault gate interrupted", interrupted);
        }
    }

    static CanonicalBytes data(long offset, int leader, int seq, boolean txn) {
        var record = new SimpleRecord(100 + offset, new byte[] {1}, new byte[] {(byte) offset});
        return canonical(
                txn
                        ? MemoryRecords.withTransactionalRecords(
                                RecordBatch.MAGIC_VALUE_V2,
                                offset,
                                Compression.NONE,
                                42,
                                (short) 0,
                                seq,
                                leader,
                                record)
                        : MemoryRecords.withIdempotentRecords(
                                RecordBatch.MAGIC_VALUE_V2,
                                offset,
                                Compression.NONE,
                                42,
                                (short) 0,
                                seq,
                                leader,
                                record));
    }

    static CanonicalBytes marker(long offset, int leader, ControlRecordType type) {
        return canonical(MemoryRecords.withEndTransactionMarker(
                offset, 100 + offset, leader, 42, (short) 0, new EndTransactionMarker(type, 1)));
    }

    static CanonicalBytes canonical(MemoryRecords records) {
        byte[] raw = new byte[records.sizeInBytes()];
        records.buffer().duplicate().get(raw);
        return CanonicalBytes.copyOf(raw);
    }

    record Candidate(
            Partition partition,
            KafkaObjectCompletionTrackerV1.AssignedTicket ticket,
            KafkaSpeculativeCommitV1 commit,
            KafkaObjectNativeStateV1 nativeState,
            CanonicalBytes raw) {}

    static final class Partition {
        final KafkaPartitionFenceV1 fence;
        final KafkaCoherentCommitCoordinatorV1 coordinator;
        final KafkaObjectCompletionTrackerV1 tracker;
        final KafkaObjectPublicationBridgeV1 bridge;

        Partition(
                Fixture f,
                KafkaPartitionFenceV1 fence,
                KafkaCoherentCommitCoordinatorV1 coordinator,
                KafkaObjectRecoveredTailV1 recovered) {
            this.fence = fence;
            this.coordinator = coordinator;
            tracker = recovered == null
                    ? new KafkaObjectCompletionTrackerV1(8, 65536, fence.ownerEpoch())
                    : new KafkaObjectCompletionTrackerV1(8, 65536, fence.ownerEpoch(), 0, recovered);
            tracker.bindAuthorization(f.authority, fence, 4096);
            bridge = new KafkaObjectPublicationBridgeV1(
                    KafkaObjectAuthorizationV1.binding(fence),
                    coordinator.captureObject().root().frontiers().durableEndOffset(),
                    tracker,
                    coordinator);
        }

        KafkaObjectCoherentProtocolSnapshotV1 snapshot() {
            return coordinator.captureObject();
        }

        Candidate prepare(Fixture f, CanonicalBytes raw) {
            f.pipeline.bindTracker(tracker);
            var reservation = tracker.reserveBeforePosition(); // Before any protocol allocation or plan staging.
            return prepareReserved(f, raw, reservation);
        }

        Candidate prepare(Fixture f, CanonicalBytes raw, WalCheckpointPublisher.Reservation physical) {
            f.pipeline.bindTracker(tracker);
            return prepareReserved(f, raw, tracker.reserveBeforePosition(physical));
        }

        private Candidate prepareReserved(
                Fixture f, CanonicalBytes raw, KafkaObjectCompletionTrackerV1.Reservation reservation) {
            var assigned = KafkaNativeAssignedRecordBatchV1.validate(KafkaRawAssignedRecordBatchFactsV1.parse(raw));
            var delta = new KafkaNativeProtocolBatchAdapterV1().protocolDelta(assigned);
            var plan = new KafkaProtocolAppendPlanV1(fence, List.of(delta));
            var hooks = coordinator.protocolHooks(plan);
            hooks.validateBeforeOffsetAssignment();
            hooks.prepareAfterOffsetAssignment(
                    new KafkaOffsetAssignedAppendV1(assigned.baseOffset(), assigned.endOffsetExclusive(), ignored -> {
                        throw new AssertionError("Object cannot create a BK group");
                    }));
            var commit = KafkaSpeculativeCommitV1.assign(plan, assigned.baseOffset(), assigned.endOffsetExclusive());
            var ticket = tracker.assignPosition(reservation, commit);
            var before = snapshot();
            var txn = before.transactionState().apply(commit);
            long end = commit.endOffsetExclusive();
            var state = new KafkaObjectNativeStateV1(
                    before.committedProducerState().apply(commit),
                    txn,
                    before.leaderEpochIndex().observe(fence.kafkaLeaderEpoch(), commit.startOffset()),
                    end,
                    Math.min(end, txn.firstUnstableOffset(end).orElse(end)));
            return new Candidate(this, ticket, commit, state, raw);
        }

        void append(Fixture f, CanonicalBytes raw) {
            var candidate = prepare(f, raw);
            // Every partition has its own tracker; the shared path is also the one-member physical path.
            f.pipeline.writeResolveAndInstallShared(
                    f.plan(List.of(candidate), 0),
                    List.of(new KafkaNwg1ObjectPipelineV1.SharedMember(
                            candidate.ticket, candidate.commit, candidate.nativeState, tracker)),
                    2);
            assertThat(bridge.publishNext(() -> {})).isPresent();
        }
    }

    static final class Fixture implements AutoCloseable {
        final AsyncOxiaClient oxia;
        final FaultMetadata metadata;
        final FaultProvider provider;
        final KafkaObjectAuthorizationV1 authority;
        final NwgHttpCounts http = new NwgHttpCounts();
        final WalCheckpointPublisher initialPublisher;
        WalCheckpointPublisher publisher;
        final List<WalCheckpointPublisher> publishers = new ArrayList<>();
        WalRunRootRecord root;
        WalRunObjectSession session;
        Nwg1VerificationContextV1 verification;
        KafkaNwg1ObjectPipelineV1 pipeline;
        volatile Runnable beforeNativeValidation;
        final Map<String, byte[]> witnesses = new HashMap<>();
        final Map<Sha256Digest, WalRunObjectSession> sources = new HashMap<>();
        final Map<Sha256Digest, WalRunRootRecord> roots = new HashMap<>();

        Fixture(int maxDebt) throws Exception {
            this(maxDebt, false);
        }

        Fixture(int maxDebt, boolean versioned) throws Exception {
            this(maxDebt, versioned, null);
        }

        Fixture(int maxDebt, WalCheckpointPolicy policy) throws Exception {
            this(maxDebt, false, policy);
        }

        Fixture(int maxDebt, boolean versioned, WalCheckpointPolicy policy) throws Exception {
            this(maxDebt, versioned, policy, 4);
        }

        Fixture(int maxDebt, boolean versioned, WalCheckpointPolicy policy, int recoveryConcurrency) throws Exception {
            this(maxDebt, versioned, policy, recoveryConcurrency, null, 1);
        }

        Fixture(
                int maxDebt,
                boolean versioned,
                WalCheckpointPolicy policy,
                int recoveryConcurrency,
                KmsCellSession sharedKms,
                long epoch)
                throws Exception {
            this(maxDebt, versioned, policy, recoveryConcurrency, sharedKms, epoch, false);
        }

        Fixture(
                int maxDebt,
                boolean versioned,
                WalCheckpointPolicy policy,
                int recoveryConcurrency,
                KmsCellSession sharedKms,
                long epoch,
                boolean createRunKey)
                throws Exception {
            String attempt = "t4" + UUID.randomUUID().toString().replace("-", "");
            oxia = OxiaClientBuilder.create(System.getProperty("nereus.nsip1.object.oxia"))
                    .namespace("default")
                    .requestTimeout(Duration.ofSeconds(10))
                    .asyncClient()
                    .get();
            metadata = new FaultMetadata(new OxiaCanonicalControlMetadataStore(oxia, "/nsip1/" + attempt, 7));
            var client = S3Client.builder()
                    .endpointOverride(URI.create(System.getProperty("nereus.nsip1.object.endpoint")))
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create("nereus-m3-evidence", "nereus-m3-evidence-secret")))
                    .region(Region.US_EAST_1)
                    .forcePathStyle(true)
                    .overrideConfiguration(c -> c.addExecutionInterceptor(http))
                    .build();
            client.createBucket(b -> b.bucket(attempt));
            if (versioned) {
                client.putBucketVersioning(b -> b.bucket(attempt)
                        .versioningConfiguration(v ->
                                v.status(software.amazon.awssdk.services.s3.model.BucketVersioningStatus.ENABLED)));
            }
            provider = new FaultProvider(
                    new S3C1ObjectProviderTransport(client, attempt, "minio/RELEASE.2025-09-07T16-13-09Z", true, 100));
            var old = StorageObjectNwkcp1BackendV1Test.root();
            try (var newRunKey =
                    createRunKey ? sharedKms.beginNewRunKey(new RunKeyCacheIdentity(old.shardId(), epoch)) : null) {
                root = new WalRunRootRecord(
                        old.shardId(),
                        epoch,
                        epoch == 1 ? old.walRunSessionId() : new com.nereusstream.domain.identity.Id128(31, epoch),
                        old.openedAtMillis(),
                        old.protocolCellIdentity(),
                        old.providerScopeId(),
                        old.formatContract(),
                        old.nwg1AdmissionCaps(),
                        old.bounds(),
                        policy == null ? old.checkpointPolicy() : policy,
                        new ObjectProviderRootConfiguration(
                                ObjectProviderAccessProfile.C1_SINGLE_PUT_SINGLE_RANGE_STRONG_LIST,
                                S3C1ObjectProviderTransport.ADAPTER_VERSION,
                                "canonical-key-v1",
                                attempt,
                                ProviderProofMode.NONE,
                                0,
                                1024 * 1024,
                                1024 * 1024,
                                4096,
                                1,
                                100,
                                Sha256Digest.hash(CanonicalBytes.copyOf(
                                        ("minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e"
                                                        + "2960462bd8936e/"
                                                        + provider.capabilities())
                                                .getBytes(java.nio.charset.StandardCharsets.UTF_8)))),
                        withConcurrency(old.recoveryEnvelope(), recoveryConcurrency),
                        newRunKey == null ? old.wrappedRunKey() : newRunKey.wrappedRunKey(),
                        Optional.empty());
                var lifecycle = new WalRunLifecycleManager(metadata);
                var rootKey = WalRunControlKeys.rootKey(7, epoch);
                var created = newRunKey == null
                        ? lifecycle.createRootAndInitializePointer(rootKey, root)
                        : lifecycle.createRootAndInitializePointer(rootKey, root, newRunKey);
                var rawProvider = new C1ObjectProviderSession(
                        provider,
                        root.providerScopeId(),
                        attempt,
                        1024 * 1024,
                        root.nwg1AdmissionCaps().maxDirectoryPrefixBytes());
                session = newRunKey == null
                        ? WalRunObjectSession.openNew(
                                created.ownerAuthority().orElseThrow(),
                                rawProvider,
                                sharedKms != null
                                        ? sharedKms
                                        : new KmsCellSession(
                                                new StorageObjectNwkcp1BackendV1Test.FakeKmsTransport(),
                                                root.providerScopeId(),
                                                "kms/cell-a",
                                                2,
                                                new SecureRandom()),
                                () -> 0)
                        : WalRunObjectSession.openNew(
                                created.ownerAuthority().orElseThrow(), rawProvider, newRunKey, () -> 0);
            }
            verification = new Nwg1VerificationContextV1(
                    root.protocolCellIdentity(),
                    root.providerScopeId().digest().bytes().toByteArray(),
                    session.rootSha256().bytes().toByteArray(),
                    Nwg1EnvelopeV1.decode(root.wrappedRunKey().framedBytes().toByteArray()),
                    (binding, kind, version) ->
                            witnesses.get(java.util.HexFormat.of().formatHex(binding)),
                    new Nwg1VerificationContextV1.NativePayloadVerifier() {
                        @Override
                        public Nwg1VerificationContextV1.NativeCoverage validateKafka(
                                byte[] raw, long partition, long leader, long start, long end) {
                            var hook = beforeNativeValidation;
                            beforeNativeValidation = null;
                            if (hook != null) {
                                hook.run();
                            }
                            var batch = KafkaNativeAssignedRecordBatchV1.validate(
                                    KafkaRawAssignedRecordBatchFactsV1.parse(CanonicalBytes.copyOf(raw)));
                            new KafkaNativeProtocolBatchAdapterV1().protocolDelta(batch);
                            if (batch.baseOffset() != start
                                    || batch.endOffsetExclusive() != end
                                    || batch.partitionLeaderEpoch() != leader) {
                                throw new IllegalArgumentException("native Object frame identity differs");
                            }
                            return new Nwg1VerificationContextV1.NativeCoverage(start, end);
                        }
                    },
                    0,
                    0);
            sources.put(session.rootSha256(), session);
            roots.put(session.rootSha256(), root);
            authority = new KafkaObjectAuthorizationV1(
                    metadata, 7, new KafkaObjectAuthorizationV1.Bounds(128, maxDebt, 1024 * 1024, 1024));
            var physical = new KafkaObjectPhysicalFrontiersV1(session.rootSha256());
            var publisher = new WalCheckpointPublisher(
                    metadata,
                    WalRunControlKeys.checkpointHeadKey(7, epoch),
                    WalRunControlKeys.checkpointPagePrefix(7, epoch),
                    root,
                    WalCheckpointHeadV1.empty(session.rootSha256(), epoch, 1),
                    session);
            initialPublisher = publisher;
            this.publisher = publisher;
            publishers.add(publisher);
            publisher.initializeHead();
            pipeline = new KafkaNwg1ObjectPipelineV1(
                    root,
                    session,
                    verification,
                    physical,
                    new KafkaObjectCompletionTrackerV1(8, 65536, 6),
                    publisher,
                    authority);
        }

        KafkaPartitionFenceV1 fence(int partition, long owner, int leader) {
            var original = ObjectKafkaTestFixtures.runBinding().topicIncarnation();
            var incarnation = partition == 0
                    ? original
                    : new com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity(
                            new com.nereusstream.domain.identity.KafkaTopicId(
                                    new com.nereusstream.domain.identity.Id128(17, partition)),
                            new com.nereusstream.domain.protocol.KafkaTopicName("shared-partition-" + partition));
            var binding = DeterministicTopicIdsV1.deriveBindingId(root.protocolCellIdentity(), incarnation);
            var fence = new KafkaPartitionFenceV1(
                    binding,
                    incarnation,
                    partition,
                    11,
                    DeterministicTopicIdsV1.deriveStorageEpochId(binding, 0),
                    owner,
                    leader);
            witnesses.put(
                    binding.digest().toHex(),
                    Sha256Digest.hash(CanonicalBytes.copyOf(
                                    fence.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8)))
                            .bytes()
                            .toByteArray());
            return fence;
        }

        Partition partition(int partition, long owner, int leader) {
            var fence = fence(partition, owner, leader);
            authority.open(fence, 0, Optional.empty());
            return new Partition(
                    this, fence, KafkaCoherentCommitCoordinatorV1.bootstrapObject(fence, 0, ignored -> {}), null);
        }

        Nbke2RunBindingV1 run(KafkaPartitionFenceV1 fence) {
            return new Nbke2RunBindingV1(
                    fence.bindingId(),
                    fence.topicIncarnation(),
                    fence.partitionId(),
                    fence.storageEpochId(),
                    fence.ownerEpoch(),
                    fence.kafkaLeaderEpoch(),
                    root.providerScopeId(),
                    new StorageRunId(root.walRunSessionId()));
        }

        KafkaObjectAuthorizedRecoveryV1.Result recover(KafkaObjectAuthorizationV1.ClosedHistory closed)
                throws IOException {
            // A fresh authority has no local pending candidates or protocol state. Shared physical service stays alive.
            var fresh =
                    new KafkaObjectAuthorizationV1(metadata, 7, closed.head().bounds());
            return KafkaObjectAuthorizedRecoveryV1.recover(
                    fresh,
                    fresh.readClosed(closed.head().fence()),
                    run(closed.head().fence()),
                    session.rootSha256(),
                    grant -> new KafkaObjectAuthorizedRecoveryV1.Source(
                            sources.get(grant.locator().extent().walRunRootSha()),
                            new Nwg1VerificationContextV1(
                                    root.protocolCellIdentity(),
                                    root.providerScopeId().digest().bytes().toByteArray(),
                                    sources.get(grant.locator().extent().walRunRootSha())
                                            .rootSha256()
                                            .bytes()
                                            .toByteArray(),
                                    Nwg1EnvelopeV1.decode(
                                            roots.get(grant.locator().extent().walRunRootSha())
                                                    .wrappedRunKey()
                                                    .framedBytes()
                                                    .toByteArray()),
                                    (binding, kind, version) -> Sha256Digest.hash(CanonicalBytes.copyOf(grant.fence()
                                                    .toString()
                                                    .getBytes(java.nio.charset.StandardCharsets.UTF_8)))
                                            .bytes()
                                            .toByteArray(),
                                    verification.nativePayloadVerifier(),
                                    0,
                                    0)));
        }

        Partition takeover(KafkaObjectAuthorizationV1.ClosedHistory closed, long owner, int leader) throws IOException {
            var recovered = recover(closed);
            var fence = fence(closed.head().fence().partitionId(), owner, leader);
            authority.open(fence, closed.head().endOffset(), Optional.of(closed));
            return new Partition(
                    this,
                    fence,
                    KafkaCoherentCommitCoordinatorV1.bootstrapObjectRecovered(
                            fence,
                            0,
                            closed.head().endOffset(),
                            closed.head().endOffset(),
                            recovered.state(),
                            recovered.tail(),
                            ignored -> {}),
                    recovered.tail());
        }

        com.nereusstream.kafka.bookkeeper.object.read.KafkaObjectWalM4ReaderV1.ReadResult readAtLso(
                Partition partition, long end) {
            return readAtIsolation(
                    partition, end, com.nereusstream.kafka.bookkeeper.protocol.KafkaReadIsolationV1.READ_COMMITTED);
        }

        com.nereusstream.kafka.bookkeeper.object.read.KafkaObjectWalM4ReaderV1.ReadResult readAtIsolation(
                Partition partition,
                long end,
                com.nereusstream.kafka.bookkeeper.protocol.KafkaReadIsolationV1 isolation) {
            var snapshot = partition.snapshot();
            var selector = new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector(
                    com.nereusstream.kafka.bookkeeper.object.read.KafkaObjectBindingReadAdapterV1.bindingIdentity(
                            snapshot),
                    session.rootSha256(),
                    partition.fence.ownerEpoch(),
                    1,
                    1,
                    com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode.PREFERRED_ONLY,
                    com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.AdmissionState.ADMITTING,
                    Optional.empty(),
                    new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding(
                            1,
                            Sha256Digest.hash(metadata.get(authority.headKey(partition.fence))
                                    .orElseThrow())),
                    List.of(),
                    List.of());
            var pool = new com.nereusstream.storage.object.read.BindingReadHazardPoolV1(8, 4);
            var protection = new KafkaObjectSourceProtectionTrackerV1(
                    KafkaObjectAuthorizationV1.binding(partition.fence), session.rootSha256());
            var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
            try {
                var reader = new com.nereusstream.kafka.bookkeeper.object.read.KafkaObjectWalM4ReaderV1(
                        snapshot,
                        selector,
                        protection,
                        (locator, start, stop) -> {
                            try {
                                var grant = authority.read(partition.fence).orElseThrow().grants().stream()
                                        .filter(g -> g.locator().equals(locator))
                                        .findFirst()
                                        .orElseThrow();
                                var output = new java.io.ByteArrayOutputStream();
                                var context = new Nwg1VerificationContextV1(
                                        root.protocolCellIdentity(),
                                        root.providerScopeId().digest().bytes().toByteArray(),
                                        session.rootSha256().bytes().toByteArray(),
                                        verification.envelope(),
                                        (binding, kind, version) -> Sha256Digest.hash(
                                                        CanonicalBytes.copyOf(grant.fence()
                                                                .toString()
                                                                .getBytes(java.nio.charset.StandardCharsets.UTF_8)))
                                                .bytes()
                                                .toByteArray(),
                                        verification.nativePayloadVerifier(),
                                        0,
                                        0);
                                session.readRoutineNwg1AppendUnit(
                                        grant.object(), context, locator.firstDirectoryRow(), (frame, payload) -> {
                                            byte[] raw = new byte[payload.remaining()];
                                            payload.get(raw);
                                            output.writeBytes(raw);
                                        });
                                return java.util.concurrent.CompletableFuture.completedFuture(
                                        new com.nereusstream.kafka.bookkeeper.object.read.KafkaObjectWalM4ReaderV1
                                                .ValidatedRange(
                                                locator, start, stop, CanonicalBytes.copyOf(output.toByteArray())));
                            } catch (Exception failure) {
                                return java.util.concurrent.CompletableFuture.failedFuture(failure);
                            }
                        },
                        pool,
                        executor);
                return reader.read(0, end, isolation).join();
            } finally {
                executor.shutdownNow();
            }
        }

        Nwkcp1EncodedObjectV1 protocolObject(Partition partition) {
            var snapshot = partition.snapshot();
            long end = snapshot.root().frontiers().durableEndOffset();
            var common = new com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointStateV1(
                    new com.nereusstream.kafka.bookkeeper.checkpoint.KafkaRecoveryCheckpointVectorV1(
                            run(partition.fence), end, end, end, end),
                    snapshot.committedProducerState(),
                    snapshot.transactionState(),
                    snapshot.leaderEpochIndex());
            String prefix = root.providerConfiguration().exclusiveNamespacePrefix() + "/runs/" + root.shardRunEpoch();
            return Nwkcp1CodecV1.encode(prefix, new Nwkcp1ObjectV1(session.rootSha256(), List.of(common)));
        }

        private static RecoveryEnvelopeLimits withConcurrency(RecoveryEnvelopeLimits old, int concurrency) {
            return new RecoveryEnvelopeLimits(
                    old.maxLiveRoots(),
                    old.maxPredecessorRuns(),
                    old.maxListPages(),
                    old.maxListedKeys(),
                    old.maxListedKeyBytes(),
                    old.maxHeadRequests(),
                    old.maxRangeGetRequests(),
                    old.maxFullGetRequests(),
                    old.maxCanonicalBodyBytes(),
                    old.maxDecodedContexts(),
                    old.maxDecodedFrames(),
                    old.maxDecodedCommitSets(),
                    old.maxWorkingMemoryBytes(),
                    concurrency,
                    old.maxRetryAttempts(),
                    old.maxWallTimeNanos());
        }

        void rollover(Partition partition) throws IOException {
            var snapshot = partition.snapshot();
            long end = snapshot.root().frontiers().durableEndOffset();
            var common = new com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointStateV1(
                    new com.nereusstream.kafka.bookkeeper.checkpoint.KafkaRecoveryCheckpointVectorV1(
                            run(partition.fence), end, end, end, end),
                    snapshot.committedProducerState(),
                    snapshot.transactionState(),
                    snapshot.leaderEpochIndex());
            String prefix = root.providerConfiguration().exclusiveNamespacePrefix() + "/runs/" + root.shardRunEpoch();
            var backend = new StorageObjectNwkcp1BackendV1(session, metadata, prefix);
            var store = new ObjectKafkaProtocolCheckpointStoreV1(
                    prefix,
                    new KafkaNwkcp1WalRunContextV1(session.rootSha256(), run(partition.fence)),
                    partition.fence.ownerEpoch(),
                    backend);
            store.publish(common).toCompletableFuture().join();
            session.stopAdmission(WalRunRuntime.StopReason.OWNER_REQUEST);
            session.sealRuntime();
            var lifecycle = new WalRunLifecycleManager(metadata);
            var proof = pipeline.publishPhysicalSeal(lifecycle);
            var binding = store.terminalize(proof).toCompletableFuture().join();
            var predecessorSession = session;
            lifecycle = new WalRunLifecycleManager(
                    metadata, store, predecessor -> predecessorSession.terminalLineageProtocolObjectReader());
            var old = root;
            long epoch = old.shardRunEpoch() + 1;
            root = new WalRunRootRecord(
                    old.shardId(),
                    epoch,
                    new com.nereusstream.domain.identity.Id128(31, epoch),
                    old.openedAtMillis(),
                    old.protocolCellIdentity(),
                    old.providerScopeId(),
                    old.formatContract(),
                    old.nwg1AdmissionCaps(),
                    old.bounds(),
                    old.checkpointPolicy(),
                    new ObjectProviderRootConfiguration(
                            old.providerConfiguration().accessProfile(),
                            old.providerConfiguration().adapterVersion(),
                            old.providerConfiguration().canonicalizerVersion(),
                            old.providerConfiguration().exclusiveNamespacePrefix() + "r" + epoch,
                            old.providerConfiguration().proofMode(),
                            old.providerConfiguration().proofTokenHardCap(),
                            old.providerConfiguration().maxObjectBodyBytes(),
                            old.providerConfiguration().maxSinglePutBytes(),
                            old.providerConfiguration().maxSingleRangeReadBytes(),
                            old.providerConfiguration().maxPrefixSegmentsPerExtent(),
                            old.providerConfiguration().maxListPageKeys(),
                            old.providerConfiguration().capabilityReceiptSha256()),
                    old.recoveryEnvelope(),
                    new StorageObjectNwkcp1BackendV1Test.FakeKmsTransport()
                            .wrap("kms/cell-a", new SecureRandom().generateSeed(32)),
                    Optional.of(new com.nereusstream.storage.object.control.WalRunPredecessor(
                            proof.root(), proof.sealKey(), proof.sealSha256(), Optional.of(binding))));
            var seal = com.nereusstream.storage.object.control.WalRunControlCodec.decodeSeal(
                    metadata.get(proof.sealKey()).orElseThrow());
            var created = lifecycle.publishSuccessorAndAdvanceWithOwnerAuthority(
                    WalRunControlKeys.pointerKey(7),
                    new com.nereusstream.storage.object.control.CurrentWalRunPointer(proof.root()),
                    proof.sealKey(),
                    seal,
                    WalRunControlKeys.rootKey(7, epoch),
                    root);
            session = WalRunObjectSession.openNew(
                    created.ownerAuthority().orElseThrow(),
                    new C1ObjectProviderSession(
                            provider,
                            root.providerScopeId(),
                            root.providerConfiguration().exclusiveNamespacePrefix(),
                            1024 * 1024,
                            root.nwg1AdmissionCaps().maxDirectoryPrefixBytes()),
                    new KmsCellSession(
                            new StorageObjectNwkcp1BackendV1Test.FakeKmsTransport(),
                            root.providerScopeId(),
                            "kms/cell-a",
                            2,
                            new SecureRandom()),
                    () -> 0);
            sources.put(session.rootSha256(), session);
            roots.put(session.rootSha256(), root);
            verification = new Nwg1VerificationContextV1(
                    root.protocolCellIdentity(),
                    root.providerScopeId().digest().bytes().toByteArray(),
                    session.rootSha256().bytes().toByteArray(),
                    Nwg1EnvelopeV1.decode(root.wrappedRunKey().framedBytes().toByteArray()),
                    verification.ownerWitnessProvider(),
                    verification.nativePayloadVerifier(),
                    0,
                    0);
            var publisher = new WalCheckpointPublisher(
                    metadata,
                    WalRunControlKeys.checkpointHeadKey(7, epoch),
                    WalRunControlKeys.checkpointPagePrefix(7, epoch),
                    root,
                    WalCheckpointHeadV1.empty(session.rootSha256(), epoch, 1),
                    session);
            this.publisher = publisher;
            publishers.add(publisher);
            publisher.initializeHead();
            pipeline = new KafkaNwg1ObjectPipelineV1(
                    root,
                    session,
                    verification,
                    new KafkaObjectPhysicalFrontiersV1(session.rootSha256()),
                    new KafkaObjectCompletionTrackerV1(8, 65536, 6),
                    publisher,
                    authority);
            pipeline.bindTracker(partition.tracker);
        }

        KafkaNwg1ObjectPipelineV1.SharedWriteResult writeShared(List<Candidate> candidates, int lane, long now) {
            return pipeline.writeResolveAndInstallShared(
                    plan(candidates, lane),
                    candidates.stream()
                            .map(candidate -> new KafkaNwg1ObjectPipelineV1.SharedMember(
                                    candidate.ticket,
                                    candidate.commit,
                                    candidate.nativeState,
                                    candidate.partition.tracker))
                            .toList(),
                    now);
        }

        void verifyFreshUnwrap(RoundTripKmsTransport transport, KafkaNwg1ObjectPipelineV1.SharedWriteResult published)
                throws IOException {
            var identity = published.identity();
            byte[] body;
            try (var stored = provider.delegate.get(identity.key(), Optional.empty())) {
                body = stored.body().readAllBytes();
            }
            assertThat(body.length).isEqualTo(identity.bodyLength());
            assertThat(Sha256Digest.hash(CanonicalBytes.copyOf(body))).isEqualTo(identity.bodySha256());
            var envelope = verification.envelope();
            var rootAuthority = new Nwg1RootAuthorityV1(
                    verification.exactNpc1(),
                    Nwg1CommitmentsV1.protocolCell(verification.exactNpc1()),
                    verification.cellProviderScopeId(),
                    verification.walRunRootSha256(),
                    envelope.framedBytes(),
                    Nwg1CommitmentsV1.wrappedEnvelope(envelope));
            var leaf = ObjectWalLeafKeyV1.parseFull(root.providerConfiguration(), identity.key())
                    .relativeKey()
                    .getBytes(StandardCharsets.US_ASCII);
            try (var reader =
                    new KmsCellSession(transport, root.providerScopeId(), "kms/cell-a", 1, new SecureRandom())) {
                var verified = reader.verifyNwg1(
                        new RunKeyCacheIdentity(root.shardId(), root.shardRunEpoch()),
                        root.wrappedRunKey(),
                        Nwg1VerificationPathV1.FULL_BODY_RECONCILIATION,
                        rootAuthority,
                        verification,
                        leaf,
                        CanonicalBytes.copyOf(body),
                        0);
                assertThat(verified.decodedFrames()).isNotEmpty();
                assertThat(reader.unwrapCalls()).isOne();
            }
        }

        GroupEncodingPlanV1 plan(List<Candidate> candidates, int lane) {
            var contexts = new ArrayList<Nwg1DirectoryV1.BindingContext>();
            var units = new ArrayList<Nwg1DirectoryV1.KafkaAppendUnit>();
            var frames = new ArrayList<GroupEncodingPlanV1.PlannedFrame>();
            long total = 0;
            for (int i = 0; i < candidates.size(); i++) {
                var c = candidates.get(i);
                var fence = c.commit.expectedFence();
                byte[] witness = witnesses.get(fence.bindingId().digest().toHex());
                // One topic Binding context may cover multiple partitions with this same Owner witness.
                contexts.add(new Nwg1DirectoryV1.BindingContext(
                        fence.bindingId().digest().bytes().toByteArray(),
                        fence.storageEpochId().digest().bytes().toByteArray(),
                        Nwg1CommitmentsV1.ownerFence(1, 1, witness),
                        TopicIncarnationIdentityCodecV1.encode(fence.topicIncarnation())
                                .toByteArray(),
                        1,
                        1,
                        1,
                        1,
                        1,
                        1));
                byte[] raw = c.raw.toByteArray();
                total += raw.length;
                units.add(new Nwg1DirectoryV1.KafkaAppendUnit(
                        i,
                        i,
                        1,
                        fence.partitionId(),
                        fence.kafkaLeaderEpoch(),
                        c.commit.startOffset(),
                        c.commit.endOffsetExclusive(),
                        KafkaNwg1ObjectPipelineV1.commitSetId(c.commit),
                        KafkaNwg1ObjectPipelineV1.storageAttemptId(c.ticket),
                        Sha256Digest.hash(c.raw).bytes().toByteArray()));
                frames.add(new GroupEncodingPlanV1.PlannedFrame(
                        i, raw, raw, c.commit.startOffset(), c.commit.endOffsetExclusive(), 0, 0));
            }
            return new GroupEncodingPlanV1(
                    1,
                    7,
                    root.shardRunEpoch(),
                    lane,
                    root.formatContract().packingPolicyCatalogVersion(),
                    total,
                    0,
                    0,
                    10,
                    Nwg1CommitmentsV1.protocolCell(verification.exactNpc1()),
                    root.providerScopeId().digest().bytes().toByteArray(),
                    session.rootSha256().bytes().toByteArray(),
                    Nwg1CommitmentsV1.wrappedEnvelope(verification.envelope()),
                    contexts,
                    units,
                    frames);
        }

        @Override
        public void close() throws Exception {
            for (var publisher : publishers) {
                publisher.closeAfterDrain();
            }
            for (var source : sources.values()) {
                source.close();
            }
            provider.delegate.close();
            oxia.close();
        }
    }

    static final class FaultMetadata implements CanonicalControlMetadataStore {
        final CanonicalControlMetadataStore delegate;
        Runnable beforeGrant;
        String beforeKey;
        Runnable afterGrant;
        String failReadKey;
        volatile Runnable beforePhysicalPage;
        volatile Runnable beforePhysicalHead;
        volatile Runnable afterPhysicalHead;
        volatile Runnable beforeSeal;
        volatile Runnable afterSeal;
        volatile String failPhysicalReadKey;
        final List<PhysicalPage> physicalPages = java.util.Collections.synchronizedList(new ArrayList<>());
        final List<PhysicalHead> physicalHeads = java.util.Collections.synchronizedList(new ArrayList<>());

        record PhysicalPage(String key, CanonicalBytes value) {}

        record PhysicalHead(Optional<CanonicalBytes> expected, CanonicalBytes candidate) {}

        FaultMetadata(CanonicalControlMetadataStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public Optional<CanonicalBytes> get(String key) {
            if (key.equals(failReadKey)) {
                failReadKey = null;
                throw new IllegalStateException("grant reconciliation read unavailable");
            }
            var result = delegate.get(key);
            if (key.equals(failPhysicalReadKey)) {
                failPhysicalReadKey = null;
                throw new IllegalStateException("physical checkpoint read unavailable");
            }
            return result;
        }

        @Override
        public ControlMutationOutcome putIfAbsent(String key, CanonicalBytes value) {
            if (key.contains("/checkpoint/pages/")) {
                physicalPages.add(new PhysicalPage(key, value));
                var hook = beforePhysicalPage;
                beforePhysicalPage = null;
                if (hook != null) {
                    hook.run();
                }
            }
            if (key.endsWith("/seal")) {
                var hook = beforeSeal;
                beforeSeal = null;
                if (hook != null) {
                    hook.run();
                }
            }
            var result = delegate.putIfAbsent(key, value);
            if (key.endsWith("/seal")) {
                var hook = afterSeal;
                afterSeal = null;
                if (hook != null) {
                    hook.run();
                    return ControlMutationOutcome.RESPONSE_UNKNOWN;
                }
            }
            return result;
        }

        @Override
        public ControlMutationOutcome compareAndSet(
                String key, Optional<CanonicalBytes> expected, CanonicalBytes value) {
            if (key.endsWith("/checkpoint/head")) {
                physicalHeads.add(new PhysicalHead(expected, value));
                var before = beforePhysicalHead;
                beforePhysicalHead = null;
                if (before != null) {
                    before.run();
                }
                var result = delegate.compareAndSet(key, expected, value);
                var after = afterPhysicalHead;
                afterPhysicalHead = null;
                if (after != null) {
                    after.run();
                    return ControlMutationOutcome.RESPONSE_UNKNOWN;
                }
                return result;
            }
            if (key.contains("/authorize/") && expected.isPresent()) {
                if (beforeGrant != null && (beforeKey == null || beforeKey.equals(key))) {
                    var hook = beforeGrant;
                    beforeGrant = null;
                    hook.run();
                }
                var result = delegate.compareAndSet(key, expected, value);
                if (afterGrant != null) {
                    var hook = afterGrant;
                    afterGrant = null;
                    hook.run();
                    return ControlMutationOutcome.RESPONSE_UNKNOWN;
                }
                return result;
            }
            return delegate.compareAndSet(key, expected, value);
        }
    }

    static final class FaultProvider implements ObjectProviderTransport {
        final S3C1ObjectProviderTransport delegate;
        volatile Runnable beforeNwgPut;
        volatile Runnable beforeFullGet;
        boolean losePut;
        boolean loseListResponse;
        boolean precreate;
        boolean precreateConflict;
        boolean dropCreationEvidence;
        long lastNwgBodyBytes;
        long putBytes;
        long getBytes;
        ConditionalCreateResponse lastResponse;
        int puts;
        int lists;
        int ranges;
        int fullGets;

        FaultProvider(S3C1ObjectProviderTransport delegate) {
            this.delegate = delegate;
        }

        @Override
        public ObjectProviderCapabilities capabilities() {
            return delegate.capabilities();
        }

        @Override
        public ConditionalCreateResult putIfAbsent(ObjectIdentity identity, InputStream body) throws IOException {
            return putIfAbsentWithEvidence(identity, body).outcome();
        }

        @Override
        public ConditionalCreateResponse putIfAbsentWithEvidence(ObjectIdentity identity, InputStream body)
                throws IOException {
            puts++;
            boolean nwg = identity.key().endsWith(".nwg");
            if (nwg) {
                lastNwgBodyBytes = identity.bodyLength();
                var hook = beforeNwgPut;
                beforeNwgPut = null;
                if (hook != null) {
                    hook.run();
                }
            }
            if (precreate || precreateConflict) {
                precreate = false;
                byte[] bytes = body.readAllBytes();
                byte[] existing = bytes.clone();
                if (precreateConflict) {
                    precreateConflict = false;
                    existing[existing.length - 1] ^= 1;
                }
                var existingIdentity = new ObjectIdentity(
                        identity.key(), existing.length, Sha256Digest.hash(CanonicalBytes.copyOf(existing)));
                delegate.putIfAbsentWithEvidence(
                        existingIdentity, counting(new ByteArrayInputStream(existing), nwg, true));
                body = new ByteArrayInputStream(bytes);
            }
            var result = delegate.putIfAbsentWithEvidence(identity, counting(body, nwg, true));
            if (nwg) {
                lastResponse = result;
            }
            if (losePut) {
                losePut = false;
                return ConditionalCreateResponse.outcome(ConditionalCreateResult.RESPONSE_UNKNOWN);
            }
            if (dropCreationEvidence && result.outcome() == ConditionalCreateResult.CREATED) {
                return ConditionalCreateResponse.outcome(result.outcome());
            }
            return result;
        }

        @Override
        public StreamingObject get(String key, Optional<CanonicalBytes> version) throws IOException {
            if (key.endsWith(".nwg")) {
                fullGets++;
            }
            var result = delegate.get(key, version);
            var hook = beforeFullGet;
            beforeFullGet = null;
            if (hook != null) {
                hook.run();
            }
            return new StreamingObject(
                    result.bodyLength(),
                    result.inclusiveStart(),
                    result.exclusiveEnd(),
                    result.immutableVersionToken(),
                    counting(result.body(), key.endsWith(".nwg"), false));
        }

        @Override
        public StreamingObject getRange(String key, long start, long end, Optional<CanonicalBytes> version)
                throws IOException {
            ranges++;
            return delegate.getRange(key, start, end, version);
        }

        @Override
        public ListPage list(String prefix, Optional<CanonicalBytes> token, int max) throws IOException {
            lists++;
            var result = delegate.list(prefix, token, max);
            if (loseListResponse) {
                loseListResponse = false;
                throw new IOException("actual LIST reply unavailable");
            }
            return result;
        }

        @Override
        public FailureKind classifyFailure(IOException failure) {
            return delegate.classifyFailure(failure);
        }

        private InputStream counting(InputStream input, boolean nwg, boolean upload) {
            return new FilterInputStream(input) {
                private void count(int n) {
                    if (n > 0 && nwg) {
                        if (upload) {
                            putBytes += n;
                        } else {
                            getBytes += n;
                        }
                    }
                }

                @Override
                public int read() throws IOException {
                    int value = in.read();
                    count(value < 0 ? 0 : 1);
                    return value;
                }

                @Override
                public int read(byte[] bytes, int offset, int length) throws IOException {
                    int n = in.read(bytes, offset, length);
                    count(n);
                    return n;
                }
            };
        }
    }

    static final class NwgHttpCounts implements ExecutionInterceptor {
        int puts;
        int heads;
        int gets;
        int ranges;

        @Override
        public void beforeTransmission(Context.BeforeTransmission context, ExecutionAttributes attributes) {
            var request = context.httpRequest();
            if (!request.encodedPath().endsWith(".nwg")) {
                return;
            }
            switch (request.method().name()) {
                case "PUT" -> puts++;
                case "HEAD" -> heads++;
                case "GET" -> {
                    if (request.firstMatchingHeader("Range").isPresent()) {
                        ranges++;
                    } else {
                        gets++;
                    }
                }
                default -> throw new AssertionError("unexpected NWG HTTP method");
            }
        }
    }
}
