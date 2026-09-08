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

package com.nereusstream.kafka.bookkeeper.compaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.KafkaTopicId;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.KafkaTopicName;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2AppendGroupDescriptorV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2CodecV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2DataV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunFooterV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunHeaderV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaBookKeeperNativeRootVerifierV2;
import com.nereusstream.kafka.bookkeeper.run.KafkaBookKeeperRunLifecycleV1;
import com.nereusstream.metadata.oxia.v2.compaction.OxiaKafkaRunRootAuthorityV2;
import com.nereusstream.metadata.oxia.v2.mutation.AsyncOxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.mutation.AuthorityRecord;
import com.nereusstream.metadata.oxia.v2.mutation.OxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMetadataNamespaceV2;
import com.nereusstream.metadata.oxia.v2.retention.OxiaQuotaTargetDeleteStoreV2;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
import com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationResultV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerAppendRequestV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerConfigurationV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2;
import com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2.Scope;
import com.nereusstream.storage.api.kafka.KafkaRunRootSnapshotV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootStateV1;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.bookkeeper.ImmutableRetainedStoragePayload;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNamespaceAuthorityV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateClientV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateSpecV2;
import com.nereusstream.storage.bookkeeper.RealBookKeeperCellSessionV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2;
import com.nereusstream.storage.object.gc.SyntheticDeleteAuthorityFixturesV2;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Native run-root transport and NBKE2 bytes; protocol-owner admission and GC eligibility remain fixtures. */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class KafkaBookKeeperRunRootsV2RealTest {
    private static final List<String> TOPICS = List.of("__consumer_offsets", "__transaction_state");

    @Test
    void nativeLifecyclePublishesSealedRootsAndDataForBothInternalTopics() throws Exception {
        for (int i = 0; i < TOPICS.size(); i++) {
            try (var f = new Fixture(1501 + i, TOPICS.get(i), null)) {
                var run = await(KafkaBookKeeperRunLifecycleV1.createActive(f.admitting, f.roots, f.runBinding(0), 0));
                assertThat(await(f.source.captureExactTarget(run.snapshot().handle()))
                                .exactTarget())
                        .isEmpty();
                f.writeData(run);
                await(run.drain());
                var sealed = await(run.seal(f.footer(
                                run.snapshot().runBinding(), 2, run.snapshot().nextEntryId())))
                        .root();
                assertThat(await(f.roots.openRoot(sealed.runId()))).contains(sealed);
                var next = await(run.createSuccessor(f.runBinding(1)));
                assertThat(next.snapshot().root().kafkaStartOffset()).isEqualTo(2);
                assertThat(await(f.roots.openRoot(next.snapshot().root().runId())))
                        .contains(next.snapshot().root());
                assertThat(f.record(sealed).initialLink())
                        .isEqualTo(f.stored(sealed).initialLink());
                f.verifyData(run.snapshot().handle());
                assertThat(f.tickets(sealed.ledgerIdentity())).isZero();
                assertThat(f.tickets(next.snapshot().handle().ledgerIdentity())).isZero();
                await(next.drain());
                await(next.seal(f.footer(
                        next.snapshot().runBinding(), 2, next.snapshot().nextEntryId())));
            }
        }
    }

    @Test
    void wrongNativeHeaderAndFooterCannotPublishAndExactRootsReconcileRejectedAttempts() throws Exception {
        try (var f = new Fixture(1503, "orders", null)) {
            var root = f.prepareSealedSource();
            var wrongHeader = new KafkaRunRootSnapshotV1(
                    root.bindingId(),
                    root.topicIncarnation(),
                    root.partitionId(),
                    root.storageEpochId(),
                    root.creatorOwnerEpoch(),
                    root.kafkaLeaderEpoch(),
                    root.providerScopeId(),
                    root.runId(),
                    root.ledgerIdentity(),
                    1,
                    OptionalLong.empty(),
                    KafkaRunRootStateV1.ACTIVE,
                    Optional.empty());
            assertThat(await(f.roots.createRoot(wrongHeader)).outcome())
                    .isEqualTo(ProviderMutationOutcomeV1.OUTCOME_UNKNOWN);
            assertThat(await(f.nativeClient.read(f.roots.nativeRootKey(root.runId()))))
                    .isEmpty();
            assertThat(f.tickets(root.ledgerIdentity())).isEqualTo(1);
            assertThat(await(f.roots.createRoot(root)).exactProof()).contains(root);
            assertThat(await(f.roots.createRoot(wrongHeader)).outcome())
                    .isEqualTo(ProviderMutationOutcomeV1.FENCED_OR_CONFLICT);
            assertThat(f.tickets(root.ledgerIdentity())).isZero();
            var wrongFooter = sealed(root, 3);
            assertThat(await(f.roots.sealRoot(root, wrongFooter)).outcome())
                    .isEqualTo(ProviderMutationOutcomeV1.OUTCOME_UNKNOWN);
            assertThat(await(f.roots.openRoot(root.runId()))).contains(root);
            assertThat(await(f.roots.sealRoot(root, sealed(root, 2))).exactProof())
                    .contains(sealed(root, 2));
            assertThat(await(f.roots.sealRoot(root, wrongFooter)).outcome())
                    .isEqualTo(ProviderMutationOutcomeV1.FENCED_OR_CONFLICT);
            assertThat(f.tickets(root.ledgerIdentity())).isZero();
        }
    }

    @Test
    void nativeSuccessorRaceKeepsOneChildAndTheWinningMetadataVersion() throws Exception {
        try (var f = new Fixture(1504, "orders", null)) {
            var faults = new Faults(f.nativeClient);
            var roots = f.faulted(faults);
            var run = await(KafkaBookKeeperRunLifecycleV1.createActive(f.admitting, roots, f.runBinding(0), 0));
            f.writeData(run);
            await(run.drain());
            var sealed = await(run.seal(f.footer(
                            run.snapshot().runBinding(), 2, run.snapshot().nextEntryId())))
                    .root();
            faults.holdKey = roots.nativeRootKey(sealed.runId());
            var older = run.createSuccessor(f.runBinding(1)).toCompletableFuture();
            try {
                faults.held.get(30, TimeUnit.SECONDS);
                assertThat(await(roots.openRoot(f.runBinding(1).runId()))).isEmpty();
                var winner = await(run.createSuccessor(f.runBinding(2)));
                var nativeWinner = await(f.nativeClient.read(roots.nativeRootKey(sealed.runId())))
                        .orElseThrow();
                assertThat(older).isNotDone();
                faults.release.run();
                assertThatThrownBy(() -> older.get(30, TimeUnit.SECONDS))
                        .hasRootCauseMessage("run-root mutation was not established exactly");
                assertThat(await(f.nativeClient.read(roots.nativeRootKey(sealed.runId()))))
                        .contains(nativeWinner);
                assertThat(await(roots.openRoot(f.runBinding(1).runId()))).isEmpty();
                assertThat(await(roots.openRoot(winner.snapshot().root().runId())))
                        .contains(winner.snapshot().root());
                for (var handle : f.handles.values()) {
                    assertThat(f.tickets(handle.ledgerIdentity())).isZero();
                }
                await(winner.drain());
                await(winner.seal(f.footer(
                        winner.snapshot().runBinding(), 2, winner.snapshot().nextEntryId())));
            } finally {
                if (faults.release != null) {
                    faults.release.run();
                }
            }
        }
    }

    @Test
    void nativePhysicalFencePreventsRootRecordPublication() throws Exception {
        try (var f = new Fixture(1505, "orders", null)) {
            var root = f.prepareSealedSource();
            var resource = f.record(root).resource();
            var before = await(f.route.read(resource.authorityKey())).orElseThrow();
            assertThat(await(f.route.compareAndSet(
                            Optional.of(before),
                            resource.authorityKey(),
                            M5TargetDeleteAuthorityCodecV1.encodeAuthority(
                                    SyntheticDeleteAuthorityFixturesV2.phases(resource)
                                            .get(1)))))
                    .isEqualTo(
                            com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome
                                    .APPLIED_EXACT);
            assertThat(await(f.roots.createRoot(root)).outcome())
                    .isEqualTo(ProviderMutationOutcomeV1.FENCED_OR_CONFLICT);
            assertThat(await(f.nativeClient.read(f.roots.nativeRootKey(root.runId()))))
                    .isEmpty();
            assertThat(await(f.nativeClient.read(f.roots.nativeGenesisKey()))).isEmpty();
        }
    }

    @Test
    void writeBeforeServerRestart() throws Exception {
        Files.createDirectories(checkpoint());
        for (int i = 0; i < TOPICS.size(); i++) {
            try (var f = new Fixture(1510 + i, TOPICS.get(i), null)) {
                var root = f.prepareSealedSource();
                var faults = new Faults(f.nativeClient);
                faults.loseGenesis = true;
                assertThat(await(f.faulted(faults).createRoot(root)).outcome())
                        .isEqualTo(ProviderMutationOutcomeV1.OUTCOME_UNKNOWN);
                assertThat(f.tickets(root.ledgerIdentity())).isEqualTo(1);
                var rootValue = await(f.nativeClient.read(f.roots.nativeRootKey(root.runId())))
                        .orElseThrow();
                var genesis =
                        await(f.nativeClient.read(f.roots.nativeGenesisKey())).orElseThrow();
                var authority = await(f.route.read(f.record(root).resource().authorityKey()))
                        .orElseThrow();
                Files.write(
                        checkpoint().resolve(TOPICS.get(i)),
                        List.of(
                                f.source.spec().encode().toHex(),
                                root.runId().value().toHex(),
                                Long.toString(root.ledgerIdentity().ledgerId()),
                                identity(rootValue),
                                identity(genesis),
                                authority.canonicalStoredSha256().toHex() + ":"
                                        + authority.metadataVersion().value().toHex()));
            }
        }
    }

    @Test
    void readAfterServerRestart() throws Exception {
        for (int i = 0; i < TOPICS.size(); i++) {
            var lines = Files.readAllLines(checkpoint().resolve(TOPICS.get(i)));
            try (var f = new Fixture(1510 + i, TOPICS.get(i), lines)) {
                var runId = new StorageRunId(
                        Id128.fromBytes(java.util.HexFormat.of().parseHex(lines.get(1))));
                var rootValue =
                        await(f.nativeClient.read(f.roots.nativeRootKey(runId))).orElseThrow();
                var genesis =
                        await(f.nativeClient.read(f.roots.nativeGenesisKey())).orElseThrow();
                assertThat(identity(rootValue)).isEqualTo(lines.get(3));
                assertThat(identity(genesis)).isEqualTo(lines.get(4));
                var actual = await(f.roots.openRoot(runId)).orElseThrow();
                assertThat(actual.ledgerIdentity().ledgerId()).isEqualTo(Long.parseLong(lines.get(2)));
                var authority = await(f.route.read(f.record(actual).resource().authorityKey()))
                        .orElseThrow();
                assertThat(authority.canonicalStoredSha256().toHex() + ":"
                                + authority.metadataVersion().value().toHex())
                        .isEqualTo(lines.get(5));
                assertThat(f.tickets(actual.ledgerIdentity())).isEqualTo(1);
                assertThat(await(f.roots.createRoot(actual)).exactProof()).contains(actual);
                assertThat(f.tickets(actual.ledgerIdentity())).isZero();
                assertThat(await(f.nativeClient.read(f.roots.nativeRootKey(runId))))
                        .contains(rootValue);
                assertThat(await(f.nativeClient.read(f.roots.nativeGenesisKey())))
                        .contains(genesis);
                var sealed = sealed(actual, 2);
                assertThat(await(f.roots.sealRoot(actual, sealed)).exactProof()).contains(sealed);
                assertThat(await(f.roots.openRoot(runId))).contains(sealed);
                var handle = new RunLedgerHandleV1(
                        actual.providerScopeId(),
                        actual.runId(),
                        actual.ledgerIdentity(),
                        f.verifier.capabilitySha256());
                f.verifyData(handle);
            }
        }
    }

    private static Path checkpoint() {
        return Path.of(System.getProperty("nereus.m5.runroot.restartCheckpoint"));
    }

    private static String identity(AuthorityRecord record) {
        return Sha256Digest.hash(record.storedBytes()).toHex() + ":" + record.versionId();
    }

    private static <T> T await(CompletionStage<T> stage) throws Exception {
        return stage.toCompletableFuture().get(30, TimeUnit.SECONDS);
    }

    private static KafkaRunRootSnapshotV1 sealed(KafkaRunRootSnapshotV1 root, long end) {
        return new KafkaRunRootSnapshotV1(
                root.bindingId(),
                root.topicIncarnation(),
                root.partitionId(),
                root.storageEpochId(),
                root.creatorOwnerEpoch(),
                root.kafkaLeaderEpoch(),
                root.providerScopeId(),
                root.runId(),
                root.ledgerIdentity(),
                root.kafkaStartOffset(),
                OptionalLong.of(end),
                KafkaRunRootStateV1.SEALED,
                root.predecessorRunId());
    }

    private static final class Fixture implements AutoCloseable {
        final long attempt;
        final Scope scope;
        final List<CanonicalBytes> bodies;
        final AsyncOxiaClient oxia;
        final OxiaConditionalClient nativeClient;
        final M5BookKeeperNamespaceAuthorityV2 backend;
        final PhysicalNamespaceAuthorityBindingV2 binding;
        final M5BookKeeperNativeCreateClientV2 source;
        final RealBookKeeperCellSessionV1 session;
        final KafkaBookKeeperNativeRootVerifierV2 verifier;
        final OxiaQuotaTargetDeleteStoreV2 route;
        final BookKeeperCellSession admitting;
        final OxiaKafkaRunRootAuthorityV2 roots;
        final Map<StorageRunId, RunLedgerHandleV1> handles = new ConcurrentHashMap<>();

        Fixture(long attempt, String topic, List<String> restore) throws Exception {
            this.attempt = attempt;
            var input = KafkaBookKeeperNativeCreateV2RealTest.nativeInput(false, attempt);
            var capability = input.layout().task().capability();
            bodies = input.plan().inputBatches().stream()
                    .map(KafkaCompactionRecordsV1.InputBatch::canonicalBody)
                    .toList();
            scope = new Scope(
                    new TopicBindingId(digest("native-root-binding/" + topic + attempt)),
                    new KafkaTopicIncarnationIdentity(
                            new KafkaTopicId(new Id128(attempt, 1)), new KafkaTopicName(topic)),
                    0,
                    new StorageEpochId(digest("native-root-storage/" + topic + attempt)),
                    capability.providerScopeId());
            var builder = OxiaClientBuilder.create(System.getProperty("nereus.m5.oxia.serviceAddress"));
            assertArtifact(builder.getClass(), "0ca719e6d11bd2ee2c2e7e94b42c6843e60f776bea12f7b5814cff9928e2e4c5");
            assertArtifact(
                    org.apache.bookkeeper.client.BookKeeper.class,
                    capability.clientArtifactSha256().toHex());
            oxia = builder.namespace("default")
                    .requestTimeout(Duration.ofSeconds(10))
                    .asyncClient()
                    .get(30, TimeUnit.SECONDS);
            nativeClient = new AsyncOxiaConditionalClient(oxia);
            String uri = System.getProperty("nereus.bookkeeper.metadataServiceUri");
            backend = M5BookKeeperNamespaceAuthorityV2.connect(uri, capability);
            var namespace = restore == null
                    ? await(OxiaPhysicalMetadataNamespaceV2.provision(oxia))
                    : await(OxiaPhysicalMetadataNamespaceV2.connect(
                            oxia, await(backend.readBinding()).orElseThrow().metadataNamespace()));
            binding = await(namespace.bind(backend));
            var spec = restore == null
                    ? M5BookKeeperNativeCreateSpecV2.of(
                            M5BookKeeperNativeCreateClientV2.discoverInstanceId(uri, capability),
                            digest("native-root-task/" + topic + attempt),
                            java.util.stream.IntStream.range(0, 3)
                                    .mapToObj(ordinal -> RunLedgerConfigurationV1.from(
                                            capability, new StorageRunId(new Id128(attempt, ordinal + 1))))
                                    .toList())
                    : M5BookKeeperNativeCreateSpecV2.decode(
                            CanonicalBytes.copyOf(java.util.HexFormat.of().parseHex(restore.get(0))));
            source = M5BookKeeperNativeCreateClientV2.connect(uri, capability, spec, binding);
            session = source.newSession();
            verifier = new KafkaBookKeeperNativeRootVerifierV2(source);
            route = await(namespace.openAuthorityRoute(backend, new Oxia09ExactMetadataTransactionStoreV1(oxia)));
            var quota = await(route.initialize(500_000_000));
            if (quota.head().capacityBytes() < 500_000_000) {
                assertThat(await(route.quota().expand(500_000_000))).isTrue();
            }
            roots = await(namespace.openKafkaRunRoots(
                    backend, new Oxia09ExactMetadataTransactionStoreV1(oxia), scope, verifier));
            admitting = new AdmittingSession(this);
        }

        OxiaKafkaRunRootAuthorityV2 faulted(OxiaConditionalClient client) {
            return new OxiaKafkaRunRootAuthorityV2(
                    client,
                    binding,
                    scope,
                    verifier,
                    new M5TargetDeleteMultiWriterGuardV2(new M5TargetDeleteAuthorityCoordinatorV1(route)));
        }

        Nbke2RunBindingV1 runBinding(int ordinal) {
            return new Nbke2RunBindingV1(
                    scope.bindingId(),
                    scope.topic(),
                    scope.partition(),
                    scope.storageEpoch(),
                    1,
                    1,
                    scope.providerScope(),
                    source.spec().configurations().get(ordinal).runId());
        }

        KafkaRunRootRecordV2 record(KafkaRunRootSnapshotV1 root) {
            return new KafkaRunRootRecordV2(
                    new PhysicalResourceIdV2.BookKeeperLedger(
                            binding.physicalNamespace(), root.ledgerIdentity().ledgerId()),
                    root,
                    root.state() == KafkaRunRootStateV1.SEALED,
                    Optional.empty());
        }

        KafkaRunRootRecordV2 stored(KafkaRunRootSnapshotV1 root) throws Exception {
            return KafkaRunRootRecordV2.decode(await(nativeClient.read(roots.nativeRootKey(root.runId())))
                    .orElseThrow()
                    .storedBytes());
        }

        CompletionStage<Void> admit(RunLedgerHandleV1 handle) {
            var resource = new PhysicalResourceIdV2.BookKeeperLedger(
                    binding.physicalNamespace(), handle.ledgerIdentity().ledgerId());
            return route.compareAndSet(
                            Optional.empty(),
                            resource.authorityKey(),
                            M5TargetDeleteAuthorityCodecV1.encodeAuthority(
                                    SyntheticDeleteAuthorityFixturesV2.phases(resource)
                                            .get(0)))
                    .thenAccept(outcome -> {
                        assertThat(outcome)
                                .isEqualTo(
                                        com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1
                                                .MutationOutcome.APPLIED_EXACT);
                        handles.put(handle.runId(), handle);
                    });
        }

        int tickets(BookKeeperLedgerIdentity ledger) throws Exception {
            var resource = new PhysicalResourceIdV2.BookKeeperLedger(binding.physicalNamespace(), ledger.ledgerId());
            return M5TargetDeleteAuthorityCodecV1.decodeAuthority(await(route.read(resource.authorityKey()))
                            .orElseThrow()
                            .canonicalStoredBytes())
                    .activeWriterTickets()
                    .size();
        }

        void writeData(KafkaBookKeeperRunLifecycleV1 run) throws Exception {
            for (int i = 0; i < bodies.size(); i++) {
                var reservation = run.reserveDataGroup(1);
                data(
                        run.snapshot().handle(),
                        run.snapshot().runBinding(),
                        reservation.firstEntryId(),
                        i,
                        bodies.get(i));
                run.completeDataGroup(reservation);
            }
        }

        void data(RunLedgerHandleV1 handle, Nbke2RunBindingV1 binding, long entry, long offset, CanonicalBytes body)
                throws Exception {
            var descriptor =
                    new Nbke2AppendGroupDescriptorV1(offset, offset + 1, entry, entry, Sha256Digest.hash(body));
            append(
                    handle,
                    entry,
                    Nbke2CodecV1.encode(
                            handle.ledgerIdentity().ledgerId(),
                            entry,
                            new Nbke2DataV1(
                                    binding,
                                    offset,
                                    0,
                                    0,
                                    1,
                                    new Id128(attempt, entry + 1),
                                    new Id128(attempt + 1, entry + 1),
                                    Optional.of(descriptor),
                                    body)));
        }

        Nbke2RunFooterV1 footer(Nbke2RunBindingV1 binding, long end, long entry) {
            return new Nbke2RunFooterV1(binding, end, entry + 1, -1, -1, 1, List.of());
        }

        KafkaRunRootSnapshotV1 prepareSealedSource() throws Exception {
            var binding = runBinding(0);
            var handle = await(admitting.createRunLedger(
                            source.spec().configurations().get(0)))
                    .exactProof()
                    .orElseThrow();
            append(
                    handle,
                    0,
                    Nbke2CodecV1.encode(
                            handle.ledgerIdentity().ledgerId(),
                            0,
                            new Nbke2RunHeaderV1(binding, 0, 1, verifier.capabilitySha256())));
            for (int i = 0; i < bodies.size(); i++) {
                data(handle, binding, i + 1, i, bodies.get(i));
            }
            long entry = bodies.size() + 1L;
            append(
                    handle,
                    entry,
                    Nbke2CodecV1.encode(handle.ledgerIdentity().ledgerId(), entry, footer(binding, 2, entry)));
            assertThat(await(session.closeRunLedger(handle)).exactProof()).isPresent();
            return new KafkaRunRootSnapshotV1(
                    scope.bindingId(),
                    scope.topic(),
                    scope.partition(),
                    scope.storageEpoch(),
                    1,
                    1,
                    scope.providerScope(),
                    handle.runId(),
                    handle.ledgerIdentity(),
                    0,
                    OptionalLong.empty(),
                    KafkaRunRootStateV1.ACTIVE,
                    Optional.empty());
        }

        void append(RunLedgerHandleV1 handle, long entry, byte[] bytes) throws Exception {
            var payload = ImmutableRetainedStoragePayload.copyOf(bytes);
            try {
                assertThat(await(session.appendExplicitEntry(new RunLedgerAppendRequestV1(handle, entry, payload)))
                                .exactProof())
                        .isPresent();
            } finally {
                assertThat(payload.release()).isTrue();
            }
        }

        void verifyData(RunLedgerHandleV1 handle) throws Exception {
            assertThat(await(session.openRunLedger(handle)).exactHandle()).contains(handle);
            for (int i = 0; i < bodies.size(); i++) {
                var entry = await(session.readExactEntry(handle, i + 1))
                        .exactEntry()
                        .orElseThrow();
                var frame = (Nbke2DataV1) Nbke2CodecV1.decode(
                        entry.payload().toByteArray(), handle.ledgerIdentity().ledgerId(), i + 1);
                assertThat(frame.rawAssignedRecordBatch()).isEqualTo(bodies.get(i));
            }
        }

        public void close() throws Exception {
            try {
                verifier.close();
            } finally {
                try {
                    await(session.closeAsync());
                } finally {
                    try {
                        source.close();
                    } finally {
                        try {
                            backend.close();
                        } finally {
                            oxia.close();
                        }
                    }
                }
            }
        }
    }

    private static final class AdmittingSession implements BookKeeperCellSession {
        private final Fixture f;

        AdmittingSession(Fixture fixture) {
            f = fixture;
        }

        public com.nereusstream.storage.api.bookkeeper.CellProviderScopeId providerScopeId() {
            return f.session.providerScopeId();
        }

        public com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1 capabilitySnapshot() {
            return f.session.capabilitySnapshot();
        }

        public CompletionStage<ProviderMutationResultV1<RunLedgerHandleV1>> createRunLedger(
                RunLedgerConfigurationV1 configuration) {
            return f.session
                    .createRunLedger(configuration)
                    .thenCompose(result -> result.exactProof().isPresent()
                            ? f.admit(result.exactProof().orElseThrow()).thenApply(ignored -> result)
                            : CompletableFuture.completedFuture(result));
        }

        public CompletionStage<com.nereusstream.storage.api.bookkeeper.RunLedgerOpenResultV1> openRunLedger(
                RunLedgerHandleV1 handle) {
            return f.session.openRunLedger(handle);
        }

        public CompletionStage<ProviderMutationResultV1<com.nereusstream.storage.api.bookkeeper.AppendQuorumProofV1>>
                appendExplicitEntry(RunLedgerAppendRequestV1 request) {
            return f.session.appendExplicitEntry(request);
        }

        public CompletionStage<com.nereusstream.storage.api.bookkeeper.RunLedgerReadResultV1> readExactEntry(
                RunLedgerHandleV1 handle, long entry) {
            return f.session.readExactEntry(handle, entry);
        }

        public CompletionStage<
                        ProviderMutationResultV1<com.nereusstream.storage.api.bookkeeper.RunLedgerRecoveryProofV1>>
                fenceAndRecoverRunLedger(RunLedgerHandleV1 handle) {
            return f.session.fenceAndRecoverRunLedger(handle);
        }

        public CompletionStage<ProviderMutationResultV1<com.nereusstream.storage.api.bookkeeper.RunLedgerCloseProofV1>>
                closeRunLedger(RunLedgerHandleV1 handle) {
            return f.session.closeRunLedger(handle);
        }

        public CompletionStage<Void> drain() {
            return f.session.drain();
        }

        public CompletionStage<Void> closeAsync() {
            return f.session.closeAsync();
        }
    }

    private static final class Faults implements OxiaConditionalClient {
        final OxiaConditionalClient nativeClient;
        final CompletableFuture<Void> held = new CompletableFuture<>();
        volatile String holdKey;
        volatile boolean loseGenesis;
        volatile boolean blocked;
        Runnable release;

        Faults(OxiaConditionalClient client) {
            nativeClient = client;
        }

        public CompletionStage<Optional<AuthorityRecord>> read(String key) {
            return blocked
                    ? CompletableFuture.failedFuture(new IllegalStateException("native root read delivery loss"))
                    : nativeClient.read(key);
        }

        public CompletionStage<Void> createIfAbsent(String key, CanonicalBytes bytes) {
            return nativeClient.createIfAbsent(key, bytes).thenCompose(ignored -> {
                if (loseGenesis && key.endsWith("/genesis-v2")) {
                    loseGenesis = false;
                    blocked = true;
                    return CompletableFuture.failedFuture(
                            new IllegalStateException("native genesis applied response loss"));
                }
                return CompletableFuture.completedFuture(null);
            });
        }

        public CompletionStage<Void> compareAndSet(String key, CanonicalBytes bytes, long version) {
            if (key.equals(holdKey)) {
                holdKey = null;
                var result = new CompletableFuture<Void>();
                var released = new java.util.concurrent.atomic.AtomicBoolean();
                release = () -> {
                    if (released.compareAndSet(false, true)) {
                        nativeClient.compareAndSet(key, bytes, version).whenComplete((ignored, failure) -> {
                            if (failure == null) {
                                result.complete(null);
                            } else {
                                result.completeExceptionally(failure);
                            }
                        });
                    }
                };
                held.complete(null);
                return result;
            }
            return nativeClient.compareAndSet(key, bytes, version);
        }
    }

    private static Sha256Digest digest(String text) {
        return SyntheticDeleteAuthorityFixturesV2.digest(text);
    }

    private static void assertArtifact(Class<?> type, String expected) throws Exception {
        var jar =
                Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI());
        assertThat(Sha256Digest.hash(CanonicalBytes.copyOf(Files.readAllBytes(jar)))
                        .toHex())
                .isEqualTo(expected);
    }
}
