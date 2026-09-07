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
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperCompactionTestSupportV2.Input;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperCompactionTestSupportV2.Store;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1;
import com.nereusstream.storage.bookkeeper.RealBookKeeperCellSessionV1;
import com.nereusstream.storage.bookkeeper.RealBookKeeperClientConfigurationV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexKind;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PublicationOutcome;
import com.nereusstream.storage.object.read.control.M4ReadControlCoordinatorV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.SimpleRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Real locked BK IO and native Kafka RecordBatch encodings; the control/source authority fixture is synthetic. */
class KafkaSealedBookKeeperDescriptorV2RealTest {
    private static final AtomicLong ATTEMPTS = new AtomicLong(100);
    private static BookKeeper client;

    @BeforeAll
    static void connect() throws Exception {
        var cap = KafkaBookKeeperCompactionTestSupportV2.input(false, 1)
                .layout()
                .task()
                .capability();
        var artifact = Path.of(BookKeeper.class
                .getProtectionDomain()
                .getCodeSource()
                .getLocation()
                .toURI());
        assertThat(Sha256Digest.hash(CanonicalBytes.copyOf(Files.readAllBytes(artifact))))
                .isEqualTo(cap.clientArtifactSha256());
        client = BookKeeper.newBuilder(RealBookKeeperClientConfigurationV1.from(
                        System.getProperty("nereus.bookkeeper.metadataServiceUri"), cap))
                .build();
        assertThat(client.isDriverMetadataServiceAvailable().get(10, TimeUnit.SECONDS))
                .isTrue();
    }

    @AfterAll
    static void closeClient() throws Exception {
        if (client != null) {
            client.close();
        }
    }

    @Test
    void publishedMultiLedgerDescriptorRestartsWithoutExpectedOutputOrOldSourceBodies() throws Exception {
        var context = new Context(KafkaBookKeeperCompactionTestSupportV2.input(false, ATTEMPTS.incrementAndGet()));
        try {
            assertThat(context.publish()).isEqualTo(PublicationOutcome.APPLIED_EXACT);
            var selected = context.m4.readSelector().orElseThrow();
            context.writer.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
            var fresh = context.session();
            try {
                var recovered = context.publication(fresh)
                        .recoverSelected(selected)
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS);
                assertThat(recovered.descriptor().descriptorSha256()).isEqualTo(selected.selectedViewSha256());
                assertThat(recovered.lookup(0).orElseThrow().coverage().inclusiveStart())
                        .isEqualTo(1);
                assertThat(recovered.listOffset(2)).hasValue(1);
                assertThat(recovered.allowsPredecessorOffset(0)).isFalse();
                assertThat(KafkaRecordBatchCodecV1.parse(recovered.readBatch(0))
                                .records()
                                .get(0)
                                .value()
                                .orElseThrow()
                                .length())
                        .isEqualTo(1024);
            } finally {
                fresh.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
            }
        } finally {
            context.close();
        }
    }

    @Test
    void nativeProducerTransactionAndAbortedIndexesRecoverAcrossInterleavedProducerIds() throws Exception {
        var bodies = List.of(
                KafkaSemanticCompactorV1Test.transactionalRecords(
                        0, 1, 88, (short) 2, 0, new SimpleRecord(10, new byte[] {1}, new byte[] {9})),
                KafkaSemanticCompactorV1Test.idempotentRecords(
                        1, 1, 91, (short) 1, 0, new SimpleRecord(11, new byte[] {2}, new byte[] {8})),
                KafkaSemanticCompactorV1Test.control(2, 1, 88, (short) 2, ControlRecordType.ABORT, 5));
        var input = KafkaBookKeeperCompactionTestSupportV2.input(
                bodies,
                0,
                3,
                List.of(),
                List.of(new KafkaCompactionRecordsV1.TransactionRange(
                        88, 0, 3, KafkaCompactionRecordsV1.TransactionOutcome.ABORTED, 5)),
                ATTEMPTS.incrementAndGet());
        var context = new Context(input);
        try {
            assertThat(context.publish()).isEqualTo(PublicationOutcome.APPLIED_EXACT);
            var fresh = context.session();
            try {
                var recovered = context.publication(fresh)
                        .recoverSelected(context.m4.readSelector().orElseThrow())
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS);
                assertThat(recovered.index(IndexKind.PRODUCER_RECOVERY).rows())
                        .extracting(row -> row.coverage().inclusiveStart())
                        .containsExactly(0L, 1L, 2L);
                assertThat(recovered.index(IndexKind.TRANSACTION).rows())
                        .extracting(row -> row.coverage().inclusiveStart())
                        .containsExactly(0L, 2L);
                assertThat(recovered.index(IndexKind.ABORTED_TRANSACTION).rows())
                        .extracting(row -> row.coverage().inclusiveStart())
                        .containsExactly(0L, 2L);
                for (IndexKind kind : IndexKind.values()) {
                    assertThat(recovered.index(kind))
                            .isEqualTo(input.semantic().indexes().get(kind.ordinal()));
                }
            } finally {
                fresh.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
            }
        } finally {
            context.close();
        }
    }

    @Test
    void lostSelectorResponseSelectsEmptyCoverageOnceAndRestartsWithNoDataLedger() throws Exception {
        var context = new Context(KafkaBookKeeperCompactionTestSupportV2.input(true, ATTEMPTS.incrementAndGet()));
        try {
            context.store.loseNextSelectorCasResponse = true;
            assertThat(context.publish()).isEqualTo(PublicationOutcome.EXISTING_EXACT);
            assertThat(context.publish()).isEqualTo(PublicationOutcome.EXISTING_EXACT);
            assertThat(context.store.selectorCasCount).isEqualTo(1);
            var fresh = context.session();
            try {
                var view = context.publication(fresh)
                        .recoverSelected(context.m4.readSelector().orElseThrow())
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS);
                assertThat(view.descriptor().batchCount()).isZero();
                assertThat(view.lookup(0)).isEmpty();
                assertThat(view.allowsPredecessorOffset(0)).isFalse();
                assertThat(view.descriptor().task().parts())
                        .allMatch(part -> part.kind() == KafkaBookKeeperInventoryV2.PartKind.INDEX);
            } finally {
                fresh.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
            }
        } finally {
            context.close();
        }
    }

    @Test
    void missingSelectedLedgerFailsWithoutRecreationRecoveryFencingOrObsoleteFallback() throws Exception {
        var context = new Context(KafkaBookKeeperCompactionTestSupportV2.input(false, ATTEMPTS.incrementAndGet()));
        try {
            assertThat(context.publish()).isEqualTo(PublicationOutcome.APPLIED_EXACT);
            // Fixture-only loss injection in this isolated test cluster, never an M5-D deletion authorization.
            client.newDeleteLedgerOp()
                    .withLedgerId(context.descriptor
                            .sealedParts()
                            .get(0)
                            .handle()
                            .ledgerIdentity()
                            .ledgerId())
                    .execute()
                    .get(10, TimeUnit.SECONDS);
            var fresh = context.session();
            try {
                assertThatThrownBy(() -> context.publication(fresh)
                                .recoverSelected(context.m4.readSelector().orElseThrow())
                                .toCompletableFuture()
                                .get(30, TimeUnit.SECONDS))
                        .hasRootCauseMessage("selected BK native sealed metadata is missing, changed or unknown");
                assertThat(context.store.selectorCasCount).isEqualTo(1);
            } finally {
                fresh.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
            }
        } finally {
            context.close();
        }
    }

    static final class Context {
        final Input input;
        final Store store = new Store();
        final RealBookKeeperCellSessionV1 writer;
        final KafkaSealedBookKeeperDescriptorV2 descriptor;
        final List<SourceProtectionIdentity> protections;
        final M4ReadControlCoordinatorV1 m4;

        Context(Input input) throws Exception {
            this.input = input;
            writer = session();
            var inspector = new M5BookKeeperDeleteAdapterV1(
                    client, input.layout().task().capability(), new byte[0]);
            var output = new KafkaBookKeeperCompactionWriterV2(
                            new KafkaBookKeeperInventoryV2(store), writer, inspector::captureExactTarget)
                    .write(input.layout())
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS);
            descriptor =
                    KafkaSealedBookKeeperDescriptorV2.create(input.plan(), input.semantic(), input.layout(), output);
            protections = KafkaBookKeeperCompactionTestSupportV2.installReadAuthority(store, input);
            m4 = new M4ReadControlCoordinatorV1(
                    store, 7, input.plan().sourceCut().identity().binding());
        }

        RealBookKeeperCellSessionV1 session() {
            return new RealBookKeeperCellSessionV1(client, input.layout().task().capability(), new byte[0]);
        }

        KafkaBookKeeperCompactionPublicationV2 publication(BookKeeperCellSession provider) {
            return new KafkaBookKeeperCompactionPublicationV2(
                    store, 7, input.plan().sourceCut().identity().binding(), reader(provider));
        }

        KafkaSealedBookKeeperReaderV2 reader(BookKeeperCellSession provider) {
            var inspector = new M5BookKeeperDeleteAdapterV1(
                    client, input.layout().task().capability(), new byte[0]);
            var readOnly = (BookKeeperCellSession) Proxy.newProxyInstance(
                    BookKeeperCellSession.class.getClassLoader(),
                    new Class<?>[] {BookKeeperCellSession.class},
                    (proxy, method, arguments) -> {
                        if (!List.of("capabilitySnapshot", "openRunLedger", "readExactEntry")
                                .contains(method.getName())) {
                            throw new AssertionError("descriptor recovery attempted mutation: " + method.getName());
                        }
                        try {
                            return method.invoke(provider, arguments);
                        } catch (InvocationTargetException failure) {
                            throw failure.getCause();
                        }
                    });
            return new KafkaSealedBookKeeperReaderV2(readOnly, inspector::captureExactTarget, 1_000_000);
        }

        PublicationOutcome publish() throws Exception {
            return publication(writer)
                    .publish(
                            input.plan(),
                            input.semantic(),
                            descriptor,
                            protections,
                            () -> new KafkaCompactionPublicationFenceV1().expected(input.plan()))
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS);
        }

        void close() throws Exception {
            writer.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }
}
