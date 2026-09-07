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
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperCompactionLayoutV2.Layout;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperCompactionTestSupportV2.Store;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperInventoryV2.PartKind;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
import com.nereusstream.storage.api.bookkeeper.ExactLedgerEntryV1;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationResultV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerAppendRequestV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerReadResultV1;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1;
import com.nereusstream.storage.bookkeeper.RealBookKeeperCellSessionV1;
import com.nereusstream.storage.bookkeeper.RealBookKeeperClientConfigurationV1;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Real source-locked BK IO with explicitly synthetic source/control fixtures; no selector or native broker claim. */
class KafkaBookKeeperCompactionWriterV2RealTest {
    private static final AtomicLong ATTEMPTS = new AtomicLong();
    private static BookKeeper client;

    @BeforeAll
    static void connect() throws Exception {
        var capability =
                KafkaBookKeeperCompactionTestSupportV2.layout(false, 0).task().capability();
        var artifact = java.nio.file.Path.of(BookKeeper.class
                .getProtectionDomain()
                .getCodeSource()
                .getLocation()
                .toURI());
        assertThat(Sha256Digest.hash(CanonicalBytes.copyOf(java.nio.file.Files.readAllBytes(artifact))))
                .isEqualTo(capability.clientArtifactSha256());
        client = BookKeeper.newBuilder(RealBookKeeperClientConfigurationV1.from(
                        System.getProperty("nereus.bookkeeper.metadataServiceUri"), capability))
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
    void multiLedgerDataAndEightIndexesSealAndVerifyAgainThroughAFreshSession() throws Exception {
        var context = new Context(false);
        try {
            var verified = context.writer(context.session)
                    .write(context.layout)
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS);
            assertThat(verified).hasSizeGreaterThan(2);
            assertThat(verified)
                    .extracting(part -> part.part().resource().ledgerId())
                    .doesNotHaveDuplicates();
            context.session.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
            var restarted = context.newSession();
            try {
                var restoredTask = KafkaBookKeeperInventoryCodecV2.decodeTask(context.store.values.get(
                        KafkaBookKeeperInventoryV2.taskKey(context.layout.task().taskIdSha256())));
                var restoredLayout = new Layout(restoredTask, context.layout.parts());
                var recovered = context.writer(restarted)
                        .recover(restoredLayout)
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS);
                assertThat(recovered).isEqualTo(verified);
            } finally {
                restarted.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
            }
        } finally {
            context.close();
        }
    }

    @Test
    void emptyOutputCreatesOnlyIndexLedgersAndRecoversItsCompleteEmptyCoverage() throws Exception {
        var context = new Context(true);
        try {
            assertThat(context.layout.parts()).allMatch(part -> part.kind() == PartKind.INDEX);
            var verified = context.writer(context.session)
                    .write(context.layout)
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS);
            assertThat(verified).hasSize(context.layout.parts().size());
            assertThat(context.writer(context.session)
                            .recover(context.layout)
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS))
                    .isEqualTo(verified);
        } finally {
            context.close();
        }
    }

    @Test
    void lostCreateResponseLeavesOneInventoriedSealedEmptyPartAndCannotAllocateAnUntrackedReplacement()
            throws Exception {
        var context = new Context(false);
        AtomicInteger creates = new AtomicInteger();
        var lost = context.wrap((name, arguments, operation) -> {
            if (name.equals("createReservedRunLedger")) {
                creates.incrementAndGet();
                return operation.thenApply(ignored -> ProviderMutationResultV1.outcomeUnknown());
            }
            return operation;
        });
        try {
            assertThatThrownBy(() -> context.writer(lost)
                            .write(context.layout)
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS))
                    .hasRootCauseMessage("BK compaction part is missing, unsealed or incomplete");
            var first = context.inventory.readPart(context.layout.task(), 0).orElseThrow();
            assertThat(context.inventory.readPart(context.layout.task(), 1)).isEmpty();
            assertThatThrownBy(() -> context.writer(context.session)
                            .write(context.layout)
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS))
                    .hasRootCauseMessage("BK compaction part is missing, unsealed or incomplete");
            assertThat(context.inventory.readPart(context.layout.task(), 0)).contains(first);
            assertThat(creates).hasValue(1);
        } finally {
            context.close();
        }
    }

    @Test
    void discardedLastAppendResponseRecoversTheFullPartBeforeContinuing() throws Exception {
        var context = new Context(false);
        AtomicBoolean discarded = new AtomicBoolean();
        int last = context.layout.parts().get(0).entries().size() - 1;
        var lost = context.wrap((name, arguments, operation) -> {
            if (name.equals("appendExplicitEntry")
                    && ((RunLedgerAppendRequestV1) arguments[0]).expectedEntryId() == last
                    && discarded.compareAndSet(false, true)) {
                return operation.thenApply(ignored -> ProviderMutationResultV1.outcomeUnknown());
            }
            return operation;
        });
        try {
            assertThat(context.writer(lost)
                            .write(context.layout)
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS))
                    .hasSize(context.layout.parts().size());
            assertThat(discarded).isTrue();
        } finally {
            context.close();
        }
    }

    @Test
    void discardedEarlyAppendResponseSealsPartialOutputAndNeverRewritesItOnRetry() throws Exception {
        var context = new Context(false);
        AtomicInteger appends = new AtomicInteger();
        var lost = context.wrap((name, arguments, operation) -> {
            if (name.equals("appendExplicitEntry")) {
                appends.incrementAndGet();
                return operation.thenApply(ignored -> ProviderMutationResultV1.outcomeUnknown());
            }
            return operation;
        });
        try {
            assertThat(context.layout.parts().get(0).entries().size()).isGreaterThan(1);
            assertThatThrownBy(() -> context.writer(lost)
                            .write(context.layout)
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS))
                    .hasRootCauseMessage("BK compaction part is missing, unsealed or incomplete");
            assertThatThrownBy(() -> context.writer(lost)
                            .write(context.layout)
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS))
                    .hasRootCauseMessage("BK compaction part is missing, unsealed or incomplete");
            assertThat(appends).hasValue(1);
        } finally {
            context.close();
        }
    }

    @Test
    void changedReadBytesPreventTheWholeOutputFromVerifying() throws Exception {
        var context = new Context(false);
        AtomicBoolean changed = new AtomicBoolean();
        var corrupt = context.wrap((name, arguments, operation) -> {
            if (name.equals("readExactEntry") && changed.compareAndSet(false, true)) {
                return operation.thenApply(value -> {
                    var exact = ((RunLedgerReadResultV1) value).exactEntry().orElseThrow();
                    byte[] bytes = exact.payload().toByteArray();
                    bytes[bytes.length - 1] ^= 1;
                    CanonicalBytes payload = CanonicalBytes.copyOf(bytes);
                    return RunLedgerReadResultV1.foundExact(new ExactLedgerEntryV1(
                            exact.handle(), exact.entryId(), payload, Sha256Digest.hash(payload)));
                });
            }
            return operation;
        });
        try {
            assertThatThrownBy(() -> context.writer(corrupt)
                            .write(context.layout)
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS))
                    .hasRootCauseMessage("BK output entry differs from the semantic carrier bytes");
            assertThat(context.inventory.readPart(context.layout.task(), 1)).isEmpty();
            assertThat(changed).isTrue();
        } finally {
            context.close();
        }
    }

    @Test
    void recoveryRejectsForeignRunMetadataBeforeNativeFencing() throws Exception {
        var context = new Context(false);
        var configuration = com.nereusstream.storage.api.bookkeeper.RunLedgerConfigurationV1.from(
                context.layout.task().capability(),
                new com.nereusstream.storage.api.bookkeeper.StorageRunId(
                        new com.nereusstream.domain.identity.Id128(101, ATTEMPTS.incrementAndGet())));
        var foreign = context.session
                .createRunLedger(configuration)
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS)
                .exactProof()
                .orElseThrow();
        var expected = context.layout.task().configuration(0);
        var substituted = new KafkaBookKeeperInventoryV2.Part(
                context.layout.task().taskIdSha256(),
                0,
                new com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.BookKeeperLedger(
                        context.layout.task().namespace(),
                        foreign.ledgerIdentity().ledgerId()),
                new com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1(
                        expected.providerScopeId(),
                        expected.runId(),
                        foreign.ledgerIdentity(),
                        expected.configurationDigest()));
        context.inventory.register(context.layout.task());
        context.store.values.put(
                KafkaBookKeeperInventoryV2.partKey(context.layout.task().taskIdSha256(), 0),
                KafkaBookKeeperInventoryCodecV2.encodePart(substituted));
        AtomicInteger fences = new AtomicInteger();
        var observed = context.wrap((name, arguments, operation) -> {
            if (name.equals("fenceAndRecoverRunLedger")) {
                fences.incrementAndGet();
            }
            return operation;
        });
        try {
            assertThatThrownBy(() -> context.writer(observed)
                            .recover(context.layout)
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS))
                    .isInstanceOf(java.util.concurrent.ExecutionException.class);
            assertThat(client.getLedgerMetadata(foreign.ledgerIdentity().ledgerId())
                            .get(10, TimeUnit.SECONDS)
                            .isClosed())
                    .isFalse();
            assertThat(fences).hasValue(0);
        } finally {
            context.close();
        }
    }

    @FunctionalInterface
    private interface Interceptor {
        CompletionStage<?> intercept(String name, Object[] arguments, CompletionStage<?> operation);
    }

    private static final class Context {
        final Layout layout;
        final Store store = new Store();
        final KafkaBookKeeperInventoryV2 inventory = new KafkaBookKeeperInventoryV2(store);
        final RealBookKeeperCellSessionV1 session;

        Context(boolean empty) {
            layout = KafkaBookKeeperCompactionTestSupportV2.layout(empty, ATTEMPTS.incrementAndGet());
            session = newSession();
        }

        RealBookKeeperCellSessionV1 newSession() {
            return new RealBookKeeperCellSessionV1(client, layout.task().capability(), new byte[0]);
        }

        KafkaBookKeeperCompactionWriterV2 writer(BookKeeperCellSession target) {
            var inspector =
                    new M5BookKeeperDeleteAdapterV1(client, layout.task().capability(), new byte[0]);
            return new KafkaBookKeeperCompactionWriterV2(inventory, target, inspector::captureExactTarget);
        }

        BookKeeperCellSession wrap(Interceptor interceptor) {
            return (BookKeeperCellSession) Proxy.newProxyInstance(
                    BookKeeperCellSession.class.getClassLoader(),
                    new Class<?>[] {BookKeeperCellSession.class},
                    (proxy, method, arguments) -> {
                        try {
                            Object result = method.invoke(session, arguments);
                            return result instanceof CompletionStage<?> stage
                                    ? interceptor.intercept(method.getName(), arguments, stage)
                                    : result;
                        } catch (InvocationTargetException failure) {
                            throw failure.getCause();
                        }
                    });
        }

        void close() throws Exception {
            session.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }
}
