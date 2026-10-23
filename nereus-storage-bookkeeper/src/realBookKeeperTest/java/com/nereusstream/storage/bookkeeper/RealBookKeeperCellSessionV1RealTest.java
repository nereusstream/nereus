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

package com.nereusstream.storage.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperDigestTypeV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperProtocolModeV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperTimeoutClassV1;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationResultV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerAppendRequestV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerConfigurationV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerOpenOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerReadOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.CreateAdvBuilder;
import org.apache.bookkeeper.client.api.CreateBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.OpenBuilder;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteAdvHandle;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class RealBookKeeperCellSessionV1RealTest {
    private static final AtomicLong RUN_IDS = new AtomicLong(100);
    private static final byte[] PASSWORD = new byte[0];
    private static BookKeeper client;
    private static BookKeeperCapabilitySnapshotV1 capability;

    @BeforeAll
    static void connectExactClient() throws Exception {
        capability = capability();
        String metadataServiceUri = System.getProperty("nereus.bookkeeper.metadataServiceUri");
        client = BookKeeper.newBuilder(RealBookKeeperClientConfigurationV1.from(metadataServiceUri, capability))
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
    void createAppendReadAndCloseCarryExactRealQuorumProofs() throws Exception {
        RealBookKeeperCellSessionV1 session = session();
        RunLedgerHandleV1 handle = create(session);
        byte[] payload = new byte[] {1, 2, 3, 4};
        ImmutableRetainedStoragePayload retained = ImmutableRetainedStoragePayload.copyOf(payload);

        var append = session.appendExplicitEntry(new RunLedgerAppendRequestV1(handle, 0, retained))
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
        assertThat(retained.release()).isTrue();
        assertThat(append.outcome()).isEqualTo(ProviderMutationOutcomeV1.APPLIED_EXACT);
        assertThat(append.exactProof().orElseThrow().acknowledgedBookies()).isEqualTo(2);
        assertThat(session.readExactEntry(handle, 0)
                        .toCompletableFuture()
                        .get(10, TimeUnit.SECONDS)
                        .exactEntry()
                        .orElseThrow()
                        .payload()
                        .toByteArray())
                .containsExactly(payload);

        var close = session.closeRunLedger(handle).toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertThat(close.outcome()).isEqualTo(ProviderMutationOutcomeV1.APPLIED_EXACT);
        assertThat(close.exactProof().orElseThrow().lastAddConfirmed()).isEqualTo(0);
        session.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    @Test
    void closeProofUsesTheConfirmedSealAfterQueuedAppendWins() throws Exception {
        var dispatch = new CompletableFuture<Void>();
        var writer = new RealBookKeeperCellSessionV1(withWriteCloseDispatch(client, dispatch), capability, PASSWORD);
        try {
            var handle = create(writer);
            append(writer, handle, 0, new byte[] {13});
            var closing = writer.closeRunLedger(handle).toCompletableFuture();
            assertThat(closing).isNotDone();
            // The real SDK close has not run yet; this real quorum append reaches its final seal first.
            append(writer, handle, 1, new byte[] {21});
            dispatch.complete(null);
            var closed = closing.get(10, TimeUnit.SECONDS);
            assertThat(closed.outcome()).isEqualTo(ProviderMutationOutcomeV1.APPLIED_EXACT);
            try (var observed = client.newOpenLedgerOp()
                    .withLedgerId(handle.ledgerIdentity().ledgerId())
                    .withRecovery(false)
                    .withPassword(PASSWORD)
                    .withDigestType(DigestType.CRC32C)
                    .execute()
                    .get(10, TimeUnit.SECONDS)) {
                assertThat(observed.getLedgerMetadata().isClosed()).isTrue();
                assertThat(observed.getLastAddConfirmed()).isEqualTo(1);
                assertThat(closed.exactProof().orElseThrow().lastAddConfirmed())
                        .isEqualTo(observed.getLastAddConfirmed());
                try (var entries = observed.readAsync(1, 1).get(10, TimeUnit.SECONDS)) {
                    assertThat(entries.getEntry(1).getEntryBytes()).containsExactly((byte) 21);
                }
            }
        } finally {
            dispatch.complete(null);
            writer.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void freshCellOpensReadsAndRecoversTheExactSealedLedger() throws Exception {
        RealBookKeeperCellSessionV1 writer = session();
        RunLedgerHandleV1 handle = create(writer);
        append(writer, handle, 0, new byte[] {7});
        writer.closeRunLedger(handle).toCompletableFuture().get(10, TimeUnit.SECONDS);
        writer.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);

        RealBookKeeperCellSessionV1 reader = session();
        assertThat(reader.openRunLedger(handle)
                        .toCompletableFuture()
                        .get(10, TimeUnit.SECONDS)
                        .outcome())
                .isEqualTo(RunLedgerOpenOutcomeV1.OPENED_EXACT);
        assertThat(reader.readExactEntry(handle, 0)
                        .toCompletableFuture()
                        .get(10, TimeUnit.SECONDS)
                        .outcome())
                .isEqualTo(RunLedgerReadOutcomeV1.FOUND_EXACT);
        var recovered =
                reader.fenceAndRecoverRunLedger(handle).toCompletableFuture().get(30, TimeUnit.SECONDS);
        assertThat(recovered.outcome()).isEqualTo(ProviderMutationOutcomeV1.APPLIED_EXACT);
        assertThat(recovered.exactProof().orElseThrow().lastAddConfirmed()).isEqualTo(0);
        reader.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    @ParameterizedTest
    @ValueSource(strings = {"PENDING", "FAILED", "THROWN"})
    void replacedHandleCloseRemainsOwnedThroughSessionClose(String mode) throws Exception {
        var writer = session();
        var handle = create(writer);
        append(writer, handle, 0, new byte[] {7});
        writer.closeRunLedger(handle).toCompletableFuture().get(10, TimeUnit.SECONDS);
        writer.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);

        var closeResponse = new CompletableFuture<Void>();
        if (mode.equals("FAILED")) {
            closeResponse.completeExceptionally(new IllegalStateException("retired read close response lost"));
        }
        var reader = new RealBookKeeperCellSessionV1(
                withReadCloseResponse(client, closeResponse, mode.equals("THROWN")), capability, PASSWORD);
        assertThat(reader.openRunLedger(handle)
                        .toCompletableFuture()
                        .get(10, TimeUnit.SECONDS)
                        .outcome())
                .isEqualTo(RunLedgerOpenOutcomeV1.OPENED_EXACT);
        assertThat(reader.readExactEntry(handle, 0)
                        .toCompletableFuture()
                        .get(10, TimeUnit.SECONDS)
                        .exactEntry())
                .isPresent();
        assertThat(reader.fenceAndRecoverRunLedger(handle)
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS)
                        .outcome())
                .isEqualTo(ProviderMutationOutcomeV1.APPLIED_EXACT);
        var close = reader.closeAsync().toCompletableFuture();
        if (mode.equals("PENDING")) {
            assertThat(close).isNotDone();
            assertThat(close.cancel(true)).isTrue();
            var remaining = reader.closeAsync().toCompletableFuture();
            assertThat(remaining).isNotDone();
            closeResponse.complete(null);
            remaining.get(10, TimeUnit.SECONDS);
        } else {
            assertThatThrownBy(() -> close.get(10, TimeUnit.SECONDS))
                    .hasRootCauseMessage("retired read close response lost");
        }
    }

    @Test
    void ownedHeaderReadsEntryZeroWhileTheNativeReaderLacIsMinusOne() throws Exception {
        var writer = session();
        var handle = create(writer);
        var payload = new byte[] {11, 12, 13};
        append(writer, handle, 0, payload);
        var openedReaderLac = new AtomicLong(Long.MIN_VALUE);
        var reader = new RealBookKeeperCellSessionV1(
                withReadCloseResponse(
                        client,
                        CompletableFuture.completedFuture(null),
                        false,
                        nativeReader -> openedReaderLac.set(nativeReader.getLastAddConfirmed())),
                capability,
                PASSWORD);
        try {
            var header =
                    reader.readNativeRunHeader(handle).toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertThat(openedReaderLac.get()).isEqualTo(-1);
            assertThat(header.handle()).isEqualTo(handle);
            assertThat(header.entryId()).isZero();
            assertThat(header.payload().toByteArray()).containsExactly(payload);
            reader.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
        } finally {
            writer.closeRunLedger(handle).toCompletableFuture().get(10, TimeUnit.SECONDS);
            writer.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"PENDING", "FAILED", "THROWN"})
    void nativeHeaderCloseRetainsItsHandleAndIsolatesCancelledObservers(String mode) throws Exception {
        var writer = session();
        var handle = create(writer);
        append(writer, handle, 0, new byte[] {19});
        var response = new CompletableFuture<Void>();
        if (mode.equals("FAILED")) {
            response.completeExceptionally(new IllegalStateException("retired read close response lost"));
        }
        var reader = new RealBookKeeperCellSessionV1(
                withReadCloseResponse(client, response, mode.equals("THROWN")), capability, PASSWORD);
        try {
            assertThat(reader.readNativeRunHeader(handle)
                            .toCompletableFuture()
                            .get(10, TimeUnit.SECONDS)
                            .payload()
                            .toByteArray())
                    .containsExactly((byte) 19);
            var closing = reader.closeAsync().toCompletableFuture();
            if (mode.equals("PENDING")) {
                assertThat(closing).isNotDone();
                assertThat(closing.cancel(true)).isTrue();
                var retained = reader.closeAsync().toCompletableFuture();
                assertThat(retained).isNotDone();
                response.complete(null);
                retained.get(10, TimeUnit.SECONDS);
            } else {
                assertThatThrownBy(() -> closing.get(10, TimeUnit.SECONDS))
                        .hasRootCauseMessage("retired read close response lost");
                assertThatThrownBy(
                                () -> reader.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS))
                        .hasRootCauseMessage("retired read close response lost");
            }
        } finally {
            response.complete(null);
            writer.closeRunLedger(handle).toCompletableFuture().get(10, TimeUnit.SECONDS);
            writer.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    /** Only the response to closing a real opened reader is held/lost; native open, read, recovery and close run. */
    private static BookKeeper withWriteCloseDispatch(BookKeeper delegate, CompletableFuture<Void> dispatch) {
        return (BookKeeper) Proxy.newProxyInstance(
                BookKeeper.class.getClassLoader(), new Class<?>[] {BookKeeper.class}, (proxy, method, args) -> {
                    if (!method.getName().equals("newCreateLedgerOp")) {
                        return invoke(delegate, method, args);
                    }
                    var builder = delegate.newCreateLedgerOp();
                    return Proxy.newProxyInstance(
                            CreateBuilder.class.getClassLoader(),
                            new Class<?>[] {CreateBuilder.class},
                            (createProxy, createMethod, createArgs) -> {
                                var value = invoke(builder, createMethod, createArgs);
                                if (value == builder) {
                                    return createProxy;
                                }
                                if (!createMethod.getName().equals("makeAdv")) {
                                    return value;
                                }
                                var advanced = (CreateAdvBuilder) value;
                                return Proxy.newProxyInstance(
                                        CreateAdvBuilder.class.getClassLoader(),
                                        new Class<?>[] {CreateAdvBuilder.class},
                                        (advProxy, advMethod, advArgs) -> {
                                            var result = invoke(advanced, advMethod, advArgs);
                                            if (result == advanced) {
                                                return advProxy;
                                            }
                                            if (!advMethod.getName().equals("execute")) {
                                                return result;
                                            }
                                            @SuppressWarnings("unchecked")
                                            var created = (CompletableFuture<WriteAdvHandle>) result;
                                            return created.thenApply(handle -> (WriteAdvHandle) Proxy.newProxyInstance(
                                                    WriteAdvHandle.class.getClassLoader(),
                                                    new Class<?>[] {WriteAdvHandle.class},
                                                    (writeProxy, writeMethod, writeArgs) -> {
                                                        if (writeMethod
                                                                .getName()
                                                                .equals("closeAsync")) {
                                                            return dispatch.thenCompose(ignored -> handle.closeAsync());
                                                        }
                                                        return invoke(handle, writeMethod, writeArgs);
                                                    }));
                                        });
                            });
                });
    }

    private static BookKeeper withReadCloseResponse(
            BookKeeper delegate, CompletableFuture<Void> closeResponse, boolean throwClose) {
        return withReadCloseResponse(delegate, closeResponse, throwClose, ignored -> {});
    }

    private static BookKeeper withReadCloseResponse(
            BookKeeper delegate,
            CompletableFuture<Void> closeResponse,
            boolean throwClose,
            java.util.function.Consumer<ReadHandle> openedReader) {
        return (BookKeeper) Proxy.newProxyInstance(
                BookKeeper.class.getClassLoader(), new Class<?>[] {BookKeeper.class}, (proxy, method, args) -> {
                    if (!method.getName().equals("newOpenLedgerOp")) {
                        return invoke(delegate, method, args);
                    }
                    var builder = delegate.newOpenLedgerOp();
                    var recovery = new AtomicBoolean(true);
                    return Proxy.newProxyInstance(
                            OpenBuilder.class.getClassLoader(),
                            new Class<?>[] {OpenBuilder.class},
                            (openProxy, openMethod, openArgs) -> {
                                if (openMethod.getName().equals("withRecovery")) {
                                    recovery.set((boolean) openArgs[0]);
                                }
                                var result = invoke(builder, openMethod, openArgs);
                                if (result == builder) {
                                    return openProxy;
                                }
                                if (!openMethod.getName().equals("execute") || recovery.get()) {
                                    return result;
                                }
                                @SuppressWarnings("unchecked")
                                var opened = (CompletableFuture<ReadHandle>) result;
                                return opened.thenApply(handle -> {
                                    openedReader.accept(handle);
                                    return (ReadHandle) Proxy.newProxyInstance(
                                            ReadHandle.class.getClassLoader(),
                                            new Class<?>[] {ReadHandle.class},
                                            (readProxy, readMethod, readArgs) -> {
                                                if (!readMethod.getName().equals("closeAsync")) {
                                                    return invoke(handle, readMethod, readArgs);
                                                }
                                                var actualClose = handle.closeAsync();
                                                if (throwClose) {
                                                    actualClose.join();
                                                    throw new IllegalStateException("retired read close response lost");
                                                }
                                                return actualClose.thenCompose(ignored -> closeResponse);
                                            });
                                });
                            });
                });
    }

    private static Object invoke(Object target, java.lang.reflect.Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException failure) {
            throw failure.getCause();
        }
    }

    @Test
    void recoveryFencesTheOldWriterBeforeASecondEntryCanApply() throws Exception {
        RealBookKeeperCellSessionV1 oldOwner = session();
        RunLedgerHandleV1 handle = create(oldOwner);
        append(oldOwner, handle, 0, new byte[] {1});

        RealBookKeeperCellSessionV1 newOwner = session();
        var recovered =
                newOwner.fenceAndRecoverRunLedger(handle).toCompletableFuture().get(30, TimeUnit.SECONDS);
        assertThat(recovered.outcome()).isEqualTo(ProviderMutationOutcomeV1.APPLIED_EXACT);

        ImmutableRetainedStoragePayload payload = ImmutableRetainedStoragePayload.copyOf(new byte[] {2});
        var fencedAppend = oldOwner.appendExplicitEntry(new RunLedgerAppendRequestV1(handle, 1, payload))
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
        assertThat(payload.release()).isTrue();
        assertThat(fencedAppend.outcome()).isEqualTo(ProviderMutationOutcomeV1.FENCED_OR_CONFLICT);
        oldOwner.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
        newOwner.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    @Test
    void discardedAppendResponseReconcilesFromRealRecoveredBytes() throws Exception {
        RealBookKeeperCellSessionV1 writer = session();
        RunLedgerHandleV1 handle = create(writer);
        byte[] payload = new byte[] {9, 8, 7};
        append(writer, handle, 0, payload);

        RealBookKeeperCellSessionV1 resolver = session();
        assertThat(resolver.fenceAndRecoverRunLedger(handle)
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS)
                        .outcome())
                .isEqualTo(ProviderMutationOutcomeV1.APPLIED_EXACT);
        assertThat(resolver.readExactEntry(handle, 0)
                        .toCompletableFuture()
                        .get(10, TimeUnit.SECONDS)
                        .exactEntry()
                        .orElseThrow()
                        .payload()
                        .toByteArray())
                .containsExactly(payload);
        writer.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
        resolver.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    @Test
    void realOpenRejectsAReboundRunIdentityOnTheSameLedger() throws Exception {
        RealBookKeeperCellSessionV1 writer = session();
        RunLedgerHandleV1 handle = create(writer);
        RunLedgerHandleV1 substituted = new RunLedgerHandleV1(
                handle.providerScopeId(),
                new StorageRunId(new Id128(0, RUN_IDS.incrementAndGet())),
                handle.ledgerIdentity(),
                handle.configurationDigest());

        RealBookKeeperCellSessionV1 reader = session();
        assertThat(reader.openRunLedger(substituted)
                        .toCompletableFuture()
                        .get(10, TimeUnit.SECONDS)
                        .outcome())
                .isEqualTo(RunLedgerOpenOutcomeV1.CONFIGURATION_MISMATCH);
        writer.closeRunLedger(handle).toCompletableFuture().get(10, TimeUnit.SECONDS);
        writer.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
        reader.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    @Test
    void realReadDistinguishesAnEntryThatWasNeverWritten() throws Exception {
        RealBookKeeperCellSessionV1 writer = session();
        RunLedgerHandleV1 handle = create(writer);
        append(writer, handle, 0, new byte[] {1});
        writer.closeRunLedger(handle).toCompletableFuture().get(10, TimeUnit.SECONDS);
        writer.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);

        RealBookKeeperCellSessionV1 reader = session();
        assertThat(reader.openRunLedger(handle)
                        .toCompletableFuture()
                        .get(10, TimeUnit.SECONDS)
                        .outcome())
                .isEqualTo(RunLedgerOpenOutcomeV1.OPENED_EXACT);

        assertThat(reader.readExactEntry(handle, 1)
                        .toCompletableFuture()
                        .get(10, TimeUnit.SECONDS)
                        .outcome())
                .isEqualTo(RunLedgerReadOutcomeV1.DEFINITIVELY_ABSENT);
        reader.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    @Test
    void m5DeleteAdapterDeletesOnlyTheExactSealedLedgerAndReconcilesAbsence() throws Exception {
        RealBookKeeperCellSessionV1 writer = session();
        RunLedgerHandleV1 handle = create(writer);
        append(writer, handle, 0, new byte[] {4, 5, 6});
        writer.closeRunLedger(handle).toCompletableFuture().get(10, TimeUnit.SECONDS);
        writer.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);

        M5BookKeeperDeleteAdapterV1 adapter = new M5BookKeeperDeleteAdapterV1(client, capability, PASSWORD);
        var target = adapter.captureExactTarget(handle)
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS)
                .exactTarget()
                .orElseThrow();
        var stale = new M5BookKeeperDeleteAdapterV1.BookKeeperDeleteTargetV1(
                target.handle(),
                target.sealedLastEntryId(),
                target.sealedLength() + 1,
                target.ensembleSize(),
                target.writeQuorumSize(),
                target.ackQuorumSize(),
                target.digestType(),
                target.passwordCredentialIdentityVersion(),
                target.passwordSha256(),
                target.metadataFormatVersion(),
                target.metadataCToken(),
                target.metadataSha256());

        assertThat(adapter.deleteAndReconcile(stale)
                        .toCompletableFuture()
                        .get(10, TimeUnit.SECONDS)
                        .outcome())
                .isEqualTo(M5BookKeeperDeleteAdapterV1.DeleteOutcome.DIFFERENT_LEDGER_OR_METADATA);
        assertThat(adapter.deleteAndReconcile(target)
                        .toCompletableFuture()
                        .get(10, TimeUnit.SECONDS)
                        .outcome())
                .isEqualTo(M5BookKeeperDeleteAdapterV1.DeleteOutcome.AUTHORITATIVELY_ABSENT);
        assertThat(adapter.deleteAndReconcile(target)
                        .toCompletableFuture()
                        .get(10, TimeUnit.SECONDS)
                        .outcome())
                .isEqualTo(M5BookKeeperDeleteAdapterV1.DeleteOutcome.AUTHORITATIVELY_ABSENT);
        assertThat(adapter.captureExactTarget(handle)
                        .toCompletableFuture()
                        .get(10, TimeUnit.SECONDS)
                        .outcome())
                .isEqualTo(M5BookKeeperDeleteAdapterV1.CaptureOutcome.DEFINITIVELY_ABSENT);
    }

    private static RealBookKeeperCellSessionV1 session() {
        return new RealBookKeeperCellSessionV1(client, capability, PASSWORD);
    }

    @Test
    void reservedIdentityReconcilesTheSameLedgerAfterRestartAndRejectsForeignRunAdoption() throws Exception {
        RealBookKeeperCellSessionV1 writer = session();
        var reserved = writer.reserveLedgerIdentity().toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertThat(reserved.outcome()).isEqualTo(ProviderMutationOutcomeV1.APPLIED_EXACT);
        var id = reserved.exactProof().orElseThrow();
        var configuration =
                RunLedgerConfigurationV1.from(capability, new StorageRunId(new Id128(0, RUN_IDS.incrementAndGet())));
        // The durable M5 task records this exact ID before creation; reconstruct the handle from those inputs.
        var expected = new RunLedgerHandleV1(
                capability.providerScopeId(), configuration.runId(), id, configuration.configurationDigest());
        var created = writer.createReservedRunLedger(configuration, id)
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
        assertThat(created.exactProof()).contains(expected);
        append(writer, expected, 0, new byte[] {3, 1, 4});
        writer.closeRunLedger(expected).toCompletableFuture().get(10, TimeUnit.SECONDS);
        writer.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);

        RealBookKeeperCellSessionV1 restarted = session();
        var duplicate = restarted
                .createReservedRunLedger(configuration, id)
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
        assertThat(duplicate.outcome()).isEqualTo(ProviderMutationOutcomeV1.FENCED_OR_CONFLICT);
        assertThat(restarted
                        .openRunLedger(expected)
                        .toCompletableFuture()
                        .get(10, TimeUnit.SECONDS)
                        .outcome())
                .isEqualTo(RunLedgerOpenOutcomeV1.OPENED_EXACT);
        assertThat(restarted
                        .readExactEntry(expected, 0)
                        .toCompletableFuture()
                        .get(10, TimeUnit.SECONDS)
                        .exactEntry()
                        .orElseThrow()
                        .payload()
                        .toByteArray())
                .containsExactly(3, 1, 4);
        var foreign = new RunLedgerHandleV1(
                expected.providerScopeId(),
                new StorageRunId(new Id128(0, RUN_IDS.incrementAndGet())),
                id,
                expected.configurationDigest());
        assertThat(restarted
                        .openRunLedger(foreign)
                        .toCompletableFuture()
                        .get(10, TimeUnit.SECONDS)
                        .outcome())
                .isEqualTo(RunLedgerOpenOutcomeV1.CONFIGURATION_MISMATCH);
        var next = restarted
                .reserveLedgerIdentity()
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS)
                .exactProof()
                .orElseThrow();
        assertThat(next).isNotEqualTo(id);
        restarted.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    private static RunLedgerHandleV1 create(RealBookKeeperCellSessionV1 session) throws Exception {
        RunLedgerConfigurationV1 configuration =
                RunLedgerConfigurationV1.from(capability, new StorageRunId(new Id128(0, RUN_IDS.incrementAndGet())));
        ProviderMutationResultV1<RunLedgerHandleV1> result =
                session.createRunLedger(configuration).toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertThat(result.outcome()).isEqualTo(ProviderMutationOutcomeV1.APPLIED_EXACT);
        return result.exactProof().orElseThrow();
    }

    private static void append(
            RealBookKeeperCellSessionV1 session, RunLedgerHandleV1 handle, long entryId, byte[] bytes)
            throws Exception {
        ImmutableRetainedStoragePayload payload = ImmutableRetainedStoragePayload.copyOf(bytes);
        var result = session.appendExplicitEntry(new RunLedgerAppendRequestV1(handle, entryId, payload))
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
        assertThat(payload.release()).isTrue();
        assertThat(result.outcome()).isEqualTo(ProviderMutationOutcomeV1.APPLIED_EXACT);
    }

    private static BookKeeperCapabilitySnapshotV1 capability() {
        int frameLimit = 5_242_880;
        return new BookKeeperCapabilitySnapshotV1(
                new CellProviderScopeId(digest(1)),
                "cd06340851d6d657b7c7546df01df365c18980de",
                Sha256Digest.copyOf(java.util.HexFormat.of()
                        .parseHex("8e64f2b7436bb814705f611eb0ac48d64d90de7a50d295905c459d89bc3f9d8f")),
                "cd06340851d6d657b7c7546df01df365c18980de",
                Sha256Digest.copyOf(java.util.HexFormat.of()
                        .parseHex("c0a128931c402d6bf6a6f973ba2f305b9be261659e30754ab95a29510a33bc0d")),
                BookKeeperProtocolModeV1.V3,
                frameLimit,
                frameLimit,
                BookKeeperV3Crc32cAddPayloadLimitV1.maximumAddPayloadBytes(frameLimit, frameLimit),
                true,
                3,
                3,
                2,
                BookKeeperDigestTypeV1.CRC32C,
                true,
                true,
                new BookKeeperTimeoutClassV1(10_000, 5_000, 5_000, 30_000),
                "bk-k0-no-auth:v1",
                Sha256Digest.copyOf(java.util.HexFormat.of()
                        .parseHex("eaf41c4b42b767b8ea6e86023a784425b8073f174dbade92b4249c8f3d301dbd")));
    }

    private static Sha256Digest digest(int lastByte) {
        byte[] bytes = new byte[Sha256Digest.LENGTH];
        bytes[bytes.length - 1] = (byte) lastByte;
        return Sha256Digest.copyOf(bytes);
    }
}
