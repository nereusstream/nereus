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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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

    private static RealBookKeeperCellSessionV1 session() {
        return new RealBookKeeperCellSessionV1(client, capability, PASSWORD);
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
