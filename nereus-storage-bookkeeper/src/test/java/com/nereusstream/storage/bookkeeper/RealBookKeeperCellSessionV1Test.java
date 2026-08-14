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
import com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity;
import com.nereusstream.storage.api.bookkeeper.BookKeeperProtocolModeV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperTimeoutClassV1;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.bookkeeper.RunLedgerConfigurationV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.net.BookieId;
import org.junit.jupiter.api.Test;

class RealBookKeeperCellSessionV1Test {
    @Test
    void ledgerMetadataRoundTripsTheExactCellRunAndConfigurationIdentity() {
        RunLedgerConfigurationV1 configuration = configuration();
        Map<String, byte[]> customMetadata = RealBookKeeperCellSessionV1.metadata(configuration);
        RunLedgerHandleV1 handle = new RunLedgerHandleV1(
                configuration.providerScopeId(),
                configuration.runId(),
                new BookKeeperLedgerIdentity(41),
                configuration.configurationDigest());
        LedgerMetadata metadata = metadata(41, customMetadata);

        assertThat(RealBookKeeperCellSessionV1.metadataMatches(metadata, handle))
                .isTrue();
        assertThat(customMetadata)
                .containsOnlyKeys(
                        RealBookKeeperCellSessionV1.METADATA_SCHEMA,
                        RealBookKeeperCellSessionV1.METADATA_PROVIDER_SCOPE,
                        RealBookKeeperCellSessionV1.METADATA_RUN_ID,
                        RealBookKeeperCellSessionV1.METADATA_CONFIGURATION);
    }

    @Test
    void metadataMismatchNeverOpensAsTheExpectedRun() {
        RunLedgerConfigurationV1 configuration = configuration();
        Map<String, byte[]> customMetadata =
                new java.util.LinkedHashMap<>(RealBookKeeperCellSessionV1.metadata(configuration));
        customMetadata.put(RealBookKeeperCellSessionV1.METADATA_RUN_ID, new byte[Id128.LENGTH]);
        RunLedgerHandleV1 handle = new RunLedgerHandleV1(
                configuration.providerScopeId(),
                configuration.runId(),
                new BookKeeperLedgerIdentity(41),
                configuration.configurationDigest());

        assertThat(RealBookKeeperCellSessionV1.metadataMatches(metadata(41, customMetadata), handle))
                .isFalse();
        assertThat(RealBookKeeperCellSessionV1.metadataMatches(metadata(42, customMetadata), handle))
                .isFalse();
    }

    @Test
    void sessionRejectsAnyRunConfigurationOutsideTheAdmittedCapability() {
        BookKeeperCapabilitySnapshotV1 capability = capability();
        RealBookKeeperCellSessionV1 session =
                new RealBookKeeperCellSessionV1(noopClient(new AtomicInteger()), capability, new byte[0]);
        RunLedgerConfigurationV1 mismatch = new RunLedgerConfigurationV1(
                capability.providerScopeId(),
                new StorageRunId(new Id128(0, 9)),
                3,
                3,
                3,
                capability.digestType(),
                capability.configurationDigest());

        assertThatThrownBy(() -> session.createRunLedger(mismatch))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("differs from the admitted Cell capability");
    }

    @Test
    void closingCellSessionNeverClosesTheBorrowedBookKeeperTransport() {
        AtomicInteger transportCloseCalls = new AtomicInteger();
        RealBookKeeperCellSessionV1 session =
                new RealBookKeeperCellSessionV1(noopClient(transportCloseCalls), capability(), new byte[0]);

        assertThat(session.closeAsync().toCompletableFuture()).isCompletedWithValue(null);
        assertThat(session.closeAsync()).isSameAs(session.closeAsync());
        assertThat(transportCloseCalls).hasValue(0);
    }

    @Test
    void realProviderRejectsAFrameProjectionThatWasNotSourceLocked() {
        BookKeeperCapabilitySnapshotV1 source = capability();
        BookKeeperCapabilitySnapshotV1 mismatch = new BookKeeperCapabilitySnapshotV1(
                source.providerScopeId(),
                source.clientSourceCommit(),
                source.clientArtifactSha256(),
                source.serverSourceCommit(),
                source.serverImageManifestSha256(),
                source.protocolMode(),
                source.clientFrameLimitBytes(),
                source.serverFrameLimitBytes(),
                source.maximumAddPayloadBytes() - 1,
                source.explicitEntryIdsSupported(),
                source.ensembleSize(),
                source.writeQuorumSize(),
                source.ackQuorumSize(),
                source.digestType(),
                source.fencingSupported(),
                source.recoverySupported(),
                source.timeoutClass(),
                source.credentialIdentityVersion(),
                source.configurationDigest());

        assertThatThrownBy(
                        () -> new RealBookKeeperCellSessionV1(noopClient(new AtomicInteger()), mismatch, new byte[0]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exact v3 CRC32C frame projection");
    }

    private static BookKeeper noopClient(AtomicInteger closeCalls) {
        return (BookKeeper) Proxy.newProxyInstance(
                RealBookKeeperCellSessionV1Test.class.getClassLoader(),
                new Class<?>[] {BookKeeper.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("close")) {
                        closeCalls.incrementAndGet();
                        return null;
                    }
                    throw new AssertionError("unexpected borrowed-client call: " + method.getName());
                });
    }

    private static LedgerMetadata metadata(long ledgerId, Map<String, byte[]> customMetadata) {
        return LedgerMetadataBuilder.create()
                .withId(ledgerId)
                .withMetadataFormatVersion(3)
                .withPassword(new byte[0])
                .withDigestType(DigestType.CRC32C)
                .withEnsembleSize(3)
                .withWriteQuorumSize(3)
                .withAckQuorumSize(2)
                .withCustomMetadata(customMetadata)
                .newEnsembleEntry(
                        0,
                        List.of(
                                BookieId.parse("localhost:3181"),
                                BookieId.parse("localhost:3182"),
                                BookieId.parse("localhost:3183")))
                .build();
    }

    private static RunLedgerConfigurationV1 configuration() {
        BookKeeperCapabilitySnapshotV1 capability = capability();
        return RunLedgerConfigurationV1.from(capability, new StorageRunId(new Id128(0, 9)));
    }

    private static BookKeeperCapabilitySnapshotV1 capability() {
        int frameLimit = 5_242_880;
        return new BookKeeperCapabilitySnapshotV1(
                new CellProviderScopeId(digest(1)),
                "cd06340851d6d657b7c7546df01df365c18980de",
                digest(2),
                "cd06340851d6d657b7c7546df01df365c18980de",
                digest(3),
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
                digest(4));
    }

    private static Sha256Digest digest(int lastByte) {
        byte[] bytes = new byte[Sha256Digest.LENGTH];
        bytes[bytes.length - 1] = (byte) lastByte;
        return Sha256Digest.copyOf(bytes);
    }
}
