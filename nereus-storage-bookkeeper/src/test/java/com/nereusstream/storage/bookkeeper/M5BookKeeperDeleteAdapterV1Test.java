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
import com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity;
import com.nereusstream.storage.api.bookkeeper.BookKeeperProtocolModeV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperTimeoutClassV1;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.bookkeeper.RunLedgerConfigurationV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.net.BookieId;
import org.junit.jupiter.api.Test;

class M5BookKeeperDeleteAdapterV1Test {
    private static final byte[] PASSWORD = new byte[0];

    @Test
    void capturesOnlyTheExactClosedLedgerMetadataIdentity() throws Exception {
        Fixture fixture = fixture();
        fixture.client().reads.add(metadata(fixture.configuration(), 41, true, 7, 800));

        var captured = fixture.adapter()
                .captureExactTarget(fixture.handle())
                .toCompletableFuture()
                .get(5, TimeUnit.SECONDS);

        assertThat(captured.outcome()).isEqualTo(M5BookKeeperDeleteAdapterV1.CaptureOutcome.EXACT_TARGET);
        var target = captured.exactTarget().orElseThrow();
        assertThat(target.handle()).isEqualTo(fixture.handle());
        assertThat(target.sealedLastEntryId()).isEqualTo(7);
        assertThat(target.sealedLength()).isEqualTo(800);
        assertThat(target.metadataFormatVersion()).isEqualTo(3);
        assertThat(target.metadataCToken()).isEqualTo(91);
        assertThat(target.passwordCredentialIdentityVersion()).isEqualTo("bk-k0-no-auth:v1");
        assertThat(target.metadataSha256().isZero()).isFalse();
    }

    @Test
    void unsealedOrReboundMetadataNeverBecomesADeleteTarget() throws Exception {
        Fixture fixture = fixture();
        fixture.client().reads.add(metadata(fixture.configuration(), 41, false, -1, 0));
        RunLedgerConfigurationV1 rebound =
                RunLedgerConfigurationV1.from(fixture.capability(), new StorageRunId(new Id128(0, 99)));
        fixture.client().reads.add(metadata(rebound, 41, true, 0, 10));

        assertThat(fixture.adapter()
                        .captureExactTarget(fixture.handle())
                        .toCompletableFuture()
                        .get(5, TimeUnit.SECONDS)
                        .outcome())
                .isEqualTo(M5BookKeeperDeleteAdapterV1.CaptureOutcome.DIFFERENT_OR_UNSEALED);
        assertThat(fixture.adapter()
                        .captureExactTarget(fixture.handle())
                        .toCompletableFuture()
                        .get(5, TimeUnit.SECONDS)
                        .outcome())
                .isEqualTo(M5BookKeeperDeleteAdapterV1.CaptureOutcome.DIFFERENT_OR_UNSEALED);
        assertThat(fixture.client().deleteCalls).hasValue(0);
    }

    @Test
    void definitivePreReadAbsenceCompletesWithoutDispatch() throws Exception {
        Fixture fixture = fixture();
        var target = captureTarget(fixture);
        fixture.client().reads.add(noSuchLedger());

        var result = fixture.adapter()
                .deleteAndReconcile(target)
                .toCompletableFuture()
                .get(5, TimeUnit.SECONDS);

        assertThat(result.outcome()).isEqualTo(M5BookKeeperDeleteAdapterV1.DeleteOutcome.AUTHORITATIVELY_ABSENT);
        assertThat(fixture.client().deleteCalls).hasValue(0);
    }

    @Test
    void deleteSuccessRequiresAuthoritativePostReadAbsence() throws Exception {
        Fixture fixture = fixture();
        var target = captureTarget(fixture);
        fixture.client().reads.add(metadata(fixture.configuration(), 41, true, 7, 800));
        fixture.client().reads.add(noSuchLedgerOnMetadataServer());
        fixture.client().deletes.add(null);

        var result = fixture.adapter()
                .deleteAndReconcile(target)
                .toCompletableFuture()
                .get(5, TimeUnit.SECONDS);

        assertThat(result.outcome()).isEqualTo(M5BookKeeperDeleteAdapterV1.DeleteOutcome.AUTHORITATIVELY_ABSENT);
        assertThat(fixture.client().deleteCalls).hasValue(1);
    }

    @Test
    void lostDeleteResponseCanOnlyResolveThroughAuthoritativeAbsence() throws Exception {
        Fixture fixture = fixture();
        var target = captureTarget(fixture);
        fixture.client().reads.add(metadata(fixture.configuration(), 41, true, 7, 800));
        fixture.client().reads.add(noSuchLedger());
        fixture.client().deletes.add(new IllegalStateException("discarded response"));

        var result = fixture.adapter()
                .deleteAndReconcile(target)
                .toCompletableFuture()
                .get(5, TimeUnit.SECONDS);

        assertThat(result.outcome()).isEqualTo(M5BookKeeperDeleteAdapterV1.DeleteOutcome.AUTHORITATIVELY_ABSENT);
    }

    @Test
    void exactLedgerRemainingIsRetryableButChangedMetadataIsAConflict() throws Exception {
        Fixture fixture = fixture();
        var target = captureTarget(fixture);
        fixture.client().reads.add(metadata(fixture.configuration(), 41, true, 7, 800));
        fixture.client().reads.add(metadata(fixture.configuration(), 41, true, 7, 800));
        fixture.client().deletes.add(new IllegalStateException("timeout"));

        assertThat(fixture.adapter()
                        .deleteAndReconcile(target)
                        .toCompletableFuture()
                        .get(5, TimeUnit.SECONDS)
                        .outcome())
                .isEqualTo(M5BookKeeperDeleteAdapterV1.DeleteOutcome.EXACT_LEDGER_REMAINS);

        fixture.client().reads.add(metadata(fixture.configuration(), 41, true, 7, 800));
        fixture.client().reads.add(metadata(fixture.configuration(), 41, true, 7, 801));
        fixture.client().deletes.add(null);
        assertThat(fixture.adapter()
                        .deleteAndReconcile(target)
                        .toCompletableFuture()
                        .get(5, TimeUnit.SECONDS)
                        .outcome())
                .isEqualTo(M5BookKeeperDeleteAdapterV1.DeleteOutcome.DIFFERENT_LEDGER_OR_METADATA);
    }

    @Test
    void ambiguousReadsStayUnknownBeforeAndAfterDispatch() throws Exception {
        Fixture fixture = fixture();
        var target = captureTarget(fixture);
        fixture.client().reads.add(new IllegalStateException("metadata unavailable"));

        assertThat(fixture.adapter()
                        .deleteAndReconcile(target)
                        .toCompletableFuture()
                        .get(5, TimeUnit.SECONDS)
                        .outcome())
                .isEqualTo(M5BookKeeperDeleteAdapterV1.DeleteOutcome.OUTCOME_UNKNOWN);
        assertThat(fixture.client().deleteCalls).hasValue(0);

        fixture.client().reads.add(metadata(fixture.configuration(), 41, true, 7, 800));
        fixture.client().reads.add(new IllegalStateException("post-delete metadata unavailable"));
        fixture.client().deletes.add(null);
        assertThat(fixture.adapter()
                        .deleteAndReconcile(target)
                        .toCompletableFuture()
                        .get(5, TimeUnit.SECONDS)
                        .outcome())
                .isEqualTo(M5BookKeeperDeleteAdapterV1.DeleteOutcome.OUTCOME_UNKNOWN);
    }

    private static M5BookKeeperDeleteAdapterV1.BookKeeperDeleteTargetV1 captureTarget(Fixture fixture)
            throws Exception {
        fixture.client().reads.add(metadata(fixture.configuration(), 41, true, 7, 800));
        return fixture.adapter()
                .captureExactTarget(fixture.handle())
                .toCompletableFuture()
                .get(5, TimeUnit.SECONDS)
                .exactTarget()
                .orElseThrow();
    }

    private static Throwable noSuchLedger() {
        return BKException.create(org.apache.bookkeeper.client.api.BKException.Code.NoSuchLedgerExistsException);
    }

    private static Throwable noSuchLedgerOnMetadataServer() {
        return BKException.create(
                org.apache.bookkeeper.client.api.BKException.Code.NoSuchLedgerExistsOnMetadataServerException);
    }

    private static LedgerMetadata metadata(
            RunLedgerConfigurationV1 configuration, long ledgerId, boolean closed, long lastEntryId, long length) {
        LedgerMetadataBuilder builder = LedgerMetadataBuilder.create()
                .withId(ledgerId)
                .withMetadataFormatVersion(3)
                .withPassword(PASSWORD)
                .withDigestType(DigestType.CRC32C)
                .withEnsembleSize(3)
                .withWriteQuorumSize(3)
                .withAckQuorumSize(2)
                .withCustomMetadata(RealBookKeeperCellSessionV1.metadata(configuration))
                .withCreationTime(1234)
                .storingCreationTime(true)
                .withCToken(91)
                .newEnsembleEntry(
                        0,
                        List.of(
                                BookieId.parse("localhost:3181"),
                                BookieId.parse("localhost:3182"),
                                BookieId.parse("localhost:3183")));
        if (closed) {
            builder.withClosedState().withLastEntryId(lastEntryId).withLength(length);
        }
        return builder.build();
    }

    private static Fixture fixture() {
        BookKeeperCapabilitySnapshotV1 capability = capability();
        RunLedgerConfigurationV1 configuration =
                RunLedgerConfigurationV1.from(capability, new StorageRunId(new Id128(0, 9)));
        RunLedgerHandleV1 handle = new RunLedgerHandleV1(
                configuration.providerScopeId(),
                configuration.runId(),
                new BookKeeperLedgerIdentity(41),
                configuration.configurationDigest());
        FakeDeleteClient client = new FakeDeleteClient();
        return new Fixture(
                client,
                capability,
                configuration,
                handle,
                new M5BookKeeperDeleteAdapterV1(client, capability, PASSWORD));
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

    private record Fixture(
            FakeDeleteClient client,
            BookKeeperCapabilitySnapshotV1 capability,
            RunLedgerConfigurationV1 configuration,
            RunLedgerHandleV1 handle,
            M5BookKeeperDeleteAdapterV1 adapter) {}

    private static final class FakeDeleteClient implements M5BookKeeperDeleteAdapterV1.DeleteClient {
        private final Queue<Object> reads = new ArrayDeque<>();
        private final Queue<Object> deletes = new java.util.LinkedList<>();
        private final AtomicInteger deleteCalls = new AtomicInteger();

        @Override
        public CompletionStage<LedgerMetadata> read(long ledgerId) {
            Object result = reads.remove();
            if (result instanceof Throwable failure) {
                return CompletableFuture.failedFuture(failure);
            }
            return CompletableFuture.completedFuture((LedgerMetadata) result);
        }

        @Override
        public CompletionStage<Void> delete(long ledgerId) {
            deleteCalls.incrementAndGet();
            Object result = deletes.remove();
            if (result instanceof Throwable failure) {
                return CompletableFuture.failedFuture(failure);
            }
            return CompletableFuture.completedFuture(null);
        }
    }
}
