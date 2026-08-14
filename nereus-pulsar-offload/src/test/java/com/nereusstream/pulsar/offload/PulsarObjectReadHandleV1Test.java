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

package com.nereusstream.pulsar.offload;

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.pulsar.offload.PulsarObjectReadHandleV1.ObjectReadException;
import com.nereusstream.pulsar.offload.PulsarObjectReadHandleV1.ReadFailureKind;
import com.nereusstream.pulsar.offload.PulsarOffloadObjectStoreV1.Body;
import com.nereusstream.pulsar.offload.PulsarOffloadObjectStoreV1.Capabilities;
import com.nereusstream.pulsar.offload.PulsarOffloadObjectStoreV1.FailureKind;
import com.nereusstream.pulsar.offload.PulsarOffloadObjectStoreV1.ImmutableObject;
import com.nereusstream.pulsar.offload.PulsarOffloadObjectStoreV1.ObjectStoreException;
import com.nereusstream.pulsar.offload.PulsarSealedLedgerAttemptV1.DeleteState;
import com.nereusstream.pulsar.offload.PulsarSealedLedgerAttemptV1.RetentionClass;
import com.nereusstream.pulsar.offload.PulsarSealedLedgerPublisherV1.PreparedAttempt;
import com.nereusstream.pulsar.offload.PulsarSealedLedgerPublisherV1.Publication;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.CompressionFamily;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.EntryPayload;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.CustomMetadataValue;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.DigestType;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.EnsembleSegment;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.SealedLedgerSection;
import java.io.IOException;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PulsarObjectReadHandleV1Test {
    private static final UUID ATTEMPT = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
    private static final SecretKey KEY = new SecretKeySpec(new byte[32], "AES");
    private static final PulsarOffloadLimitCandidateV1 LIMITS = new PulsarOffloadLimitCandidateV1(
            PulsarOffloadLimitCandidateV1.FOUR_GIB,
            1_024,
            64L * PulsarOffloadLimitCandidateV1.MIB,
            64L * PulsarOffloadLimitCandidateV1.MIB,
            2,
            List.of(
                    PulsarOffloadLimitCandidateV1.MIB,
                    4 * PulsarOffloadLimitCandidateV1.MIB,
                    8 * PulsarOffloadLimitCandidateV1.MIB,
                    16 * PulsarOffloadLimitCandidateV1.MIB));

    @TempDir
    Path temporaryDirectory;

    @Test
    void opensTheBoundedPairAndReadsExactRangesAcrossSparseBlocks() {
        Fixture fixture = fixture();
        PulsarObjectReadHandleV1 handle = open(fixture);

        assertThat(handle.read(0, 0).toCompletableFuture().join())
                .extracting(EntryPayload::entryId)
                .containsExactly(0L);
        assertThat(handle.read(1, 3).toCompletableFuture().join())
                .extracting(EntryPayload::entryId)
                .containsExactly(1L, 2L, 3L);
        assertThat(handle.read(4, 4).toCompletableFuture().join())
                .extracting(EntryPayload::entryId)
                .containsExactly(4L);
    }

    @Test
    void verifiesTheCompleteDataShaCountsLengthAndEveryBlock() {
        Fixture fixture = fixture();
        PulsarObjectReadHandleV1.VerificationReport report =
                open(fixture).verifyCompleteLedger().toCompletableFuture().join();

        assertThat(report.entryCount()).isEqualTo(5);
        assertThat(report.logicalLength()).isEqualTo(15);
        assertThat(report.blockCount()).isEqualTo(3);
        assertThat(report.dataSha256())
                .isEqualTo(fixture.publication.root().dataExtent().dataSha256());
    }

    @Test
    void productionPublicationVerifierUsesTheRealObjectReadPath() {
        PreparedAttempt prepared = prepared(ATTEMPT);
        InMemoryStore store = new InMemoryStore();
        PulsarPublishedAttemptVerifierV1 verifier = new PulsarPublishedAttemptVerifierV1(store, LIMITS, attempt -> KEY);
        PulsarSealedLedgerPublisherV1 publisher =
                new PulsarSealedLedgerPublisherV1(store, LIMITS, verifier, Runnable::run);

        Publication publication =
                publisher.publish(prepared).toCompletableFuture().join();

        assertThat(publication.root().sparseIndex()).hasSize(3);
        assertThat(store.rangeReadCount).isGreaterThanOrEqualTo(5);
    }

    @Test
    void rejectsRootOuterDigestAndSelfDigestCorruptionBeforeIndexUse() {
        Fixture outer = fixture();
        outer.store.flipByte(outer.prepared.attempt().keys().rootKey(), false);
        assertThat(failure(PulsarObjectReadHandleV1.open(outer.store, LIMITS, outer.prepared.attempt(), KEY))
                        .kind())
                .isEqualTo(ReadFailureKind.INTEGRITY);

        Fixture self = fixture();
        self.store.flipByte(self.prepared.attempt().keys().rootKey(), true);
        assertThat(failure(PulsarObjectReadHandleV1.open(self.store, LIMITS, self.prepared.attempt(), KEY))
                        .kind())
                .isEqualTo(ReadFailureKind.INTEGRITY);
        assertThat(self.store.rangeReadCount).isEqualTo(1);
    }

    @Test
    void rejectsShortRootAndMissingRootWithTypedFailures() {
        Fixture shortRoot = fixture();
        shortRoot.store.shortReadKey = shortRoot.prepared.attempt().keys().rootKey();
        assertThat(failure(PulsarObjectReadHandleV1.open(shortRoot.store, LIMITS, shortRoot.prepared.attempt(), KEY))
                        .kind())
                .isEqualTo(ReadFailureKind.SHORT_READ);

        Fixture missing = fixture();
        missing.store.objects.remove(missing.prepared.attempt().keys().rootKey());
        assertThat(failure(PulsarObjectReadHandleV1.open(missing.store, LIMITS, missing.prepared.attempt(), KEY))
                        .kind())
                .isEqualTo(ReadFailureKind.NOT_FOUND);
    }

    @Test
    void rejectsDataImmutableVersionSubstitutionAtOpen() {
        Fixture fixture = fixture();
        fixture.store.changeVersion(fixture.prepared.attempt().keys().dataKey());

        assertThat(failure(PulsarObjectReadHandleV1.open(fixture.store, LIMITS, fixture.prepared.attempt(), KEY))
                        .kind())
                .isEqualTo(ReadFailureKind.INTEGRITY);
    }

    @Test
    void rejectsCorruptEncodedBlockWithoutReturningAPartialRange() {
        Fixture fixture = fixture();
        PulsarObjectReadHandleV1 handle = open(fixture);
        fixture.store.corruptOffset =
                fixture.publication.root().sparseIndex().get(1).blockOffset();

        assertThat(failure(handle.read(1, 3)).kind()).isEqualTo(ReadFailureKind.INTEGRITY);
    }

    @Test
    void rejectsInvalidRangesAndReadsAfterCloseWithoutProviderIo() {
        Fixture fixture = fixture();
        PulsarObjectReadHandleV1 handle = open(fixture);
        int before = fixture.store.rangeReadCount;

        assertThat(failure(handle.read(3, 2)).kind()).isEqualTo(ReadFailureKind.INVALID_RANGE);
        handle.close().toCompletableFuture().join();
        assertThat(failure(handle.read(0, 0)).kind()).isEqualTo(ReadFailureKind.CLOSED);
        assertThat(fixture.store.rangeReadCount).isEqualTo(before);
    }

    private PulsarObjectReadHandleV1 open(Fixture fixture) {
        return PulsarObjectReadHandleV1.open(fixture.store, LIMITS, fixture.prepared.attempt(), KEY)
                .toCompletableFuture()
                .join();
    }

    private Fixture fixture() {
        PreparedAttempt prepared = prepared(ATTEMPT);
        InMemoryStore store = new InMemoryStore();
        Publication publication = new PulsarSealedLedgerPublisherV1(
                        store, LIMITS, candidate -> CompletableFuture.completedFuture(null), Runnable::run)
                .publish(prepared)
                .toCompletableFuture()
                .join();
        store.rangeReadCount = 0;
        return new Fixture(prepared, publication, store);
    }

    private PreparedAttempt prepared(UUID attemptUuid) {
        List<EntryPayload> entries = List.of(
                new EntryPayload(0, new byte[] {0, 1, 2}),
                new EntryPayload(1, new byte[] {3, 4, 5}),
                new EntryPayload(2, new byte[] {6, 7, 8}),
                new EntryPayload(3, new byte[] {9, 10, 11}),
                new EntryPayload(4, new byte[] {12, 13, 14}));
        Npd1CodecV1.DataObject data = Npd1CodecV1.encode(
                temporaryDirectory.resolve(attemptUuid + ".npd1"),
                entries,
                PulsarOffloadLimitCandidateV1.MIB,
                CompressionFamily.NONE,
                KEY,
                attemptUuid,
                LIMITS);
        PulsarSealedLedgerAttemptV1 attempt = new PulsarSealedLedgerAttemptV1(
                42,
                attemptUuid,
                4,
                5,
                15,
                100,
                7,
                "cells/pulsar-a",
                RetentionClass.DELETE_AFTER_VERIFIED,
                DeleteState.BK_DELETE_NONE,
                false);
        Map<String, CustomMetadataValue> metadata = new LinkedHashMap<>();
        metadata.put("binary", new CustomMetadataValue(new byte[] {0, (byte) 0xff}));
        SealedLedgerSection sealed = new SealedLedgerSection(
                4,
                5,
                15,
                100,
                7,
                3,
                3,
                2,
                DigestType.CRC32C,
                metadata,
                List.of(new EnsembleSegment(0, List.of("bookie-1", "bookie-2", "bookie-3"))));
        return new PreparedAttempt(attempt, sealed, data, PulsarOffloadLimitCandidateV1.MIB);
    }

    private static ObjectReadException failure(CompletionStage<?> stage) {
        try {
            stage.toCompletableFuture().join();
            throw new AssertionError("expected Object read failure");
        } catch (CompletionException failure) {
            Throwable cause = failure.getCause();
            if (cause instanceof ObjectReadException objectReadException) {
                return objectReadException;
            }
            throw failure;
        }
    }

    private record Fixture(PreparedAttempt prepared, Publication publication, InMemoryStore store) {}

    private static final class InMemoryStore implements PulsarOffloadObjectStoreV1 {
        private final Map<String, StoredObject> objects = new HashMap<>();
        private String shortReadKey;
        private long corruptOffset = -1;
        private int rangeReadCount;

        @Override
        public Capabilities capabilities() {
            return new Capabilities(
                    LIMITS.maxDataObjectBytes(), 1, LIMITS.maxDataObjectBytes(), 1_024, true, true, true, true, true);
        }

        @Override
        public CompletionStage<ImmutableObject> createImmutable(String key, Body body) {
            try {
                byte[] bytes;
                try (var input = body.inputStreamFactory().open()) {
                    bytes = input.readAllBytes();
                }
                StoredObject stored = new StoredObject("version-" + objects.size(), bytes, body.sha256());
                StoredObject previous = objects.putIfAbsent(key, stored);
                if (previous != null && !previous.sha.equals(stored.sha)) {
                    return CompletableFuture.failedFuture(
                            new ObjectStoreException(FailureKind.CONFLICT, "conditional conflict"));
                }
                return CompletableFuture.completedFuture((previous == null ? stored : previous).proof());
            } catch (IOException failure) {
                return CompletableFuture.failedFuture(failure);
            }
        }

        @Override
        public CompletionStage<ImmutableObject> head(String key) {
            StoredObject stored = objects.get(key);
            return stored == null
                    ? CompletableFuture.failedFuture(new ObjectStoreException(FailureKind.NOT_FOUND, "absent"))
                    : CompletableFuture.completedFuture(stored.proof());
        }

        @Override
        public CompletionStage<byte[]> readRange(String key, long offset, int length) {
            rangeReadCount++;
            StoredObject stored = objects.get(key);
            if (stored == null) {
                return CompletableFuture.failedFuture(new ObjectStoreException(FailureKind.NOT_FOUND, "absent"));
            }
            int actualLength = key.equals(shortReadKey) ? Math.max(0, length - 1) : length;
            if (offset < 0 || offset + actualLength > stored.bytes.length) {
                return CompletableFuture.failedFuture(new ObjectStoreException(FailureKind.SHORT_READ, "outside"));
            }
            byte[] result =
                    Arrays.copyOfRange(stored.bytes, Math.toIntExact(offset), Math.toIntExact(offset + actualLength));
            if (offset == corruptOffset && result.length > 0) {
                result[result.length / 2] ^= 1;
            }
            return CompletableFuture.completedFuture(result);
        }

        @Override
        public CompletionStage<Void> deleteAndProveAbsent(String key) {
            objects.remove(key);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> cleanupAttemptMultipartResidue(String attemptPrefix) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> close() {
            return CompletableFuture.completedFuture(null);
        }

        private void flipByte(String key, boolean reproof) {
            StoredObject stored = objects.get(key);
            byte[] changed = stored.bytes.clone();
            changed[changed.length / 2] ^= 1;
            objects.put(key, new StoredObject(stored.version, changed, reproof ? sha256(changed) : stored.sha));
        }

        private void changeVersion(String key) {
            StoredObject stored = objects.get(key);
            objects.put(key, new StoredObject(stored.version + "-changed", stored.bytes, stored.sha));
        }
    }

    private record StoredObject(String version, byte[] bytes, String sha) {
        private ImmutableObject proof() {
            return new ImmutableObject(version, bytes.length, sha);
        }
    }

    private static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (Exception failure) {
            throw new AssertionError(failure);
        }
    }
}
