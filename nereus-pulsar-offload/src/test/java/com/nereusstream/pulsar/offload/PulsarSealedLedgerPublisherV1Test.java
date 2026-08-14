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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.pulsar.offload.PulsarOffloadObjectStoreV1.Body;
import com.nereusstream.pulsar.offload.PulsarOffloadObjectStoreV1.Capabilities;
import com.nereusstream.pulsar.offload.PulsarOffloadObjectStoreV1.ImmutableObject;
import com.nereusstream.pulsar.offload.PulsarSealedLedgerAttemptV1.DeleteState;
import com.nereusstream.pulsar.offload.PulsarSealedLedgerAttemptV1.RetentionClass;
import com.nereusstream.pulsar.offload.PulsarSealedLedgerPublisherV1.PreparedAttempt;
import com.nereusstream.pulsar.offload.PulsarSealedLedgerPublisherV1.Publication;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.CompressionFamily;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.EntryPayload;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.AttemptKeyEnvelope;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.CustomMetadataValue;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.DigestType;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.EnsembleSegment;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.SealedLedgerSection;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PulsarSealedLedgerPublisherV1Test {
    private static final PulsarOffloadLimitCandidateV1 LIMITS =
            PulsarOffloadLimitCandidateV1.adr0056EvidenceCandidate();
    private static final UUID ATTEMPT = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
    private static final AttemptKeyEnvelope KEY_ENVELOPE = new AttemptKeyEnvelope(
            1, "test-kms", "cells/pulsar-a/kms", "version-7", "aes-kwp", new byte[] {1, 2, 3, 4});

    @TempDir
    Path temporaryDirectory;

    @Test
    void publishesDataBeforeRootThenVerifiesTheActualReadPath() {
        PreparedAttempt prepared = prepared(ATTEMPT);
        InMemoryStore store = new InMemoryStore();
        AtomicInteger verified = new AtomicInteger();
        PulsarSealedLedgerPublisherV1 publisher = publisher(store, publication -> {
            verified.incrementAndGet();
            assertThat(Npo1CodecV1.parseCanonical(publication.rootBytes(), LIMITS))
                    .isEqualTo(publication.root());
            return CompletableFuture.completedFuture(null);
        });

        Publication publication =
                publisher.publish(prepared).toCompletableFuture().join();

        assertThat(store.calls)
                .containsExactly(
                        "create:" + prepared.attempt().keys().dataKey(),
                        "create:" + prepared.attempt().keys().rootKey());
        assertThat(verified).hasValue(1);
        assertThat(publication.root().dataExtent().immutableVersion())
                .isEqualTo(publication.dataObject().immutableVersion());
    }

    @Test
    void resolvesLostDataAndRootCreateResponsesByExactHeadProof() {
        PreparedAttempt prepared = prepared(ATTEMPT);
        InMemoryStore store = new InMemoryStore();
        store.loseCreateResponse.add(prepared.attempt().keys().dataKey());
        store.loseCreateResponse.add(prepared.attempt().keys().rootKey());

        publisher(store, publication -> CompletableFuture.completedFuture(null))
                .publish(prepared)
                .toCompletableFuture()
                .join();

        assertThat(store.calls)
                .containsExactly(
                        "create:" + prepared.attempt().keys().dataKey(),
                        "head:" + prepared.attempt().keys().dataKey(),
                        "create:" + prepared.attempt().keys().rootKey(),
                        "head:" + prepared.attempt().keys().rootKey());
    }

    @Test
    void rejectsADataProofMismatchBeforePublishingRoot() {
        PreparedAttempt prepared = prepared(ATTEMPT);
        InMemoryStore store = new InMemoryStore();
        store.corruptCreateProof.add(prepared.attempt().keys().dataKey());

        assertThatThrownBy(() -> publisher(store, publication -> CompletableFuture.completedFuture(null))
                        .publish(prepared)
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("immutable Object proof differs from canonical body");
        assertThat(store.calls)
                .containsExactly("create:" + prepared.attempt().keys().dataKey());
    }

    @Test
    void rejectsARootProofMismatchBeforeReadPathVerification() {
        PreparedAttempt prepared = prepared(ATTEMPT);
        InMemoryStore store = new InMemoryStore();
        store.corruptCreateProof.add(prepared.attempt().keys().rootKey());
        AtomicInteger verified = new AtomicInteger();

        assertThatThrownBy(() -> publisher(store, publication -> {
                            verified.incrementAndGet();
                            return CompletableFuture.completedFuture(null);
                        })
                        .publish(prepared)
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("immutable Object proof differs from canonical body");
        assertThat(verified).hasValue(0);
    }

    @Test
    void readPathVerificationFailurePreventsOffloadCompletion() {
        PreparedAttempt prepared = prepared(ATTEMPT);
        InMemoryStore store = new InMemoryStore();

        assertThatThrownBy(() -> publisher(
                                store,
                                publication -> CompletableFuture.failedFuture(new IOException("read path differs")))
                        .publish(prepared)
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("read path differs");
        assertThat(store.objects)
                .containsKeys(
                        prepared.attempt().keys().dataKey(),
                        prepared.attempt().keys().rootKey());
    }

    @Test
    void detectsStagedDataMutationBeforeTheFirstProviderCall() throws Exception {
        PreparedAttempt prepared = prepared(ATTEMPT);
        Files.write(prepared.dataObject().path(), new byte[] {1}, StandardOpenOption.APPEND);
        InMemoryStore store = new InMemoryStore();

        assertThatThrownBy(() -> publisher(store, publication -> CompletableFuture.completedFuture(null))
                        .publish(prepared)
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("staged NPD1 length or SHA changed before upload");
        assertThat(store.calls).isEmpty();
    }

    @Test
    void cleanupProvesRootAbsentBeforeDataThenMultipartResidue() {
        PreparedAttempt prepared = prepared(ATTEMPT);
        InMemoryStore store = new InMemoryStore();
        PulsarSealedLedgerPublisherV1 publisher =
                publisher(store, publication -> CompletableFuture.completedFuture(null));
        publisher.publish(prepared).toCompletableFuture().join();
        store.calls.clear();

        publisher.deleteAttempt(prepared.attempt()).toCompletableFuture().join();

        PulsarOffloadKeysV1 keys = prepared.attempt().keys();
        assertThat(store.calls)
                .containsExactly(
                        "delete:" + keys.rootKey(), "delete:" + keys.dataKey(), "cleanup:" + keys.attemptPrefix());
        assertThat(store.objects).doesNotContainKeys(keys.rootKey(), keys.dataKey());
    }

    @Test
    void cleanupNeverDeletesDataWhenRootAbsenceIsUnproven() {
        PreparedAttempt prepared = prepared(ATTEMPT);
        InMemoryStore store = new InMemoryStore();
        store.failDelete.add(prepared.attempt().keys().rootKey());

        assertThatThrownBy(() -> publisher(store, publication -> CompletableFuture.completedFuture(null))
                        .deleteAttempt(prepared.attempt())
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("delete unavailable");
        assertThat(store.calls)
                .containsExactly("delete:" + prepared.attempt().keys().rootKey());
    }

    @Test
    void attemptUuidKeepsPublicationAndCleanupIsolated() {
        PreparedAttempt first = prepared(ATTEMPT);
        PreparedAttempt second = prepared(UUID.fromString("223e4567-e89b-12d3-a456-426614174000"));
        InMemoryStore store = new InMemoryStore();
        PulsarSealedLedgerPublisherV1 publisher =
                publisher(store, publication -> CompletableFuture.completedFuture(null));
        publisher.publish(first).toCompletableFuture().join();
        publisher.publish(second).toCompletableFuture().join();

        publisher.deleteAttempt(first.attempt()).toCompletableFuture().join();

        assertThat(store.objects)
                .containsKeys(
                        second.attempt().keys().dataKey(),
                        second.attempt().keys().rootKey())
                .doesNotContainKeys(
                        first.attempt().keys().dataKey(), first.attempt().keys().rootKey());
    }

    private PulsarSealedLedgerPublisherV1 publisher(
            InMemoryStore store, PulsarSealedLedgerPublisherV1.PublishedAttemptVerifier verifier) {
        return new PulsarSealedLedgerPublisherV1(store, LIMITS, verifier, Runnable::run);
    }

    private PreparedAttempt prepared(UUID attemptUuid) {
        List<EntryPayload> entries = List.of(
                new EntryPayload(0, new byte[] {0, 1, 2}),
                new EntryPayload(1, new byte[] {3, 4, 5}),
                new EntryPayload(2, new byte[] {6, 7, 8}),
                new EntryPayload(3, new byte[] {9, 10, 11}),
                new EntryPayload(4, new byte[] {12, 13, 14}));
        Path dataPath = temporaryDirectory.resolve(attemptUuid + ".npd1");
        Npd1CodecV1.DataObject data = Npd1CodecV1.encode(
                dataPath,
                entries,
                PulsarOffloadLimitCandidateV1.MIB,
                CompressionFamily.NONE,
                new SecretKeySpec(new byte[32], "AES"),
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
        return new PreparedAttempt(attempt, sealed, data, PulsarOffloadLimitCandidateV1.MIB, KEY_ENVELOPE);
    }

    private static final class InMemoryStore implements PulsarOffloadObjectStoreV1 {
        private final Map<String, StoredObject> objects = new HashMap<>();
        private final List<String> calls = new ArrayList<>();
        private final Set<String> loseCreateResponse = new HashSet<>();
        private final Set<String> corruptCreateProof = new HashSet<>();
        private final Set<String> failDelete = new HashSet<>();

        @Override
        public Capabilities capabilities() {
            return new Capabilities(
                    LIMITS.maxDataObjectBytes(), 1, LIMITS.maxDataObjectBytes(), 1_024, true, true, true, true, true);
        }

        @Override
        public CompletionStage<ImmutableObject> createImmutable(String key, Body body) {
            calls.add("create:" + key);
            try {
                byte[] bytes;
                try (var input = body.inputStreamFactory().open()) {
                    bytes = input.readAllBytes();
                }
                if (bytes.length != body.bytes() || !sha256(bytes).equals(body.sha256())) {
                    return CompletableFuture.failedFuture(new IOException("request body differs"));
                }
                StoredObject stored =
                        new StoredObject("version-" + HexFormat.of().formatHex(digest(key)), bytes, body.sha256());
                StoredObject previous = objects.putIfAbsent(key, stored);
                if (previous != null && !previous.sameBody(stored)) {
                    return CompletableFuture.failedFuture(new IOException("conditional create conflict"));
                }
                StoredObject resolved = previous == null ? stored : previous;
                if (loseCreateResponse.contains(key)) {
                    return CompletableFuture.failedFuture(new IOException("response lost"));
                }
                if (corruptCreateProof.contains(key)) {
                    return CompletableFuture.completedFuture(
                            new ImmutableObject(resolved.version, resolved.bytes.length, "f".repeat(64)));
                }
                return CompletableFuture.completedFuture(resolved.proof());
            } catch (IOException failure) {
                return CompletableFuture.failedFuture(failure);
            }
        }

        @Override
        public CompletionStage<ImmutableObject> head(String key) {
            calls.add("head:" + key);
            StoredObject stored = objects.get(key);
            return stored == null
                    ? CompletableFuture.failedFuture(new IOException("absent"))
                    : CompletableFuture.completedFuture(stored.proof());
        }

        @Override
        public CompletionStage<byte[]> readRange(String key, long offset, int length) {
            StoredObject stored = objects.get(key);
            if (stored == null || offset < 0 || length < 0 || offset + length > stored.bytes.length) {
                return CompletableFuture.failedFuture(new IOException("range absent"));
            }
            return CompletableFuture.completedFuture(
                    Arrays.copyOfRange(stored.bytes, Math.toIntExact(offset), Math.toIntExact(offset + length)));
        }

        @Override
        public CompletionStage<Void> deleteAndProveAbsent(String key) {
            calls.add("delete:" + key);
            if (failDelete.contains(key)) {
                return CompletableFuture.failedFuture(new IOException("delete unavailable"));
            }
            objects.remove(key);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> cleanupAttemptMultipartResidue(String attemptPrefix) {
            calls.add("cleanup:" + attemptPrefix);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> close() {
            return CompletableFuture.completedFuture(null);
        }

        private static byte[] digest(String value) {
            try {
                return MessageDigest.getInstance("SHA-256")
                        .digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            } catch (Exception failure) {
                throw new AssertionError(failure);
            }
        }

        private static String sha256(byte[] bytes) {
            try {
                return HexFormat.of()
                        .formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
            } catch (Exception failure) {
                throw new AssertionError(failure);
            }
        }
    }

    private record StoredObject(String version, byte[] bytes, String sha) {
        private ImmutableObject proof() {
            return new ImmutableObject(version, bytes.length, sha);
        }

        private boolean sameBody(StoredObject other) {
            return sha.equals(other.sha) && Arrays.equals(bytes, other.bytes);
        }
    }
}
