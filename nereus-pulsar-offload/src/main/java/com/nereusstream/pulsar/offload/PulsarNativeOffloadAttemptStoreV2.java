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

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.pulsar.offload.NereusPulsarLedgerOffloaderV1.AttemptKeyManager;
import com.nereusstream.pulsar.offload.NereusPulsarLedgerOffloaderV1.PreparedAttemptKey;
import com.nereusstream.pulsar.offload.PulsarOffloadObjectStoreV1.Body;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.AttemptSection;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.SealedLedgerSection;
import com.nereusstream.storage.object.retention.M5MaterializationWorkerV1;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/** An immutable attempt key and its two chosen Object bodies, outside the ManagedLedger publication authority. */
final class PulsarNativeOffloadAttemptStoreV2 {
    private static final int MAGIC = 0x4e4f4931; // NOI1
    private static final int MAX_ATTEMPT_BYTES = 64 * 1024;
    private final ExactMetadataTransactionStoreV1 store;
    private final String root;

    PulsarNativeOffloadAttemptStoreV2(
            ExactMetadataTransactionStoreV1 store, String authorityRoot, Sha256Digest bindingId) {
        this.store = store;
        this.root = authorityRoot + "/native-offload-attempts/" + bindingId.toHex();
    }

    record Original(String key, PulsarOffloadKeysV1 keys, PreparedAttemptKey keyMaterial, Sha256Digest fingerprint) {}

    CompletionStage<Original> open(
            PulsarSealedLedgerAttemptV1 attempt,
            SealedLedgerSection ledger,
            int blockTargetBytes,
            Map<String, String> policy,
            AttemptKeyManager kms,
            Executor executor) {
        String key = attemptKey(attempt.ledgerId(), attempt.attemptUuid());
        var fingerprint = fingerprint(ledger, policy);
        return store.read(key).thenCompose(existing -> {
            if (existing.isPresent()) {
                return unwrap(
                        key,
                        existing.orElseThrow().canonicalStoredBytes(),
                        attempt,
                        blockTargetBytes,
                        policy,
                        fingerprint,
                        kms,
                        executor);
            }
            return CompletableFuture.supplyAsync(
                            () -> kms.create(attempt.ledgerId(), attempt.attemptUuid(), policy), executor)
                    .thenCompose(created -> {
                        var section = new AttemptSection(
                                attempt.ledgerId(),
                                attempt.attemptUuid(),
                                attempt.providerScopePrefix(),
                                PulsarOffloadKeysV1.KEY_DERIVATION_VERSION,
                                attempt.retentionClass(),
                                blockTargetBytes,
                                created.envelope());
                        var exact = encode(fingerprint, section);
                        return store.compareAndSet(Optional.empty(), key, exact)
                                .thenCompose(ignored -> store.read(key))
                                .thenCompose(result -> {
                                    var original = result.orElseThrow(
                                            () -> new IllegalStateException("Original offload key is unresolved"));
                                    if (original.canonicalStoredBytes().equals(exact)) {
                                        return CompletableFuture.completedFuture(
                                                new Original(key, attempt.keys(), created, fingerprint));
                                    }
                                    return unwrap(
                                            key,
                                            original.canonicalStoredBytes(),
                                            attempt,
                                            blockTargetBytes,
                                            policy,
                                            fingerprint,
                                            kms,
                                            executor);
                                });
                    });
        });
    }

    CompletionStage<Void> bindBody(Original original, String key, Body body) {
        String role;
        if (original.keys().dataKey().equals(key)) {
            role = "npd1";
        } else if (original.keys().rootKey().equals(key)) {
            role = "npo1";
        } else {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("Object key differs from original attempt"));
        }
        byte[] name = key.getBytes(StandardCharsets.UTF_8);
        byte[] digest = HexFormat.of().parseHex(body.sha256());
        var bytes = CanonicalBytes.copyOf(ByteBuffer.allocate(16 + digest.length + name.length)
                .putInt(MAGIC)
                .putInt(name.length)
                .put(name)
                .putLong(body.bytes())
                .put(digest)
                .array());
        String authority = original.key() + "/" + role;
        return store.read(authority).thenCompose(existing -> {
            if (existing.isPresent()) {
                requireExactBody(existing.orElseThrow().canonicalStoredBytes(), bytes);
                return CompletableFuture.completedFuture(null);
            }
            return store.compareAndSet(Optional.empty(), authority, bytes)
                    .thenCompose(ignored -> store.read(authority))
                    .thenAccept(stored -> requireExactBody(
                            stored.orElseThrow(() ->
                                            new IllegalStateException("Original offload Object body is unresolved"))
                                    .canonicalStoredBytes(),
                            bytes));
        });
    }

    CompletionStage<M5MaterializationWorkerV1> bindBirthOwner(Original original, M5MaterializationWorkerV1 candidate) {
        String key = original.key() + "/birth-owner";
        return store.read(key).thenCompose(existing -> {
            if (existing.isPresent()) {
                return CompletableFuture.completedFuture(
                        requireSameBirthPlan(existing.orElseThrow().canonicalStoredBytes(), candidate));
            }
            return store.compareAndSet(Optional.empty(), key, candidate.encode())
                    .thenCompose(ignored -> store.read(key))
                    .thenApply(found -> requireSameBirthPlan(
                            found.orElseThrow(() ->
                                            new IllegalStateException("Original offload birth owner is unresolved"))
                                    .canonicalStoredBytes(),
                            candidate));
        });
    }

    String attemptKey(long ledgerId, UUID attempt) {
        return root + "/" + ledgerId + "/" + attempt;
    }

    CompletionStage<M5MaterializationWorkerV1> readBirthOwner(String attemptKey) {
        return store.read(attemptKey + "/birth-owner")
                .thenApply(found -> M5MaterializationWorkerV1.decode(found.orElseThrow(
                                () -> new IllegalStateException("Published native offload birth owner is missing"))
                        .canonicalStoredBytes()));
    }

    private static M5MaterializationWorkerV1 requireSameBirthPlan(
            CanonicalBytes stored, M5MaterializationWorkerV1 candidate) {
        var original = M5MaterializationWorkerV1.decode(stored);
        if (!original.identity()
                        .protocolCellSha256()
                        .equals(candidate.identity().protocolCellSha256())
                || !original.identity()
                        .providerScopeSha256()
                        .equals(candidate.identity().providerScopeSha256())
                || !original.identity().binding().equals(candidate.identity().binding())
                || !original.identity().capability().equals(candidate.identity().capability())
                || !original.nativeNamespaceBindingSha256().equals(candidate.nativeNamespaceBindingSha256())
                || !original.predecessorSelectorSha256().equals(candidate.predecessorSelectorSha256())
                || !original.sourceCutSha256().equals(candidate.sourceCutSha256())
                || !original.planRootSha256().equals(candidate.planRootSha256())
                || !original.taskIdSha256().equals(candidate.taskIdSha256())) {
            throw new IllegalStateException("Original offload birth plan changed");
        }
        return original;
    }

    private static void requireExactBody(CanonicalBytes actual, CanonicalBytes expected) {
        if (!actual.equals(expected)) {
            throw new IllegalStateException("Offload retry produced a different Object body");
        }
    }

    private static CompletionStage<Original> unwrap(
            String key,
            CanonicalBytes stored,
            PulsarSealedLedgerAttemptV1 attempt,
            int blockTargetBytes,
            Map<String, String> policy,
            Sha256Digest fingerprint,
            AttemptKeyManager kms,
            Executor executor) {
        var section = decode(stored, fingerprint);
        if (section.ledgerId() != attempt.ledgerId()
                || !section.attemptUuid().equals(attempt.attemptUuid())
                || !section.providerScopePrefix().equals(attempt.providerScopePrefix())
                || section.keyDerivationVersion() != PulsarOffloadKeysV1.KEY_DERIVATION_VERSION
                || section.retentionClass() != attempt.retentionClass()
                || section.blockTargetBytes() != blockTargetBytes) {
            return CompletableFuture.failedFuture(new IllegalStateException("Original offload attempt facts differ"));
        }
        return CompletableFuture.supplyAsync(
                () -> new Original(
                        key,
                        attempt.keys(),
                        new PreparedAttemptKey(
                                kms.unwrap(attempt.ledgerId(), attempt.attemptUuid(), section.keyEnvelope(), policy),
                                section.keyEnvelope()),
                        fingerprint),
                executor);
    }

    private static Sha256Digest fingerprint(SealedLedgerSection ledger, Map<String, String> policy) {
        try {
            var bytes = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(bytes)) {
                out.writeInt(MAGIC);
                out.write(HexFormat.of().parseHex(Npo1CodecV1.sealedLedgerSectionSha256(ledger)));
                out.writeInt(policy.size());
                for (var setting : new TreeMap<>(policy).entrySet()) {
                    out.writeUTF(setting.getKey());
                    out.writeUTF(setting.getValue());
                }
            }
            return Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray()));
        } catch (IOException failure) {
            throw new IllegalArgumentException("Offload policy cannot be bound", failure);
        }
    }

    private static CanonicalBytes encode(Sha256Digest fingerprint, AttemptSection section) {
        byte[] body = Npo1CodecV1.canonicalAttemptSection(section);
        if (body.length > MAX_ATTEMPT_BYTES - 36) {
            throw new IllegalArgumentException("Original offload attempt exceeds its bound");
        }
        return CanonicalBytes.copyOf(ByteBuffer.allocate(36 + body.length)
                .putInt(MAGIC)
                .put(fingerprint.bytes().toByteArray())
                .put(body)
                .array());
    }

    private static AttemptSection decode(CanonicalBytes bytes, Sha256Digest fingerprint) {
        if (bytes.length() <= 36 || bytes.length() > MAX_ATTEMPT_BYTES) {
            throw new IllegalStateException("Original offload attempt is outside its bound");
        }
        var input = ByteBuffer.wrap(bytes.toByteArray());
        if (input.getInt() != MAGIC) {
            throw new IllegalStateException("Original offload attempt format differs");
        }
        byte[] actual = new byte[Sha256Digest.LENGTH];
        input.get(actual);
        if (!Arrays.equals(actual, fingerprint.bytes().toByteArray())) {
            throw new IllegalStateException("Original sealed ledger or offload policy differs");
        }
        byte[] section = new byte[input.remaining()];
        input.get(section);
        return Npo1CodecV1.parseCanonicalAttemptSection(section);
    }
}
