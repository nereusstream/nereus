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

import com.nereusstream.pulsar.offload.PulsarOffloadObjectStoreV1.Body;
import com.nereusstream.pulsar.offload.PulsarOffloadObjectStoreV1.ImmutableObject;
import com.nereusstream.pulsar.offload.PulsarSealedLedgerAttemptV1.DeleteState;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.DataObject;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.AttemptSection;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.DataExtentSection;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.Root;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.SealedLedgerSection;
import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/** Data-before-root publisher and deterministic root-first cleanup for one sealed-ledger attempt. */
public final class PulsarSealedLedgerPublisherV1 {
    public record PreparedAttempt(
            PulsarSealedLedgerAttemptV1 attempt,
            SealedLedgerSection sealedLedger,
            DataObject dataObject,
            int blockTargetBytes) {
        public PreparedAttempt {
            Objects.requireNonNull(attempt, "attempt");
            Objects.requireNonNull(sealedLedger, "sealedLedger");
            Objects.requireNonNull(dataObject, "dataObject");
            if (attempt.deleteState() != DeleteState.BK_DELETE_NONE
                    || attempt.bookkeeperDeleted()
                    || sealedLedger.lastAddConfirmed() != attempt.lastAddConfirmed()
                    || sealedLedger.entryCount() != attempt.entryCount()
                    || sealedLedger.logicalLength() != attempt.logicalLength()
                    || dataObject.firstEntryId() != 0
                    || dataObject.lastEntryId() != attempt.lastAddConfirmed()
                    || blockTargetBytes <= 0) {
                throw new IllegalArgumentException("prepared attempt differs from sealed native authority");
            }
        }
    }

    public record Publication(
            PulsarSealedLedgerAttemptV1 attempt,
            PulsarOffloadKeysV1 keys,
            ImmutableObject dataObject,
            ImmutableObject rootObject,
            Root root,
            byte[] rootBytes) {
        public Publication {
            Objects.requireNonNull(attempt, "attempt");
            Objects.requireNonNull(keys, "keys");
            Objects.requireNonNull(dataObject, "dataObject");
            Objects.requireNonNull(rootObject, "rootObject");
            Objects.requireNonNull(root, "root");
            rootBytes = rootBytes.clone();
        }

        @Override
        public byte[] rootBytes() {
            return rootBytes.clone();
        }
    }

    @FunctionalInterface
    public interface PublishedAttemptVerifier {
        CompletionStage<Void> verify(Publication publication);
    }

    private final PulsarOffloadObjectStoreV1 objectStore;
    private final PulsarOffloadLimitCandidateV1 limits;
    private final PublishedAttemptVerifier verifier;
    private final Executor fileExecutor;

    public PulsarSealedLedgerPublisherV1(
            PulsarOffloadObjectStoreV1 objectStore,
            PulsarOffloadLimitCandidateV1 limits,
            PublishedAttemptVerifier verifier,
            Executor fileExecutor) {
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
        this.limits = Objects.requireNonNull(limits, "limits");
        this.verifier = Objects.requireNonNull(verifier, "verifier");
        this.fileExecutor = Objects.requireNonNull(fileExecutor, "fileExecutor");
        PulsarOffloadProfileAdmissionV1.requireAdmitted(limits, objectStore.capabilities());
    }

    public CompletionStage<Publication> publish(PreparedAttempt prepared) {
        Objects.requireNonNull(prepared, "prepared");
        PulsarOffloadKeysV1 keys = prepared.attempt().keys();
        return CompletableFuture.supplyAsync(() -> verifiedDataBody(prepared.dataObject()), fileExecutor)
                .thenCompose(body -> createOrResolve(keys.dataKey(), body))
                .thenApplyAsync(dataProof -> draft(prepared, dataProof), fileExecutor)
                .thenCompose(draft -> createOrResolve(keys.rootKey(), draft.rootBody())
                        .thenApply(rootProof -> draft.publication(rootProof)))
                .thenCompose(publication ->
                        callStage(() -> verifier.verify(publication)).thenApply(ignored -> publication));
    }

    public CompletionStage<Void> deleteAttempt(PulsarSealedLedgerAttemptV1 attempt) {
        Objects.requireNonNull(attempt, "attempt");
        PulsarOffloadKeysV1 keys = attempt.keys();
        return callStage(() -> objectStore.deleteAndProveAbsent(keys.rootKey()))
                .thenCompose(ignored -> callStage(() -> objectStore.deleteAndProveAbsent(keys.dataKey())))
                .thenCompose(
                        ignored -> callStage(() -> objectStore.cleanupAttemptMultipartResidue(keys.attemptPrefix())));
    }

    private Draft draft(PreparedAttempt prepared, ImmutableObject dataProof) {
        PulsarSealedLedgerAttemptV1 attempt = prepared.attempt();
        AttemptSection attemptSection = new AttemptSection(
                attempt.ledgerId(),
                attempt.attemptUuid(),
                attempt.providerScopePrefix(),
                PulsarOffloadKeysV1.KEY_DERIVATION_VERSION,
                attempt.retentionClass(),
                prepared.blockTargetBytes());
        DataObject data = prepared.dataObject();
        DataExtentSection extent = new DataExtentSection(
                1, attempt.keys().dataKey(), data.bytes(), data.sha256(), dataProof.immutableVersion());
        Root root = new Root(attemptSection, prepared.sealedLedger(), extent, data.blocks());
        byte[] rootBytes = Npo1CodecV1.canonicalBytes(root, limits);
        Root parsed = Npo1CodecV1.parseCanonical(rootBytes, limits);
        if (!parsed.equals(root)) {
            throw new IllegalStateException("local NPO1 round trip differs before publication");
        }
        String rootSha = Npo1CodecV1.rootSha256(rootBytes);
        Body rootBody = new Body(rootBytes.length, rootSha, () -> new ByteArrayInputStream(rootBytes));
        return new Draft(prepared, dataProof, parsed, rootBytes, rootBody);
    }

    private Body verifiedDataBody(DataObject data) {
        long actualBytes = 0;
        MessageDigest digest = digest();
        byte[] buffer = new byte[128 * 1_024];
        try (InputStream input = Files.newInputStream(data.path())) {
            int count;
            while ((count = input.read(buffer)) >= 0) {
                if (count == 0) {
                    continue;
                }
                digest.update(buffer, 0, count);
                actualBytes = Math.addExact(actualBytes, count);
                if (actualBytes > limits.maxDataObjectBytes()) {
                    throw new IllegalArgumentException("staged NPD1 exceeds the admitted Object maximum");
                }
            }
        } catch (IOException failure) {
            throw new CompletionException("cannot verify staged NPD1", failure);
        }
        String actualSha = HexFormat.of().formatHex(digest.digest());
        if (actualBytes != data.bytes() || !actualSha.equals(data.sha256())) {
            throw new IllegalArgumentException("staged NPD1 length or SHA changed before upload");
        }
        return new Body(
                data.bytes(),
                data.sha256(),
                () -> new ExactBodyInputStream(Files.newInputStream(data.path()), data.bytes(), data.sha256()));
    }

    private CompletionStage<ImmutableObject> createOrResolve(String key, Body body) {
        CompletableFuture<ImmutableObject> result = new CompletableFuture<>();
        callStage(() -> objectStore.createImmutable(key, body)).whenComplete((created, createFailure) -> {
            if (createFailure == null) {
                completeProof(result, created, body, null);
                return;
            }
            Throwable primary = unwrap(createFailure);
            callStage(() -> objectStore.head(key)).whenComplete((existing, headFailure) -> {
                if (headFailure != null) {
                    primary.addSuppressed(unwrap(headFailure));
                    result.completeExceptionally(primary);
                } else {
                    completeProof(result, existing, body, primary);
                }
            });
        });
        return result;
    }

    private static void completeProof(
            CompletableFuture<ImmutableObject> result,
            ImmutableObject proof,
            Body expected,
            Throwable responseFailure) {
        try {
            if (proof.bytes() != expected.bytes() || !proof.sha256().equals(expected.sha256())) {
                throw new IllegalArgumentException("immutable Object proof differs from canonical body");
            }
            result.complete(proof);
        } catch (RuntimeException mismatch) {
            if (responseFailure != null) {
                responseFailure.addSuppressed(mismatch);
                result.completeExceptionally(responseFailure);
            } else {
                result.completeExceptionally(mismatch);
            }
        }
    }

    private static <T> CompletionStage<T> callStage(Supplier<CompletionStage<T>> call) {
        try {
            CompletionStage<T> stage = call.get();
            if (stage == null) {
                return CompletableFuture.failedFuture(new NullPointerException("provider returned a null stage"));
            }
            return stage;
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private static Throwable unwrap(Throwable failure) {
        if ((failure instanceof CompletionException || failure instanceof java.util.concurrent.ExecutionException)
                && failure.getCause() != null) {
            return failure.getCause();
        }
        return failure;
    }

    private static MessageDigest digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("JDK lacks SHA-256", failure);
        }
    }

    private record Draft(
            PreparedAttempt prepared, ImmutableObject dataProof, Root root, byte[] rootBytes, Body rootBody) {
        private Publication publication(ImmutableObject rootProof) {
            return new Publication(
                    prepared.attempt(), prepared.attempt().keys(), dataProof, rootProof, root, rootBytes);
        }
    }

    private static final class ExactBodyInputStream extends FilterInputStream {
        private final long expectedBytes;
        private final String expectedSha;
        private final MessageDigest digest = digest();
        private long actualBytes;
        private boolean verified;

        private ExactBodyInputStream(InputStream input, long expectedBytes, String expectedSha) {
            super(input);
            this.expectedBytes = expectedBytes;
            this.expectedSha = expectedSha;
        }

        @Override
        public int read() throws IOException {
            int value = super.read();
            if (value < 0) {
                verifyEnd();
            } else {
                digest.update((byte) value);
                actualBytes = Math.addExact(actualBytes, 1);
            }
            return value;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            int count = super.read(bytes, offset, length);
            if (count < 0) {
                verifyEnd();
            } else if (count > 0) {
                digest.update(bytes, offset, count);
                actualBytes = Math.addExact(actualBytes, count);
            }
            return count;
        }

        @Override
        public long skip(long count) throws IOException {
            if (count <= 0) {
                return 0;
            }
            long remaining = count;
            byte[] buffer = new byte[(int) Math.min(8_192, count)];
            while (remaining > 0) {
                int read = read(buffer, 0, (int) Math.min(buffer.length, remaining));
                if (read < 0) {
                    break;
                }
                remaining -= read;
            }
            return count - remaining;
        }

        @Override
        public void close() throws IOException {
            IOException incomplete =
                    verified ? null : new IOException("provider closed NPD1 body before exact EOF proof");
            try {
                super.close();
            } catch (IOException closeFailure) {
                if (incomplete != null) {
                    incomplete.addSuppressed(closeFailure);
                } else {
                    throw closeFailure;
                }
            }
            if (incomplete != null) {
                throw incomplete;
            }
        }

        private void verifyEnd() throws IOException {
            if (verified) {
                return;
            }
            String actualSha = HexFormat.of().formatHex(digest.digest());
            if (actualBytes != expectedBytes || !actualSha.equals(expectedSha)) {
                throw new IOException("provider stream differs from staged NPD1 descriptor");
            }
            verified = true;
        }
    }
}
