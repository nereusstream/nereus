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

package com.nereusstream.kafka.bookkeeper.object.nwkcp1;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.protocol.ProtocolKindV1;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointPublicationV1;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointStateV1;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointStoreV1;
import com.nereusstream.storage.object.control.TerminalProtocolCheckpointBindingV1;
import com.nereusstream.storage.object.control.TerminalProtocolCheckpointVerifierV1;
import com.nereusstream.storage.object.control.WalRunControlCodec;
import com.nereusstream.storage.object.control.WalRunProtocolTerminalizerV1;
import com.nereusstream.storage.object.control.WalRunRootRecord;
import com.nereusstream.storage.object.control.WalRunSealRecord;
import com.nereusstream.storage.object.control.WalRunTerminalClosureProofV1;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Object-WAL implementation of the profile-neutral Kafka checkpoint store. */
public final class ObjectKafkaProtocolCheckpointStoreV1
        implements KafkaProtocolCheckpointStoreV1, WalRunProtocolTerminalizerV1, TerminalProtocolCheckpointVerifierV1 {
    private final String walRunPrefix;
    private final Sha256Digest rootSha;
    private final com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1 expectedRunBinding;
    private long publisherEpoch;
    private final Nwkcp1BackendV1 backend;
    private boolean publicationInFlight;

    public ObjectKafkaProtocolCheckpointStoreV1(
            String walRunPrefix,
            KafkaNwkcp1WalRunContextV1 walRunContext,
            long publisherEpoch,
            Nwkcp1BackendV1 backend) {
        this.walRunPrefix = Objects.requireNonNull(walRunPrefix, "walRunPrefix");
        Objects.requireNonNull(walRunContext, "walRunContext");
        this.rootSha = walRunContext.rootSha();
        this.expectedRunBinding = walRunContext.kafkaRunBinding();
        this.backend = Objects.requireNonNull(backend, "backend");
        if (rootSha.isZero() || publisherEpoch <= 0) {
            throw new IllegalArgumentException("Object checkpoint Root/publisher fence is outside the v1 domain");
        }
        this.publisherEpoch = publisherEpoch;
        Nwkcp1ObjectKeyV1.headKey(walRunPrefix);
    }

    @Override
    public CompletionStage<KafkaProtocolCheckpointPublicationV1> publish(KafkaProtocolCheckpointStateV1 checkpoint) {
        Objects.requireNonNull(checkpoint, "checkpoint");
        if (!checkpoint.vector().isAlignedCompoundCheckpoint()) {
            throw new IllegalArgumentException("Object checkpoint publication is unaligned");
        }
        if (!checkpoint.vector().runBinding().equals(expectedRunBinding)) {
            throw new IllegalArgumentException(
                    "Object checkpoint differs from the expected ProviderScope/StorageRun context");
        }
        beginMutation();
        try {
            Nwkcp1EncodedObjectV1 encoded =
                    Nwkcp1CodecV1.encode(walRunPrefix, new Nwkcp1ObjectV1(rootSha, List.of(checkpoint)));
            return preflight(checkpoint, encoded)
                    .thenCompose(existing -> {
                        if (existing.isPresent()) {
                            return CompletableFuture.completedFuture(existing.orElseThrow());
                        }
                        return createAndVerify(encoded).thenCompose(ignored -> select(encoded, checkpoint));
                    })
                    .whenComplete((result, failure) -> endMutation());
        } catch (RuntimeException failure) {
            endMutation();
            throw failure;
        }
    }

    private CompletionStage<Optional<KafkaProtocolCheckpointPublicationV1>> preflight(
            KafkaProtocolCheckpointStateV1 checkpoint, Nwkcp1EncodedObjectV1 encoded) {
        String headKey = Nwkcp1ObjectKeyV1.headKey(walRunPrefix);
        return backend.readHead(headKey).thenCompose(current -> {
            if (current.isEmpty()) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            KafkaProtocolCheckpointHeadV1 head = decodeHead(current.orElseThrow());
            requireRoot(head);
            if (head.state() != KafkaProtocolCheckpointHeadStateV1.OPEN || head.publisherEpoch() != publisherEpoch) {
                throw new KafkaObjectCheckpointException("checkpoint Head is terminal or publisher-fenced");
            }
            List<KafkaCheckpointCoverageV1> vector = List.of(KafkaCheckpointCoverageV1.from(checkpoint.vector()));
            if (!head.coveredThroughVector().equals(vector)) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            if (!head.checkpointObjectKey().equals(encoded.key())
                    || head.checkpointObjectLength() != encoded.length()
                    || !head.checkpointObjectDigest().equals(encoded.digest())) {
                throw new KafkaObjectCheckpointException(
                        "the same checkpoint vector names different canonical protocol state");
            }
            Nwkcp1BackendV1.SelectedObjectToken selectedToken = backend.selectObjectFromHead(
                    headKey, current.orElseThrow(), Sha256Digest.hash(current.orElseThrow()));
            return backend.readSelectedObject(selectedToken, false).thenApply(body -> {
                requireSelectedObject(
                        head,
                        body.orElseThrow(() -> new KafkaObjectCheckpointException(
                                "same-vector retry selected an absent NWKCP1 Object")));
                return Optional.of(new KafkaProtocolCheckpointPublicationV1(head.checkpointOrdinal(), checkpoint));
            });
        });
    }

    public CompletionStage<KafkaProtocolCheckpointHeadV1> takeover(long newPublisherEpoch) {
        if (newPublisherEpoch <= publisherEpoch) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("checkpoint publisher takeover epoch does not advance"));
        }
        beginMutation();
        String headKey = Nwkcp1ObjectKeyV1.headKey(walRunPrefix);
        try {
            return backend.readHead(headKey)
                    .thenCompose(current -> {
                        CanonicalBytes currentBytes = current.orElseThrow(
                                () -> new KafkaObjectCheckpointException("cannot take over an absent checkpoint Head"));
                        KafkaProtocolCheckpointHeadV1 before = decodeHead(currentBytes);
                        requireRoot(before);
                        KafkaProtocolCheckpointHeadV1 replacement = before.takeover(newPublisherEpoch);
                        return convergeHeadCas(headKey, Optional.of(currentBytes), replacement)
                                .thenApply(selected -> {
                                    synchronized (ObjectKafkaProtocolCheckpointStoreV1.this) {
                                        publisherEpoch = newPublisherEpoch;
                                    }
                                    return selected;
                                });
                    })
                    .whenComplete((result, failure) -> endMutation());
        } catch (RuntimeException failure) {
            endMutation();
            throw failure;
        }
    }

    @Override
    public CompletionStage<TerminalProtocolCheckpointBindingV1> terminalize(WalRunTerminalClosureProofV1 closureProof) {
        Objects.requireNonNull(closureProof, "closureProof");
        if (!closureProof.root().rootSha256().equals(rootSha)) {
            return CompletableFuture.failedFuture(
                    new KafkaObjectCheckpointException("physical closure proof belongs to another WalRun Root"));
        }
        beginMutation();
        String headKey = Nwkcp1ObjectKeyV1.headKey(walRunPrefix);
        try {
            return backend.readHead(headKey)
                    .thenCompose(current -> {
                        CanonicalBytes currentBytes = current.orElseThrow(
                                () -> new KafkaObjectCheckpointException("cannot terminate an absent checkpoint Head"));
                        KafkaProtocolCheckpointHeadV1 before = decodeHead(currentBytes);
                        requireRoot(before);
                        if (before.publisherEpoch() != publisherEpoch) {
                            throw new KafkaObjectCheckpointException(
                                    "stale publisher cannot terminate the checkpoint Head");
                        }
                        Nwkcp1BackendV1.SelectedObjectToken selectedToken =
                                backend.selectObjectFromHead(headKey, currentBytes, Sha256Digest.hash(currentBytes));
                        return backend.readSelectedObject(selectedToken, false).thenCompose(body -> {
                            KafkaProtocolCheckpointStateV1 finalState = requireSelectedObject(
                                    before,
                                    body.orElseThrow(() -> new KafkaObjectCheckpointException(
                                            "final selected NWKCP1 Object is absent")));
                            backend.verifyPhysicalClosure(closureProof, expectedRunBinding, finalState);
                            if (before.state() == KafkaProtocolCheckpointHeadStateV1.TERMINAL) {
                                return CompletableFuture.completedFuture(binding(headKey, currentBytes));
                            }
                            KafkaProtocolCheckpointHeadV1 terminal = before.terminal();
                            CanonicalBytes terminalBytes =
                                    KafkaProtocolCheckpointHeadCodecV1.encode(walRunPrefix, rootSha, terminal);
                            return convergeHeadCas(headKey, Optional.of(currentBytes), terminal)
                                    .thenApply(ignored -> binding(headKey, terminalBytes));
                        });
                    })
                    .whenComplete((result, failure) -> endMutation());
        } catch (RuntimeException failure) {
            endMutation();
            throw failure;
        }
    }

    @Override
    public void verifyTerminal(
            WalRunRootRecord predecessorRoot,
            WalRunSealRecord predecessorSeal,
            TerminalProtocolCheckpointBindingV1 binding,
            CanonicalBytes exactTerminalHeadValue,
            TerminalProtocolCheckpointVerifierV1.RecoveryContext recoveryContext) {
        Objects.requireNonNull(predecessorRoot, "predecessorRoot");
        Objects.requireNonNull(predecessorSeal, "predecessorSeal");
        Objects.requireNonNull(binding, "binding");
        Objects.requireNonNull(exactTerminalHeadValue, "exactTerminalHeadValue");
        Objects.requireNonNull(recoveryContext, "recoveryContext");
        String expectedKey = Nwkcp1ObjectKeyV1.headKey(walRunPrefix);
        Sha256Digest expectedRoot = WalRunControlCodec.rootSha256(predecessorRoot);
        if (binding.protocolKind() != ProtocolKindV1.KAFKA
                || !binding.terminalHeadKey().equals(expectedKey)
                || !Sha256Digest.hash(exactTerminalHeadValue).equals(binding.terminalHeadValueSha256())
                || !predecessorSeal.root().rootSha256().equals(expectedRoot)
                || !expectedRoot.equals(rootSha)
                || !WalRunControlCodec.rootSha256(recoveryContext.protocolObjectRoot())
                        .equals(expectedRoot)) {
            throw new IllegalStateException("terminal Kafka Head binding differs from the predecessor Root/Seal");
        }
        KafkaProtocolCheckpointHeadV1 head =
                KafkaProtocolCheckpointHeadCodecV1.decode(walRunPrefix, rootSha, exactTerminalHeadValue);
        if (head.state() != KafkaProtocolCheckpointHeadStateV1.TERMINAL) {
            throw new IllegalStateException("successor predecessor binding selected a non-terminal Kafka Head");
        }
        CanonicalBytes selected = recoveryContext.readVerifiedProtocolObject(new ObjectIdentity(
                head.checkpointObjectKey(), head.checkpointObjectLength(), head.checkpointObjectDigest()));
        requireSelectedObject(head, selected);
    }

    public CompletionStage<Optional<KafkaProtocolCheckpointHeadV1>> currentHead() {
        return backend.readHead(Nwkcp1ObjectKeyV1.headKey(walRunPrefix))
                .thenApply(value -> value.map(currentBytes -> {
                    return decodeHead(currentBytes);
                }));
    }

    private CompletionStage<Void> createAndVerify(Nwkcp1EncodedObjectV1 encoded) {
        return backend.conditionalCreateObject(encoded.key(), encoded.body(), encoded.digest())
                .thenCompose(result -> {
                    if (result.disposition() == Nwkcp1BackendV1.CreateDisposition.DEFINITIVELY_NOT_APPLIED) {
                        return CompletableFuture.failedFuture(new KafkaObjectCheckpointException(
                                "NWKCP1 conditional create was definitively not applied"));
                    }
                    if (result.disposition() == Nwkcp1BackendV1.CreateDisposition.CONFLICT) {
                        return CompletableFuture.failedFuture(new KafkaObjectCheckpointException(
                                "NWKCP1 conditional create returned a definitive same-key conflict"));
                    }
                    if (result.disposition() == Nwkcp1BackendV1.CreateDisposition.UNKNOWN) {
                        return CompletableFuture.failedFuture(new KafkaObjectCheckpointException(
                                "NWKCP1 conditional create remained outcome-unknown"));
                    }
                    Nwkcp1BackendV1.CreatedObjectToken token =
                            result.createdToken().orElseThrow();
                    return backend.readCreatedObject(token).thenApply(found -> {
                        CanonicalBytes body = found.orElseThrow(() -> new KafkaObjectCheckpointException(
                                "NWKCP1 create outcome did not converge to an exact same-key object"));
                        if (body.length() != encoded.length()
                                || !Sha256Digest.hash(body).equals(encoded.digest())
                                || !body.equals(encoded.body())) {
                            throw new KafkaObjectCheckpointException(
                                    "NWKCP1 same-key reread conflicts with the sealed request body");
                        }
                        Nwkcp1ObjectV1 decoded = Nwkcp1CodecV1.decodeVerified(
                                walRunPrefix, encoded.key(), encoded.length(), encoded.digest(), body);
                        if (!decoded.walRunRootSha().equals(rootSha)) {
                            throw new KafkaObjectCheckpointException("NWKCP1 reread belongs to another Root");
                        }
                        return null;
                    });
                });
    }

    private CompletionStage<KafkaProtocolCheckpointPublicationV1> select(
            Nwkcp1EncodedObjectV1 encoded, KafkaProtocolCheckpointStateV1 checkpoint) {
        String headKey = Nwkcp1ObjectKeyV1.headKey(walRunPrefix);
        return backend.readHead(headKey).thenCompose(current -> {
            KafkaProtocolCheckpointHeadV1 before = current.map(this::decodeHead).orElse(null);
            if (before != null) {
                requireRoot(before);
                if (before.state() != KafkaProtocolCheckpointHeadStateV1.OPEN
                        || before.publisherEpoch() != publisherEpoch) {
                    throw new KafkaObjectCheckpointException("checkpoint Head is terminal or publisher-fenced");
                }
            }
            List<KafkaCheckpointCoverageV1> vector = List.of(KafkaCheckpointCoverageV1.from(checkpoint.vector()));
            if (before != null && before.coveredThroughVector().equals(vector)) {
                if (!before.checkpointObjectDigest().equals(encoded.digest())
                        || !before.checkpointObjectKey().equals(encoded.key())
                        || before.checkpointObjectLength() != encoded.length()) {
                    throw new KafkaObjectCheckpointException(
                            "the same checkpoint vector names different canonical protocol state");
                }
                return CompletableFuture.completedFuture(
                        new KafkaProtocolCheckpointPublicationV1(before.checkpointOrdinal(), checkpoint));
            }
            KafkaProtocolCheckpointHeadV1 candidate =
                    KafkaProtocolCheckpointHeadV1.open(rootSha, publisherEpoch, before, encoded, vector);
            return convergeHeadCas(headKey, current, candidate)
                    .thenApply(selected ->
                            new KafkaProtocolCheckpointPublicationV1(selected.checkpointOrdinal(), checkpoint));
        });
    }

    private CompletionStage<KafkaProtocolCheckpointHeadV1> convergeHeadCas(
            String headKey, Optional<CanonicalBytes> exactExpected, KafkaProtocolCheckpointHeadV1 replacement) {
        CanonicalBytes encoded = KafkaProtocolCheckpointHeadCodecV1.encode(walRunPrefix, rootSha, replacement);
        return backend.compareAndSetHead(headKey, exactExpected, encoded).thenCompose(disposition -> {
            if (disposition == Nwkcp1BackendV1.CasDisposition.APPLIED) {
                return CompletableFuture.completedFuture(replacement);
            }
            return backend.readHead(headKey).thenApply(reread -> {
                CanonicalBytes selected = reread.orElseThrow(() ->
                        new KafkaObjectCheckpointException("checkpoint Head CAS did not converge to a selected value"));
                if (!selected.equals(encoded)) {
                    throw new KafkaObjectCheckpointException("checkpoint Head CAS lost to a fork or stale publisher");
                }
                return replacement;
            });
        });
    }

    private void requireRoot(KafkaProtocolCheckpointHeadV1 head) {
        if (!head.walRunRootSha().equals(rootSha)) {
            throw new KafkaObjectCheckpointException("checkpoint Head belongs to another WalRun Root");
        }
    }

    private KafkaProtocolCheckpointStateV1 requireSelectedObject(
            KafkaProtocolCheckpointHeadV1 head, CanonicalBytes body) {
        Nwkcp1ObjectV1 object = Nwkcp1CodecV1.decodeVerified(
                walRunPrefix,
                head.checkpointObjectKey(),
                head.checkpointObjectLength(),
                head.checkpointObjectDigest(),
                body);
        List<KafkaCheckpointCoverageV1> actualVector = object.rows().stream()
                .map(row -> KafkaCheckpointCoverageV1.from(row.vector()))
                .toList();
        if (!object.walRunRootSha().equals(rootSha)
                || object.rows().size() != 1
                || !object.rows().get(0).vector().runBinding().equals(expectedRunBinding)
                || !actualVector.equals(head.coveredThroughVector())) {
            throw new KafkaObjectCheckpointException(
                    "final NWKCP1 Object/vector differs from Root/ProviderScope/StorageRun Head context");
        }
        return object.rows().get(0);
    }

    private static TerminalProtocolCheckpointBindingV1 binding(String headKey, CanonicalBytes exactHeadBytes) {
        return new TerminalProtocolCheckpointBindingV1(
                ProtocolKindV1.KAFKA, headKey, Sha256Digest.hash(exactHeadBytes));
    }

    private KafkaProtocolCheckpointHeadV1 decodeHead(CanonicalBytes bytes) {
        return KafkaProtocolCheckpointHeadCodecV1.decode(walRunPrefix, rootSha, bytes);
    }

    private synchronized void beginMutation() {
        if (publicationInFlight) {
            throw new IllegalStateException("Kafka protocol checkpoint mutation is already in flight");
        }
        publicationInFlight = true;
    }

    private synchronized void endMutation() {
        publicationInFlight = false;
    }
}
