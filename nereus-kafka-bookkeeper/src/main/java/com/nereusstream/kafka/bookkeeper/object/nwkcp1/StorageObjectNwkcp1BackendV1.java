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
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointStateV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.control.WalRunControlCodec;
import com.nereusstream.storage.object.control.WalRunControlKeys;
import com.nereusstream.storage.object.control.WalRunObjectSession;
import com.nereusstream.storage.object.control.WalRunRootRecord;
import com.nereusstream.storage.object.control.WalRunTerminalClosureProofV1;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.provider.ProviderObjectOutcome;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Production mapping from the shared C1 Provider/control sessions to the NWKCP1 backend contract. */
public final class StorageObjectNwkcp1BackendV1 implements Nwkcp1BackendV1 {
    private static final long MAX_CONTROL_METADATA_BYTES = 1024L * 1024;
    private final WalRunObjectSession objectSession;
    private final CanonicalControlMetadataStore metadata;

    public StorageObjectNwkcp1BackendV1(WalRunObjectSession objectSession, CanonicalControlMetadataStore metadata) {
        this.objectSession = Objects.requireNonNull(objectSession, "objectSession");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
    }

    @Override
    public CompletionStage<CreateResult> conditionalCreateObject(
            String key, CanonicalBytes body, Sha256Digest bodyDigest) {
        Objects.requireNonNull(body, "body");
        ObjectIdentity identity = new ObjectIdentity(key, body.length(), bodyDigest);
        try {
            Nwkcp1ObjectV1 decoded =
                    Nwkcp1CodecV1.decodeVerified(walRunPrefix(key), key, body.length(), bodyDigest, body);
            if (!decoded.walRunRootSha().equals(objectSession.rootSha256())) {
                throw new KafkaObjectCheckpointException("NWKCP1 create body belongs to another WalRun Root");
            }
            WalRunObjectSession.ValidatedKafkaProtocolObject validated =
                    objectSession.validateKafkaProtocolObject(identity, body);
            ProviderObjectOutcome outcome;
            try {
                outcome = objectSession
                        .conditionalCreateKafkaProtocolObject(validated)
                        .outcome();
            } catch (IOException failure) {
                try {
                    outcome = objectSession
                            .reconcileUnknownProtocolObject(identity)
                            .outcome();
                } catch (IOException | RuntimeException reconciliationFailure) {
                    reconciliationFailure.addSuppressed(failure);
                    throw reconciliationFailure;
                }
            }
            if (outcome == ProviderObjectOutcome.OUTCOME_UNKNOWN) {
                outcome = objectSession.reconcileUnknownProtocolObject(identity).outcome();
            }
            CreateDisposition disposition = map(outcome);
            Optional<CreatedObjectToken> token =
                    disposition == CreateDisposition.APPLIED || disposition == CreateDisposition.EXISTING_EXACT
                            ? Optional.of(new CreatedToken(identity))
                            : Optional.empty();
            return CompletableFuture.completedFuture(new CreateResult(disposition, token));
        } catch (IOException | RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    @Override
    public CompletionStage<Optional<CanonicalBytes>> readCreatedObject(CreatedObjectToken token) {
        if (token == null || token.getClass() != CreatedToken.class || ((CreatedToken) token).owner != this) {
            return CompletableFuture.failedFuture(
                    new KafkaObjectCheckpointException("NWKCP1 created-object token was not issued by this backend"));
        }
        ObjectIdentity identity = ((CreatedToken) token).identity;
        try {
            CanonicalBytes body = objectSession.readVerifiedProtocolObjectForPublication(identity);
            if (body.length() != identity.bodyLength()
                    || !Sha256Digest.hash(body).equals(identity.bodySha256())) {
                throw new KafkaObjectCheckpointException("C1 full-body reader returned a different NWKCP1 identity");
            }
            return CompletableFuture.completedFuture(Optional.of(body));
        } catch (IOException | RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    @Override
    public SelectedObjectToken selectObjectFromHead(
            String headKey, CanonicalBytes exactHeadValue, Sha256Digest exactHeadValueSha256) {
        Objects.requireNonNull(exactHeadValue, "exactHeadValue");
        Objects.requireNonNull(exactHeadValueSha256, "exactHeadValueSha256");
        String prefix = headPrefix(headKey);
        if (!Nwkcp1ObjectKeyV1.headKey(prefix).equals(headKey)
                || !Sha256Digest.hash(exactHeadValue).equals(exactHeadValueSha256)) {
            throw new KafkaObjectCheckpointException("NWKCP1 selected-object token differs from the exact Head value");
        }
        KafkaProtocolCheckpointHeadV1 head =
                KafkaProtocolCheckpointHeadCodecV1.decode(prefix, objectSession.rootSha256(), exactHeadValue);
        return new SelectedToken(
                new ObjectIdentity(
                        head.checkpointObjectKey(), head.checkpointObjectLength(), head.checkpointObjectDigest()),
                exactHeadValueSha256);
    }

    @Override
    public CompletionStage<Optional<CanonicalBytes>> readSelectedObject(SelectedObjectToken token, boolean recovery) {
        if (token == null || token.getClass() != SelectedToken.class || ((SelectedToken) token).owner != this) {
            return CompletableFuture.failedFuture(
                    new KafkaObjectCheckpointException("NWKCP1 selected-object token was not issued by this backend"));
        }
        SelectedToken selected = (SelectedToken) token;
        try {
            CanonicalBytes body = recovery
                    ? objectSession.readVerifiedProtocolObject(selected.identity)
                    : objectSession.readVerifiedProtocolObjectForPublication(selected.identity);
            return CompletableFuture.completedFuture(Optional.of(body));
        } catch (IOException | RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    @Override
    public CompletionStage<Optional<CanonicalBytes>> readHead(String key) {
        try {
            Optional<CanonicalBytes> value = metadata.get(key);
            return CompletableFuture.completedFuture(value);
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    @Override
    public void chargeControlMetadata(long canonicalBytes) {
        objectSession.chargeRecoveryControlMetadata(canonicalBytes);
    }

    @Override
    public void chargeDecoded(long contexts, long frames, long commitSets) {
        objectSession.chargeRecoveryDecoded(contexts, frames, commitSets);
    }

    @Override
    public void enterFallback() {
        objectSession.enterRecoveryFallback();
    }

    @Override
    public CompletionStage<CasDisposition> compareAndSetHead(
            String key, Optional<CanonicalBytes> exactExpected, CanonicalBytes replacement) {
        try {
            ControlMutationOutcome outcome = metadata.compareAndSet(key, exactExpected, replacement);
            return CompletableFuture.completedFuture(
                    switch (outcome) {
                        case APPLIED -> CasDisposition.APPLIED;
                        case DEFINITIVE_CONFLICT -> CasDisposition.NOT_APPLIED;
                        case RESPONSE_UNKNOWN -> CasDisposition.UNKNOWN;
                    });
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    @Override
    public void verifyPhysicalClosure(
            WalRunTerminalClosureProofV1 proof,
            Nbke2RunBindingV1 expectedRunBinding,
            KafkaProtocolCheckpointStateV1 finalProtocolState) {
        Objects.requireNonNull(proof, "proof");
        Objects.requireNonNull(expectedRunBinding, "expectedRunBinding");
        Objects.requireNonNull(finalProtocolState, "finalProtocolState");
        WalRunRootRecord root = objectSession.rootRecord();
        if (!WalRunControlCodec.rootSha256(root).equals(proof.root().rootSha256())
                || root.shardId() != proof.root().shardId()
                || root.shardRunEpoch() != proof.root().shardRunEpoch()
                || !proof.root().rootKey().equals(WalRunControlKeys.rootKey(root.shardId(), root.shardRunEpoch()))
                || !proof.sealKey().equals(WalRunControlKeys.sealKey(root.shardId(), root.shardRunEpoch()))
                || !proof.finalPhysicalCheckpointHeadKey()
                        .equals(WalRunControlKeys.checkpointHeadKey(root.shardId(), root.shardRunEpoch()))) {
            throw new KafkaObjectCheckpointException("physical closure Root differs from the exact owner session");
        }
        objectSession.chargeRecoveryControlMetadata(MAX_CONTROL_METADATA_BYTES);
        CanonicalBytes sealBytes = metadata.get(proof.sealKey())
                .orElseThrow(() -> new KafkaObjectCheckpointException("physical closure Seal is absent"));
        if (!Sha256Digest.hash(sealBytes).equals(proof.sealSha256())) {
            throw new KafkaObjectCheckpointException("physical closure Seal differs from its exact SHA");
        }
        var seal = WalRunControlCodec.decodeSeal(sealBytes);
        objectSession.chargeRecoveryControlMetadata(MAX_CONTROL_METADATA_BYTES);
        CanonicalBytes headBytes = metadata.get(proof.finalPhysicalCheckpointHeadKey())
                .orElseThrow(() -> new KafkaObjectCheckpointException("final physical checkpoint Head is absent"));
        if (!Sha256Digest.hash(headBytes).equals(proof.finalPhysicalCheckpointHeadSha256())) {
            throw new KafkaObjectCheckpointException("final physical checkpoint Head differs from its exact SHA");
        }
        var physicalHead = WalRunControlCodec.decodeCheckpointHead(headBytes);
        var chain = objectSession.verifyCheckpointChainStreaming(metadata, physicalHead, ignored -> {});
        if (!seal.root().equals(proof.root())
                || !seal.terminalSequence().equals(proof.terminalSequence())
                || !seal.finalCheckpointHeadKey().equals(proof.finalPhysicalCheckpointHeadKey())
                || !seal.finalCheckpointHeadSha256().equals(proof.finalPhysicalCheckpointHeadSha256())
                || seal.aggregateExtentCount() != proof.aggregateExtentCount()
                || seal.aggregateCanonicalBodyBytes() != proof.aggregateCanonicalBodyBytes()
                || !chain.coveredThrough().equals(proof.terminalSequence())
                || chain.aggregateExtentCount() != proof.aggregateExtentCount()
                || chain.aggregateCanonicalBodyBytes() != proof.aggregateCanonicalBodyBytes()) {
            throw new KafkaObjectCheckpointException("physical Root/Seal/Head/page-chain closure facts diverge");
        }
        if (!finalProtocolState.vector().isAlignedCompoundCheckpoint()
                || !finalProtocolState.vector().runBinding().equals(expectedRunBinding)
                || !expectedRunBinding.providerScopeId().equals(root.providerScopeId())
                || !expectedRunBinding.runId().value().equals(root.walRunSessionId())
                || root.protocolCellIdentity().protocolKind()
                        != com.nereusstream.domain.protocol.ProtocolKindV1.KAFKA) {
            throw new KafkaObjectCheckpointException(
                    "final Kafka vector is incompatible with the exact physical closure context");
        }
    }

    private static CreateDisposition map(ProviderObjectOutcome outcome) {
        return switch (outcome) {
            case APPLIED_EXACT -> CreateDisposition.APPLIED;
            case EXISTING_EXACT -> CreateDisposition.EXISTING_EXACT;
            case DEFINITIVELY_NOT_APPLIED -> CreateDisposition.DEFINITIVELY_NOT_APPLIED;
            case DEFINITIVE_CONFLICT -> CreateDisposition.CONFLICT;
            case OUTCOME_UNKNOWN -> CreateDisposition.UNKNOWN;
        };
    }

    private static String walRunPrefix(String key) {
        Objects.requireNonNull(key, "key");
        String marker = "/" + Nwkcp1ConstantsV1.FAMILY_PREFIX + "/";
        int offset = key.lastIndexOf(marker);
        if (offset <= 0) {
            throw new IllegalArgumentException("NWKCP1 object key omits its exact WalRun family prefix");
        }
        return key.substring(0, offset);
    }

    private static String headPrefix(String key) {
        Objects.requireNonNull(key, "key");
        String suffix = "/" + Nwkcp1ConstantsV1.FAMILY_PREFIX + "/" + Nwkcp1ConstantsV1.HEAD_TOKEN;
        if (!key.endsWith(suffix) || key.length() == suffix.length()) {
            throw new IllegalArgumentException("NWKCP1 Head key omits its exact WalRun prefix");
        }
        return key.substring(0, key.length() - suffix.length());
    }

    private final class CreatedToken implements CreatedObjectToken {
        private final StorageObjectNwkcp1BackendV1 owner = StorageObjectNwkcp1BackendV1.this;
        private final ObjectIdentity identity;

        private CreatedToken(ObjectIdentity identity) {
            this.identity = identity;
        }

        @Override
        public String key() {
            return identity.key();
        }

        @Override
        public long length() {
            return identity.bodyLength();
        }

        @Override
        public Sha256Digest digest() {
            return identity.bodySha256();
        }
    }

    private final class SelectedToken implements SelectedObjectToken {
        private final StorageObjectNwkcp1BackendV1 owner = StorageObjectNwkcp1BackendV1.this;
        private final ObjectIdentity identity;

        @SuppressWarnings("unused")
        private final Sha256Digest exactHeadValueSha256;

        private SelectedToken(ObjectIdentity identity, Sha256Digest exactHeadValueSha256) {
            this.identity = identity;
            this.exactHeadValueSha256 = exactHeadValueSha256;
        }

        @Override
        public String key() {
            return identity.key();
        }

        @Override
        public long length() {
            return identity.bodyLength();
        }

        @Override
        public Sha256Digest digest() {
            return identity.bodySha256();
        }

        @Override
        public Sha256Digest exactHeadValueSha256() {
            return exactHeadValueSha256;
        }
    }
}
