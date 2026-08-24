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

package com.nereusstream.storage.object.control;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.object.kms.KmsCellSession;
import com.nereusstream.storage.object.kms.KmsTransport;
import com.nereusstream.storage.object.kms.RunKeyCacheIdentity;
import com.nereusstream.storage.object.kms.WrappedRunKeyEnvelope;
import com.nereusstream.storage.object.provider.C1ObjectProviderSession;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.provider.ObjectProviderCapabilities;
import com.nereusstream.storage.object.provider.ObjectProviderTransport;
import com.nereusstream.storage.object.provider.ProviderObjectOutcome;
import com.nereusstream.storage.object.recovery.RecoveryEnvelopeExceededException;
import com.nereusstream.storage.object.recovery.RecoveryEnvelopeLimits;
import java.io.IOException;
import java.io.InputStream;
import java.security.SecureRandom;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class WalRunObjectSessionTest {
    @Test
    void ownsProviderAndKmsLifecycleAndErasesRunKeysOnClose() {
        WalRunRootRecord root = ObjectWalControlTestFixtures.root(1, Optional.empty());
        WalRunRuntime runtime = new WalRunRuntime(root);
        C1ObjectProviderSession provider = provider(root);
        KmsCellSession kms = kms(root);
        kms.createRunKey(new RunKeyCacheIdentity(99, 1));
        assertThat(kms.cachedRunKeyCount()).isEqualTo(1);
        WalRunObjectSession session = new WalRunObjectSession(root, runtime, provider, kms, () -> 0);

        session.close();

        assertThat(runtime.state()).isEqualTo(WalRunRuntime.State.STOPPING);
        assertThat(runtime.stopReason()).contains(WalRunRuntime.StopReason.OWNER_REQUEST);
        assertThat(provider.state()).isEqualTo(C1ObjectProviderSession.State.CLOSED);
        assertThat(kms.state()).isEqualTo(KmsCellSession.State.OPEN);
        assertThat(kms.cachedRunKeyCount()).isOne();
        kms.evict(new RunKeyCacheIdentity(99, 1));
        kms.close();
        assertThat(kms.state()).isEqualTo(KmsCellSession.State.CLOSED);
        assertThatThrownBy(session::runtimeState)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
    }

    @Test
    void drainPreservesKmsAndUnknownCandidateUntilReconcileThenClose() throws Exception {
        WalRunRootRecord root = ObjectWalControlTestFixtures.root(1, Optional.empty());
        UnknownProviderTransport transport = new UnknownProviderTransport();
        C1ObjectProviderSession provider = new C1ObjectProviderSession(
                transport,
                root.providerScopeId(),
                root.providerConfiguration().exclusiveNamespacePrefix(),
                root.providerConfiguration().maxObjectBodyBytes(),
                root.nwg1AdmissionCaps().maxDirectoryPrefixBytes());
        KmsCellSession kms = kms(root);
        kms.createRunKey(new RunKeyCacheIdentity(99, 1));
        WalRunObjectSession session = new WalRunObjectSession(root, new WalRunRuntime(root), provider, kms, () -> 0);
        CanonicalBytes body = CanonicalBytes.copyOf(new byte[] {1, 2, 3});
        ObjectIdentity identity = new ObjectIdentity(
                root.providerConfiguration().exclusiveNamespacePrefix()
                        + "/protocol/kafka/nwkcp1-v1/objects/sha256-v1-"
                        + Sha256Digest.hash(body).toHex()
                        + ".nwkcp1",
                body.length(),
                Sha256Digest.hash(body));
        WalRunObjectSession.ValidatedKafkaProtocolObject candidate =
                session.validateKafkaProtocolObject(identity, body);

        assertThat(session.conditionalCreateKafkaProtocolObject(candidate).outcome())
                .isEqualTo(ProviderObjectOutcome.OUTCOME_UNKNOWN);
        session.drain();
        assertThat(session.state()).isEqualTo(WalRunObjectSession.State.DRAINING);
        assertThatThrownBy(session::close)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("reconcile before close");
        assertThat(session.state()).isEqualTo(WalRunObjectSession.State.DRAINING);
        assertThat(kms.state()).isEqualTo(KmsCellSession.State.OPEN);
        assertThat(kms.cachedRunKeyCount()).isEqualTo(1);
        assertThatThrownBy(kms::close).isInstanceOf(IllegalStateException.class).hasMessageContaining("transferred");
        // The WalRun lease fences only its exact run identity. An unrelated raw Cell cache entry remains
        // independently owned and may be evicted while the recovered/active run is draining.
        kms.evict(new RunKeyCacheIdentity(99, 1));
        assertThat(kms.cachedRunKeyCount()).isZero();

        assertThat(session.reconcileUnknownProtocolObject(identity).outcome())
                .isEqualTo(ProviderObjectOutcome.DEFINITIVELY_NOT_APPLIED);
        session.close();
        assertThat(session.state()).isEqualTo(WalRunObjectSession.State.CLOSED);
        assertThat(kms.cachedRunKeyCount()).isZero();
        assertThat(transport.putCalls).isEqualTo(1);
        assertThat(transport.listCalls).isEqualTo(1);
        assertThat(transport.lastListPrefix).isEqualTo(identity.key());
        assertThat(transport.lastListMaximumKeys).isEqualTo(1);
        assertThat(transport.fullGetCalls).isEqualTo(1);
    }

    @Test
    void sameCandidatePut2ConsumesRootRetryBudgetBeforeProviderIo() throws Exception {
        WalRunRootRecord root = withMaximumRetries(ObjectWalControlTestFixtures.root(1, Optional.empty()), 0);
        UnknownProviderTransport transport = new UnknownProviderTransport();
        C1ObjectProviderSession provider = new C1ObjectProviderSession(
                transport,
                root.providerScopeId(),
                root.providerConfiguration().exclusiveNamespacePrefix(),
                root.providerConfiguration().maxObjectBodyBytes(),
                root.nwg1AdmissionCaps().maxDirectoryPrefixBytes());
        WalRunObjectSession session = new WalRunObjectSession(root, provider, kms(root), () -> 0);
        CanonicalBytes body = CanonicalBytes.copyOf(new byte[] {1, 2, 3});
        ObjectIdentity identity = new ObjectIdentity(
                root.providerConfiguration().exclusiveNamespacePrefix()
                        + "/protocol/kafka/nwkcp1-v1/objects/sha256-v1-"
                        + Sha256Digest.hash(body).toHex()
                        + ".nwkcp1",
                body.length(),
                Sha256Digest.hash(body));
        WalRunObjectSession.ValidatedKafkaProtocolObject candidate =
                session.validateKafkaProtocolObject(identity, body);

        assertThat(session.conditionalCreateKafkaProtocolObject(candidate).outcome())
                .isEqualTo(ProviderObjectOutcome.OUTCOME_UNKNOWN);
        assertThatThrownBy(() -> session.conditionalCreateKafkaProtocolObject(candidate))
                .isInstanceOf(RecoveryEnvelopeExceededException.class)
                .hasMessageContaining("retry attempts");
        assertThat(transport.putCalls).isEqualTo(1);

        assertThat(session.reconcileUnknownProtocolObject(identity).outcome())
                .isEqualTo(ProviderObjectOutcome.DEFINITIVELY_NOT_APPLIED);
        session.close();
    }

    @Test
    void rejectsProviderOrKmsCellSubstitutionBeforeRuntimeUse() {
        WalRunRootRecord root = ObjectWalControlTestFixtures.root(1, Optional.empty());
        WalRunRootRecord other = ObjectWalControlTestFixtures.root(2, Optional.empty());

        assertThatThrownBy(() ->
                        new WalRunObjectSession(root, new WalRunRuntime(root), provider(other), kms(root), () -> 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("differs");
        assertThatThrownBy(() -> new WalRunObjectSession(
                        root,
                        new WalRunRuntime(root),
                        provider(root),
                        kms(new CellProviderScopeId(ObjectWalControlTestFixtures.digest(9))),
                        () -> 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("authorities differ");
    }

    private static C1ObjectProviderSession provider(WalRunRootRecord root) {
        return new C1ObjectProviderSession(
                new NoIoProviderTransport(),
                root.providerScopeId(),
                root.providerConfiguration().exclusiveNamespacePrefix(),
                root.providerConfiguration().maxObjectBodyBytes(),
                root.nwg1AdmissionCaps().maxDirectoryPrefixBytes());
    }

    private static KmsCellSession kms(WalRunRootRecord root) {
        return kms(root.providerScopeId());
    }

    private static KmsCellSession kms(CellProviderScopeId providerScopeId) {
        return new KmsCellSession(
                new FakeKmsTransport(), providerScopeId, "kms/cell-a", 2, new SecureRandom(new byte[] {1, 2, 3}));
    }

    private static WalRunRootRecord withMaximumRetries(WalRunRootRecord root, int maximumRetries) {
        RecoveryEnvelopeLimits limits = root.recoveryEnvelope();
        return new WalRunRootRecord(
                root.shardId(),
                root.shardRunEpoch(),
                root.walRunSessionId(),
                root.openedAtMillis(),
                root.protocolCellIdentity(),
                root.providerScopeId(),
                root.formatContract(),
                root.nwg1AdmissionCaps(),
                root.bounds(),
                root.checkpointPolicy(),
                root.providerConfiguration(),
                new RecoveryEnvelopeLimits(
                        limits.maxLiveRoots(),
                        limits.maxPredecessorRuns(),
                        limits.maxListPages(),
                        limits.maxListedKeys(),
                        limits.maxListedKeyBytes(),
                        limits.maxHeadRequests(),
                        limits.maxRangeGetRequests(),
                        limits.maxFullGetRequests(),
                        limits.maxCanonicalBodyBytes(),
                        limits.maxDecodedContexts(),
                        limits.maxDecodedFrames(),
                        limits.maxDecodedCommitSets(),
                        limits.maxWorkingMemoryBytes(),
                        limits.maxConcurrency(),
                        maximumRetries,
                        limits.maxWallTimeNanos()),
                root.wrappedRunKey(),
                root.predecessor());
    }

    private static final class FakeKmsTransport implements KmsTransport {
        @Override
        public WrappedRunKeyEnvelope wrap(String keyIdentity, byte[] plaintextRunKey) {
            return new WrappedRunKeyEnvelope(
                    "fake-kms", "aes-kw-v1", keyIdentity, "version-test", CanonicalBytes.copyOf(plaintextRunKey));
        }

        @Override
        public byte[] unwrap(WrappedRunKeyEnvelope envelope) {
            return envelope.wrappedKey().toByteArray();
        }
    }

    private static final class UnknownProviderTransport implements ObjectProviderTransport {
        private int putCalls;
        private int listCalls;
        private int fullGetCalls;
        private String lastListPrefix;
        private int lastListMaximumKeys;

        @Override
        public ObjectProviderCapabilities capabilities() {
            return new ObjectProviderCapabilities("fake", true, true, true, true, true, 1024 * 1024, 4096, 100);
        }

        @Override
        public ConditionalCreateResult putIfAbsent(ObjectIdentity identity, InputStream body) throws IOException {
            putCalls++;
            body.transferTo(java.io.OutputStream.nullOutputStream());
            return ConditionalCreateResult.RESPONSE_UNKNOWN;
        }

        @Override
        public StreamingObject get(String key, Optional<CanonicalBytes> exactVersionToken) throws IOException {
            fullGetCalls++;
            throw new MissingObjectException();
        }

        @Override
        public StreamingObject getRange(
                String key, long inclusiveStart, long exclusiveEnd, Optional<CanonicalBytes> versionToken) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListPage list(String prefix, Optional<CanonicalBytes> continuationToken, int maximumKeys) {
            listCalls++;
            lastListPrefix = prefix;
            lastListMaximumKeys = maximumKeys;
            return new ListPage(List.of(), Optional.empty());
        }

        @Override
        public FailureKind classifyFailure(IOException failure) {
            return failure instanceof MissingObjectException ? FailureKind.NOT_FOUND : FailureKind.FATAL;
        }

        private static final class MissingObjectException extends IOException {}
    }

    private static final class NoIoProviderTransport implements ObjectProviderTransport {
        @Override
        public ObjectProviderCapabilities capabilities() {
            return new ObjectProviderCapabilities("fake", true, true, true, true, true, 1024 * 1024, 4096, 100);
        }

        @Override
        public ConditionalCreateResult putIfAbsent(ObjectIdentity identity, InputStream body) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StreamingObject get(String key, Optional<CanonicalBytes> exactVersionToken) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StreamingObject getRange(
                String key, long inclusiveStart, long exclusiveEnd, Optional<CanonicalBytes> versionToken) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListPage list(String prefix, Optional<CanonicalBytes> continuationToken, int maximumKeys) {
            return new ListPage(List.of(), Optional.empty());
        }
    }
}
