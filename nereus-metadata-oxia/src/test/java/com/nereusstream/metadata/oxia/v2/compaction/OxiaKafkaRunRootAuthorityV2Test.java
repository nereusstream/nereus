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

package com.nereusstream.metadata.oxia.v2.compaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.KafkaTopicId;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.KafkaTopicName;
import com.nereusstream.metadata.oxia.v2.mutation.AuthorityRecord;
import com.nereusstream.metadata.oxia.v2.mutation.OxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2;
import com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2.Scope;
import com.nereusstream.storage.api.kafka.KafkaRunRootSnapshotV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootStateV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootVerifierV2;
import com.nereusstream.storage.api.lifecycle.MetadataNamespaceIdentityV2;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2;
import com.nereusstream.storage.object.gc.SyntheticDeleteAuthorityFixturesV2;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class OxiaKafkaRunRootAuthorityV2Test {
    @Test
    void canonicalRecordsBindFullRootAndPermanentSuccessorWithoutChangingInitialIdentity() {
        var f = new Fixture();
        var active = f.record(root(1, 0, 0));
        var sealed = active.admit().seal(sealed(active.root(), 10));
        var child = f.record(root(2, 1, 10));
        var chosen = sealed.select(child);
        for (var record : List.of(active, active.admit(), sealed, chosen)) {
            assertThat(KafkaRunRootRecordV2.decode(record.encode())).isEqualTo(record);
            assertThat(record.initialLink()).isEqualTo(active.initialLink());
            assertThat(record.encode().length()).isLessThan(1024);
        }
        assertThat(KafkaRunRootRecordV2.Link.decode(child.initialLink().encode()))
                .isEqualTo(child.initialLink());
        assertThatThrownBy(() -> chosen.select(f.record(root(3, 1, 10)))).isInstanceOf(IllegalArgumentException.class);
        var bytes = chosen.encode().toByteArray();
        bytes[8] ^= 1;
        assertThatThrownBy(() -> KafkaRunRootRecordV2.decode(CanonicalBytes.copyOf(bytes)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> KafkaRunRootRecordV2.decode(CanonicalBytes.copyOf(Arrays.copyOf(bytes, 10))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void createSealAndSuccessorUseNativeVersionsAndKeepSealedHistoryReadable() {
        var f = new Fixture();
        var a = root(1, 0, 0);
        var b = root(2, 1, 10);
        f.admit(a, b);
        assertThat(join(f.roots.createRoot(a)).exactProof()).contains(a);
        var sealed = sealed(a, 10);
        assertThat(join(f.roots.sealRoot(a, sealed)).exactProof()).contains(sealed);
        assertThat(join(f.roots.createSuccessor(sealed, b)).exactProof()).contains(b);
        assertThat(join(f.roots.openRoot(a.runId()))).contains(sealed);
        assertThat(join(f.roots.openRoot(b.runId()))).contains(b);
        assertThat(f.stored(a).successor()).contains(f.record(b).initialLink());
        assertThat(join(f.roots.readSelectedRoot(f.roots.nativeRootKey(a.runId()))))
                .contains(f.stored(a));
        assertThatThrownBy(() -> f.roots.readSelectedRoot(f.roots.nativeGenesisKey()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> f.roots.readSelectedRoot("/foreign/" + f.roots.nativeRootKey(a.runId())))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(f.tickets(a)).isZero();
        assertThat(f.tickets(b)).isZero();
        assertThat(f.verifications).isEqualTo(3);
        assertThat(join(f.roots.createRoot(a)).outcome()).isEqualTo(ProviderMutationOutcomeV1.FENCED_OR_CONFLICT);
    }

    @Test
    void concurrentGenesisPrewritesStayInvisibleAndOnlyOneChoiceWins() {
        var f = new Fixture();
        var a = root(1, 0, 0);
        var b = root(2, 0, 0);
        f.admit(a, b);
        f.client.holdKey = f.roots.nativeGenesisKey();
        var held = f.roots.createRoot(a).toCompletableFuture();
        assertThat(join(f.roots.openRoot(a.runId()))).isEmpty();
        assertThat(join(f.roots.readSelectedRoot(f.roots.nativeRootKey(a.runId()))))
                .isEmpty();
        assertThat(f.tickets(a)).isEqualTo(1);
        assertThat(join(f.roots.createRoot(b)).exactProof()).contains(b);
        f.client.release.run();
        assertThat(held.join().outcome()).isEqualTo(ProviderMutationOutcomeV1.FENCED_OR_CONFLICT);
        assertThat(join(f.roots.openRoot(a.runId()))).isEmpty();
        assertThat(join(f.roots.openRoot(b.runId()))).contains(b);
        assertThat(f.tickets(a)).isZero();
    }

    @Test
    void competingSuccessorsRaceAtTheExactParentCasAndCannotFork() {
        var f = new Fixture();
        var a = root(1, 0, 0);
        var b = root(2, 1, 10);
        var c = root(3, 1, 10);
        f.admit(a, b, c);
        join(f.roots.createRoot(a));
        var sealed = sealed(a, 10);
        join(f.roots.sealRoot(a, sealed));
        f.client.holdKey = f.roots.nativeRootKey(a.runId());
        var old = f.roots.createSuccessor(sealed, b).toCompletableFuture();
        assertThat(join(f.roots.openRoot(b.runId()))).isEmpty();
        assertThat(join(f.roots.createSuccessor(sealed, c)).exactProof()).contains(c);
        var winner = f.client.values.get(f.roots.nativeRootKey(a.runId()));
        f.client.release.run();
        assertThat(old.join().outcome()).isEqualTo(ProviderMutationOutcomeV1.FENCED_OR_CONFLICT);
        assertThat(f.client.values.get(f.roots.nativeRootKey(a.runId()))).isEqualTo(winner);
        assertThat(join(f.roots.openRoot(b.runId()))).isEmpty();
        assertThat(join(f.roots.openRoot(c.runId()))).contains(c);
        for (var root : List.of(a, b, c)) {
            assertThat(f.tickets(root)).isZero();
        }
    }

    @Test
    void lostSelectedResponseAndReadDeliveryRetainTicketsUntilFreshRecovery() {
        var f = new Fixture();
        var a = root(1, 0, 0);
        var b = root(2, 1, 10);
        f.admit(a, b);
        join(f.roots.createRoot(a));
        var sealed = sealed(a, 10);
        join(f.roots.sealRoot(a, sealed));
        f.client.loseAndBlock = f.roots.nativeRootKey(a.runId());
        assertThat(join(f.roots.createSuccessor(sealed, b)).outcome())
                .isEqualTo(ProviderMutationOutcomeV1.OUTCOME_UNKNOWN);
        assertThat(f.tickets(a)).isEqualTo(1);
        assertThat(f.tickets(b)).isEqualTo(1);
        f.client.blockReads = false;
        var restarted = f.authority();
        assertThat(f.stored(b).admitted()).isFalse();
        assertThat(join(restarted.openRoot(b.runId()))).contains(b);
        int writes = f.client.rootWrites;
        assertThat(join(restarted.createSuccessor(sealed, b)).exactProof()).contains(b);
        assertThat(f.client.rootWrites).isEqualTo(writes);
        assertThat(f.tickets(a)).isZero();
        assertThat(f.tickets(b)).isZero();
    }

    @Test
    void observerCancellationCannotAbandonAnAdmittedMutationOrClearTicketsEarly() {
        var f = new Fixture();
        var a = root(1, 0, 0);
        f.admit(a);
        f.client.holdKey = f.roots.nativeGenesisKey();
        var observer = f.roots.createRoot(a).toCompletableFuture();
        assertThat(observer.cancel(false)).isTrue();
        assertThat(f.tickets(a)).isEqualTo(1);
        f.client.release.run();
        assertThat(f.tickets(a)).isZero();
        assertThat(join(f.roots.openRoot(a.runId()))).contains(a);
    }

    @Test
    void fencedPhysicalTargetPreventsNativeVerificationAndRootPrewrite() {
        var f = new Fixture();
        var a = root(1, 0, 0);
        f.admit(a);
        var resource = f.record(a).resource();
        f.client.put(
                resource.authorityKey(),
                M5TargetDeleteAuthorityCodecV1.encodeAuthority(
                        SyntheticDeleteAuthorityFixturesV2.phases(resource).get(1)));
        assertThat(join(f.roots.createRoot(a)).outcome()).isEqualTo(ProviderMutationOutcomeV1.FENCED_OR_CONFLICT);
        assertThat(f.client.rootWrites).isZero();
        assertThat(f.verifications).isZero();
    }

    private static <T> T join(CompletionStage<T> stage) {
        return stage.toCompletableFuture().join();
    }

    private static Sha256Digest digest(String text) {
        return SyntheticDeleteAuthorityFixturesV2.digest(text);
    }

    private static KafkaRunRootSnapshotV1 root(int id, int parent, long start) {
        return new KafkaRunRootSnapshotV1(
                new TopicBindingId(digest("binding")),
                new KafkaTopicIncarnationIdentity(new KafkaTopicId(new Id128(0, 2)), new KafkaTopicName("orders")),
                0,
                new StorageEpochId(digest("storage")),
                3,
                2,
                new CellProviderScopeId(digest("scope")),
                new StorageRunId(new Id128(0, id)),
                new BookKeeperLedgerIdentity(id),
                start,
                OptionalLong.empty(),
                KafkaRunRootStateV1.ACTIVE,
                parent == 0 ? Optional.empty() : Optional.of(new StorageRunId(new Id128(0, parent))));
    }

    private static KafkaRunRootSnapshotV1 sealed(KafkaRunRootSnapshotV1 root, long end) {
        return new KafkaRunRootSnapshotV1(
                root.bindingId(),
                root.topicIncarnation(),
                root.partitionId(),
                root.storageEpochId(),
                root.creatorOwnerEpoch(),
                root.kafkaLeaderEpoch(),
                root.providerScopeId(),
                root.runId(),
                root.ledgerIdentity(),
                root.kafkaStartOffset(),
                OptionalLong.of(end),
                KafkaRunRootStateV1.SEALED,
                root.predecessorRunId());
    }

    private static final class Fixture {
        final Client client = new Client();
        final PhysicalNamespaceAuthorityBindingV2 binding = new PhysicalNamespaceAuthorityBindingV2(
                new PhysicalResourceIdV2.Namespace(
                        PhysicalResourceIdV2.ProviderKind.BOOKKEEPER,
                        CanonicalUtf8.fromString("actual-instance-fixture"),
                        CanonicalUtf8.fromString("ledger-id-space")),
                new MetadataNamespaceIdentityV2(new Id128(0, 11), 7));
        final M5TargetDeleteMultiWriterGuardV2 guard = new M5TargetDeleteMultiWriterGuardV2(
                new M5TargetDeleteAuthorityCoordinatorV1(new Oxia09ExactMetadataTransactionStoreV1(client)));
        final OxiaKafkaRunRootAuthorityV2 roots = authority();
        int verifications;

        OxiaKafkaRunRootAuthorityV2 authority() {
            return new OxiaKafkaRunRootAuthorityV2(
                    client,
                    binding,
                    Scope.of(root(1, 0, 0)),
                    new KafkaRunRootVerifierV2() {
                        public Sha256Digest capabilitySha256() {
                            return digest("capability");
                        }

                        public CompletionStage<Void> requireNative(KafkaRunRootRecordV2 record) {
                            assertThat(tickets(record.root())).isPositive();
                            verifications++;
                            return CompletableFuture.completedFuture(null);
                        }
                    },
                    guard);
        }

        KafkaRunRootRecordV2 record(KafkaRunRootSnapshotV1 root) {
            return new KafkaRunRootRecordV2(
                    new PhysicalResourceIdV2.BookKeeperLedger(
                            binding.physicalNamespace(), root.ledgerIdentity().ledgerId()),
                    root,
                    root.state() == KafkaRunRootStateV1.SEALED,
                    Optional.empty());
        }

        void admit(KafkaRunRootSnapshotV1... roots) {
            for (var root : roots) {
                var resource = record(root).resource();
                client.put(
                        resource.authorityKey(),
                        M5TargetDeleteAuthorityCodecV1.encodeAuthority(
                                SyntheticDeleteAuthorityFixturesV2.phases(resource)
                                        .get(0)));
            }
        }

        KafkaRunRootRecordV2 stored(KafkaRunRootSnapshotV1 root) {
            return KafkaRunRootRecordV2.decode(
                    client.values.get(roots.nativeRootKey(root.runId())).storedBytes());
        }

        int tickets(KafkaRunRootSnapshotV1 root) {
            return M5TargetDeleteAuthorityCodecV1.decodeAuthority(client.values
                            .get(record(root).resource().authorityKey())
                            .storedBytes())
                    .activeWriterTickets()
                    .size();
        }
    }

    private static final class Client implements OxiaConditionalClient {
        final Map<String, AuthorityRecord> values = new HashMap<>();
        long nextVersion;
        String holdKey;
        String loseAndBlock;
        boolean blockReads;
        Runnable release;
        int rootWrites;

        void put(String key, CanonicalBytes bytes) {
            values.put(key, new AuthorityRecord(key, bytes, ++nextVersion));
        }

        public CompletionStage<Optional<AuthorityRecord>> read(String key) {
            return blockReads && key.startsWith("/")
                    ? CompletableFuture.failedFuture(new IllegalStateException("native read delivery lost"))
                    : CompletableFuture.completedFuture(Optional.ofNullable(values.get(key)));
        }

        public CompletionStage<Void> createIfAbsent(String key, CanonicalBytes bytes) {
            return mutate(key, bytes, -1);
        }

        public CompletionStage<Void> compareAndSet(String key, CanonicalBytes bytes, long version) {
            return mutate(key, bytes, version);
        }

        private CompletionStage<Void> mutate(String key, CanonicalBytes bytes, long version) {
            if (key.equals(holdKey)) {
                holdKey = null;
                var future = new CompletableFuture<Void>();
                release = () -> apply(key, bytes, version).whenComplete((ignored, failure) -> {
                    if (failure == null) {
                        future.complete(null);
                    } else {
                        future.completeExceptionally(failure);
                    }
                });
                return future;
            }
            return apply(key, bytes, version);
        }

        private CompletionStage<Void> apply(String key, CanonicalBytes bytes, long version) {
            var current = values.get(key);
            if (current == null ? version != -1 : current.versionId() != version) {
                return CompletableFuture.failedFuture(new IllegalStateException("native exact version conflict"));
            }
            put(key, bytes);
            if (key.startsWith("/")) {
                rootWrites++;
            }
            if (key.equals(loseAndBlock)) {
                loseAndBlock = null;
                blockReads = true;
                return CompletableFuture.failedFuture(new IllegalStateException("native applied response lost"));
            }
            return CompletableFuture.completedFuture(null);
        }
    }
}
