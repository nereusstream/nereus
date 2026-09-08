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

package com.nereusstream.kafka.bookkeeper.compaction;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateClientV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeDeleteAuthorityV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeDeleteAuthorityV2.Snapshot;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeDeleteIntentV2;
import com.nereusstream.storage.object.gc.DeleteObservationAuthorityVerifierV2;
import com.nereusstream.storage.object.gc.DeleteObservationContextV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityStateMachineV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Native GC owner/capability facts and intent binding. Protocol eligibility and dispatch admission are separate. */
public final class KafkaBookKeeperDeleteObservationAuthorityV2 implements DeleteObservationAuthorityVerifierV2 {
    private static final String FACT_PREFIX = "v2/bk-native-delete-epoch/";
    private static final int VERSION_MAGIC = 0x4d354556; // M5EV
    private final M5BookKeeperNativeDeleteAuthorityV2 nativeAuthority;
    private final KafkaBookKeeperDeleteIdentityReaderV2 reader;
    private final UUID owner;
    private final PhysicalResourceIdV2.BookKeeperLedger resource;
    private final Sha256Digest resourceSha;
    private final String key;

    public KafkaBookKeeperDeleteObservationAuthorityV2(
            M5BookKeeperNativeCreateClientV2 client, RunLedgerHandleV1 handle, UUID owner) {
        this.nativeAuthority = client.deleteAuthority(handle);
        this.reader = new KafkaBookKeeperDeleteIdentityReaderV2(client, handle);
        this.owner = Objects.requireNonNull(owner, "owner");
        if (owner.equals(new UUID(0, 0))) {
            throw new IllegalArgumentException("native GC owner is empty");
        }
        resource = nativeAuthority.resource();
        resourceSha = Sha256Digest.hash(resource.canonicalBytes());
        key = FACT_PREFIX + resourceSha.toHex() + "/fact-v1";
    }

    public String factKey() {
        return key;
    }

    /** Claim is explicit and separate from observing/binding; every successor invalidates old native operations. */
    public CompletionStage<Snapshot> claim(Optional<Snapshot> previous) {
        return nativeAuthority.claim(previous, owner);
    }

    public CompletionStage<DeleteObservationContextV2> observe(
            long observationEpoch, Optional<DeleteObservationContextV2> predecessor) {
        Objects.requireNonNull(predecessor, "predecessor");
        return currentSnapshot().thenCompose(snapshot -> {
            var fact = fact(value(snapshot));
            boolean changed = predecessor.isPresent()
                    && !predecessor.orElseThrow().coordinatorOwner().equals(fact);
            var context = new DeleteObservationContextV2(
                    observationEpoch, fact, fact, changed ? Optional.of(fact) : Optional.empty());
            CompletionStage<Void> verified = changed
                    ? requirePredecessorFenced(resource, predecessor.orElseThrow(), context)
                    : requireCurrent(resource, context);
            return verified.thenApply(ignored -> context);
        });
    }

    @Override
    public CompletionStage<Void> requireCurrent(
            PhysicalResourceIdV2 expectedResource, DeleteObservationContextV2 context) {
        if (!resource.equals(expectedResource)) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("native GC observation resource differs"));
        }
        return currentSnapshot().thenAccept(snapshot -> {
            var expected = fact(value(snapshot));
            if (!context.coordinatorOwner().equals(expected)
                    || !context.capability().equals(expected)
                    || context.predecessorOwnerFenced()
                            .filter(value -> !value.equals(expected))
                            .isPresent()) {
                throw new IllegalStateException("native GC observation does not match actual epoch bytes/version");
            }
        });
    }

    @Override
    public CompletionStage<Void> requirePredecessorFenced(
            PhysicalResourceIdV2 expectedResource,
            DeleteObservationContextV2 previous,
            DeleteObservationContextV2 successor) {
        if (!previous.coordinatorOwner().equals(previous.capability())
                || !successor.predecessorOwnerFenced().equals(Optional.of(successor.coordinatorOwner()))) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("native predecessor proof roles differ"));
        }
        int oldVersion = nativeVersion(previous.coordinatorOwner());
        int newVersion = nativeVersion(successor.coordinatorOwner());
        if (newVersion <= oldVersion) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("native predecessor epoch was not advanced"));
        }
        // Native epoch versions never decrease/reappear and participate atomically in every guarded delete.
        return requireCurrent(expectedResource, successor);
    }

    /** Actual native bytes in a separate read-only fact family; other facts remain owned by their native producers. */
    public ExactMetadataTransactionStoreV1 readOnlyFacts(ExactMetadataTransactionStoreV1 otherFacts) {
        Objects.requireNonNull(otherFacts, "otherFacts");
        return new ExactMetadataTransactionStoreV1() {
            public CompletionStage<Optional<VersionedValue>> read(String requested) {
                if (!requested.startsWith(FACT_PREFIX)) {
                    return otherFacts.read(requested);
                }
                if (!key.equals(requested)) {
                    return CompletableFuture.failedFuture(new IllegalArgumentException("native GC fact route differs"));
                }
                return nativeAuthority.read().thenApply(snapshot -> snapshot.map(value -> value(value)));
            }

            public CompletionStage<MutationOutcome> compareAndSet(
                    Optional<VersionedValue> previous, String requested, CanonicalBytes candidate) {
                return CompletableFuture.failedFuture(
                        new UnsupportedOperationException("native GC fact route is read-only"));
            }

            public CompletionStage<TransactionOutcome> conditionalTransaction(ExactTransaction transaction) {
                return CompletableFuture.completedFuture(TransactionOutcome.UNSUPPORTED);
            }

            public boolean supportsAtomicMultiKeyTransactions() {
                return false;
            }
        };
    }

    /**
     * Binds the actual stored M5 INTENT to native epoch/token/full identity. This does not dispatch deletion or
     * supply eligibility/grace/Cell capacity. Changed metadata retains a fenced native binding for later recovery.
     */
    public CompletionStage<M5BookKeeperNativeDeleteIntentV2> bindIntent(
            ExactMetadataTransactionStoreV1 metadata, VersionedValue exactIntent) {
        Objects.requireNonNull(metadata, "metadata");
        var intent = M5TargetDeleteAuthorityCodecV1.decodeAuthority(exactIntent.canonicalStoredBytes());
        var context = M5TargetDeleteAuthorityStateMachineV1.dispatchContext(intent);
        if (!resource.authorityKey().equals(exactIntent.key())
                || !resource.equals(intent.target().resourceId())) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("native intent resource/key differs"));
        }
        CompletionStage<M5BookKeeperNativeDeleteIntentV2> operation = requireExactIntent(metadata, exactIntent)
                .thenCompose(ignored -> requireCurrent(resource, context))
                .thenCompose(
                        ignored -> reader.rereadTarget(intent.externalIdentity().orElseThrow()))
                .thenCompose(target -> {
                    if (target.isEmpty()) {
                        return CompletableFuture.failedFuture(
                                new IllegalStateException("native target is absent; reconcile M5 done"));
                    }
                    return currentSnapshot().thenCompose(snapshot -> {
                        if (!fact(value(snapshot)).equals(context.coordinatorOwner())) {
                            return CompletableFuture.failedFuture(
                                    new IllegalStateException("native intent epoch changed"));
                        }
                        return nativeAuthority.bindIntent(
                                snapshot,
                                intent.deleteIntent().orElseThrow().dispatchTokenSha256(),
                                exactIntent.canonicalStoredSha256(),
                                target.orElseThrow().metadataSha256());
                    });
                })
                .thenCompose(bound -> requireCurrent(resource, context)
                        .thenCompose(ignored -> requireExactIntent(metadata, exactIntent))
                        .thenApply(ignored -> bound));
        return operation.thenApply(value -> value);
    }

    private CompletionStage<Void> requireExactIntent(
            ExactMetadataTransactionStoreV1 metadata, VersionedValue expected) {
        return metadata.read(expected.key()).thenAccept(actual -> {
            if (!actual.equals(Optional.of(expected))) {
                throw new IllegalStateException("M5 intent is no longer the exact stored authority");
            }
        });
    }

    private CompletionStage<Snapshot> currentSnapshot() {
        return nativeAuthority.read().thenCompose(stored -> {
            var snapshot = stored.orElseThrow(() -> new IllegalStateException("native GC epoch is absent"));
            if (!snapshot.owner().equals(owner)) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("native GC owner differs from this coordinator"));
            }
            return nativeAuthority.requireCurrent(snapshot).thenApply(ignored -> snapshot);
        });
    }

    private VersionedValue value(Snapshot snapshot) {
        if (!snapshot.resource().equals(resource)) {
            throw new IllegalArgumentException("native GC fact resource differs");
        }
        var version = new MetadataVersion(CanonicalBytes.copyOf(ByteBuffer.allocate(40)
                .putInt(VERSION_MAGIC)
                .put(resourceSha.bytes().toByteArray())
                .putInt(snapshot.nativeVersion())
                .array()));
        // Snapshot.encode is the exact canonical body validated by the native getData reader, not a projection.
        return VersionedValue.of(key, snapshot.encode(), version);
    }

    private int nativeVersion(AuthorityFactV1 fact) {
        if (!key.equals(fact.key()) || fact.metadataVersion().value().length() != 40) {
            throw new IllegalArgumentException("native GC fact version route differs");
        }
        var in = ByteBuffer.wrap(fact.metadataVersion().value().toByteArray());
        int magic = in.getInt();
        byte[] digest = new byte[32];
        in.get(digest);
        int version = in.getInt();
        if (magic != VERSION_MAGIC || !resourceSha.equals(Sha256Digest.copyOf(digest)) || version < 0) {
            throw new IllegalArgumentException("native GC fact version is invalid or foreign");
        }
        return version;
    }

    private static AuthorityFactV1 fact(VersionedValue value) {
        return new AuthorityFactV1(value.key(), value.metadataVersion(), value.canonicalStoredSha256());
    }
}
