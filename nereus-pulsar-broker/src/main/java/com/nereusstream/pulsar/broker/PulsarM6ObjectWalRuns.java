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

package com.nereusstream.pulsar.broker;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.protocol.ProtocolKindV1;
import com.nereusstream.domain.registry.allocator.ManagedLedgerIncarnationIdV1;
import com.nereusstream.metadata.oxia.v2.objectwal.OxiaCanonicalControlMetadataStore;
import com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.storage.object.control.CurrentWalRunPointer;
import com.nereusstream.storage.object.control.Nwg1RootAdmissionCaps;
import com.nereusstream.storage.object.control.ObjectProviderAccessProfile;
import com.nereusstream.storage.object.control.ObjectProviderRootConfiguration;
import com.nereusstream.storage.object.control.ProviderProofMode;
import com.nereusstream.storage.object.control.WalCheckpointHeadV1;
import com.nereusstream.storage.object.control.WalCheckpointPolicy;
import com.nereusstream.storage.object.control.WalCheckpointPublisher;
import com.nereusstream.storage.object.control.WalRunBounds;
import com.nereusstream.storage.object.control.WalRunControlCodec;
import com.nereusstream.storage.object.control.WalRunControlKeys;
import com.nereusstream.storage.object.control.WalRunFormatContractV1;
import com.nereusstream.storage.object.control.WalRunLifecycleManager;
import com.nereusstream.storage.object.control.WalRunObjectSession;
import com.nereusstream.storage.object.control.WalRunPredecessor;
import com.nereusstream.storage.object.control.WalRunReference;
import com.nereusstream.storage.object.control.WalRunRootRecord;
import com.nereusstream.storage.object.kms.KmsCellSession;
import com.nereusstream.storage.object.kms.RunKeyCacheIdentity;
import com.nereusstream.storage.object.nwg1.Nwg1ConstantsV1;
import com.nereusstream.storage.object.provider.C1ObjectProviderSession;
import com.nereusstream.storage.object.recovery.RecoveryEnvelopeLimits;
import io.oxia.client.api.AsyncOxiaClient;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/** Owns the current physical Root pointer and immutable creator proofs for one Pulsar Binding. */
final class PulsarM6ObjectWalRuns {
    private static final int SHARD = 0; // each Binding has its own metadata and Provider shard namespace
    private static final int MAX_EXTENTS = 1024;
    private static final int MAX_PREDECESSORS = 8;
    private static final int PAGE_ROWS = 64;
    private static final int UNCOVERED = 16;
    private static final int DIRECTORY_CAP = 16 * 1024;
    private static final int MAX_RECOVERY_RETRIES = 16;
    private static final long CHECKPOINT_MILLIS = 1_000;
    private static final long RUN_MILLIS = 30 * 60 * 1000L;
    private static final int MAX_CONTROL_BYTES = 1024 * 1024;
    private static final String POINTER = WalRunControlKeys.pointerKey(SHARD);

    private final PulsarM6ObjectWalProvider provider;
    private final OxiaCanonicalControlMetadataStore metadata;
    private final ExactMetadataTransactionStoreV1 exact;
    private final String prefix;
    private final String binding;
    private final Duration timeout;
    private final Nwg1RootAdmissionCaps caps;
    private final WalRunBounds bounds;
    private final WalCheckpointPolicy checkpoints;
    private final Sha256Digest capability;

    PulsarM6ObjectWalRuns(
            AsyncOxiaClient oxia,
            String authorityRoot,
            ManagedLedgerIncarnationIdV1 incarnation,
            PulsarM6ObjectWalProvider provider,
            int maximumEntryBytes,
            Duration timeout) {
        this.provider = provider;
        this.timeout = timeout;
        binding = incarnation.value().toHex();
        prefix = authorityRoot + "/native-object-wal/" + binding;
        metadata = new OxiaCanonicalControlMetadataStore(oxia, prefix, SHARD);
        exact = new Oxia09ExactMetadataTransactionStoreV1(oxia);
        var profile = provider.profile();
        capability = Sha256Digest.hash(profile.encode());
        int tag = Nwg1ConstantsV1.GCM_TAG_BYTES;
        long body =
                Math.min(profile.maximumObjectBytes(), Math.addExact((long) maximumEntryBytes, DIRECTORY_CAP + tag));
        body = Math.min(body, WalRunRootRecord.IMPLEMENTATION_MAX_CANONICAL_BODY_BYTES);
        if (maximumEntryBytes <= 0
                || maximumEntryBytes > Nwg1ConstantsV1.MAX_DECODED_FRAME_BYTES
                || body < maximumEntryBytes + (long) DIRECTORY_CAP + tag
                || profile.maximumRangeBytes() < Math.max(DIRECTORY_CAP, maximumEntryBytes + tag)) {
            throw new IllegalArgumentException("Object WAL cannot hold the full admitted Pulsar entry");
        }
        caps = new Nwg1RootAdmissionCaps(
                body,
                DIRECTORY_CAP,
                DIRECTORY_CAP - Nwg1ConstantsV1.HEADER_BYTES - tag,
                1,
                1,
                1,
                maximumEntryBytes,
                Math.addExact(maximumEntryBytes, tag),
                body,
                body);
        bounds = new WalRunBounds(MAX_EXTENTS, Math.multiplyExact(MAX_EXTENTS, body), RUN_MILLIS, MAX_PREDECESSORS);
        checkpoints = new WalCheckpointPolicy(
                CHECKPOINT_MILLIS,
                UNCOVERED,
                Math.multiplyExact(UNCOVERED, body),
                2 * CHECKPOINT_MILLIS,
                PAGE_ROWS,
                WalCheckpointPolicy.SUCCESSOR_PAGE_FIXED_BYTES + PAGE_ROWS * WalCheckpointPolicy.PROOF_NONE_ROW_BYTES);
    }

    record Selected(WalRunReference reference, WalRunRootRecord root) {}

    record Fresh(
            Selected selected,
            WalRunObjectSession session,
            WalCheckpointPublisher publisher,
            PulsarM6ObjectWalDispatchFence dispatch) {}

    OxiaCanonicalControlMetadataStore metadata() {
        return metadata;
    }

    Optional<Selected> current() {
        return metadata.get(POINTER)
                .map(bytes -> read(WalRunControlCodec.decodePointer(bytes).current()));
    }

    Selected read(WalRunReference reference) {
        var bytes = requireControl(reference.rootKey());
        if (!Sha256Digest.hash(bytes).equals(reference.rootSha256())) {
            throw new IllegalStateException("Selected Pulsar Object WAL Root digest differs");
        }
        var root = WalRunControlCodec.decodeRoot(bytes);
        requireRoot(reference, root);
        return new Selected(reference, root);
    }

    /** A candidate already selected by a prior attempt is recover-only. */
    Optional<Selected> recoverUnselected(Optional<WalRunPredecessor> predecessor, WalRunLifecycleManager lifecycle) {
        long epoch = predecessor
                .map(previous -> Math.incrementExact(previous.root().shardRunEpoch()))
                .orElse(1L);
        String key = WalRunControlKeys.rootKey(SHARD, epoch);
        return metadata.get(key).map(bytes -> {
            var reference = new WalRunReference(key, Sha256Digest.hash(bytes), SHARD, epoch);
            var selected = read(reference);
            if (!selected.root().predecessor().equals(predecessor)) {
                throw new IllegalStateException("Retained Pulsar Object WAL Root has a different predecessor");
            }
            if (predecessor.isEmpty()) {
                lifecycle.initializePointer(POINTER, new CurrentWalRunPointer(reference));
            } else {
                var prior = predecessor.orElseThrow();
                lifecycle.completeSealedPointer(
                        POINTER,
                        prior.root(),
                        Optional.of(new WalRunLifecycleManager.SuccessorCandidate(key, selected.root())));
            }
            if (!lifecycle.readPointer(POINTER).current().equals(reference)) {
                throw new IllegalStateException("Pulsar Object WAL pointer selected a different retained Root");
            }
            return selected;
        });
    }

    Fresh create(
            Optional<WalRunPredecessor> predecessor,
            PulsarM6ObjectWalOwner owner,
            KmsCellSession kms,
            WalRunLifecycleManager lifecycle)
            throws Exception {
        await(owner.requireCurrent());
        await(provider.requireCurrent());
        long epoch = predecessor
                .map(previous -> Math.incrementExact(previous.root().shardRunEpoch()))
                .orElse(1L);
        String rootKey = WalRunControlKeys.rootKey(SHARD, epoch);
        try (var key = kms.beginNewRunKey(new RunKeyCacheIdentity(SHARD, epoch))) {
            var root = createRoot(epoch, key, predecessor);
            var dispatch = dispatch(root);
            dispatch.initializeFresh();
            String creatorKey = creatorKey(WalRunControlCodec.rootSha256(root));
            var witness = owner.witness();
            await(exact.compareAndSet(Optional.empty(), creatorKey, witness));
            if (!await(exact.read(creatorKey))
                    .map(ExactMetadataTransactionStoreV1.VersionedValue::canonicalStoredBytes)
                    .equals(Optional.of(witness))) {
                throw new IllegalStateException("Pulsar Object WAL Root creator remains unresolved");
            }
            await(owner.requireCurrent());
            WalRunReference reference;
            Optional<WalRunLifecycleManager.NewWalRunOwnerAuthority> original;
            if (predecessor.isEmpty()) {
                var created = lifecycle.createRootAndInitializePointer(rootKey, root, key);
                reference = created.reference();
                original = created.ownerAuthority();
            } else {
                var prior = predecessor.orElseThrow();
                var sealBytes = requireControl(prior.sealKey());
                if (!Sha256Digest.hash(sealBytes).equals(prior.sealSha256())) {
                    throw new IllegalStateException("Pulsar Object WAL predecessor Seal digest differs");
                }
                key.markRootPublicationAttempt(root);
                var created = lifecycle.publishSuccessorAndAdvanceWithOwnerAuthority(
                        POINTER,
                        new CurrentWalRunPointer(prior.root()),
                        prior.sealKey(),
                        WalRunControlCodec.decodeSeal(sealBytes),
                        rootKey,
                        root);
                reference = created.reference();
                original = created.ownerAuthority();
            }
            await(owner.requireCurrent());
            var authority = original.orElseThrow(
                    () -> new IllegalStateException("Pulsar Object WAL Root must reenter through fenced recovery"));
            var io = provider.openRun(root, dispatch);
            WalRunObjectSession session = null;
            try {
                session = WalRunObjectSession.openNew(authority, io, key, System::nanoTime);
                var publisher = new WalCheckpointPublisher(
                        metadata,
                        WalRunControlKeys.checkpointHeadKey(SHARD, epoch),
                        WalRunControlKeys.checkpointPagePrefix(SHARD, epoch),
                        root,
                        WalCheckpointHeadV1.empty(reference.rootSha256(), epoch, owner.epoch()),
                        session);
                publisher.initializeHead();
                dispatch.admitAfterPhysicalHead();
                return new Fresh(new Selected(reference, root), session, publisher, dispatch);
            } catch (Exception | Error failure) {
                try {
                    if (session == null) {
                        io.close();
                    } else {
                        session.close();
                    }
                } catch (RuntimeException cleanup) {
                    failure.addSuppressed(cleanup);
                }
                throw failure;
            }
        }
    }

    private WalRunRootRecord createRoot(
            long epoch, KmsCellSession.NewRunKeyCreation key, Optional<WalRunPredecessor> predecessor) {
        var profile = provider.profile();
        var config = providerConfiguration(epoch);
        return new WalRunRootRecord(
                SHARD,
                epoch,
                Id128.fromBytes(PulsarM6ObjectWalProjection.newId().toByteArray()),
                System.currentTimeMillis(),
                profile.protocolCell(),
                profile.scopeId(),
                WalRunFormatContractV1.frozen(),
                caps,
                bounds,
                checkpoints,
                config,
                recoveryEnvelope(config),
                key.wrappedRunKey(),
                predecessor);
    }

    private RecoveryEnvelopeLimits recoveryEnvelope(ObjectProviderRootConfiguration providerConfig) {
        var active = WalRunRootRecord.boundedRecoveryEnvelope(
                ProtocolKindV1.PULSAR,
                bounds,
                caps,
                checkpoints,
                providerConfig,
                1,
                MAX_RECOVERY_RETRIES,
                timeout.toNanos());
        long history = Math.multiplyExact(MAX_PREDECESSORS, bounds.maxExtentCount());
        return new RecoveryEnvelopeLimits(
                active.maxLiveRoots(),
                active.maxPredecessorRuns(),
                active.maxListPages(),
                active.maxListedKeys(),
                active.maxListedKeyBytes(),
                active.maxHeadRequests(),
                Math.addExact(active.maxRangeGetRequests(), Math.toIntExact(Math.multiplyExact(history, 2))),
                active.maxFullGetRequests(),
                Math.addExact(
                        active.maxCanonicalBodyBytes(),
                        Math.multiplyExact(MAX_PREDECESSORS, bounds.maxCanonicalBodyBytes())),
                Math.addExact(active.maxDecodedContexts(), history),
                Math.addExact(active.maxDecodedFrames(), history),
                Math.addExact(active.maxDecodedCommitSets(), history),
                active.maxWorkingMemoryBytes(),
                active.maxConcurrency(),
                active.maxRetryAttempts(),
                active.maxWallTimeNanos());
    }

    CanonicalBytes creator(Selected selected) throws Exception {
        return await(exact.read(creatorKey(selected.reference().rootSha256())))
                .orElseThrow(() -> new IllegalStateException("Pulsar Object WAL Root lacks its exact creator witness"))
                .canonicalStoredBytes();
    }

    private String creatorKey(Sha256Digest digest) {
        return prefix + "/creators/" + digest.toHex();
    }

    C1ObjectProviderSession readProvider(Selected selected) {
        return provider.openRun(selected.root(), null);
    }

    void abandonFresh(Fresh fresh, Throwable failure) {
        try {
            closeAndRequireDrained(fresh.selected());
        } catch (Exception cleanup) {
            if (cleanup instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            failure.addSuppressed(cleanup);
        }
        try {
            fresh.session().close();
        } catch (RuntimeException cleanup) {
            failure.addSuppressed(cleanup);
        }
    }

    PulsarM6ObjectWalDispatchFence.Closure closeAndRequireDrained(Selected selected) throws Exception {
        try (var io = readProvider(selected)) {
            return dispatch(selected.root()).closeAndRequireDrained(io);
        }
    }

    private PulsarM6ObjectWalDispatchFence dispatch(WalRunRootRecord root) {
        return new PulsarM6ObjectWalDispatchFence(exact, prefix, root, timeout);
    }

    private void requireRoot(WalRunReference reference, WalRunRootRecord root) {
        root.requireM3ProductionProviderProofMode();
        if (reference.shardId() != SHARD
                || root.shardId() != SHARD
                || root.shardRunEpoch() != reference.shardRunEpoch()
                || !root.protocolCellIdentity().equals(provider.profile().protocolCell())
                || !root.providerScopeId().equals(provider.profile().scopeId())
                || !root.providerConfiguration().equals(providerConfiguration(root.shardRunEpoch()))
                || !root.wrappedRunKey().wrappingKeyId().equals(provider.wrappingKeyId())) {
            throw new IllegalStateException("Pulsar Object WAL Root is outside its admitted Binding and Provider");
        }
    }

    private ObjectProviderRootConfiguration providerConfiguration(long epoch) {
        var profile = provider.profile();
        return new ObjectProviderRootConfiguration(
                ObjectProviderAccessProfile.C1_SINGLE_PUT_SINGLE_RANGE_STRONG_LIST,
                profile.adapterVersion(),
                "canonical-key-v1",
                profile.exclusiveNamespacePrefix() + "/object-wal/" + binding + "/" + epoch,
                ProviderProofMode.NONE,
                0,
                caps.maxCanonicalBodyBytes(),
                caps.maxCanonicalBodyBytes(),
                profile.maximumRangeBytes(),
                1,
                profile.maximumListPageKeys(),
                capability);
    }

    private CanonicalBytes requireControl(String key) {
        var bytes = metadata.get(key).orElseThrow(() -> new IllegalStateException("Missing Pulsar Object WAL control"));
        if (bytes.length() > MAX_CONTROL_BYTES) {
            throw new IllegalStateException("Pulsar Object WAL control exceeds its byte bound");
        }
        return bytes;
    }

    private <T> T await(CompletionStage<T> stage) throws Exception {
        return stage.toCompletableFuture().get(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }
}
