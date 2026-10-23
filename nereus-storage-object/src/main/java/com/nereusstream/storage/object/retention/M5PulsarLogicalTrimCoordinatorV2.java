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

package com.nereusstream.storage.object.retention;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetentionFloorSnapshotV1;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Exact trim protocol on the existing Pulsar aggregate authority. */
public final class M5PulsarLogicalTrimCoordinatorV2 extends M5SingleAuthorityLogicalTrimCoordinatorV2 {
    private final ExactMetadataTransactionStoreV1 metadata;
    private final String authorityKey;

    public M5PulsarLogicalTrimCoordinatorV2(ExactMetadataTransactionStoreV1 metadata, String authorityKey) {
        super(metadata, authorityKey, true);
        this.metadata = metadata;
        this.authorityKey = authorityKey;
    }

    /** First freeze every enrolled writer; no caller-declared floor snapshot is needed to enter this phase. */
    public java.util.concurrent.CompletionStage<Result> beginSourceScan(
            ExactMetadataTransactionStoreV1.VersionedValue exactOpen,
            com.nereusstream.domain.bytes.Sha256Digest attemptId) {
        var current = sourceAuthority(exactOpen);
        if (current.state() != M5PulsarAggregateAuthorityRecordsV1.PulsarAggregateAuthorityStateV1.OPEN_V1
                || current.writerEnrollment().isEmpty()
                || !current.referenceMutationTickets().isEmpty()) {
            return java.util.concurrent.CompletableFuture.completedFuture(
                    new Result(Outcome.RETAIN, java.util.Optional.empty()));
        }
        var scan =
                new M5PulsarAggregateAuthorityRecordsV1.PulsarLogicalTrimSourceScanV3(attemptId, sourceFact(exactOpen));
        var trim = current.logicalTrim()
                .orElse(new M5LogicalTrimRecordsV2.LogicalTrimStateV2(
                        java.util.Optional.empty(), java.util.Optional.empty()));
        return mutate(
                exactOpen,
                M5PulsarAggregateAuthorityCodecV1.encodeAuthority(M5PulsarAggregateAuthorityCodecV1.sourceScanSuccessor(
                        current, java.util.Optional.of(scan), trim)));
    }

    /** Actual same-key reread for a source observer; a stale/caller-created scan value grants no scan. */
    public java.util.concurrent.CompletionStage<Void> requireSourceScanCurrent(
            ExactMetadataTransactionStoreV1.VersionedValue exactScan) {
        sourceAuthority(exactScan).logicalTrimSourceScan().orElseThrow();
        return metadata.read(authorityKey).thenAccept(value -> {
            if (value.filter(exactScan::equals).isEmpty()) {
                throw new M5ReferenceFreshnessVerifierV1.StaleAuthorityException(
                        "Pulsar source-scan authority changed");
            }
        });
    }

    /** Binds the actual scan result before frontier commit, with the source-scan revision as its predecessor. */
    public java.util.concurrent.CompletionStage<Result> sealSourceScan(
            ExactMetadataTransactionStoreV1.VersionedValue exactScan,
            M5RetentionRecordsV1.RetentionFloorSnapshotV1 snapshot,
            com.nereusstream.domain.bytes.Sha256Digest attemptId) {
        var current = sourceAuthority(exactScan);
        requireSourceAttempt(current, attemptId);
        var nta = com.nereusstream.domain.codec.Nta1CodecV1.decode(current.canonicalAggregateBytes());
        var binding = new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity(
                current.bindingId(),
                com.nereusstream.domain.bytes.Sha256Digest.hash(
                        com.nereusstream.domain.codec.TopicIncarnationIdentityCodecV1.encode(current.incarnation())),
                nta.initialEpoch().storageEpochId().digest());
        if (!snapshot.identity().binding().equals(binding)
                || !snapshot.identity().capability().equals(current.capability())
                || snapshot.pulsarContext().isEmpty()) {
            throw new IllegalArgumentException("source-scan snapshot differs from the actual Pulsar Binding");
        }
        var fact = sourceFact(exactScan);
        boolean bound = snapshot.ownerFence().equals(fact)
                || snapshot.storageFence().equals(fact)
                || snapshot.rows().stream().anyMatch(row -> row.authority().equals(fact));
        if (!bound) {
            throw new IllegalArgumentException("floor snapshot lacks its exact source-scan authority");
        }
        var prior = current.logicalTrim().orElseThrow().frontier();
        new M5RetentionPlannerV1().plan(snapshot, prior);
        var trim = new M5LogicalTrimRecordsV2.LogicalTrimStateV2(
                prior,
                java.util.Optional.of(
                        new M5LogicalTrimRecordsV2.LogicalTrimFenceV2(attemptId, snapshot.snapshotRootSha256(), fact)));
        return new M5ReferenceFreshnessVerifierV1(metadata)
                .requireFresh(snapshot)
                .thenCompose(ignored -> persistSnapshot(snapshot))
                .thenCompose(confirmed -> confirmed
                        ? mutate(
                                exactScan,
                                M5PulsarAggregateAuthorityCodecV1.encodeAuthority(
                                        M5PulsarAggregateAuthorityCodecV1.sourceScanSuccessor(
                                                current, java.util.Optional.empty(), trim)))
                        : CompletableFuture.completedFuture(new Result(Outcome.RESPONSE_UNKNOWN, Optional.empty())));
    }

    /** Immutable payload adjacent to this exact authority; detached bytes grant no trim authority. */
    public String floorSnapshotKey(Sha256Digest snapshotRoot) {
        M5RetentionRecordsV1.requireDigest(snapshotRoot, "snapshot root");
        String key = authorityKey + "/logical-trim-floor-snapshots/" + snapshotRoot.toHex();
        if (key.getBytes(StandardCharsets.UTF_8).length > ExactMetadataTransactionStoreV1.MAX_KEY_BYTES) {
            throw new IllegalArgumentException("Pulsar trim snapshot key exceeds its metadata bound");
        }
        return key;
    }

    private CompletionStage<Boolean> persistSnapshot(RetentionFloorSnapshotV1 snapshot) {
        String key = floorSnapshotKey(snapshot.snapshotRootSha256());
        CanonicalBytes bytes = M5RetentionCodecV1.encodeSnapshot(snapshot);
        // Only absent->exact creation is allowed. An uncertain original create is settled by actual readback.
        return metadata.compareAndSet(Optional.empty(), key, bytes)
                .handle((outcome, failure) -> null)
                .thenCompose(unused -> metadata.read(key).handle((observed, failure) -> {
                    if (failure != null || observed.isEmpty()) {
                        return false;
                    }
                    if (!observed.orElseThrow().canonicalStoredBytes().equals(bytes)) {
                        throw new IllegalStateException("Pulsar trim snapshot key contains different canonical bytes");
                    }
                    return true;
                }));
    }

    /** Cold continuation uses only the current fixed fence and its durable canonical SF, never actor memory. */
    public CompletionStage<Result> commitStoredSourceScan(VersionedValue exactFence, Sha256Digest attemptId) {
        var current = sourceAuthority(exactFence);
        var fence = current.logicalTrim()
                .flatMap(M5LogicalTrimRecordsV2.LogicalTrimStateV2::fence)
                .orElseThrow(() -> new IllegalArgumentException("Pulsar fixed trim fence is absent"));
        if (!fence.attemptIdSha256().equals(attemptId)) {
            throw new IllegalArgumentException("Pulsar trim fence belongs to another attempt");
        }
        return metadata.read(authorityKey).thenCompose(observed -> {
            if (observed.filter(exactFence::equals).isEmpty()) {
                return CompletableFuture.completedFuture(new Result(Outcome.CONFLICT, Optional.empty()));
            }
            return metadata.read(floorSnapshotKey(fence.floorSnapshotRootSha256()))
                    .thenCompose(value -> {
                        var stored =
                                value.orElseThrow(() -> new IllegalStateException("Pulsar trim snapshot is absent"));
                        var snapshot = M5RetentionCodecV1.decodeSnapshot(stored.canonicalStoredBytes());
                        if (!snapshot.snapshotRootSha256().equals(fence.floorSnapshotRootSha256())) {
                            throw new IllegalStateException("Pulsar trim snapshot differs from its fixed fence");
                        }
                        return super.commit(exactFence, snapshot, attemptId);
                    });
        });
    }

    /** Exact abort prevents a delayed scan result from sealing or committing. */
    public java.util.concurrent.CompletionStage<Result> abortSourceScan(
            ExactMetadataTransactionStoreV1.VersionedValue exactScan,
            com.nereusstream.domain.bytes.Sha256Digest attemptId) {
        var current = sourceAuthority(exactScan);
        requireSourceAttempt(current, attemptId);
        return mutate(
                exactScan,
                M5PulsarAggregateAuthorityCodecV1.encodeAuthority(M5PulsarAggregateAuthorityCodecV1.sourceScanSuccessor(
                        current,
                        java.util.Optional.empty(),
                        current.logicalTrim().orElseThrow())));
    }

    private M5PulsarAggregateAuthorityRecordsV1.PulsarAggregateRetirementAuthorityV1 sourceAuthority(
            ExactMetadataTransactionStoreV1.VersionedValue value) {
        if (!authorityKey.equals(value.key())) {
            throw new IllegalArgumentException("source scan belongs to another Pulsar authority key");
        }
        return M5PulsarAggregateAuthorityCodecV1.decodeAuthority(value.canonicalStoredBytes());
    }

    private static void requireSourceAttempt(
            M5PulsarAggregateAuthorityRecordsV1.PulsarAggregateRetirementAuthorityV1 current,
            com.nereusstream.domain.bytes.Sha256Digest attempt) {
        if (!current.logicalTrimSourceScan().orElseThrow().attemptIdSha256().equals(attempt)) {
            throw new IllegalArgumentException("Pulsar source scan belongs to another attempt");
        }
    }

    private static M5RetentionRecordsV1.AuthorityFactV1 sourceFact(
            ExactMetadataTransactionStoreV1.VersionedValue value) {
        return new M5RetentionRecordsV1.AuthorityFactV1(
                value.key(), value.metadataVersion(), value.canonicalStoredSha256());
    }
}
