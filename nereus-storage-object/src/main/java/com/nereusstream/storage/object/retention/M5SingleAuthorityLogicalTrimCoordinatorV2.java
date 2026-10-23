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

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.Nta1CodecV1;
import com.nereusstream.domain.codec.TopicIncarnationIdentityCodecV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.retention.M5LogicalTrimRecordsV2.LogicalTrimFenceV2;
import com.nereusstream.storage.object.retention.M5LogicalTrimRecordsV2.LogicalTrimStateV2;
import com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityRecordsV1.PulsarAggregateAuthorityStateV1;
import com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityRecordsV1.PulsarAggregateRetirementAuthorityV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.BindingTrimFrontierV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetentionFloorSnapshotV1;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Shared exact-CAS trim protocol for the two existing authority record families. */
public sealed class M5SingleAuthorityLogicalTrimCoordinatorV2
        permits M5PulsarLogicalTrimCoordinatorV2, M5BindingLogicalTrimCoordinatorV2 {
    public enum Outcome {
        APPLIED_EXACT,
        EXISTING_EXACT,
        DEFINITIVELY_NOT_APPLIED,
        RETAIN,
        CONFLICT,
        RESPONSE_UNKNOWN
    }

    public record Result(Outcome outcome, Optional<VersionedValue> exactAuthority) {
        public Result {
            Objects.requireNonNull(outcome, "outcome");
            exactAuthority = Objects.requireNonNull(exactAuthority, "exactAuthority");
            if ((outcome == Outcome.APPLIED_EXACT || outcome == Outcome.EXISTING_EXACT) != exactAuthority.isPresent()) {
                throw new IllegalArgumentException("only a confirmed trim transition carries authority");
            }
        }
    }

    private final ExactMetadataTransactionStoreV1 metadata;
    private final String key;
    private final boolean pulsar;

    M5SingleAuthorityLogicalTrimCoordinatorV2(
            ExactMetadataTransactionStoreV1 metadata, String authorityKey, boolean pulsar) {
        this.pulsar = pulsar;
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        key = Objects.requireNonNull(authorityKey, "authorityKey");
        if (key.isBlank()) {
            throw new IllegalArgumentException("logical trim authority key is empty");
        }
    }

    /** Must use the exact original OPEN value; every mutation remains at this same authority key. */
    public CompletionStage<Result> fence(
            VersionedValue exactOpen, RetentionFloorSnapshotV1 snapshot, Sha256Digest attemptId) {
        var current = authority(exactOpen);
        requireSnapshot(current, snapshot);
        M5RetentionRecordsV1.requireDigest(attemptId, "attemptId");
        if (!current.open() || !current.writersQuiescent()) {
            return CompletableFuture.completedFuture(result(Outcome.RETAIN));
        }
        var prior = current.logicalTrim().flatMap(LogicalTrimStateV2::frontier);
        new M5RetentionPlannerV1().plan(snapshot, prior);
        var fence = new LogicalTrimFenceV2(attemptId, snapshot.snapshotRootSha256(), fact(exactOpen));
        var candidate = successor(current, new LogicalTrimStateV2(prior, Optional.of(fence)));
        return new M5ReferenceFreshnessVerifierV1(metadata)
                .requireFresh(snapshot)
                .thenCompose(ignored -> mutate(exactOpen, candidate));
    }

    /** The successful CAS atomically stores the frontier and reopens writer admission. */
    public CompletionStage<Result> commit(
            VersionedValue exactFence, RetentionFloorSnapshotV1 snapshot, Sha256Digest attemptId) {
        var current = authority(exactFence);
        requireSnapshot(current, snapshot);
        var trim = current.logicalTrim().orElseThrow(() -> new IllegalArgumentException("trim state is absent"));
        var fence = requireFence(current, trim, attemptId);
        if (!fence.floorSnapshotRootSha256().equals(snapshot.snapshotRootSha256())) {
            throw new IllegalArgumentException("trim fence binds another floor snapshot");
        }
        var frontier = new M5RetentionPlannerV1().plan(snapshot, trim.frontier());
        var candidate = successor(current, new LogicalTrimStateV2(Optional.of(frontier), Optional.empty()));
        return requireFrozenFacts(snapshot, fence.openAuthority())
                .thenCompose(ignored -> mutate(exactFence, candidate))
                .thenCompose(
                        result -> result.outcome() == Outcome.RESPONSE_UNKNOWN || result.outcome() == Outcome.CONFLICT
                                ? confirmFrontier(frontier, result)
                                : CompletableFuture.completedFuture(result));
    }

    /** Exact abort fences any delayed original commit CAS; no separate frontier was exposed. */
    public CompletionStage<Result> abort(VersionedValue exactFence, Sha256Digest attemptId) {
        var current = authority(exactFence);
        var trim = current.logicalTrim().orElseThrow(() -> new IllegalArgumentException("trim state is absent"));
        requireFence(current, trim, attemptId);
        return mutate(exactFence, successor(current, new LogicalTrimStateV2(trim.frontier(), Optional.empty())));
    }

    public CompletionStage<Optional<BindingTrimFrontierV1>> readFrontier() {
        return metadata.read(key)
                .thenApply(
                        value -> value.flatMap(v -> authority(v).logicalTrim().flatMap(LogicalTrimStateV2::frontier)));
    }

    private CompletionStage<Void> requireFrozenFacts(RetentionFloorSnapshotV1 snapshot, AuthorityFactV1 open) {
        var facts = new ArrayList<AuthorityFactV1>();
        snapshot.rows().forEach(row -> facts.add(row.authority()));
        facts.add(snapshot.ownerFence());
        facts.add(snapshot.storageFence());
        var distinct = new LinkedHashMap<String, AuthorityFactV1>();
        for (var fact : facts) {
            var old = distinct.putIfAbsent(fact.key(), fact);
            if (old != null && !old.equals(fact)) {
                throw new IllegalArgumentException("floor vector has conflicting authority facts");
            }
        }
        CompletionStage<Void> stage = CompletableFuture.completedFuture(null);
        for (var fact : distinct.values()) {
            if (fact.key().equals(key)) {
                if (!fact.equals(open)) {
                    throw new IllegalArgumentException("floor vector does not bind the fence's exact OPEN predecessor");
                }
                // The fence's own exact CAS substituted this one version; final commit CAS checks that fence.
                continue;
            }
            stage = stage.thenCompose(ignored -> metadata.read(fact.key()).thenApply(value -> {
                if (value.filter(v -> fact(v).equals(fact)).isEmpty()) {
                    throw new M5ReferenceFreshnessVerifierV1.StaleAuthorityException(
                            "frozen logical trim floor version/value changed: " + fact.key());
                }
                return null;
            }));
        }
        return stage;
    }

    protected final CompletionStage<Result> mutate(
            VersionedValue predecessor, com.nereusstream.domain.bytes.CanonicalBytes bytes) {
        return metadata.compareAndSet(Optional.of(predecessor), key, bytes)
                .thenCompose(mutation -> metadata.read(key).handle((observed, failure) -> {
                    if (failure != null) {
                        return result(Outcome.RESPONSE_UNKNOWN);
                    }
                    if (observed.filter(v -> v.canonicalStoredBytes().equals(bytes))
                            .isPresent()) {
                        return new Result(
                                mutation == MutationOutcome.APPLIED_EXACT
                                        ? Outcome.APPLIED_EXACT
                                        : Outcome.EXISTING_EXACT,
                                observed);
                    }
                    if (observed.filter(predecessor::equals).isPresent()) {
                        return result(
                                mutation == MutationOutcome.DEFINITIVE_CONFLICT
                                                || mutation == MutationOutcome.PREDECESSOR_UNCHANGED
                                        ? Outcome.DEFINITIVELY_NOT_APPLIED
                                        : Outcome.RESPONSE_UNKNOWN);
                    }
                    return result(Outcome.CONFLICT);
                }));
    }

    private CompletionStage<Result> confirmFrontier(BindingTrimFrontierV1 frontier, Result unresolved) {
        return metadata.read(key).handle((value, failure) -> {
            if (failure == null
                    && value.flatMap(v -> authority(v).logicalTrim())
                            .flatMap(LogicalTrimStateV2::frontier)
                            .filter(frontier::equals)
                            .isPresent()) {
                return new Result(Outcome.EXISTING_EXACT, value);
            }
            return unresolved;
        });
    }

    private Authority authority(VersionedValue exact) {
        Objects.requireNonNull(exact, "exact authority");
        if (!exact.key().equals(key)) {
            throw new IllegalArgumentException("logical trim predecessor belongs to another authority key");
        }
        if (pulsar) {
            var value = M5PulsarAggregateAuthorityCodecV1.decodeAuthority(exact.canonicalStoredBytes());
            var aggregate = Nta1CodecV1.decode(value.canonicalAggregateBytes());
            var binding = new BindingIdentity(
                    value.bindingId(),
                    Sha256Digest.hash(TopicIncarnationIdentityCodecV1.encode(value.incarnation())),
                    aggregate.initialEpoch().storageEpochId().digest());
            return new Authority(
                    binding,
                    value.capability(),
                    value.state() == PulsarAggregateAuthorityStateV1.OPEN_V1,
                    value.state() == PulsarAggregateAuthorityStateV1.LOGICAL_TRIM_SCAN_FENCED_V2,
                    value.writerEnrollment().isPresent()
                            && value.referenceMutationTickets().isEmpty(),
                    value.logicalTrim(),
                    value);
        }
        var value = M5BindingAuthorityCodecV1.decodeAuthority(exact.canonicalStoredBytes());
        return new Authority(
                value.binding(),
                value.capability(),
                value.state() == M5BindingAuthorityRecordsV1.BindingAuthorityStateV1.OPEN_V1,
                value.state() == M5BindingAuthorityRecordsV1.BindingAuthorityStateV1.LOGICAL_TRIM_SCAN_FENCED_V2,
                value.writerEnrollment().isPresent()
                        && value.referenceMutationTickets().isEmpty()
                        && value.trimMutationTickets().isEmpty(),
                value.logicalTrim(),
                value);
    }

    private static com.nereusstream.domain.bytes.CanonicalBytes successor(Authority current, LogicalTrimStateV2 trim) {
        if (current.original() instanceof PulsarAggregateRetirementAuthorityV1 pulsar) {
            return M5PulsarAggregateAuthorityCodecV1.encodeAuthority(
                    M5PulsarAggregateAuthorityCodecV1.logicalTrimSuccessor(pulsar, trim));
        }
        var binding = (M5BindingAuthorityRecordsV1.BindingRetirementAuthorityV1) current.original();
        return M5BindingAuthorityCodecV1.encodeAuthority(
                M5BindingAuthorityCodecV1.logicalTrimSuccessor(binding, trim, java.util.List.of()));
    }

    private record Authority(
            BindingIdentity binding,
            com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding capability,
            boolean open,
            boolean trimFenced,
            boolean writersQuiescent,
            Optional<LogicalTrimStateV2> logicalTrim,
            Object original) {}

    private static LogicalTrimFenceV2 requireFence(Authority current, LogicalTrimStateV2 trim, Sha256Digest attemptId) {
        var fence = trim.fence().orElseThrow(() -> new IllegalArgumentException("logical trim fence is absent"));
        if (!current.trimFenced() || !fence.attemptIdSha256().equals(attemptId)) {
            throw new IllegalArgumentException("logical trim fence belongs to another attempt");
        }
        return fence;
    }

    private void requireSnapshot(Authority authority, RetentionFloorSnapshotV1 snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        M5RetentionCodecV1.encodeSnapshot(snapshot);
        if (pulsar && snapshot.pulsarContext().isEmpty()
                || !snapshot.identity().binding().equals(authority.binding())
                || !snapshot.identity().capability().equals(authority.capability())) {
            throw new IllegalArgumentException("floor snapshot differs from the exact authority family/Binding");
        }
    }

    private static AuthorityFactV1 fact(VersionedValue value) {
        return new AuthorityFactV1(value.key(), value.metadataVersion(), value.canonicalStoredSha256());
    }

    private static Result result(Outcome outcome) {
        return new Result(outcome, Optional.empty());
    }
}
