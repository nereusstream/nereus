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
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.BindingTrimFrontierV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetentionFloorSnapshotV1;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/** Fenced monotonic logical-trim CAS. This coordinator has no physical-delete API. */
public final class M5LogicalTrimCoordinatorV1 {
    public enum Outcome {
        APPLIED_EXACT,
        EXISTING_EXACT,
        DEFINITIVELY_NOT_APPLIED,
        RESPONSE_UNKNOWN,
        CONFLICT
    }

    public record Result(Outcome outcome, Optional<BindingTrimFrontierV1> exactFrontier) {
        public Result {
            Objects.requireNonNull(outcome, "outcome");
            exactFrontier = Objects.requireNonNull(exactFrontier, "exactFrontier");
            if ((outcome == Outcome.APPLIED_EXACT || outcome == Outcome.EXISTING_EXACT) != exactFrontier.isPresent()) {
                throw new IllegalArgumentException("only exact trim outcomes carry a frontier");
            }
        }
    }

    private final ExactMetadataTransactionStoreV1 metadata;
    private final M5ReferenceFreshnessVerifierV1 freshness;
    private final M5RetentionPlannerV1 planner = new M5RetentionPlannerV1();

    public M5LogicalTrimCoordinatorV1(ExactMetadataTransactionStoreV1 metadata) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.freshness = new M5ReferenceFreshnessVerifierV1(metadata);
    }

    public CompletionStage<Result> advance(String frontierKey, RetentionFloorSnapshotV1 snapshot) {
        Objects.requireNonNull(frontierKey, "frontierKey");
        Objects.requireNonNull(snapshot, "snapshot");
        return freshness.requireFresh(snapshot).thenCompose(ignored -> metadata.read(frontierKey)
                .thenCompose(current -> {
                    Optional<BindingTrimFrontierV1> predecessor = current.map(value -> decode(frontierKey, value));
                    if (predecessor.isPresent()
                            && predecessor.orElseThrow().newFrontier() == snapshot.minimumSafeFloor()) {
                        return java.util.concurrent.CompletableFuture.completedFuture(
                                new Result(Outcome.EXISTING_EXACT, predecessor));
                    }
                    BindingTrimFrontierV1 candidate = planner.plan(snapshot, predecessor);
                    CanonicalBytes candidateBytes = M5RetentionCodecV1.encodeTrimFrontier(candidate);
                    return metadata.compareAndSet(current, frontierKey, candidateBytes)
                            .thenCompose(
                                    mutation -> reconcile(frontierKey, current, candidate, candidateBytes, mutation));
                }));
    }

    private CompletionStage<Result> reconcile(
            String key,
            Optional<VersionedValue> predecessor,
            BindingTrimFrontierV1 candidate,
            CanonicalBytes candidateBytes,
            MutationOutcome mutation) {
        return metadata.read(key).thenApply(observed -> {
            if (observed.isPresent()
                    && observed.orElseThrow().canonicalStoredBytes().equals(candidateBytes)) {
                return new Result(
                        mutation == MutationOutcome.APPLIED_EXACT ? Outcome.APPLIED_EXACT : Outcome.EXISTING_EXACT,
                        Optional.of(candidate));
            }
            if (observed.equals(predecessor)) {
                return new Result(
                        mutation == MutationOutcome.RESPONSE_UNKNOWN
                                ? Outcome.RESPONSE_UNKNOWN
                                : Outcome.DEFINITIVELY_NOT_APPLIED,
                        Optional.empty());
            }
            return new Result(
                    mutation == MutationOutcome.RESPONSE_UNKNOWN ? Outcome.RESPONSE_UNKNOWN : Outcome.CONFLICT,
                    Optional.empty());
        });
    }

    private static BindingTrimFrontierV1 decode(String key, VersionedValue stored) {
        if (!key.equals(stored.key())) {
            throw new IllegalArgumentException("trim reread returned a different authority key");
        }
        return M5RetentionCodecV1.decodeTrimFrontier(stored.canonicalStoredBytes());
    }
}
