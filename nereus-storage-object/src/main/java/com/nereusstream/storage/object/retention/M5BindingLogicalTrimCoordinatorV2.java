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
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Exact trim protocol on the existing Binding selector authority. */
public final class M5BindingLogicalTrimCoordinatorV2 extends M5SingleAuthorityLogicalTrimCoordinatorV2 {
    private final ExactMetadataTransactionStoreV1 metadata;
    private final String authorityKey;

    public M5BindingLogicalTrimCoordinatorV2(ExactMetadataTransactionStoreV1 metadata, String authorityKey) {
        super(metadata, authorityKey, false);
        this.metadata = metadata;
        this.authorityKey = authorityKey;
    }

    /** Configuration/audit changes compete with source capture at this same authority CAS. */
    public CompletionStage<Result> updateKafkaPolicy(VersionedValue exactOpen, M5KafkaBindingPolicyV1 policy) {
        var current = policyAuthority(exactOpen);
        if (current.kafkaPolicy().isEmpty()
                || current.state() != M5BindingAuthorityRecordsV1.BindingAuthorityStateV1.OPEN_V1
                || !current.referenceMutationTickets().isEmpty()
                || !current.trimMutationTickets().isEmpty()) {
            return CompletableFuture.completedFuture(new Result(Outcome.RETAIN, Optional.empty()));
        }
        return mutate(
                exactOpen,
                M5BindingAuthorityCodecV1.encodeAuthority(
                        M5BindingAuthorityCodecV1.kafkaPolicySuccessor(current, policy)));
    }

    /** Freeze the actual enrolled policy/writer domain before collecting its floor/reference vector. */
    public CompletionStage<Result> beginSourceScan(VersionedValue exactOpen, Sha256Digest attemptId) {
        var current = policyAuthority(exactOpen);
        if (current.kafkaPolicy().isEmpty()
                || current.writerEnrollment().isEmpty()
                || current.state() != M5BindingAuthorityRecordsV1.BindingAuthorityStateV1.OPEN_V1
                || !current.referenceMutationTickets().isEmpty()
                || !current.trimMutationTickets().isEmpty()) {
            return CompletableFuture.completedFuture(new Result(Outcome.RETAIN, Optional.empty()));
        }
        return mutate(
                exactOpen,
                M5BindingAuthorityCodecV1.encodeAuthority(
                        M5BindingAuthorityCodecV1.sourceScanSuccessor(current, attemptId)));
    }

    public CompletionStage<Void> requireSourceScanCurrent(VersionedValue exactScan) {
        policyAuthority(exactScan).sourceScan().orElseThrow();
        return metadata.read(authorityKey).thenAccept(actual -> {
            if (actual.filter(exactScan::equals).isEmpty()) {
                throw new M5ReferenceFreshnessVerifierV1.StaleAuthorityException("Kafka source-scan authority changed");
            }
        });
    }

    public CompletionStage<Result> abortSourceScan(VersionedValue exactScan, Sha256Digest attemptId) {
        var current = policyAuthority(exactScan);
        if (!current.sourceScan().orElseThrow().attemptId().equals(attemptId)) {
            throw new IllegalArgumentException("Kafka source scan belongs to another attempt");
        }
        return mutate(
                exactScan,
                M5BindingAuthorityCodecV1.encodeAuthority(M5BindingAuthorityCodecV1.abortSourceScan(current)));
    }

    /**
     * Cancels an unconfirmed original scan while its exact OPEN predecessor is still current.
     * Advancing the authority revision prevents a delayed original fence CAS from landing after close.
     * An unchanged reread alone cannot establish that the original request will never apply.
     */
    public CompletionStage<Result> cancelUnconfirmedSourceScan(VersionedValue originalOpen) {
        var current = policyAuthority(originalOpen);
        if (current.state() != M5BindingAuthorityRecordsV1.BindingAuthorityStateV1.OPEN_V1
                || current.kafkaPolicy().isEmpty()
                || current.writerEnrollment().isEmpty()
                || !current.referenceMutationTickets().isEmpty()
                || !current.trimMutationTickets().isEmpty()) {
            return CompletableFuture.completedFuture(new Result(Outcome.RETAIN, Optional.empty()));
        }
        var successor = M5BindingAuthorityCodecV1.successor(
                current,
                current.state(),
                current.selectorProjection(),
                current.batchSlots(),
                Optional.empty(),
                List.of());
        return mutate(originalOpen, M5BindingAuthorityCodecV1.encodeAuthority(successor));
    }

    private M5BindingAuthorityRecordsV1.BindingRetirementAuthorityV1 policyAuthority(VersionedValue exact) {
        if (!exact.key().equals(authorityKey)) {
            throw new IllegalArgumentException("Kafka policy predecessor belongs to another authority key");
        }
        return M5BindingAuthorityCodecV1.decodeAuthority(exact.canonicalStoredBytes());
    }
}
