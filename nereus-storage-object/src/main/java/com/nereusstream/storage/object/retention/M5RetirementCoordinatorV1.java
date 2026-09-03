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
import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorStateV1;
import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorValueV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.ExactCondition;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.ExactPut;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.ExactTransaction;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.TransactionOutcome;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtection;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceRetirementBatch;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.BatchMetadataStateV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.FullSourceRetirementBatchV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.M4ReleaseBindingV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.PhysicalCleanupSummaryV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceFreeProofV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceTargetKindV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetiredSourceRetirementBatchTombstoneV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetiredTopicIncarnationTombstoneV1;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * M5-C retirement transitions over a real all-or-nothing metadata transaction capability.
 *
 * <p>When that capability is absent, every method returns {@code UNSUPPORTED} before mutation. No
 * sequential-CAS fallback exists.
 */
public final class M5RetirementCoordinatorV1 {
    public enum Outcome {
        APPLIED_EXACT,
        EXISTING_EXACT,
        ALREADY_RETIRED,
        DEFINITIVELY_NOT_APPLIED,
        RESPONSE_UNKNOWN,
        CONFLICT,
        QUARANTINED,
        UNSUPPORTED
    }

    public record ExternalizationRequest(
            String partitionKey,
            String selectorKey,
            String batchKey,
            VersionedValue exactSelector,
            CanonicalBytes selectorSuccessorBytes,
            Optional<VersionedValue> exactExistingBatch,
            FullSourceRetirementBatchV1 fullBatch,
            ReferenceFreeProofV1 proof) {
        public ExternalizationRequest {
            requireText(partitionKey, "partitionKey");
            requireText(selectorKey, "selectorKey");
            requireText(batchKey, "batchKey");
            Objects.requireNonNull(exactSelector, "exactSelector");
            Objects.requireNonNull(selectorSuccessorBytes, "selectorSuccessorBytes");
            exactExistingBatch = Objects.requireNonNull(exactExistingBatch, "exactExistingBatch");
            Objects.requireNonNull(fullBatch, "fullBatch");
            Objects.requireNonNull(proof, "proof");
            if (!selectorKey.equals(exactSelector.key())) {
                throw new IllegalArgumentException("selector request key differs from its exact predecessor");
            }
            if (exactExistingBatch.isPresent()
                    && !batchKey.equals(exactExistingBatch.orElseThrow().key())) {
                throw new IllegalArgumentException("batch request key differs from its exact predecessor");
            }
        }
    }

    public record BatchRetirementRequest(
            String partitionKey,
            String batchKey,
            VersionedValue exactFullValue,
            FullSourceRetirementBatchV1 fullBatch,
            ReferenceFreeProofV1 proof) {
        public BatchRetirementRequest {
            requireText(partitionKey, "partitionKey");
            requireText(batchKey, "batchKey");
            Objects.requireNonNull(exactFullValue, "exactFullValue");
            Objects.requireNonNull(fullBatch, "fullBatch");
            Objects.requireNonNull(proof, "proof");
            if (!batchKey.equals(exactFullValue.key())) {
                throw new IllegalArgumentException("FULL predecessor key differs from the batch key");
            }
        }
    }

    public record PulsarRetirementRequest(
            String partitionKey,
            String selectorKey,
            VersionedValue exactSelector,
            PulsarTopicGenerationSelectorValueV1 selectorValue,
            String aggregateKey,
            VersionedValue exactAggregate,
            ReferenceFreeProofV1 proof,
            PhysicalCleanupSummaryV1 cleanup,
            RetiredTopicIncarnationTombstoneV1 tombstone) {
        public PulsarRetirementRequest {
            requireText(partitionKey, "partitionKey");
            requireText(selectorKey, "selectorKey");
            requireText(aggregateKey, "aggregateKey");
            Objects.requireNonNull(exactSelector, "exactSelector");
            Objects.requireNonNull(selectorValue, "selectorValue");
            Objects.requireNonNull(exactAggregate, "exactAggregate");
            Objects.requireNonNull(proof, "proof");
            Objects.requireNonNull(cleanup, "cleanup");
            Objects.requireNonNull(tombstone, "tombstone");
            if (!selectorKey.equals(exactSelector.key()) || !aggregateKey.equals(exactAggregate.key())) {
                throw new IllegalArgumentException("Pulsar retirement exact predecessor key differs");
            }
        }
    }

    private final ExactMetadataTransactionStoreV1 metadata;
    private final M5ReferenceFreshnessVerifierV1 freshness;

    public M5RetirementCoordinatorV1(ExactMetadataTransactionStoreV1 metadata) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.freshness = new M5ReferenceFreshnessVerifierV1(metadata);
    }

    public CompletionStage<Outcome> externalize(ExternalizationRequest request) {
        Objects.requireNonNull(request, "request");
        ValidatedExternalization validated = validateExternalization(request);
        if (!metadata.supportsAtomicMultiKeyTransactions()) {
            return CompletableFuture.completedFuture(Outcome.UNSUPPORTED);
        }
        return freshness.requireFresh(request.proof()).thenCompose(freshValues -> {
            List<ExactCondition> conditions =
                    conditions(freshValues, request.exactSelector(), request.batchKey(), request.exactExistingBatch());
            List<ExactPut> puts = new ArrayList<>();
            puts.add(ExactPut.of(request.selectorKey(), request.selectorSuccessorBytes()));
            if (request.exactExistingBatch().isEmpty()) {
                puts.add(ExactPut.of(request.batchKey(), validated.fullBytes()));
            }
            puts.sort(Comparator.comparing(ExactPut::key));
            ExactTransaction transaction = new ExactTransaction(request.partitionKey(), conditions, puts);
            return metadata.conditionalTransaction(transaction)
                    .thenCompose(result -> reconcileExternalization(request, validated, result));
        });
    }

    public CompletionStage<Outcome> retireBatch(BatchRetirementRequest request) {
        Objects.requireNonNull(request, "request");
        CanonicalBytes fullBytes = M5RetentionCodecV1.encodeFullBatch(request.fullBatch());
        if (!request.exactFullValue().canonicalStoredBytes().equals(fullBytes)
                || request.proof().targetKind() != ReferenceTargetKindV1.RETIREMENT_BATCH
                || !request.proof()
                        .targetIdentitySha256()
                        .equals(request.fullBatch().batchIdSha256())
                || !request.proof()
                        .identity()
                        .binding()
                        .equals(request.fullBatch().binding())
                || !request.proof()
                        .identity()
                        .capability()
                        .equals(request.fullBatch().capability())) {
            throw new IllegalArgumentException("batch retirement predecessor or reference-free proof differs");
        }
        RetiredSourceRetirementBatchTombstoneV1 tombstone =
                M5RetentionCodecV1.finalizeRetiredBatch(new RetiredSourceRetirementBatchTombstoneV1(
                        BatchMetadataStateV1.RETIRED_V1,
                        request.fullBatch().binding(),
                        request.fullBatch().batchIdSha256(),
                        request.fullBatch().fullBatchSha256(),
                        request.proof().proofSha256(),
                        request.exactFullValue().metadataVersion(),
                        request.exactFullValue().canonicalStoredSha256(),
                        request.fullBatch().capability(),
                        placeholder()));
        CanonicalBytes tombstoneBytes = M5RetentionCodecV1.encodeRetiredBatch(tombstone);
        if (!metadata.supportsAtomicMultiKeyTransactions()) {
            return CompletableFuture.completedFuture(Outcome.UNSUPPORTED);
        }
        return freshness.requireFresh(request.proof()).thenCompose(freshValues -> {
            List<ExactCondition> conditions = conditions(
                    freshValues, request.exactFullValue(), request.batchKey(), Optional.of(request.exactFullValue()));
            ExactTransaction transaction = new ExactTransaction(
                    request.partitionKey(), conditions, List.of(ExactPut.of(request.batchKey(), tombstoneBytes)));
            return metadata.conditionalTransaction(transaction)
                    .thenCompose(result -> reconcileBatchRetirement(request, tombstoneBytes, result));
        });
    }

    public CompletionStage<Outcome> retirePulsarAggregate(PulsarRetirementRequest request) {
        Objects.requireNonNull(request, "request");
        validatePulsarRetirement(request);
        CanonicalBytes tombstoneBytes = M5RetentionCodecV1.encodeRetiredPulsar(request.tombstone());
        if (!metadata.supportsAtomicMultiKeyTransactions()) {
            return CompletableFuture.completedFuture(Outcome.UNSUPPORTED);
        }
        return freshness.requireFresh(request.proof()).thenCompose(freshValues -> {
            return requireFresh(request.cleanup().cleanupRoot()).thenCompose(cleanupValue -> {
                List<VersionedValue> allFresh = new ArrayList<>(freshValues);
                allFresh.add(cleanupValue);
                List<ExactCondition> conditions = conditions(
                        allFresh,
                        request.exactAggregate(),
                        request.aggregateKey(),
                        Optional.of(request.exactAggregate()));
                conditions = addCondition(conditions, request.exactSelector());
                ExactTransaction transaction = new ExactTransaction(
                        request.partitionKey(),
                        conditions,
                        List.of(ExactPut.of(request.aggregateKey(), tombstoneBytes)));
                return metadata.conditionalTransaction(transaction)
                        .thenCompose(result -> reconcilePulsarRetirement(request, tombstoneBytes, result));
            });
        });
    }

    private ValidatedExternalization validateExternalization(ExternalizationRequest request) {
        CanonicalBytes fullBytes = M5RetentionCodecV1.encodeFullBatch(request.fullBatch());
        if (!Sha256Digest.hash(request.exactSelector().canonicalStoredBytes())
                .equals(request.fullBatch().selectorPredecessorValueSha256())) {
            throw new IllegalArgumentException("FULL record selector predecessor SHA-256 differs");
        }
        BindingReadSelector predecessor =
                M4ReadControlCodecV1.decodeSelector(request.exactSelector().canonicalStoredBytes());
        BindingReadSelector successor = M4ReadControlCodecV1.decodeSelector(request.selectorSuccessorBytes());
        SourceRetirementBatch exactBatch =
                request.fullBatch().canonicalM4BatchBytes().isEmpty()
                        ? null
                        : M4ReadControlCodecV1.decodeBatch(request.fullBatch().canonicalM4BatchBytes());
        List<SourceRetirementBatch> survivors = predecessor.activeBatches().stream()
                .filter(batch ->
                        !batch.batchIdSha256().equals(request.fullBatch().batchIdSha256()))
                .toList();
        if (exactBatch == null
                || predecessor.activeBatches().size() - survivors.size() != 1
                || !predecessor.activeBatches().contains(exactBatch)
                || !withoutBatches(predecessor, survivors).equals(successor)) {
            throw new IllegalArgumentException("selector successor does not remove exactly one canonical inline batch");
        }
        if (request.proof().targetKind() != ReferenceTargetKindV1.RETIREMENT_BATCH
                || !request.proof().targetIdentitySha256().equals(exactBatch.batchIdSha256())
                || !request.proof()
                        .identity()
                        .binding()
                        .equals(request.fullBatch().binding())
                || !request.proof()
                        .identity()
                        .capability()
                        .equals(request.fullBatch().capability())
                || !request.proof().proofSha256().equals(request.fullBatch().referenceFreeProofSha256())
                || !request.proof().selectorRoot().key().equals(request.selectorKey())
                || !request.proof()
                        .selectorRoot()
                        .metadataVersion()
                        .equals(request.exactSelector().metadataVersion())
                || !request.proof()
                        .selectorRoot()
                        .valueSha256()
                        .equals(request.exactSelector().canonicalStoredSha256())) {
            throw new IllegalArgumentException("externalization proof target or selector predecessor differs");
        }
        validateReleases(exactBatch, request.proof().m4Releases());
        request.exactExistingBatch().ifPresent(existing -> {
            if (!existing.canonicalStoredBytes().equals(fullBytes)) {
                throw new IllegalArgumentException("existing FULL batch differs from the exact candidate");
            }
        });
        return new ValidatedExternalization(fullBytes);
    }

    private void validatePulsarRetirement(PulsarRetirementRequest request) {
        PulsarTopicGenerationSelectorValueV1 selector = request.selectorValue();
        RetiredTopicIncarnationTombstoneV1 tombstone = request.tombstone();
        PhysicalCleanupSummaryV1 cleanup = request.cleanup();
        if (!request.exactSelector().canonicalStoredBytes().equals(selector.canonicalStoredBytes())
                || !request.exactSelector().canonicalStoredSha256().equals(selector.canonicalStoredDigest())
                || selector.state() != PulsarTopicGenerationSelectorStateV1.DELETED
                || !selector.persistenceName().equals(tombstone.incarnation().persistenceName())
                || selector.generation().value() != tombstone.selectorGeneration()
                || !selector.aggregateBindingId().equals(tombstone.bindingId())
                || !selector.aggregateCanonicalStoredDigest()
                        .equals(request.exactAggregate().canonicalStoredSha256())
                || !request.exactSelector().metadataVersion().equals(tombstone.selectorVersion())
                || !request.exactAggregate().metadataVersion().equals(tombstone.aggregatePredecessorVersion())
                || !request.exactAggregate().canonicalStoredSha256().equals(tombstone.aggregatePredecessorValueSha256())
                || !request.exactAggregate().canonicalStoredSha256().equals(tombstone.originalAggregateSha256())
                || request.proof().targetKind() != ReferenceTargetKindV1.PULSAR_AGGREGATE
                || !request.proof().targetIdentitySha256().equals(tombstone.originalAggregateSha256())
                || !request.proof().proofSha256().equals(tombstone.referenceFreeProofSha256())
                || !request.proof().identity().binding().bindingId().equals(tombstone.bindingId())
                || !request.proof().identity().capability().equals(tombstone.capability())
                || !cleanup.incarnation().equals(tombstone.incarnation())
                || !cleanup.bindingId().equals(tombstone.bindingId())
                || !cleanup.originalAggregateSha256().equals(tombstone.originalAggregateSha256())
                || !cleanup.capability().equals(tombstone.capability())) {
            throw new IllegalArgumentException("Pulsar aggregate retirement selector, predecessor, or proof differs");
        }
        M5RetentionCodecV1.encodeRetiredPulsar(tombstone);
    }

    private CompletionStage<Outcome> reconcileExternalization(
            ExternalizationRequest request, ValidatedExternalization validated, TransactionOutcome result) {
        if (result == TransactionOutcome.UNSUPPORTED) {
            return CompletableFuture.completedFuture(Outcome.UNSUPPORTED);
        }
        return metadata.read(request.selectorKey())
                .thenCombine(metadata.read(request.batchKey()), (selector, batch) -> {
                    if (selector.isEmpty()) {
                        return Outcome.QUARANTINED;
                    }
                    boolean inline = hasBatch(
                            selector.orElseThrow().canonicalStoredBytes(),
                            request.fullBatch().batchIdSha256());
                    if (inline && batch.isEmpty()) {
                        return result == TransactionOutcome.RESPONSE_UNKNOWN
                                ? Outcome.RESPONSE_UNKNOWN
                                : Outcome.DEFINITIVELY_NOT_APPLIED;
                    }
                    if (inline || batch.isEmpty()) {
                        return Outcome.QUARANTINED;
                    }
                    if (!selector.orElseThrow().canonicalStoredBytes().equals(request.selectorSuccessorBytes())) {
                        return Outcome.CONFLICT;
                    }
                    CanonicalBytes batchBytes = batch.orElseThrow().canonicalStoredBytes();
                    if (batchBytes.equals(validated.fullBytes())) {
                        return result == TransactionOutcome.APPLIED_EXACT
                                ? Outcome.APPLIED_EXACT
                                : Outcome.EXISTING_EXACT;
                    }
                    try {
                        RetiredSourceRetirementBatchTombstoneV1 retired =
                                M5RetentionCodecV1.decodeRetiredBatch(batchBytes);
                        return matches(retired, request.fullBatch()) ? Outcome.ALREADY_RETIRED : Outcome.CONFLICT;
                    } catch (IllegalArgumentException ignored) {
                        return Outcome.CONFLICT;
                    }
                });
    }

    private CompletionStage<Outcome> reconcileBatchRetirement(
            BatchRetirementRequest request, CanonicalBytes tombstoneBytes, TransactionOutcome result) {
        if (result == TransactionOutcome.UNSUPPORTED) {
            return CompletableFuture.completedFuture(Outcome.UNSUPPORTED);
        }
        return metadata.read(request.batchKey()).thenApply(observed -> {
            if (observed.isEmpty()) {
                return Outcome.QUARANTINED;
            }
            CanonicalBytes bytes = observed.orElseThrow().canonicalStoredBytes();
            if (bytes.equals(tombstoneBytes)) {
                return result == TransactionOutcome.APPLIED_EXACT ? Outcome.APPLIED_EXACT : Outcome.EXISTING_EXACT;
            }
            if (bytes.equals(request.exactFullValue().canonicalStoredBytes())) {
                return result == TransactionOutcome.RESPONSE_UNKNOWN
                        ? Outcome.RESPONSE_UNKNOWN
                        : Outcome.DEFINITIVELY_NOT_APPLIED;
            }
            try {
                RetiredSourceRetirementBatchTombstoneV1 retired = M5RetentionCodecV1.decodeRetiredBatch(bytes);
                return matches(retired, request.fullBatch())
                                && retired.fullPredecessorVersion()
                                        .equals(request.exactFullValue().metadataVersion())
                        ? Outcome.ALREADY_RETIRED
                        : Outcome.CONFLICT;
            } catch (IllegalArgumentException ignored) {
                return Outcome.QUARANTINED;
            }
        });
    }

    private CompletionStage<Outcome> reconcilePulsarRetirement(
            PulsarRetirementRequest request, CanonicalBytes tombstoneBytes, TransactionOutcome result) {
        if (result == TransactionOutcome.UNSUPPORTED) {
            return CompletableFuture.completedFuture(Outcome.UNSUPPORTED);
        }
        return metadata.read(request.aggregateKey()).thenApply(observed -> {
            if (observed.isEmpty()) {
                return Outcome.QUARANTINED;
            }
            CanonicalBytes bytes = observed.orElseThrow().canonicalStoredBytes();
            if (bytes.equals(tombstoneBytes)) {
                return result == TransactionOutcome.APPLIED_EXACT ? Outcome.APPLIED_EXACT : Outcome.EXISTING_EXACT;
            }
            if (bytes.equals(request.exactAggregate().canonicalStoredBytes())) {
                return result == TransactionOutcome.RESPONSE_UNKNOWN
                        ? Outcome.RESPONSE_UNKNOWN
                        : Outcome.DEFINITIVELY_NOT_APPLIED;
            }
            try {
                RetiredTopicIncarnationTombstoneV1 retired = M5RetentionCodecV1.decodeRetiredPulsar(bytes);
                return samePulsarRetirement(retired, request.tombstone()) ? Outcome.ALREADY_RETIRED : Outcome.CONFLICT;
            } catch (IllegalArgumentException ignored) {
                return Outcome.QUARANTINED;
            }
        });
    }

    private CompletionStage<VersionedValue> requireFresh(AuthorityFactV1 fact) {
        return metadata.read(fact.key()).thenApply(observed -> observed.filter(
                        value -> value.metadataVersion().equals(fact.metadataVersion())
                                && value.canonicalStoredSha256().equals(fact.valueSha256()))
                .orElseThrow(() -> new M5ReferenceFreshnessVerifierV1.StaleAuthorityException(
                        "physical cleanup root changed: " + fact.key())));
    }

    private static List<ExactCondition> conditions(
            List<VersionedValue> freshValues,
            VersionedValue primary,
            String mutationKey,
            Optional<VersionedValue> mutationPredecessor) {
        Map<String, ExactCondition> byKey = new LinkedHashMap<>();
        freshValues.forEach(value -> merge(byKey, ExactCondition.present(value)));
        merge(byKey, ExactCondition.present(primary));
        merge(
                byKey,
                mutationPredecessor.map(ExactCondition::present).orElseGet(() -> ExactCondition.absent(mutationKey)));
        return byKey.values().stream()
                .sorted(Comparator.comparing(ExactCondition::key))
                .toList();
    }

    private static List<ExactCondition> addCondition(List<ExactCondition> conditions, VersionedValue value) {
        Map<String, ExactCondition> byKey = new LinkedHashMap<>();
        conditions.forEach(condition -> merge(byKey, condition));
        merge(byKey, ExactCondition.present(value));
        return byKey.values().stream()
                .sorted(Comparator.comparing(ExactCondition::key))
                .toList();
    }

    private static void merge(Map<String, ExactCondition> target, ExactCondition condition) {
        ExactCondition existing = target.putIfAbsent(condition.key(), condition);
        if (existing != null && !existing.equals(condition)) {
            throw new IllegalArgumentException("transaction has conflicting exact conditions for " + condition.key());
        }
    }

    private static void validateReleases(SourceRetirementBatch batch, List<M4ReleaseBindingV1> releases) {
        if (releases.size() != batch.sources().size()) {
            throw new IllegalArgumentException("M4 RELEASED binding count differs from the full batch");
        }
        for (SourceProtectionIdentity source : batch.sources()) {
            M4ReleaseBindingV1 release = releases.stream()
                    .filter(value -> value.sourceIdentitySha256().equals(source.sourceIdentitySha256())
                            && value.protectionGeneration() == source.protectionGeneration())
                    .findFirst()
                    .orElseThrow(
                            () -> new IllegalArgumentException("full batch member lacks exact M4 RELEASED binding"));
            if (!release.releasedByBatchSha256().equals(batch.batchIdSha256())) {
                throw new IllegalArgumentException("M4 RELEASED binding names a different batch");
            }
            SourceProtection exact = M4ReadControlCodecV1.decodeProtection(release.canonicalProtectionBytes());
            if (!exact.binding().equals(batch.binding()) || !exact.identity().equals(source)) {
                throw new IllegalArgumentException("M4 RELEASED value names a different binding or source");
            }
        }
    }

    private static BindingReadSelector withoutBatches(
            BindingReadSelector selector, List<SourceRetirementBatch> batches) {
        return new BindingReadSelector(
                selector.binding(),
                selector.selectedViewSha256(),
                selector.ownerEpoch(),
                selector.readAdmissionEpoch(),
                selector.sourceGeneration(),
                selector.mode(),
                selector.admissionState(),
                selector.fallbackSetSha256(),
                selector.capability(),
                selector.pendingAnchors(),
                batches);
    }

    private static boolean hasBatch(CanonicalBytes selectorBytes, Sha256Digest batchIdSha256) {
        try {
            return M4ReadControlCodecV1.decodeSelector(selectorBytes).activeBatches().stream()
                    .anyMatch(batch -> batch.batchIdSha256().equals(batchIdSha256));
        } catch (IllegalArgumentException failure) {
            return false;
        }
    }

    private static boolean matches(
            RetiredSourceRetirementBatchTombstoneV1 tombstone, FullSourceRetirementBatchV1 full) {
        return tombstone.binding().equals(full.binding())
                && tombstone.batchIdSha256().equals(full.batchIdSha256())
                && tombstone.fullBatchSha256().equals(full.fullBatchSha256())
                && tombstone
                        .fullPredecessorValueSha256()
                        .equals(Sha256Digest.hash(M5RetentionCodecV1.encodeFullBatch(full)))
                && tombstone.capability().equals(full.capability());
    }

    private static boolean samePulsarRetirement(
            RetiredTopicIncarnationTombstoneV1 observed, RetiredTopicIncarnationTombstoneV1 candidate) {
        return observed.incarnation().equals(candidate.incarnation())
                && observed.bindingId().equals(candidate.bindingId())
                && observed.originalAggregateSha256().equals(candidate.originalAggregateSha256())
                && observed.selectorGeneration() == candidate.selectorGeneration()
                && observed.selectorState() == candidate.selectorState()
                && observed.selectorVersion().equals(candidate.selectorVersion())
                && observed.aggregatePredecessorVersion().equals(candidate.aggregatePredecessorVersion())
                && observed.aggregatePredecessorValueSha256().equals(candidate.aggregatePredecessorValueSha256())
                && observed.capability().equals(candidate.capability());
    }

    private static Sha256Digest placeholder() {
        return Sha256Digest.hash(CanonicalBytes.copyOf(new byte[] {1}));
    }

    private static void requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " is blank");
        }
    }

    private record ValidatedExternalization(CanonicalBytes fullBytes) {}
}
