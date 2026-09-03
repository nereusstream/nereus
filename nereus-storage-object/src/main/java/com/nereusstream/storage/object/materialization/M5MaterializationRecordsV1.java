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

package com.nereusstream.storage.object.materialization;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.AdmissionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Closed immutable values for M5-A materialization and manifest publication. */
public final class M5MaterializationRecordsV1 {
    public static final int MAX_SOURCES = 256;
    public static final int MAX_PARTS = 256;
    public static final int MAX_INDEXES = 32;
    public static final int MAX_MEMBER_BINDINGS = 256;
    public static final long MAX_OBJECT_BYTES = 4_294_967_296L;

    private M5MaterializationRecordsV1() {}

    public enum PositionDomain {
        KAFKA_OFFSET,
        PULSAR_ENTRY
    }

    public enum SourceKind {
        OBJECT_WAL_NWG1,
        BOOKKEEPER_LEDGER,
        PULSAR_NPD1_DATA,
        PULSAR_NPO1_ROOT
    }

    public enum RepresentationMode {
        REFERENCE_REUSE,
        INDEX_ONLY_GENERATION,
        REWRITE_GENERATION
    }

    public enum PayloadKind {
        KAFKA_BATCH_PRESERVING_V1,
        KAFKA_SEMANTIC_COMPACTED_V1,
        PULSAR_ENTRY_PRESERVING_V1,
        NATIVE_EXTENT_REFERENCE_V1
    }

    public enum IndexKind {
        OFFSET_OR_POSITION,
        PAYLOAD_LOCATOR,
        TIMESTAMP,
        PRODUCER_RECOVERY,
        TRANSACTION,
        ABORTED_TRANSACTION,
        LEADER_EPOCH,
        CHECKSUM_COVERAGE
    }

    public enum TaskState {
        PLANNED,
        OUTPUT_VERIFIED,
        PUBLISHED,
        CANCELLED_STALE,
        QUARANTINED
    }

    public enum PublicationOutcome {
        APPLIED_EXACT,
        EXISTING_EXACT,
        DEFINITIVELY_NOT_APPLIED,
        CANCELLED_STALE,
        OUTCOME_UNKNOWN,
        CONFLICT,
        QUARANTINED
    }

    /** Exact common authority envelope. Worker identity is a fence, never publication authority. */
    public record IdentityEnvelope(
            Sha256Digest protocolCellSha256,
            Sha256Digest providerScopeSha256,
            BindingIdentity binding,
            long ownerEpoch,
            long workerEpoch,
            long storageFence,
            CapabilityBinding capability) {
        public IdentityEnvelope {
            requireDigest(protocolCellSha256, "protocolCellSha256");
            requireDigest(providerScopeSha256, "providerScopeSha256");
            Objects.requireNonNull(binding, "binding");
            Objects.requireNonNull(capability, "capability");
            if (ownerEpoch <= 0 || workerEpoch <= 0 || storageFence <= 0) {
                throw new IllegalArgumentException("M5 identity fence is outside its domain");
            }
        }
    }

    /** Typed inclusive/exclusive protocol coverage. */
    public record ProtocolCoverage(PositionDomain domain, long inclusiveStart, long exclusiveEnd) {
        public ProtocolCoverage {
            Objects.requireNonNull(domain, "domain");
            if (inclusiveStart < 0 || exclusiveEnd <= inclusiveStart) {
                throw new IllegalArgumentException("protocol coverage is empty or reversed");
            }
        }

        public boolean contains(ProtocolCoverage other) {
            return domain == other.domain
                    && inclusiveStart <= other.inclusiveStart
                    && exclusiveEnd >= other.exclusiveEnd;
        }

        public boolean adjacentTo(ProtocolCoverage other) {
            return domain == other.domain && exclusiveEnd == other.inclusiveStart;
        }
    }

    /** Immutable source member with exact body and backend identity. */
    public record SourceExtent(
            SourceKind kind,
            Sha256Digest sourceIdentitySha256,
            ProtocolCoverage coverage,
            String physicalKey,
            long canonicalLength,
            int recordCount,
            long minimumTimestamp,
            long maximumTimestamp,
            Sha256Digest bodySha256,
            Optional<CanonicalBytes> immutableProviderVersionToken,
            Optional<Sha256Digest> ledgerIdentitySha256,
            Sha256Digest formatRootSha256,
            Sha256Digest encryptionPolicySha256,
            boolean payloadLongLivedReadable,
            boolean requiredIndexesPresent,
            List<Sha256Digest> memberBindingIds) {
        public SourceExtent {
            Objects.requireNonNull(kind, "kind");
            requireDigest(sourceIdentitySha256, "sourceIdentitySha256");
            Objects.requireNonNull(coverage, "coverage");
            requireText(physicalKey, "physicalKey");
            if (canonicalLength <= 0 || canonicalLength > MAX_OBJECT_BYTES) {
                throw new IllegalArgumentException("source canonical length exceeds the M5 cap");
            }
            if (recordCount < 0
                    || (recordCount == 0 && (minimumTimestamp != -1 || maximumTimestamp != -1))
                    || (recordCount > 0 && (minimumTimestamp < 0 || maximumTimestamp < minimumTimestamp))) {
                throw new IllegalArgumentException("source record/timestamp summary is invalid");
            }
            requireDigest(bodySha256, "bodySha256");
            immutableProviderVersionToken = copyOptionalBytes(immutableProviderVersionToken);
            ledgerIdentitySha256 = Objects.requireNonNull(ledgerIdentitySha256, "ledgerIdentitySha256");
            ledgerIdentitySha256.ifPresent(value -> requireDigest(value, "ledgerIdentitySha256"));
            requireDigest(formatRootSha256, "formatRootSha256");
            requireDigest(encryptionPolicySha256, "encryptionPolicySha256");
            memberBindingIds = sortedUniqueDigests(memberBindingIds, MAX_MEMBER_BINDINGS, "memberBindingIds");
            if (kind == SourceKind.BOOKKEEPER_LEDGER && ledgerIdentitySha256.isEmpty()) {
                throw new IllegalArgumentException("BookKeeper source lacks a ledger identity");
            }
            if (kind != SourceKind.BOOKKEEPER_LEDGER && ledgerIdentitySha256.isPresent()) {
                throw new IllegalArgumentException("non-BookKeeper source carries a ledger identity");
            }
        }

        public boolean sharedPhysicalObject() {
            return memberBindingIds.size() > 1;
        }
    }

    /** Frozen source cut; its sourceSetSha256 is independently recomputed before planning. */
    public record MaterializationSourceCut(
            IdentityEnvelope identity,
            BindingReadSelector predecessorSelector,
            Sha256Digest predecessorSelectorValueSha256,
            Sha256Digest predecessorViewSha256,
            ProtocolCoverage coverage,
            long durableFrontier,
            long logEndFrontier,
            long highWatermark,
            long lastStableFrontier,
            long trimFrontier,
            Sha256Digest protocolStateRootSha256,
            Sha256Digest recoveryCheckpointRootSha256,
            Sha256Digest materializationPolicySha256,
            Sha256Digest outputFormatPolicySha256,
            Sha256Digest sourceSetSha256,
            List<SourceExtent> sources) {
        public MaterializationSourceCut {
            Objects.requireNonNull(identity, "identity");
            Objects.requireNonNull(predecessorSelector, "predecessorSelector");
            requireDigest(predecessorSelectorValueSha256, "predecessorSelectorValueSha256");
            requireDigest(predecessorViewSha256, "predecessorViewSha256");
            Objects.requireNonNull(coverage, "coverage");
            requireDigest(protocolStateRootSha256, "protocolStateRootSha256");
            requireDigest(recoveryCheckpointRootSha256, "recoveryCheckpointRootSha256");
            requireDigest(materializationPolicySha256, "materializationPolicySha256");
            requireDigest(outputFormatPolicySha256, "outputFormatPolicySha256");
            requireDigest(sourceSetSha256, "sourceSetSha256");
            sources = List.copyOf(Objects.requireNonNull(sources, "sources"));
            if (sources.isEmpty() || sources.size() > MAX_SOURCES) {
                throw new IllegalArgumentException("source cut count is outside the M5 cap");
            }
            if (!identity.binding().equals(predecessorSelector.binding())
                    || predecessorSelector.admissionState() != AdmissionState.ADMITTING
                    || !predecessorViewSha256.equals(predecessorSelector.selectedViewSha256())) {
                throw new IllegalArgumentException("source cut predecessor authority differs");
            }
            requireFrontiers(
                    coverage, durableFrontier, logEndFrontier, highWatermark, lastStableFrontier, trimFrontier);
            requireExactCoverage(coverage, sources);
        }
    }

    /** Deterministic output-part plan. */
    public record OutputPartPlan(
            int ordinal,
            ProtocolCoverage coverage,
            PayloadKind payloadKind,
            Sha256Digest canonicalPlanSha256,
            String objectKey) {
        public OutputPartPlan {
            if (ordinal < 0 || ordinal >= MAX_PARTS) {
                throw new IllegalArgumentException("output part ordinal is outside the M5 cap");
            }
            Objects.requireNonNull(coverage, "coverage");
            Objects.requireNonNull(payloadKind, "payloadKind");
            requireDigest(canonicalPlanSha256, "canonicalPlanSha256");
            requireText(objectKey, "objectKey");
        }
    }

    /** One immutable required index plan. */
    public record IndexPlan(
            IndexKind kind,
            ProtocolCoverage coverage,
            int parserVersion,
            Sha256Digest canonicalPlanSha256,
            String objectKey) {
        public IndexPlan {
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(coverage, "coverage");
            if (parserVersion != 1) {
                throw new IllegalArgumentException("only the frozen M5 index parser v1 is admitted");
            }
            requireDigest(canonicalPlanSha256, "canonicalPlanSha256");
            requireText(objectKey, "objectKey");
        }
    }

    /** Deterministic materialization plan and its two domain-separated identities. */
    public record MaterializationPlan(
            MaterializationSourceCut sourceCut,
            RepresentationMode representationMode,
            PayloadKind payloadKind,
            Sha256Digest taskIdSha256,
            Sha256Digest outputIdentitySha256,
            Sha256Digest encryptionGenerationSha256,
            Sha256Digest compressionPolicySha256,
            Sha256Digest checksumPolicySha256,
            List<OutputPartPlan> outputParts,
            List<IndexPlan> indexes) {
        public MaterializationPlan {
            Objects.requireNonNull(sourceCut, "sourceCut");
            Objects.requireNonNull(representationMode, "representationMode");
            Objects.requireNonNull(payloadKind, "payloadKind");
            requireDigest(taskIdSha256, "taskIdSha256");
            requireDigest(outputIdentitySha256, "outputIdentitySha256");
            requireDigest(encryptionGenerationSha256, "encryptionGenerationSha256");
            requireDigest(compressionPolicySha256, "compressionPolicySha256");
            requireDigest(checksumPolicySha256, "checksumPolicySha256");
            outputParts = sortedUniqueParts(outputParts);
            indexes = sortedUniqueIndexes(indexes);
            if (outputParts.isEmpty() || outputParts.size() > MAX_PARTS || indexes.size() > MAX_INDEXES) {
                throw new IllegalArgumentException("materialization output plan exceeds its M5 cap");
            }
            requireExactPartCoverage(sourceCut.coverage(), outputParts);
            if (representationMode == RepresentationMode.REFERENCE_REUSE
                    && (payloadKind != PayloadKind.NATIVE_EXTENT_REFERENCE_V1 || !indexes.isEmpty())) {
                throw new IllegalArgumentException("reference reuse cannot create payload or index Objects");
            }
            if (representationMode == RepresentationMode.INDEX_ONLY_GENERATION
                    && payloadKind != PayloadKind.NATIVE_EXTENT_REFERENCE_V1) {
                throw new IllegalArgumentException("index-only generation must reference the native payload");
            }
            if (representationMode == RepresentationMode.REWRITE_GENERATION
                    && payloadKind == PayloadKind.NATIVE_EXTENT_REFERENCE_V1) {
                throw new IllegalArgumentException("rewrite generation requires a concrete NMS1 payload kind");
            }
        }
    }

    /** Exact created or reused immutable payload/index Object. */
    public record GenerationObject(
            int ordinal,
            IndexKind indexKind,
            ProtocolCoverage coverage,
            ObjectIdentity identity,
            Optional<CanonicalBytes> immutableProviderVersionToken) {
        public GenerationObject {
            if (ordinal < 0 || ordinal >= MAX_PARTS) {
                throw new IllegalArgumentException("generation Object ordinal is outside the M5 cap");
            }
            Objects.requireNonNull(coverage, "coverage");
            Objects.requireNonNull(identity, "identity");
            immutableProviderVersionToken = copyOptionalBytes(immutableProviderVersionToken);
        }

        public boolean payload() {
            return indexKind == null;
        }
    }

    /** Independent full validation result required by publication. */
    public record GenerationValidationRoot(
            Sha256Digest taskIdSha256,
            Sha256Digest outputIdentitySha256,
            Sha256Digest sourceSetSha256,
            Sha256Digest validatedObjectsRootSha256,
            Sha256Digest coverageRootSha256,
            Sha256Digest lookupBoundaryRootSha256,
            Sha256Digest payloadEqualityOrSemanticRootSha256,
            Sha256Digest authorityFenceRootSha256,
            int validatedPayloadObjects,
            int validatedIndexObjects,
            long validatedCanonicalBytes) {
        public GenerationValidationRoot {
            requireDigest(taskIdSha256, "taskIdSha256");
            requireDigest(outputIdentitySha256, "outputIdentitySha256");
            requireDigest(sourceSetSha256, "sourceSetSha256");
            requireDigest(validatedObjectsRootSha256, "validatedObjectsRootSha256");
            requireDigest(coverageRootSha256, "coverageRootSha256");
            requireDigest(lookupBoundaryRootSha256, "lookupBoundaryRootSha256");
            requireDigest(payloadEqualityOrSemanticRootSha256, "payloadEqualityOrSemanticRootSha256");
            requireDigest(authorityFenceRootSha256, "authorityFenceRootSha256");
            if (validatedPayloadObjects <= 0 || validatedIndexObjects < 0 || validatedCanonicalBytes <= 0) {
                throw new IllegalArgumentException("validation summary is empty");
            }
        }
    }

    /** Immutable generation descriptor. */
    public record MaterializedGeneration(
            IdentityEnvelope identity,
            RepresentationMode representationMode,
            PayloadKind payloadKind,
            Sha256Digest taskIdSha256,
            Sha256Digest outputIdentitySha256,
            Sha256Digest sourceSetSha256,
            long sourceGeneration,
            ProtocolCoverage coverage,
            Sha256Digest protocolStateRootSha256,
            Optional<Sha256Digest> semanticProofRootSha256,
            Sha256Digest validationRootSha256,
            Sha256Digest predecessorSelectedViewSha256,
            Optional<Sha256Digest> fallbackSetSha256,
            List<GenerationObject> payloadObjects,
            List<GenerationObject> indexObjects) {
        public MaterializedGeneration {
            Objects.requireNonNull(identity, "identity");
            Objects.requireNonNull(representationMode, "representationMode");
            Objects.requireNonNull(payloadKind, "payloadKind");
            requireDigest(taskIdSha256, "taskIdSha256");
            requireDigest(outputIdentitySha256, "outputIdentitySha256");
            requireDigest(sourceSetSha256, "sourceSetSha256");
            Objects.requireNonNull(coverage, "coverage");
            requireDigest(protocolStateRootSha256, "protocolStateRootSha256");
            semanticProofRootSha256 = Objects.requireNonNull(semanticProofRootSha256, "semanticProofRootSha256");
            semanticProofRootSha256.ifPresent(value -> requireDigest(value, "semanticProofRootSha256"));
            requireDigest(validationRootSha256, "validationRootSha256");
            requireDigest(predecessorSelectedViewSha256, "predecessorSelectedViewSha256");
            fallbackSetSha256 = Objects.requireNonNull(fallbackSetSha256, "fallbackSetSha256");
            fallbackSetSha256.ifPresent(value -> requireDigest(value, "fallbackSetSha256"));
            payloadObjects = sortedUniqueGenerationObjects(payloadObjects, true);
            indexObjects = sortedUniqueGenerationObjects(indexObjects, false);
            if (sourceGeneration <= 0 || payloadObjects.isEmpty()) {
                throw new IllegalArgumentException("materialized generation is empty");
            }
            if ((payloadKind == PayloadKind.KAFKA_SEMANTIC_COMPACTED_V1) != semanticProofRootSha256.isPresent()) {
                throw new IllegalArgumentException("Kafka compacted payload and semantic root disagree");
            }
        }
    }

    /** Immutable manifest view: one preferred generation and the exact predecessor view as fallback. */
    public record BindingManifestView(
            IdentityEnvelope identity,
            Sha256Digest preferredGenerationSha256,
            Sha256Digest exactPredecessorViewSha256,
            Sha256Digest exactPredecessorSelectorValueSha256,
            ProtocolCoverage coverage,
            Optional<Sha256Digest> compactionSuppressionRootSha256) {
        public BindingManifestView {
            Objects.requireNonNull(identity, "identity");
            requireDigest(preferredGenerationSha256, "preferredGenerationSha256");
            requireDigest(exactPredecessorViewSha256, "exactPredecessorViewSha256");
            requireDigest(exactPredecessorSelectorValueSha256, "exactPredecessorSelectorValueSha256");
            Objects.requireNonNull(coverage, "coverage");
            compactionSuppressionRootSha256 =
                    Objects.requireNonNull(compactionSuppressionRootSha256, "compactionSuppressionRootSha256");
            compactionSuppressionRootSha256.ifPresent(value -> requireDigest(value, "compactionSuppressionRootSha256"));
        }
    }

    /** Persistent task row; terminal failure never grants deletion authority. */
    public record MaterializationTask(
            Sha256Digest taskIdSha256,
            TaskState state,
            Sha256Digest sourceCutSha256,
            Sha256Digest outputIdentitySha256,
            Optional<Sha256Digest> validationRootSha256,
            Optional<Sha256Digest> generationSha256,
            Optional<Sha256Digest> manifestViewSha256) {
        public MaterializationTask {
            requireDigest(taskIdSha256, "taskIdSha256");
            Objects.requireNonNull(state, "state");
            requireDigest(sourceCutSha256, "sourceCutSha256");
            requireDigest(outputIdentitySha256, "outputIdentitySha256");
            validationRootSha256 = optionalDigest(validationRootSha256, "validationRootSha256");
            generationSha256 = optionalDigest(generationSha256, "generationSha256");
            manifestViewSha256 = optionalDigest(manifestViewSha256, "manifestViewSha256");
            if (state == TaskState.PLANNED
                    && (validationRootSha256.isPresent()
                            || generationSha256.isPresent()
                            || manifestViewSha256.isPresent())) {
                throw new IllegalArgumentException("planned task carries successor authority");
            }
            if (state == TaskState.OUTPUT_VERIFIED
                    && (validationRootSha256.isEmpty() || generationSha256.isEmpty() || manifestViewSha256.isEmpty())) {
                throw new IllegalArgumentException("verified task lacks immutable outputs");
            }
            if (state == TaskState.PUBLISHED
                    && (validationRootSha256.isEmpty() || generationSha256.isEmpty() || manifestViewSha256.isEmpty())) {
                throw new IllegalArgumentException("published task lacks immutable outputs");
            }
        }
    }

    public static boolean predecessorHasFallback(MaterializationSourceCut sourceCut) {
        return sourceCut.predecessorSelector().mode() == SelectorMode.PREFERRED_WITH_FALLBACK;
    }

    private static void requireFrontiers(
            ProtocolCoverage coverage, long durable, long logEnd, long highWatermark, long lastStable, long trim) {
        if (durable < coverage.exclusiveEnd()
                || logEnd < durable
                || highWatermark < coverage.exclusiveEnd()
                || highWatermark > logEnd
                || lastStable < coverage.inclusiveStart()
                || lastStable > highWatermark
                || trim < 0
                || trim > coverage.inclusiveStart()) {
            throw new IllegalArgumentException("source cut frontier relation is invalid");
        }
    }

    private static void requireExactCoverage(ProtocolCoverage coverage, List<SourceExtent> sources) {
        List<SourceExtent> sorted = sources.stream()
                .sorted(Comparator.comparingLong(value -> value.coverage().inclusiveStart()))
                .toList();
        if (!sources.equals(sorted)
                || sources.get(0).coverage().inclusiveStart() != coverage.inclusiveStart()
                || sources.get(sources.size() - 1).coverage().exclusiveEnd() != coverage.exclusiveEnd()) {
            throw new IllegalArgumentException("source membership is not canonical exact coverage");
        }
        for (int index = 0; index < sources.size(); index++) {
            SourceExtent source = sources.get(index);
            if (source.coverage().domain() != coverage.domain()
                    || !coverage.contains(source.coverage())
                    || (index > 0 && !sources.get(index - 1).coverage().adjacentTo(source.coverage()))) {
                throw new IllegalArgumentException("source membership contains a gap, overlap, or domain mismatch");
            }
        }
    }

    private static void requireExactPartCoverage(ProtocolCoverage coverage, List<OutputPartPlan> parts) {
        if (parts.get(0).coverage().inclusiveStart() != coverage.inclusiveStart()
                || parts.get(parts.size() - 1).coverage().exclusiveEnd() != coverage.exclusiveEnd()) {
            throw new IllegalArgumentException("output parts do not cover the source cut");
        }
        for (int index = 0; index < parts.size(); index++) {
            OutputPartPlan part = parts.get(index);
            if (part.ordinal() != index
                    || part.coverage().domain() != coverage.domain()
                    || !coverage.contains(part.coverage())
                    || (index > 0 && !parts.get(index - 1).coverage().adjacentTo(part.coverage()))) {
                throw new IllegalArgumentException("output parts are not canonical exact coverage");
            }
        }
    }

    private static List<OutputPartPlan> sortedUniqueParts(List<OutputPartPlan> values) {
        values = List.copyOf(Objects.requireNonNull(values, "outputParts"));
        List<OutputPartPlan> sorted = values.stream()
                .sorted(Comparator.comparingInt(OutputPartPlan::ordinal))
                .toList();
        if (!values.equals(sorted)
                || values.stream().map(OutputPartPlan::ordinal).distinct().count() != values.size()) {
            throw new IllegalArgumentException("output parts are not sorted unique");
        }
        return values;
    }

    private static List<IndexPlan> sortedUniqueIndexes(List<IndexPlan> values) {
        values = List.copyOf(Objects.requireNonNull(values, "indexes"));
        List<IndexPlan> sorted = values.stream()
                .sorted(Comparator.comparing(value -> value.kind().ordinal()))
                .toList();
        if (!values.equals(sorted)
                || values.stream().map(IndexPlan::kind).distinct().count() != values.size()) {
            throw new IllegalArgumentException("index plans are not sorted unique");
        }
        return values;
    }

    private static List<GenerationObject> sortedUniqueGenerationObjects(
            List<GenerationObject> values, boolean payloadExpected) {
        values = List.copyOf(Objects.requireNonNull(values, "generationObjects"));
        List<GenerationObject> sorted = values.stream()
                .sorted(Comparator.comparingInt(GenerationObject::ordinal))
                .toList();
        if (!values.equals(sorted)
                || values.stream().map(GenerationObject::ordinal).distinct().count() != values.size()
                || values.stream().anyMatch(value -> value.payload() != payloadExpected)) {
            throw new IllegalArgumentException("generation Objects are not canonical for their class");
        }
        return values;
    }

    private static List<Sha256Digest> sortedUniqueDigests(List<Sha256Digest> values, int maximum, String label) {
        values = List.copyOf(Objects.requireNonNull(values, label));
        if (values.isEmpty() || values.size() > maximum) {
            throw new IllegalArgumentException(label + " count is outside its M5 cap");
        }
        values.forEach(value -> requireDigest(value, label));
        List<Sha256Digest> sorted = values.stream()
                .sorted(Comparator.comparing(Sha256Digest::toHex))
                .toList();
        if (!values.equals(sorted) || values.stream().distinct().count() != values.size()) {
            throw new IllegalArgumentException(label + " is not sorted unique");
        }
        return values;
    }

    private static Optional<CanonicalBytes> copyOptionalBytes(Optional<CanonicalBytes> value) {
        Objects.requireNonNull(value, "immutableProviderVersionToken");
        return value.map(bytes -> {
            if (bytes.isEmpty() || bytes.length() > 65_535) {
                throw new IllegalArgumentException("Provider version token is empty or oversized");
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        });
    }

    private static Optional<Sha256Digest> optionalDigest(Optional<Sha256Digest> value, String label) {
        value = Objects.requireNonNull(value, label);
        value.ifPresent(digest -> requireDigest(digest, label));
        return value;
    }

    private static void requireDigest(Sha256Digest value, String label) {
        Objects.requireNonNull(value, label);
        if (value.isZero()) {
            throw new IllegalArgumentException(label + " is the zero digest");
        }
    }

    private static void requireText(String value, String label) {
        if (value == null || value.isEmpty() || value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(label + " is empty or non-canonical");
        }
    }
}
