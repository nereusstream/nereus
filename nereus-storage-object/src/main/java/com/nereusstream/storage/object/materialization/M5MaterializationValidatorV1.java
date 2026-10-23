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
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.BindingManifestView;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.GenerationObject;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.GenerationValidationRoot;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexPlan;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationPlan;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializedGeneration;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.OutputPartPlan;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PayloadKind;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.RepresentationMode;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceExtent;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceKind;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Independent full-byte, coverage, index, and authority validation before M5-A publication. */
public final class M5MaterializationValidatorV1 {
    public record VerifiedObjectRead(CanonicalBytes body, Optional<CanonicalBytes> immutableProviderVersionToken) {
        public VerifiedObjectRead {
            body = CanonicalBytes.copyOf(Objects.requireNonNull(body, "body").toByteArray());
            immutableProviderVersionToken = Objects.requireNonNull(
                            immutableProviderVersionToken, "immutableProviderVersionToken")
                    .map(value -> CanonicalBytes.copyOf(value.toByteArray()));
            if (body.isEmpty()) {
                throw new IllegalArgumentException("verified Object read is empty");
            }
        }
    }

    public interface MaterializationDataReader {
        VerifiedObjectRead readObject(ObjectIdentity identity) throws IOException;

        CanonicalBytes readSource(SourceExtent source) throws IOException;

        default void verifyPulsarSource(SourceExtent source) throws IOException {
            throw new IOException("Pulsar validation requires independent native/provider source qualification");
        }

        default M5PulsarEntriesV2.Cursor openPulsarEntries(SourceExtent source) throws IOException {
            throw new IOException("Pulsar validation requires a per-entry native source reader");
        }
    }

    /** Actual implementations keep Provider response and original source cursor ownership inside this call. */
    public interface PulsarStreamedDataReader extends MaterializationDataReader {
        Nms1CodecV1.VerifiedPulsarPart readPulsarPart(MaterializationPlan plan, GenerationObject descriptor)
                throws IOException;

        VerifiedObjectRead readPulsarIndex(GenerationObject descriptor) throws IOException;
    }

    /** Issued only by independent full output/original-entry verification. This grants no M4 publication. */
    public static final class VerifiedPulsarOutputs {
        private final MaterializationPlan plan;
        private final List<GenerationObject> payloads;
        private final List<GenerationObject> indexes;
        private final long bytes;
        private final Sha256Digest lookupBoundaries;
        private final Sha256Digest entryEquality;

        private VerifiedPulsarOutputs(
                MaterializationPlan plan,
                List<GenerationObject> payloads,
                List<GenerationObject> indexes,
                long bytes,
                List<CanonicalBytes> indexBodies,
                Sha256Digest equality) {
            this.plan = plan;
            this.payloads = List.copyOf(payloads);
            this.indexes = List.copyOf(indexes);
            this.bytes = bytes;
            lookupBoundaries = hashLookupBoundaries(indexBodies);
            entryEquality = equality;
        }

        public MaterializationPlan plan() {
            return plan;
        }

        public List<GenerationObject> payloads() {
            return payloads;
        }

        public List<GenerationObject> indexes() {
            return indexes;
        }

        public long canonicalBytes() {
            return bytes;
        }
    }

    private record ReusedPhysicalRead(
            Sha256Digest providerScopeSha256,
            M5MaterializationRecordsV1.SourceKind kind,
            String physicalKey,
            Optional<CanonicalBytes> immutableProviderVersionToken,
            long canonicalLength,
            Sha256Digest bodySha256,
            Sha256Digest formatRootSha256,
            Sha256Digest encryptionPolicySha256) {
        private ReusedPhysicalRead(MaterializationPlan plan, SourceExtent source) {
            this(
                    plan.sourceCut().identity().providerScopeSha256(),
                    source.kind(),
                    source.physicalKey(),
                    source.immutableProviderVersionToken(),
                    source.canonicalLength(),
                    source.bodySha256(),
                    source.formatRootSha256(),
                    source.encryptionPolicySha256());
        }
    }

    public record ValidatedGeneration(
            GenerationValidationRoot validationRoot,
            MaterializedGeneration generation,
            BindingManifestView manifestView) {
        public ValidatedGeneration {
            Objects.requireNonNull(validationRoot, "validationRoot");
            Objects.requireNonNull(generation, "generation");
            Objects.requireNonNull(manifestView, "manifestView");
        }
    }

    /** M5-B-owned proof binding required before semantic Kafka output may use M5-A publication. */
    public record SemanticValidationProof(
            Sha256Digest taskIdSha256,
            Sha256Digest outputIdentitySha256,
            Sha256Digest sourceSetSha256,
            Sha256Digest semanticValidationRootSha256,
            Sha256Digest protocolStateRootSha256,
            Sha256Digest compactionSuppressionRootSha256,
            Sha256Digest payloadBodiesRootSha256,
            Sha256Digest indexBodiesRootSha256) {
        public SemanticValidationProof {
            requireDigest(taskIdSha256, "taskIdSha256");
            requireDigest(outputIdentitySha256, "outputIdentitySha256");
            requireDigest(sourceSetSha256, "sourceSetSha256");
            requireDigest(semanticValidationRootSha256, "semanticValidationRootSha256");
            requireDigest(protocolStateRootSha256, "protocolStateRootSha256");
            requireDigest(compactionSuppressionRootSha256, "compactionSuppressionRootSha256");
            requireDigest(payloadBodiesRootSha256, "payloadBodiesRootSha256");
            requireDigest(indexBodiesRootSha256, "indexBodiesRootSha256");
        }
    }

    public ValidatedGeneration validate(
            MaterializationPlan plan,
            BindingReadSelector currentSelector,
            List<SourceProtectionIdentity> exactFallbackSources,
            List<GenerationObject> payloadObjects,
            List<GenerationObject> indexObjects,
            MaterializationDataReader reader)
            throws IOException {
        Objects.requireNonNull(plan, "plan");
        Objects.requireNonNull(currentSelector, "currentSelector");
        Objects.requireNonNull(exactFallbackSources, "exactFallbackSources");
        Objects.requireNonNull(reader, "reader");
        M5MaterializationCodecV1.encodePlan(plan);
        requireCurrentAuthority(plan, currentSelector);
        requireExactCutMembership(plan);
        Sha256Digest fallbackSet = requireFallbackSources(plan, exactFallbackSources);
        payloadObjects = sortedGenerationObjects(payloadObjects, true);
        indexObjects = sortedGenerationObjects(indexObjects, false);
        requireOutputShape(plan, payloadObjects, indexObjects);
        if (plan.sourceCut().coverage().typedPulsar()) {
            return validatePulsar(plan, currentSelector, fallbackSet, payloadObjects, indexObjects, reader);
        }

        Map<ReusedPhysicalRead, VerifiedObjectRead> reusedReads = new HashMap<>();
        List<CanonicalBytes> sourceBodies = readAndValidateSources(plan, reader, reusedReads);
        List<CanonicalBytes> payloadBodies = readAndValidatePayloads(plan, payloadObjects, reader, reusedReads);
        List<CanonicalBytes> indexBodies = readAndValidateIndexes(plan, indexObjects, reader);
        requireBytePreservingSemantics(plan, sourceBodies, payloadBodies);

        return finishGeneration(
                plan,
                currentSelector,
                fallbackSet,
                payloadObjects,
                indexObjects,
                payloadBodies,
                indexBodies,
                hashBodies("M5-A-BYTE-EQUALITY-V1", sourceBodies, payloadBodies));
    }

    private ValidatedGeneration finishGeneration(
            MaterializationPlan plan,
            BindingReadSelector currentSelector,
            Sha256Digest fallbackSet,
            List<GenerationObject> payloadObjects,
            List<GenerationObject> indexObjects,
            List<CanonicalBytes> payloadBodies,
            List<CanonicalBytes> indexBodies,
            Sha256Digest payloadEquality) {
        long totalBytes = 0;
        for (CanonicalBytes body : payloadBodies) {
            totalBytes = Math.addExact(totalBytes, body.length());
        }
        for (CanonicalBytes body : indexBodies) {
            totalBytes = Math.addExact(totalBytes, body.length());
        }
        return finishGeneration(
                plan,
                currentSelector,
                fallbackSet,
                payloadObjects,
                indexObjects,
                totalBytes,
                indexBodies,
                payloadEquality);
    }

    private ValidatedGeneration finishGeneration(
            MaterializationPlan plan,
            BindingReadSelector currentSelector,
            Sha256Digest fallbackSet,
            List<GenerationObject> payloadObjects,
            List<GenerationObject> indexObjects,
            long totalBytes,
            List<CanonicalBytes> indexBodies,
            Sha256Digest payloadEquality) {
        return finishGeneration(
                plan,
                currentSelector,
                fallbackSet,
                payloadObjects,
                indexObjects,
                totalBytes,
                hashLookupBoundaries(indexBodies),
                payloadEquality);
    }

    private ValidatedGeneration finishGeneration(
            MaterializationPlan plan,
            BindingReadSelector currentSelector,
            Sha256Digest fallbackSet,
            List<GenerationObject> payloadObjects,
            List<GenerationObject> indexObjects,
            long totalBytes,
            Sha256Digest lookupBoundaries,
            Sha256Digest payloadEquality) {
        GenerationValidationRoot validation = new GenerationValidationRoot(
                plan.taskIdSha256(),
                plan.outputIdentitySha256(),
                plan.sourceCut().sourceSetSha256(),
                hashObjects("M5-A-VALIDATED-OBJECTS-V1", payloadObjects, indexObjects),
                hashCoverage("M5-A-COVERAGE-V1", payloadObjects, indexObjects),
                lookupBoundaries,
                payloadEquality,
                hashAuthority(plan, currentSelector),
                payloadObjects.size(),
                indexObjects.size(),
                totalBytes);
        Sha256Digest validationSha = M5MaterializationCodecV1.validationRootSha256(validation);
        long successorGeneration = Math.addExact(currentSelector.sourceGeneration(), 1);
        MaterializedGeneration generation = new MaterializedGeneration(
                plan.sourceCut().identity(),
                plan.representationMode(),
                plan.payloadKind(),
                plan.taskIdSha256(),
                plan.outputIdentitySha256(),
                plan.sourceCut().sourceSetSha256(),
                successorGeneration,
                plan.sourceCut().coverage(),
                plan.sourceCut().protocolStateRootSha256(),
                Optional.empty(),
                validationSha,
                plan.sourceCut().predecessorViewSha256(),
                Optional.of(fallbackSet),
                payloadObjects,
                indexObjects);
        Sha256Digest generationSha = M5MaterializationCodecV1.generationSha256(generation);
        BindingManifestView manifest = new BindingManifestView(
                plan.sourceCut().identity(),
                generationSha,
                plan.sourceCut().predecessorViewSha256(),
                plan.sourceCut().predecessorSelectorValueSha256(),
                plan.sourceCut().coverage(),
                Optional.empty());
        return new ValidatedGeneration(validation, generation, manifest);
    }

    private ValidatedGeneration validatePulsar(
            MaterializationPlan plan,
            BindingReadSelector current,
            Sha256Digest fallback,
            List<GenerationObject> payloads,
            List<GenerationObject> indexes,
            MaterializationDataReader reader)
            throws IOException {
        if (plan.representationMode() != RepresentationMode.REWRITE_GENERATION) {
            throw new IOException("Pulsar reuse validation requires its actual native payload locator adapter");
        }
        if (reader instanceof PulsarStreamedDataReader streaming) {
            return validatePulsarStreamed(plan, current, fallback, payloads, indexes, streaming);
        }
        List<CanonicalBytes> payloadBodies = new ArrayList<>();
        List<M5LookupIndexV1.Row> locators = new ArrayList<>();
        List<String> roots = new ArrayList<>();
        for (int ordinal = 0; ordinal < payloads.size(); ordinal++) {
            var source = plan.sourceCut().sources().get(ordinal);
            reader.verifyPulsarSource(source);
            var descriptor = payloads.get(ordinal);
            var read = reader.readObject(descriptor.identity());
            requireObjectIdentity(descriptor, read);
            if (!read.immutableProviderVersionToken().equals(descriptor.immutableProviderVersionToken())) {
                throw new IOException("Pulsar payload Provider version changed");
            }
            var decoded = Nms1CodecV1.decode(read.body());
            requirePulsarPayloadHeader(plan, decoded, ordinal);
            if (!decoded.sources()
                    .equals(List.of(new Nms1ObjectV1.SourceContribution(
                            source.sourceIdentitySha256(),
                            source.coverage(),
                            source.pulsarEntryRootSha256().orElseThrow())))) {
                throw new IOException("Pulsar source contribution differs from its frozen entry root");
            }
            var commitment = new M5PulsarEntriesV2.Commitment(
                    source.coverage().pulsarCoverage().orElseThrow());
            verifyPulsarEntries(reader, source, decoded, commitment, ordinal, locators);
            var root = commitment.finish();
            commitment.requireTimestampSummary(source.minimumTimestamp(), source.maximumTimestamp());
            if (!root.equals(source.pulsarEntryRootSha256().orElseThrow())) {
                throw new IOException("Pulsar independent entry root differs from the frozen cut");
            }
            reader.verifyPulsarSource(source);
            roots.add(root.toHex());
            payloadBodies.add(read.body());
        }
        List<CanonicalBytes> indexBodies = new ArrayList<>();
        for (int ordinal = 0; ordinal < indexes.size(); ordinal++) {
            var descriptor = indexes.get(ordinal);
            var read = reader.readObject(descriptor.identity());
            requireObjectIdentity(descriptor, read);
            if (!read.immutableProviderVersionToken().equals(descriptor.immutableProviderVersionToken())) {
                throw new IOException("Pulsar index Provider version changed");
            }
            var decoded = M5LookupIndexV1.decode(read.body());
            if (decoded.kind() != descriptor.indexKind()
                    || !decoded.coverage().equals(descriptor.coverage())
                    || !decoded.taskIdSha256().equals(plan.taskIdSha256())
                    || !decoded.outputIdentitySha256().equals(plan.outputIdentitySha256())
                    || !decoded.rows().equals(locators)) {
                throw new IOException("Pulsar index does not bind every exact original entry locator");
            }
            requireLookupBoundaries(decoded);
            indexBodies.add(read.body());
        }
        requireCurrentAuthority(plan, current);
        return finishGeneration(
                plan,
                current,
                fallback,
                payloads,
                indexes,
                payloadBodies,
                indexBodies,
                hash("M5-A-PULSAR-ENTRY-EQUALITY-V2", roots));
    }

    private ValidatedGeneration validatePulsarStreamed(
            MaterializationPlan plan,
            BindingReadSelector current,
            Sha256Digest fallback,
            List<GenerationObject> payloads,
            List<GenerationObject> indexes,
            PulsarStreamedDataReader reader)
            throws IOException {
        var verified = verifyPulsarOutputs(plan, payloads, indexes, reader);
        requireCurrentAuthority(plan, current);
        return finishGeneration(
                plan,
                current,
                fallback,
                verified.payloads,
                verified.indexes,
                verified.bytes,
                verified.lookupBoundaries,
                verified.entryEquality);
    }

    public VerifiedPulsarOutputs verifyPulsarOutputs(
            MaterializationPlan plan,
            List<GenerationObject> payloads,
            List<GenerationObject> indexes,
            PulsarStreamedDataReader reader)
            throws IOException {
        M5MaterializationCodecV1.encodePlan(plan);
        requireExactCutMembership(plan);
        if (!plan.sourceCut().coverage().typedPulsar()
                || plan.representationMode() != RepresentationMode.REWRITE_GENERATION) {
            throw new IllegalArgumentException("streamed verification requires the original typed Pulsar rewrite");
        }
        payloads = sortedGenerationObjects(payloads, true);
        indexes = sortedGenerationObjects(indexes, false);
        requireOutputShape(plan, payloads, indexes);
        long bytes = 0;
        List<M5LookupIndexV1.Row> locators = new ArrayList<>();
        List<String> roots = new ArrayList<>();
        for (int ordinal = 0; ordinal < payloads.size(); ordinal++) {
            var source = plan.sourceCut().sources().get(ordinal);
            reader.verifyPulsarSource(source);
            var descriptor = payloads.get(ordinal);
            var verified = reader.readPulsarPart(plan, descriptor);
            if (verified.body().bytes() != descriptor.identity().bodyLength()
                    || !verified.body().sha256().equals(descriptor.identity().bodySha256())) {
                throw new IOException("Pulsar streamed Object identity differs");
            }
            bytes = Math.addExact(bytes, verified.body().bytes());
            locators.addAll(verified.locators());
            roots.add(source.pulsarEntryRootSha256().orElseThrow().toHex());
            reader.verifyPulsarSource(source);
        }
        List<CanonicalBytes> indexBodies = new ArrayList<>();
        for (var descriptor : indexes) {
            var read = reader.readPulsarIndex(descriptor);
            requireObjectIdentity(descriptor, read);
            if (!read.immutableProviderVersionToken().equals(descriptor.immutableProviderVersionToken())) {
                throw new IOException("Pulsar streamed index Provider version changed");
            }
            var decoded = M5LookupIndexV1.decode(read.body());
            if (decoded.kind() != descriptor.indexKind()
                    || !decoded.coverage().equals(descriptor.coverage())
                    || !decoded.taskIdSha256().equals(plan.taskIdSha256())
                    || !decoded.outputIdentitySha256().equals(plan.outputIdentitySha256())
                    || !decoded.rows().equals(locators)) {
                throw new IOException("Pulsar streamed index differs from exact entry locators");
            }
            requireLookupBoundaries(decoded);
            bytes = Math.addExact(bytes, read.body().length());
            indexBodies.add(read.body());
        }
        return new VerifiedPulsarOutputs(
                plan, payloads, indexes, bytes, indexBodies, hash("M5-A-PULSAR-ENTRY-EQUALITY-V2", roots));
    }

    /** Freezes publication roots from the same private full-read proof and exact current M4 protections. */
    public ValidatedGeneration finalizePulsarOutputs(
            VerifiedPulsarOutputs verified,
            BindingReadSelector current,
            List<SourceProtectionIdentity> exactFallbackSources) {
        Objects.requireNonNull(verified, "original independently verified outputs");
        requireCurrentAuthority(verified.plan, current);
        Sha256Digest fallback = requireFallbackProtections(verified.plan.sourceCut(), exactFallbackSources);
        return finishGeneration(
                verified.plan,
                current,
                fallback,
                verified.payloads,
                verified.indexes,
                verified.bytes,
                verified.lookupBoundaries,
                verified.entryEquality);
    }

    private static void requirePulsarPayloadHeader(MaterializationPlan plan, Nms1ObjectV1 value, int ordinal) {
        if (!value.indexes().isEmpty()) {
            throw new IllegalStateException("Pulsar rewrite plan does not contain embedded indexes");
        }
        if (!value.identity().equals(plan.sourceCut().identity())
                || value.payloadKind() != plan.payloadKind()
                || !value.taskIdSha256().equals(plan.taskIdSha256())
                || !value.outputIdentitySha256().equals(plan.outputIdentitySha256())
                || !value.coverage().equals(plan.outputParts().get(ordinal).coverage())
                || value.partOrdinal() != ordinal
                || value.partCount() != plan.outputParts().size()
                || !value.encryptionGenerationSha256().equals(plan.encryptionGenerationSha256())
                || !value.compressionPolicySha256().equals(plan.compressionPolicySha256())
                || !value.checksumPolicySha256().equals(plan.checksumPolicySha256())) {
            throw new IllegalStateException("Pulsar payload header differs from the exact plan");
        }
    }

    private static void verifyPulsarEntries(
            MaterializationDataReader reader,
            SourceExtent source,
            Nms1ObjectV1 payload,
            M5PulsarEntriesV2.Commitment commitment,
            int ordinal,
            List<M5LookupIndexV1.Row> locators)
            throws IOException {
        byte[] body = payload.payload().toByteArray();
        try (var entries = reader.openPulsarEntries(source)) {
            for (var extent : payload.extents()) {
                var original = entries.next().orElseThrow(() -> new IOException("Pulsar original entry is missing"));
                commitment.accept(original);
                var bytes = CanonicalBytes.copyOf(Arrays.copyOfRange(
                        body, extent.payloadOffset(), Math.addExact(extent.payloadOffset(), extent.payloadLength())));
                if (!original.position()
                                .equals(extent.coverage()
                                        .pulsarCoverage()
                                        .orElseThrow()
                                        .firstPosition())
                        || !original.payload().equals(bytes)
                        || !Sha256Digest.hash(bytes).equals(extent.payloadSha256())
                        || extent.minimumTimestamp() != original.timestamp()
                        || extent.maximumTimestamp() != original.timestamp()) {
                    throw new IOException("Pulsar payload entry bytes/position/timestamp differ from native source");
                }
                locators.add(new M5LookupIndexV1.Row(
                        extent.coverage(),
                        ordinal,
                        extent.payloadOffset(),
                        extent.payloadLength(),
                        extent.minimumTimestamp(),
                        extent.maximumTimestamp(),
                        extent.payloadSha256()));
            }
            if (entries.next().isPresent()) {
                throw new IOException("Pulsar original source has extra entries outside the cut");
            }
        }
    }

    /**
     * Applies the unchanged M5-A authority/publication envelope to independently validated M5-B semantic output.
     */
    public ValidatedGeneration validateSemantic(
            MaterializationPlan plan,
            BindingReadSelector currentSelector,
            List<SourceProtectionIdentity> exactFallbackSources,
            List<GenerationObject> payloadObjects,
            List<GenerationObject> indexObjects,
            MaterializationDataReader reader,
            SemanticValidationProof proof)
            throws IOException {
        Objects.requireNonNull(plan, "plan");
        Objects.requireNonNull(currentSelector, "currentSelector");
        Objects.requireNonNull(exactFallbackSources, "exactFallbackSources");
        Objects.requireNonNull(reader, "reader");
        Objects.requireNonNull(proof, "proof");
        M5MaterializationCodecV1.encodePlan(plan);
        if (plan.representationMode() != RepresentationMode.REWRITE_GENERATION
                || plan.payloadKind() != PayloadKind.KAFKA_SEMANTIC_COMPACTED_V1
                || !proof.taskIdSha256().equals(plan.taskIdSha256())
                || !proof.outputIdentitySha256().equals(plan.outputIdentitySha256())
                || !proof.sourceSetSha256().equals(plan.sourceCut().sourceSetSha256())) {
            throw new IllegalArgumentException("M5-B semantic proof differs from the materialization plan");
        }
        requireCurrentAuthority(plan, currentSelector);
        requireExactCutMembership(plan);
        Sha256Digest fallbackSet = requireFallbackSources(plan, exactFallbackSources);
        payloadObjects = sortedGenerationObjects(payloadObjects, true);
        indexObjects = sortedGenerationObjects(indexObjects, false);
        requireOutputShape(plan, payloadObjects, indexObjects);

        readAndValidateSources(plan, reader, Map.of());
        List<CanonicalBytes> payloadBodies = readAndValidatePayloads(plan, payloadObjects, reader, Map.of());
        List<CanonicalBytes> indexBodies = readAndValidateSemanticIndexes(indexObjects, reader);
        if (!semanticPayloadBodiesRoot(payloadBodies).equals(proof.payloadBodiesRootSha256())
                || !semanticIndexBodiesRoot(indexBodies).equals(proof.indexBodiesRootSha256())) {
            throw new IllegalStateException("M5-B semantic proof does not bind the reread output bytes");
        }

        long totalBytes = 0;
        for (CanonicalBytes body : payloadBodies) {
            totalBytes = Math.addExact(totalBytes, body.length());
        }
        for (CanonicalBytes body : indexBodies) {
            totalBytes = Math.addExact(totalBytes, body.length());
        }
        GenerationValidationRoot validation = new GenerationValidationRoot(
                plan.taskIdSha256(),
                plan.outputIdentitySha256(),
                plan.sourceCut().sourceSetSha256(),
                hashObjects("M5-B-VALIDATED-OBJECTS-V1", payloadObjects, indexObjects),
                hashCoverage("M5-B-COVERAGE-V1", payloadObjects, indexObjects),
                proof.indexBodiesRootSha256(),
                proof.semanticValidationRootSha256(),
                hashAuthority(plan, currentSelector),
                payloadObjects.size(),
                indexObjects.size(),
                totalBytes);
        Sha256Digest validationSha = M5MaterializationCodecV1.validationRootSha256(validation);
        MaterializedGeneration generation = new MaterializedGeneration(
                plan.sourceCut().identity(),
                plan.representationMode(),
                plan.payloadKind(),
                plan.taskIdSha256(),
                plan.outputIdentitySha256(),
                plan.sourceCut().sourceSetSha256(),
                Math.addExact(currentSelector.sourceGeneration(), 1),
                plan.sourceCut().coverage(),
                proof.protocolStateRootSha256(),
                Optional.of(proof.semanticValidationRootSha256()),
                validationSha,
                plan.sourceCut().predecessorViewSha256(),
                Optional.of(fallbackSet),
                payloadObjects,
                indexObjects);
        Sha256Digest generationSha = M5MaterializationCodecV1.generationSha256(generation);
        BindingManifestView manifest = new BindingManifestView(
                plan.sourceCut().identity(),
                generationSha,
                plan.sourceCut().predecessorViewSha256(),
                plan.sourceCut().predecessorSelectorValueSha256(),
                plan.sourceCut().coverage(),
                Optional.of(proof.compactionSuppressionRootSha256()));
        return new ValidatedGeneration(validation, generation, manifest);
    }

    public static Sha256Digest semanticPayloadBodiesRoot(List<CanonicalBytes> bodies) {
        return hashBodies("M5-B-PAYLOAD-BODIES-V1", List.of(), List.copyOf(bodies));
    }

    public static Sha256Digest semanticIndexBodiesRoot(List<CanonicalBytes> bodies) {
        return hashBodies("M5-B-INDEX-BODIES-V1", List.of(), List.copyOf(bodies));
    }

    private static void requireCurrentAuthority(MaterializationPlan plan, BindingReadSelector current) {
        BindingReadSelector predecessor = plan.sourceCut().predecessorSelector();
        if (!predecessor.equals(current)
                || current.ownerEpoch() != plan.sourceCut().identity().ownerEpoch()
                || !current.capability().equals(plan.sourceCut().identity().capability())
                || !Sha256Digest.hash(M4ReadControlCodecV1.encodeSelector(current))
                        .equals(plan.sourceCut().predecessorSelectorValueSha256())) {
            throw new IllegalStateException("M5 materialization authority is stale");
        }
    }

    private static void requireExactCutMembership(MaterializationPlan plan) {
        plan.sourceCut().coverage().requireCurrentPulsar();
        Sha256Digest binding = plan.sourceCut().identity().binding().bindingId().digest();
        for (SourceExtent source : plan.sourceCut().sources()) {
            if (!source.memberBindingIds().contains(binding)
                    || (source.sharedPhysicalObject()
                            && source.kind() != SourceKind.OBJECT_WAL_NWG1
                            && source.kind() != SourceKind.PULSAR_NPD1_DATA)) {
                throw new IllegalArgumentException("M5 source has foreign or unsupported physical membership");
            }
        }
    }

    private static Sha256Digest requireFallbackSources(
            MaterializationPlan plan, List<SourceProtectionIdentity> sources) {
        return requireFallbackProtections(plan.sourceCut(), sources);
    }

    /** Shared source-cut membership validation; it constructs no Object materialization plan. */
    public static Sha256Digest requireFallbackProtections(
            M5MaterializationRecordsV1.MaterializationSourceCut cut, List<SourceProtectionIdentity> sources) {
        sources = List.copyOf(sources);
        List<SourceProtectionIdentity> sorted = sources.stream()
                .sorted(Comparator.comparing(
                        value -> value.sourceIdentitySha256().toHex()))
                .toList();
        if (sources.isEmpty()
                || !sources.equals(sorted)
                || sources.stream().distinct().count() != sources.size()
                || sources.stream().anyMatch(value -> !value.capability()
                        .equals(cut.identity().capability()))) {
            throw new IllegalArgumentException("M5 fallback protections are empty, non-canonical, or capability-stale");
        }
        List<Sha256Digest> protectedIdentities = sources.stream()
                .map(SourceProtectionIdentity::sourceIdentitySha256)
                .toList();
        List<Sha256Digest> cutIdentities = cut.sources().stream()
                .map(SourceExtent::sourceIdentitySha256)
                .sorted(Comparator.comparing(Sha256Digest::toHex))
                .toList();
        if (!protectedIdentities.equals(cutIdentities)) {
            throw new IllegalArgumentException("M5 fallback protections differ from the frozen source identities");
        }
        Sha256Digest calculated = M4ReadControlCodecV1.calculateFallbackSetSha256(sources);
        Optional<Sha256Digest> existing = cut.predecessorSelector().fallbackSetSha256();
        if (existing.isPresent() && !existing.orElseThrow().equals(calculated)) {
            throw new IllegalArgumentException("M5 fallback protection membership differs from the predecessor");
        }
        if (existing.isEmpty()) {
            long introducedEpoch = Math.addExact(cut.predecessorSelector().readAdmissionEpoch(), 1);
            if (sources.stream()
                    .anyMatch(source -> source.firstFallbackCapableReadAdmissionEpoch() != introducedEpoch)) {
                throw new IllegalArgumentException(
                        "new M5 fallback protection must begin in the introduced read epoch");
            }
        }
        return calculated;
    }

    private static List<CanonicalBytes> readAndValidateSources(
            MaterializationPlan plan,
            MaterializationDataReader reader,
            Map<ReusedPhysicalRead, VerifiedObjectRead> reusedReads)
            throws IOException {
        List<CanonicalBytes> result = new ArrayList<>(plan.sourceCut().sources().size());
        for (SourceExtent source : plan.sourceCut().sources()) {
            CanonicalBytes body;
            if (plan.representationMode() == RepresentationMode.REWRITE_GENERATION) {
                body = CanonicalBytes.copyOf(reader.readSource(source).toByteArray());
            } else {
                ReusedPhysicalRead key = new ReusedPhysicalRead(plan, source);
                VerifiedObjectRead read = reusedReads.get(key);
                if (read == null) {
                    ObjectIdentity identity =
                            new ObjectIdentity(source.physicalKey(), source.canonicalLength(), source.bodySha256());
                    read = reader.readObject(identity);
                    reusedReads.put(key, read);
                }
                if (!read.immutableProviderVersionToken().equals(source.immutableProviderVersionToken())) {
                    throw new IllegalStateException("M5 reused source Provider version differs");
                }
                body = read.body();
            }
            if (body.length() != source.canonicalLength()
                    || !Sha256Digest.hash(body).equals(source.bodySha256())) {
                throw new IllegalStateException("M5 source body identity differs: " + source.physicalKey());
            }
            result.add(body);
        }
        return List.copyOf(result);
    }

    private static List<CanonicalBytes> readAndValidatePayloads(
            MaterializationPlan plan,
            List<GenerationObject> objects,
            MaterializationDataReader reader,
            Map<ReusedPhysicalRead, VerifiedObjectRead> reusedReads)
            throws IOException {
        List<CanonicalBytes> result = new ArrayList<>(objects.size());
        for (int index = 0; index < objects.size(); index++) {
            GenerationObject object = objects.get(index);
            OutputPartPlan part = plan.outputParts().get(index);
            VerifiedObjectRead read = plan.representationMode() == RepresentationMode.REWRITE_GENERATION
                    ? reader.readObject(object.identity())
                    : reusedReads.get(new ReusedPhysicalRead(
                            plan, plan.sourceCut().sources().get(index)));
            if (read == null) {
                throw new IllegalStateException("M5 reused source was not independently read");
            }
            requireObjectIdentity(object, read);
            if (!read.immutableProviderVersionToken().equals(object.immutableProviderVersionToken())) {
                throw new IllegalStateException("M5 payload Provider version differs");
            }
            if (plan.representationMode() == RepresentationMode.REWRITE_GENERATION) {
                Nms1ObjectV1 decoded = Nms1CodecV1.decode(read.body());
                if (!decoded.identity().equals(plan.sourceCut().identity())
                        || decoded.payloadKind() != plan.payloadKind()
                        || !decoded.taskIdSha256().equals(plan.taskIdSha256())
                        || !decoded.outputIdentitySha256().equals(plan.outputIdentitySha256())
                        || !decoded.coverage().equals(part.coverage())
                        || decoded.partOrdinal() != index
                        || decoded.partCount() != objects.size()) {
                    throw new IllegalStateException("NMS1 payload header differs from the deterministic plan");
                }
                result.add(decoded.payload());
            } else {
                SourceExtent source = plan.sourceCut().sources().get(index);
                if (!object.identity().key().equals(source.physicalKey())
                        || object.identity().bodyLength() != source.canonicalLength()
                        || !object.identity().bodySha256().equals(source.bodySha256())
                        || !object.immutableProviderVersionToken().equals(source.immutableProviderVersionToken())) {
                    throw new IllegalStateException("native payload reference differs from the source cut");
                }
                result.add(read.body());
            }
        }
        return List.copyOf(result);
    }

    private static List<CanonicalBytes> readAndValidateIndexes(
            MaterializationPlan plan, List<GenerationObject> objects, MaterializationDataReader reader)
            throws IOException {
        List<CanonicalBytes> result = new ArrayList<>(objects.size());
        for (int index = 0; index < objects.size(); index++) {
            GenerationObject object = objects.get(index);
            IndexPlan expected = plan.indexes().get(index);
            VerifiedObjectRead read = reader.readObject(object.identity());
            requireObjectIdentity(object, read);
            if (!read.immutableProviderVersionToken().equals(object.immutableProviderVersionToken())) {
                throw new IllegalStateException("M5 index Provider version differs");
            }
            M5LookupIndexV1 decoded = M5LookupIndexV1.decode(read.body());
            if (decoded.kind() != expected.kind()
                    || !decoded.coverage().equals(expected.coverage())
                    || !decoded.taskIdSha256().equals(plan.taskIdSha256())
                    || !decoded.outputIdentitySha256().equals(plan.outputIdentitySha256())) {
                throw new IllegalStateException("M5 index identity differs from the deterministic plan");
            }
            requireLookupBoundaries(decoded);
            result.add(read.body());
        }
        return List.copyOf(result);
    }

    private static List<CanonicalBytes> readAndValidateSemanticIndexes(
            List<GenerationObject> objects, MaterializationDataReader reader) throws IOException {
        List<CanonicalBytes> result = new ArrayList<>(objects.size());
        for (GenerationObject object : objects) {
            VerifiedObjectRead read = reader.readObject(object.identity());
            requireObjectIdentity(object, read);
            if (!read.immutableProviderVersionToken().equals(object.immutableProviderVersionToken())) {
                throw new IllegalStateException("M5-B index Provider version differs");
            }
            result.add(read.body());
        }
        return List.copyOf(result);
    }

    private static void requireOutputShape(
            MaterializationPlan plan, List<GenerationObject> payload, List<GenerationObject> indexes) {
        if (payload.size() != plan.outputParts().size()
                || indexes.size() != plan.indexes().size()) {
            throw new IllegalArgumentException("M5 output Object count differs from the deterministic plan");
        }
        for (int index = 0; index < payload.size(); index++) {
            GenerationObject object = payload.get(index);
            OutputPartPlan expected = plan.outputParts().get(index);
            if (object.ordinal() != index
                    || !object.payload()
                    || !object.coverage().equals(expected.coverage())
                    || !object.identity().key().equals(expected.objectKey())) {
                throw new IllegalArgumentException("M5 payload Object differs from its part plan");
            }
        }
        for (int index = 0; index < indexes.size(); index++) {
            GenerationObject object = indexes.get(index);
            IndexPlan expected = plan.indexes().get(index);
            if (object.ordinal() != index
                    || object.indexKind() != expected.kind()
                    || !object.coverage().equals(expected.coverage())
                    || !object.identity().key().equals(expected.objectKey())) {
                throw new IllegalArgumentException("M5 index Object differs from its index plan");
            }
        }
    }

    private static void requireObjectIdentity(GenerationObject object, VerifiedObjectRead read) {
        if (read.body().length() != object.identity().bodyLength()
                || !Sha256Digest.hash(read.body()).equals(object.identity().bodySha256())) {
            throw new IllegalStateException("M5 Object length or full-body SHA-256 differs");
        }
    }

    private static void requireBytePreservingSemantics(
            MaterializationPlan plan, List<CanonicalBytes> sources, List<CanonicalBytes> payloads) {
        if (plan.payloadKind() == PayloadKind.KAFKA_SEMANTIC_COMPACTED_V1) {
            throw new IllegalArgumentException("M5-A cannot validate semantic compaction output");
        }
        if (!equalSegments(sources, payloads)) {
            throw new IllegalStateException("M5-A byte-preserving payload differs from the frozen source cut");
        }
    }

    private static void requireLookupBoundaries(M5LookupIndexV1 index) {
        if (index.coverage().typedPulsar()) {
            var selected = index.coverage().pulsarCoverage().orElseThrow();
            var first = selected.firstPosition();
            if ((first.entryId() > 0
                            && index.lookup(new M5PulsarCoverageV2.Position(first.ledgerId(), first.entryId() - 1))
                                    .isPresent())
                    || index.lookup(selected.endPosition()).isPresent()) {
                throw new IllegalStateException("Pulsar index exposes a position outside its selected cut");
            }
            for (int row = 0; row < index.rows().size(); row++) {
                var current = index.rows().get(row);
                var coverage = current.coverage().pulsarCoverage().orElseThrow();
                if (!index.lookup(coverage.firstPosition()).orElseThrow().equals(current)) {
                    throw new IllegalStateException("Pulsar index cannot locate its exact original entry");
                }
                var next = index.lookup(coverage.endPosition());
                if (row + 1 < index.rows().size()
                        ? next.isEmpty()
                                || !next.orElseThrow().equals(index.rows().get(row + 1))
                        : next.isPresent()) {
                    throw new IllegalStateException("Pulsar index cannot locate its captured-chain successor");
                }
            }
            return;
        }
        long first = index.coverage().inclusiveStart();
        long last = index.coverage().exclusiveEnd() - 1;
        if (!index.rows().isEmpty()) {
            if (index.lookup(first).isEmpty() || index.lookup(last).isEmpty()) {
                throw new IllegalStateException("M5 index cannot resolve first/last boundary or successor");
            }
            for (int row = 1; row < index.rows().size(); row++) {
                long predecessorEnd = index.rows().get(row - 1).coverage().exclusiveEnd();
                long successorStart = index.rows().get(row).coverage().inclusiveStart();
                if (predecessorEnd < successorStart
                        && !index.lookup(predecessorEnd)
                                .orElseThrow()
                                .equals(index.rows().get(row))) {
                    throw new IllegalStateException("M5 index gap does not select its first successor");
                }
            }
        }
    }

    private static List<GenerationObject> sortedGenerationObjects(List<GenerationObject> values, boolean payload) {
        values = List.copyOf(Objects.requireNonNull(values, "generationObjects"));
        List<GenerationObject> sorted = values.stream()
                .sorted(Comparator.comparingInt(GenerationObject::ordinal))
                .toList();
        if (!values.equals(sorted)
                || values.stream().map(GenerationObject::ordinal).distinct().count() != values.size()
                || values.stream().anyMatch(value -> value.payload() != payload)) {
            throw new IllegalArgumentException("M5 generation Objects are not sorted unique for their class");
        }
        return values;
    }

    private static Sha256Digest hashObjects(
            String domain, List<GenerationObject> payload, List<GenerationObject> indexes) {
        List<String> values = new ArrayList<>();
        for (GenerationObject object : payload) {
            values.add(object.identity().key());
            values.add(Long.toString(object.identity().bodyLength()));
            values.add(object.identity().bodySha256().toHex());
        }
        for (GenerationObject object : indexes) {
            values.add(object.indexKind().name());
            values.add(object.identity().key());
            values.add(Long.toString(object.identity().bodyLength()));
            values.add(object.identity().bodySha256().toHex());
        }
        return hash(domain, values);
    }

    private static Sha256Digest hashCoverage(
            String domain, List<GenerationObject> payload, List<GenerationObject> indexes) {
        List<String> values = new ArrayList<>();
        for (GenerationObject object : payload) {
            values.add(coverageText(object));
        }
        for (GenerationObject object : indexes) {
            values.add(object.indexKind().name());
            values.add(coverageText(object));
        }
        return hash(domain, values);
    }

    private static Sha256Digest hashLookupBoundaries(List<CanonicalBytes> indexBodies) {
        try {
            MessageDigest sha = MessageDigest.getInstance("SHA-256");
            sha.update("M5-A-LOOKUP-BOUNDARIES-V1\0".getBytes(StandardCharsets.UTF_8));
            if (indexBodies.isEmpty()) {
                sha.update("NO_NEW_INDEX_OBJECTS".getBytes(StandardCharsets.UTF_8));
            }
            byte[] hex = new byte[8192];
            for (int index = 0; index < indexBodies.size(); index++) {
                if (index > 0) {
                    sha.update((byte) 0);
                }
                byte[] body = indexBodies.get(index).toByteArray();
                for (int offset = 0; offset < body.length; ) {
                    int count = Math.min(body.length - offset, hex.length / 2);
                    for (int byteIndex = 0; byteIndex < count; byteIndex++) {
                        int value = Byte.toUnsignedInt(body[offset + byteIndex]);
                        hex[byteIndex * 2] = (byte) Character.forDigit(value >>> 4, 16);
                        hex[byteIndex * 2 + 1] = (byte) Character.forDigit(value & 15, 16);
                    }
                    sha.update(hex, 0, count * 2);
                    offset += count;
                }
            }
            return Sha256Digest.copyOf(sha.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("JDK has no SHA-256 provider", e);
        }
    }

    private static Sha256Digest hashBodies(String domain, List<CanonicalBytes> sources, List<CanonicalBytes> payloads) {
        return hash(
                domain,
                List.of(hashSegments(sources).toHex(), hashSegments(payloads).toHex()));
    }

    private static Sha256Digest hashAuthority(MaterializationPlan plan, BindingReadSelector selector) {
        return hash(
                "M5-A-AUTHORITY-FENCE-V1",
                List.of(
                        plan.sourceCut().identity().protocolCellSha256().toHex(),
                        plan.sourceCut().identity().providerScopeSha256().toHex(),
                        Long.toString(plan.sourceCut().identity().ownerEpoch()),
                        Long.toString(plan.sourceCut().identity().workerEpoch()),
                        Long.toString(plan.sourceCut().identity().storageFence()),
                        Sha256Digest.hash(M4ReadControlCodecV1.encodeSelector(selector))
                                .toHex()));
    }

    private static String coverageText(GenerationObject object) {
        if (object.coverage().typedPulsar()) {
            return "PULSAR_ENTRY_V2:"
                    + object.coverage().pulsarCoverage().orElseThrow().sha256().toHex();
        }
        return object.coverage().domain().name()
                + ":"
                + object.coverage().inclusiveStart()
                + ":"
                + object.coverage().exclusiveEnd();
    }

    private static Sha256Digest hash(String domain, List<String> values) {
        String body = domain + "\0" + String.join("\0", values);
        return Sha256Digest.hash(CanonicalBytes.copyOf(body.getBytes(StandardCharsets.UTF_8)));
    }

    private static void requireDigest(Sha256Digest value, String label) {
        Objects.requireNonNull(value, label);
        if (value.isZero()) {
            throw new IllegalArgumentException(label + " is the zero digest");
        }
    }

    private static Sha256Digest hashSegments(List<CanonicalBytes> values) {
        try {
            MessageDigest sha = MessageDigest.getInstance("SHA-256");
            for (CanonicalBytes value : values) {
                sha.update(value.toByteArray());
            }
            return Sha256Digest.copyOf(sha.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("JDK has no SHA-256 provider", e);
        }
    }

    private static boolean equalSegments(List<CanonicalBytes> sources, List<CanonicalBytes> payloads) {
        long sourceLength = sources.stream().mapToLong(CanonicalBytes::length).sum();
        long payloadLength = payloads.stream().mapToLong(CanonicalBytes::length).sum();
        if (sourceLength != payloadLength) {
            return false;
        }
        int sourcePart = 0;
        int payloadPart = 0;
        int sourceOffset = 0;
        int payloadOffset = 0;
        byte[] source = new byte[0];
        byte[] payload = new byte[0];
        long remaining = sourceLength;
        while (remaining > 0) {
            if (sourceOffset == source.length) {
                source = sources.get(sourcePart++).toByteArray();
                sourceOffset = 0;
            }
            if (payloadOffset == payload.length) {
                payload = payloads.get(payloadPart++).toByteArray();
                payloadOffset = 0;
            }
            int compared = Math.min(source.length - sourceOffset, payload.length - payloadOffset);
            if (Arrays.mismatch(
                            source,
                            sourceOffset,
                            sourceOffset + compared,
                            payload,
                            payloadOffset,
                            payloadOffset + compared)
                    >= 0) {
                return false;
            }
            sourceOffset += compared;
            payloadOffset += compared;
            remaining -= compared;
        }
        return true;
    }
}
