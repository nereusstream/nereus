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
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
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
        Sha256Digest fallbackSet = requireFallbackSources(plan, exactFallbackSources);
        payloadObjects = sortedGenerationObjects(payloadObjects, true);
        indexObjects = sortedGenerationObjects(indexObjects, false);
        requireOutputShape(plan, payloadObjects, indexObjects);

        List<CanonicalBytes> sourceBodies = readAndValidateSources(plan, reader);
        List<CanonicalBytes> payloadBodies = readAndValidatePayloads(plan, payloadObjects, reader);
        List<CanonicalBytes> indexBodies = readAndValidateIndexes(plan, indexObjects, reader);
        requireBytePreservingSemantics(plan, sourceBodies, payloadBodies);

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
                hashObjects("M5-A-VALIDATED-OBJECTS-V1", payloadObjects, indexObjects),
                hashCoverage("M5-A-COVERAGE-V1", payloadObjects, indexObjects),
                hashLookupBoundaries(indexBodies),
                hashBodies("M5-A-BYTE-EQUALITY-V1", sourceBodies, payloadBodies),
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

    private static Sha256Digest requireFallbackSources(
            MaterializationPlan plan, List<SourceProtectionIdentity> sources) {
        sources = List.copyOf(sources);
        List<SourceProtectionIdentity> sorted = sources.stream()
                .sorted(Comparator.comparing(
                        value -> value.sourceIdentitySha256().toHex()))
                .toList();
        if (sources.isEmpty()
                || !sources.equals(sorted)
                || sources.stream().distinct().count() != sources.size()
                || sources.stream().anyMatch(value -> !value.capability()
                        .equals(plan.sourceCut().identity().capability()))) {
            throw new IllegalArgumentException("M5 fallback protections are empty, non-canonical, or capability-stale");
        }
        List<Sha256Digest> protectedIdentities = sources.stream()
                .map(SourceProtectionIdentity::sourceIdentitySha256)
                .toList();
        List<Sha256Digest> cutIdentities = plan.sourceCut().sources().stream()
                .map(SourceExtent::sourceIdentitySha256)
                .sorted(Comparator.comparing(Sha256Digest::toHex))
                .toList();
        if (!protectedIdentities.equals(cutIdentities)) {
            throw new IllegalArgumentException("M5 fallback protections differ from the frozen source identities");
        }
        Sha256Digest calculated = M4ReadControlCodecV1.calculateFallbackSetSha256(sources);
        Optional<Sha256Digest> existing = plan.sourceCut().predecessorSelector().fallbackSetSha256();
        if (existing.isPresent() && !existing.orElseThrow().equals(calculated)) {
            throw new IllegalArgumentException("M5 fallback protection membership differs from the predecessor");
        }
        return calculated;
    }

    private static List<CanonicalBytes> readAndValidateSources(
            MaterializationPlan plan, MaterializationDataReader reader) throws IOException {
        List<CanonicalBytes> result = new ArrayList<>(plan.sourceCut().sources().size());
        for (SourceExtent source : plan.sourceCut().sources()) {
            CanonicalBytes body =
                    CanonicalBytes.copyOf(reader.readSource(source).toByteArray());
            if (body.length() != source.canonicalLength()
                    || !Sha256Digest.hash(body).equals(source.bodySha256())) {
                throw new IllegalStateException("M5 source body identity differs: " + source.physicalKey());
            }
            result.add(body);
        }
        return List.copyOf(result);
    }

    private static List<CanonicalBytes> readAndValidatePayloads(
            MaterializationPlan plan, List<GenerationObject> objects, MaterializationDataReader reader)
            throws IOException {
        List<CanonicalBytes> result = new ArrayList<>(objects.size());
        for (int index = 0; index < objects.size(); index++) {
            GenerationObject object = objects.get(index);
            OutputPartPlan part = plan.outputParts().get(index);
            VerifiedObjectRead read = reader.readObject(object.identity());
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
                        || !object.identity().bodySha256().equals(source.bodySha256())) {
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
        CanonicalBytes sourceBytes = concatenate(sources);
        CanonicalBytes payloadBytes = concatenate(payloads);
        if (!sourceBytes.equals(payloadBytes)) {
            throw new IllegalStateException("M5-A byte-preserving payload differs from the frozen source cut");
        }
    }

    private static void requireLookupBoundaries(M5LookupIndexV1 index) {
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
        List<String> values = indexBodies.stream().map(CanonicalBytes::toHex).toList();
        if (values.isEmpty()) {
            values = List.of("NO_NEW_INDEX_OBJECTS");
        }
        return hash("M5-A-LOOKUP-BOUNDARIES-V1", values);
    }

    private static Sha256Digest hashBodies(String domain, List<CanonicalBytes> sources, List<CanonicalBytes> payloads) {
        return hash(
                domain,
                List.of(
                        Sha256Digest.hash(concatenate(sources)).toHex(),
                        Sha256Digest.hash(concatenate(payloads)).toHex()));
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

    private static CanonicalBytes concatenate(List<CanonicalBytes> values) {
        try {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            for (CanonicalBytes value : values) {
                output.write(value.toByteArray());
            }
            return CanonicalBytes.copyOf(output.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory M5 byte comparison failed", impossible);
        }
    }
}
