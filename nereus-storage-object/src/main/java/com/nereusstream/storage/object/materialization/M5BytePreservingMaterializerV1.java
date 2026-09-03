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
import com.nereusstream.storage.object.materialization.M5MaterializationObjectSessionV1.Candidate;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.GenerationObject;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexPlan;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationPlan;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.OutputPartPlan;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.RepresentationMode;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceExtent;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Builds deterministic byte-preserving payload/index Objects; Kafka semantic deletion is deliberately absent. */
public final class M5BytePreservingMaterializerV1 {
    @FunctionalInterface
    public interface SourceReader {
        CanonicalBytes readExact(SourceExtent source) throws IOException;
    }

    public record BuildOutput(
            List<GenerationObject> payloadObjects,
            List<GenerationObject> indexObjects,
            List<Candidate> payloadCandidates,
            List<Candidate> indexCandidates) {
        public BuildOutput {
            payloadObjects = List.copyOf(Objects.requireNonNull(payloadObjects, "payloadObjects"));
            indexObjects = List.copyOf(Objects.requireNonNull(indexObjects, "indexObjects"));
            payloadCandidates = List.copyOf(Objects.requireNonNull(payloadCandidates, "payloadCandidates"));
            indexCandidates = List.copyOf(Objects.requireNonNull(indexCandidates, "indexCandidates"));
            if (payloadObjects.isEmpty()
                    || payloadCandidates.stream()
                            .anyMatch(value -> !value.descriptor().payload())
                    || indexCandidates.stream()
                            .anyMatch(value -> value.descriptor().payload())) {
                throw new IllegalArgumentException("M5 byte-preserving build output shape is invalid");
            }
        }
    }

    public BuildOutput build(MaterializationPlan plan, SourceReader reader) throws IOException {
        Objects.requireNonNull(plan, "plan");
        Objects.requireNonNull(reader, "reader");
        M5MaterializationCodecV1.encodePlan(plan);
        if (plan.representationMode() == RepresentationMode.REFERENCE_REUSE
                && !plan.indexes().isEmpty()) {
            throw new IllegalArgumentException("reference reuse cannot generate replacement indexes");
        }
        List<CanonicalBytes> sources =
                new ArrayList<>(plan.sourceCut().sources().size());
        for (SourceExtent source : plan.sourceCut().sources()) {
            CanonicalBytes body = CanonicalBytes.copyOf(reader.readExact(source).toByteArray());
            if (body.length() != source.canonicalLength()
                    || !Sha256Digest.hash(body).equals(source.bodySha256())) {
                throw new IllegalStateException(
                        "materializer source length or digest differs: " + source.physicalKey());
            }
            sources.add(body);
        }

        List<GenerationObject> payloadObjects = new ArrayList<>(sources.size());
        List<Candidate> payloadCandidates = new ArrayList<>();
        for (int ordinal = 0; ordinal < sources.size(); ordinal++) {
            SourceExtent source = plan.sourceCut().sources().get(ordinal);
            OutputPartPlan part = plan.outputParts().get(ordinal);
            if (plan.representationMode() == RepresentationMode.REWRITE_GENERATION) {
                Nms1ObjectV1 object = new Nms1ObjectV1(
                        plan.sourceCut().identity(),
                        plan.payloadKind(),
                        plan.taskIdSha256(),
                        plan.outputIdentitySha256(),
                        part.coverage(),
                        ordinal,
                        sources.size(),
                        plan.encryptionGenerationSha256(),
                        plan.compressionPolicySha256(),
                        plan.checksumPolicySha256(),
                        List.of(new Nms1ObjectV1.SourceContribution(
                                source.sourceIdentitySha256(), source.coverage(), source.bodySha256())),
                        List.of(new Nms1ObjectV1.ExtentRow(
                                source.coverage(),
                                0,
                                sources.get(ordinal).length(),
                                source.recordCount(),
                                source.minimumTimestamp(),
                                source.maximumTimestamp(),
                                source.bodySha256(),
                                0)),
                        sources.get(ordinal),
                        List.of());
                CanonicalBytes body = Nms1CodecV1.encode(object);
                GenerationObject descriptor = descriptor(ordinal, null, part.coverage(), part.objectKey(), body);
                payloadObjects.add(descriptor);
                payloadCandidates.add(new Candidate(descriptor, body));
            } else {
                ObjectIdentity identity =
                        new ObjectIdentity(part.objectKey(), source.canonicalLength(), source.bodySha256());
                payloadObjects.add(new GenerationObject(
                        ordinal, null, part.coverage(), identity, source.immutableProviderVersionToken()));
            }
        }

        List<GenerationObject> indexObjects = new ArrayList<>(plan.indexes().size());
        List<Candidate> indexCandidates = new ArrayList<>(plan.indexes().size());
        for (int ordinal = 0; ordinal < plan.indexes().size(); ordinal++) {
            IndexPlan indexPlan = plan.indexes().get(ordinal);
            List<M5LookupIndexV1.Row> rows = new ArrayList<>(sources.size());
            for (int sourceOrdinal = 0; sourceOrdinal < sources.size(); sourceOrdinal++) {
                SourceExtent source = plan.sourceCut().sources().get(sourceOrdinal);
                rows.add(new M5LookupIndexV1.Row(
                        source.coverage(),
                        sourceOrdinal,
                        0,
                        sources.get(sourceOrdinal).length(),
                        source.minimumTimestamp(),
                        source.maximumTimestamp(),
                        source.bodySha256()));
            }
            M5LookupIndexV1 index = new M5LookupIndexV1(
                    indexPlan.kind(), indexPlan.coverage(), plan.taskIdSha256(), plan.outputIdentitySha256(), rows);
            CanonicalBytes body = index.encode();
            GenerationObject descriptor =
                    descriptor(ordinal, indexPlan.kind(), indexPlan.coverage(), indexPlan.objectKey(), body);
            indexObjects.add(descriptor);
            indexCandidates.add(new Candidate(descriptor, body));
        }
        return new BuildOutput(payloadObjects, indexObjects, payloadCandidates, indexCandidates);
    }

    private static GenerationObject descriptor(
            int ordinal,
            M5MaterializationRecordsV1.IndexKind kind,
            M5MaterializationRecordsV1.ProtocolCoverage coverage,
            String key,
            CanonicalBytes body) {
        return new GenerationObject(
                ordinal,
                kind,
                coverage,
                new ObjectIdentity(key, body.length(), Sha256Digest.hash(body)),
                Optional.empty());
    }
}
