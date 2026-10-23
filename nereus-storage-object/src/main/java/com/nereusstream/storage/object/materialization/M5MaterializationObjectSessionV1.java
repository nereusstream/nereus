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
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.GenerationObject;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationPlan;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.RepresentationMode;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceExtent;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceKind;
import com.nereusstream.storage.object.materialization.M5MaterializationValidatorV1.MaterializationDataReader;
import com.nereusstream.storage.object.materialization.M5MaterializationValidatorV1.VerifiedObjectRead;
import com.nereusstream.storage.object.provider.C1ObjectProviderSession;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.provider.ProviderObjectOutcome;
import com.nereusstream.storage.object.provider.ProviderObjectResult;
import com.nereusstream.storage.object.provider.ProviderReconciliationResult;
import com.nereusstream.storage.object.provider.RepeatableObjectBody;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** Cell-scoped deterministic output creation and response-loss convergence over the existing C1 session. */
public final class M5MaterializationObjectSessionV1 {
    public record Candidate(GenerationObject descriptor, CanonicalBytes canonicalBody) {
        public Candidate {
            Objects.requireNonNull(descriptor, "descriptor");
            canonicalBody = CanonicalBytes.copyOf(
                    Objects.requireNonNull(canonicalBody, "canonicalBody").toByteArray());
            if (canonicalBody.length() != descriptor.identity().bodyLength()
                    || !Sha256Digest.hash(canonicalBody)
                            .equals(descriptor.identity().bodySha256())) {
                throw new IllegalArgumentException("M5 output candidate body identity differs");
            }
        }
    }

    public record CreationResult(List<GenerationObject> exactObjects, int createdOrAdopted, int reconciledUnknowns) {
        public CreationResult {
            exactObjects = List.copyOf(Objects.requireNonNull(exactObjects, "exactObjects"));
            if (createdOrAdopted != exactObjects.size() || reconciledUnknowns < 0) {
                throw new IllegalArgumentException("M5 creation summary is inconsistent");
            }
        }
    }

    private final C1ObjectProviderSession provider;
    private final int maximumListPages;
    private final long maximumListKeys;
    private final long maximumListKeyBytes;
    private final int maximumSingleKeyBytes;

    public M5MaterializationObjectSessionV1(
            C1ObjectProviderSession provider,
            int maximumListPages,
            long maximumListKeys,
            long maximumListKeyBytes,
            int maximumSingleKeyBytes) {
        this.provider = Objects.requireNonNull(provider, "provider");
        if (maximumListPages <= 0 || maximumListKeys <= 0 || maximumListKeyBytes <= 0 || maximumSingleKeyBytes <= 0) {
            throw new IllegalArgumentException("M5 reconciliation bounds must be positive");
        }
        this.maximumListPages = maximumListPages;
        this.maximumListKeys = maximumListKeys;
        this.maximumListKeyBytes = maximumListKeyBytes;
        this.maximumSingleKeyBytes = maximumSingleKeyBytes;
    }

    public CreationResult createExact(MaterializationPlan plan, List<Candidate> candidates) throws IOException {
        Objects.requireNonNull(plan, "plan");
        M5MaterializationCodecV1.encodePlan(plan);
        requireProviderScope(plan);
        candidates = List.copyOf(Objects.requireNonNull(candidates, "candidates"));
        List<Candidate> sorted = candidates.stream()
                .sorted(Comparator.comparingInt(value -> value.descriptor().ordinal()))
                .toList();
        if (candidates.isEmpty()
                || !candidates.equals(sorted)
                || candidates.stream()
                                .map(value -> value.descriptor().identity().key())
                                .distinct()
                                .count()
                        != candidates.size()) {
            throw new IllegalArgumentException("M5 output candidates are empty, reordered, or duplicate");
        }
        for (Candidate candidate : candidates) {
            GenerationObject descriptor = candidate.descriptor();
            if (descriptor.payload()) {
                requirePayloadCandidate(plan, descriptor);
            } else {
                if (descriptor.ordinal() >= plan.indexes().size()) {
                    throw new IllegalArgumentException("M5 index candidate is outside the frozen plan");
                }
                var index = plan.indexes().get(descriptor.ordinal());
                if (descriptor.indexKind() != index.kind()
                        || !descriptor.coverage().equals(index.coverage())
                        || !descriptor.identity().key().equals(index.objectKey())) {
                    throw new IllegalArgumentException("M5 index candidate differs from the frozen plan");
                }
            }
        }
        List<GenerationObject> exact = new ArrayList<>(candidates.size());
        int reconciled = 0;
        for (Candidate candidate : candidates) {
            var created = createObject(candidate.descriptor(), new Body(candidate));
            exact.add(created.descriptor());
            reconciled += created.reconciled() ? 1 : 0;
        }
        return new CreationResult(exact, exact.size(), reconciled);
    }

    /** Streams owned Pulsar parts through C1. Native Task/Cell/writer admission remains the caller's prerequisite. */
    public CreationResult createStagedExact(MaterializationPlan plan, List<M5PulsarStagedPartV2> parts)
            throws IOException {
        M5MaterializationCodecV1.encodePlan(plan);
        requireProviderScope(plan);
        if (!plan.sourceCut().coverage().typedPulsar()
                || plan.representationMode() != RepresentationMode.REWRITE_GENERATION) {
            throw new IllegalArgumentException("M5 staged creation requires a typed Pulsar rewrite plan");
        }
        parts = List.copyOf(parts);
        int previous = -1;
        for (var part : parts) {
            var descriptor = part.descriptor();
            if (descriptor.ordinal() <= previous) {
                throw new IllegalArgumentException("M5 staged candidates are reordered or duplicate");
            }
            requirePayloadCandidate(plan, descriptor);
            previous = descriptor.ordinal();
        }
        if (parts.isEmpty()) {
            throw new IllegalArgumentException("M5 staged candidates are empty");
        }
        List<GenerationObject> exact = new ArrayList<>(parts.size());
        int reconciled = 0;
        for (var part : parts) {
            var created = createObject(part.descriptor(), part);
            exact.add(created.descriptor());
            reconciled += created.reconciled() ? 1 : 0;
        }
        return new CreationResult(exact, exact.size(), reconciled);
    }

    private static void requirePayloadCandidate(MaterializationPlan plan, GenerationObject descriptor) {
        if (descriptor.ordinal() >= plan.outputParts().size()) {
            throw new IllegalArgumentException("M5 payload candidate is outside the frozen plan");
        }
        var part = plan.outputParts().get(descriptor.ordinal());
        if (!descriptor.coverage().equals(part.coverage())
                || !descriptor.identity().key().equals(part.objectKey())) {
            throw new IllegalArgumentException("M5 payload candidate differs from the frozen plan");
        }
    }

    private CreatedObject createObject(GenerationObject descriptor, RepeatableObjectBody body) throws IOException {
        ProviderObjectResult result = provider.conditionalCreate(body);
        boolean reconciled = result.outcome() == ProviderObjectOutcome.OUTCOME_UNKNOWN;
        if (reconciled) {
            ProviderReconciliationResult reconciliation = provider.reconcileUnknown(
                    descriptor.identity(),
                    leafPrefix(descriptor.identity().key()),
                    maximumListPages,
                    maximumListKeys,
                    maximumListKeyBytes,
                    maximumSingleKeyBytes);
            result = reconciliation.objectResult();
        }
        if (result.outcome() != ProviderObjectOutcome.APPLIED_EXACT
                && result.outcome() != ProviderObjectOutcome.EXISTING_EXACT) {
            throw new IllegalStateException(
                    "M5 deterministic Object create did not converge exactly: " + result.outcome());
        }
        return new CreatedObject(
                new GenerationObject(
                        descriptor.ordinal(),
                        descriptor.indexKind(),
                        descriptor.coverage(),
                        descriptor.identity(),
                        result.versionToken()),
                reconciled);
    }

    private record CreatedObject(GenerationObject descriptor, boolean reconciled) {}

    public M5MaterializationValidatorV1.VerifiedObjectRead readExact(ObjectIdentity identity) throws IOException {
        C1ObjectProviderSession.VerifiedObjectRead read = provider.readVerifiedObjectWithVersion(identity);
        return new M5MaterializationValidatorV1.VerifiedObjectRead(read.canonicalBody(), read.immutableVersionToken());
    }

    /** Output verification uses this exact C1 session and an independently qualified original source cursor. */
    public M5MaterializationValidatorV1.PulsarStreamedDataReader dataReaderForPulsarRewrite(
            MaterializationPlan plan, M5BytePreservingMaterializerV1.SourceReader originals) {
        M5MaterializationCodecV1.encodePlan(plan);
        requireProviderScope(plan);
        Objects.requireNonNull(originals, "originals");
        if (!plan.sourceCut().coverage().typedPulsar()
                || plan.representationMode() != RepresentationMode.REWRITE_GENERATION) {
            throw new IllegalArgumentException("Pulsar streamed reader requires its exact rewrite plan");
        }
        return new M5MaterializationValidatorV1.PulsarStreamedDataReader() {
            @Override
            public Nms1CodecV1.VerifiedPulsarPart readPulsarPart(
                    MaterializationPlan actualPlan, GenerationObject descriptor) throws IOException {
                if (!actualPlan.equals(plan) || !descriptor.payload()) {
                    throw new IOException("Pulsar streamed reader received a different frozen plan/part");
                }
                requirePayloadCandidate(plan, descriptor);
                var source = plan.sourceCut().sources().get(descriptor.ordinal());
                try (var entries = originals.openPulsarEntries(source)) {
                    return provider.consumeVerifiedObject(
                                    descriptor.identity(),
                                    descriptor.immutableProviderVersionToken(),
                                    input -> Nms1CodecV1.verifyPulsarPart(plan, descriptor.ordinal(), input, entries))
                            .value();
                }
            }

            @Override
            public VerifiedObjectRead readPulsarIndex(GenerationObject descriptor) throws IOException {
                if (descriptor.payload()
                        || descriptor.ordinal() >= plan.indexes().size()) {
                    throw new IOException("Pulsar streamed reader received an unplanned index");
                }
                var index = plan.indexes().get(descriptor.ordinal());
                if (descriptor.indexKind() != index.kind()
                        || !descriptor.coverage().equals(index.coverage())
                        || !descriptor.identity().key().equals(index.objectKey())
                        || descriptor.identity().bodyLength() > Nms1ObjectV1.MAX_INDEX_BYTES) {
                    throw new IOException("Pulsar streamed index differs from its exact plan/cap");
                }
                var read = provider.consumeVerifiedObject(
                        descriptor.identity(),
                        descriptor.immutableProviderVersionToken(),
                        input -> CanonicalBytes.copyOf(input.readAllBytes()));
                return new VerifiedObjectRead(read.value(), read.immutableVersionToken());
            }

            @Override
            public VerifiedObjectRead readObject(ObjectIdentity identity) throws IOException {
                if (plan.indexes().stream().noneMatch(index -> index.objectKey().equals(identity.key()))) {
                    throw new IOException("Pulsar streamed reader only buffers planned indexes");
                }
                return readExact(identity);
            }

            @Override
            public CanonicalBytes readSource(SourceExtent source) throws IOException {
                throw new IOException("Pulsar streamed source must use original ledger/entry positions");
            }

            @Override
            public void verifyPulsarSource(SourceExtent source) throws IOException {
                originals.verifyPulsarSource(source);
            }

            @Override
            public M5PulsarEntriesV2.Cursor openPulsarEntries(SourceExtent source) throws IOException {
                return originals.openPulsarEntries(source);
            }
        };
    }

    /** Binds native Object reuse validation to this Cell's exact Provider scope and planned keys. */
    public MaterializationDataReader dataReaderForReuse(MaterializationPlan plan) {
        Objects.requireNonNull(plan, "plan");
        M5MaterializationCodecV1.encodePlan(plan);
        if (plan.representationMode() == RepresentationMode.REWRITE_GENERATION
                || plan.sourceCut().sources().stream()
                        .anyMatch(source -> source.kind() == SourceKind.BOOKKEEPER_LEDGER
                                || source.kind() == SourceKind.KAFKA_BK_COMPACTED_GENERATION_V2)) {
            throw new IllegalArgumentException(
                    "M5 native Object reuse reader requires an Object-backed reference plan");
        }
        requireProviderScope(plan);
        return new MaterializationDataReader() {
            @Override
            public VerifiedObjectRead readObject(ObjectIdentity identity) throws IOException {
                Objects.requireNonNull(identity, "identity");
                boolean plannedOutput = plan.outputParts().stream()
                                .anyMatch(part -> part.objectKey().equals(identity.key()))
                        || plan.indexes().stream()
                                .anyMatch(index -> index.objectKey().equals(identity.key()));
                if (!plannedOutput) {
                    throw new IllegalArgumentException("M5 Object read key is outside the frozen plan");
                }
                return readExact(identity);
            }

            @Override
            public CanonicalBytes readSource(SourceExtent source) {
                throw new IllegalStateException("M5 rewrite source requires its native decoder");
            }
        };
    }

    private void requireProviderScope(MaterializationPlan plan) {
        if (!plan.sourceCut()
                .identity()
                .providerScopeSha256()
                .equals(provider.providerScopeId().digest())) {
            throw new IllegalArgumentException("M5 plan Provider scope differs from the Cell session");
        }
    }

    private static String leafPrefix(String key) {
        int slash = key.lastIndexOf('/');
        if (slash <= 0) {
            throw new IllegalArgumentException("M5 Object key has no deterministic parent prefix");
        }
        return key.substring(0, slash + 1);
    }

    private record Body(Candidate candidate) implements RepeatableObjectBody {
        @Override
        public ObjectIdentity identity() {
            return candidate.descriptor().identity();
        }

        @Override
        public InputStream openStream() {
            return new ByteArrayInputStream(candidate.canonicalBody().toByteArray());
        }
    }
}
