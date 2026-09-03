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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.materialization.M5MaterializationPlannerV1.PlanningPolicy;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.GenerationObject;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IdentityEnvelope;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexKind;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationPlan;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationSourceCut;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PayloadKind;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.ProtocolCoverage;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PublicationOutcome;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.RepresentationMode;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceExtent;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceKind;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.TaskState;
import com.nereusstream.storage.object.materialization.M5MaterializationValidatorV1.MaterializationDataReader;
import com.nereusstream.storage.object.materialization.M5MaterializationValidatorV1.ValidatedGeneration;
import com.nereusstream.storage.object.materialization.M5MaterializationValidatorV1.VerifiedObjectRead;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlCoordinatorV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.AdmissionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityEvidence;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityKind;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ProtectionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtection;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class M5MaterializationV1Test {
    @Test
    void deterministicPlannerSelectsReuseIndexOnlyAndRewriteWithoutCopyingHealthyObjectWal() {
        Fixture fixture = new Fixture(SourceKind.OBJECT_WAL_NWG1, true, true);
        MaterializationPlan reuse = fixture.plan(false, List.of());
        assertThat(reuse.representationMode()).isEqualTo(RepresentationMode.REFERENCE_REUSE);
        assertThat(reuse.payloadKind()).isEqualTo(PayloadKind.NATIVE_EXTENT_REFERENCE_V1);
        assertThat(reuse.outputParts()).singleElement().satisfies(part -> assertThat(part.objectKey())
                .isEqualTo(fixture.source.physicalKey()));

        Fixture missingIndex = new Fixture(SourceKind.OBJECT_WAL_NWG1, true, false);
        MaterializationPlan indexOnly = missingIndex.plan(false, List.of(IndexKind.OFFSET_OR_POSITION));
        assertThat(indexOnly.representationMode()).isEqualTo(RepresentationMode.INDEX_ONLY_GENERATION);
        assertThat(indexOnly.outputParts()).singleElement().satisfies(part -> assertThat(part.objectKey())
                .isEqualTo(missingIndex.source.physicalKey()));
        assertThat(indexOnly.indexes()).singleElement().satisfies(index -> assertThat(index.kind())
                .isEqualTo(IndexKind.OFFSET_OR_POSITION));

        Fixture bookKeeper = new Fixture(SourceKind.BOOKKEEPER_LEDGER, false, false);
        MaterializationPlan rewritten = bookKeeper.plan(false, List.of(IndexKind.OFFSET_OR_POSITION));
        assertThat(rewritten.representationMode()).isEqualTo(RepresentationMode.REWRITE_GENERATION);
        assertThat(rewritten.payloadKind()).isEqualTo(PayloadKind.KAFKA_BATCH_PRESERVING_V1);
        assertThat(rewritten.outputParts().get(0).objectKey()).contains("/m5/materialization/");

        assertThat(fixture.plan(false, List.of()).taskIdSha256())
                .isEqualTo(fixture.plan(false, List.of()).taskIdSha256());
    }

    @Test
    void controlAndNms1CodecsRoundTripAndRejectTrailingOrMutatedBytes() {
        Fixture fixture = new Fixture(SourceKind.BOOKKEEPER_LEDGER, false, false);
        MaterializationPlan plan = fixture.plan(true, List.of(IndexKind.OFFSET_OR_POSITION));
        assertThat(M5MaterializationCodecV1.decodeSourceCut(M5MaterializationCodecV1.encodeSourceCut(fixture.cut)))
                .isEqualTo(fixture.cut);
        assertThat(M5MaterializationCodecV1.decodePlan(M5MaterializationCodecV1.encodePlan(plan)))
                .isEqualTo(plan);

        Nms1ObjectV1 object = fixture.nms1(plan);
        CanonicalBytes encoded = Nms1CodecV1.encode(object);
        assertThat(Nms1CodecV1.decode(encoded)).isEqualTo(object);
        byte[] trailing = Arrays.copyOf(encoded.toByteArray(), encoded.length() + 1);
        assertThatThrownBy(() -> Nms1CodecV1.decode(CanonicalBytes.copyOf(trailing)))
                .isInstanceOf(IllegalArgumentException.class);
        byte[] mutated = encoded.toByteArray();
        mutated[mutated.length / 2] ^= 1;
        assertThatThrownBy(() -> Nms1CodecV1.decode(CanonicalBytes.copyOf(mutated)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void bytePreservingMaterializerBuildsExactRewriteIndexAndReuseOutputs() throws Exception {
        Fixture rewritten = new Fixture(SourceKind.BOOKKEEPER_LEDGER, false, false);
        MaterializationPlan rewritePlan =
                rewritten.plan(true, List.of(IndexKind.OFFSET_OR_POSITION, IndexKind.TIMESTAMP));
        M5BytePreservingMaterializerV1.BuildOutput rewrite =
                new M5BytePreservingMaterializerV1().build(rewritePlan, source -> rewritten.sourceBody);
        assertThat(rewrite.payloadCandidates()).hasSize(1);
        assertThat(rewrite.indexCandidates()).hasSize(2);
        assertThat(Nms1CodecV1.decode(rewrite.payloadCandidates().get(0).canonicalBody())
                        .payload())
                .isEqualTo(rewritten.sourceBody);
        assertThat(rewrite.indexCandidates())
                .extracting(candidate ->
                        M5LookupIndexV1.decode(candidate.canonicalBody()).kind())
                .containsExactly(IndexKind.OFFSET_OR_POSITION, IndexKind.TIMESTAMP);

        Fixture reused = new Fixture(SourceKind.OBJECT_WAL_NWG1, true, true);
        M5BytePreservingMaterializerV1.BuildOutput reuse =
                new M5BytePreservingMaterializerV1().build(reused.plan(false, List.of()), source -> reused.sourceBody);
        assertThat(reuse.payloadCandidates()).isEmpty();
        assertThat(reuse.indexCandidates()).isEmpty();
        assertThat(reuse.payloadObjects()).singleElement().satisfies(object -> {
            assertThat(object.identity().key()).isEqualTo(reused.source.physicalKey());
            assertThat(object.identity().bodySha256()).isEqualTo(reused.source.bodySha256());
        });
    }

    @Test
    void lookupIndexUsesFloorCoverageThenSuccessorAcrossSparseGaps() {
        Fixture fixture = new Fixture(SourceKind.BOOKKEEPER_LEDGER, false, false);
        MaterializationPlan plan = fixture.plan(true, List.of(IndexKind.OFFSET_OR_POSITION));
        M5LookupIndexV1 index = new M5LookupIndexV1(
                IndexKind.OFFSET_OR_POSITION,
                fixture.coverage,
                plan.taskIdSha256(),
                plan.outputIdentitySha256(),
                List.of(
                        new M5LookupIndexV1.Row(
                                new ProtocolCoverage(PositionDomain.KAFKA_OFFSET, 10, 12),
                                0,
                                0,
                                2,
                                100,
                                101,
                                digest("row-a")),
                        new M5LookupIndexV1.Row(
                                new ProtocolCoverage(PositionDomain.KAFKA_OFFSET, 15, 20),
                                0,
                                2,
                                5,
                                105,
                                109,
                                digest("row-b"))));
        assertThat(M5LookupIndexV1.decode(index.encode())).isEqualTo(index);
        assertThat(index.lookup(10)).contains(index.rows().get(0));
        assertThat(index.lookup(12)).contains(index.rows().get(1));
        assertThat(index.lookup(19)).contains(index.rows().get(1));
    }

    @Test
    void persistedCellAdmissionReservesBeforeDispatchAndFailsClosedAtEveryCap() {
        Fixture fixture = new Fixture(SourceKind.BOOKKEEPER_LEDGER, false, false);
        MaterializationPlan plan = fixture.plan(true, List.of(IndexKind.OFFSET_OR_POSITION));
        M5MaterializationAdmissionV1 admission =
                new M5MaterializationAdmissionV1(fixture.store, fixture.envelope.protocolCellSha256());
        M5MaterializationAdmissionV1.Caps caps = new M5MaterializationAdmissionV1.Caps(1, 10, 1_000_000, 1, 1, 1, 1);
        assertThat(admission.install(caps)).isEqualTo(M5MaterializationAdmissionV1.Outcome.APPLIED_EXACT);
        assertThat(admission.reserve(plan, 1000, 1)).isEqualTo(M5MaterializationAdmissionV1.Outcome.APPLIED_EXACT);
        assertThat(admission.reserve(plan, 1000, 1)).isEqualTo(M5MaterializationAdmissionV1.Outcome.EXISTING_EXACT);
        assertThat(M5MaterializationAdmissionV1.decode(
                        M5MaterializationAdmissionV1.encode(admission.read().orElseThrow())))
                .isEqualTo(admission.read().orElseThrow());

        Fixture second = new Fixture(SourceKind.OBJECT_WAL_NWG1, true, true);
        MaterializationPlan other = second.plan(false, List.of());
        assertThat(admission.reserve(other, 1, 1)).isEqualTo(M5MaterializationAdmissionV1.Outcome.REJECTED_CAP);
        assertThat(admission.release(plan.taskIdSha256()))
                .isEqualTo(M5MaterializationAdmissionV1.Outcome.APPLIED_EXACT);
        assertThat(admission.read().orElseThrow().reservations()).isEmpty();
    }

    @Test
    void fullValidationAndSelectorPublicationAreExactIdempotentAndResponseLossSafe() throws Exception {
        Fixture fixture = new Fixture(SourceKind.BOOKKEEPER_LEDGER, false, false);
        MaterializationPlan plan = fixture.plan(true, List.of(IndexKind.OFFSET_OR_POSITION));
        fixture.installM4();
        Output output = fixture.output(plan);
        ValidatedGeneration validated = fixture.validate(plan, output);
        M5MaterializationCoordinatorV1 coordinator =
                new M5MaterializationCoordinatorV1(fixture.store, 7, fixture.binding);

        assertThat(coordinator.register(plan)).isEqualTo(PublicationOutcome.APPLIED_EXACT);
        fixture.store.applyButUnknownOn("/read-m4/", "/selector");
        assertThat(coordinator.publish(plan, validated, List.of(fixture.protection)))
                .isEqualTo(PublicationOutcome.EXISTING_EXACT);
        BindingReadSelector selected = fixture.m4.readSelector().orElseThrow();
        assertThat(selected.selectedViewSha256())
                .isEqualTo(M5MaterializationCodecV1.manifestSha256(validated.manifestView()));
        assertThat(selected.mode()).isEqualTo(SelectorMode.PREFERRED_WITH_FALLBACK);
        assertThat(selected.sourceGeneration()).isEqualTo(8);
        assertThat(coordinator.readTask(plan.taskIdSha256()).orElseThrow().state())
                .isEqualTo(TaskState.PUBLISHED);

        assertThat(coordinator.publish(plan, validated, List.of(fixture.protection)))
                .isEqualTo(PublicationOutcome.EXISTING_EXACT);
    }

    @Test
    void validationRejectsStaleAuthorityCorruptPayloadIndexAndWrongFallbackMembership() throws Exception {
        Fixture fixture = new Fixture(SourceKind.BOOKKEEPER_LEDGER, false, false);
        MaterializationPlan plan = fixture.plan(true, List.of(IndexKind.OFFSET_OR_POSITION));
        Output output = fixture.output(plan);
        BindingReadSelector stale = new BindingReadSelector(
                fixture.binding,
                fixture.selector.selectedViewSha256(),
                2,
                fixture.selector.readAdmissionEpoch(),
                fixture.selector.sourceGeneration(),
                fixture.selector.mode(),
                fixture.selector.admissionState(),
                fixture.selector.fallbackSetSha256(),
                fixture.selector.capability(),
                fixture.selector.pendingAnchors(),
                fixture.selector.activeBatches());
        assertThatThrownBy(() -> fixture.validator.validate(
                        plan,
                        stale,
                        List.of(fixture.protection),
                        output.payloadObjects,
                        output.indexObjects,
                        output.reader))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("stale");

        Map<String, CanonicalBytes> corrupt = new LinkedHashMap<>(output.reader.objects);
        corrupt.put(output.indexObjects.get(0).identity().key(), bytes("corrupt-index"));
        DataReader corruptReader = new DataReader(corrupt, fixture.sourceBody, fixture.source);
        assertThatThrownBy(() -> fixture.validator.validate(
                        plan,
                        fixture.selector,
                        List.of(fixture.protection),
                        output.payloadObjects,
                        output.indexObjects,
                        corruptReader))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("length or full-body");

        SourceProtectionIdentity wrong =
                new SourceProtectionIdentity(digest("other-source"), 1, 1, 7, fixture.capability);
        assertThatThrownBy(() -> fixture.validator.validate(
                        plan,
                        fixture.selector,
                        List.of(wrong),
                        output.payloadObjects,
                        output.indexObjects,
                        output.reader))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("source identities");
    }

    private static final class Fixture {
        private final Store store = new Store();
        private final BindingIdentity binding = new BindingIdentity(
                new TopicBindingId(digest("binding")), digest("incarnation"), digest("storage-epoch"));
        private final CapabilityEvidence capabilityEvidence = new CapabilityEvidence(
                binding,
                1,
                1,
                CapabilityKind.DURABLE_DRAIN_ONLY_V1,
                CapabilityState.ADMITTED,
                digest("adapter"),
                digest("backend-config"),
                digest("read-contract"),
                digest("verifier"),
                digest("receipt-id"),
                digest("receipt"),
                digest("authority-time"),
                10_000,
                0,
                0);
        private final CapabilityBinding capability =
                new CapabilityBinding(1, M4ReadControlCodecV1.capabilityEvidenceSha256(capabilityEvidence));
        private final ProtocolCoverage coverage = new ProtocolCoverage(PositionDomain.KAFKA_OFFSET, 10, 20);
        private final CanonicalBytes sourceBody = bytes("0123456789");
        private final SourceExtent source;
        private final SourceProtectionIdentity protection;
        private final BindingReadSelector selector;
        private final IdentityEnvelope envelope;
        private final MaterializationSourceCut cut;
        private final M5MaterializationValidatorV1 validator = new M5MaterializationValidatorV1();
        private final M4ReadControlCoordinatorV1 m4 = new M4ReadControlCoordinatorV1(store, 7, binding);

        private Fixture(SourceKind kind, boolean reusable, boolean indexed) {
            Sha256Digest sourceIdentity = digest("source-" + kind.name());
            source = new SourceExtent(
                    kind,
                    sourceIdentity,
                    coverage,
                    "cell-a/source/000001",
                    sourceBody.length(),
                    10,
                    100,
                    109,
                    Sha256Digest.hash(sourceBody),
                    Optional.of(bytes("provider-v1")),
                    kind == SourceKind.BOOKKEEPER_LEDGER ? Optional.of(digest("ledger-identity")) : Optional.empty(),
                    digest("format-root"),
                    digest("encryption-policy"),
                    reusable,
                    indexed,
                    List.of(binding.bindingId().digest()));
            protection = new SourceProtectionIdentity(sourceIdentity, 1, 1, 7, capability);
            selector = new BindingReadSelector(
                    binding,
                    digest("predecessor-view"),
                    1,
                    1,
                    7,
                    SelectorMode.PREFERRED_ONLY,
                    AdmissionState.ADMITTING,
                    Optional.empty(),
                    capability,
                    List.of(),
                    List.of());
            envelope = new IdentityEnvelope(digest("cell-a"), digest("provider-scope-a"), binding, 1, 1, 1, capability);
            Sha256Digest sourceSet = M5MaterializationCodecV1.calculateSourceSetSha256(List.of(source));
            cut = new MaterializationSourceCut(
                    envelope,
                    selector,
                    Sha256Digest.hash(M4ReadControlCodecV1.encodeSelector(selector)),
                    selector.selectedViewSha256(),
                    coverage,
                    20,
                    20,
                    20,
                    20,
                    10,
                    digest("protocol-state"),
                    digest("recovery"),
                    digest("materialization-policy"),
                    digest("output-policy"),
                    sourceSet,
                    List.of(source));
        }

        private MaterializationPlan plan(boolean forceRewrite, List<IndexKind> indexes) {
            return new M5MaterializationPlannerV1()
                    .plan(
                            cut,
                            new PlanningPolicy(
                                    PayloadKind.KAFKA_BATCH_PRESERVING_V1,
                                    forceRewrite,
                                    digest("encryption-generation"),
                                    digest("compression"),
                                    digest("checksum"),
                                    indexes,
                                    "cell-a"));
        }

        private Nms1ObjectV1 nms1(MaterializationPlan plan) {
            return new Nms1ObjectV1(
                    envelope,
                    plan.payloadKind(),
                    plan.taskIdSha256(),
                    plan.outputIdentitySha256(),
                    coverage,
                    0,
                    1,
                    plan.encryptionGenerationSha256(),
                    plan.compressionPolicySha256(),
                    plan.checksumPolicySha256(),
                    List.of(new Nms1ObjectV1.SourceContribution(
                            source.sourceIdentitySha256(), coverage, Sha256Digest.hash(sourceBody))),
                    List.of(new Nms1ObjectV1.ExtentRow(
                            coverage, 0, sourceBody.length(), 10, 100, 109, Sha256Digest.hash(sourceBody), 0)),
                    sourceBody,
                    List.of());
        }

        private Output output(MaterializationPlan plan) {
            CanonicalBytes payload = plan.representationMode() == RepresentationMode.REWRITE_GENERATION
                    ? Nms1CodecV1.encode(nms1(plan))
                    : sourceBody;
            GenerationObject payloadObject = new GenerationObject(
                    0,
                    null,
                    coverage,
                    new ObjectIdentity(
                            plan.outputParts().get(0).objectKey(), payload.length(), Sha256Digest.hash(payload)),
                    Optional.of(bytes("provider-v2")));
            Map<String, CanonicalBytes> objects = new LinkedHashMap<>();
            objects.put(payloadObject.identity().key(), payload);
            List<GenerationObject> indexObjects;
            if (plan.indexes().isEmpty()) {
                indexObjects = List.of();
            } else {
                M5LookupIndexV1 index = new M5LookupIndexV1(
                        plan.indexes().get(0).kind(),
                        coverage,
                        plan.taskIdSha256(),
                        plan.outputIdentitySha256(),
                        List.of(new M5LookupIndexV1.Row(
                                coverage, 0, 0, sourceBody.length(), 100, 109, Sha256Digest.hash(sourceBody))));
                CanonicalBytes indexBytes = index.encode();
                GenerationObject indexObject = new GenerationObject(
                        0,
                        plan.indexes().get(0).kind(),
                        coverage,
                        new ObjectIdentity(
                                plan.indexes().get(0).objectKey(), indexBytes.length(), Sha256Digest.hash(indexBytes)),
                        Optional.of(bytes("provider-v2")));
                objects.put(indexObject.identity().key(), indexBytes);
                indexObjects = List.of(indexObject);
            }
            return new Output(List.of(payloadObject), indexObjects, new DataReader(objects, sourceBody, source));
        }

        private ValidatedGeneration validate(MaterializationPlan plan, Output output) throws Exception {
            return validator.validate(
                    plan, selector, List.of(protection), output.payloadObjects, output.indexObjects, output.reader);
        }

        private void installM4() {
            assertThat(m4.createCapability(capabilityEvidence)).isEqualTo(M4ReadControlCoordinatorV1.Outcome.APPLIED);
            assertThat(m4.createProtection(new SourceProtection(
                            binding, protection, ProtectionState.PROTECTED, Optional.empty(), Optional.empty())))
                    .isEqualTo(M4ReadControlCoordinatorV1.Outcome.APPLIED);
            assertThat(m4.createSelector(selector)).isEqualTo(M4ReadControlCoordinatorV1.Outcome.APPLIED);
        }
    }

    private record Output(
            List<GenerationObject> payloadObjects, List<GenerationObject> indexObjects, DataReader reader) {}

    private static final class DataReader implements MaterializationDataReader {
        private final Map<String, CanonicalBytes> objects;
        private final CanonicalBytes sourceBody;
        private final SourceExtent source;

        private DataReader(Map<String, CanonicalBytes> objects, CanonicalBytes sourceBody, SourceExtent source) {
            this.objects = Map.copyOf(objects);
            this.sourceBody = sourceBody;
            this.source = source;
        }

        @Override
        public VerifiedObjectRead readObject(ObjectIdentity identity) {
            CanonicalBytes body = objects.get(identity.key());
            if (body == null) {
                throw new IllegalStateException("missing test Object");
            }
            return new VerifiedObjectRead(body, Optional.of(bytes("provider-v2")));
        }

        @Override
        public CanonicalBytes readSource(SourceExtent expected) {
            assertThat(expected).isEqualTo(source);
            return sourceBody;
        }
    }

    private static final class Store implements CanonicalControlMetadataStore {
        private final Map<String, CanonicalBytes> values = new LinkedHashMap<>();
        private String unknownContainsA;
        private String unknownContainsB;

        @Override
        public Optional<CanonicalBytes> get(String key) {
            return Optional.ofNullable(values.get(key));
        }

        @Override
        public ControlMutationOutcome putIfAbsent(String key, CanonicalBytes exactValue) {
            CanonicalBytes previous = values.putIfAbsent(key, exactValue);
            return previous == null ? ControlMutationOutcome.APPLIED : ControlMutationOutcome.DEFINITIVE_CONFLICT;
        }

        @Override
        public ControlMutationOutcome compareAndSet(
                String key, Optional<CanonicalBytes> exactExpected, CanonicalBytes exactCandidate) {
            Optional<CanonicalBytes> actual = Optional.ofNullable(values.get(key));
            if (!actual.equals(exactExpected)) {
                return ControlMutationOutcome.DEFINITIVE_CONFLICT;
            }
            values.put(key, exactCandidate);
            if (unknownContainsA != null && key.contains(unknownContainsA) && key.contains(unknownContainsB)) {
                unknownContainsA = null;
                unknownContainsB = null;
                return ControlMutationOutcome.RESPONSE_UNKNOWN;
            }
            return ControlMutationOutcome.APPLIED;
        }

        private void applyButUnknownOn(String containsA, String containsB) {
            unknownContainsA = containsA;
            unknownContainsB = containsB;
        }
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(bytes(value));
    }

    private static CanonicalBytes bytes(String value) {
        return CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8));
    }
}
