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
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.BindingManifestView;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.GenerationObject;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IdentityEnvelope;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexKind;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationSourceCut;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializedGeneration;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PayloadKind;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.ProtocolCoverage;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.RepresentationMode;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceExtent;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceKind;
import com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.EntryRange;
import com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.Position;
import com.nereusstream.storage.object.materialization.M5PulsarFrontiersV2.LedgerBoundary;
import com.nereusstream.storage.object.provider.C1ObjectProviderSession;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.provider.ObjectProviderCapabilities;
import com.nereusstream.storage.object.provider.ObjectProviderTransport;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.AdmissionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Declared-value/wire regression only; the fixtures provide no actual Native source or publication qualification. */
class M5PulsarMaterializationCodecV2Test {
    @Test
    void recoveryInputRoundTripsExactNativeMetadataAndRejectsWrongKeyAndTruncation() throws Exception {
        var binding = rewritePlan().sourceCut().identity().binding();
        var namespace = new com.nereusstream.storage.api.lifecycle.MetadataNamespaceIdentityV2(
                new com.nereusstream.domain.identity.Id128(1, 2), 4);
        var bytes = CanonicalUtf8.fromString("declared-native-metadata").bytes();
        var input = new M5PulsarRecoveryInputV2(
                namespace, binding, "tenant/ns/topic", "/managed-ledgers/tenant/ns/topic", 7, bytes);
        assertThat(M5PulsarRecoveryInputV2.decode(input.encode())).isEqualTo(input);
        assertThat(new M5PulsarRecoveryInputV2(
                                namespace, binding, input.managedLedgerName(), input.metadataKey(), 8, bytes)
                        .sha256())
                .isNotEqualTo(input.sha256());
        assertThatThrownBy(() -> new M5PulsarRecoveryInputV2(
                        namespace, binding, input.managedLedgerName(), "/managed-ledgers/foreign", 7, bytes))
                .hasMessageContaining("key/name/version");
        var encoded = input.encode().toByteArray();
        assertThatThrownBy(() -> M5PulsarRecoveryInputV2.decode(
                        CanonicalBytes.copyOf(Arrays.copyOf(encoded, encoded.length - 1))))
                .hasMessageContaining("truncated");
        assertThatThrownBy(() -> M5PulsarRecoveryInputV2.decode(
                        CanonicalBytes.copyOf(Arrays.copyOf(encoded, encoded.length + 1))))
                .hasMessageContaining("canonical");
    }

    @Test
    void streamedVerifierMatchesCanonicalPartsAndIndependentEntryLocators() throws Exception {
        var plan = rewritePlan();
        var expected = new M5BytePreservingMaterializerV1().build(plan, new DeclaredEntryReader());
        var allRows = M5LookupIndexV1.decode(expected.indexCandidates().get(0).canonicalBody())
                .rows();
        for (var candidate : expected.payloadCandidates()) {
            int ordinal = candidate.descriptor().ordinal();
            try (var entries = new DeclaredEntryReader()
                    .openPulsarEntries(plan.sourceCut().sources().get(ordinal))) {
                var verified = Nms1CodecV1.verifyPulsarPart(
                        plan,
                        ordinal,
                        new ByteArrayInputStream(candidate.canonicalBody().toByteArray()),
                        entries);
                assertThat(verified.body().bytes())
                        .isEqualTo(candidate.descriptor().identity().bodyLength());
                assertThat(verified.body().sha256())
                        .isEqualTo(candidate.descriptor().identity().bodySha256());
                assertThat(verified.locators())
                        .containsExactlyElementsOf(allRows.stream()
                                .filter(row -> row.objectOrdinal() == ordinal)
                                .toList());
            }
        }
    }

    @Test
    void streamedVerifierRejectsChangedPayloadFooterEofAndOriginalSource() throws Exception {
        var plan = rewritePlan();
        var output = new M5BytePreservingMaterializerV1().build(plan, new DeclaredEntryReader());
        byte[] canonical = output.payloadCandidates().get(0).canonicalBody().toByteArray();
        byte[] payload = canonical.clone();
        payload[payload.length - Nms1CodecV1.FOOTER_BYTES - 5] ^= 1;
        byte[] footer = canonical.clone();
        footer[footer.length - Nms1CodecV1.FOOTER_BYTES + 8] ^= 1;
        for (byte[] changed : List.of(payload, footer, Arrays.copyOf(canonical, canonical.length + 1))) {
            try (var entries = new DeclaredEntryReader()
                    .openPulsarEntries(plan.sourceCut().sources().get(0))) {
                assertThatThrownBy(
                                () -> Nms1CodecV1.verifyPulsarPart(plan, 0, new ByteArrayInputStream(changed), entries))
                        .isInstanceOf(IOException.class);
            }
        }
        var changedSource = new DeclaredEntryReader();
        changedSource.changeFirstEntry = true;
        try (var entries =
                changedSource.openPulsarEntries(plan.sourceCut().sources().get(0))) {
            assertThatThrownBy(
                            () -> Nms1CodecV1.verifyPulsarPart(plan, 0, new ByteArrayInputStream(canonical), entries))
                    .hasMessageContaining("entry position/length");
        }
    }

    @Test
    void locatedStreamedReadPreservesPositionsTimestampsAndZeroLengthEntries() throws Exception {
        var plan = rewritePlan();
        var output = new M5BytePreservingMaterializerV1().build(plan, new DeclaredEntryReader());
        for (var candidate : output.payloadCandidates()) {
            int ordinal = candidate.descriptor().ordinal();
            try (var entries = new DeclaredEntryReader()
                    .openPulsarEntries(plan.sourceCut().sources().get(ordinal))) {
                for (var entry = entries.next(); entry.isPresent(); entry = entries.next()) {
                    var expected = entry.orElseThrow();
                    var read = Nms1CodecV1.readPulsarEntry(
                            plan,
                            ordinal,
                            new ByteArrayInputStream(candidate.canonicalBody().toByteArray()),
                            expected.position(),
                            1024);
                    assertThat(read.entry()).isEqualTo(expected);
                    assertThat(read.body().bytes())
                            .isEqualTo(candidate.descriptor().identity().bodyLength());
                    assertThat(read.body().sha256())
                            .isEqualTo(candidate.descriptor().identity().bodySha256());
                }
            }
        }
    }

    @Test
    void locatedStreamedReadRejectsUnrequestedCorruptionOversizeAndIncompleteBody() throws Exception {
        var plan = rewritePlan();
        var output = new M5BytePreservingMaterializerV1().build(plan, new DeclaredEntryReader());
        var first = output.payloadCandidates().get(0).canonicalBody().toByteArray();
        var firstPosition = plan.outputParts()
                .get(0)
                .coverage()
                .pulsarCoverage()
                .orElseThrow()
                .firstPosition();
        assertThatThrownBy(
                        () -> Nms1CodecV1.readPulsarEntry(plan, 0, new ByteArrayInputStream(first), firstPosition, 4))
                .hasMessageContaining("admitted byte cap");
        var second = output.payloadCandidates().get(1).canonicalBody().toByteArray();
        var emptyPosition = plan.outputParts()
                .get(1)
                .coverage()
                .pulsarCoverage()
                .orElseThrow()
                .firstPosition();
        var corrupt = second.clone();
        corrupt[corrupt.length - Nms1CodecV1.FOOTER_BYTES - 1] ^= 1;
        assertThatThrownBy(() ->
                        Nms1CodecV1.readPulsarEntry(plan, 1, new ByteArrayInputStream(corrupt), emptyPosition, 1024))
                .hasMessageContaining("entry bytes differ");
        for (var changed :
                List.of(Arrays.copyOf(second, second.length - 1), Arrays.copyOf(second, second.length + 1))) {
            assertThatThrownBy(() -> Nms1CodecV1.readPulsarEntry(
                            plan, 1, new ByteArrayInputStream(changed), emptyPosition, 1024))
                    .isInstanceOf(IOException.class);
        }
        var unread = new ByteArrayInputStream(second);
        assertThatThrownBy(() -> Nms1CodecV1.readPulsarEntry(plan, 1, unread, new Position(7, 1), 1024))
                .hasMessageContaining("outside its exact planned part");
        assertThat(unread.available()).isEqualTo(second.length);
    }

    @Test
    void nms1SourceCannotEnterLegacyScalarCoverage() throws Exception {
        var original = rewritePlan().sourceCut().sources().get(0);
        assertThatThrownBy(() -> new SourceExtent(
                        SourceKind.PULSAR_NMS1_DATA_V2,
                        original.sourceIdentitySha256(),
                        new ProtocolCoverage(M5MaterializationRecordsV1.PositionDomain.KAFKA_OFFSET, 0, 1),
                        "declared-nms1",
                        original.canonicalLength(),
                        1,
                        1,
                        1,
                        original.bodySha256(),
                        Optional.of(CanonicalBytes.copyOf(new byte[] {1})),
                        Optional.empty(),
                        original.formatRootSha256(),
                        original.encryptionPolicySha256(),
                        true,
                        false,
                        original.memberBindingIds()))
                .hasMessageContaining("exact typed single-Binding");
    }

    @Test
    void stagingRejectsReferencePlansBeforeSourceIo(@TempDir Path root) throws Exception {
        var original = rewritePlan().sourceCut();
        for (var kind : List.of(SourceKind.PULSAR_NPD1_DATA, SourceKind.PULSAR_NMS1_DATA_V2)) {
            for (boolean existingIndexes : List.of(true, false)) {
                var sources = original.sources().stream()
                        .map(source -> new SourceExtent(
                                kind,
                                source.sourceIdentitySha256(),
                                source.coverage(),
                                source.physicalKey(),
                                source.canonicalLength(),
                                source.recordCount(),
                                source.minimumTimestamp(),
                                source.maximumTimestamp(),
                                source.bodySha256(),
                                Optional.of(CanonicalBytes.copyOf(new byte[] {1})),
                                Optional.empty(),
                                source.formatRootSha256(),
                                source.encryptionPolicySha256(),
                                true,
                                existingIndexes,
                                source.memberBindingIds(),
                                source.pulsarEntryRootSha256()))
                        .toList();
                var cut = new MaterializationSourceCut(
                        original.identity(),
                        original.predecessorSelector(),
                        original.predecessorSelectorValueSha256(),
                        original.predecessorViewSha256(),
                        original.coverage(),
                        -1,
                        -1,
                        -1,
                        -1,
                        -1,
                        original.protocolStateRootSha256(),
                        original.recoveryCheckpointRootSha256(),
                        original.materializationPolicySha256(),
                        original.outputFormatPolicySha256(),
                        M5MaterializationCodecV1.calculateSourceSetSha256(sources),
                        sources,
                        original.pulsarFrontiers());
                var plan = new M5MaterializationPlannerV1()
                        .plan(
                                cut,
                                new M5MaterializationPlannerV1.PlanningPolicy(
                                        PayloadKind.PULSAR_ENTRY_PRESERVING_V1,
                                        false,
                                        digest("encryption"),
                                        digest("compression"),
                                        digest("checksum"),
                                        List.of(IndexKind.OFFSET_OR_POSITION),
                                        "pulsar-cell"));
                assertThat(M5MaterializationCodecV1.decodePlan(M5MaterializationCodecV1.encodePlan(plan)))
                        .isEqualTo(plan);
                assertThat(plan.representationMode())
                        .isEqualTo(
                                existingIndexes
                                        ? RepresentationMode.REFERENCE_REUSE
                                        : RepresentationMode.INDEX_ONLY_GENERATION);
                assertThatThrownBy(() -> M5PulsarStagedPartV2.stage(
                                plan,
                                0,
                                source -> {
                                    throw new AssertionError("Reference staging must reject before source I/O");
                                },
                                root,
                                1_000_000))
                        .hasMessageContaining("rewrite plan");
            }
            try (var files = Files.list(root)) {
                assertThat(files.toList()).isEmpty();
            }
        }
    }

    @Test
    void stagedProviderPathChecksAllPartsBeforePutAndStreamsExactBodies(@TempDir Path root) throws Exception {
        var plan = rewritePlan();
        var transport = new DeclaredOutputTransport();
        try (var first = M5PulsarStagedPartV2.stage(plan, 0, new DeclaredEntryReader(), root, 1_000_000);
                var second = M5PulsarStagedPartV2.stage(plan, 1, new DeclaredEntryReader(), root, 1_000_000);
                var provider = new C1ObjectProviderSession(
                        transport, new CellProviderScopeId(digest("scope")), "pulsar-cell", 1_000_000, 4096)) {
            var session = new M5MaterializationObjectSessionV1(provider, 2, 4, 4096, 1024);
            assertThatThrownBy(() -> session.createStagedExact(plan, List.of(second, first)))
                    .hasMessageContaining("reordered or duplicate");
            assertThatThrownBy(() -> session.createStagedExact(plan, List.of(first, first)))
                    .hasMessageContaining("reordered or duplicate");
            assertThat(transport.objects).isEmpty();
            var created = session.createStagedExact(plan, List.of(first, second));
            assertThat(created.createdOrAdopted()).isEqualTo(2);
            var expected = new M5BytePreservingMaterializerV1().build(plan, new DeclaredEntryReader());
            for (var candidate : expected.payloadCandidates()) {
                assertThat(transport.objects.get(
                                candidate.descriptor().identity().key()))
                        .isEqualTo(candidate.canonicalBody());
            }
            assertThat(session.createStagedExact(plan, List.of(first, second)).exactObjects())
                    .isEqualTo(created.exactObjects());
            var indexObjects =
                    session.createExact(plan, expected.indexCandidates()).exactObjects();
            var fallback = plan.sourceCut().sources().stream()
                    .map(source -> new SourceProtectionIdentity(
                            source.sourceIdentitySha256(),
                            1,
                            2,
                            1,
                            plan.sourceCut().identity().capability()))
                    .sorted(Comparator.comparing(
                            value -> value.sourceIdentitySha256().toHex()))
                    .toList();
            var streamed = new M5MaterializationValidatorV1()
                    .validate(
                            plan,
                            plan.sourceCut().predecessorSelector(),
                            fallback,
                            created.exactObjects(),
                            indexObjects,
                            session.dataReaderForPulsarRewrite(plan, new DeclaredEntryReader()));
            var bufferedReader = new DeclaredEntryReader();
            bufferedReader.objects.putAll(transport.objects);
            var buffered = new M5MaterializationValidatorV1()
                    .validate(
                            plan,
                            plan.sourceCut().predecessorSelector(),
                            fallback,
                            created.exactObjects(),
                            indexObjects,
                            bufferedReader);
            assertThat(streamed).isEqualTo(buffered);
            var validator = new M5MaterializationValidatorV1();
            var originalProof = validator.verifyPulsarOutputs(
                    plan,
                    created.exactObjects(),
                    indexObjects,
                    session.dataReaderForPulsarRewrite(plan, new DeclaredEntryReader()));
            assertThat(validator.finalizePulsarOutputs(
                            originalProof, plan.sourceCut().predecessorSelector(), fallback))
                    .isEqualTo(streamed);
            assertThatThrownBy(() -> validator.finalizePulsarOutputs(
                            originalProof, plan.sourceCut().predecessorSelector(), List.of()))
                    .hasMessageContaining("protections");
        }
    }

    @Test
    void stagedPartsMatchCanonicalProjectionAndRetainOpenReaders(@TempDir Path root) throws Exception {
        var plan = rewritePlan();
        var expected = new M5BytePreservingMaterializerV1().build(plan, new DeclaredEntryReader());
        for (int ordinal = 0; ordinal < plan.outputParts().size(); ordinal++) {
            var staged = M5PulsarStagedPartV2.stage(plan, ordinal, new DeclaredEntryReader(), root, 1_000_000);
            assertThat(staged.descriptor())
                    .isEqualTo(expected.payloadCandidates().get(ordinal).descriptor());
            for (int read = 0; read < 2; read++) {
                try (var stream = staged.openStream()) {
                    assertThat(CanonicalBytes.copyOf(stream.readAllBytes()))
                            .isEqualTo(expected.payloadCandidates().get(ordinal).canonicalBody());
                }
            }
            try (var held = staged.openStream()) {
                assertThatThrownBy(staged::close).hasMessageContaining("undrained original readers");
                assertThatThrownBy(staged::openStream).hasMessageContaining("admission is closed");
                assertThat(held.readAllBytes())
                        .hasSize(Math.toIntExact(staged.identity().bodyLength()));
            }
            staged.close();
            staged.close();
        }
        try (var files = Files.list(root)) {
            assertThat(files.toList()).isEmpty();
        }
    }

    @Test
    void stagedFailureRemovesOnlyOwnedTemporaryFiles(@TempDir Path root) throws Exception {
        var sibling = root.resolve("unrelated");
        Files.writeString(sibling, "preserve");
        assertThatThrownBy(() -> M5PulsarStagedPartV2.stage(rewritePlan(), 0, new DeclaredEntryReader(), root, 1))
                .hasMessageContaining("staging exceeds");
        var changed = new DeclaredEntryReader();
        changed.changeFirstEntry = true;
        assertThatThrownBy(() -> M5PulsarStagedPartV2.stage(rewritePlan(), 0, changed, root, 1_000_000))
                .hasMessageContaining("entry root changed");
        try (var files = Files.list(root)) {
            assertThat(files.toList()).containsExactly(sibling);
        }
        assertThat(Files.readString(sibling)).isEqualTo("preserve");
    }

    @Test
    void streamedWriterRejectsChangedTruncatedOrTrailingPayload() throws Exception {
        var plan = rewritePlan();
        var expected = new M5BytePreservingMaterializerV1().build(plan, new DeclaredEntryReader());
        var object = Nms1CodecV1.decode(expected.payloadCandidates().get(0).canonicalBody());
        for (var changed : List.of("alph", "alpha-extra", "wrong")) {
            assertThatThrownBy(() -> Nms1CodecV1.writePulsarPart(
                            plan,
                            0,
                            object.extents(),
                            object.payload().length(),
                            Sha256Digest.hash(object.payload()),
                            new ByteArrayInputStream(changed.getBytes(StandardCharsets.UTF_8)),
                            new ByteArrayOutputStream()))
                    .isInstanceOf(IOException.class);
        }
    }

    @Test
    void independentLookupGoldenPreservesEmptyEntryAndCrossLedgerSuccessor() throws Exception {
        var bytes = golden("m5-pulsar-lookup-v2.hex");
        var index = M5LookupIndexV1.decode(bytes);
        assertThat(index.encode()).isEqualTo(bytes);
        assertThat(Sha256Digest.hash(bytes).toHex())
                .isEqualTo("e8aa32cd6b5755e11dceb7e45ef2f91c7db2e1626ca3a3dd344f4fe182fad68b");
        assertThat(index.coverage().entryCount()).isEqualTo(3);
        assertThat(index.rows()).hasSize(3);
        assertThat(index.lookup(new Position(9, 2))).contains(index.rows().get(1));
        assertThat(index.lookup(new Position(3, 0)).orElseThrow().byteLength()).isZero();
        assertThat(index.lookup(new Position(3, 1))).contains(index.rows().get(2));
        assertThat(index.lookup(new Position(7, 0))).isEmpty();
        assertThatThrownBy(() -> index.lookup(0)).hasMessageContaining("ledger-keyed position");
        assertThatThrownBy(() -> index.coverage().inclusiveStart()).hasMessageContaining("ledger-keyed positions");
    }

    @Test
    void typedIndexRejectsBeforeCutWithoutLosingInsideCutSuccessor() throws Exception {
        var index = M5LookupIndexV1.decode(golden("m5-pulsar-lookup-v2.hex"));
        assertThat(index.lookup(new Position(9, 0))).isEmpty();
        assertThat(index.lookup(new Position(9, 2))).contains(index.rows().get(1));
        assertThat(index.lookup(new Position(3, 2))).isEmpty();
    }

    @Test
    void typedEmbeddedIndexMustParseItsActualVersionAndBody() throws Exception {
        var object = Nms1CodecV1.decode(golden("m5-pulsar-nms1-v2.hex"));
        var scalar = new ProtocolCoverage(M5MaterializationRecordsV1.PositionDomain.KAFKA_OFFSET, 0, 1);
        var v1 = new M5LookupIndexV1(
                        IndexKind.OFFSET_OR_POSITION,
                        scalar,
                        object.taskIdSha256(),
                        object.outputIdentitySha256(),
                        List.of(new M5LookupIndexV1.Row(scalar, 0, 0, 1, 1, 1, digest("unit"))))
                .encode();
        for (var body : List.of(v1, CanonicalBytes.copyOf(new byte[] {1}))) {
            assertThatThrownBy(() -> new Nms1ObjectV1.IndexSection(
                            IndexKind.OFFSET_OR_POSITION, object.coverage(), 2, body, Sha256Digest.hash(body)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void unexpectedStructurallyValidEmbeddedIndexCannotValidateRewritePlan() throws Exception {
        var plan = rewritePlan();
        var output = new M5BytePreservingMaterializerV1().build(plan, new DeclaredEntryReader());
        var reader = new DeclaredEntryReader();
        var descriptors = new ArrayList<>(output.payloadObjects());
        for (var candidate : output.payloadCandidates()) {
            var object = Nms1CodecV1.decode(candidate.canonicalBody());
            if (object.partOrdinal() == 0) {
                var indexBody = new M5LookupIndexV1(
                                IndexKind.OFFSET_OR_POSITION,
                                object.coverage(),
                                object.taskIdSha256(),
                                object.outputIdentitySha256(),
                                List.of(new M5LookupIndexV1.Row(
                                        object.coverage(),
                                        0,
                                        999,
                                        object.payload().length(),
                                        42,
                                        42,
                                        Sha256Digest.hash(object.payload()))))
                        .encode();
                var index = new Nms1ObjectV1.IndexSection(
                        IndexKind.OFFSET_OR_POSITION, object.coverage(), 2, indexBody, Sha256Digest.hash(indexBody));
                var changed = new Nms1ObjectV1(
                        object.identity(),
                        object.payloadKind(),
                        object.taskIdSha256(),
                        object.outputIdentitySha256(),
                        object.coverage(),
                        object.partOrdinal(),
                        object.partCount(),
                        object.encryptionGenerationSha256(),
                        object.compressionPolicySha256(),
                        object.checksumPolicySha256(),
                        object.sources(),
                        object.extents(),
                        object.payload(),
                        List.of(index));
                var body = Nms1CodecV1.encode(changed);
                var old = candidate.descriptor();
                descriptors.set(
                        0,
                        new GenerationObject(
                                0,
                                null,
                                old.coverage(),
                                new ObjectIdentity(old.identity().key(), body.length(), Sha256Digest.hash(body)),
                                Optional.empty()));
                reader.objects.put(old.identity().key(), body);
            } else {
                reader.objects.put(candidate.descriptor().identity().key(), candidate.canonicalBody());
            }
        }
        for (var candidate : output.indexCandidates()) {
            reader.objects.put(candidate.descriptor().identity().key(), candidate.canonicalBody());
        }
        var fallback = plan.sourceCut().sources().stream()
                .map(source -> new SourceProtectionIdentity(
                        source.sourceIdentitySha256(),
                        1,
                        2,
                        1,
                        plan.sourceCut().identity().capability()))
                .sorted(Comparator.comparing(
                        value -> value.sourceIdentitySha256().toHex()))
                .toList();
        assertThatThrownBy(() -> new M5MaterializationValidatorV1()
                        .validate(
                                plan,
                                plan.sourceCut().predecessorSelector(),
                                fallback,
                                descriptors,
                                output.indexObjects(),
                                reader))
                .hasMessageContaining("embedded indexes");
    }

    @Test
    void independentNmsGoldenCarriesThreeOriginalEntryBoundaries() throws Exception {
        var bytes = golden("m5-pulsar-nms1-v2.hex");
        var object = Nms1CodecV1.decode(bytes);
        assertThat(Nms1CodecV1.encode(object)).isEqualTo(bytes);
        assertThat(Sha256Digest.hash(bytes).toHex())
                .isEqualTo("db030ae235463d6a7aa7c84e773802d308255ab1b0832a5bb503c1cd639d0651");
        assertThat(object.coverage().entryCount()).isEqualTo(3);
        assertThat(object.extents()).hasSize(3);
        assertThat(object.extents().get(1).recordCount()).isEqualTo(1);
        assertThat(object.extents().get(1).payloadLength()).isZero();
        assertThat(object.payload())
                .isEqualTo(CanonicalUtf8.fromString("alphabeta").bytes());
        for (var section : object.indexes()) {
            assertThat(section.parserVersion()).isEqualTo(2);
            var index = M5LookupIndexV1.decode(section.body());
            assertThat(index.coverage()).isEqualTo(object.coverage());
        }
    }

    @Test
    void rejectsMultiEntryBlobIncompleteIndexAndDifferentNestedChain() throws Exception {
        var object = Nms1CodecV1.decode(golden("m5-pulsar-nms1-v2.hex"));
        assertThatThrownBy(() -> new Nms1ObjectV1.ExtentRow(object.coverage(), 0, 9, 3, 42, 44, digest("body"), 0))
                .hasMessageContaining("one original entry");
        var index = M5LookupIndexV1.decode(golden("m5-pulsar-lookup-v2.hex"));
        assertThatThrownBy(() -> new M5LookupIndexV1(
                        index.kind(),
                        index.coverage(),
                        index.taskIdSha256(),
                        index.outputIdentitySha256(),
                        List.of(index.rows().get(0), index.rows().get(2))))
                .hasMessageContaining("overlaps or escapes coverage");
        var parent = object.coverage().pulsarCoverage().orElseThrow();
        var foreign =
                M5PulsarCoverageV2.of(parent.binding(), digest("foreign-authority"), parent.chain(), parent.ranges());
        var source = object.sources().get(0);
        var sources = new ArrayList<>(object.sources());
        sources.set(
                0,
                new Nms1ObjectV1.SourceContribution(
                        source.sourceIdentitySha256(),
                        ProtocolCoverage.pulsar(foreign),
                        source.contributedBytesSha256()));
        assertThatThrownBy(() -> new Nms1ObjectV1(
                        object.identity(),
                        object.payloadKind(),
                        object.taskIdSha256(),
                        object.outputIdentitySha256(),
                        object.coverage(),
                        0,
                        1,
                        object.encryptionGenerationSha256(),
                        object.compressionPolicySha256(),
                        object.checksumPolicySha256(),
                        sources,
                        object.extents(),
                        object.payload(),
                        object.indexes()))
                .hasMessageContaining("another coverage context");
    }

    @Test
    void rejectsMixedSectionVersionsAndTruncation() throws Exception {
        byte[] nms = golden("m5-pulsar-nms1-v2.hex").toByteArray();
        for (int magic : new int[] {
            Nms1CodecV1.HEADER_MAGIC,
            Nms1CodecV1.SOURCE_MAGIC,
            Nms1CodecV1.EXTENT_MAGIC,
            Nms1CodecV1.INDEX_MAGIC,
            Nms1CodecV1.FOOTER_MAGIC
        }) {
            byte[] changed = nms.clone();
            int offset = findMagic(changed, magic);
            ByteBuffer.wrap(changed).putInt(offset + 4, 1);
            assertThatThrownBy(() -> Nms1CodecV1.decode(CanonicalBytes.copyOf(changed)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        byte[] lookup = golden("m5-pulsar-lookup-v2.hex").toByteArray();
        for (int length = 0; length < lookup.length; length++) {
            byte[] truncated = Arrays.copyOf(lookup, length);
            assertThatThrownBy(() -> M5LookupIndexV1.decode(CanonicalBytes.copyOf(truncated)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> Nms1CodecV1.decode(CanonicalBytes.copyOf(Arrays.copyOf(nms, nms.length + 1))))
                .hasMessageContaining("trailing bytes");
    }

    @Test
    void sourceCutAndPlanCarryTypedFrontiersAndSourceEntryRoots() throws Exception {
        var coverage = Nms1CodecV1.decode(golden("m5-pulsar-nms1-v2.hex")).coverage();
        var cut = cut(coverage);
        var encoded = M5MaterializationCodecV1.encodeSourceCut(cut);
        assertThat(ByteBuffer.wrap(encoded.toByteArray()).getInt(4)).isEqualTo(2);
        assertThat(M5MaterializationCodecV1.decodeSourceCut(encoded)).isEqualTo(cut);
        assertThat(cut.durableFrontier()).isEqualTo(-1);
        var policy = new M5MaterializationPlannerV1.PlanningPolicy(
                PayloadKind.PULSAR_ENTRY_PRESERVING_V1,
                true,
                digest("encryption"),
                digest("compression"),
                digest("checksum"),
                List.of(IndexKind.OFFSET_OR_POSITION, IndexKind.CHECKSUM_COVERAGE),
                "pulsar-cell");
        var plan = new M5MaterializationPlannerV1().plan(cut, policy);
        assertThat(plan.indexes()).allMatch(value -> value.parserVersion() == 2);
        assertThat(M5MaterializationCodecV1.decodePlan(M5MaterializationCodecV1.encodePlan(plan)))
                .isEqualTo(plan);
        assertThatThrownBy(() -> new M5PulsarFrontiersV2(new LedgerBoundary(9, 1), Optional.empty(), Optional.empty())
                        .requireCut(coverage.pulsarCoverage().orElseThrow()))
                .hasMessageContaining("sealed cut");
        assertThatThrownBy(() -> new M5PulsarFrontiersV2(
                                new LedgerBoundary(7, 1), Optional.of(new LedgerBoundary(3, 1)), Optional.empty())
                        .requireCut(coverage.pulsarCoverage().orElseThrow()))
                .hasMessageContaining("sealed cut");
    }

    @Test
    void generationAndManifestKeepNativeChainWithoutNumericProjection() throws Exception {
        var object = Nms1CodecV1.decode(golden("m5-pulsar-nms1-v2.hex"));
        var payload = new GenerationObject(
                0,
                null,
                object.coverage(),
                new ObjectIdentity("pulsar-cell/payload", 3109, Nms1CodecV1.bodySha256(object)),
                Optional.empty());
        var generation = new MaterializedGeneration(
                object.identity(),
                RepresentationMode.REWRITE_GENERATION,
                PayloadKind.PULSAR_ENTRY_PRESERVING_V1,
                object.taskIdSha256(),
                object.outputIdentitySha256(),
                digest("source-set"),
                2,
                object.coverage(),
                digest("protocol"),
                Optional.empty(),
                digest("validation"),
                digest("predecessor"),
                Optional.of(digest("fallback")),
                List.of(payload),
                List.of());
        assertThat(M5MaterializationCodecV1.decodeGeneration(M5MaterializationCodecV1.encodeGeneration(generation)))
                .isEqualTo(generation);
        var manifest = new BindingManifestView(
                object.identity(),
                M5MaterializationCodecV1.generationSha256(generation),
                digest("predecessor"),
                digest("selector"),
                object.coverage(),
                Optional.empty());
        assertThat(M5MaterializationCodecV1.decodeManifest(M5MaterializationCodecV1.encodeManifest(manifest)))
                .isEqualTo(manifest);
    }

    @Test
    void declaredEntryRewriteAndIndependentRereadKeepEveryPositionAndLocator() throws Exception {
        var plan = rewritePlan();
        var builder = new DeclaredEntryReader();
        var output = new M5BytePreservingMaterializerV1().build(plan, builder);
        var independent = new DeclaredEntryReader();
        for (var candidate : output.payloadCandidates()) {
            independent.objects.put(candidate.descriptor().identity().key(), candidate.canonicalBody());
        }
        for (var candidate : output.indexCandidates()) {
            independent.objects.put(candidate.descriptor().identity().key(), candidate.canonicalBody());
        }
        var fallback = plan.sourceCut().sources().stream()
                .map(source -> new SourceProtectionIdentity(
                        source.sourceIdentitySha256(),
                        1,
                        2,
                        1,
                        plan.sourceCut().identity().capability()))
                .sorted(Comparator.comparing(
                        value -> value.sourceIdentitySha256().toHex()))
                .toList();
        var result = new M5MaterializationValidatorV1()
                .validate(
                        plan,
                        plan.sourceCut().predecessorSelector(),
                        fallback,
                        output.payloadObjects(),
                        output.indexObjects(),
                        independent);
        assertThat(result.generation().coverage()).isEqualTo(plan.sourceCut().coverage());
        var index = M5LookupIndexV1.decode(output.indexCandidates().get(0).canonicalBody());
        assertThat(index.rows()).hasSize(3);
        assertThat(index.lookup(new Position(9, 2)).orElseThrow().byteLength()).isZero();
        assertThat(index.lookup(new Position(3, 1)).orElseThrow().objectOrdinal())
                .isEqualTo(1);
        independent.changeFirstEntry = true;
        assertThatThrownBy(() -> new M5MaterializationValidatorV1()
                        .validate(
                                plan,
                                plan.sourceCut().predecessorSelector(),
                                fallback,
                                output.payloadObjects(),
                                output.indexObjects(),
                                independent))
                .hasMessageContaining("entry bytes/position/timestamp differ");
    }

    @Test
    void flatOnlyReaderCannotMaterializeTypedPulsarCut() throws Exception {
        var plan = rewritePlan();
        assertThatThrownBy(() -> new M5BytePreservingMaterializerV1().build(plan, source -> {
                    throw new AssertionError("flat reader must not be invoked for Pulsar");
                }))
                .hasMessageContaining("independent native/provider qualification");
    }

    @Test
    void incrementalEntryRootRejectsReorderingMissingAndLateExtraEntry() throws Exception {
        var scope = rewritePlan().sourceCut().coverage().pulsarCoverage().orElseThrow();
        var root = new M5PulsarEntriesV2.Commitment(scope);
        var first = new M5PulsarEntriesV2.Entry(
                new Position(9, 1), CanonicalUtf8.fromString("alpha").bytes(), 42);
        assertThatThrownBy(
                        () -> root.accept(new M5PulsarEntriesV2.Entry(new Position(3, 0), CanonicalBytes.empty(), 43)))
                .hasMessageContaining("exact captured cut");
        root.accept(first);
        assertThatThrownBy(root::finish).hasMessageContaining("incomplete");
        root.accept(new M5PulsarEntriesV2.Entry(new Position(3, 0), CanonicalBytes.empty(), 43));
        root.accept(new M5PulsarEntriesV2.Entry(
                new Position(3, 1), CanonicalUtf8.fromString("beta").bytes(), 44));
        assertThat(root.finish().isZero()).isFalse();
        assertThatThrownBy(() -> root.accept(first)).hasMessageContaining("exact captured cut");
    }

    private static M5MaterializationRecordsV1.MaterializationPlan rewritePlan() throws IOException {
        var coverage = Nms1CodecV1.decode(golden("m5-pulsar-nms1-v2.hex")).coverage();
        return new M5MaterializationPlannerV1()
                .plan(
                        cut(coverage),
                        new M5MaterializationPlannerV1.PlanningPolicy(
                                PayloadKind.PULSAR_ENTRY_PRESERVING_V1,
                                true,
                                digest("encryption"),
                                digest("compression"),
                                digest("checksum"),
                                List.of(IndexKind.OFFSET_OR_POSITION, IndexKind.CHECKSUM_COVERAGE),
                                "pulsar-cell"));
    }

    /** A declaration-only in-memory oracle, not a Native reader/source proof. */
    private static final class DeclaredEntryReader
            implements M5BytePreservingMaterializerV1.SourceReader,
                    M5MaterializationValidatorV1.MaterializationDataReader {
        private final Map<String, CanonicalBytes> objects = new HashMap<>();
        private boolean changeFirstEntry;

        @Override
        public CanonicalBytes readExact(SourceExtent source) {
            throw new AssertionError("Pulsar must not use a flat source body");
        }

        @Override
        public CanonicalBytes readSource(SourceExtent source) {
            throw new AssertionError("Pulsar must not use a flat source body");
        }

        @Override
        public M5MaterializationValidatorV1.VerifiedObjectRead readObject(ObjectIdentity identity) {
            return new M5MaterializationValidatorV1.VerifiedObjectRead(objects.get(identity.key()), Optional.empty());
        }

        @Override
        public void verifyPulsarSource(SourceExtent source) {
            // This oracle checks declared entry behavior only, never actual protocol/source qualification.
        }

        @Override
        public M5PulsarEntriesV2.Cursor openPulsarEntries(SourceExtent source) {
            var entries = source.coverage()
                                    .pulsarCoverage()
                                    .orElseThrow()
                                    .ranges()
                                    .get(0)
                                    .ledgerId()
                            == 9
                    ? List.of(new M5PulsarEntriesV2.Entry(
                            new Position(9, 1),
                            CanonicalUtf8.fromString(changeFirstEntry ? "wrong" : "alpha")
                                    .bytes(),
                            42))
                    : List.of(
                            new M5PulsarEntriesV2.Entry(new Position(3, 0), CanonicalBytes.empty(), 43),
                            new M5PulsarEntriesV2.Entry(
                                    new Position(3, 1),
                                    CanonicalUtf8.fromString("beta").bytes(),
                                    44));
            var iterator = entries.iterator();
            return new M5PulsarEntriesV2.Cursor() {
                @Override
                public Optional<M5PulsarEntriesV2.Entry> next() {
                    return iterator.hasNext() ? Optional.of(iterator.next()) : Optional.empty();
                }

                @Override
                public void close() {}
            };
        }
    }

    /** Declared local transport tests the library bridge only; it supplies no actual Provider admission proof. */
    private static final class DeclaredOutputTransport implements ObjectProviderTransport {
        private final Map<String, CanonicalBytes> objects = new HashMap<>();

        @Override
        public ObjectProviderCapabilities capabilities() {
            return new ObjectProviderCapabilities("declared-output", true, true, true, true, true, 1_000_000, 4096, 4);
        }

        @Override
        public ConditionalCreateResult putIfAbsent(ObjectIdentity identity, InputStream body) throws IOException {
            if (objects.containsKey(identity.key())) {
                return ConditionalCreateResult.ALREADY_EXISTS;
            }
            objects.put(identity.key(), CanonicalBytes.copyOf(body.readAllBytes()));
            return ConditionalCreateResult.CREATED;
        }

        @Override
        public StreamingObject get(String key, Optional<CanonicalBytes> exactVersionToken) throws IOException {
            var body = objects.get(key);
            if (body == null) {
                throw new IOException("undeclared output");
            }
            return new StreamingObject(
                    body.length(), 0, body.length(), Optional.empty(), new ByteArrayInputStream(body.toByteArray()));
        }

        @Override
        public StreamingObject getRange(String key, long start, long end, Optional<CanonicalBytes> version) {
            throw new AssertionError("No range GET is needed by exact creation");
        }

        @Override
        public ListPage list(String prefix, Optional<CanonicalBytes> continuation, int maximumKeys) {
            throw new AssertionError("No response loss is injected by this declaration-only case");
        }
    }

    private static MaterializationSourceCut cut(ProtocolCoverage coverage) {
        var scope = coverage.pulsarCoverage().orElseThrow();
        var capability = new CapabilityBinding(1, digest("cap"));
        var identity = new IdentityEnvelope(digest("cell"), digest("scope"), scope.binding(), 1, 1, 1, capability);
        var selector = new BindingReadSelector(
                scope.binding(),
                digest("predecessor"),
                1,
                1,
                1,
                SelectorMode.PREFERRED_ONLY,
                AdmissionState.ADMITTING,
                Optional.empty(),
                capability,
                List.of(),
                List.of());
        var sources = List.of(
                source(
                        scope.withRanges(List.of(new EntryRange(9, 1, 2))),
                        "9",
                        1,
                        "ee9cffa4a0102dff364a29b3283b650d66d6b43816290d53abcef084e5069d44"),
                source(
                        scope.withRanges(List.of(new EntryRange(3, 0, 2))),
                        "3",
                        2,
                        "a841d58e37f236055fcf92c272457fea89db06ff5322a3b4e2b63e9b039c2e46"));
        return new MaterializationSourceCut(
                identity,
                selector,
                Sha256Digest.hash(M4ReadControlCodecV1.encodeSelector(selector)),
                selector.selectedViewSha256(),
                coverage,
                -1,
                -1,
                -1,
                -1,
                -1,
                digest("protocol"),
                digest("recovery"),
                digest("policy"),
                digest("format-policy"),
                M5MaterializationCodecV1.calculateSourceSetSha256(sources),
                sources,
                Optional.of(new M5PulsarFrontiersV2(new LedgerBoundary(7, 1), Optional.empty(), Optional.empty())));
    }

    private static SourceExtent source(M5PulsarCoverageV2 coverage, String id, int count, String entryRoot) {
        return new SourceExtent(
                SourceKind.BOOKKEEPER_LEDGER,
                digest("source-" + id),
                ProtocolCoverage.pulsar(coverage),
                "bk:" + id,
                9,
                count,
                id.equals("9") ? 42 : 43,
                id.equals("9") ? 42 : 44,
                digest("body-" + id),
                Optional.empty(),
                Optional.of(digest("ledger-" + id)),
                digest("format"),
                digest("encryption"),
                false,
                false,
                List.of(coverage.binding().bindingId().digest()),
                Optional.of(Sha256Digest.copyOf(HexFormat.of().parseHex(entryRoot))));
    }

    private static int findMagic(byte[] bytes, int magic) {
        for (int index = 0; index <= bytes.length - 8; index++) {
            if (ByteBuffer.wrap(bytes).getInt(index) == magic) {
                return index;
            }
        }
        throw new AssertionError("missing section magic");
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalUtf8.fromString(value).bytes());
    }

    private static CanonicalBytes golden(String name) throws IOException {
        try (var stream = M5PulsarMaterializationCodecV2Test.class.getResourceAsStream("/materialization/" + name)) {
            assertThat(stream).isNotNull();
            return CanonicalBytes.copyOf(HexFormat.of()
                    .parseHex(new String(stream.readAllBytes(), StandardCharsets.US_ASCII).replaceAll("\\s+", "")));
        }
    }
}
