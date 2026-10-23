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

package com.nereusstream.metadata.oxia.v2.retention;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.aggregate.InitialStorageEpochV1;
import com.nereusstream.domain.aggregate.StorageProfileV1;
import com.nereusstream.domain.aggregate.TopicBindingAggregateV1;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.Nta1CodecV1;
import com.nereusstream.domain.codec.TopicIncarnationIdentityCodecV1;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.metadata.oxia.v2.codec.Nps1SelectorAuthorityCodec;
import com.nereusstream.metadata.oxia.v2.key.OxiaV2AuthorityKeys;
import com.nereusstream.metadata.oxia.v2.mutation.MetadataVersionMapper;
import com.nereusstream.metadata.oxia.v2.testing.DeterministicOxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.testing.O2TestValues;
import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorStateV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.api.lifecycle.MetadataNamespaceIdentityV2;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdCodecV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import com.nereusstream.storage.object.materialization.M5MaterializationKeysV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1;
import com.nereusstream.storage.object.materialization.M5PulsarCoverageV2;
import com.nereusstream.storage.object.materialization.M5PulsarFrontiersV2;
import com.nereusstream.storage.object.materialization.M5PulsarRecoveryInputV2;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlKeysV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** Declared metadata/component fixtures only; these tests provide no Native Task/backend qualification. */
class OxiaPulsarMaterializationInputV2Test {
    private static final String ROOT = "/nereus/cells/declared_input";
    private static final int SHARD = 7;

    @Test
    void canonicalWireMatchesIndependentFieldOrderAndReconstructsOriginalRecoveryInput() throws Exception {
        var fixture = fixture();
        var value = fixture.value();
        assertThat(value.encode()).isEqualTo(independentWire(value));
        assertThat(OxiaPulsarMaterializationInputV2.decode(independentWire(value)))
                .isEqualTo(value);
        assertThat(value.verifySourceView(fixture.view())).isEqualTo(fixture.recovery());
        assertThat(value.storageFence()).isEqualTo(1);
        assertThatThrownBy(() -> OxiaPulsarMaterializationInputV2.decode(CanonicalBytes.copyOf(Arrays.copyOf(
                        value.encode().toByteArray(), value.encode().length() - 1))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> OxiaPulsarMaterializationInputV2.decode(CanonicalBytes.copyOf(Arrays.copyOf(
                        value.encode().toByteArray(), value.encode().length() + 1))))
                .hasMessageContaining("trailing");
    }

    @Test
    void exactOriginalP1TokenSurvivesAnActualVersionedEnvelopeSuccessor() throws Exception {
        var fixture = fixture();
        var migrated = com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.migrateLegacy(
                fixture.aggregate(),
                new M4ReadControlRecordsV1.CapabilityBinding(
                        1, Sha256Digest.hash(CanonicalBytes.copyOf(new byte[] {1}))));
        var next = com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.successor(
                migrated, migrated.state(), Optional.empty(), List.of());
        var bytes = com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.encodeAuthority(next);
        var changedPhysicalVersion = VersionedValue.of(
                fixture.aggregate().key(),
                bytes,
                MetadataVersionMapper.fromOxia(fixture.value().aggregateVersion() + 17));
        fixture.value().requireCurrentP1(changedPhysicalVersion, fixture.selector());
        assertThat(changedPhysicalVersion.metadataVersion())
                .isNotEqualTo(fixture.aggregate().metadataVersion());
    }

    @Test
    void equalValuesWithNewNativeVersionsInvalidateInputAndReferencedView() throws Exception {
        var fixture = fixture();
        var input = fixture.value();
        input.requireCurrentP1(fixture.aggregate(), fixture.selector());
        assertThatThrownBy(() -> input.requireCurrentP1(
                        version(fixture.aggregate(), input.aggregateVersion() + 1), fixture.selector()))
                .hasMessageContaining("P1 key/version/value changed");
        assertThatThrownBy(() -> input.requireCurrentP1(
                        fixture.aggregate(), version(fixture.selector(), input.selectorVersion() + 1)))
                .hasMessageContaining("P1 key/version/value changed");
        assertThatThrownBy(() -> input.verifySourceView(version(fixture.view(), input.sourceViewVersion() + 1)))
                .hasMessageContaining("source-view key/version/value differs");
        var changed = fixture.view().canonicalStoredBytes().toByteArray();
        changed[changed.length - 1] ^= 1;
        assertThatThrownBy(() -> input.verifySourceView(VersionedValue.of(
                        input.sourceViewKey(),
                        CanonicalBytes.copyOf(changed),
                        fixture.view().metadataVersion())))
                .hasMessageContaining("source-view key/version/value differs");
        var successor = new OxiaPulsarMaterializationInputV2(
                input.metadataNamespace(),
                input.authorityRoot(),
                input.aggregateKey(),
                input.aggregateVersion() + 1,
                input.aggregateValue(),
                input.selectorKey(),
                input.selectorVersion(),
                input.selectorValue(),
                input.sourceViewKey(),
                input.sourceViewVersion(),
                input.sourceViewValueSha256(),
                input.coverage(),
                input.recoveryInputSha256());
        assertThat(successor.protocolStateRootSha256()).isNotEqualTo(input.protocolStateRootSha256());
        assertThatThrownBy(() -> successor.verifySourceView(fixture.view())).hasMessageContaining("original P1 view");
    }

    @Test
    void routeRecoversExactLostReplyAndRejectsForeignOrChangedReferences() throws Exception {
        var fixture = fixture();
        var input = fixture.value();
        var client = seeded(fixture);
        var route = route(client, input);
        String key = new M5MaterializationKeysV1(SHARD, input.binding()).nativeInput(input.protocolStateRootSha256());
        client.nextMutation(DeterministicOxiaConditionalClient.MutationMode.APPLY_THEN_RESPONSE_LOSS);
        assertThat(route.compareAndSet(Optional.empty(), key, input.encode())
                        .toCompletableFuture()
                        .join())
                .isEqualTo(MutationOutcome.APPLIED_EXACT);
        var restarted = route(client, input);
        assertThat(restarted
                        .read(key)
                        .toCompletableFuture()
                        .join()
                        .orElseThrow()
                        .canonicalStoredBytes())
                .isEqualTo(input.encode());
        assertThat(restarted
                        .compareAndSet(Optional.empty(), key, input.encode())
                        .toCompletableFuture()
                        .join())
                .isEqualTo(MutationOutcome.DEFINITIVE_CONFLICT);
        assertThatThrownBy(() -> restarted.compareAndSet(
                        restarted.read(key).toCompletableFuture().join(), key, input.encode()))
                .hasMessageContaining("create-only");
        assertThatThrownBy(
                        () -> new OxiaBindingLifecycleMetadataStoreV2(client, ROOT, SHARD, input.binding()).read(key))
                .hasMessageContaining("exact lifecycle Binding route");
        var foreign = new OxiaPulsarMaterializationInputV2(
                input.metadataNamespace(),
                input.authorityRoot(),
                input.aggregateKey(),
                input.aggregateVersion(),
                input.aggregateValue(),
                input.selectorKey(),
                input.selectorVersion(),
                input.selectorValue(),
                "/foreign/map",
                input.sourceViewVersion(),
                input.sourceViewValueSha256(),
                input.coverage(),
                input.recoveryInputSha256());
        assertThatThrownBy(() -> route.compareAndSet(
                        Optional.empty(),
                        new M5MaterializationKeysV1(SHARD, input.binding())
                                .nativeInput(foreign.protocolStateRootSha256()),
                        foreign.encode()))
                .hasMessageContaining("source-view route differs");
        var changed = seeded(fixture);
        changed.seed(input.selectorKey(), input.selectorValue(), input.selectorVersion() + 1);
        assertThatThrownBy(() -> route(changed, input)
                        .compareAndSet(Optional.empty(), key, input.encode())
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("Native input P1 key/version/value changed");
    }

    @Test
    void typedCutRequiresExactPersistedInputRecoveryRootAndStorageFence() throws Exception {
        var fixture = fixture();
        var input = fixture.value();
        var client = seeded(fixture);
        var route = route(client, input);
        var keys = new M5MaterializationKeysV1(SHARD, input.binding());
        var cut = cut(input, input.recoveryInputSha256(), input.storageFence());
        var task = M5MaterializationCodecV1.calculateTaskId(cut);
        assertThatThrownBy(() -> route.compareAndSet(
                                Optional.empty(), keys.sourceCut(task), M5MaterializationCodecV1.encodeSourceCut(cut))
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("Native source cut lacks its complete stored input");
        assertThat(route.compareAndSet(
                                Optional.empty(), keys.nativeInput(input.protocolStateRootSha256()), input.encode())
                        .toCompletableFuture()
                        .join())
                .isEqualTo(MutationOutcome.APPLIED_EXACT);
        assertThat(route.compareAndSet(
                                Optional.empty(), keys.sourceCut(task), M5MaterializationCodecV1.encodeSourceCut(cut))
                        .toCompletableFuture()
                        .join())
                .isEqualTo(MutationOutcome.APPLIED_EXACT);
        for (var wrong : List.of(
                cut(input, digest("foreign-recovery"), input.storageFence()),
                cut(input, input.recoveryInputSha256(), input.storageFence() + 1))) {
            assertThatThrownBy(() -> route.compareAndSet(
                                    Optional.empty(),
                                    keys.sourceCut(M5MaterializationCodecV1.calculateTaskId(wrong)),
                                    M5MaterializationCodecV1.encodeSourceCut(wrong))
                            .toCompletableFuture()
                            .join())
                    .hasRootCauseMessage("Native cut/input coverage, recovery or Storage Epoch differs");
        }
    }

    private static M5MaterializationRecordsV1.MaterializationSourceCut cut(
            OxiaPulsarMaterializationInputV2 input, Sha256Digest recovery, long fence) {
        var identity = new M5MaterializationRecordsV1.IdentityEnvelope(
                input.protocolCellSha256(),
                digest("scope"),
                input.binding(),
                1,
                1,
                fence,
                new M4ReadControlRecordsV1.CapabilityBinding(1, digest("capability")));
        var coverage = M5MaterializationRecordsV1.ProtocolCoverage.pulsar(input.coverage());
        var source = new M5MaterializationRecordsV1.SourceExtent(
                M5MaterializationRecordsV1.SourceKind.BOOKKEEPER_LEDGER,
                input.coverage().chain().get(0).sourceIdentitySha256(),
                coverage,
                "bookkeeper-ledger:10",
                40,
                2,
                1,
                2,
                digest("body"),
                Optional.empty(),
                Optional.of(digest("ledger")),
                digest("format"),
                digest("encryption"),
                false,
                false,
                List.of(input.binding().bindingId().digest()),
                Optional.of(digest("entry-root")));
        var selected = new M4ReadControlRecordsV1.BindingReadSelector(
                input.binding(),
                input.coverage().chainAuthorityRootSha256(),
                1,
                1,
                1,
                M4ReadControlRecordsV1.SelectorMode.PREFERRED_ONLY,
                M4ReadControlRecordsV1.AdmissionState.ADMITTING,
                Optional.empty(),
                identity.capability(),
                List.of(),
                List.of());
        return new M5MaterializationRecordsV1.MaterializationSourceCut(
                identity,
                selected,
                Sha256Digest.hash(M4ReadControlCodecV1.encodeSelector(selected)),
                selected.selectedViewSha256(),
                coverage,
                -1,
                -1,
                -1,
                -1,
                -1,
                input.protocolStateRootSha256(),
                recovery,
                digest("policy"),
                digest("output-format"),
                M5MaterializationCodecV1.calculateSourceSetSha256(List.of(source)),
                List.of(source),
                Optional.of(new M5PulsarFrontiersV2(
                        new M5PulsarFrontiersV2.LedgerBoundary(20, 1), Optional.empty(), Optional.empty())));
    }

    private static DeterministicOxiaConditionalClient seeded(Fixture fixture) {
        var client = new DeterministicOxiaConditionalClient();
        for (var value : List.of(fixture.aggregate(), fixture.selector(), fixture.view())) {
            client.seed(
                    value.key(), value.canonicalStoredBytes(), MetadataVersionMapper.toOxia(value.metadataVersion()));
        }
        return client;
    }

    private static OxiaBindingLifecycleMetadataStoreV2 route(
            DeterministicOxiaConditionalClient client, OxiaPulsarMaterializationInputV2 input) {
        return new OxiaBindingLifecycleMetadataStoreV2(
                client, ROOT, SHARD, input.binding(), input.protocolCellSha256());
    }

    private record Fixture(
            OxiaPulsarMaterializationInputV2 value,
            VersionedValue aggregate,
            VersionedValue selector,
            VersionedValue view,
            M5PulsarRecoveryInputV2 recovery) {}

    private static Fixture fixture() throws Exception {
        var original = O2TestValues.productionAggregateCandidate().aggregate();
        var initial = original.initialEpoch();
        var aggregate = new TopicBindingAggregateV1(
                original.aggregateSchemaVersion(),
                original.binding(),
                new InitialStorageEpochV1(
                        initial.storageEpochId(),
                        initial.epochOrdinal(),
                        StorageProfileV1.BOOKKEEPER_WAL_ASYNC_OBJECT,
                        initial.profileOrigin(),
                        initial.policyCatalogDigest(),
                        initial.frameEncodingPolicy()));
        var aggregateBytes = Nta1CodecV1.encode(aggregate);
        var incarnation = O2TestValues.incarnation(1);
        var binding = new M4ReadControlRecordsV1.BindingIdentity(
                aggregate.binding().bindingId(),
                Sha256Digest.hash(TopicIncarnationIdentityCodecV1.encode(incarnation)),
                initial.storageEpochId().digest());
        var namespace = new MetadataNamespaceIdentityV2(new Id128(9, 10), 0);
        var objects = new PhysicalNamespaceAuthorityBindingV2(
                new PhysicalResourceIdV2.Namespace(
                        PhysicalResourceIdV2.ProviderKind.OBJECT_PROVIDER, text("object-service"), text("bucket")),
                namespace);
        var bk = new PhysicalNamespaceAuthorityBindingV2(
                new PhysicalResourceIdV2.Namespace(
                        PhysicalResourceIdV2.ProviderKind.BOOKKEEPER, text("bk-service"), text("ledgers")),
                namespace);
        var recovery = new M5PulsarRecoveryInputV2(
                namespace,
                binding,
                incarnation.persistenceName().value().value(),
                "/managed-ledgers/" + incarnation.persistenceName().value().value(),
                7,
                text("original-metadata").bytes());
        var sealed = bkSource(bk, 10, false, 1);
        var tail = bkSource(bk, 20, true, 0);
        var viewBytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(viewBytes)) {
            out.writeInt(0x4e504256);
            out.writeInt(1);
            write(out, text("NEREUS_V2_NATIVE_PULSAR_BINDING_READ_VIEW_V1").bytes());
            out.write(binding.bindingId().digest().bytes().toByteArray());
            out.write(binding.incarnationSha256().bytes().toByteArray());
            out.write(binding.storageEpochSha256().bytes().toByteArray());
            out.write(Sha256Digest.hash(aggregateBytes).bytes().toByteArray());
            write(out, MetadataVersionMapper.fromOxia(2).value());
            write(out, MetadataVersionMapper.fromOxia(3).value());
            write(out, objects.encode());
            out.writeLong(recovery.metadataVersion());
            write(out, recovery.storedMetadataBytes());
            out.writeLong(20);
            out.writeLong(0);
            out.writeInt(2);
            out.writeLong(10);
            out.writeByte(1);
            write(out, sealed);
            out.writeLong(20);
            out.writeByte(1);
            write(out, tail);
        }
        var actualView = CanonicalBytes.copyOf(viewBytes.toByteArray());
        var viewSha = Sha256Digest.hash(actualView);
        var coverage = M5PulsarCoverageV2.of(
                binding,
                viewSha,
                List.of(
                        new M5PulsarCoverageV2.ChainLedger(10, false, 2, Sha256Digest.hash(sealed)),
                        new M5PulsarCoverageV2.ChainLedger(20, true, 1, Sha256Digest.hash(tail))),
                List.of(new M5PulsarCoverageV2.EntryRange(10, 0, 2)));
        var mapBytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(mapBytes)) {
            out.writeInt(0x4e50534d);
            out.writeInt(1);
            out.write(viewSha.bytes().toByteArray());
            write(out, actualView);
        }
        String selectorRoute = ROOT + "/" + new M4ReadControlKeysV1(SHARD, binding).selector();
        String sourceKey = selectorRoute.substring(0, selectorRoute.length() - "/selector".length())
                + "/native-source-maps/" + viewSha.toHex();
        var view = VersionedValue.of(
                sourceKey, CanonicalBytes.copyOf(mapBytes.toByteArray()), MetadataVersionMapper.fromOxia(4));
        var p1Keys = new OxiaV2AuthorityKeys("/p1");
        var selector = Nps1SelectorAuthorityCodec.createValue(
                incarnation.persistenceName(),
                incarnation.bindingGeneration(),
                PulsarTopicGenerationSelectorStateV1.ACTIVE,
                binding.bindingId(),
                Sha256Digest.hash(aggregateBytes));
        var value = new OxiaPulsarMaterializationInputV2(
                namespace,
                "/p1",
                p1Keys.aggregateKey(incarnation),
                2,
                aggregateBytes,
                p1Keys.selectorKey(incarnation.persistenceName()),
                3,
                selector.canonicalStoredBytes(),
                view.key(),
                4,
                view.canonicalStoredSha256(),
                coverage,
                recovery.sha256());
        return new Fixture(
                value,
                VersionedValue.of(value.aggregateKey(), aggregateBytes, MetadataVersionMapper.fromOxia(2)),
                VersionedValue.of(value.selectorKey(), value.selectorValue(), MetadataVersionMapper.fromOxia(3)),
                view,
                recovery);
    }

    private static CanonicalBytes bkSource(
            PhysicalNamespaceAuthorityBindingV2 namespace, long ledger, boolean active, long last) throws Exception {
        var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) {
            write(out, text("NEREUS_V2_NATIVE_PULSAR_BK_READ_SOURCE_V1").bytes());
            write(out, namespace.encode());
            write(
                    out,
                    PhysicalResourceIdCodecV2.encode(
                            new PhysicalResourceIdV2.BookKeeperLedger(namespace.physicalNamespace(), ledger)));
            out.writeLong(5);
            out.writeBoolean(active);
            out.writeLong(last);
            if (!active) {
                out.writeLong(20);
            }
        }
        return CanonicalBytes.copyOf(bytes.toByteArray());
    }

    private static CanonicalBytes independentWire(OxiaPulsarMaterializationInputV2 input) throws Exception {
        var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) {
            out.writeInt(0x4e504d49);
            out.writeInt(2);
            out.write(input.metadataNamespace().encode().toByteArray());
            write(out, text(input.authorityRoot()).bytes());
            write(out, text(input.aggregateKey()).bytes());
            out.writeLong(input.aggregateVersion());
            write(out, input.aggregateValue());
            write(out, text(input.selectorKey()).bytes());
            out.writeLong(input.selectorVersion());
            write(out, input.selectorValue());
            write(out, text(input.sourceViewKey()).bytes());
            out.writeLong(input.sourceViewVersion());
            out.write(input.sourceViewValueSha256().bytes().toByteArray());
            write(out, input.coverage().encode());
            out.write(input.recoveryInputSha256().bytes().toByteArray());
        }
        return CanonicalBytes.copyOf(bytes.toByteArray());
    }

    private static VersionedValue version(VersionedValue value, long version) {
        return VersionedValue.of(value.key(), value.canonicalStoredBytes(), MetadataVersionMapper.fromOxia(version));
    }

    private static void write(DataOutputStream out, CanonicalBytes bytes) throws Exception {
        out.writeInt(bytes.length());
        out.write(bytes.toByteArray());
    }

    private static CanonicalUtf8 text(String value) {
        return CanonicalUtf8.fromString(value);
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(text(value).bytes());
    }
}
