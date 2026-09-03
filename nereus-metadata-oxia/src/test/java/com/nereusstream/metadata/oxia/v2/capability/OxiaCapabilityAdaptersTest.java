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

package com.nereusstream.metadata.oxia.v2.capability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.aggregate.TopicBindingAggregateV1;
import com.nereusstream.domain.aggregate.TopicBindingV1;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.KafkaTopicId;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.KafkaTopicName;
import com.nereusstream.metadata.oxia.v2.codec.AggregateAuthorityCodec;
import com.nereusstream.metadata.oxia.v2.codec.Nta1AggregateAuthorityCodec;
import com.nereusstream.metadata.oxia.v2.codec.OxiaV2CodecSet;
import com.nereusstream.metadata.oxia.v2.key.OxiaV2AuthorityKeys;
import com.nereusstream.metadata.oxia.v2.mutation.ConditionalMutationEngine;
import com.nereusstream.metadata.oxia.v2.mutation.MutationFailureClassifier;
import com.nereusstream.metadata.oxia.v2.testing.DeterministicAuthorityCodecs;
import com.nereusstream.metadata.oxia.v2.testing.DeterministicOxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.testing.DeterministicOxiaConditionalClient.MutationMode;
import com.nereusstream.metadata.oxia.v2.testing.O2TestValues;
import com.nereusstream.metadata.spi.model.AggregatePublicationCandidate;
import com.nereusstream.metadata.spi.model.ConditionalCasOutcome;
import com.nereusstream.metadata.spi.model.CreateMutationOutcome;
import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorStateV1;
import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorValueV1;
import com.nereusstream.metadata.spi.model.PulsarVirtualLedgerNamespaceRegistryValueV1;
import com.nereusstream.metadata.spi.model.VersionedAggregateSnapshot;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OxiaCapabilityAdaptersTest {
    private DeterministicOxiaConditionalClient client;
    private DeterministicAuthorityCodecs codecs;
    private OxiaV2AuthorityKeys keys;
    private OxiaTopicBindingAggregatePublisher publisher;
    private OxiaTopicBindingAggregateReader reader;
    private OxiaPulsarTopicGenerationSelectorStore selectors;
    private OxiaPulsarVirtualLedgerNamespaceRegistryStore registries;

    @BeforeEach
    void setUp() {
        client = new DeterministicOxiaConditionalClient();
        codecs = new DeterministicAuthorityCodecs();
        keys = new OxiaV2AuthorityKeys("/nereus/test");
        ConditionalMutationEngine engine = new ConditionalMutationEngine(client, new MutationFailureClassifier());
        OxiaV2CodecSet codecSet = codecs.codecSet();
        publisher = new OxiaTopicBindingAggregatePublisher(() -> {}, keys, codecSet.aggregate(), engine);
        reader = new OxiaTopicBindingAggregateReader(() -> {}, keys, codecSet.aggregate(), client);
        selectors = new OxiaPulsarTopicGenerationSelectorStore(() -> {}, keys, codecSet.selector(), client, engine);
        registries =
                new OxiaPulsarVirtualLedgerNamespaceRegistryStore(() -> {}, keys, codecSet.registry(), client, engine);
    }

    @Test
    void aggregatePublisherAndReaderShareOneImmutableAuthorityKey() {
        AggregatePublicationCandidate candidate = O2TestValues.aggregateCandidate("aggregate-one");

        var publishResult =
                publisher.publishIfAbsent(candidate).toCompletableFuture().join();
        var readResult = reader.readAggregate(candidate.aggregate().binding().incarnationIdentity())
                .toCompletableFuture()
                .join();

        assertThat(publishResult.outcome()).isEqualTo(CreateMutationOutcome.CREATED);
        assertThat(readResult).contains(publishResult.exactSnapshot().orElseThrow());
        assertThat(client.stored(
                        keys.aggregateKey(candidate.aggregate().binding().incarnationIdentity())))
                .isPresent();
    }

    @Test
    void productionAggregateAdaptersProjectOneM5AuthorityEnvelope() {
        AggregatePublicationCandidate candidate = O2TestValues.productionAggregateCandidate();
        AggregateAuthorityCodec productionCodec = new Nta1AggregateAuthorityCodec();
        var authority =
                M5PulsarAggregateAuthorityCodecV1.encodeAuthority(M5PulsarAggregateAuthorityCodecV1.migrateLegacy(
                        candidate.canonicalStoredBytes(),
                        new CapabilityBinding(7, Sha256Digest.hash(O2TestValues.bytes("m5-capability")))));
        String key = keys.aggregateKey(candidate.aggregate().binding().incarnationIdentity());
        client.seed(key, authority, 3);
        var productionReader = new OxiaTopicBindingAggregateReader(() -> {}, keys, productionCodec, client);
        var productionPublisher = new OxiaTopicBindingAggregatePublisher(
                () -> {},
                keys,
                productionCodec,
                new ConditionalMutationEngine(client, new MutationFailureClassifier()));

        VersionedAggregateSnapshot snapshot = productionReader
                .readAggregate(candidate.aggregate().binding().incarnationIdentity())
                .toCompletableFuture()
                .join()
                .orElseThrow();
        var publish = productionPublisher
                .publishIfAbsent(candidate)
                .toCompletableFuture()
                .join();

        assertThat(snapshot.aggregate()).isEqualTo(candidate.aggregate());
        assertThat(snapshot.canonicalStoredBytes()).isEqualTo(candidate.canonicalStoredBytes());
        assertThat(snapshot.canonicalStoredDigest()).isEqualTo(candidate.canonicalStoredDigest());
        assertThat(publish.outcome()).isEqualTo(CreateMutationOutcome.EXISTING_EXACT);
        assertThat(client.stored(key).orElseThrow().storedBytes()).isEqualTo(authority);
    }

    @Test
    void aggregateResponseLossAfterApplyIsExistingExact() {
        client.nextMutation(MutationMode.APPLY_THEN_RESPONSE_LOSS);

        var result = publisher
                .publishIfAbsent(O2TestValues.aggregateCandidate("aggregate"))
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(CreateMutationOutcome.EXISTING_EXACT);
    }

    @Test
    void aggregateWellFormedDifferentValueIsDefinitiveConflict() {
        AggregatePublicationCandidate existing = O2TestValues.aggregateCandidate("existing");
        AggregatePublicationCandidate candidate = O2TestValues.aggregateCandidate("candidate");
        codecs.register(existing);
        client.seed(
                keys.aggregateKey(existing.aggregate().binding().incarnationIdentity()),
                existing.canonicalStoredBytes(),
                3);

        var result = publisher.publishIfAbsent(candidate).toCompletableFuture().join();

        assertThat(result.outcome()).isEqualTo(CreateMutationOutcome.DEFINITIVE_CONFLICT);
    }

    @Test
    void aggregateReaderRejectsKafkaBeforeBackendIo() {
        var kafka = new KafkaTopicIncarnationIdentity(new KafkaTopicId(new Id128(7, 8)), new KafkaTopicName("orders"));

        assertThatThrownBy(
                        () -> reader.readAggregate(kafka).toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .hasRootCauseInstanceOf(IllegalArgumentException.class);
        assertThat(client.readCount()).isZero();
    }

    @Test
    void aggregatePublisherRejectsWrongRederivedBindingIdBeforeBackendIo() {
        AggregatePublicationCandidate valid = O2TestValues.aggregateCandidate("aggregate");
        TopicBindingV1 binding = valid.aggregate().binding();
        TopicBindingAggregateV1 malformed = new TopicBindingAggregateV1(
                TopicBindingAggregateV1.SCHEMA_VERSION,
                new TopicBindingV1(
                        binding.protocolKind(),
                        O2TestValues.aggregateCandidate("other")
                                .aggregate()
                                .binding()
                                .bindingId(),
                        binding.cellIdentity(),
                        O2TestValues.incarnation(2)),
                valid.aggregate().initialEpoch());
        AggregatePublicationCandidate candidate = new AggregatePublicationCandidate(
                malformed, valid.canonicalStoredBytes(), valid.canonicalStoredDigest());

        assertThatThrownBy(() -> publisher
                        .publishIfAbsent(candidate)
                        .toCompletableFuture()
                        .join())
                .hasRootCauseInstanceOf(IllegalArgumentException.class);
        assertThat(client.createCount()).isZero();
    }

    @Test
    void aggregateMalformedStoredBytesFailClosedWithoutRepair() {
        var incarnation = O2TestValues.incarnation(1);
        client.seed(keys.aggregateKey(incarnation), O2TestValues.bytes("unregistered"), 3);

        assertThatThrownBy(() ->
                        reader.readAggregate(incarnation).toCompletableFuture().join())
                .hasRootCauseInstanceOf(IllegalArgumentException.class);
        assertThat(client.createCount()).isZero();
        assertThat(client.casCount()).isZero();
    }

    @Test
    void aggregateDigestMismatchFailsClosedWithoutRepair() {
        AggregatePublicationCandidate candidate = O2TestValues.aggregateCandidate("aggregate");
        String key = keys.aggregateKey(candidate.aggregate().binding().incarnationIdentity());
        client.seed(key, candidate.canonicalStoredBytes(), 3);
        AggregateAuthorityCodec digestMismatch = new AggregateAuthorityCodec() {
            @Override
            public boolean available() {
                return true;
            }

            @Override
            public com.nereusstream.domain.bytes.CanonicalBytes encode(AggregatePublicationCandidate ignored) {
                return ignored.canonicalStoredBytes();
            }

            @Override
            public VersionedAggregateSnapshot decode(
                    String ignoredKey,
                    com.nereusstream.domain.protocol.PulsarTopicIncarnationIdentity ignoredIncarnation,
                    com.nereusstream.domain.bytes.CanonicalBytes storedBytes,
                    com.nereusstream.metadata.spi.model.MetadataVersion metadataVersion) {
                return new VersionedAggregateSnapshot(
                        candidate.aggregate(),
                        storedBytes,
                        Sha256Digest.hash(O2TestValues.bytes("different")),
                        metadataVersion);
            }
        };
        var digestCheckingReader = new OxiaTopicBindingAggregateReader(() -> {}, keys, digestMismatch, client);

        assertThatThrownBy(() -> digestCheckingReader
                        .readAggregate(candidate.aggregate().binding().incarnationIdentity())
                        .toCompletableFuture()
                        .join())
                .hasRootCauseInstanceOf(IllegalArgumentException.class);
        assertThat(client.createCount()).isZero();
        assertThat(client.casCount()).isZero();
    }

    @Test
    void aggregateUnknownSchemaFailsClosed() {
        AggregatePublicationCandidate valid = O2TestValues.aggregateCandidate("schema-two");
        AggregatePublicationCandidate schemaTwo = new AggregatePublicationCandidate(
                new TopicBindingAggregateV1(
                        2, valid.aggregate().binding(), valid.aggregate().initialEpoch()),
                valid.canonicalStoredBytes(),
                valid.canonicalStoredDigest());
        codecs.register(schemaTwo);
        client.seed(
                keys.aggregateKey(schemaTwo.aggregate().binding().incarnationIdentity()),
                schemaTwo.canonicalStoredBytes(),
                3);

        assertThatThrownBy(() -> reader.readAggregate(
                                schemaTwo.aggregate().binding().incarnationIdentity())
                        .toCompletableFuture()
                        .join())
                .hasRootCauseInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aggregateIncarnationMismatchFailsClosed() {
        AggregatePublicationCandidate wrong = O2TestValues.aggregateCandidate("wrong");
        var expected = O2TestValues.incarnation(2);
        codecs.register(wrong);
        client.seed(keys.aggregateKey(expected), wrong.canonicalStoredBytes(), 3);

        assertThatThrownBy(() ->
                        reader.readAggregate(expected).toCompletableFuture().join())
                .hasRootCauseInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void selectorCreateReadAndCasUseOneKey() {
        PulsarTopicGenerationSelectorValueV1 reserved =
                O2TestValues.selector(PulsarTopicGenerationSelectorStateV1.RESERVED, "reserved");
        PulsarTopicGenerationSelectorValueV1 active =
                O2TestValues.selector(PulsarTopicGenerationSelectorStateV1.ACTIVE, "active");

        var created = selectors.createSelector(reserved).toCompletableFuture().join();
        var read = selectors
                .readSelector(reserved.persistenceName())
                .toCompletableFuture()
                .join();
        var applied = selectors
                .compareAndSetSelector(read.orElseThrow(), active)
                .toCompletableFuture()
                .join();

        assertThat(created.outcome()).isEqualTo(CreateMutationOutcome.CREATED);
        assertThat(applied.outcome()).isEqualTo(ConditionalCasOutcome.APPLIED_EXACT);
        assertThat(client.stored(keys.selectorKey(reserved.persistenceName())))
                .get()
                .extracting(record -> record.storedBytes())
                .isEqualTo(active.canonicalStoredBytes());
    }

    @Test
    void selectorResponseLossAndUnchangedPredecessorAreClosed() {
        PulsarTopicGenerationSelectorValueV1 reserved =
                O2TestValues.selector(PulsarTopicGenerationSelectorStateV1.RESERVED, "reserved");
        PulsarTopicGenerationSelectorValueV1 active =
                O2TestValues.selector(PulsarTopicGenerationSelectorStateV1.ACTIVE, "active");
        codecs.register(reserved);
        client.seed(keys.selectorKey(reserved.persistenceName()), reserved.canonicalStoredBytes(), 5);
        client.nextMutation(MutationMode.RESPONSE_LOSS_WITHOUT_APPLY);

        var result = selectors
                .compareAndSetSelector(
                        O2TestValues.selectorSnapshot(PulsarTopicGenerationSelectorStateV1.RESERVED, "reserved", 5),
                        active)
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(ConditionalCasOutcome.PREDECESSOR_UNCHANGED);
    }

    @Test
    void selectorIdentityMismatchFailsClosed() {
        PulsarTopicGenerationSelectorValueV1 value =
                O2TestValues.selector(PulsarTopicGenerationSelectorStateV1.RESERVED, "selector");
        codecs.register(value);
        var differentName =
                com.nereusstream.domain.protocol.PulsarPersistenceName.fromString("persistent://tenant/ns/different");
        client.seed(keys.selectorKey(differentName), value.canonicalStoredBytes(), 4);

        assertThatThrownBy(() -> selectors
                        .readSelector(differentName)
                        .toCompletableFuture()
                        .join())
                .hasRootCauseInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void registryCreateReadAndCasUseOneKey() {
        PulsarVirtualLedgerNamespaceRegistryValueV1 epochOne = O2TestValues.registry(1, "epoch-one");
        PulsarVirtualLedgerNamespaceRegistryValueV1 epochTwo = O2TestValues.registry(2, "epoch-two");

        var created = registries.createRegistry(epochOne).toCompletableFuture().join();
        var read = registries
                .readRegistry(O2TestValues.DEPLOYMENT, O2TestValues.RESERVATION, O2TestValues.NAMESPACE_ID)
                .toCompletableFuture()
                .join();
        var applied = registries
                .compareAndSetRegistry(read.orElseThrow(), epochTwo)
                .toCompletableFuture()
                .join();

        assertThat(created.outcome()).isEqualTo(CreateMutationOutcome.CREATED);
        assertThat(applied.outcome()).isEqualTo(ConditionalCasOutcome.APPLIED_EXACT);
    }

    @Test
    void registryResponseLossAfterApplyIsAppliedExact() {
        PulsarVirtualLedgerNamespaceRegistryValueV1 epochOne = O2TestValues.registry(1, "epoch-one");
        PulsarVirtualLedgerNamespaceRegistryValueV1 epochTwo = O2TestValues.registry(2, "epoch-two");
        codecs.register(epochOne);
        client.seed(
                keys.registryKey(O2TestValues.DEPLOYMENT, O2TestValues.RESERVATION, O2TestValues.NAMESPACE_ID),
                epochOne.canonicalStoredBytes(),
                5);
        client.nextMutation(MutationMode.APPLY_THEN_RESPONSE_LOSS);

        var result = registries
                .compareAndSetRegistry(O2TestValues.registrySnapshot(1, "epoch-one", 5), epochTwo)
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(ConditionalCasOutcome.APPLIED_EXACT);
    }

    @Test
    void registryIdentityMismatchFailsClosed() {
        PulsarVirtualLedgerNamespaceRegistryValueV1 value = O2TestValues.registry(1, "registry");
        codecs.register(value);
        var differentDeployment = new com.nereusstream.domain.identity.DeploymentId(new Id128(9, 10));
        client.seed(
                keys.registryKey(differentDeployment, O2TestValues.RESERVATION, O2TestValues.NAMESPACE_ID),
                value.canonicalStoredBytes(),
                4);

        assertThatThrownBy(() -> registries
                        .readRegistry(differentDeployment, O2TestValues.RESERVATION, O2TestValues.NAMESPACE_ID)
                        .toCompletableFuture()
                        .join())
                .hasRootCauseInstanceOf(IllegalArgumentException.class);
    }
}
