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
import com.nereusstream.metadata.oxia.v2.codec.OxiaV2CodecSet;
import com.nereusstream.metadata.oxia.v2.codec.UnavailableProductionCodecException;
import com.nereusstream.metadata.oxia.v2.key.OxiaV2AuthorityKeys;
import com.nereusstream.metadata.oxia.v2.mutation.ConditionalMutationEngine;
import com.nereusstream.metadata.oxia.v2.mutation.MutationFailureClassifier;
import com.nereusstream.metadata.oxia.v2.testing.DeterministicOxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.testing.O2TestValues;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ProductionCodecFailClosedTest {
    private DeterministicOxiaConditionalClient client;
    private OxiaTopicBindingAggregateReader aggregateReader;
    private OxiaPulsarTopicGenerationSelectorStore selectorStore;
    private OxiaPulsarVirtualLedgerNamespaceRegistryStore registryStore;

    @BeforeEach
    void setUp() {
        client = new DeterministicOxiaConditionalClient();
        var keys = new OxiaV2AuthorityKeys("/nereus/test");
        var codecs = OxiaV2CodecSet.productionUnavailable();
        var engine = new ConditionalMutationEngine(client, new MutationFailureClassifier());
        aggregateReader = new OxiaTopicBindingAggregateReader(() -> {}, keys, codecs.aggregate(), client);
        selectorStore = new OxiaPulsarTopicGenerationSelectorStore(() -> {}, keys, codecs.selector(), client, engine);
        registryStore =
                new OxiaPulsarVirtualLedgerNamespaceRegistryStore(() -> {}, keys, codecs.registry(), client, engine);
    }

    @Test
    void aggregateCodecGapFailsBeforeIo() {
        assertUnavailable(() -> aggregateReader
                .readAggregate(O2TestValues.incarnation(1))
                .toCompletableFuture()
                .join());
    }

    @Test
    void aggregatePublisherCodecGapFailsBeforeIo() {
        var keys = new OxiaV2AuthorityKeys("/nereus/test");
        var codecs = OxiaV2CodecSet.productionUnavailable();
        var publisher = new OxiaTopicBindingAggregatePublisher(
                () -> {},
                keys,
                codecs.aggregate(),
                new ConditionalMutationEngine(client, new MutationFailureClassifier()));

        assertUnavailable(() -> publisher
                .publishIfAbsent(O2TestValues.aggregateCandidate("aggregate"))
                .toCompletableFuture()
                .join());
    }

    @Test
    void selectorCodecGapFailsBeforeIo() {
        assertUnavailable(() -> selectorStore
                .readSelector(O2TestValues.incarnation(1).persistenceName())
                .toCompletableFuture()
                .join());
    }

    @Test
    void registryCodecGapFailsBeforeIo() {
        assertUnavailable(() -> registryStore
                .readRegistry(O2TestValues.DEPLOYMENT, O2TestValues.RESERVATION, O2TestValues.NAMESPACE_ID)
                .toCompletableFuture()
                .join());
    }

    private void assertUnavailable(Runnable operation) {
        assertThatThrownBy(operation::run).hasRootCauseInstanceOf(UnavailableProductionCodecException.class);
        assertThat(client.readCount()).isZero();
        assertThat(client.createCount()).isZero();
        assertThat(client.casCount()).isZero();
    }
}
