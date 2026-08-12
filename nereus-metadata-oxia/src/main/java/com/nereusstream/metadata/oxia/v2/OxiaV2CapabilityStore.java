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

package com.nereusstream.metadata.oxia.v2;

import com.nereusstream.metadata.oxia.v2.capability.OxiaPulsarTopicGenerationSelectorStore;
import com.nereusstream.metadata.oxia.v2.capability.OxiaPulsarVirtualLedgerNamespaceRegistryStore;
import com.nereusstream.metadata.oxia.v2.capability.OxiaTopicBindingAggregatePublisher;
import com.nereusstream.metadata.oxia.v2.capability.OxiaTopicBindingAggregateReader;
import com.nereusstream.metadata.oxia.v2.capability.PulsarTopicAuthorityCoordinator;
import com.nereusstream.metadata.oxia.v2.codec.OxiaV2CodecSet;
import com.nereusstream.metadata.oxia.v2.continuity.RevalidationScheduler;
import com.nereusstream.metadata.oxia.v2.continuity.StoreContinuity;
import com.nereusstream.metadata.oxia.v2.continuity.StoreContinuitySnapshot;
import com.nereusstream.metadata.oxia.v2.key.OxiaV2AuthorityKeys;
import com.nereusstream.metadata.oxia.v2.mutation.AsyncOxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.mutation.ConditionalMutationEngine;
import com.nereusstream.metadata.oxia.v2.mutation.MutationFailureClassifier;
import com.nereusstream.metadata.oxia.v2.mutation.OxiaConditionalClient;
import com.nereusstream.metadata.spi.capability.PulsarTopicGenerationSelectorStore;
import com.nereusstream.metadata.spi.capability.PulsarVirtualLedgerNamespaceRegistryStore;
import com.nereusstream.metadata.spi.capability.TopicBindingAggregatePublisher;
import com.nereusstream.metadata.spi.capability.TopicBindingAggregateReader;
import io.oxia.client.api.AsyncOxiaClient;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/** Owns the shared client and store-wide O2 lifecycle; selector/Registry and activation remain fail closed. */
public final class OxiaV2CapabilityStore implements AutoCloseable {
    private final AsyncOxiaClient client;
    private final StoreContinuity continuity;
    private final RevalidationScheduler scheduler;
    private final TopicBindingAggregatePublisher aggregatePublisher;
    private final TopicBindingAggregateReader aggregateReader;
    private final PulsarTopicGenerationSelectorStore selectorStore;
    private final PulsarVirtualLedgerNamespaceRegistryStore registryStore;
    private final boolean pulsarSelectorReady;
    private final AtomicBoolean closed = new AtomicBoolean();

    OxiaV2CapabilityStore(
            AsyncOxiaClient client,
            StoreContinuity continuity,
            RevalidationScheduler scheduler,
            OxiaV2AuthorityKeys keys,
            OxiaV2CodecSet codecs) {
        this.client = Objects.requireNonNull(client, "client");
        this.continuity = Objects.requireNonNull(continuity, "continuity");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        Objects.requireNonNull(keys, "keys");
        Objects.requireNonNull(codecs, "codecs");
        pulsarSelectorReady =
                codecs.aggregate().available() && codecs.selector().available();
        OxiaConditionalClient conditionalClient = new AsyncOxiaConditionalClient(client);
        ConditionalMutationEngine mutationEngine =
                new ConditionalMutationEngine(conditionalClient, new MutationFailureClassifier());
        aggregatePublisher =
                new OxiaTopicBindingAggregatePublisher(this::requireOpen, keys, codecs.aggregate(), mutationEngine);
        aggregateReader =
                new OxiaTopicBindingAggregateReader(this::requireOpen, keys, codecs.aggregate(), conditionalClient);
        selectorStore = new OxiaPulsarTopicGenerationSelectorStore(
                this::requireOpen, keys, codecs.selector(), conditionalClient, mutationEngine);
        registryStore = new OxiaPulsarVirtualLedgerNamespaceRegistryStore(
                this::requireOpen, keys, codecs.registry(), conditionalClient, mutationEngine);
    }

    public StoreContinuitySnapshot continuitySnapshot() {
        return continuity.current();
    }

    /** O2 cannot activate until later slices supply selector/Registry codecs and ownership fences. */
    public boolean productionActivationReady() {
        return false;
    }

    /** P1 metadata capability only; native ownership and R1 are separate activation requirements. */
    public boolean pulsarSelectorReady() {
        return pulsarSelectorReady;
    }

    public TopicBindingAggregatePublisher aggregatePublisher() {
        return aggregatePublisher;
    }

    public TopicBindingAggregateReader aggregateReader() {
        return aggregateReader;
    }

    public PulsarTopicGenerationSelectorStore selectorStore() {
        return selectorStore;
    }

    public PulsarVirtualLedgerNamespaceRegistryStore registryStore() {
        return registryStore;
    }

    /** Creates the P1 coordinator over this store's exact narrow capabilities. */
    public PulsarTopicAuthorityCoordinator pulsarTopicAuthorityCoordinator() {
        requireOpen();
        if (!pulsarSelectorReady) {
            throw new IllegalStateException("P1 selector capability is unavailable");
        }
        return new PulsarTopicAuthorityCoordinator(aggregatePublisher, aggregateReader, selectorStore);
    }

    AsyncOxiaClient client() {
        return client;
    }

    StoreContinuity continuity() {
        return continuity;
    }

    private void requireOpen() {
        if (closed.get()) {
            throw new IllegalStateException("O2 capability store is closed");
        }
    }

    @Override
    public void close() throws Exception {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        Exception failure = null;
        try {
            continuity.close();
        } catch (RuntimeException closeFailure) {
            failure = closeFailure;
        }
        try {
            scheduler.close();
        } catch (RuntimeException closeFailure) {
            failure = accumulate(failure, closeFailure);
        }
        try {
            client.close();
        } catch (Exception closeFailure) {
            failure = accumulate(failure, closeFailure);
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static Exception accumulate(Exception existing, Exception additional) {
        if (existing == null) {
            return additional;
        }
        existing.addSuppressed(additional);
        return existing;
    }
}
