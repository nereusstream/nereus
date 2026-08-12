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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.metadata.oxia.v2.codec.OxiaV2CodecSet;
import com.nereusstream.metadata.oxia.v2.testing.FakeO1ContinuityClient;
import com.nereusstream.metadata.oxia.v2.testing.O2TestValues;
import com.nereusstream.metadata.oxia.v2.testing.RecordingRevalidationScheduler;
import com.nereusstream.metadata.spi.capability.PulsarTopicGenerationSelectorStore;
import com.nereusstream.metadata.spi.capability.PulsarVirtualLedgerNamespaceRegistryStore;
import com.nereusstream.metadata.spi.capability.TopicBindingAggregatePublisher;
import com.nereusstream.metadata.spi.capability.TopicBindingAggregateReader;
import io.oxia.client.api.NotificationContinuityState;
import java.util.ArrayList;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class OxiaV2CapabilityStoreTest {
    @Test
    void compositionExposesExactlyTheFourCapabilityInterfaces() throws Exception {
        FakeO1ContinuityClient client = new FakeO1ContinuityClient(1, NotificationContinuityState.READY);
        OxiaV2CapabilityStore store = attach(client, new RecordingRevalidationScheduler());

        assertThat(store.aggregatePublisher()).isInstanceOf(TopicBindingAggregatePublisher.class);
        assertThat(store.aggregateReader()).isInstanceOf(TopicBindingAggregateReader.class);
        assertThat(store.selectorStore()).isInstanceOf(PulsarTopicGenerationSelectorStore.class);
        assertThat(store.registryStore()).isInstanceOf(PulsarVirtualLedgerNamespaceRegistryStore.class);
        assertThat(store.productionActivationReady()).isFalse();
        store.close();
    }

    @Test
    void closeOrdersInvalidationRegistrationSchedulerAndClient() throws Exception {
        var events = new ArrayList<String>();
        FakeO1ContinuityClient client = new FakeO1ContinuityClient(1, NotificationContinuityState.READY, events);
        RecordingRevalidationScheduler scheduler = new RecordingRevalidationScheduler(events);
        OxiaV2CapabilityStore store = attach(client, scheduler);

        store.close();
        store.close();

        assertThat(store.continuitySnapshot().state())
                .isEqualTo(com.nereusstream.metadata.oxia.v2.continuity.StoreContinuityState.CLOSED);
        assertThat(events)
                .containsSubsequence("registration-open", "registration-close", "scheduler-close", "client-close");
        assertThat(client.registrationClosed()).isTrue();
        assertThat(scheduler.closed()).isTrue();
        assertThat(client.clientClosed()).isTrue();
    }

    @Test
    void closedStoreRejectsNewAdapterOperationBeforeClientIo() throws Exception {
        FakeO1ContinuityClient client = new FakeO1ContinuityClient(1, NotificationContinuityState.READY);
        OxiaV2CapabilityStore store = attach(client, new RecordingRevalidationScheduler());
        store.close();

        assertThatThrownBy(() -> store.aggregateReader()
                        .readAggregate(O2TestValues.incarnation(1))
                        .toCompletableFuture()
                        .join())
                .isInstanceOf(CompletionException.class)
                .hasRootCauseMessage("O2 capability store is closed");
        assertThat(client.unsupportedOperationCount()).isZero();
    }

    @Test
    void eachStoreOwnsAnIndependentClientContinuityAndScheduler() throws Exception {
        FakeO1ContinuityClient firstClient = new FakeO1ContinuityClient(1, NotificationContinuityState.READY);
        FakeO1ContinuityClient secondClient = new FakeO1ContinuityClient(1, NotificationContinuityState.READY);
        OxiaV2CapabilityStore first = attach(firstClient, new RecordingRevalidationScheduler());
        OxiaV2CapabilityStore second = attach(secondClient, new RecordingRevalidationScheduler());

        first.close();

        assertThat(first.continuitySnapshot().state())
                .isEqualTo(com.nereusstream.metadata.oxia.v2.continuity.StoreContinuityState.CLOSED);
        assertThat(second.continuitySnapshot().state())
                .isEqualTo(com.nereusstream.metadata.oxia.v2.continuity.StoreContinuityState.READY);
        assertThat(secondClient.clientClosed()).isFalse();
        second.close();
    }

    private static OxiaV2CapabilityStore attach(
            FakeO1ContinuityClient client, RecordingRevalidationScheduler scheduler) {
        return OxiaV2CapabilityStoreFactory.attach(
                client.client(),
                scheduler,
                new OxiaV2StoreConfiguration("localhost:6648", "test", "/nereus/test"),
                OxiaV2CodecSet.productionUnavailable());
    }
}
