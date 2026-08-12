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

package com.nereusstream.metadata.oxia.v2.invalidation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.metadata.oxia.v2.continuity.StoreContinuity;
import com.nereusstream.metadata.oxia.v2.key.OxiaV2AuthorityKeys;
import com.nereusstream.metadata.oxia.v2.testing.FakeO1ContinuityClient;
import com.nereusstream.metadata.oxia.v2.testing.O2TestValues;
import com.nereusstream.metadata.oxia.v2.testing.RecordingRevalidationScheduler;
import io.oxia.client.api.Notification;
import io.oxia.client.api.NotificationContinuityState;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class AuthorityInvalidationRegistryTest {
    @Test
    void exactRecordAndContinuityEventsInvalidateButUnrelatedKeysDoNot() {
        FakeO1ContinuityClient client = new FakeO1ContinuityClient(1, NotificationContinuityState.READY);
        StoreContinuity continuity = StoreContinuity.attach(client.client(), new RecordingRevalidationScheduler());
        OxiaV2AuthorityKeys keys = new OxiaV2AuthorityKeys("/nereus/test");
        AuthorityInvalidationRegistry registry = new AuthorityInvalidationRegistry(client.client(), continuity, keys);
        AtomicInteger invalidations = new AtomicInteger();
        var incarnation = O2TestValues.incarnation(1);
        var registration = registry.register(incarnation, invalidations::incrementAndGet);

        client.emit(new Notification.KeyModified("/unrelated", 1));
        client.emit(new Notification.KeyModified(keys.selectorKey(incarnation.persistenceName()), 2));
        client.emit(new Notification.KeyDeleted(keys.aggregateKey(incarnation)));
        client.emit(2, NotificationContinuityState.ARMING);
        client.emit(2, NotificationContinuityState.READY);

        assertThat(invalidations).hasValue(3);
        registration.close();
        client.emit(new Notification.KeyModified(keys.selectorKey(incarnation.persistenceName()), 3));
        assertThat(invalidations).hasValue(3);
    }

    @Test
    void rangeDeleteAndCloseInvalidateActiveRegistrations() {
        FakeO1ContinuityClient client = new FakeO1ContinuityClient(1, NotificationContinuityState.READY);
        StoreContinuity continuity = StoreContinuity.attach(client.client(), new RecordingRevalidationScheduler());
        OxiaV2AuthorityKeys keys = new OxiaV2AuthorityKeys("/nereus/test");
        AuthorityInvalidationRegistry registry = new AuthorityInvalidationRegistry(client.client(), continuity, keys);
        AtomicInteger invalidations = new AtomicInteger();
        registry.register(O2TestValues.incarnation(1), invalidations::incrementAndGet);

        client.emit(new Notification.KeyRangeDelete("/nereus/test/", "/nereus/test0"));
        registry.close();

        assertThat(invalidations.get()).isGreaterThanOrEqualTo(2);
        assertThatThrownBy(() -> registry.register(O2TestValues.incarnation(1), () -> {}))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void registrationDuringArmingCannotAppearValid() {
        FakeO1ContinuityClient client = new FakeO1ContinuityClient(1, NotificationContinuityState.ARMING);
        StoreContinuity continuity = StoreContinuity.attach(client.client(), new RecordingRevalidationScheduler());
        AuthorityInvalidationRegistry registry =
                new AuthorityInvalidationRegistry(client.client(), continuity, new OxiaV2AuthorityKeys("/nereus/test"));
        AtomicInteger invalidations = new AtomicInteger();

        registry.register(O2TestValues.incarnation(1), invalidations::incrementAndGet);

        assertThat(invalidations).hasValue(1);
    }
}
