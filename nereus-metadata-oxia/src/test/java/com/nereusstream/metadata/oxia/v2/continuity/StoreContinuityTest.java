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

package com.nereusstream.metadata.oxia.v2.continuity;

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.metadata.oxia.v2.testing.FakeO1ContinuityClient;
import com.nereusstream.metadata.oxia.v2.testing.RecordingRevalidationScheduler;
import io.oxia.client.api.NotificationContinuityState;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;

class StoreContinuityTest {
    @Test
    void initialArmingSnapshotIsFailClosed() {
        FakeO1ContinuityClient client = new FakeO1ContinuityClient(1, NotificationContinuityState.ARMING);
        RecordingRevalidationScheduler scheduler = new RecordingRevalidationScheduler();

        StoreContinuity continuity = StoreContinuity.attach(client.client(), scheduler);

        assertThat(continuity.current().state()).isEqualTo(StoreContinuityState.ARMING);
        assertThat(continuity.current().clientGeneration()).isOne();
        assertThat(continuity.captureInstallPermit()).isEmpty();
        assertThat(scheduler.requests()).isEmpty();
    }

    @Test
    void armingToReadySchedulesAuthoritativeRevalidationOnly() {
        FakeO1ContinuityClient client = new FakeO1ContinuityClient(1, NotificationContinuityState.ARMING);
        RecordingRevalidationScheduler scheduler = new RecordingRevalidationScheduler();
        StoreContinuity continuity = StoreContinuity.attach(client.client(), scheduler);

        client.emit(1, NotificationContinuityState.READY);

        assertThat(continuity.current().state()).isEqualTo(StoreContinuityState.READY);
        assertThat(scheduler.requests())
                .containsExactly(new RecordingRevalidationScheduler.Request(
                        1, continuity.current().invalidationEpoch()));
        assertThat(continuity.captureInstallPermit()).isPresent();
    }

    @Test
    void generationLossInvalidatesBeforeTheNextReadyRequest() {
        FakeO1ContinuityClient client = new FakeO1ContinuityClient(1, NotificationContinuityState.READY);
        RecordingRevalidationScheduler scheduler = new RecordingRevalidationScheduler();
        StoreContinuity continuity = StoreContinuity.attach(client.client(), scheduler);
        InstallPermit stale = continuity.captureInstallPermit().orElseThrow();
        long firstEpoch = continuity.current().invalidationEpoch();

        client.emit(2, NotificationContinuityState.ARMING);

        assertThat(continuity.current().state()).isEqualTo(StoreContinuityState.ARMING);
        assertThat(continuity.current().invalidationEpoch()).isGreaterThan(firstEpoch);
        assertThat(continuity.isCurrent(stale)).isFalse();

        client.emit(2, NotificationContinuityState.READY);
        assertThat(scheduler.requests().getLast().invalidationEpoch())
                .isEqualTo(continuity.current().invalidationEpoch());
    }

    @Test
    void repeatedLossForTheSameGenerationIsMonotonicAndFailClosed() {
        FakeO1ContinuityClient client = new FakeO1ContinuityClient(1, NotificationContinuityState.READY);
        StoreContinuity continuity = StoreContinuity.attach(client.client(), new RecordingRevalidationScheduler());

        client.emit(1, NotificationContinuityState.ARMING);
        long invalidatedEpoch = continuity.current().invalidationEpoch();
        client.emit(1, NotificationContinuityState.ARMING);

        assertThat(continuity.current().state()).isEqualTo(StoreContinuityState.ARMING);
        assertThat(continuity.current().invalidationEpoch()).isEqualTo(invalidatedEpoch);
    }

    @Test
    void reassignmentAdvancesGenerationAndInvalidationEpoch() {
        FakeO1ContinuityClient client = new FakeO1ContinuityClient(1, NotificationContinuityState.READY);
        StoreContinuity continuity = StoreContinuity.attach(client.client(), new RecordingRevalidationScheduler());
        long firstEpoch = continuity.current().invalidationEpoch();

        client.emit(3, NotificationContinuityState.ARMING);

        assertThat(continuity.current().clientGeneration()).isEqualTo(3);
        assertThat(continuity.current().invalidationEpoch()).isGreaterThan(firstEpoch);
    }

    @Test
    void staleOlderGenerationCallbackCannotRegressReadyState() {
        FakeO1ContinuityClient client = new FakeO1ContinuityClient(2, NotificationContinuityState.READY);
        StoreContinuity continuity = StoreContinuity.attach(client.client(), new RecordingRevalidationScheduler());
        StoreContinuitySnapshot before = continuity.current();

        client.emit(1, NotificationContinuityState.ARMING);

        assertThat(continuity.current()).isEqualTo(before);
    }

    @Test
    void closedIsTerminalAndRejectsInstallPermit() {
        FakeO1ContinuityClient client = new FakeO1ContinuityClient(1, NotificationContinuityState.READY);
        RecordingRevalidationScheduler scheduler = new RecordingRevalidationScheduler();
        StoreContinuity continuity = StoreContinuity.attach(client.client(), scheduler);

        client.emit(1, NotificationContinuityState.CLOSED);
        client.emit(2, NotificationContinuityState.READY);

        assertThat(continuity.current().state()).isEqualTo(StoreContinuityState.CLOSED);
        assertThat(continuity.captureInstallPermit()).isEmpty();
    }

    @Test
    void registrationCallbackBeforeHandleReturnIsReconciled() {
        FakeO1ContinuityClient client = new FakeO1ContinuityClient(1, NotificationContinuityState.READY);
        client.callbackBeforeRegistrationReturns();
        RecordingRevalidationScheduler scheduler = new RecordingRevalidationScheduler();

        StoreContinuity continuity = StoreContinuity.attach(client.client(), scheduler);

        assertThat(continuity.current().state()).isEqualTo(StoreContinuityState.READY);
        assertThat(scheduler.requests()).hasSize(1);
    }

    @Test
    void rejectedRecoveryRequestReturnsToFailClosedState() {
        FakeO1ContinuityClient client = new FakeO1ContinuityClient(1, NotificationContinuityState.ARMING);
        RecordingRevalidationScheduler scheduler = new RecordingRevalidationScheduler();
        scheduler.rejectRequests();
        StoreContinuity continuity = StoreContinuity.attach(client.client(), scheduler);

        client.emit(1, NotificationContinuityState.READY);

        assertThat(continuity.current().state()).isEqualTo(StoreContinuityState.ARMING);
        assertThat(continuity.captureInstallPermit()).isEmpty();
    }

    @Test
    void closeIsIdempotentAndDeregistersListener() {
        FakeO1ContinuityClient client = new FakeO1ContinuityClient(1, NotificationContinuityState.READY);
        StoreContinuity continuity = StoreContinuity.attach(client.client(), new RecordingRevalidationScheduler());

        continuity.close();
        continuity.close();

        assertThat(continuity.current().state()).isEqualTo(StoreContinuityState.CLOSED);
        assertThat(client.registrationClosed()).isTrue();
        assertThat(client.lifecycleEvents()).containsOnlyOnce("registration-close");
    }

    @Test
    void callbacksPerformNoRemoteClientOperation() {
        FakeO1ContinuityClient client = new FakeO1ContinuityClient(1, NotificationContinuityState.ARMING);
        StoreContinuity.attach(client.client(), new RecordingRevalidationScheduler());

        client.emit(1, NotificationContinuityState.READY);
        client.emit(2, NotificationContinuityState.ARMING);

        assertThat(client.unsupportedOperationCount()).isZero();
    }

    @Test
    void differentStoresRemainIsolatedAcrossLossAndClose() {
        FakeO1ContinuityClient firstClient = new FakeO1ContinuityClient(1, NotificationContinuityState.READY);
        FakeO1ContinuityClient secondClient = new FakeO1ContinuityClient(1, NotificationContinuityState.READY);
        StoreContinuity first = StoreContinuity.attach(firstClient.client(), new RecordingRevalidationScheduler());
        StoreContinuity second = StoreContinuity.attach(secondClient.client(), new RecordingRevalidationScheduler());

        firstClient.emit(2, NotificationContinuityState.ARMING);
        first.close();

        assertThat(first.current().state()).isEqualTo(StoreContinuityState.CLOSED);
        assertThat(second.current().state()).isEqualTo(StoreContinuityState.READY);
        assertThat(second.captureInstallPermit()).isPresent();
    }

    @Test
    void delayedRemoteCompletionCannotRestoreAStaleInstaller() {
        FakeO1ContinuityClient client = new FakeO1ContinuityClient(1, NotificationContinuityState.READY);
        StoreContinuity continuity = StoreContinuity.attach(client.client(), new RecordingRevalidationScheduler());
        InstallPermit permit = continuity.captureInstallPermit().orElseThrow();
        CompletableFuture<Void> delayedRemoteRead = new CompletableFuture<>();

        client.emit(2, NotificationContinuityState.ARMING);
        delayedRemoteRead.complete(null);

        assertThat(delayedRemoteRead).isCompleted();
        assertThat(continuity.isCurrent(permit)).isFalse();
    }

    @Test
    void readyVersusCloseRaceAlwaysEndsTerminalClosed() throws Exception {
        FakeO1ContinuityClient client = new FakeO1ContinuityClient(1, NotificationContinuityState.ARMING);
        StoreContinuity continuity = StoreContinuity.attach(client.client(), new RecordingRevalidationScheduler());
        CyclicBarrier start = new CyclicBarrier(2);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var ready = executor.submit(() -> {
                start.await();
                client.emit(1, NotificationContinuityState.READY);
                return null;
            });
            var close = executor.submit(() -> {
                start.await();
                continuity.close();
                return null;
            });
            ready.get();
            close.get();
        }

        assertThat(continuity.current().state()).isEqualTo(StoreContinuityState.CLOSED);
        assertThat(continuity.captureInstallPermit()).isEmpty();
    }
}
