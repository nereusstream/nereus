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

import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.NotificationContinuityRegistration;
import io.oxia.client.api.NotificationContinuitySnapshot;
import io.oxia.client.api.NotificationContinuityState;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/** Bridges O1 client continuity into one process-local, store-wide invalidation word. */
public final class StoreContinuity implements AutoCloseable {
    private static final StoreContinuitySnapshot INITIAL =
            new StoreContinuitySnapshot(StoreContinuityState.ARMING, 0, 1);

    private final Object transitionLock = new Object();
    private final AtomicReference<StoreContinuitySnapshot> snapshot = new AtomicReference<>(INITIAL);
    private final RevalidationScheduler scheduler;
    private NotificationContinuityRegistration registration;
    private boolean closeStarted;

    private StoreContinuity(RevalidationScheduler scheduler) {
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    }

    public static StoreContinuity attach(AsyncOxiaClient client, RevalidationScheduler scheduler) {
        Objects.requireNonNull(client, "client");
        StoreContinuity continuity = new StoreContinuity(scheduler);
        NotificationContinuityRegistration ownedRegistration =
                client.notificationContinuity(continuity::onClientSnapshot);
        try {
            synchronized (continuity.transitionLock) {
                continuity.registration = ownedRegistration;
            }
            continuity.onClientSnapshot(ownedRegistration.current());
            return continuity;
        } catch (RuntimeException | Error failure) {
            ownedRegistration.close();
            continuity.closeLocally();
            throw failure;
        }
    }

    public StoreContinuitySnapshot current() {
        return snapshot.get();
    }

    Optional<InstallPermit> captureInstallPermit() {
        StoreContinuitySnapshot current = snapshot.get();
        if (current.state() != StoreContinuityState.READY) {
            return Optional.empty();
        }
        return Optional.of(new InstallPermit(current.clientGeneration(), current.invalidationEpoch()));
    }

    boolean isCurrent(InstallPermit permit) {
        Objects.requireNonNull(permit, "permit");
        StoreContinuitySnapshot current = snapshot.get();
        return current.state() == StoreContinuityState.READY
                && current.clientGeneration() == permit.clientGeneration()
                && current.invalidationEpoch() == permit.invalidationEpoch();
    }

    private void onClientSnapshot(NotificationContinuitySnapshot clientSnapshot) {
        Objects.requireNonNull(clientSnapshot, "clientSnapshot");
        StoreContinuitySnapshot request = transition(clientSnapshot);
        if (request == null) {
            return;
        }
        boolean accepted;
        try {
            accepted = scheduler.request(request.clientGeneration(), request.invalidationEpoch());
        } catch (RuntimeException failure) {
            accepted = false;
        }
        if (!accepted) {
            failClosedIfCurrent(request);
        }
    }

    private StoreContinuitySnapshot transition(NotificationContinuitySnapshot clientSnapshot) {
        synchronized (transitionLock) {
            StoreContinuitySnapshot current = snapshot.get();
            if (closeStarted || current.state() == StoreContinuityState.CLOSED) {
                return null;
            }
            long generation = clientSnapshot.generation();
            if (generation < current.clientGeneration()) {
                return null;
            }

            if (clientSnapshot.state() == NotificationContinuityState.CLOSED) {
                publishClosed(current, generation);
                return null;
            }

            boolean newGeneration = generation > current.clientGeneration();
            if (clientSnapshot.state() == NotificationContinuityState.ARMING) {
                if (newGeneration || current.state() == StoreContinuityState.READY) {
                    snapshot.set(
                            new StoreContinuitySnapshot(StoreContinuityState.ARMING, generation, nextEpoch(current)));
                }
                return null;
            }

            if (clientSnapshot.state() != NotificationContinuityState.READY) {
                publishClosed(current, generation);
                return null;
            }
            if (!newGeneration && current.state() == StoreContinuityState.READY) {
                return null;
            }

            long epoch = newGeneration ? nextEpoch(current) : current.invalidationEpoch();
            StoreContinuitySnapshot ready = new StoreContinuitySnapshot(StoreContinuityState.READY, generation, epoch);
            snapshot.set(ready);
            return ready;
        }
    }

    private void failClosedIfCurrent(StoreContinuitySnapshot expectedReady) {
        synchronized (transitionLock) {
            StoreContinuitySnapshot current = snapshot.get();
            if (current.equals(expectedReady)) {
                snapshot.set(new StoreContinuitySnapshot(
                        StoreContinuityState.ARMING, current.clientGeneration(), nextEpoch(current)));
            }
        }
    }

    private void publishClosed(StoreContinuitySnapshot current, long generation) {
        snapshot.set(new StoreContinuitySnapshot(
                StoreContinuityState.CLOSED, Math.max(generation, current.clientGeneration()), nextEpoch(current)));
    }

    private static long nextEpoch(StoreContinuitySnapshot current) {
        if (current.invalidationEpoch() == Long.MAX_VALUE) {
            throw new IllegalStateException("continuity invalidation epoch exhausted");
        }
        return current.invalidationEpoch() + 1;
    }

    @Override
    public void close() {
        NotificationContinuityRegistration ownedRegistration;
        synchronized (transitionLock) {
            if (closeStarted) {
                return;
            }
            closeStarted = true;
            StoreContinuitySnapshot current = snapshot.get();
            if (current.state() != StoreContinuityState.CLOSED) {
                publishClosed(current, current.clientGeneration());
            }
            ownedRegistration = registration;
            registration = null;
        }
        if (ownedRegistration != null) {
            ownedRegistration.close();
        }
    }

    private void closeLocally() {
        synchronized (transitionLock) {
            closeStarted = true;
            StoreContinuitySnapshot current = snapshot.get();
            if (current.state() != StoreContinuityState.CLOSED) {
                publishClosed(current, current.clientGeneration());
            }
        }
    }
}
