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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

/** Bridges O1 client continuity into one process-local, store-wide invalidation word. */
public final class StoreContinuity implements AutoCloseable {
    private static final StoreContinuitySnapshot INITIAL =
            new StoreContinuitySnapshot(StoreContinuityState.ARMING, 0, 1);

    private final Object transitionLock = new Object();
    private final AtomicReference<StoreContinuitySnapshot> snapshot = new AtomicReference<>(INITIAL);
    private final CopyOnWriteArrayList<Runnable> invalidationListeners = new CopyOnWriteArrayList<>();
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

    /**
     * Registers a non-blocking callback for every store-wide invalidation. READY never invokes this
     * callback and never grants authority.
     */
    public InvalidationRegistration onInvalidation(Runnable listener) {
        Objects.requireNonNull(listener, "listener");
        invalidationListeners.add(listener);
        AtomicReference<Runnable> owned = new AtomicReference<>(listener);
        if (snapshot.get().state() != StoreContinuityState.READY) {
            invoke(listener);
        }
        return () -> {
            Runnable registered = owned.getAndSet(null);
            if (registered != null) {
                invalidationListeners.remove(registered);
            }
        };
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
        StoreContinuitySnapshot before = snapshot.get();
        StoreContinuitySnapshot request = transition(clientSnapshot);
        notifyIfInvalidated(before, snapshot.get());
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
            if (failClosedIfCurrent(request)) {
                notifyInvalidationListeners();
            }
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

    private boolean failClosedIfCurrent(StoreContinuitySnapshot expectedReady) {
        synchronized (transitionLock) {
            StoreContinuitySnapshot current = snapshot.get();
            if (current.equals(expectedReady)) {
                snapshot.set(new StoreContinuitySnapshot(
                        StoreContinuityState.ARMING, current.clientGeneration(), nextEpoch(current)));
                return true;
            }
            return false;
        }
    }

    private void notifyIfInvalidated(StoreContinuitySnapshot before, StoreContinuitySnapshot after) {
        if (after.invalidationEpoch() != before.invalidationEpoch()
                || (after.state() == StoreContinuityState.CLOSED && before.state() != StoreContinuityState.CLOSED)) {
            notifyInvalidationListeners();
        }
    }

    private void notifyInvalidationListeners() {
        invalidationListeners.forEach(StoreContinuity::invoke);
    }

    private static void invoke(Runnable listener) {
        try {
            listener.run();
        } catch (RuntimeException ignored) {
            // Invalidation fan-out must remain fail-isolated and must never trigger remote work here.
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
        boolean invalidated = false;
        synchronized (transitionLock) {
            if (closeStarted) {
                return;
            }
            closeStarted = true;
            StoreContinuitySnapshot current = snapshot.get();
            if (current.state() != StoreContinuityState.CLOSED) {
                publishClosed(current, current.clientGeneration());
                invalidated = true;
            }
            ownedRegistration = registration;
            registration = null;
        }
        if (invalidated) {
            notifyInvalidationListeners();
        }
        if (ownedRegistration != null) {
            ownedRegistration.close();
        }
    }

    private void closeLocally() {
        boolean invalidated = false;
        synchronized (transitionLock) {
            closeStarted = true;
            StoreContinuitySnapshot current = snapshot.get();
            if (current.state() != StoreContinuityState.CLOSED) {
                publishClosed(current, current.clientGeneration());
                invalidated = true;
            }
        }
        if (invalidated) {
            notifyInvalidationListeners();
        }
    }
}
