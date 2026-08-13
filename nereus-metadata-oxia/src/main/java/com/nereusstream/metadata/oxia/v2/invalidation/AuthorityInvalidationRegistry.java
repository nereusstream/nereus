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

import com.nereusstream.domain.protocol.PulsarTopicIncarnationIdentity;
import com.nereusstream.metadata.oxia.v2.continuity.InvalidationRegistration;
import com.nereusstream.metadata.oxia.v2.continuity.StoreContinuity;
import com.nereusstream.metadata.oxia.v2.continuity.StoreContinuityState;
import com.nereusstream.metadata.oxia.v2.key.OxiaV2AuthorityKeys;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.Notification;
import java.util.HashSet;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongConsumer;

/** Exact selector/aggregate plus store-wide continuity invalidation fan-out. */
public final class AuthorityInvalidationRegistry implements AutoCloseable {
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<OwnedRegistration>> registrations =
            new ConcurrentHashMap<>();
    private final InvalidationRegistration continuityRegistration;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final OxiaV2AuthorityKeys keys;
    private final StoreContinuity continuity;

    public AuthorityInvalidationRegistry(AsyncOxiaClient client, StoreContinuity continuity, OxiaV2AuthorityKeys keys) {
        Objects.requireNonNull(client, "client");
        this.keys = Objects.requireNonNull(keys, "keys");
        this.continuity = Objects.requireNonNull(continuity, "continuity");
        client.notifications(this::onNotification);
        continuityRegistration = continuity.onInvalidation(() -> invalidateAll(currentEpoch()));
    }

    /** Arms exact selector and aggregate invalidation before the caller performs authority reads. */
    public AuthorityInvalidationRegistration register(
            PulsarTopicIncarnationIdentity incarnation, LongConsumer invalidation) {
        Objects.requireNonNull(incarnation, "incarnation");
        Objects.requireNonNull(invalidation, "invalidation");
        if (closed.get()) {
            throw new IllegalStateException("authority invalidation registry is closed");
        }
        OwnedRegistration owned = new OwnedRegistration(invalidation);
        add(keys.selectorKey(incarnation.persistenceName()), owned);
        add(keys.aggregateKey(incarnation), owned);
        try {
            if (continuity.current().state() != StoreContinuityState.READY) {
                owned.invalidate(currentEpoch());
            }
        } catch (RuntimeException callbackFailure) {
            owned.close();
            throw callbackFailure;
        }
        if (closed.get()) {
            owned.close();
            throw new IllegalStateException("authority invalidation registry closed during registration");
        }
        return owned;
    }

    private void add(String key, OwnedRegistration registration) {
        registrations
                .computeIfAbsent(key, ignored -> new CopyOnWriteArrayList<>())
                .add(registration);
        registration.keys.add(key);
    }

    private void onNotification(Notification notification) {
        if (closed.get()) {
            return;
        }
        if (notification instanceof Notification.KeyRangeDelete range) {
            HashSet<OwnedRegistration> affected = new HashSet<>();
            registrations.forEach((key, listeners) -> {
                if (key.compareTo(range.startKeyInclusive()) >= 0 && key.compareTo(range.endKeyExclusive()) < 0) {
                    affected.addAll(listeners);
                }
            });
            invalidate(affected, currentEpoch());
            return;
        }
        CopyOnWriteArrayList<OwnedRegistration> listeners = registrations.get(notification.key());
        if (listeners != null) {
            invalidate(listeners, currentEpoch());
        }
    }

    private void invalidateAll(long invalidationEpoch) {
        HashSet<OwnedRegistration> affected = new HashSet<>();
        registrations.values().forEach(affected::addAll);
        invalidate(affected, invalidationEpoch);
    }

    private static void invalidate(Iterable<OwnedRegistration> affected, long invalidationEpoch) {
        RuntimeException failure = null;
        for (OwnedRegistration registration : affected) {
            try {
                registration.invalidate(invalidationEpoch);
            } catch (RuntimeException callbackFailure) {
                if (failure == null) {
                    failure = callbackFailure;
                } else {
                    failure.addSuppressed(callbackFailure);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private long currentEpoch() {
        return continuity.current().invalidationEpoch();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        RuntimeException failure = null;
        try {
            invalidateAll(currentEpoch());
        } catch (RuntimeException callbackFailure) {
            failure = callbackFailure;
        }
        try {
            continuityRegistration.close();
        } catch (RuntimeException closeFailure) {
            if (failure == null) {
                failure = closeFailure;
            } else {
                failure.addSuppressed(closeFailure);
            }
        } finally {
            registrations.clear();
        }
        if (failure != null) {
            throw failure;
        }
    }

    private final class OwnedRegistration implements AuthorityInvalidationRegistration {
        private final CopyOnWriteArrayList<String> keys = new CopyOnWriteArrayList<>();
        private final LongConsumer invalidation;
        private final AtomicBoolean active = new AtomicBoolean(true);

        private OwnedRegistration(LongConsumer invalidation) {
            this.invalidation = invalidation;
        }

        private void invalidate(long invalidationEpoch) {
            if (!active.get()) {
                return;
            }
            invalidation.accept(invalidationEpoch);
        }

        @Override
        public void close() {
            if (!active.compareAndSet(true, false)) {
                return;
            }
            keys.forEach(key -> {
                CopyOnWriteArrayList<OwnedRegistration> listeners = registrations.get(key);
                if (listeners != null) {
                    listeners.remove(this);
                    if (listeners.isEmpty()) {
                        registrations.remove(key, listeners);
                    }
                }
            });
            keys.clear();
        }
    }
}
