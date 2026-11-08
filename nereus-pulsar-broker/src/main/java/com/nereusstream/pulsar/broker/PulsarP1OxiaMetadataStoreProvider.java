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

package com.nereusstream.pulsar.broker;

import io.oxia.client.api.NotificationContinuityRegistration;
import io.oxia.client.api.NotificationContinuitySnapshot;
import io.oxia.client.api.NotificationContinuityState;
import java.net.URI;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreProvider;
import org.apache.pulsar.metadata.api.extended.NereusOwnershipMetadataStore;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.apache.pulsar.metadata.impl.oxia.OxiaMetadataStore;
import org.apache.pulsar.metadata.impl.oxia.OxiaMetadataStoreProvider;

/** Native Pulsar MetadataStore using the same qualified Oxia stream for ownership notifications and witnesses. */
public final class PulsarP1OxiaMetadataStoreProvider implements MetadataStoreProvider {
    @Override
    public String urlScheme() {
        return "oxia-nereus";
    }

    @Override
    public MetadataStore create(String url, MetadataStoreConfig config, boolean enableSessionWatcher)
            throws MetadataStoreException {
        try {
            var address = URI.create(url);
            if (!urlScheme().equals(address.getScheme())
                    || address.getHost() == null
                    || address.getPort() <= 0
                    || address.getUserInfo() != null
                    || address.getRawQuery() != null
                    || address.getRawFragment() != null) {
                throw new IllegalArgumentException("invalid native Pulsar Oxia metadata origin");
            }
            var path = address.getRawPath();
            var namespace = path.isEmpty() ? OxiaMetadataStoreProvider.DefaultNamespace : path.substring(1);
            if (!namespace.matches("[A-Za-z0-9][A-Za-z0-9._-]*")) {
                throw new IllegalArgumentException("invalid native Pulsar Oxia namespace");
            }
            return new QualifiedStore(address.getRawAuthority(), namespace, config, enableSessionWatcher);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new MetadataStoreException(interrupted);
        } catch (Exception failure) {
            throw new MetadataStoreException(failure);
        }
    }

    private static final class QualifiedStore extends OxiaMetadataStore implements NereusOwnershipMetadataStore {
        private final CopyOnWriteArrayList<Runnable> invalidations = new CopyOnWriteArrayList<>();
        private volatile Continuity continuity = new Continuity(0, false);
        private long clientGeneration = -1;
        private boolean closing;
        private final NotificationContinuityRegistration registration;

        private QualifiedStore(String address, String namespace, MetadataStoreConfig config, boolean watch)
                throws Exception {
            super(address, namespace, config, watch);
            NotificationContinuityRegistration attached = null;
            try {
                attached = ownershipClient().notificationContinuity(this::observe);
                attached.current()
                        .readyStage()
                        .toCompletableFuture()
                        .get(config.getSessionTimeoutMillis(), TimeUnit.MILLISECONDS);
                observe(attached.current());
                if (!continuity.ready()) {
                    throw new IllegalStateException("native Pulsar Oxia continuity changed during startup");
                }
                registration = attached;
            } catch (Exception | LinkageError failure) {
                if (attached != null) {
                    try {
                        attached.close();
                    } catch (RuntimeException cleanup) {
                        failure.addSuppressed(cleanup);
                    }
                }
                try {
                    super.close();
                } catch (Exception cleanup) {
                    failure.addSuppressed(cleanup);
                }
                throw failure;
            }
        }

        @Override
        public Continuity ownershipContinuity() {
            return continuity;
        }

        @Override
        public AutoCloseable onOwnershipInvalidated(Runnable listener) {
            invalidations.add(Objects.requireNonNull(listener, "listener"));
            if (!continuity.ready()) {
                listener.run();
            }
            return () -> invalidations.remove(listener);
        }

        private synchronized void observe(NotificationContinuitySnapshot observed) {
            if (closing || observed.generation() < clientGeneration) {
                return;
            }
            var before = continuity;
            var ready = observed.state() == NotificationContinuityState.READY;
            if (observed.generation() == clientGeneration && before.ready() == ready) {
                return;
            }
            clientGeneration = observed.generation();
            var epoch = Math.incrementExact(before.epoch());
            // A new READY generation still invalidates every old permit before native work resumes.
            if (before.ready() || !ready) {
                continuity = new Continuity(epoch, false);
                invalidate();
            }
            if (ready) {
                continuity = new Continuity(epoch, true);
                receivedSessionEvent(SessionEvent.Reconnected);
            }
        }

        private void invalidate() {
            invalidations.forEach(Runnable::run);
            receivedSessionEvent(SessionEvent.ConnectionLost);
        }

        @Override
        public void close() throws Exception {
            synchronized (this) {
                if (!closing) {
                    closing = true;
                    continuity = new Continuity(Math.incrementExact(continuity.epoch()), false);
                    invalidate();
                    invalidations.clear();
                }
            }
            try {
                registration.close();
            } finally {
                super.close();
            }
        }
    }
}
