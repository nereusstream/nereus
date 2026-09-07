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

package com.nereusstream.storage.bookkeeper;

import com.nereusstream.domain.bytes.CanonicalBytes;
import java.io.IOException;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.AbstractZkLedgerManager;
import org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerIdGenerator;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.MetadataClientDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.meta.exceptions.Code;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.meta.zk.ZKMetadataClientDriver;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.ZkUtils;

/** Explicit native metadata SPI profile. Every create and generated ID passes the durable task fence. */
public final class M5BookKeeperNativeMetadataDriverV2 extends ZKMetadataClientDriver {
    static final String DRIVER_SCHEME = "m5zk";
    static final String SPEC_PROPERTY = "nereusM5NativeCreateSpecV2";
    private M5BookKeeperNativeCreateGuardV2 guard;

    static void register() {
        MetadataDrivers.registerClientDriver(DRIVER_SCHEME, M5BookKeeperNativeMetadataDriverV2.class);
    }

    @Override
    public String getScheme() {
        return DRIVER_SCHEME;
    }

    @Override
    public synchronized MetadataClientDriver initialize(
            ClientConfiguration configuration,
            ScheduledExecutorService scheduler,
            StatsLogger statsLogger,
            Optional<Object> optionalContext)
            throws MetadataException {
        try {
            if (optionalContext.isPresent()
                    || !configuration.getMetadataServiceUri().startsWith(DRIVER_SCHEME + "://")) {
                throw new IllegalArgumentException("native M5 driver requires its own exact ZooKeeper connection");
            }
            var spec = M5BookKeeperNativeCreateSpecV2.decode(
                    CanonicalBytes.copyOf(Base64.getDecoder().decode(configuration.getString(SPEC_PROPERTY))));
            var nativeConfiguration = new ClientConfiguration(configuration);
            nativeConfiguration.setMetadataServiceUri(
                    "zk" + configuration.getMetadataServiceUri().substring(DRIVER_SCHEME.length()));
            super.initialize(nativeConfiguration, scheduler, statsLogger, Optional.empty());
            var layout = layoutManager.readLedgerLayout();
            if (layout == null
                    || layout.getLayoutFormatVersion() != 2
                    || layout.getManagerVersion() != 1
                    || !layout.getManagerFactoryClass().equals(HierarchicalLedgerManagerFactory.class.getName())) {
                throw new IllegalArgumentException("native M5 profile requires exact hierarchical layout version 1");
            }
            guard = new M5BookKeeperNativeCreateGuardV2(
                    zk, ledgersRootPath, spec, ZkUtils.getACLs(nativeConfiguration));
            var factory = new FencedFactory(spec);
            factory.initialize(nativeConfiguration, layoutManager, layout.getManagerVersion());
            lmFactory = factory;
            return this;
        } catch (Exception failure) {
            close();
            if (failure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new MetadataException(
                    Code.METADATA_SERVICE_ERROR, "native M5 create profile admission failed", failure);
        }
    }

    @Override
    public synchronized LedgerManagerFactory getLedgerManagerFactory() throws MetadataException {
        if (lmFactory == null || guard == null) {
            throw new MetadataException(Code.METADATA_SERVICE_ERROR, "native M5 create profile is not admitted");
        }
        return lmFactory;
    }

    public CompletableFuture<Void> fenceCreates() {
        return guard.fenceCreates();
    }

    M5BookKeeperNativeCreateGuardV2 guard() {
        return guard;
    }

    private final class FencedFactory extends HierarchicalLedgerManagerFactory {
        private final M5BookKeeperNativeCreateSpecV2 spec;

        private FencedFactory(M5BookKeeperNativeCreateSpecV2 spec) {
            this.spec = spec;
        }

        @Override
        public LedgerManager newLedgerManager() {
            var delegate = (AbstractZkLedgerManager) super.newLedgerManager();
            return new M5BookKeeperNativeLedgerManagerV2(delegate, guard, spec, getZk(), acls);
        }

        @Override
        public LedgerIdGenerator newLedgerIdGenerator() {
            var delegate = super.newLedgerIdGenerator();
            return new LedgerIdGenerator() {
                @Override
                public void generateLedgerId(GenericCallback<Long> callback) {
                    delegate.generateLedgerId((rc, ledgerId) -> {
                        if (rc != BKException.Code.OK) {
                            callback.operationComplete(rc, null);
                        } else {
                            guard.reserve(ledgerId)
                                    .whenComplete((ignored, failure) -> callback.operationComplete(
                                            failure == null ? BKException.Code.OK : BKException.Code.ZKException,
                                            failure == null ? ledgerId : null));
                        }
                    });
                }

                @Override
                public void close() throws IOException {
                    delegate.close();
                }
            };
        }
    }
}
