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

package com.nereusstream.spike;

import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;
import io.opentelemetry.api.OpenTelemetry;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.TerminateCallback;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.storage.ManagedLedgerStorage;
import org.apache.pulsar.broker.storage.ManagedLedgerStorageClass;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

/** Compile-only probe for the exact Pulsar interfaces used by Nereus Future 2. */
final class PulsarManagedLedgerApiProbe {
    private PulsarManagedLedgerApiProbe() {
    }

    static void initializeStorage(
            ManagedLedgerStorage storage,
            ServiceConfiguration configuration,
            MetadataStoreExtended metadataStore,
            BookKeeperClientFactory bookKeeperClientFactory,
            EventLoopGroup eventLoopGroup,
            OpenTelemetry openTelemetry) throws Exception {
        storage.initialize(
                configuration,
                metadataStore,
                bookKeeperClientFactory,
                eventLoopGroup,
                openTelemetry);
    }

    static Collection<ManagedLedgerStorageClass> storage(ManagedLedgerStorage storage, String name) {
        ManagedLedgerStorageClass defaultClass = storage.getDefaultStorageClass();
        Optional<ManagedLedgerStorageClass> selected = storage.getManagedLedgerStorageClass(name);
        ManagedLedgerFactory factory = defaultClass.getManagedLedgerFactory();
        if (selected.isEmpty() || factory == null || defaultClass.getName().isBlank()) {
            throw new IllegalStateException();
        }
        return storage.getStorageClasses();
    }

    static void factory(
            ManagedLedgerFactory factory,
            String name,
            ManagedLedgerConfig config,
            Position start,
            OpenLedgerCallback callback,
            Supplier<CompletableFuture<Boolean>> ownershipChecker,
            Object ctx) throws Exception {
        factory.open(name, config);
        factory.asyncOpen(name, config, callback, ownershipChecker, ctx);
        ReadOnlyCursor cursor = factory.openReadOnlyCursor(name, start, config);
        factory.asyncExists(name);
        factory.getManagedLedgerPropertiesAsync(name);
        factory.getManagedLedgers();
        factory.getConfig();
        cursor.close();
    }

    static void ledger(
            ManagedLedger ledger,
            ByteBuf entry,
            int numberOfMessages,
            AddEntryCallback addCallback,
            OpenCursorCallback cursorCallback,
            ReadEntryCallback readCallback,
            TerminateCallback terminateCallback,
            Position position,
            Object ctx) {
        ledger.asyncAddEntry(entry, numberOfMessages, addCallback, ctx);
        ledger.asyncOpenCursor("cursor", InitialPosition.Earliest, Map.of(), Map.of(), cursorCallback, ctx);
        ledger.asyncReadEntry(position, readCallback, ctx);
        ledger.asyncTerminate(terminateCallback, ctx);
        ledger.getLastConfirmedEntry();
        ledger.getFirstPosition();
        ledger.getNumberOfEntries();
        ledger.getTotalSize();
        ledger.getLedgersInfo();
    }

    static void cursor(
            ManagedCursor cursor,
            int maxEntries,
            long maxBytes,
            ReadEntriesCallback callback,
            Position maxPosition,
            Object ctx) {
        cursor.asyncReadEntries(maxEntries, maxBytes, callback, ctx, maxPosition);
        cursor.asyncReadEntriesOrWait(maxEntries, maxBytes, callback, ctx, maxPosition);
        cursor.cancelPendingReadRequest();
        cursor.getReadPosition();
        cursor.getMarkDeletedPosition();
        cursor.getPersistentMarkDeletedPosition();
        cursor.hasMoreEntries();
        cursor.getNumberOfEntries();
        cursor.isDurable();
        cursor.isClosed();
    }
}
