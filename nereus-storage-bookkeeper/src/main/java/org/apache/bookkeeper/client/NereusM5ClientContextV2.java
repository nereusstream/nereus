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

package org.apache.bookkeeper.client;

import io.netty.buffer.ByteBufAllocator;
import java.util.Objects;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.proto.BookieClient;

/** BK4.18 ClientInternalConf is package-private; this bridge replaces only the original data port. */
public final class NereusM5ClientContextV2 implements ClientContext {
    private final ClientContext original;
    private final BookieClient dataPort;

    public NereusM5ClientContextV2(ClientContext original, BookieClient dataPort) {
        this.original = Objects.requireNonNull(original, "original");
        this.dataPort = Objects.requireNonNull(dataPort, "dataPort");
    }

    public ClientInternalConf getConf() {
        return original.getConf();
    }

    public LedgerManager getLedgerManager() {
        return original.getLedgerManager();
    }

    public BookieWatcher getBookieWatcher() {
        return original.getBookieWatcher();
    }

    public EnsemblePlacementPolicy getPlacementPolicy() {
        return original.getPlacementPolicy();
    }

    public BookieClient getBookieClient() {
        return dataPort;
    }

    public ByteBufAllocator getByteBufAllocator() {
        return original.getByteBufAllocator();
    }

    public OrderedExecutor getMainWorkerPool() {
        return original.getMainWorkerPool();
    }

    public OrderedScheduler getScheduler() {
        return original.getScheduler();
    }

    public BookKeeperClientStats getClientStats() {
        return original.getClientStats();
    }

    public boolean isClientClosed() {
        return original.isClientClosed();
    }
}
