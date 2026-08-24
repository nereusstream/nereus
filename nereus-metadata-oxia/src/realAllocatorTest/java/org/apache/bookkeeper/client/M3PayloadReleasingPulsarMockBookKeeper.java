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

import io.netty.buffer.ByteBuf;
import java.security.GeneralSecurityException;
import java.util.Map;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.CreateCallback;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedExecutor;

/**
 * Pulsar's exact mock BookKeeper ledger-ID path with acknowledged payload bytes released immediately.
 *
 * <p>The formal baseline exercises the pinned production ManagedLedger decision and native BookKeeper ledger create
 * path. It never reads entry bodies. Retaining every 64-KiB body would make the frozen 100,000-ledger population and
 * 970,560 native requests require hundreds of GiB, so this evidence-only backend retains entry ordinals and byte
 * counters but releases bodies after the real async add callback. It is not evidence for a BookKeeper provider.
 */
public final class M3PayloadReleasingPulsarMockBookKeeper extends PulsarMockBookKeeper {
    public M3PayloadReleasingPulsarMockBookKeeper(OrderedExecutor orderedExecutor) throws Exception {
        super(orderedExecutor);
    }

    @Override
    public void asyncCreateLedger(
            int ensembleSize,
            int writeQuorumSize,
            int ackQuorumSize,
            DigestType digestType,
            byte[] password,
            CreateCallback callback,
            Object context,
            Map<String, byte[]> properties) {
        getProgrammedFailure()
                .thenComposeAsync(ignored -> {
                    try {
                        long ledgerId = sequence.getAndIncrement();
                        PulsarMockLedgerHandle handle = new PayloadReleasingLedgerHandle(
                                this, ledgerId, digestType, password, properties);
                        ledgers.put(ledgerId, handle);
                        return FutureUtils.value(handle);
                    } catch (Throwable failure) {
                        return FutureUtils.<PulsarMockLedgerHandle>exception(failure);
                    }
                }, executor)
                .whenCompleteAsync((handle, failure) -> {
                    if (failure == null) {
                        callback.createComplete(BKException.Code.OK, handle, context);
                    } else {
                        callback.createComplete(getExceptionCode(failure), null, context);
                    }
                }, executor);
    }

    /** Discards a closed predecessor handle after ManagedLedger has durably advanced its own metadata. */
    public void discardClosedLedger(long ledgerId) {
        PulsarMockLedgerHandle removed = ledgers.remove(ledgerId);
        if (removed == null) {
            throw new IllegalStateException("native predecessor ledger is absent from the exact ledger-ID inventory");
        }
        synchronized (removed.entries) {
            removed.entries.forEach(LedgerEntryImpl::close);
            removed.entries.clear();
        }
    }

    public long retainedPayloadBytes() {
        long retained = 0;
        for (PulsarMockLedgerHandle handle : ledgers.values()) {
            synchronized (handle.entries) {
                for (LedgerEntryImpl entry : handle.entries) {
                    if (entry.getEntryBuffer() != null) {
                        retained = Math.addExact(retained, entry.getEntryBuffer().readableBytes());
                    }
                }
            }
        }
        return retained;
    }

    private static final class PayloadReleasingLedgerHandle extends PulsarMockLedgerHandle {
        private PayloadReleasingLedgerHandle(
                PulsarMockBookKeeper bookKeeper,
                long ledgerId,
                DigestType digestType,
                byte[] password,
                Map<String, byte[]> properties)
                throws GeneralSecurityException {
            super(bookKeeper, ledgerId, digestType, password, properties);
        }

        @Override
        public void asyncAddEntry(ByteBuf data, AddCallback callback, Object context) {
            super.asyncAddEntry(data, (result, handle, entryId, ignored) -> {
                if (result == BKException.Code.OK) {
                    LedgerEntryImpl payload;
                    synchronized (entries) {
                        payload = entries.set(
                                Math.toIntExact(entryId), LedgerEntryImpl.create(ledgerId, entryId));
                    }
                    payload.close();
                }
                callback.addComplete(result, handle, entryId, context);
            }, context);
        }
    }
}
