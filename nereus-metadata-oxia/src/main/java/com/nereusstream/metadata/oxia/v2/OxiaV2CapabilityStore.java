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

package com.nereusstream.metadata.oxia.v2;

import com.nereusstream.metadata.oxia.v2.continuity.RevalidationScheduler;
import com.nereusstream.metadata.oxia.v2.continuity.StoreContinuity;
import com.nereusstream.metadata.oxia.v2.continuity.StoreContinuitySnapshot;
import io.oxia.client.api.AsyncOxiaClient;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/** Owns the shared client and store-wide O2 lifecycle; production adapters remain fail closed. */
public final class OxiaV2CapabilityStore implements AutoCloseable {
    private final AsyncOxiaClient client;
    private final StoreContinuity continuity;
    private final RevalidationScheduler scheduler;
    private final AtomicBoolean closed = new AtomicBoolean();

    OxiaV2CapabilityStore(AsyncOxiaClient client, StoreContinuity continuity, RevalidationScheduler scheduler) {
        this.client = Objects.requireNonNull(client, "client");
        this.continuity = Objects.requireNonNull(continuity, "continuity");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    }

    public StoreContinuitySnapshot continuitySnapshot() {
        return continuity.current();
    }

    /** O2 cannot activate until later slices supply complete accepted production codecs and fences. */
    public boolean productionActivationReady() {
        return false;
    }

    AsyncOxiaClient client() {
        return client;
    }

    StoreContinuity continuity() {
        return continuity;
    }

    @Override
    public void close() throws Exception {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        continuity.close();
        scheduler.close();
        client.close();
    }
}
