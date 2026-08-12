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
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Explicit composition root for one isolated O2 client and continuity capability. */
public final class OxiaV2CapabilityStoreFactory {
    private OxiaV2CapabilityStoreFactory() {}

    public static CompletableFuture<OxiaV2CapabilityStore> connect(
            OxiaV2StoreConfiguration configuration, RevalidationScheduler scheduler) {
        Objects.requireNonNull(configuration, "configuration");
        Objects.requireNonNull(scheduler, "scheduler");

        CompletableFuture<AsyncOxiaClient> clientFuture;
        try {
            clientFuture = OxiaClientBuilder.create(configuration.serviceAddress())
                    .namespace(configuration.namespace())
                    .asyncClient();
        } catch (RuntimeException | Error failure) {
            scheduler.close();
            return CompletableFuture.failedFuture(failure);
        }

        return clientFuture.handle((client, failure) -> {
            if (failure != null) {
                scheduler.close();
                throw new CompletionException(unwrap(failure));
            }
            try {
                return attach(client, scheduler);
            } catch (RuntimeException | Error attachFailure) {
                scheduler.close();
                closeClient(client, attachFailure);
                throw attachFailure;
            }
        });
    }

    static OxiaV2CapabilityStore attach(AsyncOxiaClient client, RevalidationScheduler scheduler) {
        StoreContinuity continuity = StoreContinuity.attach(client, scheduler);
        return new OxiaV2CapabilityStore(client, continuity, scheduler);
    }

    private static Throwable unwrap(Throwable failure) {
        if (failure instanceof CompletionException && failure.getCause() != null) {
            return failure.getCause();
        }
        return failure;
    }

    private static void closeClient(AsyncOxiaClient client, Throwable originalFailure) {
        try {
            client.close();
        } catch (Exception closeFailure) {
            originalFailure.addSuppressed(closeFailure);
        }
    }
}
