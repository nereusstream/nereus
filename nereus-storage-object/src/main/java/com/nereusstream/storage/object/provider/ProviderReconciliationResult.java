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

package com.nereusstream.storage.object.provider;

import java.util.Objects;
import java.util.Optional;

/** Exact C1 response-loss outcome plus the complete strong-LIST work that preceded its one full GET. */
public record ProviderReconciliationResult(
        ProviderObjectResult objectResult, Optional<StrongListResult> inventory, int fullGetRequests) {
    public ProviderReconciliationResult {
        Objects.requireNonNull(objectResult, "objectResult");
        Objects.requireNonNull(inventory, "inventory");
        if (fullGetRequests < 0 || fullGetRequests > 1 || (inventory.isPresent() != (fullGetRequests == 1))) {
            throw new IllegalArgumentException("C1 reconciliation call profile must be either local or LIST+full-GET");
        }
    }

    public static ProviderReconciliationResult localConflict() {
        return new ProviderReconciliationResult(
                ProviderObjectResult.outcome(ProviderObjectOutcome.DEFINITIVE_CONFLICT), Optional.empty(), 0);
    }

    public static ProviderReconciliationResult providerWork(
            ProviderObjectResult objectResult, StrongListResult inventory) {
        return new ProviderReconciliationResult(objectResult, Optional.of(inventory), 1);
    }
}
