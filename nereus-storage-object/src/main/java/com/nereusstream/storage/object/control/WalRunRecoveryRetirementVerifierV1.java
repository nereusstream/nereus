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

package com.nereusstream.storage.object.control;

import com.nereusstream.storage.object.recovery.CumulativeRecoveryBudget;

/**
 * Protocol-owned verification of an immutable, complete oldest-prefix retirement decision. A supplied Root reference
 * is only a lookup hint. Implementations must read the actual certificate, verify the exact retained-to-retired
 * boundary and protected protocol transfers, and precharge every read/decoded member to the supplied Root budget.
 * Absence retains the complete lineage; unknown, corrupt or unprotected facts must fail rather than grant a cut.
 * This hook grants no logical trim, source-protection release or physical deletion authority.
 */
@FunctionalInterface
public interface WalRunRecoveryRetirementVerifierV1 {
    boolean isRetiredPrefix(
            WalRunPredecessor boundary, WalRunRootRecord retainedRoot, CumulativeRecoveryBudget recoveryBudget);

    static WalRunRecoveryRetirementVerifierV1 retainAll() {
        return (boundary, retainedRoot, budget) -> false;
    }
}
