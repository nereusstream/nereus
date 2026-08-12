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

package com.nereusstream.domain.receipt;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class M1PromotionPolicyV1Test {
    @Test
    void policyRequiresTheExactNineVirtualLedgerScenarios() {
        var scenarios = M1PromotionPolicyV1.policy().scenarios();
        assertThat(scenarios)
                .extracting(M1FinalResolverV1.ScenarioRequirement::scenarioId)
                .containsExactly(
                        "V2-POSITION-003",
                        "V2-POSITION-004",
                        "V2-POSITION-005",
                        "V2-POSITION-006",
                        "V2-POSITION-007",
                        "V2-POSITION-008",
                        "V2-POSITION-009",
                        "V2-POSITION-010",
                        "V2-POSITION-011");
    }

    @Test
    void registryAndHarnessReceiptsCannotSubstituteForEachOther() {
        Map<String, VirtualLedgerReceiptV1.ReceiptKind> kinds = M1PromotionPolicyV1.policy().scenarios().stream()
                .collect(Collectors.toMap(
                        M1FinalResolverV1.ScenarioRequirement::scenarioId,
                        M1FinalResolverV1.ScenarioRequirement::receiptKind));
        assertThat(kinds.entrySet())
                .filteredOn(entry -> entry.getKey().compareTo("V2-POSITION-010") < 0)
                .allMatch(entry -> entry.getValue() == VirtualLedgerReceiptV1.ReceiptKind.REGISTRY_CONFORMANCE);
        assertThat(kinds.get("V2-POSITION-010")).isEqualTo(VirtualLedgerReceiptV1.ReceiptKind.HARNESS_CONFORMANCE_ONLY);
        assertThat(kinds.get("V2-POSITION-011")).isEqualTo(VirtualLedgerReceiptV1.ReceiptKind.HARNESS_CONFORMANCE_ONLY);
    }
}
