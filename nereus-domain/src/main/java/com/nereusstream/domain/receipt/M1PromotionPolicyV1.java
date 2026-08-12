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

import java.util.List;
import java.util.Set;

/** Exact M1 virtual-ledger scenario-to-suite promotion policy. */
public final class M1PromotionPolicyV1 {
    private static final String CAPACITY = "com.nereusstream.domain.registry.RegistryCapacityEvidenceTest";
    private static final String ADMISSION = "com.nereusstream.domain.registry.RegistryAdmissionEvidenceV1Test";
    private static final String REGISTRY_CODEC = "com.nereusstream.domain.registry.Nvr1RegistryCodecV1Test";
    private static final String TRANSITIONS =
            "com.nereusstream.domain.registry.PulsarVirtualLedgerRegistryTransitionValidatorV1Test";
    private static final String AUTHORITY_CODEC =
            "com.nereusstream.metadata.oxia.v2.codec.Nvr1RegistryAuthorityCodecTest";
    private static final String AUTHORITY = "com.nereusstream.metadata.oxia.v2.capability.R1RegistryAuthorityTest";
    private static final String REAL_OXIA = "com.nereusstream.metadata.oxia.v2.R1RegistryOxiaIntegrationTest";
    private static final String ALLOCATOR_CUTS =
            "com.nereusstream.domain.registry.allocator.AllocatorEvidenceProtocolHarnessTest";
    private static final String RANGE_CUTS =
            "com.nereusstream.domain.registry.allocator.RangeLeasedCandidateHarnessTest";

    private M1PromotionPolicyV1() {}

    public static M1FinalResolverV1.PromotionPolicy policy() {
        return new M1FinalResolverV1.PromotionPolicy(List.of(
                registry("V2-POSITION-003", CAPACITY, ADMISSION, AUTHORITY, REAL_OXIA),
                registry("V2-POSITION-004", AUTHORITY_CODEC, AUTHORITY, REAL_OXIA),
                registry("V2-POSITION-005", REGISTRY_CODEC, AUTHORITY),
                registry("V2-POSITION-006", TRANSITIONS, AUTHORITY),
                registry("V2-POSITION-007", CAPACITY, REGISTRY_CODEC),
                registry("V2-POSITION-008", TRANSITIONS, AUTHORITY),
                registry("V2-POSITION-009", CAPACITY, REGISTRY_CODEC, AUTHORITY),
                harness("V2-POSITION-010", ALLOCATOR_CUTS),
                harness("V2-POSITION-011", RANGE_CUTS)));
    }

    private static M1FinalResolverV1.ScenarioRequirement registry(String scenarioId, String... suiteIds) {
        return requirement(scenarioId, VirtualLedgerReceiptV1.ReceiptKind.REGISTRY_CONFORMANCE, suiteIds);
    }

    private static M1FinalResolverV1.ScenarioRequirement harness(String scenarioId, String... suiteIds) {
        return requirement(scenarioId, VirtualLedgerReceiptV1.ReceiptKind.HARNESS_CONFORMANCE_ONLY, suiteIds);
    }

    private static M1FinalResolverV1.ScenarioRequirement requirement(
            String scenarioId, VirtualLedgerReceiptV1.ReceiptKind kind, String... suiteIds) {
        return new M1FinalResolverV1.ScenarioRequirement(scenarioId, kind, Set.of(suiteIds));
    }
}
