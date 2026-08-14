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

package com.nereusstream.pulsar.offload.evidence;

import java.util.List;
import java.util.Set;

/** Exact policy for the eleven BookKeeper scenarios completely owned by M2 after both sub-aggregates pass. */
public final class PulsarM2PromotionPolicyV1 {
    private static final String LOCAL = "com.nereusstream.pulsar.offload.";
    private static final String NATIVE = "org.apache.bookkeeper.mledger.impl.";
    private static final String P6 = "com.nereusstream.pulsar.offload.";

    private PulsarM2PromotionPolicyV1() {}

    public record ScenarioRequirement(String scenarioId, Set<String> requiredSuiteIds) {
        public ScenarioRequirement {
            requiredSuiteIds = Set.copyOf(requiredSuiteIds);
            if (requiredSuiteIds.isEmpty()) {
                throw new IllegalArgumentException("each promoted scenario requires named suites");
            }
        }
    }

    public static List<ScenarioRequirement> policy() {
        return List.of(
                requirement(
                        "V2-BK-001",
                        LOCAL + "NereusPulsarLedgerOffloaderV1Test",
                        NATIVE + "OffloadLedgerDeleteTest",
                        "nereus.kafka.m2.final"),
                requirement(
                        "V2-BK-002",
                        LOCAL + "NereusPulsarLedgerOffloaderV1Test",
                        LOCAL + "PulsarOffloadP0ContractTest",
                        NATIVE + "OffloadLedgerDeleteTest"),
                requirement(
                        "V2-BK-004",
                        LOCAL + "NereusPulsarLedgerOffloaderV1Test",
                        LOCAL + "PulsarSealedLedgerPublisherV1Test",
                        "nereus.pulsar.m2.p6.minio-provider"),
                requirement(
                        "V2-BK-005",
                        LOCAL + "PulsarOffloadP0ContractTest",
                        LOCAL + "PulsarSealedLedgerPublisherV1Test",
                        P6 + "P6MinioProviderEvidenceTest"),
                requirement("V2-BK-006", LOCAL + "npo1.Npo1CodecV1Test"),
                requirement(
                        "V2-BK-007",
                        LOCAL + "PulsarBookKeeperDeletionCoordinatorV1Test",
                        LOCAL + "PulsarObjectReadHandleV1Test",
                        NATIVE + "OffloadLedgerDeleteTest"),
                requirement(
                        "V2-BK-008",
                        LOCAL + "PulsarDualSourceReadHandleV1Test",
                        NATIVE + "DualSourceReadHandleTest",
                        "nereus.pulsar.m2.p6.native-baseline"),
                requirement(
                        "V2-BK-009",
                        LOCAL + "PulsarObjectReadHandleV1Test",
                        LOCAL + "npd1.Npd1CodecV1Test",
                        LOCAL + "npo1.Npo1CodecV1Test"),
                requirement(
                        "V2-BK-010",
                        LOCAL + "PulsarBookKeeperDeletionCoordinatorV1Test",
                        LOCAL + "PulsarDualSourceReadHandleV1Test",
                        NATIVE + "DualSourceReadHandleTest",
                        NATIVE + "OffloadLedgerDeleteTest"),
                requirement(
                        "V2-BK-012",
                        LOCAL + "npd1.Npd1CodecV1Test",
                        P6 + "PulsarP6CandidateEvidenceTest",
                        P6 + "S3PulsarOffloadObjectStoreV1Test"),
                requirement(
                        "V2-BK-013",
                        LOCAL + "NereusPulsarLedgerOffloaderV1Test",
                        LOCAL + "PulsarOffloadBlockPolicyV1Test",
                        "nereus.pulsar.m2.p6.candidate-matrix"));
    }

    private static ScenarioRequirement requirement(String scenarioId, String... suiteIds) {
        return new ScenarioRequirement(scenarioId, Set.of(suiteIds));
    }
}
