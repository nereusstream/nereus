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

import com.nereusstream.pulsar.offload.evidence.PulsarM2FinalReceiptV1.ReceiptRejectedException;
import com.nereusstream.pulsar.offload.evidence.PulsarM2FinalReceiptV1.RejectionCode;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Resolves one published Pulsar M2 Final receipt without treating P6 or Kafka Final as implicit evidence. */
public final class PulsarM2FinalResolverV1 {
    private PulsarM2FinalResolverV1() {}

    public record Resolution(
            PulsarM2FinalReceiptV1.SourceTuple sourceTuple,
            Set<String> promotedScenarios,
            long scenarioSuiteReferences,
            long uniqueAttachments) {
        public Resolution {
            promotedScenarios = Set.copyOf(promotedScenarios);
        }
    }

    public static Resolution resolve(Path repositoryRoot, Path receiptFile) {
        Objects.requireNonNull(repositoryRoot, "repositoryRoot");
        Objects.requireNonNull(receiptFile, "receiptFile");
        PulsarM2FinalReceiptV1.Receipt receipt = PulsarM2FinalReceiptV1.parseCanonicalFile(receiptFile);
        List<PulsarM2PromotionPolicyV1.ScenarioRequirement> policy = PulsarM2PromotionPolicyV1.policy();
        if (receipt.scenarios().size() != policy.size()) {
            throw reject(RejectionCode.SCENARIO_SET_INVALID, "receipt and policy scenario counts differ");
        }

        Set<String> promoted = new LinkedHashSet<>();
        long suiteReferences = 0;
        for (int index = 0; index < policy.size(); index++) {
            PulsarM2PromotionPolicyV1.ScenarioRequirement expected = policy.get(index);
            PulsarM2FinalReceiptV1.ScenarioResult actual = receipt.scenarios().get(index);
            if (!expected.scenarioId().equals(actual.scenarioId())) {
                throw reject(RejectionCode.SCENARIO_SET_INVALID, "receipt contains an unowned or missing scenario");
            }
            Set<String> actualSuites = new HashSet<>();
            for (PulsarM2FinalReceiptV1.SuiteResult suite : actual.suites()) {
                actualSuites.add(suite.suiteId());
                suiteReferences++;
            }
            if (!actualSuites.equals(expected.requiredSuiteIds())) {
                throw reject(RejectionCode.SUITE_SET_INVALID, "scenario suite set differs from exact Pulsar policy");
            }
            promoted.add(actual.scenarioId());
        }
        PulsarM2FinalReceiptV1.verifyAttachments(repositoryRoot, receipt);
        return new Resolution(
                receipt.sourceTuple(),
                promoted,
                suiteReferences,
                receipt.attachments().size());
    }

    private static ReceiptRejectedException reject(RejectionCode code, String detail) {
        return new ReceiptRejectedException(code, detail);
    }
}
