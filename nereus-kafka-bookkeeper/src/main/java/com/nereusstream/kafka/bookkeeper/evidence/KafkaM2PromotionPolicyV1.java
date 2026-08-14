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

package com.nereusstream.kafka.bookkeeper.evidence;

import java.util.List;
import java.util.Set;

/** Exact K10 policy for the ten Kafka-owned scenarios whose complete claim belongs only to M2. */
public final class KafkaM2PromotionPolicyV1 {
    private static final String PREFIX = "com.nereusstream.kafka.bookkeeper.";
    private static final String REAL_ENGINE = PREFIX + "RealBookKeeperKafkaEngineV1Test";
    private static final String REAL_PROVIDER =
            "com.nereusstream.storage.bookkeeper.RealBookKeeperCellSessionV1RealTest";

    private KafkaM2PromotionPolicyV1() {}

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
                        "V2-BK-003",
                        "nereus.kafka.m2.k9.scale.10000",
                        "nereus.kafka.m2.k9.scale.100000",
                        PREFIX + "operational.KafkaBookKeeperOperationalDefaultsV1Test",
                        REAL_PROVIDER),
                requirement(
                        "V2-BK-014",
                        "org.apache.kafka.clients.record.Kafka43AssignedRecordBatchConformance",
                        PREFIX + "adapter.KafkaAssignedRecordBatchGroupAdapterV1Test",
                        PREFIX + "nbke2.Nbke2CodecV1Test",
                        PREFIX + "read.KafkaPackedLocatorLookupV1Test",
                        PREFIX + "run.KafkaBookKeeperRunLifecycleV1Test"),
                requirement(
                        "V2-BK-015",
                        PREFIX + "pipeline.KafkaBookKeeperOrderedCompletionV1Test",
                        PREFIX + "pipeline.KafkaBookKeeperPipelineAdmissionV1Test",
                        PREFIX + "pipeline.KafkaCoherentCommitCoordinatorV1Test",
                        REAL_ENGINE),
                requirement(
                        "V2-BK-016",
                        PREFIX + "nbke2.Nbke2CorruptionMatrixV1Test",
                        PREFIX + "read.KafkaBookKeeperTargetedReaderV1Test",
                        PREFIX + "read.KafkaPackedLocatorLookupV1Test",
                        REAL_ENGINE),
                requirement(
                        "V2-BK-017",
                        PREFIX + "checkpoint.BookKeeperKafkaProtocolCheckpointStoreV1Test",
                        PREFIX + "checkpoint.KafkaProtocolCheckpointCodecV1Test",
                        PREFIX + "recovery.KafkaBookKeeperTakeoverRecoveryV1Test",
                        REAL_ENGINE,
                        REAL_PROVIDER),
                requirement(
                        "V2-KAF-DATA-001",
                        PREFIX + "pipeline.KafkaBookKeeperOrderedCompletionV1Test",
                        PREFIX + "protocol.KafkaPartitionPublicationCellV1Test",
                        PREFIX + "protocol.KafkaPartitionPublicationInterleavingTest"),
                requirement(
                        "V2-KAF-DATA-002",
                        PREFIX + "pipeline.KafkaBookKeeperOrderedCompletionV1Test",
                        PREFIX + "pipeline.KafkaBookKeeperPipelineAdmissionV1Test",
                        PREFIX + "recovery.KafkaBookKeeperTakeoverRecoveryV1Test",
                        PREFIX + "run.KafkaBookKeeperRunFailureCutsV1Test"),
                requirement(
                        "V2-KAF-DATA-004",
                        PREFIX + "pipeline.KafkaCoherentCommitCoordinatorV1Test",
                        PREFIX + "protocol.KafkaPartitionSpeculativePublicationV1Test",
                        PREFIX + "protocol.KafkaProducerTransactionStateV1Test"),
                requirement(
                        "V2-KAF-DATA-005",
                        PREFIX + "checkpoint.BookKeeperKafkaProtocolCheckpointStoreV1Test",
                        PREFIX + "checkpoint.KafkaProtocolCheckpointCodecV1Test",
                        PREFIX + "recovery.KafkaBookKeeperTakeoverRecoveryV1Test",
                        PREFIX + "replication.KafkaReplicaFollowerKernelV1Test"),
                requirement(
                        "V2-KAF-DATA-014",
                        PREFIX + "admission.KafkaBookKeeperDataAdmissionV1Test",
                        PREFIX + "admission.KafkaBookKeeperNumericProjectionV1Test",
                        PREFIX + "pipeline.KafkaAppendCapacityControllerV1Test",
                        PREFIX + "pipeline.KafkaBookKeeperPipelineAdmissionV1Test"));
    }

    private static ScenarioRequirement requirement(String scenarioId, String... suiteIds) {
        return new ScenarioRequirement(scenarioId, Set.of(suiteIds));
    }
}
