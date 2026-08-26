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

package com.nereusstream.storage.object.recovery;

import static org.assertj.core.api.Assertions.assertThat;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

class WalRunRecoveryManifestV1Test {
    private static final String RESOURCE = "/com/nereusstream/storage/object/recovery/walrun-recovery-manifest-v1.tsv";

    @Test
    void closedInventoryBindsExactControlAndRecoveryCases() throws IOException {
        try (InputStream stream = WalRunRecoveryManifestV1Test.class.getResourceAsStream(RESOURCE)) {
            assertThat(stream).isNotNull();
            assertThat(new String(stream.readAllBytes(), StandardCharsets.UTF_8))
                    .isEqualTo(canonicalTsv());
        }
    }

    private static String canonicalTsv() {
        StringBuilder output = new StringBuilder("recordId\tcomponent\ttestClass\ttestMethod\tclaim\n");
        for (Row row : rows()) {
            output.append(row.recordId())
                    .append('\t')
                    .append(row.component())
                    .append('\t')
                    .append(row.testClass())
                    .append('\t')
                    .append(row.testMethod())
                    .append('\t')
                    .append(row.claim())
                    .append('\n');
        }
        return output.toString();
    }

    private static List<Row> rows() {
        return List.of(
                new Row(
                        "R01_CONTROL_WIRE",
                        "CONTROL_WIRE",
                        "com.nereusstream.storage.object.control.WalRunControlCodecTest",
                        "rootPointerSealAndCheckpointRecordsRoundTripCanonically",
                        "strict canonical Root Pointer Seal checkpoint round trip"),
                new Row(
                        "R02_LAZY_LANES",
                        "LAZY_LANES",
                        "com.nereusstream.storage.object.control.WalRunRuntimeTest",
                        "lanesInstantiateLazilyAndResolveIndependently",
                        "three lazy lanes remain independently resolved"),
                new Row(
                        "R03_CHECKPOINT_PUBLISH",
                        "CHECKPOINT_PUBLISH",
                        "com.nereusstream.storage.object.control.WalCheckpointPublisherTest",
                        "takeoverPreservesCommittedHeadAndStaleEpochCannotRegress",
                        "checkpoint takeover preserves the committed Head"),
                new Row(
                        "R04_CHECKPOINT_RECOVERY",
                        "CHECKPOINT_RECOVERY",
                        "com.nereusstream.storage.object.control.WalCheckpointChainVerifierTest",
                        "streamingRecoveryWalksBackFromHeadAndChargesTheRootOwnedBudget",
                        "streaming chain recovery charges the Root budget"),
                new Row(
                        "R05_SEAL_SUCCESSOR",
                        "SEAL_SUCCESSOR",
                        "com.nereusstream.storage.object.control.WalRunLifecycleManagerTest",
                        "sealedPointerCrashNeedsCandidateThenAdvancesOrAdoptsOnlyExactLineage",
                        "sealed pointer recovery accepts only exact successor lineage"),
                new Row(
                        "R06_LINEAGE_RECOVERY",
                        "LINEAGE_RECOVERY",
                        "com.nereusstream.storage.object.recovery.WalRunLineageRecoveryTest",
                        "exactLineageWalkValidatesRootAndPredecessorSeal",
                        "bounded lineage walk verifies every Root and predecessor Seal"),
                new Row(
                        "R07_BOUNDED_TAIL",
                        "BOUNDED_TAIL",
                        "com.nereusstream.storage.object.recovery.BoundedObjectTailRecoveryTest",
                        "productionRootBoundInventoryParsesExactLeafAndRejectsRuntimeExpansion",
                        "Root bound inventory rejects runtime expansion"),
                new Row(
                        "R08_SESSION_LIFECYCLE",
                        "SESSION_LIFECYCLE",
                        "com.nereusstream.storage.object.control.WalRunObjectSessionTest",
                        "ownsProviderAndKmsLifecycleAndErasesRunKeysOnClose",
                        "Provider and KMS sessions close with run key erasure"));
    }

    private record Row(String recordId, String component, String testClass, String testMethod, String claim) {}
}
