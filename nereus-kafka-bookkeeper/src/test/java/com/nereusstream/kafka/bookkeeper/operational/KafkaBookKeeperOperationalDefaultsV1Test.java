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

package com.nereusstream.kafka.bookkeeper.operational;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import com.nereusstream.kafka.bookkeeper.operational.KafkaBookKeeperOperationalDefaultsV1.HandleBudget;
import com.nereusstream.kafka.bookkeeper.operational.KafkaBookKeeperOperationalDefaultsV1.PipelineBudget;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class KafkaBookKeeperOperationalDefaultsV1Test {
    private static final Path PROJECTION =
            Path.of("..", "docs", "v2", "wire", "kafka-bookkeeper-m2-k9-selected-defaults-v1.json");

    @Test
    void selectedDefaultsProduceEveryK0OperationalAdapter() {
        KafkaBookKeeperOperationalDefaultsV1 selected = KafkaBookKeeperOperationalDefaultsV1.evidenceSelected();

        assertThat(selected.recoveryEnvelope().maximumEntries()).isEqualTo(4_096);
        assertThat(selected.partitionPipelineBudget().maximumGroups()).isEqualTo(16);
        assertThat(selected.globalPipelineBudget().maximumBytes()).isEqualTo(268_435_456);
        assertThat(selected.replicaEligibilityBounds().maximumApplyLagOffsets()).isEqualTo(4_096);
        assertThat(selected.replicaJournalBounds().maximumRecords()).isEqualTo(8_192);
    }

    @Test
    void independentCanonicalProjectionMatchesProductionSelection() throws Exception {
        assertThat(Files.readString(PROJECTION).strip())
                .isEqualTo(
                        KafkaBookKeeperOperationalDefaultsV1.evidenceSelected().toCanonicalJson());
    }

    @Test
    void exactSelectionIsAnAllowedNoOpLowering() {
        KafkaBookKeeperOperationalDefaultsV1 selected = KafkaBookKeeperOperationalDefaultsV1.evidenceSelected();

        assertThat(selected.loweredBy(selected)).isSameAs(selected);
    }

    @Test
    void componentWiseLowerHandlePressureIsAccepted() {
        KafkaBookKeeperOperationalDefaultsV1 selected = KafkaBookKeeperOperationalDefaultsV1.evidenceSelected();
        KafkaBookKeeperOperationalDefaultsV1 lower = withHandles(selected, new HandleBudget(512, 32));

        assertThat(selected.loweredBy(lower)).isEqualTo(lower);
    }

    @Test
    void lowerAuthorityCannotEnlargeOneSelectedBound() {
        KafkaBookKeeperOperationalDefaultsV1 selected = KafkaBookKeeperOperationalDefaultsV1.evidenceSelected();
        KafkaBookKeeperOperationalDefaultsV1 enlarged = withHandles(selected, new HandleBudget(2_048, 64));

        assertThatIllegalArgumentException().isThrownBy(() -> selected.loweredBy(enlarged));
    }

    @Test
    void globalPipelineCannotBeLowerThanOnePartition() {
        KafkaBookKeeperOperationalDefaultsV1 selected = KafkaBookKeeperOperationalDefaultsV1.evidenceSelected();
        PipelineBudget invalid = new PipelineBudget(
                selected.pipeline().partition(), new KafkaBookKeeperOperationalDefaultsV1.CapacityBudget(1, 1, 1));

        assertThatIllegalArgumentException()
                .isThrownBy(() -> new KafkaBookKeeperOperationalDefaultsV1(
                        selected.checkpoint(),
                        selected.index(),
                        selected.activeTail(),
                        selected.recovery(),
                        invalid,
                        selected.rollover(),
                        selected.handles(),
                        selected.replica(),
                        selected.waiters(),
                        selected.cursor()));
    }

    private static KafkaBookKeeperOperationalDefaultsV1 withHandles(
            KafkaBookKeeperOperationalDefaultsV1 selected, HandleBudget handles) {
        return new KafkaBookKeeperOperationalDefaultsV1(
                selected.checkpoint(),
                selected.index(),
                selected.activeTail(),
                selected.recovery(),
                selected.pipeline(),
                selected.rollover(),
                handles,
                selected.replica(),
                selected.waiters(),
                selected.cursor());
    }
}
