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

package com.nereusstream.pulsar.offload;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.pulsar.offload.PulsarOffloadBlockPolicyV1.Authority;
import com.nereusstream.pulsar.offload.PulsarOffloadBlockPolicyV1.BlockClass;
import com.nereusstream.pulsar.offload.PulsarOffloadBlockPolicyV1.CellAdmission;
import com.nereusstream.pulsar.offload.PulsarOffloadBlockPolicyV1.HostCeiling;
import java.util.Set;
import org.junit.jupiter.api.Test;

class PulsarOffloadBlockPolicyV1Test {
    @Test
    void resolvesTopicThenNamespaceThenDeploymentWithoutCellDefaultAuthority() {
        HostCeiling ceiling = new HostCeiling(64L * PulsarOffloadLimitCandidateV1.MIB);
        CellAdmission admission = CellAdmission.allSelectedClasses();

        assertThat(PulsarOffloadBlockPolicyV1.resolve(BlockClass.BALANCED_4_MIB, null, null, admission, ceiling))
                .extracting("blockClass", "authority")
                .containsExactly(BlockClass.BALANCED_4_MIB, Authority.DEPLOYMENT);
        assertThat(PulsarOffloadBlockPolicyV1.resolve(BlockClass.BALANCED_4_MIB, "scan-8mib", null, admission, ceiling))
                .extracting("blockClass", "authority")
                .containsExactly(BlockClass.SCAN_8_MIB, Authority.NAMESPACE);
        assertThat(PulsarOffloadBlockPolicyV1.resolve(
                        BlockClass.BALANCED_4_MIB, "scan-8mib", "latency-1mib", admission, ceiling))
                .extracting("blockClass", "authority")
                .containsExactly(BlockClass.LATENCY_1_MIB, Authority.TOPIC);
    }

    @Test
    void rejectsUnknownOverrideAndPressureInsteadOfRelabeling() {
        HostCeiling ceiling = new HostCeiling(2L * PulsarOffloadLimitCandidateV1.MIB);
        CellAdmission latencyOnly = new CellAdmission(Set.of(BlockClass.LATENCY_1_MIB), Integer.MAX_VALUE);

        assertThatThrownBy(() -> PulsarOffloadBlockPolicyV1.resolve(
                        BlockClass.LATENCY_1_MIB, null, "unknown", latencyOnly, ceiling))
                .hasMessageContaining("not admitted");
        assertThatThrownBy(() ->
                        PulsarOffloadBlockPolicyV1.resolve(BlockClass.BALANCED_4_MIB, null, null, latencyOnly, ceiling))
                .hasMessage("resolved Pulsar block class exceeds Cell or host admission");
    }

    @Test
    void selectedCatalogHasAtMostThreeExactEvidenceCandidates() {
        assertThat(PulsarOffloadBlockPolicyV1.selectedClassIds())
                .containsExactlyInAnyOrder("latency-1mib", "balanced-4mib", "scan-8mib");
        assertThat(BlockClass.values()).extracting(BlockClass::targetBytes).containsExactly(1 << 20, 4 << 20, 8 << 20);
    }
}
