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

package com.nereusstream.domain.protocol;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.protocol.PulsarNameInventoryAdmissionV1.DeploymentKind;
import com.nereusstream.domain.protocol.PulsarNameInventoryAdmissionV1.NamePair;
import java.util.List;
import org.junit.jupiter.api.Test;

class PulsarNameInventoryAdmissionV1Test {
    @Test
    void freshDeploymentDoesNotRequireAnInventory() {
        assertThat(PulsarNameInventoryAdmissionV1.admit(DeploymentKind.FRESH_DEPLOYMENT, null))
                .isEqualTo(new PulsarNameInventoryAdmissionV1.Admission(DeploymentKind.FRESH_DEPLOYMENT, 0));
    }

    @Test
    void existingClusterImportFailsClosedWithoutAnInventory() {
        assertThatThrownBy(() -> PulsarNameInventoryAdmissionV1.admit(DeploymentKind.EXISTING_CLUSTER_IMPORT, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void qualifiesExplicitEmptyAndValidExistingClusterInventories() {
        assertThat(PulsarNameInventoryAdmissionV1.admit(DeploymentKind.EXISTING_CLUSTER_IMPORT, List.of())
                        .validatedNameCount())
                .isZero();
        assertThat(PulsarNameInventoryAdmissionV1.admit(
                                DeploymentKind.EXISTING_CLUSTER_IMPORT,
                                List.of(
                                        NamePair.fromStrings(
                                                "tenant/ns/persistent/orders", "persistent://tenant/ns/orders"),
                                        NamePair.fromStrings("a/b/persistent/c", "persistent://a/b/c")))
                        .validatedNameCount())
                .isEqualTo(2);
    }

    @Test
    void rejectsOversizeNonClassicAndInconsistentInventoryRows() {
        assertInventoryRejected(NamePair.fromStrings("t/n/persistent/a", "topic://t/n/a"));
        assertInventoryRejected(NamePair.fromStrings("t/n/persistent/a", "persistent://t/n/b"));
        assertInventoryRejected(NamePair.fromStrings("p".repeat(4097), "persistent://t/n/a"));
        String prefix = "persistent://t/n/";
        assertInventoryRejected(NamePair.fromStrings("t/n/persistent/a", prefix + "a".repeat(4097 - prefix.length())));
    }

    private static void assertInventoryRejected(NamePair entry) {
        assertThatThrownBy(() ->
                        PulsarNameInventoryAdmissionV1.admit(DeploymentKind.EXISTING_CLUSTER_IMPORT, List.of(entry)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
