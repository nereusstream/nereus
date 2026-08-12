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

import java.util.List;
import java.util.Objects;

/** Pure-input name qualification for future Pulsar deployment/import admission. */
public final class PulsarNameInventoryAdmissionV1 {
    private PulsarNameInventoryAdmissionV1() {}

    public enum DeploymentKind {
        FRESH_DEPLOYMENT,
        EXISTING_CLUSTER_IMPORT
    }

    public record NamePair(PulsarPersistenceName persistenceName, PulsarTopicName topicName) {
        public NamePair {
            Objects.requireNonNull(persistenceName, "persistenceName");
            Objects.requireNonNull(topicName, "topicName");
        }

        public static NamePair fromStrings(String persistenceName, String topicName) {
            return new NamePair(
                    PulsarPersistenceName.fromString(persistenceName), PulsarTopicName.fromString(topicName));
        }
    }

    public record Admission(DeploymentKind deploymentKind, int validatedNameCount) {
        public Admission {
            Objects.requireNonNull(deploymentKind, "deploymentKind");
            if (validatedNameCount < 0) {
                throw new IllegalArgumentException("validated name count must be non-negative");
            }
        }
    }

    /**
     * Validates an already collected inventory without connecting to Pulsar.
     *
     * <p>An existing-cluster import must provide an explicit inventory, including an explicit empty list when the
     * cluster has no topics. A fresh deployment needs no inventory. This method is an admission boundary, not an
     * ordinary topic/open/read path.
     */
    public static Admission admit(DeploymentKind deploymentKind, List<NamePair> inventory) {
        Objects.requireNonNull(deploymentKind, "deploymentKind");
        if (deploymentKind == DeploymentKind.EXISTING_CLUSTER_IMPORT && inventory == null) {
            throw new IllegalArgumentException("existing Pulsar cluster import requires a qualified name inventory");
        }
        if (inventory == null) {
            return new Admission(deploymentKind, 0);
        }
        List<NamePair> snapshot = List.copyOf(inventory);
        for (NamePair entry : snapshot) {
            PulsarClassicNameV1.validate(entry.persistenceName(), entry.topicName());
        }
        return new Admission(deploymentKind, snapshot.size());
    }
}
