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

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** Selected M2 block classes and one-way Deployment/Namespace/Topic resolution. */
public final class PulsarOffloadBlockPolicyV1 {
    private PulsarOffloadBlockPolicyV1() {}

    public enum BlockClass {
        LATENCY_1_MIB("latency-1mib", PulsarOffloadLimitCandidateV1.MIB),
        BALANCED_4_MIB("balanced-4mib", 4 * PulsarOffloadLimitCandidateV1.MIB),
        THROUGHPUT_16_MIB("throughput-16mib", 16 * PulsarOffloadLimitCandidateV1.MIB);

        private final String id;
        private final int targetBytes;

        BlockClass(String id, int targetBytes) {
            this.id = id;
            this.targetBytes = targetBytes;
        }

        public String id() {
            return id;
        }

        public int targetBytes() {
            return targetBytes;
        }

        public static BlockClass fromId(String id) {
            return Arrays.stream(values())
                    .filter(candidate -> candidate.id.equals(id))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Pulsar block class is not admitted: " + id));
        }
    }

    public enum Authority {
        DEPLOYMENT,
        NAMESPACE,
        TOPIC
    }

    public record CellAdmission(Set<BlockClass> admittedClasses, int maximumTargetBytes) {
        public CellAdmission {
            admittedClasses = Set.copyOf(admittedClasses);
            if (admittedClasses.isEmpty()
                    || admittedClasses.size() > 3
                    || maximumTargetBytes <= 0
                    || !Set.of(BlockClass.values()).containsAll(admittedClasses)) {
                throw new IllegalArgumentException("Cell block-class admission is empty or invalid");
            }
        }

        public static CellAdmission allSelectedClasses() {
            return new CellAdmission(Set.of(BlockClass.values()), Integer.MAX_VALUE);
        }
    }

    public record HostCeiling(long maximumDecodedBytesPerBlock) {
        public HostCeiling {
            if (maximumDecodedBytesPerBlock <= 0) {
                throw new IllegalArgumentException("host decoded-block ceiling is non-positive");
            }
        }
    }

    public record Resolved(BlockClass blockClass, Authority authority) {
        public Resolved {
            Objects.requireNonNull(blockClass, "blockClass");
            Objects.requireNonNull(authority, "authority");
        }
    }

    public static Resolved resolve(
            BlockClass deploymentDefault,
            String namespaceOverride,
            String topicOverride,
            CellAdmission cellAdmission,
            HostCeiling hostCeiling) {
        Objects.requireNonNull(deploymentDefault, "deploymentDefault");
        Objects.requireNonNull(cellAdmission, "cellAdmission");
        Objects.requireNonNull(hostCeiling, "hostCeiling");
        Resolved resolved;
        if (topicOverride != null) {
            resolved = new Resolved(BlockClass.fromId(requireOverride(topicOverride)), Authority.TOPIC);
        } else if (namespaceOverride != null) {
            resolved = new Resolved(BlockClass.fromId(requireOverride(namespaceOverride)), Authority.NAMESPACE);
        } else {
            resolved = new Resolved(deploymentDefault, Authority.DEPLOYMENT);
        }
        if (!cellAdmission.admittedClasses().contains(resolved.blockClass())
                || resolved.blockClass().targetBytes() > cellAdmission.maximumTargetBytes()
                || resolved.blockClass().targetBytes() > hostCeiling.maximumDecodedBytesPerBlock()) {
            throw new IllegalArgumentException("resolved Pulsar block class exceeds Cell or host admission");
        }
        return resolved;
    }

    public static Set<String> selectedClassIds() {
        return Arrays.stream(BlockClass.values()).map(BlockClass::id).collect(Collectors.toUnmodifiableSet());
    }

    private static String requireOverride(String override) {
        if (override.isBlank()) {
            throw new IllegalArgumentException("Pulsar block-class override is blank");
        }
        return override;
    }
}
