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

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1;
import com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1.WriterDeclarationV1;
import com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.FloorClassV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Fixed Native writer ownership. The admitting controller must independently qualify source digests. */
public final class PulsarNativeWriterRegistryV2 {
    private PulsarNativeWriterRegistryV2() {}

    public static Set<Class<?>> implementationOwners() {
        return Set.of(
                PulsarNativeBindingV1.class,
                PulsarNativeMetadataWriterV2.class,
                PulsarNativeM4ControlV1.class,
                PulsarNativeM4ReadOwnerV1.class,
                PulsarNativeMaterializationTaskV2.class,
                OxiaPhysicalMemberInventoryV2.class,
                M5PulsarAggregateRetirementCoordinatorV1.class);
    }

    /** Build the fixed inventory from controller-qualified implementation source identities; this performs no I/O. */
    public static M5ClosedWriterRegistryV1 create(CapabilityBinding capability, Map<Class<?>, Sha256Digest> sources) {
        Objects.requireNonNull(capability, "capability");
        var exact = Map.copyOf(Objects.requireNonNull(sources, "sources"));
        if (!exact.keySet().equals(implementationOwners())) {
            throw new IllegalArgumentException("Native writer implementation source inventory differs");
        }
        return new M5ClosedWriterRegistryV1(implementationOwners().stream()
                .map(owner -> new WriterDeclarationV1(
                        owner.getName(),
                        capability,
                        Arrays.stream(FloorClassV1.values())
                                .filter(kind -> floorOwner(kind) == owner)
                                .toList(),
                        Arrays.stream(ReferenceKindV1.values())
                                .filter(kind -> referenceOwner(kind) == owner)
                                .toList(),
                        exact.get(owner)))
                .toList());
    }

    /** Root equality alone cannot establish that a writer is enrolled under the class used by its production path. */
    public static void requireNativeOwners(M5ClosedWriterRegistryV1 registry) {
        Objects.requireNonNull(registry, "registry");
        for (var declaration : registry.declarations()) {
            if (declaration.floorClasses().stream()
                            .anyMatch(kind -> !floorOwner(kind).getName().equals(declaration.writerClass()))
                    || declaration.referenceKinds().stream()
                            .anyMatch(kind -> !referenceOwner(kind).getName().equals(declaration.writerClass()))) {
                throw new IllegalArgumentException(
                        "Native writer registry assigns a class to a different production owner");
            }
        }
    }

    private static Class<?> floorOwner(FloorClassV1 kind) {
        return switch (kind) {
            case PULSAR_CURSOR_SUBSCRIPTION -> PulsarNativeMetadataWriterV2.class;
            case GENERATION_READ -> PulsarNativeM4ReadOwnerV1.class;
            case LIFECYCLE_TASK -> PulsarNativeMaterializationTaskV2.class;
            case SHARED_PHYSICAL_SOURCE -> OxiaPhysicalMemberInventoryV2.class;
            case AUDIT_GRACE -> M5PulsarAggregateRetirementCoordinatorV1.class;
            case BINDING_EPOCH_POLICY,
                    KAFKA_CONSUMER_GROUP,
                    KAFKA_PRODUCER_TRANSACTION,
                    KAFKA_REPLICATION_RECOVERY,
                    PROJECTION_MIGRATION -> PulsarNativeBindingV1.class;
        };
    }

    private static Class<?> referenceOwner(ReferenceKindV1 kind) {
        return switch (kind) {
            case PULSAR_SUBSCRIPTION_OR_REPLICATION_CURSOR, RECOVERY_CHECKPOINT_OR_SNAPSHOT ->
                PulsarNativeMetadataWriterV2.class;
            case MANIFEST_SELECTED, MANIFEST_FALLBACK, SOURCE_PROTECTION -> PulsarNativeM4ControlV1.class;
            case READ_GENERATION_PIN_OR_OPEN_HANDLE -> PulsarNativeM4ReadOwnerV1.class;
            case MATERIALIZATION_OR_COMPACTION_TASK -> PulsarNativeMaterializationTaskV2.class;
            case SHARED_PHYSICAL_MEMBER -> OxiaPhysicalMemberInventoryV2.class;
            case RETIREMENT_OR_DELETE_RECONCILIATION, AUDIT_GRACE -> M5PulsarAggregateRetirementCoordinatorV1.class;
            case KAFKA_GROUP_RETENTION,
                    KAFKA_PRODUCER_RECOVERY,
                    KAFKA_TRANSACTION_OR_ABORTED_RECOVERY,
                    KAFKA_REPLICA_OR_LEADER_EPOCH_RECOVERY,
                    PROJECTION_MIGRATION_OR_EXPORT -> PulsarNativeBindingV1.class;
        };
    }
}
