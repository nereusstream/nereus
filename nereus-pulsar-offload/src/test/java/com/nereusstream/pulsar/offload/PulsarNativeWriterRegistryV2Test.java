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
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1;
import com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1.WriterDeclarationV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.FloorClassV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Ownership/entry-point tests only. The synthetic source digests are not native qualification evidence. */
class PulsarNativeWriterRegistryV2Test {
    private static final Sha256Digest DIGEST = Sha256Digest.hash(CanonicalBytes.copyOf(new byte[] {1}));
    private static final CapabilityBinding CAPABILITY = new CapabilityBinding(1, DIGEST);

    @Test
    void fixedOwnersRequireTheExactImplementationSourceInventory() {
        var sources = sources();
        var registry = PulsarNativeWriterRegistryV2.create(CAPABILITY, sources);
        PulsarNativeWriterRegistryV2.requireNativeOwners(registry);
        assertThat(registry.writerFor(ReferenceKindV1.MANIFEST_FALLBACK).writerClass())
                .isEqualTo(PulsarNativeM4ControlV1.class.getName());
        assertThat(registry.writerFor(ReferenceKindV1.RECOVERY_CHECKPOINT_OR_SNAPSHOT)
                        .writerClass())
                .isEqualTo(PulsarNativeMetadataWriterV2.class.getName());
        assertThat(registry.declarations().stream().flatMap(row -> row.floorClasses().stream()))
                .containsExactlyInAnyOrder(FloorClassV1.values());
        sources.remove(PulsarNativeMetadataWriterV2.class);
        assertThatThrownBy(() -> PulsarNativeWriterRegistryV2.create(CAPABILITY, sources))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("inventory");
        sources.put(PulsarNativeMetadataWriterV2.class, DIGEST);
        sources.put(getClass(), DIGEST);
        assertThatThrownBy(() -> PulsarNativeWriterRegistryV2.create(CAPABILITY, sources))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("inventory");
    }

    @Test
    void completeButMisassignedFloorOrReferenceOwnerIsRejected() {
        var registry = PulsarNativeWriterRegistryV2.create(CAPABILITY, sources());
        assertThatThrownBy(() -> PulsarNativeWriterRegistryV2.requireNativeOwners(misassigned(registry, true)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("production owner");
        assertThatThrownBy(() -> PulsarNativeWriterRegistryV2.requireNativeOwners(misassigned(registry, false)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("production owner");
    }

    @Test
    void invalidRegistryRejectsBeforeOriginalSdkOrMetadataAdmission() {
        var invalid = misassigned(PulsarNativeWriterRegistryV2.create(CAPABILITY, sources()), false);
        var factory = PulsarNativeManagedLedgerFactoryV2.open(
                null, null, "unreached", null, null, 0, null, invalid, null, "unreached", null, null, 0, Runnable::run);
        assertThatThrownBy(factory::join)
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("production owner");
        var metadata = PulsarNativeMetadataWriterV2.install(null, null, null, invalid, null, "unreached", null);
        assertThatThrownBy(metadata::join)
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("production owner");
    }

    private static Map<Class<?>, Sha256Digest> sources() {
        var sources = new HashMap<Class<?>, Sha256Digest>();
        PulsarNativeWriterRegistryV2.implementationOwners().forEach(owner -> sources.put(owner, DIGEST));
        return sources;
    }

    private static M5ClosedWriterRegistryV1 misassigned(M5ClosedWriterRegistryV1 original, boolean floor) {
        var declarations = new ArrayList<WriterDeclarationV1>();
        for (var row : original.declarations()) {
            var floors = new ArrayList<>(row.floorClasses());
            var references = new ArrayList<>(row.referenceKinds());
            if (floor) {
                floors.remove(FloorClassV1.PULSAR_CURSOR_SUBSCRIPTION);
            } else {
                references.remove(ReferenceKindV1.MANIFEST_FALLBACK);
            }
            if (row.writerClass().equals(PulsarNativeBindingV1.class.getName())) {
                if (floor) {
                    floors.add(FloorClassV1.PULSAR_CURSOR_SUBSCRIPTION);
                } else {
                    references.add(ReferenceKindV1.MANIFEST_FALLBACK);
                }
            }
            declarations.add(new WriterDeclarationV1(
                    row.writerClass(), row.capability(), floors, references, row.implementationSourceSha256()));
        }
        return new M5ClosedWriterRegistryV1(declarations);
    }
}
