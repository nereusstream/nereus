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

package com.nereusstream.storage.object.retention;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.ReferenceWriterEnrollmentV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.FloorClassV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Canonical closed ownership registry for every proof-bound floor and reference writer class. */
public final class M5ClosedWriterRegistryV1 {
    public static final int MAX_WRITER_CLASSES = 64;
    public static final int MAX_WRITER_CLASS_BYTES = 1_024;

    public record WriterDeclarationV1(
            String writerClass,
            CapabilityBinding capability,
            List<FloorClassV1> floorClasses,
            List<ReferenceKindV1> referenceKinds,
            Sha256Digest implementationSourceSha256) {
        public WriterDeclarationV1 {
            writerClass = requireWriterClass(writerClass);
            Objects.requireNonNull(capability, "capability");
            floorClasses = sortedUnique(floorClasses, FloorClassV1.class, "floor classes");
            referenceKinds = sortedUnique(referenceKinds, ReferenceKindV1.class, "reference kinds");
            M5RetentionRecordsV1.requireDigest(implementationSourceSha256, "implementationSourceSha256");
            if (floorClasses.isEmpty() && referenceKinds.isEmpty()) {
                throw new IllegalArgumentException("writer declaration owns no proof-bound class");
            }
        }
    }

    /** Registry-bound writer identity; every use is revalidated against the exact closed registry. */
    public record RegisteredReferenceWriterV1(
            ReferenceKindV1 referenceKind,
            String writerClass,
            CapabilityBinding capability,
            Sha256Digest registryRootSha256) {
        public RegisteredReferenceWriterV1 {
            Objects.requireNonNull(referenceKind, "referenceKind");
            writerClass = requireWriterClass(writerClass);
            Objects.requireNonNull(capability, "capability");
            M5RetentionRecordsV1.requireDigest(registryRootSha256, "registryRootSha256");
        }
    }

    private final List<WriterDeclarationV1> declarations;
    private final CapabilityBinding capability;
    private final Sha256Digest registryRootSha256;
    private final Map<ReferenceKindV1, WriterDeclarationV1> referenceOwners;

    public M5ClosedWriterRegistryV1(List<WriterDeclarationV1> declarations) {
        List<WriterDeclarationV1> copy = new ArrayList<>(Objects.requireNonNull(declarations, "declarations"));
        copy.sort(Comparator.comparing(WriterDeclarationV1::writerClass));
        if (copy.isEmpty() || copy.size() > MAX_WRITER_CLASSES) {
            throw new IllegalArgumentException("writer registry count is outside its hard cap");
        }
        Set<String> names = new HashSet<>();
        capability = copy.get(0).capability();
        EnumMap<FloorClassV1, WriterDeclarationV1> floorOwners = new EnumMap<>(FloorClassV1.class);
        EnumMap<ReferenceKindV1, WriterDeclarationV1> referenceOwners = new EnumMap<>(ReferenceKindV1.class);
        for (WriterDeclarationV1 declaration : copy) {
            if (!names.add(declaration.writerClass())
                    || !declaration.capability().equals(capability)) {
                throw new IllegalArgumentException("writer registry has a duplicate name or mixed capability");
            }
            declaration.floorClasses().forEach(kind -> requireUniqueOwner(floorOwners, kind, declaration));
            declaration.referenceKinds().forEach(kind -> requireUniqueOwner(referenceOwners, kind, declaration));
        }
        if (!floorOwners.keySet().equals(EnumSet.allOf(FloorClassV1.class))
                || !referenceOwners.keySet().equals(EnumSet.allOf(ReferenceKindV1.class))) {
            throw new IllegalArgumentException("writer registry does not exactly cover the closed proof inventory");
        }
        this.declarations = List.copyOf(copy);
        this.referenceOwners = Map.copyOf(referenceOwners);
        registryRootSha256 = calculateRegistryRoot(this.declarations);
    }

    public List<WriterDeclarationV1> declarations() {
        return declarations;
    }

    public CapabilityBinding capability() {
        return capability;
    }

    public Sha256Digest registryRootSha256() {
        return registryRootSha256;
    }

    public ReferenceWriterEnrollmentV1 enrollment() {
        return new ReferenceWriterEnrollmentV1(
                capability, List.of(FloorClassV1.values()), List.of(ReferenceKindV1.values()), registryRootSha256);
    }

    public RegisteredReferenceWriterV1 writerFor(ReferenceKindV1 kind) {
        WriterDeclarationV1 owner = referenceOwners.get(Objects.requireNonNull(kind, "kind"));
        if (owner == null) {
            throw new IllegalArgumentException("reference kind has no registered writer owner");
        }
        return new RegisteredReferenceWriterV1(kind, owner.writerClass(), capability, registryRootSha256);
    }

    public void requireRegistered(RegisteredReferenceWriterV1 writer) {
        Objects.requireNonNull(writer, "writer");
        WriterDeclarationV1 owner = referenceOwners.get(writer.referenceKind());
        if (owner == null
                || !owner.writerClass().equals(writer.writerClass())
                || !capability.equals(writer.capability())
                || !registryRootSha256.equals(writer.registryRootSha256())) {
            throw new IllegalArgumentException("reference mutation writer is not exact to the closed registry");
        }
    }

    private static Sha256Digest calculateRegistryRoot(List<WriterDeclarationV1> declarations) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.writeInt(declarations.size());
                for (WriterDeclarationV1 declaration : declarations) {
                    byte[] writerClass = CanonicalUtf8.fromString(declaration.writerClass())
                            .bytes()
                            .toByteArray();
                    output.writeInt(writerClass.length);
                    output.write(writerClass);
                    output.writeLong(declaration.capability().generation());
                    output.write(
                            declaration.capability().evidenceSha256().bytes().toByteArray());
                    output.writeInt(declaration.floorClasses().size());
                    for (FloorClassV1 floor : declaration.floorClasses()) {
                        output.writeByte(floor.ordinal());
                    }
                    output.writeInt(declaration.referenceKinds().size());
                    for (ReferenceKindV1 reference : declaration.referenceKinds()) {
                        output.writeByte(reference.ordinal());
                    }
                    output.write(
                            declaration.implementationSourceSha256().bytes().toByteArray());
                }
            }
            return Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray()));
        } catch (IOException exception) {
            throw new IllegalStateException("failed to encode the closed writer registry", exception);
        }
    }

    private static <K extends Enum<K>> void requireUniqueOwner(
            Map<K, WriterDeclarationV1> owners, K kind, WriterDeclarationV1 declaration) {
        if (owners.put(kind, declaration) != null) {
            throw new IllegalArgumentException("proof-bound class has more than one writer owner: " + kind);
        }
    }

    private static <T extends Enum<T>> List<T> sortedUnique(List<T> values, Class<T> type, String label) {
        List<T> copy = new ArrayList<>(Objects.requireNonNull(values, label));
        if (copy.stream().anyMatch(value -> value == null || value.getDeclaringClass() != type)
                || copy.stream().distinct().count() != copy.size()) {
            throw new IllegalArgumentException(label + " are not unique enum members");
        }
        copy.sort(Comparator.comparingInt(Enum::ordinal));
        return List.copyOf(copy);
    }

    private static String requireWriterClass(String value) {
        Objects.requireNonNull(value, "writerClass");
        int length = CanonicalUtf8.fromString(value).bytes().length();
        if (value.isBlank() || length > MAX_WRITER_CLASS_BYTES) {
            throw new IllegalArgumentException("writer class is blank or exceeds its hard cap");
        }
        return value;
    }
}
