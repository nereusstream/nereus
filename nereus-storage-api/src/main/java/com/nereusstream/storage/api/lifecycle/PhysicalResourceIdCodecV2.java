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

package com.nereusstream.storage.api.lifecycle;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.BookKeeperLedger;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.MultipartUpload;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.Namespace;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.ObjectIdentityKind;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.ObjectVersion;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.ProviderKind;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

/** Strict canonical M5RI v2 identity codec. Tags are explicit and independent of format roles or enum ordinals. */
public final class PhysicalResourceIdCodecV2 {
    private static final int MAGIC = 0x4d355249;
    private static final int VERSION = 2;
    private static final CanonicalUtf8 DOMAIN = CanonicalUtf8.fromString("NEREUS_V2_M5_PHYSICAL_RESOURCE_V2");

    private PhysicalResourceIdCodecV2() {}

    public static CanonicalBytes encode(PhysicalResourceIdV2 resource) {
        Objects.requireNonNull(resource, "resource");
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.writeInt(MAGIC);
                output.writeShort(VERSION);
                output.writeByte(resource instanceof ObjectVersion ? 1 : resource instanceof BookKeeperLedger ? 2 : 3);
                Namespace namespace = resource.namespace();
                output.writeByte(namespace.providerKind() == ProviderKind.OBJECT_PROVIDER ? 1 : 2);
                writeText(output, namespace.serviceIdentity());
                writeText(output, namespace.containerIdentity());
                if (resource instanceof ObjectVersion object) {
                    writeText(output, object.objectKey());
                    output.writeByte(object.identityKind() == ObjectIdentityKind.IMMUTABLE_VERSION ? 1 : 2);
                    writeText(output, object.immutableIdentity());
                } else if (resource instanceof BookKeeperLedger ledger) {
                    output.writeLong(ledger.ledgerId());
                } else if (resource instanceof MultipartUpload upload) {
                    writeText(output, upload.objectKey());
                    writeText(output, upload.uploadId());
                } else {
                    throw new IllegalArgumentException("unsupported physical resource type");
                }
            }
            if (bytes.size() > PhysicalResourceIdV2.MAX_ENCODED_BYTES) {
                throw new IllegalArgumentException("physical resource exceeds encoded cap");
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException error) {
            throw new IllegalStateException("cannot encode physical resource", error);
        }
    }

    public static PhysicalResourceIdV2 decode(CanonicalBytes bytes) {
        Objects.requireNonNull(bytes, "bytes");
        if (bytes.length() > PhysicalResourceIdV2.MAX_ENCODED_BYTES) {
            throw new IllegalArgumentException("physical resource exceeds encoded cap");
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            if (input.readInt() != MAGIC || input.readUnsignedShort() != VERSION) {
                throw new IllegalArgumentException("physical resource preamble differs");
            }
            int kind = input.readUnsignedByte();
            ProviderKind provider =
                    switch (input.readUnsignedByte()) {
                        case 1 -> ProviderKind.OBJECT_PROVIDER;
                        case 2 -> ProviderKind.BOOKKEEPER;
                        default -> throw new IllegalArgumentException("unknown physical provider kind");
                    };
            Namespace namespace = new Namespace(provider, readText(input), readText(input));
            PhysicalResourceIdV2 resource =
                    switch (kind) {
                        case 1 ->
                            new ObjectVersion(
                                    namespace, readText(input), readObjectIdentityKind(input), readText(input));
                        case 2 -> new BookKeeperLedger(namespace, input.readLong());
                        case 3 -> new MultipartUpload(namespace, readText(input), readText(input));
                        default -> throw new IllegalArgumentException("unknown physical resource kind");
                    };
            if (input.available() != 0 || !encode(resource).equals(bytes)) {
                throw new IllegalArgumentException("physical resource is not canonical or has trailing bytes");
            }
            return resource;
        } catch (IOException error) {
            throw new IllegalArgumentException("truncated physical resource", error);
        }
    }

    public static Sha256Digest sha256(PhysicalResourceIdV2 resource) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                writeText(output, DOMAIN);
                CanonicalBytes encoded = encode(resource);
                output.writeInt(encoded.length());
                output.write(encoded.toByteArray());
            }
            return Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray()));
        } catch (IOException error) {
            throw new IllegalStateException("cannot hash physical resource", error);
        }
    }

    private static ObjectIdentityKind readObjectIdentityKind(DataInputStream input) throws IOException {
        return switch (input.readUnsignedByte()) {
            case 1 -> ObjectIdentityKind.IMMUTABLE_VERSION;
            case 2 -> ObjectIdentityKind.IMMUTABLE_CREATE;
            default -> throw new IllegalArgumentException("unknown conditional object identity kind");
        };
    }

    private static void writeText(DataOutputStream output, CanonicalUtf8 value) throws IOException {
        output.writeInt(value.bytes().length());
        output.write(value.bytes().toByteArray());
    }

    private static CanonicalUtf8 readText(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length <= 0 || length > PhysicalResourceIdV2.MAX_COMPONENT_BYTES || length > input.available()) {
            throw new IllegalArgumentException("invalid physical resource component length");
        }
        return CanonicalUtf8.fromBytes(input.readNBytes(length));
    }
}
