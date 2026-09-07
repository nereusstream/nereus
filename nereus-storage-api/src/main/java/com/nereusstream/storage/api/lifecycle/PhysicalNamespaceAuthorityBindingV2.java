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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/** Permanent physical-backend assignment to one native metadata namespace and a deterministic authority root. */
public record PhysicalNamespaceAuthorityBindingV2(
        PhysicalResourceIdV2.Namespace physicalNamespace, MetadataNamespaceIdentityV2 metadataNamespace) {
    public static final int MAX_BYTES = 2 * PhysicalResourceIdV2.MAX_COMPONENT_BYTES + 148;
    private static final int MAGIC = 0x4d354e41; // M5NA

    public PhysicalNamespaceAuthorityBindingV2 {
        Objects.requireNonNull(physicalNamespace, "physicalNamespace");
        Objects.requireNonNull(metadataNamespace, "metadataNamespace");
    }

    /** Neither Cell/Binding, endpoint aliases, current owner nor capability can select another root. */
    public String authorityRoot() {
        return "/nereus/m5-native-authorities-v2/"
                + Sha256Digest.hash(namespaceBytes()).toHex();
    }

    public CanonicalBytes encode() {
        var namespace = namespaceBytes();
        var out = ByteBuffer.allocate(8 + namespace.length() + MetadataNamespaceIdentityV2.IDENTITY_BYTES + 32)
                .putInt(MAGIC)
                .putInt(2)
                .put(namespace.toByteArray())
                .put(metadataNamespace.encode().toByteArray());
        out.put(Sha256Digest.hash(CanonicalBytes.copyOf(Arrays.copyOf(out.array(), out.position())))
                .bytes()
                .toByteArray());
        return CanonicalBytes.copyOf(out.array());
    }

    public static PhysicalNamespaceAuthorityBindingV2 decode(CanonicalBytes bytes) {
        if (bytes.length() < 148 || bytes.length() > MAX_BYTES) {
            throw new IllegalArgumentException("physical namespace binding length differs");
        }
        try {
            var in = ByteBuffer.wrap(bytes.toByteArray());
            if (in.getInt() != MAGIC || in.getInt() != 2) {
                throw new IllegalArgumentException("physical namespace binding magic/version differs");
            }
            var kind =
                    switch (in.getInt()) {
                        case 1 -> PhysicalResourceIdV2.ProviderKind.OBJECT_PROVIDER;
                        case 2 -> PhysicalResourceIdV2.ProviderKind.BOOKKEEPER;
                        default ->
                            throw new IllegalArgumentException("physical namespace provider kind is unsupported");
                    };
            var namespace = new PhysicalResourceIdV2.Namespace(kind, text(in), text(in));
            var identity = new byte[MetadataNamespaceIdentityV2.IDENTITY_BYTES];
            in.get(identity);
            var binding = new PhysicalNamespaceAuthorityBindingV2(
                    namespace, MetadataNamespaceIdentityV2.decode(CanonicalBytes.copyOf(identity)));
            if (!binding.encode().equals(bytes)) {
                throw new IllegalArgumentException("physical namespace binding canonical bytes or checksum differ");
            }
            return binding;
        } catch (java.nio.BufferUnderflowException malformed) {
            throw new IllegalArgumentException("physical namespace binding is truncated", malformed);
        }
    }

    private CanonicalBytes namespaceBytes() {
        var service = physicalNamespace.serviceIdentity().bytes();
        var container = physicalNamespace.containerIdentity().bytes();
        return CanonicalBytes.copyOf(ByteBuffer.allocate(12 + service.length() + container.length())
                .putInt(physicalNamespace.providerKind() == PhysicalResourceIdV2.ProviderKind.OBJECT_PROVIDER ? 1 : 2)
                .putInt(service.length())
                .put(service.toByteArray())
                .putInt(container.length())
                .put(container.toByteArray())
                .array());
    }

    private static CanonicalUtf8 text(ByteBuffer in) {
        int length = in.getInt();
        if (length < 1 || length > PhysicalResourceIdV2.MAX_COMPONENT_BYTES || length > in.remaining()) {
            throw new IllegalArgumentException("physical namespace text length differs");
        }
        var bytes = new byte[length];
        in.get(bytes);
        return CanonicalUtf8.fromBytes(bytes);
    }
}
