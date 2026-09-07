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
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/** Identity of one immutable native metadata namespace marker; the adapter must reread it through its actual client. */
public record MetadataNamespaceIdentityV2(Id128 instanceId, long nativeCreationVersion) {
    public static final int MARKER_BYTES = 56;
    public static final int IDENTITY_BYTES = 96;
    private static final int MARKER_MAGIC = 0x4d354e4d; // M5NM
    private static final int IDENTITY_MAGIC = 0x4d354e49; // M5NI

    public MetadataNamespaceIdentityV2 {
        Objects.requireNonNull(instanceId, "instanceId");
        if (nativeCreationVersion < 0 || Arrays.equals(instanceId.bytes().toByteArray(), new byte[16])) {
            throw new IllegalArgumentException("native metadata namespace identity is invalid");
        }
    }

    public CanonicalBytes markerBytes() {
        var out = ByteBuffer.allocate(MARKER_BYTES)
                .putInt(MARKER_MAGIC)
                .putInt(2)
                .put(instanceId.bytes().toByteArray());
        return checksum(out);
    }

    public CanonicalBytes encode() {
        var out = ByteBuffer.allocate(IDENTITY_BYTES)
                .putInt(IDENTITY_MAGIC)
                .putInt(2)
                .put(instanceId.bytes().toByteArray())
                .putLong(nativeCreationVersion)
                .put(Sha256Digest.hash(markerBytes()).bytes().toByteArray());
        return checksum(out);
    }

    public static MetadataNamespaceIdentityV2 fromNativeMarker(CanonicalBytes marker, long nativeVersion) {
        var in = checked(marker, MARKER_BYTES, MARKER_MAGIC);
        var id = new byte[16];
        in.get(id);
        var identity = new MetadataNamespaceIdentityV2(Id128.fromBytes(id), nativeVersion);
        if (!identity.markerBytes().equals(marker)) {
            throw new IllegalArgumentException("native metadata namespace marker is noncanonical");
        }
        return identity;
    }

    public static MetadataNamespaceIdentityV2 decode(CanonicalBytes bytes) {
        var in = checked(bytes, IDENTITY_BYTES, IDENTITY_MAGIC);
        var id = new byte[16];
        in.get(id);
        var identity = new MetadataNamespaceIdentityV2(Id128.fromBytes(id), in.getLong());
        if (!identity.encode().equals(bytes)) {
            throw new IllegalArgumentException("metadata namespace identity checksum or marker binding differs");
        }
        return identity;
    }

    private static CanonicalBytes checksum(ByteBuffer out) {
        if (out.remaining() != 32) {
            throw new IllegalStateException("metadata namespace canonical length differs");
        }
        out.put(Sha256Digest.hash(CanonicalBytes.copyOf(Arrays.copyOf(out.array(), out.position())))
                .bytes()
                .toByteArray());
        return CanonicalBytes.copyOf(out.array());
    }

    private static ByteBuffer checked(CanonicalBytes bytes, int length, int magic) {
        Objects.requireNonNull(bytes, "bytes");
        if (bytes.length() != length) {
            throw new IllegalArgumentException("metadata namespace encoded length differs");
        }
        var raw = bytes.toByteArray();
        var in = ByteBuffer.wrap(raw);
        if (in.getInt() != magic
                || in.getInt() != 2
                || !Sha256Digest.hash(CanonicalBytes.copyOf(Arrays.copyOf(raw, length - 32)))
                        .equals(Sha256Digest.copyOf(Arrays.copyOfRange(raw, length - 32, length)))) {
            throw new IllegalArgumentException("metadata namespace wire version or checksum differs");
        }
        return in;
    }
}
