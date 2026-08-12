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

package com.nereusstream.metadata.oxia.v2.mutation;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import java.nio.ByteBuffer;
import java.util.Objects;

/** Exact eight-byte big-endian mapping of an Oxia record version ID. */
public final class MetadataVersionMapper {
    private MetadataVersionMapper() {}

    public static MetadataVersion fromOxia(long versionId) {
        if (versionId < 0) {
            throw new IllegalArgumentException("Oxia version ID must not be negative");
        }
        return new MetadataVersion(CanonicalBytes.copyOf(
                ByteBuffer.allocate(Long.BYTES).putLong(versionId).array()));
    }

    public static long toOxia(MetadataVersion version) {
        Objects.requireNonNull(version, "version");
        byte[] bytes = version.value().toByteArray();
        if (bytes.length != Long.BYTES) {
            throw new IllegalArgumentException("Oxia metadata version must be exactly eight bytes");
        }
        long versionId = ByteBuffer.wrap(bytes).getLong();
        if (versionId < 0) {
            throw new IllegalArgumentException("Oxia metadata version must not encode a negative value");
        }
        return versionId;
    }
}
