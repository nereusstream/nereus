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
import com.nereusstream.domain.bytes.Sha256Digest;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;

/** Local P1 policy generation, not an upstream policy version. Native SDK clock is system UTC. */
public record M5PulsarRetentionConfigurationV1(
        long retentionTimeMillis,
        long retentionSizeInMB,
        long generation,
        Sha256Digest originalFactoryScope,
        Optional<Sha256Digest> installedByFactoryScope) {
    private static final int MAGIC = 0x4d355043; // M5PC
    public static final int MAX_BYTES = 97;

    public M5PulsarRetentionConfigurationV1 {
        M5RetentionRecordsV1.requireDigest(originalFactoryScope, "originalFactoryScope");
        installedByFactoryScope = Objects.requireNonNull(installedByFactoryScope, "installedByFactoryScope");
        installedByFactoryScope.ifPresent(
                value -> M5RetentionRecordsV1.requireDigest(value, "installedByFactoryScope"));
        // Native getters retain negative values after unit conversion; every negative value disables that dimension.
        if (retentionSizeInMB > Long.MAX_VALUE / (1024 * 1024) || generation <= 0) {
            throw new IllegalArgumentException("Native retention size units or policy generation differ");
        }
    }

    public boolean sameValues(long timeMillis, long sizeInMB) {
        return retentionTimeMillis == timeMillis && retentionSizeInMB == sizeInMB;
    }

    public M5PulsarRetentionConfigurationV1 installed(Sha256Digest scope) {
        return new M5PulsarRetentionConfigurationV1(
                retentionTimeMillis, retentionSizeInMB, generation, originalFactoryScope, Optional.of(scope));
    }

    public CanonicalBytes encode() {
        var out = ByteBuffer.allocate(installedByFactoryScope.isPresent() ? 97 : 65)
                .putInt(MAGIC)
                .putInt(1)
                .putLong(retentionTimeMillis)
                .putLong(retentionSizeInMB)
                .putLong(generation)
                .put(originalFactoryScope.bytes().toByteArray())
                .put((byte) (installedByFactoryScope.isPresent() ? 1 : 0));
        installedByFactoryScope.ifPresent(value -> out.put(value.bytes().toByteArray()));
        return CanonicalBytes.copyOf(out.array());
    }

    public static M5PulsarRetentionConfigurationV1 decode(CanonicalBytes bytes) {
        if (bytes.length() != 65 && bytes.length() != 97) {
            throw new IllegalArgumentException("Native retention size differs");
        }
        var in = ByteBuffer.wrap(bytes.toByteArray());
        if (in.getInt() != MAGIC || in.getInt() != 1) {
            throw new IllegalArgumentException("Native retention type/version differs");
        }
        long time = in.getLong();
        long size = in.getLong();
        long generation = in.getLong();
        var original = new byte[32];
        in.get(original);
        int installed = in.get();
        Optional<Sha256Digest> scope = Optional.empty();
        if (installed == 1 && in.remaining() == 32) {
            var value = new byte[32];
            in.get(value);
            scope = Optional.of(Sha256Digest.copyOf(value));
        } else if (installed != 0 || in.hasRemaining()) {
            throw new IllegalArgumentException("Native retention installation differs");
        }
        var result = new M5PulsarRetentionConfigurationV1(time, size, generation, Sha256Digest.copyOf(original), scope);
        if (!result.encode().equals(bytes)) {
            throw new IllegalArgumentException("Native retention is not canonical");
        }
        return result;
    }
}
