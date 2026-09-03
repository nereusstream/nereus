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

package com.nereusstream.storage.object.materialization;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import java.util.Locale;
import java.util.Objects;

/** Bounded deterministic M5 materialization keys below one Binding authority. */
public final class M5MaterializationKeysV1 {
    private final String prefix;

    public M5MaterializationKeysV1(int shardId, BindingIdentity binding) {
        Objects.requireNonNull(binding, "binding");
        if (shardId < 0) {
            throw new IllegalArgumentException("shard ID must be non-negative");
        }
        prefix = String.format(
                Locale.ROOT,
                "v2/object-wal/shards/%010d/read-m5/%s",
                shardId,
                binding.bindingId().digest().toHex());
    }

    public String sourceCut(Sha256Digest taskId) {
        return prefix + "/source-cuts/" + digest(taskId);
    }

    public String task(Sha256Digest taskId) {
        return prefix + "/tasks/" + digest(taskId);
    }

    public String validation(Sha256Digest validationSha256) {
        return prefix + "/validations/" + digest(validationSha256);
    }

    public String generation(Sha256Digest generationSha256) {
        return prefix + "/generations/" + digest(generationSha256);
    }

    public String manifest(Sha256Digest manifestSha256) {
        return prefix + "/manifests/" + digest(manifestSha256);
    }

    private static String digest(Sha256Digest value) {
        Objects.requireNonNull(value, "value");
        if (value.isZero()) {
            throw new IllegalArgumentException("M5 key digest is zero");
        }
        return value.toHex();
    }
}
