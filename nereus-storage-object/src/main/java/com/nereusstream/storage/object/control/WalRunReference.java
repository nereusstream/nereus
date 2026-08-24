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

package com.nereusstream.storage.object.control;

import com.nereusstream.domain.bytes.Sha256Digest;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Exact immutable Root tuple used by the current pointer and successor lineage. */
public record WalRunReference(String rootKey, Sha256Digest rootSha256, int shardId, long shardRunEpoch) {
    public static final int MAX_METADATA_KEY_BYTES = 1024;

    public WalRunReference {
        Objects.requireNonNull(rootKey, "rootKey");
        Objects.requireNonNull(rootSha256, "rootSha256");
        int keyLength = rootKey.getBytes(StandardCharsets.UTF_8).length;
        if (keyLength == 0 || keyLength > MAX_METADATA_KEY_BYTES || rootKey.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("rootKey must be non-empty bounded UTF-8 without NUL");
        }
        if (rootSha256.isZero()) {
            throw new IllegalArgumentException("Root SHA-256 must be non-zero");
        }
        if (shardId < 0 || shardRunEpoch < 0) {
            throw new IllegalArgumentException("shard identity and run epoch must be non-negative");
        }
        WalRunControlKeys.requireRootKey(rootKey, shardId, shardRunEpoch);
    }
}
