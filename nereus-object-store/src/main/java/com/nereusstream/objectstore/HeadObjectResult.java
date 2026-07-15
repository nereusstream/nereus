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

package com.nereusstream.objectstore;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.MetadataCanonicalizer;
import com.nereusstream.api.ObjectKey;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public record HeadObjectResult(
        ObjectKey key,
        long objectLength,
        Checksum checksum,
        Optional<String> etag,
        Map<String, String> metadata) {
    public HeadObjectResult {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(checksum, "checksum");
        etag = Objects.requireNonNull(etag, "etag").map(value -> {
            if (value.isBlank()) {
                throw new IllegalArgumentException("etag cannot be blank");
            }
            return value;
        });
        metadata = MetadataCanonicalizer.canonicalStringMap(metadata, Integer.MAX_VALUE, "metadata");
        if (objectLength < 0) {
            throw new IllegalArgumentException("objectLength must be non-negative");
        }
    }

    public HeadObjectResult(
            ObjectKey key,
            long objectLength,
            Checksum checksum,
            Map<String, String> metadata) {
        this(key, objectLength, checksum, Optional.empty(), metadata);
    }
}
