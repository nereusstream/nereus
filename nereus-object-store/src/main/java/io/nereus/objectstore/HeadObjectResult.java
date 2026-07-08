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

package io.nereus.objectstore;

import io.nereus.api.Checksum;
import io.nereus.api.MetadataCanonicalizer;
import io.nereus.api.ObjectKey;
import java.util.Map;
import java.util.Objects;

public record HeadObjectResult(
        ObjectKey key,
        long objectLength,
        Checksum checksum,
        Map<String, String> metadata) {
    public HeadObjectResult {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(checksum, "checksum");
        metadata = MetadataCanonicalizer.canonicalStringMap(metadata, Integer.MAX_VALUE, "metadata");
        if (objectLength < 0) {
            throw new IllegalArgumentException("objectLength must be non-negative");
        }
    }
}
