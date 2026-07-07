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

package io.nereus.metadata.oxia.records;

import io.nereus.api.ApiLimits;
import io.nereus.api.MetadataCanonicalizer;
import java.util.Map;
import java.util.Objects;

public record StreamMetadataRecord(
        String streamId,
        String streamName,
        String streamNameHash,
        String state,
        String profile,
        Map<String, String> attributes,
        long createdAtMillis,
        long policyVersion,
        long metadataVersion) {
    public StreamMetadataRecord {
        requireNonBlank(streamId, "streamId");
        requireNonBlank(streamName, "streamName");
        requireNonBlank(streamNameHash, "streamNameHash");
        requireNonBlank(state, "state");
        requireNonBlank(profile, "profile");
        attributes = MetadataCanonicalizer.canonicalStringMap(
                attributes,
                ApiLimits.MAX_STREAM_ATTRIBUTES_ENCODED_BYTES,
                "attributes");
        if (createdAtMillis < 0 || policyVersion < 0 || metadataVersion < 0) {
            throw new IllegalArgumentException("stream metadata numeric fields must be non-negative");
        }
    }

    private static void requireNonBlank(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName);
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " cannot be blank");
        }
    }
}
