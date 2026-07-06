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

package io.nereus.api;

import java.util.Map;
import java.util.Objects;

/** Stream metadata returned by the protocol-neutral L0 API. */
public record StreamMetadata(
        StreamId streamId,
        StreamName streamName,
        StreamState state,
        StorageProfile profile,
        Map<String, String> attributes,
        long createdAtMillis,
        long metadataVersion,
        long committedEndOffset,
        long cumulativeSize,
        long trimOffset) {
    public StreamMetadata {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(streamName, "streamName");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(profile, "profile");
        attributes = MetadataCanonicalizer.canonicalStringMap(
                attributes,
                ApiLimits.MAX_STREAM_ATTRIBUTES_ENCODED_BYTES,
                "attributes");
        if (createdAtMillis < 0 || metadataVersion < 0 || committedEndOffset < 0
                || cumulativeSize < 0 || trimOffset < 0) {
            throw new IllegalArgumentException("metadata numeric fields must be non-negative");
        }
        if (trimOffset > committedEndOffset) {
            throw new IllegalArgumentException("trimOffset must be <= committedEndOffset");
        }
    }
}
