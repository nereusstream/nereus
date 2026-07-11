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

package com.nereusstream.metadata.oxia.records;

import com.nereusstream.api.ApiLimits;
import com.nereusstream.api.MetadataCanonicalizer;
import java.util.Map;
import java.util.Objects;

public record StreamHeadRecord(
        String streamId,
        String streamName,
        String streamNameHash,
        String state,
        String profile,
        Map<String, String> attributes,
        long createdAtMillis,
        long policyVersion,
        long committedEndOffset,
        long cumulativeSize,
        long commitVersion,
        long trimOffset,
        String lastCommitId,
        AppendSessionSnapshotRecord appendSession,
        long metadataVersion) {
    public StreamHeadRecord {
        streamId = requireNonBlank(streamId, "streamId");
        streamName = requireNonBlank(streamName, "streamName");
        streamNameHash = requireNonBlank(streamNameHash, "streamNameHash");
        state = requireNonBlank(state, "state");
        profile = requireNonBlank(profile, "profile");
        attributes = MetadataCanonicalizer.canonicalStringMap(
                attributes,
                ApiLimits.MAX_STREAM_ATTRIBUTES_ENCODED_BYTES,
                "attributes");
        lastCommitId = Objects.requireNonNull(lastCommitId, "lastCommitId");
        Objects.requireNonNull(appendSession, "appendSession");
        if (createdAtMillis < 0 || policyVersion < 0 || committedEndOffset < 0 || cumulativeSize < 0
                || commitVersion < 0 || trimOffset < 0 || metadataVersion < 0) {
            throw new IllegalArgumentException("stream head numeric fields must be non-negative");
        }
        if (trimOffset > committedEndOffset) {
            throw new IllegalArgumentException("trimOffset must be <= committedEndOffset");
        }
        boolean emptyCommitChain = lastCommitId.isEmpty();
        if (emptyCommitChain != (commitVersion == 0)
                || emptyCommitChain != (committedEndOffset == 0)
                || (emptyCommitChain && cumulativeSize != 0)) {
            throw new IllegalArgumentException("stream head commit anchor is inconsistent");
        }
    }

    public StreamMetadataRecord toMetadataRecord() {
        return new StreamMetadataRecord(
                streamId,
                streamName,
                streamNameHash,
                state,
                profile,
                attributes,
                createdAtMillis,
                policyVersion,
                metadataVersion);
    }

    private static String requireNonBlank(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName);
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " cannot be blank");
        }
        return value;
    }
}
