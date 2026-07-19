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

package com.nereusstream.metadata.oxia;

import com.nereusstream.metadata.oxia.records.CommittedEndOffsetRecord;
import com.nereusstream.metadata.oxia.records.StreamMetadataRecord;
import com.nereusstream.metadata.oxia.records.TrimRecord;
import java.util.Objects;

/** Three read views hydrated from one authoritative stream-head value. */
public record StreamMetadataSnapshot(
        StreamMetadataRecord metadata,
        CommittedEndOffsetRecord committedEnd,
        TrimRecord trim) {
    public StreamMetadataSnapshot {
        Objects.requireNonNull(metadata, "metadata");
        Objects.requireNonNull(committedEnd, "committedEnd");
        Objects.requireNonNull(trim, "trim");
        if (!metadata.streamId().equals(committedEnd.streamId())
                || !metadata.streamId().equals(trim.streamId())) {
            throw new IllegalArgumentException("snapshot records must belong to one stream");
        }
        if (metadata.metadataVersion() != committedEnd.metadataVersion()
                || metadata.metadataVersion() != trim.metadataVersion()) {
            throw new IllegalArgumentException("snapshot records must share one metadata version");
        }
        if (trim.trimOffset() > committedEnd.committedEndOffset()) {
            throw new IllegalArgumentException("trim offset cannot exceed committed end");
        }
    }

    public long metadataVersion() {
        return metadata.metadataVersion();
    }

    /**
     * Compares the complete versioned authority exposed by this snapshot.
     *
     * <p>{@link TrimRecord#reason()} and {@link TrimRecord#updatedAtMillis()} are hydrated
     * observation fields rather than fields in the authoritative stream-head value. They must
     * therefore not make two reads of the same head version appear different.
     */
    public boolean sameVersionedAuthority(StreamMetadataSnapshot other) {
        return other != null
                && metadataVersion() == other.metadataVersion()
                && sameSemanticAuthority(other);
    }

    /**
     * Compares stable stream authority while ignoring the shared stream-head version.
     *
     * <p>This form is reserved for protocols whose evidence deliberately excludes append-session
     * renewal and other version-only head changes. Commit, trim, policy, profile, and lifecycle
     * changes remain observable.
     */
    public boolean sameSemanticAuthority(StreamMetadataSnapshot other) {
        if (other == null) {
            return false;
        }
        StreamMetadataRecord otherMetadata = other.metadata();
        CommittedEndOffsetRecord otherCommittedEnd = other.committedEnd();
        TrimRecord otherTrim = other.trim();
        return metadata.streamId().equals(otherMetadata.streamId())
                && metadata.streamName().equals(otherMetadata.streamName())
                && metadata.streamNameHash().equals(otherMetadata.streamNameHash())
                && metadata.state().equals(otherMetadata.state())
                && metadata.profile().equals(otherMetadata.profile())
                && metadata.attributes().equals(otherMetadata.attributes())
                && metadata.createdAtMillis() == otherMetadata.createdAtMillis()
                && metadata.policyVersion() == otherMetadata.policyVersion()
                && committedEnd.streamId().equals(otherCommittedEnd.streamId())
                && committedEnd.committedEndOffset()
                        == otherCommittedEnd.committedEndOffset()
                && committedEnd.cumulativeSize()
                        == otherCommittedEnd.cumulativeSize()
                && committedEnd.commitVersion()
                        == otherCommittedEnd.commitVersion()
                && trim.streamId().equals(otherTrim.streamId())
                && trim.trimOffset() == otherTrim.trimOffset();
    }
}
