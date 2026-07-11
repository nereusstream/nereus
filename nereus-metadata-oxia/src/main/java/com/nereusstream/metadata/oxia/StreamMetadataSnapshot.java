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
}
