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

import io.nereus.api.MetadataCanonicalizer;
import io.nereus.api.SchemaRef;
import java.util.List;
import java.util.Objects;

public record StreamCommitRecord(
        String streamId,
        String commitId,
        String previousCommitId,
        long offsetStart,
        long offsetEnd,
        long generation,
        long cumulativeSize,
        long commitVersion,
        String writerId,
        String writerRunIdHash,
        long writerEpoch,
        String fencingTokenHash,
        String objectId,
        String objectKey,
        String sliceId,
        String objectType,
        String physicalFormat,
        String logicalFormat,
        String payloadFormat,
        String objectChecksumType,
        String objectChecksumValue,
        long objectOffset,
        long objectLength,
        int recordCount,
        int entryCount,
        long logicalBytes,
        List<SchemaRef> schemaRefs,
        EntryIndexReferenceRecord entryIndexRef,
        String projectionRef,
        String sliceChecksumType,
        String sliceChecksumValue,
        long minEventTimeMillis,
        long maxEventTimeMillis,
        long preparedAtMillis,
        long metadataVersion) {
    public StreamCommitRecord {
        streamId = requireNonBlank(streamId, "streamId");
        commitId = requireNonBlank(commitId, "commitId");
        previousCommitId = Objects.requireNonNull(previousCommitId, "previousCommitId");
        writerId = requireNonBlank(writerId, "writerId");
        writerRunIdHash = requireNonBlank(writerRunIdHash, "writerRunIdHash");
        fencingTokenHash = requireNonBlank(fencingTokenHash, "fencingTokenHash");
        objectId = requireNonBlank(objectId, "objectId");
        objectKey = requireNonBlank(objectKey, "objectKey");
        sliceId = requireNonBlank(sliceId, "sliceId");
        objectType = requireNonBlank(objectType, "objectType");
        physicalFormat = requireNonBlank(physicalFormat, "physicalFormat");
        logicalFormat = requireNonBlank(logicalFormat, "logicalFormat");
        payloadFormat = requireNonBlank(payloadFormat, "payloadFormat");
        objectChecksumType = requireNonBlank(objectChecksumType, "objectChecksumType");
        objectChecksumValue = requireNonBlank(objectChecksumValue, "objectChecksumValue");
        schemaRefs = MetadataCanonicalizer.canonicalSchemaRefs(schemaRefs);
        Objects.requireNonNull(entryIndexRef, "entryIndexRef");
        projectionRef = Objects.requireNonNull(projectionRef, "projectionRef");
        sliceChecksumType = requireNonBlank(sliceChecksumType, "sliceChecksumType");
        sliceChecksumValue = requireNonBlank(sliceChecksumValue, "sliceChecksumValue");
        if (offsetStart < 0 || offsetEnd <= offsetStart || generation < 0 || cumulativeSize < 0
                || commitVersion < 0 || writerEpoch < 0 || recordCount <= 0 || entryCount <= 0
                || logicalBytes < 0 || minEventTimeMillis < 0 || maxEventTimeMillis < minEventTimeMillis
                || preparedAtMillis < 0 || metadataVersion < 0) {
            throw new IllegalArgumentException("stream commit numeric fields are invalid");
        }
        MetadataRecordValidation.requireNonNegativeNonOverflowingRange(
                objectOffset,
                objectLength,
                "stream commit object");
    }

    private static String requireNonBlank(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName);
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " cannot be blank");
        }
        return value;
    }
}
