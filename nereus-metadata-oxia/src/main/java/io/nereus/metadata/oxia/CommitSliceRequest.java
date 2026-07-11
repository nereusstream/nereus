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

package io.nereus.metadata.oxia;

import io.nereus.api.Checksum;
import io.nereus.api.EntryIndexRef;
import io.nereus.api.MetadataCanonicalizer;
import io.nereus.api.ObjectId;
import io.nereus.api.ObjectKey;
import io.nereus.api.PayloadFormat;
import io.nereus.api.ProjectionRef;
import io.nereus.api.SchemaRef;
import io.nereus.api.StreamId;
import io.nereus.api.keys.DeterministicIds;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public record CommitSliceRequest(
        StreamId streamId,
        String writerId,
        String writerRunIdHash,
        long epoch,
        String fencingToken,
        long expectedStartOffset,
        String sliceId,
        int recordCount,
        int entryCount,
        long logicalBytes,
        List<SchemaRef> schemaRefs,
        ObjectId objectId,
        ObjectKey objectKey,
        Checksum objectChecksum,
        long objectOffset,
        long objectLength,
        EntryIndexRef entryIndexRef,
        Checksum sliceChecksum,
        PayloadFormat payloadFormat,
        long minEventTimeMillis,
        long maxEventTimeMillis,
        Optional<ProjectionRef> projectionRef) {
    public CommitSliceRequest {
        Objects.requireNonNull(streamId, "streamId");
        writerId = requireNonBlank(writerId, "writerId");
        writerRunIdHash = requireNonBlank(writerRunIdHash, "writerRunIdHash");
        fencingToken = requireNonBlank(fencingToken, "fencingToken");
        sliceId = requireNonBlank(sliceId, "sliceId");
        schemaRefs = MetadataCanonicalizer.canonicalSchemaRefs(schemaRefs);
        Objects.requireNonNull(objectId, "objectId");
        Objects.requireNonNull(objectKey, "objectKey");
        Objects.requireNonNull(objectChecksum, "objectChecksum");
        Objects.requireNonNull(entryIndexRef, "entryIndexRef");
        Objects.requireNonNull(sliceChecksum, "sliceChecksum");
        Objects.requireNonNull(payloadFormat, "payloadFormat");
        projectionRef = Objects.requireNonNull(projectionRef, "projectionRef");
        if (epoch < 0 || expectedStartOffset < 0 || logicalBytes < 0 || minEventTimeMillis < 0
                || maxEventTimeMillis < minEventTimeMillis) {
            throw new IllegalArgumentException("commit request numeric fields are invalid");
        }
        if (recordCount <= 0 || entryCount <= 0) {
            throw new IllegalArgumentException("recordCount and entryCount must be positive");
        }
        if (objectLength == 0) {
            throw new IllegalArgumentException("object length must be positive");
        }
        requireNonNegativeNonOverflowingRange(expectedStartOffset, recordCount, "logical offset");
        requireNonNegativeNonOverflowingRange(objectOffset, objectLength, "object");
    }

    public String fencingTokenHash() {
        return DeterministicIds.stableHashComponent(fencingToken);
    }

    public String projectionIdentity() {
        CanonicalIdentity identity = new CanonicalIdentity();
        appendProjectionIdentity(identity);
        return identity.toString();
    }

    public static String emptyProjectionIdentity() {
        CanonicalIdentity identity = new CanonicalIdentity();
        identity.add("projectionRef");
        identity.add("absent");
        return identity.toString();
    }

    public String commitId() {
        CanonicalIdentity identity = new CanonicalIdentity();
        identity.add(streamId.value());
        identity.add(Long.toString(expectedStartOffset));
        identity.add(writerId);
        identity.add(writerRunIdHash);
        identity.add(Long.toString(epoch));
        identity.add(fencingTokenHash());
        identity.add(objectId.value());
        identity.add(objectKey.value());
        identity.add(objectChecksum.type().name());
        identity.add(objectChecksum.value());
        identity.add(sliceId);
        identity.add(Long.toString(objectOffset));
        identity.add(Long.toString(objectLength));
        identity.add(sliceChecksum.type().name());
        identity.add(sliceChecksum.value());
        identity.add(Integer.toString(recordCount));
        identity.add(Integer.toString(entryCount));
        identity.add(Long.toString(logicalBytes));
        identity.add(payloadFormat.name());
        identity.add(Integer.toString(schemaRefs.size()));
        for (SchemaRef schemaRef : schemaRefs) {
            identity.add(schemaRef.namespace());
            identity.add(schemaRef.id());
            identity.add(Long.toString(schemaRef.version()));
        }
        appendEntryIndexIdentity(identity, entryIndexRef);
        identity.add(Long.toString(minEventTimeMillis));
        identity.add(Long.toString(maxEventTimeMillis));
        appendProjectionIdentity(identity);
        return DeterministicIds.stableHashComponent(identity.toString());
    }

    private static void appendEntryIndexIdentity(CanonicalIdentity identity, EntryIndexRef ref) {
        identity.add("entryIndexRef");
        identity.add(ref.location().name());
        identity.add(ref.objectId().map(ObjectId::value).orElse(""));
        identity.add(ref.objectKey().map(ObjectKey::value).orElse(""));
        identity.add(ref.inlineData().map(Base64.getEncoder()::encodeToString).orElse(""));
        identity.add(Long.toString(ref.offset()));
        identity.add(Long.toString(ref.length()));
        identity.add(ref.checksum().type().name());
        identity.add(ref.checksum().value());
    }

    private void appendProjectionIdentity(CanonicalIdentity identity) {
        identity.add("projectionRef");
        projectionRef.ifPresentOrElse(
                projection -> {
                    identity.add("present");
                    identity.add(projection.type().name());
                    identity.add(projection.value());
                },
                () -> identity.add("absent"));
    }

    private static String requireNonBlank(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName);
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " cannot be blank");
        }
        return value;
    }

    private static void requireNonNegativeNonOverflowingRange(long offset, long length, String fieldName) {
        if (offset < 0 || length < 0) {
            throw new IllegalArgumentException(fieldName + " offset and length must be non-negative");
        }
        try {
            Math.addExact(offset, length);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException(fieldName + " offset + length must not overflow", e);
        }
    }

    private static final class CanonicalIdentity {
        private final StringBuilder builder = new StringBuilder();

        void add(String value) {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            builder.append(bytes.length).append(':').append(value);
        }

        @Override
        public String toString() {
            return builder.toString();
        }
    }
}
