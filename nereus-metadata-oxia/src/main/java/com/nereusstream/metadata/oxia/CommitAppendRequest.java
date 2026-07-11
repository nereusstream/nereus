/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.MetadataCanonicalizer;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.SchemaRef;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.keys.DeterministicIds;
import com.nereusstream.api.target.ReadTarget;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.ReadTargetRecord;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Provider-neutral stable append request. */
public record CommitAppendRequest(
        StreamId streamId, String writerId, String writerRunIdHash, long epoch, String fencingToken,
        long expectedStartOffset, ReadTarget readTarget, PayloadFormat payloadFormat,
        int recordCount, int entryCount, long logicalBytes, List<SchemaRef> schemaRefs,
        long minEventTimeMillis, long maxEventTimeMillis, Optional<ProjectionRef> projectionRef) {
    public CommitAppendRequest {
        Objects.requireNonNull(streamId, "streamId");
        writerId = text(writerId, "writerId");
        writerRunIdHash = text(writerRunIdHash, "writerRunIdHash");
        fencingToken = text(fencingToken, "fencingToken");
        Objects.requireNonNull(readTarget, "readTarget");
        Objects.requireNonNull(payloadFormat, "payloadFormat");
        schemaRefs = MetadataCanonicalizer.canonicalSchemaRefs(schemaRefs);
        projectionRef = Objects.requireNonNull(projectionRef, "projectionRef");
        if (epoch < 0 || expectedStartOffset < 0 || recordCount <= 0 || entryCount <= 0 || logicalBytes < 0
                || minEventTimeMillis < 0 || maxEventTimeMillis < minEventTimeMillis) {
            throw new IllegalArgumentException("generic commit request fields are invalid");
        }
        try { Math.addExact(expectedStartOffset, recordCount); }
        catch (ArithmeticException e) { throw new IllegalArgumentException("logical range overflows", e); }
    }

    public String fencingTokenHash() { return DeterministicIds.stableHashComponent(fencingToken); }

    public ReadTargetRecord readTargetRecord() { return ReadTargetCodecRegistry.phase15().encode(readTarget); }

    public String projectionIdentity() {
        StringBuilder value = new StringBuilder();
        add(value, "projectionRef");
        projectionRef.ifPresentOrElse(projection -> {
            add(value, "present"); add(value, projection.type().name()); add(value, projection.value());
        }, () -> add(value, "absent"));
        return value.toString();
    }

    public String commitId() {
        ReadTargetRecord target = readTargetRecord();
        StringBuilder value = new StringBuilder("nereus-commit-v2\0");
        add(value, streamId.value()); add(value, Long.toString(expectedStartOffset));
        add(value, writerId); add(value, writerRunIdHash); add(value, Long.toString(epoch));
        add(value, fencingTokenHash()); add(value, Integer.toString(recordCount)); add(value, Integer.toString(entryCount));
        add(value, Long.toString(logicalBytes)); add(value, payloadFormat.name());
        add(value, Integer.toString(schemaRefs.size()));
        schemaRefs.forEach(schema -> { add(value, schema.namespace()); add(value, schema.id()); add(value, Long.toString(schema.version())); });
        add(value, Long.toString(minEventTimeMillis)); add(value, Long.toString(maxEventTimeMillis));
        add(value, projectionIdentity()); add(value, target.targetType());
        add(value, Integer.toString(target.targetVersion())); add(value, target.identityChecksumValue());
        return DeterministicIds.stableHashComponent(value.toString());
    }

    private static void add(StringBuilder builder, String value) {
        builder.append(value.getBytes(StandardCharsets.UTF_8).length).append(':').append(value);
    }
    private static String text(String value, String name) {
        Objects.requireNonNull(value, name); if (value.isBlank()) throw new IllegalArgumentException(name + " cannot be blank"); return value;
    }
}
