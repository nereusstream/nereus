/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.kafka.checkpoint;

import java.util.Arrays;

/** One independently checksummed NKC1 state section. */
public record KafkaCheckpointSection(
        int sectionType,
        int sectionVersion,
        int sectionFlags,
        byte[] payload) {
    public KafkaCheckpointSection {
        if (sectionType <= 0 || sectionType > 0xffff
                || sectionVersion <= 0 || sectionVersion > 0xffff
                || (sectionFlags & ~KafkaCheckpointFormatV1.SECTION_REQUIRED_FLAG) != 0) {
            throw new IllegalArgumentException("invalid Kafka checkpoint section header");
        }
        if (payload == null || payload.length > KafkaCheckpointFormatV1.MAX_SECTION_BYTES) {
            throw new IllegalArgumentException("Kafka checkpoint section payload is missing or too large");
        }
        payload = payload.clone();
    }

    public static KafkaCheckpointSection required(
            KafkaCheckpointSectionType type, byte[] payload) {
        return new KafkaCheckpointSection(
                type.wireId(), 1, KafkaCheckpointFormatV1.SECTION_REQUIRED_FLAG, payload);
    }

    public boolean required() {
        return (sectionFlags & KafkaCheckpointFormatV1.SECTION_REQUIRED_FLAG) != 0;
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof KafkaCheckpointSection that
                && sectionType == that.sectionType
                && sectionVersion == that.sectionVersion
                && sectionFlags == that.sectionFlags
                && Arrays.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return 31 * java.util.Objects.hash(sectionType, sectionVersion, sectionFlags)
                + Arrays.hashCode(payload);
    }
}
