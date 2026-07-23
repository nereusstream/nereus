/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.recovery;

import com.nereusstream.metadata.oxia.records.KafkaCheckpointReferenceRecord;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointHeader;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointSection;
import java.util.List;
import java.util.Objects;

/** Heap-owned decoded checkpoint state after its durable reader pin has been released. */
public record KafkaRecoveredCheckpoint(
        KafkaCheckpointReferenceRecord reference,
        KafkaCheckpointHeader header,
        List<KafkaCheckpointSection> sections) {
    public KafkaRecoveredCheckpoint {
        Objects.requireNonNull(reference, "reference");
        Objects.requireNonNull(header, "header");
        sections = List.copyOf(Objects.requireNonNull(sections, "sections"));
    }
}
