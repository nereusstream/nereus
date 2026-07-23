/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.partition;

import com.nereusstream.api.AppendResult;
import com.nereusstream.kafka.codec.EncodedKafkaAppend;
import java.util.Objects;

/** Stable append result safe for Kafka LEO/HW and derived-state publication. */
public record KafkaStableAppendResult(
        AppendResult appendResult,
        EncodedKafkaAppend encodedAppend,
        KafkaStableSnapshot stableSnapshot,
        short requiredAcks) {
    public KafkaStableAppendResult {
        Objects.requireNonNull(appendResult, "appendResult");
        Objects.requireNonNull(encodedAppend, "encodedAppend");
        Objects.requireNonNull(stableSnapshot, "stableSnapshot");
        if (appendResult.committedEndOffset() != stableSnapshot.stableEndOffset()) {
            throw new IllegalArgumentException("append result and stable snapshot end offsets differ");
        }
    }
}
