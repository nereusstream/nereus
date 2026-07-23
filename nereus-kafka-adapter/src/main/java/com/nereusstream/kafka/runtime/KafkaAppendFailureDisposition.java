/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.ErrorCode;
import java.util.Objects;
import java.util.Optional;

/** Protocol-neutral append failure facts consumed by the Kafka fork exception mapper. */
public record KafkaAppendFailureDisposition(
        ErrorCode errorCode,
        Optional<AppendOutcome> appendOutcome,
        boolean retriable,
        KafkaAppendFailureAction action) {
    public KafkaAppendFailureDisposition {
        Objects.requireNonNull(errorCode, "errorCode");
        appendOutcome = Objects.requireNonNull(appendOutcome, "appendOutcome");
        Objects.requireNonNull(action, "action");
        if (action == KafkaAppendFailureAction.REJECT_WITHOUT_FENCE
                && (!appendOutcome.isPresent()
                        || appendOutcome.orElseThrow() != AppendOutcome.KNOWN_NOT_COMMITTED)) {
            throw new IllegalArgumentException("only a known-not-committed append may remain writable");
        }
    }
}
