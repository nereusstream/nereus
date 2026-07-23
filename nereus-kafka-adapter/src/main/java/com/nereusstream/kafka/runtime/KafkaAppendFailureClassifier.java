/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/** Conservative append-outcome classifier; Kafka exception names remain owned by the fork. */
public final class KafkaAppendFailureClassifier {
    private KafkaAppendFailureClassifier() {}

    public static KafkaAppendFailureDisposition classify(Throwable failure) {
        Throwable current = unwrap(failure);
        if (current instanceof IllegalArgumentException) {
            return new KafkaAppendFailureDisposition(
                    ErrorCode.INVALID_ARGUMENT,
                    Optional.of(AppendOutcome.KNOWN_NOT_COMMITTED),
                    false,
                    KafkaAppendFailureAction.REJECT_WITHOUT_FENCE);
        }
        if (!(current instanceof NereusException nereus)) {
            return fence(ErrorCode.METADATA_INVARIANT_VIOLATION, Optional.empty(), false);
        }
        Optional<AppendOutcome> outcome = nereus.appendOutcome();
        if (isCorruption(nereus.code())) {
            return new KafkaAppendFailureDisposition(
                    nereus.code(), outcome, false, KafkaAppendFailureAction.CORRUPT_OFFLINE);
        }
        if (nereus.code() == ErrorCode.FENCED_APPEND
                || nereus.code() == ErrorCode.APPEND_SESSION_EXPIRED
                || nereus.code() == ErrorCode.OFFSET_CONFLICT) {
            return fence(nereus.code(), outcome, nereus.retriable());
        }
        if (outcome.orElse(AppendOutcome.MAY_HAVE_COMMITTED) == AppendOutcome.KNOWN_NOT_COMMITTED) {
            return new KafkaAppendFailureDisposition(
                    nereus.code(), outcome, nereus.retriable(), KafkaAppendFailureAction.REJECT_WITHOUT_FENCE);
        }
        return fence(nereus.code(), outcome, nereus.retriable());
    }

    private static KafkaAppendFailureDisposition fence(
            ErrorCode code, Optional<AppendOutcome> outcome, boolean retriable) {
        return new KafkaAppendFailureDisposition(
                code, outcome, retriable, KafkaAppendFailureAction.WRITE_FENCE_RECOVERY_REQUIRED);
    }

    private static boolean isCorruption(ErrorCode code) {
        return code == ErrorCode.OBJECT_CHECKSUM_MISMATCH
                || code == ErrorCode.PRIMARY_WAL_CHECKSUM_MISMATCH
                || code == ErrorCode.METADATA_INVARIANT_VIOLATION
                || code == ErrorCode.UNSUPPORTED_FORMAT;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
