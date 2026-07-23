/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;

class KafkaAppendFailureClassifierTest {
    @Test
    void rejectsInvalidAndKnownNotCommittedFailuresWithoutFencing() {
        assertThat(KafkaAppendFailureClassifier.classify(new IllegalArgumentException("bad batch")))
                .isEqualTo(new KafkaAppendFailureDisposition(
                        ErrorCode.INVALID_ARGUMENT,
                        java.util.Optional.of(AppendOutcome.KNOWN_NOT_COMMITTED),
                        false,
                        KafkaAppendFailureAction.REJECT_WITHOUT_FENCE));

        KafkaAppendFailureDisposition backpressure = KafkaAppendFailureClassifier.classify(new NereusException(
                ErrorCode.BACKPRESSURE_REJECTED,
                true,
                "full",
                AppendOutcome.KNOWN_NOT_COMMITTED));
        assertThat(backpressure.action()).isEqualTo(KafkaAppendFailureAction.REJECT_WITHOUT_FENCE);
        assertThat(backpressure.retriable()).isTrue();
    }

    @Test
    void fencesAuthorityConflictsAndEveryUncertainOrCommittedOutcome() {
        KafkaAppendFailureDisposition fenced = KafkaAppendFailureClassifier.classify(new NereusException(
                ErrorCode.FENCED_APPEND,
                false,
                "old leader",
                AppendOutcome.KNOWN_NOT_COMMITTED));
        assertThat(fenced.action()).isEqualTo(KafkaAppendFailureAction.WRITE_FENCE_RECOVERY_REQUIRED);

        for (AppendOutcome outcome : new AppendOutcome[] {
                AppendOutcome.MAY_HAVE_COMMITTED, AppendOutcome.KNOWN_COMMITTED
        }) {
            KafkaAppendFailureDisposition uncertain = KafkaAppendFailureClassifier.classify(
                    new CompletionException(new NereusException(
                            ErrorCode.TIMEOUT, true, "outcome", outcome)));
            assertThat(uncertain.action()).isEqualTo(KafkaAppendFailureAction.WRITE_FENCE_RECOVERY_REQUIRED);
            assertThat(uncertain.appendOutcome()).contains(outcome);
        }
    }

    @Test
    void sendsChecksumFormatAndInvariantFailuresOffline() {
        for (ErrorCode code : new ErrorCode[] {
                ErrorCode.OBJECT_CHECKSUM_MISMATCH,
                ErrorCode.PRIMARY_WAL_CHECKSUM_MISMATCH,
                ErrorCode.METADATA_INVARIANT_VIOLATION,
                ErrorCode.UNSUPPORTED_FORMAT
        }) {
            KafkaAppendFailureDisposition disposition = KafkaAppendFailureClassifier.classify(
                    new NereusException(code, false, "corrupt", AppendOutcome.MAY_HAVE_COMMITTED));
            assertThat(disposition.action()).isEqualTo(KafkaAppendFailureAction.CORRUPT_OFFLINE);
            assertThat(disposition.retriable()).isFalse();
        }
    }

    @Test
    void missingOutcomeCannotBeDeclaredSafeAndExecutionWrappersAreUnwrapped() {
        assertThatThrownBy(() -> new KafkaAppendFailureDisposition(
                        ErrorCode.TIMEOUT,
                        java.util.Optional.empty(),
                        true,
                        KafkaAppendFailureAction.REJECT_WITHOUT_FENCE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("known-not-committed");

        KafkaAppendFailureDisposition disposition = KafkaAppendFailureClassifier.classify(
                new ExecutionException(new NereusException(
                        ErrorCode.TIMEOUT,
                        true,
                        "unknown append outcome")));
        assertThat(disposition.action()).isEqualTo(KafkaAppendFailureAction.WRITE_FENCE_RECOVERY_REQUIRED);
        assertThat(disposition.appendOutcome()).isEmpty();
    }
}
