/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaStorageAdmissionTest {
    @Test
    void readinessCanRecoverBeforeDrainButDrainAndCloseAreIrreversible() {
        KafkaStorageAdmission admission = new KafkaStorageAdmission();

        assertThat(admission.health()).isEqualTo(new KafkaStorageHealth(
                KafkaStorageAdmissionState.STARTING, false, "runtime starting"));
        assertThat(admission.markReady()).isTrue();
        admission.requireReady("produce");
        assertThat(admission.markNotReady("provider circuit open")).isTrue();
        assertThat(admission.health().detail()).isEqualTo("provider circuit open");
        assertThat(admission.markReady()).isTrue();

        assertThat(admission.beginDrain(DrainReason.BROKER_SHUTDOWN)).isTrue();
        assertThat(admission.markReady()).isFalse();
        assertThat(admission.markNotReady("late provider callback")).isFalse();
        assertThat(admission.beginDrain(DrainReason.OPERATOR_REQUEST)).isFalse();
        assertThat(admission.state()).isEqualTo(KafkaStorageAdmissionState.DRAINING);

        assertThat(admission.close()).isTrue();
        assertThat(admission.close()).isFalse();
        assertThat(admission.markReady()).isFalse();
        assertThat(admission.state()).isEqualTo(KafkaStorageAdmissionState.CLOSED);
    }

    @Test
    void requireReadyFailsBeforeWorkWithStableErrorClassification() {
        KafkaStorageAdmission admission = new KafkaStorageAdmission();

        assertRejected(admission, ErrorCode.METADATA_UNAVAILABLE, true, "STARTING");
        admission.markNotReady("activation unavailable");
        assertRejected(admission, ErrorCode.METADATA_UNAVAILABLE, true, "activation unavailable");
        admission.beginDrain(DrainReason.STARTUP_FAILURE);
        assertRejected(admission, ErrorCode.METADATA_UNAVAILABLE, true, "DRAINING");
        admission.close();
        assertRejected(admission, ErrorCode.STORAGE_CLOSED, false, "CLOSED");

        assertThatThrownBy(() -> admission.requireReady(" "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void exactlyOneConcurrentDrainCallerWins() throws Exception {
        KafkaStorageAdmission admission = new KafkaStorageAdmission();
        admission.markReady();
        List<Callable<Boolean>> drains = java.util.stream.IntStream.range(0, 32)
                .mapToObj(ignored -> (Callable<Boolean>) () -> admission.beginDrain(DrainReason.BROKER_FENCED))
                .toList();

        try (var executor = Executors.newFixedThreadPool(8)) {
            long winners = executor.invokeAll(drains).stream().filter(future -> {
                try {
                    return future.get();
                } catch (Exception failure) {
                    throw new AssertionError(failure);
                }
            }).count();
            assertThat(winners).isEqualTo(1);
        }
        assertThat(admission.state()).isEqualTo(KafkaStorageAdmissionState.DRAINING);
    }

    @Test
    void healthRejectsAmbiguousReadinessAndBlankDetails() {
        assertThatThrownBy(() -> new KafkaStorageHealth(
                KafkaStorageAdmissionState.NOT_READY, true, "invalid"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new KafkaStorageHealth(
                KafkaStorageAdmissionState.READY, true, " "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static void assertRejected(
            KafkaStorageAdmission admission,
            ErrorCode code,
            boolean retriable,
            String messageFragment) {
        assertThatThrownBy(() -> admission.requireReady("fetch"))
                .isInstanceOfSatisfying(NereusException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(code);
                    assertThat(failure.retriable()).isEqualTo(retriable);
                    assertThat(failure).hasMessageContaining(messageFragment);
                });
    }
}
