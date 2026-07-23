/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Thread-safe process admission gate. DRAINING and CLOSED are irreversible so late startup or health callbacks cannot
 * accidentally reopen client traffic during shutdown.
 */
public final class KafkaStorageAdmission {
    private static final KafkaStorageHealth STARTING = new KafkaStorageHealth(
            KafkaStorageAdmissionState.STARTING, false, "runtime starting");

    private final AtomicReference<KafkaStorageHealth> health = new AtomicReference<>(STARTING);

    public KafkaStorageHealth health() {
        return health.get();
    }

    public KafkaStorageAdmissionState state() {
        return health.get().state();
    }

    public boolean ready() {
        return health.get().ready();
    }

    /** Marks local runtime prerequisites ready unless drain/close already won the race. */
    public boolean markReady() {
        while (true) {
            KafkaStorageHealth current = health.get();
            if (current.state() == KafkaStorageAdmissionState.READY) return true;
            if (terminal(current.state())) return false;
            KafkaStorageHealth ready = new KafkaStorageHealth(
                    KafkaStorageAdmissionState.READY, true, "runtime ready");
            if (health.compareAndSet(current, ready)) return true;
        }
    }

    /** Removes readiness without crossing an already-started drain/close boundary. */
    public boolean markNotReady(String detail) {
        String exactDetail = nonblank(detail, "detail");
        while (true) {
            KafkaStorageHealth current = health.get();
            if (terminal(current.state())) return false;
            KafkaStorageHealth notReady = new KafkaStorageHealth(
                    KafkaStorageAdmissionState.NOT_READY, false, exactDetail);
            if (health.compareAndSet(current, notReady)) return true;
        }
    }

    /** Starts irreversible drain. Exactly one concurrent caller returns true. */
    public boolean beginDrain(DrainReason reason) {
        DrainReason exactReason = Objects.requireNonNull(reason, "reason");
        while (true) {
            KafkaStorageHealth current = health.get();
            if (current.state() == KafkaStorageAdmissionState.DRAINING
                    || current.state() == KafkaStorageAdmissionState.CLOSED) {
                return false;
            }
            KafkaStorageHealth draining = new KafkaStorageHealth(
                    KafkaStorageAdmissionState.DRAINING,
                    false,
                    "runtime draining: " + exactReason.name());
            if (health.compareAndSet(current, draining)) return true;
        }
    }

    /** Installs the terminal state. Late callbacks cannot change it. */
    public boolean close() {
        while (true) {
            KafkaStorageHealth current = health.get();
            if (current.state() == KafkaStorageAdmissionState.CLOSED) return false;
            KafkaStorageHealth closed = new KafkaStorageHealth(
                    KafkaStorageAdmissionState.CLOSED, false, "runtime closed");
            if (health.compareAndSet(current, closed)) return true;
        }
    }

    /** Fails before buffer allocation or storage I/O unless the process is currently ready. */
    public void requireReady(String operation) {
        String exactOperation = nonblank(operation, "operation");
        KafkaStorageHealth current = health.get();
        if (current.ready()) return;
        ErrorCode code = current.state() == KafkaStorageAdmissionState.CLOSED
                ? ErrorCode.STORAGE_CLOSED
                : ErrorCode.METADATA_UNAVAILABLE;
        boolean retriable = current.state() != KafkaStorageAdmissionState.CLOSED;
        throw new NereusException(
                code,
                retriable,
                "Kafka storage rejects " + exactOperation + " while " + current.state()
                        + ": " + current.detail());
    }

    private static boolean terminal(KafkaStorageAdmissionState state) {
        return state == KafkaStorageAdmissionState.DRAINING || state == KafkaStorageAdmissionState.CLOSED;
    }

    private static String nonblank(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) throw new IllegalArgumentException(name + " must be nonblank");
        return value;
    }
}
