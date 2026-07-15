/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.records.TaskFailureClass;
import java.util.Objects;

/** Typed worker failure whose durable transition never depends on parsing an exception message. */
final class MaterializationExecutionException extends NereusException {
    private final TaskFailureClass failureClass;

    MaterializationExecutionException(
            TaskFailureClass failureClass,
            ErrorCode code,
            boolean retriable,
            String message) {
        this(failureClass, code, retriable, message, null);
    }

    MaterializationExecutionException(
            TaskFailureClass failureClass,
            ErrorCode code,
            boolean retriable,
            String message,
            Throwable cause) {
        super(code, retriable, message, cause);
        this.failureClass = Objects.requireNonNull(failureClass, "failureClass");
        if (failureClass == TaskFailureClass.NONE) {
            throw new IllegalArgumentException("execution failure class cannot be NONE");
        }
    }

    TaskFailureClass failureClass() {
        return failureClass;
    }
}
