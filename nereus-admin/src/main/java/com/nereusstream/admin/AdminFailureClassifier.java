/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.admin;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

final class AdminFailureClassifier {

    private AdminFailureClassifier() {
    }

    static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof ExecutionException
                        || current instanceof CompletionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    static AdminExitCode classify(Throwable failure, AdminExitCode fallback) {
        Throwable cause = unwrap(failure);
        if (cause instanceof TimeoutException
                || (cause instanceof NereusException nereus
                        && nereus.code() == ErrorCode.TIMEOUT)) {
            return AdminExitCode.TIMEOUT;
        }
        return fallback;
    }

    static String safeMessage(Throwable failure) {
        Throwable cause = unwrap(failure);
        String message = cause.getMessage();
        return message == null || message.isBlank()
                ? cause.getClass().getSimpleName()
                : message;
    }
}
