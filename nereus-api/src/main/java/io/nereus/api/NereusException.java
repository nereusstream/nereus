/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.nereus.api;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Public exception type used to complete asynchronous API calls exceptionally. */
public class NereusException extends RuntimeException {
    private final ErrorCode code;
    private final boolean retriable;
    private final Optional<AppendOutcome> appendOutcome;

    public NereusException(ErrorCode code, boolean retriable, String message) {
        this(code, retriable, message, null, Optional.empty());
    }

    public NereusException(ErrorCode code, boolean retriable, String message, Throwable cause) {
        this(code, retriable, message, cause, Optional.empty());
    }

    public NereusException(
            ErrorCode code,
            boolean retriable,
            String message,
            AppendOutcome appendOutcome) {
        this(code, retriable, message, null, Optional.of(Objects.requireNonNull(appendOutcome, "appendOutcome")));
    }

    public NereusException(
            ErrorCode code,
            boolean retriable,
            String message,
            Throwable cause,
            AppendOutcome appendOutcome) {
        this(code, retriable, message, cause, Optional.of(Objects.requireNonNull(appendOutcome, "appendOutcome")));
    }

    private NereusException(
            ErrorCode code,
            boolean retriable,
            String message,
            Throwable cause,
            Optional<AppendOutcome> appendOutcome) {
        super(message, cause);
        this.code = Objects.requireNonNull(code, "code");
        this.retriable = retriable;
        this.appendOutcome = Objects.requireNonNull(appendOutcome, "appendOutcome");
    }

    public ErrorCode code() {
        return code;
    }

    public boolean retriable() {
        return retriable;
    }

    /** Empty for failures that are not associated with an append commit attempt. */
    public Optional<AppendOutcome> appendOutcome() {
        return appendOutcome;
    }

    public static <T> CompletableFuture<T> failedFuture(
            ErrorCode code,
            boolean retriable,
            String message,
            Throwable cause) {
        return CompletableFuture.failedFuture(new NereusException(code, retriable, message, cause));
    }

    public static <T> CompletableFuture<T> failedFuture(
            ErrorCode code,
            boolean retriable,
            String message) {
        return CompletableFuture.failedFuture(new NereusException(code, retriable, message));
    }

    public static <T> CompletableFuture<T> failedAppendFuture(
            ErrorCode code,
            boolean retriable,
            AppendOutcome appendOutcome,
            String message,
            Throwable cause) {
        return CompletableFuture.failedFuture(
                new NereusException(code, retriable, message, cause, appendOutcome));
    }

    public static <T> CompletableFuture<T> failedAppendFuture(
            ErrorCode code,
            boolean retriable,
            AppendOutcome appendOutcome,
            String message) {
        return CompletableFuture.failedFuture(
                new NereusException(code, retriable, message, appendOutcome));
    }
}
