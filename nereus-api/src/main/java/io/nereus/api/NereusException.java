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
import java.util.concurrent.CompletableFuture;

/** Public exception type used to complete asynchronous API calls exceptionally. */
public class NereusException extends RuntimeException {
    private final ErrorCode code;
    private final boolean retriable;

    public NereusException(ErrorCode code, boolean retriable, String message) {
        super(message);
        this.code = Objects.requireNonNull(code, "code");
        this.retriable = retriable;
    }

    public NereusException(ErrorCode code, boolean retriable, String message, Throwable cause) {
        super(message, cause);
        this.code = Objects.requireNonNull(code, "code");
        this.retriable = retriable;
    }

    public ErrorCode code() {
        return code;
    }

    public boolean retriable() {
        return retriable;
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
}
