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

package com.nereusstream.metadata.oxia.v2.allocator.evidence;

import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Bounded fail-after-drain collection for a predeclared allocator operation batch. */
final class M3BoundedCompletionDrain {
    private M3BoundedCompletionDrain() {}

    static void await(
            CompletionService<Void> completions,
            int count,
            long timeoutSeconds,
            String label)
            throws Exception {
        Objects.requireNonNull(completions, "completions");
        Objects.requireNonNull(label, "label");
        if (count < 0 || timeoutSeconds <= 0 || label.isBlank()) {
            throw new IllegalArgumentException("allocator completion-drain bounds are invalid");
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        Throwable firstFailure = null;
        for (int index = 0; index < count; index++) {
            long remaining = deadline - System.nanoTime();
            Future<Void> completed = remaining <= 0
                    ? null
                    : completions.poll(remaining, TimeUnit.NANOSECONDS);
            if (completed == null) {
                TimeoutException timeout = new TimeoutException(
                        label + " did not drain " + count + " completions within " + timeoutSeconds + " seconds");
                if (firstFailure == null) {
                    throw timeout;
                }
                firstFailure.addSuppressed(timeout);
                rethrow(firstFailure);
            }
            try {
                completed.get();
            } catch (ExecutionException failure) {
                firstFailure = accumulate(firstFailure, failure.getCause());
            } catch (CancellationException failure) {
                firstFailure = accumulate(firstFailure, failure);
            }
        }
        if (firstFailure != null) {
            rethrow(firstFailure);
        }
    }

    private static Throwable accumulate(Throwable first, Throwable next) {
        Throwable exact = next == null
                ? new IllegalStateException("allocator completion failed without a cause")
                : next;
        if (first == null) {
            return exact;
        }
        if (first != exact) {
            first.addSuppressed(exact);
        }
        return first;
    }

    private static void rethrow(Throwable failure) throws Exception {
        if (failure instanceof Exception exception) {
            throw exception;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        throw new RuntimeException(failure);
    }
}
