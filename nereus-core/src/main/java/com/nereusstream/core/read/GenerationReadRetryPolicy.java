/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;

/** Per-candidate transient object-read retry bound before same-view physical fallback. */
public record GenerationReadRetryPolicy(int maxTransientRetriesBeforeFallback) {
    public GenerationReadRetryPolicy {
        if (maxTransientRetriesBeforeFallback < 0 || maxTransientRetriesBeforeFallback > 16) {
            throw new IllegalArgumentException(
                    "maxTransientRetriesBeforeFallback must be in [0, 16]");
        }
    }

    public static GenerationReadRetryPolicy defaults() {
        return new GenerationReadRetryPolicy(2);
    }

    boolean retryAfter(int completedTransientRetries) {
        if (completedTransientRetries < 0) {
            throw new IllegalArgumentException("completedTransientRetries must be non-negative");
        }
        return completedTransientRetries < maxTransientRetriesBeforeFallback;
    }
}
