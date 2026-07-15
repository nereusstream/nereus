/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import java.time.Duration;

/** Non-authoritative process metrics for registry passes and bounded shutdown. */
public interface MaterializationMetricsObserver {
    default void scanCompleted(
            RegisteredMaterializationScanResult result,
            Duration elapsed) {
    }

    default void scanFailed(Throwable failure, Duration elapsed) {
    }

    default void closeCompleted(boolean deadlineForced, Duration elapsed) {
    }

    static MaterializationMetricsObserver noop() {
        return new MaterializationMetricsObserver() {
        };
    }
}
