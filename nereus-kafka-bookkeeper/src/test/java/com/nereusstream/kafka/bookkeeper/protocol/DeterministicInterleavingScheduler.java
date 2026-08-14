/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.kafka.bookkeeper.protocol;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Test-owned named scheduler for exact publication/fence interleavings. */
final class DeterministicInterleavingScheduler {
    private final Map<String, Runnable> pending = new LinkedHashMap<>();

    void schedule(String name, Runnable action) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(action, "action");
        if (pending.putIfAbsent(name, action) != null) {
            throw new IllegalArgumentException("duplicate deterministic action: " + name);
        }
    }

    void run(String name) {
        Runnable action = pending.remove(name);
        if (action == null) {
            throw new IllegalArgumentException("unknown deterministic action: " + name);
        }
        action.run();
    }

    int pendingCount() {
        return pending.size();
    }
}
