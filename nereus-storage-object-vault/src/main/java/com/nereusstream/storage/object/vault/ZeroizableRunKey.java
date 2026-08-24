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

package com.nereusstream.storage.object.vault;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/** Application-owned 32-byte run key whose retained byte array is erased at the run/session lifecycle boundary. */
public final class ZeroizableRunKey implements AutoCloseable {
    private final byte[] key;
    private final AtomicBoolean destroyed = new AtomicBoolean();

    public ZeroizableRunKey(byte[] key) {
        Objects.requireNonNull(key, "key");
        if (key.length != 32) {
            throw new IllegalArgumentException("WalRun key must be exactly 32 bytes");
        }
        this.key = key.clone();
    }

    /** The callback sees a temporary clone which is erased immediately after it returns or throws. */
    public synchronized <T> T use(Function<byte[], T> operation) {
        Objects.requireNonNull(operation, "operation");
        requireLive();
        byte[] temporary = key.clone();
        try {
            return operation.apply(temporary);
        } finally {
            Arrays.fill(temporary, (byte) 0);
        }
    }

    public boolean isDestroyed() {
        return destroyed.get();
    }

    @Override
    public synchronized void close() {
        if (destroyed.compareAndSet(false, true)) {
            Arrays.fill(key, (byte) 0);
        }
    }

    private void requireLive() {
        if (destroyed.get()) {
            throw new IllegalStateException("WalRun key has been destroyed");
        }
    }
}
