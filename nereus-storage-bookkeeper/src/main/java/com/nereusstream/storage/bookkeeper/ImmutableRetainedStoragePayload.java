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

package com.nereusstream.storage.bookkeeper;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.RetainedStoragePayload;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/** Defensively copied replayable payload used at the provider ownership boundary. */
public final class ImmutableRetainedStoragePayload implements RetainedStoragePayload {
    private final byte[] bytes;
    private final Sha256Digest sha256;
    private final AtomicInteger references = new AtomicInteger(1);

    private ImmutableRetainedStoragePayload(byte[] bytes) {
        this.bytes = bytes;
        this.sha256 = Sha256Digest.hash(CanonicalBytes.copyOf(bytes));
    }

    public static ImmutableRetainedStoragePayload copyOf(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        if (bytes.length == 0) {
            throw new IllegalArgumentException("payload must not be empty");
        }
        return new ImmutableRetainedStoragePayload(bytes.clone());
    }

    @Override
    public int readableBytes() {
        ensureAccessible();
        return bytes.length;
    }

    @Override
    public Sha256Digest sha256() {
        ensureAccessible();
        return sha256;
    }

    @Override
    public ByteBuffer readOnlyBuffer() {
        ensureAccessible();
        return ByteBuffer.wrap(bytes).asReadOnlyBuffer();
    }

    @Override
    public RetainedStoragePayload retain() {
        while (true) {
            int current = references.get();
            if (current == 0) {
                throw new IllegalStateException("payload is already released");
            }
            if (current == Integer.MAX_VALUE) {
                throw new IllegalStateException("payload reference count overflow");
            }
            if (references.compareAndSet(current, current + 1)) {
                return this;
            }
        }
    }

    @Override
    public boolean release() {
        while (true) {
            int current = references.get();
            if (current == 0) {
                throw new IllegalStateException("payload released too many times");
            }
            if (references.compareAndSet(current, current - 1)) {
                return current == 1;
            }
        }
    }

    public int referenceCount() {
        return references.get();
    }

    private void ensureAccessible() {
        if (references.get() == 0) {
            throw new IllegalStateException("payload is already released");
        }
    }
}
