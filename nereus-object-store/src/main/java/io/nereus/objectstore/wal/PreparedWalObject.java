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

package io.nereus.objectstore.wal;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Objects;

/** Immutable, exactly-sized WAL object that has not yet been uploaded. */
public final class PreparedWalObject {
    private final WalWriteResult result;
    private final byte[] encodedBytes;
    private final Duration uploadTimeout;

    public PreparedWalObject(
            WalWriteResult result,
            byte[] encodedBytes,
            Duration uploadTimeout) {
        this.result = Objects.requireNonNull(result, "result");
        this.encodedBytes = Objects.requireNonNull(encodedBytes, "encodedBytes").clone();
        this.uploadTimeout = Objects.requireNonNull(uploadTimeout, "uploadTimeout");
        if (result.objectLength() != encodedBytes.length) {
            throw new IllegalArgumentException("result objectLength must equal encoded byte length");
        }
        if (uploadTimeout.isZero() || uploadTimeout.isNegative()) {
            throw new IllegalArgumentException("uploadTimeout must be positive");
        }
    }

    public WalWriteResult result() {
        return result;
    }

    public long objectLength() {
        return encodedBytes.length;
    }

    public ByteBuffer payload() {
        return ByteBuffer.wrap(encodedBytes).asReadOnlyBuffer();
    }

    public Duration uploadTimeout() {
        return uploadTimeout;
    }
}
