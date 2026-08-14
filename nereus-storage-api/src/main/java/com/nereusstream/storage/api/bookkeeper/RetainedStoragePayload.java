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

package com.nereusstream.storage.api.bookkeeper;

import com.nereusstream.domain.bytes.Sha256Digest;
import java.nio.ByteBuffer;

/**
 * Immutable, replayable payload with explicit reference ownership.
 *
 * <p>The caller owns its reference. A session that accepts an append retains one independent reference and releases it
 * only after exact terminal reconciliation; cancellation or timeout of an observer does not release the session's
 * reference.
 */
public interface RetainedStoragePayload {
    int readableBytes();

    Sha256Digest sha256();

    ByteBuffer readOnlyBuffer();

    RetainedStoragePayload retain();

    boolean release();
}
