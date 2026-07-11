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

package com.nereusstream.core.read;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;

final class ReadResourceLimiter {
    private final int maxConcurrentReads;
    private final long maxBufferedBytes;
    private int concurrentReads;
    private long bufferedBytes;

    ReadResourceLimiter(int maxConcurrentReads, long maxBufferedBytes) {
        this.maxConcurrentReads = maxConcurrentReads;
        this.maxBufferedBytes = maxBufferedBytes;
    }

    synchronized Reservation reserve(long bytes) {
        if (bytes <= 0 || bytes > maxBufferedBytes || concurrentReads >= maxConcurrentReads
                || bufferedBytes > maxBufferedBytes - bytes) {
            throw new NereusException(
                    ErrorCode.BACKPRESSURE_REJECTED,
                    true,
                    "read object permit or buffer budget is full");
        }
        concurrentReads++;
        bufferedBytes += bytes;
        return new Reservation(this, bytes);
    }

    private synchronized void release(Reservation reservation) {
        if (reservation.released) {
            return;
        }
        reservation.released = true;
        concurrentReads--;
        bufferedBytes -= reservation.bytes;
    }

    static final class Reservation implements AutoCloseable {
        private final ReadResourceLimiter owner;
        private final long bytes;
        private boolean released;

        private Reservation(ReadResourceLimiter owner, long bytes) {
            this.owner = owner;
            this.bytes = bytes;
        }

        @Override
        public void close() {
            owner.release(this);
        }
    }
}
