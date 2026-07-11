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

package io.nereus.core.append;

import io.nereus.api.AppendOutcome;
import io.nereus.api.ErrorCode;
import io.nereus.api.NereusException;

final class AppendResourceLimiter {
    private final int maxInFlight;
    private final long maxBufferedBytes;
    private int inFlight;
    private long bufferedBytes;

    AppendResourceLimiter(int maxInFlight, long maxBufferedBytes) {
        this.maxInFlight = maxInFlight;
        this.maxBufferedBytes = maxBufferedBytes;
    }

    synchronized Reservation accept() {
        if (inFlight >= maxInFlight) {
            throw new NereusException(
                    ErrorCode.BACKPRESSURE_REJECTED,
                    true,
                    "maxInFlightAppends is full",
                    AppendOutcome.KNOWN_NOT_COMMITTED);
        }
        inFlight++;
        return new Reservation(this);
    }

    private synchronized void reserveBuffer(Reservation reservation, long conservativeBytes) {
        if (conservativeBytes <= 0 || conservativeBytes > maxBufferedBytes
                || bufferedBytes > maxBufferedBytes - conservativeBytes) {
            throw new NereusException(
                    ErrorCode.BACKPRESSURE_REJECTED,
                    true,
                    "maxBufferedBytes is full",
                    AppendOutcome.KNOWN_NOT_COMMITTED);
        }
        bufferedBytes += conservativeBytes;
        reservation.bytes = conservativeBytes;
    }

    private synchronized void adjust(Reservation reservation, long exactBytes) {
        if (exactBytes <= 0) {
            throw new IllegalArgumentException("exactBytes must be positive");
        }
        if (reservation.bytes == 0) {
            throw new IllegalStateException("append buffer must be reserved before exact adjustment");
        }
        long withoutCurrent = bufferedBytes - reservation.bytes;
        if (exactBytes > maxBufferedBytes || withoutCurrent > maxBufferedBytes - exactBytes) {
            throw new NereusException(
                    ErrorCode.BACKPRESSURE_REJECTED,
                    true,
                    "exact WAL object size exceeds available append buffer",
                    AppendOutcome.KNOWN_NOT_COMMITTED);
        }
        bufferedBytes = withoutCurrent + exactBytes;
        reservation.bytes = exactBytes;
    }

    private synchronized void release(Reservation reservation) {
        if (reservation.released) {
            return;
        }
        reservation.released = true;
        inFlight--;
        bufferedBytes -= reservation.bytes;
    }

    static final class Reservation implements AutoCloseable {
        private final AppendResourceLimiter owner;
        private long bytes;
        private boolean released;

        private Reservation(AppendResourceLimiter owner) {
            this.owner = owner;
        }

        void reserveBuffer(long conservativeBytes) {
            owner.reserveBuffer(this, conservativeBytes);
        }

        void adjustToExactBytes(long exactBytes) {
            owner.adjust(this, exactBytes);
        }

        @Override
        public void close() {
            owner.release(this);
        }
    }
}
