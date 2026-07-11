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

/** Metrics hook for M5 read amplification, backpressure, and index-cache behavior. */
public interface ReadMetricsObserver {
    default void onSliceRead(
            long fullSlicePayloadBytes,
            long entryIndexBytes,
            long returnedPayloadBytes) {
    }

    default void onBackpressureRejected(long requestedBufferBytes) {
    }

    default void onOffsetIndexCacheHit() {
    }

    default void onOffsetIndexCacheMiss() {
    }

    static ReadMetricsObserver noop() {
        return new ReadMetricsObserver() {
        };
    }
}
