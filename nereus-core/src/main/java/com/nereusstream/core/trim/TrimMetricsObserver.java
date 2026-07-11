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

package com.nereusstream.core.trim;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.StreamId;

/** Metrics hook for Phase 1 trim outcomes. */
public interface TrimMetricsObserver {
    default void onTrimSucceeded(StreamId streamId, long trimOffset) {
    }

    default void onTrimFailed(ErrorCode errorCode) {
    }

    static TrimMetricsObserver noop() {
        return new TrimMetricsObserver() {
        };
    }
}
