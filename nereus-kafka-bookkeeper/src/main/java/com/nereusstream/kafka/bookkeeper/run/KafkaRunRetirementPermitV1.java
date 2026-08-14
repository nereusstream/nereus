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

package com.nereusstream.kafka.bookkeeper.run;

/** Exact local proof required before a sealed run stops serving reads. */
public record KafkaRunRetirementPermitV1(
        boolean manifestNoLongerSelectsRun,
        boolean sourceProtectionDrained,
        long activeReadPins,
        boolean retentionElapsed) {
    public KafkaRunRetirementPermitV1 {
        if (activeReadPins < 0) {
            throw new IllegalArgumentException("active read-pin count must be non-negative");
        }
    }

    public boolean permitsRetirement() {
        return manifestNoLongerSelectsRun && sourceProtectionDrained && activeReadPins == 0 && retentionElapsed;
    }
}
