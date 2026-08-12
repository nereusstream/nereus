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

package com.nereusstream.metadata.oxia.v2.continuity;

/**
 * Process-local P1 cut captured before authoritative reads and checked immediately before
 * installing a local ACTIVE fence.
 *
 * <p>The values are never persisted and never grant authority by themselves. They only prove that
 * the same READY notification-continuity generation remained current across an A/read/B sequence.
 */
public record InstallPermit(long clientGeneration, long invalidationEpoch) {
    public InstallPermit {
        if (clientGeneration < 0) {
            throw new IllegalArgumentException("clientGeneration must not be negative");
        }
        if (invalidationEpoch <= 0) {
            throw new IllegalArgumentException("invalidationEpoch must be positive");
        }
    }
}
