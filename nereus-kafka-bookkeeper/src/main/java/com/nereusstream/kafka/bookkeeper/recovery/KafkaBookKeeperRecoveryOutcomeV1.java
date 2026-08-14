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

package com.nereusstream.kafka.bookkeeper.recovery;

/** Closed takeover result; only the first two outcomes install recovered protocol state. */
public enum KafkaBookKeeperRecoveryOutcomeV1 {
    RECOVERED_EXACT,
    RECOVERED_WITH_INERT_RESIDUE,
    OPEN_FAILED,
    FENCE_FAILED,
    PROVIDER_FAILURE,
    ENVELOPE_EXCEEDED,
    CORRUPT_HEADER,
    PHYSICAL_SHORTFALL,
    REPLICA_APPLIED_SHORTFALL,
    ELECTION_BOUNDARY_NOT_BATCH_ALIGNED
}
