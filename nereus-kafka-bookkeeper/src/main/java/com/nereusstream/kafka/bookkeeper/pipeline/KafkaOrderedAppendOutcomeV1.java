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

package com.nereusstream.kafka.bookkeeper.pipeline;

/** K4 terminal result; COMMITTED_ORDERED is engine order only, not K5 coherent protocol publication or ACK. */
public enum KafkaOrderedAppendOutcomeV1 {
    COMMITTED_ORDERED,
    CAPACITY_REJECTED,
    PROTOCOL_VALIDATION_FAILED,
    OFFSET_ASSIGNMENT_FAILED,
    PROTOCOL_PREPARATION_FAILED,
    INVALID_ASSIGNMENT,
    DEFINITIVELY_FAILED,
    OUTCOME_UNKNOWN,
    FENCED_BY_PREDECESSOR
}
