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

package com.nereusstream.kafka.bookkeeper.adapter;

import java.util.Objects;

/** Fail-closed K2 validation failure with a stable rejection kind. */
public final class KafkaAssignedRecordBatchException extends IllegalArgumentException {
    private final KafkaAssignedRecordBatchRejectionV1 rejection;

    KafkaAssignedRecordBatchException(KafkaAssignedRecordBatchRejectionV1 rejection, String message) {
        super(message);
        this.rejection = Objects.requireNonNull(rejection, "rejection");
    }

    KafkaAssignedRecordBatchException(KafkaAssignedRecordBatchRejectionV1 rejection, String message, Throwable cause) {
        super(message, cause);
        this.rejection = Objects.requireNonNull(rejection, "rejection");
    }

    public KafkaAssignedRecordBatchRejectionV1 rejection() {
        return rejection;
    }
}
