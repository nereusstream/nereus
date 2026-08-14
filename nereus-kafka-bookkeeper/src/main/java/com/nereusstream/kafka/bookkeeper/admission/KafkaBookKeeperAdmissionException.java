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

package com.nereusstream.kafka.bookkeeper.admission;

import java.util.Objects;

/** Fail-closed rejection before Kafka offset or BookKeeper entry allocation. */
public final class KafkaBookKeeperAdmissionException extends IllegalArgumentException {
    private final KafkaBookKeeperAdmissionRejectionV1 rejection;

    public KafkaBookKeeperAdmissionException(KafkaBookKeeperAdmissionRejectionV1 rejection, String message) {
        super(message);
        this.rejection = Objects.requireNonNull(rejection, "rejection");
    }

    public KafkaBookKeeperAdmissionException(
            KafkaBookKeeperAdmissionRejectionV1 rejection, String message, Throwable cause) {
        super(message, cause);
        this.rejection = Objects.requireNonNull(rejection, "rejection");
    }

    public KafkaBookKeeperAdmissionRejectionV1 rejection() {
        return rejection;
    }
}
