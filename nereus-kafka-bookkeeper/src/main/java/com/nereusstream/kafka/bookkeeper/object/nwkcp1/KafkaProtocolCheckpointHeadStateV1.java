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

package com.nereusstream.kafka.bookkeeper.object.nwkcp1;

/** Irreversible lifecycle of one Root-bound Kafka protocol checkpoint Head. */
public enum KafkaProtocolCheckpointHeadStateV1 {
    OPEN(0),
    TERMINAL(1);

    private final int wireId;

    KafkaProtocolCheckpointHeadStateV1(int wireId) {
        this.wireId = wireId;
    }

    public int wireId() {
        return wireId;
    }

    public static KafkaProtocolCheckpointHeadStateV1 fromWire(int value) {
        return switch (value) {
            case 0 -> OPEN;
            case 1 -> TERMINAL;
            default -> throw new IllegalArgumentException("unknown Kafka protocol checkpoint Head state");
        };
    }
}
