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

package com.nereusstream.kafka.bookkeeper.replication;

/** Closed physical source kinds carried by the K8 compact replica descriptor. */
public enum KafkaReplicaSourceKindV1 {
    BOOKKEEPER_RUN(1),
    OBJECT_WAL_GROUP(2);

    private final int wireCode;

    KafkaReplicaSourceKindV1(int wireCode) {
        this.wireCode = wireCode;
    }

    int wireCode() {
        return wireCode;
    }

    static KafkaReplicaSourceKindV1 fromWireCode(int wireCode) {
        for (KafkaReplicaSourceKindV1 value : values()) {
            if (value.wireCode == wireCode) {
                return value;
            }
        }
        throw new IllegalArgumentException("unknown replica source kind");
    }
}
