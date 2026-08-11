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

package com.nereusstream.domain.identity;

import java.util.Objects;

/** A raw Kafka topic UUID excluding the two reserved UUID values. */
public record KafkaTopicId(Id128 value) {
    public KafkaTopicId {
        Objects.requireNonNull(value, "value");
        if (value.equals(Id128.zero()) || value.equals(Id128.one())) {
            throw new IllegalArgumentException("Kafka topic ID must not use a reserved UUID");
        }
    }
}
