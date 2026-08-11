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

package com.nereusstream.domain.protocol;

import com.nereusstream.domain.bytes.CanonicalBytes;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** A canonical Kafka topic name using the pinned 249-byte ASCII rule. */
public record KafkaTopicName(String value) {
    public static final int MAX_LENGTH = 249;

    public KafkaTopicName {
        Objects.requireNonNull(value, "value");
        if (value.isEmpty() || value.equals(".") || value.equals("..")) {
            throw new IllegalArgumentException("Kafka topic name is empty or reserved");
        }
        if (value.length() > MAX_LENGTH) {
            throw new IllegalArgumentException("Kafka topic name exceeds 249 ASCII bytes");
        }
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            boolean accepted = character >= 'a' && character <= 'z'
                    || character >= 'A' && character <= 'Z'
                    || character >= '0' && character <= '9'
                    || character == '.'
                    || character == '_'
                    || character == '-';
            if (!accepted) {
                throw new IllegalArgumentException("Kafka topic name contains a non-canonical character");
            }
        }
    }

    public static KafkaTopicName fromBytes(byte[] value) {
        Objects.requireNonNull(value, "value");
        for (byte character : value) {
            if ((character & 0x80) != 0) {
                throw new IllegalArgumentException("Kafka topic name is not ASCII");
            }
        }
        return new KafkaTopicName(new String(value, StandardCharsets.US_ASCII));
    }

    public CanonicalBytes bytes() {
        return CanonicalBytes.copyOf(value.getBytes(StandardCharsets.US_ASCII));
    }
}
