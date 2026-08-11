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

import com.nereusstream.domain.bytes.CanonicalUtf8;
import java.util.Objects;

/** A non-empty strict UTF-8 Pulsar topic name with no guessed format cap. */
public record PulsarTopicName(CanonicalUtf8 value) {
    public PulsarTopicName {
        Objects.requireNonNull(value, "value");
        if (value.bytes().isEmpty()) {
            throw new IllegalArgumentException("Pulsar topic name must be non-empty");
        }
    }

    public static PulsarTopicName fromString(String value) {
        return new PulsarTopicName(CanonicalUtf8.fromString(value));
    }

    public static PulsarTopicName fromBytes(byte[] value) {
        return new PulsarTopicName(CanonicalUtf8.fromBytes(value));
    }
}
