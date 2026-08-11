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

import com.nereusstream.domain.identity.KafkaTopicId;
import java.util.Objects;

/** A Kafka incarnation identified by raw topic UUID and canonical name. */
public record KafkaTopicIncarnationIdentity(KafkaTopicId topicId, KafkaTopicName topicName)
        implements TopicIncarnationIdentity {
    public KafkaTopicIncarnationIdentity {
        Objects.requireNonNull(topicId, "topicId");
        Objects.requireNonNull(topicName, "topicName");
    }

    @Override
    public ProtocolKindV1 protocolKind() {
        return ProtocolKindV1.KAFKA;
    }
}
