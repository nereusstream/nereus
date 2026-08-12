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

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** JDK-only canonical classic-persistent Pulsar name validation for NTA1 v1. */
public final class PulsarClassicNameV1 {
    public static final int MAX_PERSISTENCE_NAME_BYTES = 4096;
    public static final int MAX_TOPIC_NAME_BYTES = 4096;

    private static final String TOPIC_PREFIX = "persistent://";
    private static final String PERSISTENCE_DOMAIN = "persistent";

    private PulsarClassicNameV1() {}

    public static void validate(PulsarTopicIncarnationIdentity incarnation) {
        Objects.requireNonNull(incarnation, "incarnation");
        validate(incarnation.persistenceName(), incarnation.topicName());
    }

    public static void validate(PulsarPersistenceName persistenceName, PulsarTopicName topicName) {
        Objects.requireNonNull(persistenceName, "persistenceName");
        Objects.requireNonNull(topicName, "topicName");
        int persistenceBytes = persistenceName.value().bytes().length();
        int topicBytes = topicName.value().bytes().length();
        if (persistenceBytes > MAX_PERSISTENCE_NAME_BYTES) {
            throw new IllegalArgumentException("Pulsar canonical persistence name exceeds 4096 UTF-8 bytes");
        }
        if (topicBytes > MAX_TOPIC_NAME_BYTES) {
            throw new IllegalArgumentException("Pulsar canonical topic name exceeds 4096 UTF-8 bytes");
        }

        Components topic = parseTopic(topicName.value().value());
        Components persistence = parsePersistence(persistenceName.value().value());
        if (!topic.tenant().equals(persistence.tenant())
                || !topic.namespace().equals(persistence.namespace())
                || !topic.localName().equals(persistence.localName())) {
            throw new IllegalArgumentException("Pulsar canonical persistence and topic names do not agree");
        }

        String expectedPersistence = topic.tenant()
                + "/"
                + topic.namespace()
                + "/"
                + PERSISTENCE_DOMAIN
                + "/"
                + URLEncoder.encode(topic.localName(), StandardCharsets.UTF_8);
        String expectedTopic =
                TOPIC_PREFIX + persistence.tenant() + "/" + persistence.namespace() + "/" + persistence.localName();
        if (!persistenceName.value().value().equals(expectedPersistence)
                || !topicName.value().value().equals(expectedTopic)) {
            throw new IllegalArgumentException("Pulsar names are not the exact canonical classic round trip");
        }
    }

    private static Components parseTopic(String topic) {
        if (!topic.startsWith(TOPIC_PREFIX)) {
            throw new IllegalArgumentException("NTA1 v1 accepts only classic persistent:// Pulsar topics");
        }
        String rest = topic.substring(TOPIC_PREFIX.length());
        int tenantEnd = rest.indexOf('/');
        int namespaceEnd = tenantEnd < 0 ? -1 : rest.indexOf('/', tenantEnd + 1);
        if (tenantEnd <= 0
                || namespaceEnd <= tenantEnd + 1
                || namespaceEnd == rest.length() - 1
                || rest.indexOf('/', namespaceEnd + 1) >= 0) {
            throw new IllegalArgumentException("Pulsar topic name is not canonical tenant/namespace/local-name");
        }
        String tenant = rest.substring(0, tenantEnd);
        String namespace = rest.substring(tenantEnd + 1, namespaceEnd);
        String localName = rest.substring(namespaceEnd + 1);
        validateNamedEntity(tenant, "tenant");
        validateNamedEntity(namespace, "namespace");
        if (localName.isBlank()) {
            throw new IllegalArgumentException("Pulsar local topic name must not be blank");
        }
        return new Components(tenant, namespace, localName);
    }

    private static Components parsePersistence(String persistence) {
        String[] parts = persistence.split("/", -1);
        if (parts.length != 4
                || parts[0].isEmpty()
                || parts[1].isEmpty()
                || !PERSISTENCE_DOMAIN.equals(parts[2])
                || parts[3].isEmpty()) {
            throw new IllegalArgumentException("Pulsar persistence name is not canonical classic-persistent form");
        }
        validateNamedEntity(parts[0], "tenant");
        validateNamedEntity(parts[1], "namespace");
        final String localName;
        try {
            localName = URLDecoder.decode(parts[3], StandardCharsets.UTF_8);
        } catch (IllegalArgumentException invalidEncoding) {
            throw new IllegalArgumentException(
                    "Pulsar persistence local name has invalid URL encoding", invalidEncoding);
        }
        if (localName.isBlank()
                || !URLEncoder.encode(localName, StandardCharsets.UTF_8).equals(parts[3])) {
            throw new IllegalArgumentException("Pulsar persistence local name is not canonically URL encoded");
        }
        return new Components(parts[0], parts[1], localName);
    }

    private static void validateNamedEntity(String value, String field) {
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            boolean accepted = character >= 'a' && character <= 'z'
                    || character >= 'A' && character <= 'Z'
                    || character >= '0' && character <= '9'
                    || character == '_'
                    || character == '-'
                    || character == '='
                    || character == ':'
                    || character == '.'
                    || character == '%';
            if (!accepted) {
                throw new IllegalArgumentException("Pulsar " + field + " is not a canonical named entity");
            }
        }
    }

    private record Components(String tenant, String namespace, String localName) {}
}
