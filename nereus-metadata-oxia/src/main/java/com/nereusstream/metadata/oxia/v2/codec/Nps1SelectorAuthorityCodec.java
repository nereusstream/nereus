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

package com.nereusstream.metadata.oxia.v2.codec;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.PulsarAuthorityLeafCodecV1;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.PulsarBindingGeneration;
import com.nereusstream.domain.protocol.PulsarClassicNameV1;
import com.nereusstream.domain.protocol.PulsarPersistenceName;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorStateV1;
import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorValueV1;
import com.nereusstream.metadata.spi.model.VersionedSelectorSnapshot;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/** Strict canonical NPS1 selector-value codec. */
public final class Nps1SelectorAuthorityCodec implements SelectorAuthorityCodec {
    public static final int FIXED_BYTES = 84;
    public static final int MAX_BYTES = FIXED_BYTES + PulsarClassicNameV1.MAX_PERSISTENCE_NAME_BYTES;

    private static final byte[] MAGIC = "NPS1".getBytes(StandardCharsets.US_ASCII);
    private static final int SCHEMA_VERSION = 1;
    private static final String KEY_MARKER = "/selectors/v1/";

    @Override
    public boolean available() {
        return true;
    }

    /** Creates one value whose exact stored bytes and digest are derived from its semantic fields. */
    public static PulsarTopicGenerationSelectorValueV1 createValue(
            PulsarPersistenceName persistenceName,
            PulsarBindingGeneration generation,
            PulsarTopicGenerationSelectorStateV1 state,
            TopicBindingId aggregateBindingId,
            Sha256Digest aggregateCanonicalStoredDigest) {
        CanonicalBytes encoded =
                encodeFields(persistenceName, generation, state, aggregateBindingId, aggregateCanonicalStoredDigest);
        return new PulsarTopicGenerationSelectorValueV1(
                persistenceName,
                generation,
                state,
                aggregateBindingId,
                aggregateCanonicalStoredDigest,
                encoded,
                Sha256Digest.hash(encoded));
    }

    @Override
    public CanonicalBytes encode(PulsarTopicGenerationSelectorValueV1 candidate) {
        Objects.requireNonNull(candidate, "candidate");
        CanonicalBytes canonical = encodeFields(
                candidate.persistenceName(),
                candidate.generation(),
                candidate.state(),
                candidate.aggregateBindingId(),
                candidate.aggregateCanonicalStoredDigest());
        if (!canonical.equals(candidate.canonicalStoredBytes())
                || !Sha256Digest.hash(canonical).equals(candidate.canonicalStoredDigest())) {
            throw new IllegalArgumentException("selector candidate bytes/digest are not canonical NPS1");
        }
        return canonical;
    }

    @Override
    public VersionedSelectorSnapshot decode(
            String expectedAuthorityKey,
            PulsarPersistenceName expectedPersistenceName,
            CanonicalBytes storedBytes,
            MetadataVersion metadataVersion) {
        Objects.requireNonNull(expectedAuthorityKey, "expectedAuthorityKey");
        Objects.requireNonNull(expectedPersistenceName, "expectedPersistenceName");
        Objects.requireNonNull(storedBytes, "storedBytes");
        Objects.requireNonNull(metadataVersion, "metadataVersion");
        requireAuthorityKey(expectedAuthorityKey, expectedPersistenceName);

        byte[] encoded = storedBytes.toByteArray();
        if (encoded.length < FIXED_BYTES || encoded.length > MAX_BYTES) {
            throw new IllegalArgumentException("NPS1 input is outside its persisted-v1 length bounds");
        }
        ByteBuffer buffer = ByteBuffer.wrap(encoded);
        byte[] magic = new byte[MAGIC.length];
        buffer.get(magic);
        if (!Arrays.equals(MAGIC, magic)) {
            throw new IllegalArgumentException("wrong NPS1 magic");
        }
        int schema = Short.toUnsignedInt(buffer.getShort());
        if (schema != SCHEMA_VERSION) {
            throw new IllegalArgumentException("unknown NPS1 schema version: " + schema);
        }
        PulsarTopicGenerationSelectorStateV1 state = stateFromCode(Short.toUnsignedInt(buffer.getShort()));
        PulsarBindingGeneration generation = new PulsarBindingGeneration(buffer.getLong());
        long nameLength = Integer.toUnsignedLong(buffer.getInt());
        if (nameLength == 0 || nameLength > PulsarClassicNameV1.MAX_PERSISTENCE_NAME_BYTES) {
            throw new IllegalArgumentException("NPS1 persistence-name length is outside its v1 cap");
        }
        int expectedRemaining = Math.addExact(Math.toIntExact(nameLength), 2 * Sha256Digest.LENGTH);
        if (buffer.remaining() != expectedRemaining) {
            throw new IllegalArgumentException("NPS1 declared length does not match its exact input");
        }
        byte[] nameBytes = new byte[Math.toIntExact(nameLength)];
        buffer.get(nameBytes);
        PulsarPersistenceName persistenceName = PulsarPersistenceName.fromBytes(nameBytes);
        requirePersistenceName(persistenceName);
        if (!expectedPersistenceName.equals(persistenceName)) {
            throw new IllegalArgumentException("NPS1 persistence name does not match the authority-key input");
        }
        TopicBindingId bindingId = new TopicBindingId(Sha256Digest.copyOf(readDigest(buffer, "aggregate binding ID")));
        Sha256Digest aggregateDigest = Sha256Digest.copyOf(readDigest(buffer, "aggregate canonical stored digest"));
        if (buffer.hasRemaining()) {
            throw new IllegalArgumentException("NPS1 has trailing bytes");
        }

        PulsarTopicGenerationSelectorValueV1 value =
                createValue(persistenceName, generation, state, bindingId, aggregateDigest);
        if (!value.canonicalStoredBytes().equals(storedBytes)) {
            throw new IllegalArgumentException("NPS1 input is not canonical");
        }
        return new VersionedSelectorSnapshot(value, metadataVersion);
    }

    private static CanonicalBytes encodeFields(
            PulsarPersistenceName persistenceName,
            PulsarBindingGeneration generation,
            PulsarTopicGenerationSelectorStateV1 state,
            TopicBindingId aggregateBindingId,
            Sha256Digest aggregateCanonicalStoredDigest) {
        Objects.requireNonNull(persistenceName, "persistenceName");
        Objects.requireNonNull(generation, "generation");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(aggregateBindingId, "aggregateBindingId");
        Objects.requireNonNull(aggregateCanonicalStoredDigest, "aggregateCanonicalStoredDigest");
        requirePersistenceName(persistenceName);
        byte[] name = persistenceName.value().bytes().toByteArray();
        int size = Math.addExact(FIXED_BYTES, name.length);
        if (size > MAX_BYTES) {
            throw new IllegalArgumentException("selector exceeds the NPS1 v1 parser cap");
        }
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.put(MAGIC);
        buffer.putShort((short) SCHEMA_VERSION);
        buffer.putShort((short) stateCode(state));
        buffer.putLong(generation.value());
        buffer.putInt(name.length);
        buffer.put(name);
        buffer.put(aggregateBindingId.digest().bytes().toByteArray());
        buffer.put(aggregateCanonicalStoredDigest.bytes().toByteArray());
        if (buffer.hasRemaining()) {
            throw new IllegalStateException("NPS1 encoder did not fill its exact checked allocation");
        }
        return CanonicalBytes.copyOf(buffer.array());
    }

    private static byte[] readDigest(ByteBuffer buffer, String field) {
        if (buffer.remaining() < Sha256Digest.LENGTH) {
            throw new IllegalArgumentException("truncated NPS1 " + field);
        }
        byte[] value = new byte[Sha256Digest.LENGTH];
        buffer.get(value);
        return value;
    }

    private static int stateCode(PulsarTopicGenerationSelectorStateV1 state) {
        return switch (state) {
            case RESERVED -> 1;
            case ACTIVE -> 2;
            case DELETING -> 3;
            case DELETED -> 4;
        };
    }

    private static PulsarTopicGenerationSelectorStateV1 stateFromCode(int code) {
        return switch (code) {
            case 1 -> PulsarTopicGenerationSelectorStateV1.RESERVED;
            case 2 -> PulsarTopicGenerationSelectorStateV1.ACTIVE;
            case 3 -> PulsarTopicGenerationSelectorStateV1.DELETING;
            case 4 -> PulsarTopicGenerationSelectorStateV1.DELETED;
            default -> throw new IllegalArgumentException("unknown NPS1 selector state: " + code);
        };
    }

    private static void requireAuthorityKey(String key, PulsarPersistenceName name) {
        String suffix = KEY_MARKER + PulsarAuthorityLeafCodecV1.selectorLeaf(name);
        if (!key.startsWith("/") || !key.endsWith(suffix)) {
            throw new IllegalArgumentException("selector authority key does not match its persistence name");
        }
    }

    private static void requirePersistenceName(PulsarPersistenceName persistenceName) {
        byte[] bytes = persistenceName.value().bytes().toByteArray();
        if (bytes.length == 0 || bytes.length > PulsarClassicNameV1.MAX_PERSISTENCE_NAME_BYTES) {
            throw new IllegalArgumentException("Pulsar persistence name is outside the NPS1 v1 cap");
        }
        String value = persistenceName.value().value();
        String[] parts = value.split("/", -1);
        if (parts.length != 4
                || parts[0].isEmpty()
                || parts[1].isEmpty()
                || !"persistent".equals(parts[2])
                || parts[3].isEmpty()) {
            throw new IllegalArgumentException("Pulsar persistence name is not canonical classic-persistent form");
        }
        final String localName;
        try {
            localName = URLDecoder.decode(parts[3], StandardCharsets.UTF_8);
        } catch (IllegalArgumentException malformed) {
            throw new IllegalArgumentException("Pulsar persistence local name has invalid URL encoding", malformed);
        }
        if (localName.isBlank()
                || !URLEncoder.encode(localName, StandardCharsets.UTF_8).equals(parts[3])) {
            throw new IllegalArgumentException("Pulsar persistence local name is not canonically URL encoded");
        }
    }
}
