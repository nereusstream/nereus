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

package com.nereusstream.domain.nta1;

import static com.nereusstream.domain.DomainTestFixtures.kafkaCell;
import static com.nereusstream.domain.DomainTestFixtures.pulsarCell;
import com.nereusstream.domain.aggregate.FrameEncodingPolicyValueV1;
import com.nereusstream.domain.aggregate.InitialStorageEpochV1;
import com.nereusstream.domain.aggregate.PolicyCatalogDigest;
import com.nereusstream.domain.aggregate.ProfileOriginV1;
import com.nereusstream.domain.aggregate.StorageProfileV1;
import com.nereusstream.domain.aggregate.TopicBindingAggregateFoundationValidatorV1;
import com.nereusstream.domain.aggregate.TopicBindingAggregateV1;
import com.nereusstream.domain.aggregate.TopicBindingV1;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.DeterministicTopicIdsV1;
import com.nereusstream.domain.codec.ProtocolCellIdentityCodecV1;
import com.nereusstream.domain.codec.TopicIncarnationIdentityCodecV1;
import com.nereusstream.domain.identity.KafkaTopicId;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.KafkaTopicName;
import com.nereusstream.domain.protocol.ProtocolCellIdentity;
import com.nereusstream.domain.protocol.ProtocolKindV1;
import com.nereusstream.domain.protocol.PulsarBindingGeneration;
import com.nereusstream.domain.protocol.PulsarPersistenceName;
import com.nereusstream.domain.protocol.PulsarTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.PulsarTopicName;
import com.nereusstream.domain.protocol.TopicIncarnationIdentity;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;

/** Test-scope-only candidate wire used to collect M1.1b-Q1 readiness evidence. */
final class Nta1ReadinessHarness {
    static final int FIXED_NTA1_BYTES = 129;
    static final int KAFKA_CELL_BYTES = 38;
    static final int PULSAR_CELL_BYTES = 54;
    static final int MAX_CELL_BYTES = PULSAR_CELL_BYTES;
    static final int KAFKA_INCARNATION_FIXED_BYTES = 26;
    static final int PULSAR_INCARNATION_FIXED_BYTES = 22;
    static final int MAX_KAFKA_INCARNATION_BYTES = KAFKA_INCARNATION_FIXED_BYTES + KafkaTopicName.MAX_LENGTH;
    static final Caps PERFORMANCE_CAPS = new Caps("performance-biased", 4096, 4096);
    static final Caps COMPATIBILITY_CAPS = new Caps("compatibility-biased", 16384, 16384);

    private static final byte[] MAGIC = "NTA1".getBytes(StandardCharsets.US_ASCII);
    private static final PolicyCatalogDigest CATALOG = new PolicyCatalogDigest(
            Sha256Digest.hash(CanonicalBytes.copyOf("nta1-q1-candidate-catalog".getBytes(StandardCharsets.US_ASCII))));
    private static final byte[] KAFKA_TOPIC_ID = HexFormat.of().parseHex("404142434445464748494a4b4c4d4e4f");

    private Nta1ReadinessHarness() {}

    enum PolicyCandidate {
        NONE(0, 0, "NONE"),
        ZSTD_FAST_IF_SMALLER_V1(1, 1, "ZSTD_FAST_IF_SMALLER_V1"),
        ZSTD_FAST_IF_SAVES_12_5_PERCENT_V1(2, 1, "ZSTD_FAST_IF_SAVES_12_5_PERCENT_V1");

        private final int kind;
        private final int formatVersion;
        private final String label;

        PolicyCandidate(int kind, int formatVersion, String label) {
            this.kind = kind;
            this.formatVersion = formatVersion;
            this.label = label;
        }

        FrameEncodingPolicyValueV1 value() {
            return new FrameEncodingPolicyValueV1(kind, formatVersion, CanonicalBytes.empty());
        }

        int kind() {
            return kind;
        }

        int formatVersion() {
            return formatVersion;
        }

        String label() {
            return label;
        }

        static PolicyCandidate from(int kind, int formatVersion) {
            return Arrays.stream(values())
                    .filter(candidate -> candidate.kind == kind && candidate.formatVersion == formatVersion)
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("unknown candidate frame policy"));
        }
    }

    record Caps(String label, int maxPersistenceNameBytes, int maxTopicNameBytes) {
        Caps {
            Objects.requireNonNull(label, "label");
            if (maxPersistenceNameBytes <= 0 || maxTopicNameBytes <= 0) {
                throw new IllegalArgumentException("name caps must be positive");
            }
        }

        int maxPulsarIncarnationBytes() {
            return checkedSize(PULSAR_INCARNATION_FIXED_BYTES, maxPersistenceNameBytes, maxTopicNameBytes);
        }

        int maxIncarnationBytes() {
            return Math.max(MAX_KAFKA_INCARNATION_BYTES, maxPulsarIncarnationBytes());
        }

        int maxNta1Bytes() {
            return checkedSize(FIXED_NTA1_BYTES, MAX_CELL_BYTES, maxIncarnationBytes());
        }
    }

    record ParseResult(
            TopicBindingAggregateV1 aggregate, int largestLengthFramedAllocation, int totalLengthFramedAllocation) {}

    static TopicBindingAggregateV1 kafkaAggregate(String topicName, StorageProfileV1 profile, PolicyCandidate policy) {
        var incarnation = new KafkaTopicIncarnationIdentity(
                new KafkaTopicId(com.nereusstream.domain.identity.Id128.fromBytes(KAFKA_TOPIC_ID)),
                new KafkaTopicName(topicName));
        return aggregate(ProtocolKindV1.KAFKA, kafkaCell(), incarnation, profile, policy);
    }

    static TopicBindingAggregateV1 pulsarAggregate(
            String persistenceName, String topicName, StorageProfileV1 profile, PolicyCandidate policy) {
        var incarnation = new PulsarTopicIncarnationIdentity(
                PulsarPersistenceName.fromString(persistenceName),
                PulsarTopicName.fromString(topicName),
                new PulsarBindingGeneration(42));
        return aggregate(ProtocolKindV1.PULSAR, pulsarCell(), incarnation, profile, policy);
    }

    static TopicBindingAggregateV1 maxClassicPulsarAggregate(
            Caps caps, StorageProfileV1 profile, PolicyCandidate policy) {
        String prefix = "persistent://t/n/";
        int localBytes = caps.maxTopicNameBytes() - prefix.length();
        if (localBytes <= 0) {
            throw new IllegalArgumentException("topic-name cap is too small for the classic canonical prefix");
        }
        String local = "a".repeat(localBytes);
        return pulsarAggregate("t/n/persistent/" + local, prefix + local, profile, policy);
    }

    static byte[] encode(TopicBindingAggregateV1 aggregate, Caps caps) {
        Objects.requireNonNull(aggregate, "aggregate");
        Objects.requireNonNull(caps, "caps");
        requireCandidateLegality(aggregate, caps);

        byte[] cell = ProtocolCellIdentityCodecV1.encode(aggregate.binding().cellIdentity())
                .toByteArray();
        byte[] incarnation = TopicIncarnationIdentityCodecV1.encode(
                        aggregate.binding().incarnationIdentity())
                .toByteArray();
        if (cell.length > MAX_CELL_BYTES || incarnation.length > caps.maxIncarnationBytes()) {
            throw new IllegalArgumentException("candidate sub-encoding exceeds its parser cap");
        }
        int size = checkedSize(FIXED_NTA1_BYTES, cell.length, incarnation.length);
        if (size > caps.maxNta1Bytes()) {
            throw new IllegalArgumentException("candidate NTA1 exceeds its parser cap");
        }

        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.put(MAGIC);
        putU16(buffer, aggregate.aggregateSchemaVersion());
        putU16(buffer, aggregate.binding().protocolKind().code());
        buffer.put(aggregate.binding().bindingId().digest().bytes().toByteArray());
        putLength(buffer, cell.length);
        buffer.put(cell);
        putLength(buffer, incarnation.length);
        buffer.put(incarnation);
        buffer.put(aggregate.initialEpoch().storageEpochId().digest().bytes().toByteArray());
        buffer.putLong(aggregate.initialEpoch().epochOrdinal());
        putU16(buffer, aggregate.initialEpoch().storageProfile().code());
        putU16(buffer, aggregate.initialEpoch().profileOrigin().code());
        buffer.put(
                aggregate.initialEpoch().policyCatalogDigest().digest().bytes().toByteArray());
        putU16(buffer, aggregate.initialEpoch().frameEncodingPolicy().kind());
        putU16(buffer, aggregate.initialEpoch().frameEncodingPolicy().formatVersion());
        buffer.put((byte) 0);
        if (buffer.hasRemaining()) {
            throw new IllegalStateException("candidate encoder did not fill its checked allocation");
        }
        return buffer.array();
    }

    static ParseResult decode(byte[] encoded, Caps caps) {
        Objects.requireNonNull(encoded, "encoded");
        Objects.requireNonNull(caps, "caps");
        if (encoded.length > caps.maxNta1Bytes()) {
            throw new IllegalArgumentException("candidate NTA1 exceeds the persisted-v1 parser cap");
        }
        BoundedReader reader = new BoundedReader(encoded);
        reader.requireMagic(MAGIC);
        int schema = reader.readU16();
        ProtocolKindV1 protocol = ProtocolKindV1.fromCode(reader.readU16());
        TopicBindingId bindingId = new TopicBindingId(Sha256Digest.copyOf(reader.readFixed(Sha256Digest.LENGTH)));
        byte[] cellBytes = reader.readLengthFramed("cell", MAX_CELL_BYTES);
        byte[] incarnationBytes = reader.readLengthFramed("incarnation", caps.maxIncarnationBytes());
        StorageEpochId epochId = new StorageEpochId(Sha256Digest.copyOf(reader.readFixed(Sha256Digest.LENGTH)));
        long ordinal = reader.readLong();
        StorageProfileV1 profile = StorageProfileV1.fromCode(reader.readU16());
        ProfileOriginV1 origin = ProfileOriginV1.fromCode(reader.readU16());
        PolicyCatalogDigest catalog =
                new PolicyCatalogDigest(Sha256Digest.copyOf(reader.readFixed(Sha256Digest.LENGTH)));
        PolicyCandidate policy = PolicyCandidate.from(reader.readU16(), reader.readU16());
        if (reader.readUnsignedByte() != 0) {
            throw new IllegalArgumentException("candidate sealed-end presence must be zero");
        }
        reader.requireEof();

        ProtocolCellIdentity cell = ProtocolCellIdentityCodecV1.decode(cellBytes);
        TopicIncarnationIdentity incarnation = TopicIncarnationIdentityCodecV1.decode(incarnationBytes);
        TopicBindingAggregateV1 aggregate = new TopicBindingAggregateV1(
                schema,
                new TopicBindingV1(protocol, bindingId, cell, incarnation),
                new InitialStorageEpochV1(epochId, ordinal, profile, origin, catalog, policy.value()));
        requireCandidateLegality(aggregate, caps);
        return new ParseResult(aggregate, reader.largestLengthFramedAllocation, reader.totalLengthFramedAllocation);
    }

    static void requireCandidateLegality(TopicBindingAggregateV1 aggregate, Caps caps) {
        TopicBindingAggregateFoundationValidatorV1.validate(aggregate);
        FrameEncodingPolicyValueV1 value = aggregate.initialEpoch().frameEncodingPolicy();
        PolicyCandidate.from(value.kind(), value.formatVersion());
        if (!value.payload().isEmpty()) {
            throw new IllegalArgumentException("candidate v1 frame-policy payload must be empty");
        }
        if (aggregate.binding().incarnationIdentity() instanceof PulsarTopicIncarnationIdentity pulsar) {
            int persistenceBytes = pulsar.persistenceName().value().bytes().length();
            int topicBytes = pulsar.topicName().value().bytes().length();
            if (persistenceBytes > caps.maxPersistenceNameBytes() || topicBytes > caps.maxTopicNameBytes()) {
                throw new IllegalArgumentException("candidate Pulsar name exceeds its UTF-8 cap");
            }
            requireClassicPulsarNameConsistency(pulsar);
        }
    }

    static void requireClassicPulsarNameConsistency(PulsarTopicIncarnationIdentity incarnation) {
        String topic = incarnation.topicName().value().value();
        String persistence = incarnation.persistenceName().value().value();
        String prefix = "persistent://";
        if (!topic.startsWith(prefix)) {
            throw new IllegalArgumentException("Q1 candidate admits only classic persistent Pulsar topics");
        }
        String[] parts = topic.substring(prefix.length()).split("/", 3);
        if (parts.length != 3 || parts[0].isEmpty() || parts[1].isEmpty() || parts[2].isEmpty()) {
            throw new IllegalArgumentException("candidate Pulsar topic name is not canonical");
        }
        String expectedPersistence =
                parts[0] + "/" + parts[1] + "/persistent/" + URLEncoder.encode(parts[2], StandardCharsets.UTF_8);
        if (!persistence.equals(expectedPersistence)) {
            throw new IllegalArgumentException("candidate Pulsar persistence/topic names do not agree");
        }
        String[] persistenceParts = persistence.split("/", 4);
        String roundTripTopic = "persistent://"
                + persistenceParts[0]
                + "/"
                + persistenceParts[1]
                + "/"
                + URLDecoder.decode(persistenceParts[3], StandardCharsets.UTF_8);
        if (!topic.equals(roundTripTopic)) {
            throw new IllegalArgumentException("candidate Pulsar persistence-name round trip is not exact");
        }
    }

    static int checkedSize(int... fields) {
        int result = 0;
        for (int field : fields) {
            result = Math.addExact(result, field);
        }
        return result;
    }

    static String sha256(byte[] value) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(value)).toHex();
    }

    static List<Integer> measuredFourKibVectorSizes() {
        return List.of(
                encode(
                                kafkaAggregate("a", StorageProfileV1.BOOKKEEPER_WAL_ONLY, PolicyCandidate.NONE),
                                PERFORMANCE_CAPS)
                        .length,
                encode(
                                kafkaAggregate(
                                        "orders.v1",
                                        StorageProfileV1.OBJECT_WAL,
                                        PolicyCandidate.ZSTD_FAST_IF_SMALLER_V1),
                                PERFORMANCE_CAPS)
                        .length,
                encode(
                                kafkaAggregate(
                                        "k".repeat(KafkaTopicName.MAX_LENGTH),
                                        StorageProfileV1.OBJECT_WAL,
                                        PolicyCandidate.ZSTD_FAST_IF_SMALLER_V1),
                                PERFORMANCE_CAPS)
                        .length,
                encode(
                                pulsarAggregate(
                                        "t/n/persistent/a",
                                        "persistent://t/n/a",
                                        StorageProfileV1.BOOKKEEPER_WAL_ONLY,
                                        PolicyCandidate.NONE),
                                PERFORMANCE_CAPS)
                        .length,
                encode(
                                pulsarAggregate(
                                        "tenant/ns/persistent/orders",
                                        "persistent://tenant/ns/orders",
                                        StorageProfileV1.OBJECT_WAL,
                                        PolicyCandidate.ZSTD_FAST_IF_SMALLER_V1),
                                PERFORMANCE_CAPS)
                        .length,
                encode(
                                maxClassicPulsarAggregate(
                                        PERFORMANCE_CAPS,
                                        StorageProfileV1.OBJECT_WAL,
                                        PolicyCandidate.ZSTD_FAST_IF_SMALLER_V1),
                                PERFORMANCE_CAPS)
                        .length);
    }

    static String renderEvidenceJson() {
        byte[] kafkaTypical = encode(
                kafkaAggregate("orders.v1", StorageProfileV1.OBJECT_WAL, PolicyCandidate.ZSTD_FAST_IF_SMALLER_V1),
                PERFORMANCE_CAPS);
        byte[] pulsarTypical = encode(
                pulsarAggregate(
                        "tenant/ns/persistent/orders",
                        "persistent://tenant/ns/orders",
                        StorageProfileV1.OBJECT_WAL,
                        PolicyCandidate.ZSTD_FAST_IF_SMALLER_V1),
                PERFORMANCE_CAPS);
        byte[] fourKibMax = encode(
                maxClassicPulsarAggregate(
                        PERFORMANCE_CAPS, StorageProfileV1.OBJECT_WAL, PolicyCandidate.ZSTD_FAST_IF_SMALLER_V1),
                PERFORMANCE_CAPS);
        byte[] sixteenKibMax = encode(
                maxClassicPulsarAggregate(
                        COMPATIBILITY_CAPS, StorageProfileV1.OBJECT_WAL, PolicyCandidate.ZSTD_FAST_IF_SMALLER_V1),
                COMPATIBILITY_CAPS);
        List<Integer> measured = measuredFourKibVectorSizes();
        return """
                {
                  "schemaVersion": 1,
                  "sourceTupleId": "v2-m0",
                  "result": "READINESS_EVIDENCE_ONLY",
                  "promotionEligible": false,
                  "designStatus": "Proposed",
                  "productionCodecImplemented": false,
                  "runtimeActivated": false,
                  "scenarioPromotion": false,
                  "pinnedSources": {
                    "kafka": "76f62f3b83e882105219b6c7687dbde594a8b8a2",
                    "pulsar": "11d7ab15291ca4bbc9cc29dedd7878c4e1311ec9"
                  },
                  "candidatePolicies": [
                    {"name":"NONE","kind":0,"formatVersion":0,"payloadBytes":0,"status":"STRUCTURALLY_FROZEN"},
                    {
                      "name":"ZSTD_FAST_IF_SMALLER_V1",
                      "kind":1,
                      "formatVersion":1,
                      "payloadBytes":0,
                      "status":"RECOMMENDED_FOR_GRILL"
                    },
                    {
                      "name":"ZSTD_FAST_IF_SAVES_12_5_PERCENT_V1",
                      "kind":2,
                      "formatVersion":1,
                      "payloadBytes":0,
                      "status":"ALTERNATIVE_FOR_GRILL"
                    }
                  ],
                  "fixedBytes": {
                    "nta1ExcludingCellAndIncarnation": 129,
                    "kafkaCell": 38,
                    "pulsarCell": 54,
                    "maxCellBytes": 54,
                    "kafkaIncarnationFixed": 26,
                    "pulsarIncarnationFixed": 22,
                    "kafkaMaxIncarnationBytes": 275
                  },
                  "capCandidates": [
                    {
                      "name":"performance-biased",
                      "perNameBytes":4096,
                      "maxIncarnationBytes":8214,
                      "maxNta1Bytes":8397,
                      "maxLegalClassicPersistentVectorBytes":%d,
                      "raw100kAllMaxLegalBytes":%d
                    },
                    {
                      "name":"compatibility-biased",
                      "perNameBytes":16384,
                      "maxIncarnationBytes":32790,
                      "maxNta1Bytes":32973,
                      "maxLegalClassicPersistentVectorBytes":%d,
                      "raw100kAllMaxLegalBytes":%d
                    },
                    {
                      "name":"earlier-total-proposal",
                      "perNameBytes":16384,
                      "maxNta1Bytes":65536,
                      "status":"NOT_RECOMMENDED_OVERPROVISIONED"
                    }
                  ],
                  "measuredFourKibVectorBytes": %s,
                  "typicalVectors": {
                    "kafka":{"bytes":%d,"sha256":"%s","raw100kBytes":%d},
                    "pulsar":{"bytes":%d,"sha256":"%s","raw100kBytes":%d}
                  },
                  "maximumVectorDigests": {
                    "fourKib":{"bytes":%d,"sha256":"%s"},
                    "sixteenKib":{"bytes":%d,"sha256":"%s"}
                  },
                  "coverage": {
                    "legalProtocolProfileRows": 6,
                    "policyAlternativesMeasured": 2,
                    "strictUtf8": true,
                    "checkedArithmetic": true,
                    "allocationAfterLengthValidationOnly": true,
                    "pulsarNameConsistency": "CLASSIC_PERSISTENT_PINNED_ROUND_TRIP_CANDIDATE"
                  },
                  "limitations": [
                    "SYNTHETIC_BOUNDARY_DISTRIBUTION_NOT_PRODUCTION_HISTOGRAM",
                    "SCALABLE_PULSAR_TOPIC_DOMAINS_REQUIRE_GRILL_DECISION",
                    "NO_NWG1_COMPRESSION_BENCHMARK"
                  ]
                }
                """.formatted(
                        fourKibMax.length,
                        Math.multiplyExact((long) fourKibMax.length, 100_000L),
                        sixteenKibMax.length,
                        Math.multiplyExact((long) sixteenKibMax.length, 100_000L),
                        measured,
                        kafkaTypical.length,
                        sha256(kafkaTypical),
                        Math.multiplyExact((long) kafkaTypical.length, 100_000L),
                        pulsarTypical.length,
                        sha256(pulsarTypical),
                        Math.multiplyExact((long) pulsarTypical.length, 100_000L),
                        fourKibMax.length,
                        sha256(fourKibMax),
                        sixteenKibMax.length,
                        sha256(sixteenKibMax));
    }

    private static TopicBindingAggregateV1 aggregate(
            ProtocolKindV1 protocol,
            ProtocolCellIdentity cell,
            TopicIncarnationIdentity incarnation,
            StorageProfileV1 profile,
            PolicyCandidate policy) {
        TopicBindingId bindingId = DeterministicTopicIdsV1.deriveBindingId(cell, incarnation);
        StorageEpochId epochId = DeterministicTopicIdsV1.deriveStorageEpochId(bindingId, 0);
        return new TopicBindingAggregateV1(
                1,
                new TopicBindingV1(protocol, bindingId, cell, incarnation),
                new InitialStorageEpochV1(
                        epochId, 0, profile, ProfileOriginV1.TOPIC_EXPLICIT, CATALOG, policy.value()));
    }

    private static void putU16(ByteBuffer buffer, int value) {
        if (value < 0 || value > 0xffff) {
            throw new IllegalArgumentException("candidate u16 value is out of range");
        }
        buffer.putShort((short) value);
    }

    private static void putLength(ByteBuffer buffer, int value) {
        if (value < 0) {
            throw new IllegalArgumentException("candidate length is negative");
        }
        buffer.putInt(value);
    }

    private static final class BoundedReader {
        private final ByteBuffer buffer;
        private int largestLengthFramedAllocation;
        private int totalLengthFramedAllocation;

        private BoundedReader(byte[] encoded) {
            this.buffer = ByteBuffer.wrap(encoded);
        }

        private void requireMagic(byte[] expected) {
            if (buffer.remaining() < expected.length) {
                throw new IllegalArgumentException("truncated candidate magic");
            }
            for (byte value : expected) {
                if (buffer.get() != value) {
                    throw new IllegalArgumentException("wrong candidate magic");
                }
            }
        }

        private int readU16() {
            requireRemaining(Short.BYTES);
            return Short.toUnsignedInt(buffer.getShort());
        }

        private long readLong() {
            requireRemaining(Long.BYTES);
            return buffer.getLong();
        }

        private int readUnsignedByte() {
            requireRemaining(1);
            return Byte.toUnsignedInt(buffer.get());
        }

        private byte[] readFixed(int length) {
            requireRemaining(length);
            byte[] value = new byte[length];
            buffer.get(value);
            return value;
        }

        private byte[] readLengthFramed(String label, int cap) {
            requireRemaining(Integer.BYTES);
            long unsignedLength = Integer.toUnsignedLong(buffer.getInt());
            if (unsignedLength > Integer.MAX_VALUE || unsignedLength > cap || unsignedLength > buffer.remaining()) {
                throw new IllegalArgumentException(label + " length exceeds its validated allocation bound");
            }
            int length = Math.toIntExact(unsignedLength);
            largestLengthFramedAllocation = Math.max(largestLengthFramedAllocation, length);
            totalLengthFramedAllocation = Math.addExact(totalLengthFramedAllocation, length);
            byte[] value = new byte[length];
            buffer.get(value);
            return value;
        }

        private void requireEof() {
            if (buffer.hasRemaining()) {
                throw new IllegalArgumentException("candidate NTA1 has trailing bytes");
            }
        }

        private void requireRemaining(int length) {
            if (length < 0 || buffer.remaining() < length) {
                throw new IllegalArgumentException("candidate NTA1 is truncated");
            }
        }
    }
}
