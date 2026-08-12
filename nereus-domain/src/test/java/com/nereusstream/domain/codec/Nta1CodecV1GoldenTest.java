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

package com.nereusstream.domain.codec;

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.domain.aggregate.TopicBindingAggregateV1;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import java.io.IOException;
import java.io.InputStream;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class Nta1CodecV1GoldenTest {
    private static final String RESOURCE = "/com/nereusstream/domain/codec/nta1-v1-goldens.properties";

    @Test
    void productionCodecOwnsExactMinimumTypicalAndBoundaryGoldens() throws IOException {
        Map<String, TopicBindingAggregateV1> vectors = new LinkedHashMap<>();
        vectors.put("kafka.minimum", Nta1ProductionTestFixtures.kafkaMinimum());
        vectors.put("kafka.typical", Nta1ProductionTestFixtures.kafkaTypical());
        vectors.put("kafka.boundary", Nta1ProductionTestFixtures.kafkaBoundary());
        vectors.put("pulsar.minimum", Nta1ProductionTestFixtures.pulsarMinimum());
        vectors.put("pulsar.typical", Nta1ProductionTestFixtures.pulsarTypical());
        vectors.put("pulsar.boundary", Nta1ProductionTestFixtures.pulsarBoundary());

        Properties expected = loadProperties();
        Properties generated = new Properties();
        vectors.forEach((name, aggregate) -> recordVector(generated, name, aggregate, name.endsWith("boundary")));
        if (expected.containsValue("GENERATE")) {
            throw new AssertionError(render(generated));
        }
        assertThat(generated).containsExactlyInAnyOrderEntriesOf(expected);
    }

    private static Properties loadProperties() throws IOException {
        Properties properties = new Properties();
        try (InputStream input = Nta1CodecV1GoldenTest.class.getResourceAsStream(RESOURCE)) {
            if (input == null) {
                throw new IllegalStateException("missing NTA1 v1 golden resource");
            }
            properties.load(input);
        }
        return properties;
    }

    private static void recordVector(
            Properties target, String name, TopicBindingAggregateV1 aggregate, boolean summarized) {
        CanonicalBytes encoded = Nta1CodecV1.encode(aggregate);
        assertThat(Nta1CodecV1.decode(encoded)).isEqualTo(aggregate);
        byte[] bytes = encoded.toByteArray();
        target.setProperty(name + ".length", Integer.toString(bytes.length));
        target.setProperty(name + ".sha256", Sha256Digest.hash(encoded).toHex());
        if (summarized) {
            target.setProperty(name + ".prefix", HexFormat.of().formatHex(bytes, 0, 32));
            target.setProperty(name + ".suffix", HexFormat.of().formatHex(bytes, bytes.length - 32, bytes.length));
        } else {
            target.setProperty(name + ".hex", encoded.toHex());
        }
    }

    private static String render(Properties properties) {
        return properties.stringPropertyNames().stream()
                .sorted()
                .map(key -> key + "=" + properties.getProperty(key))
                .reduce((left, right) -> left + System.lineSeparator() + right)
                .orElseThrow();
    }
}
