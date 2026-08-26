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

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.object.ObjectKafkaTestFixtures;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HexFormat;
import java.util.List;
import org.junit.jupiter.api.Test;

public final class Nwkcp1ProtocolFixtureV1Test {
    private static final String ROOT_PREFIX = "cells/01/shards/0007/runs/0000000000000000001";
    private static final String RESOURCE =
            "/com/nereusstream/kafka/bookkeeper/object/nwkcp1/nwkcp1-protocol-fixture-v1.tsv";

    @Test
    void exactProtocolFixtureMatchesProductionCodecAndStrictRoundTrips() throws IOException {
        Fixture fixture = fixture();
        try (InputStream stream = Nwkcp1ProtocolFixtureV1Test.class.getResourceAsStream(RESOURCE)) {
            assertThat(stream).isNotNull();
            assertThat(stream.readAllBytes()).isEqualTo(fixture.tsv());
        }

        assertThat(Nwkcp1CodecV1.decodeVerified(
                        ROOT_PREFIX,
                        fixture.object().key(),
                        fixture.object().length(),
                        fixture.object().digest(),
                        fixture.object().body()))
                .isEqualTo(fixture.value());
        assertThat(KafkaProtocolCheckpointHeadCodecV1.decode(
                                ROOT_PREFIX, fixture.value().walRunRootSha(), fixture.openBytes())
                        .state())
                .isEqualTo(KafkaProtocolCheckpointHeadStateV1.OPEN);
        assertThat(KafkaProtocolCheckpointHeadCodecV1.decode(
                                ROOT_PREFIX, fixture.value().walRunRootSha(), fixture.terminalBytes())
                        .state())
                .isEqualTo(KafkaProtocolCheckpointHeadStateV1.TERMINAL);
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            throw new IllegalArgumentException("expected one explicit fixture output path");
        }
        Files.write(Path.of(args[0]), fixture().tsv(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
    }

    private static Fixture fixture() {
        Nwkcp1ObjectV1 value = new Nwkcp1ObjectV1(
                ObjectKafkaTestFixtures.digest(12), List.of(ObjectKafkaTestFixtures.checkpoint(100)));
        Nwkcp1EncodedObjectV1 object = Nwkcp1CodecV1.encode(ROOT_PREFIX, value);
        KafkaProtocolCheckpointHeadV1 open = KafkaProtocolCheckpointHeadV1.open(
                value.walRunRootSha(),
                9,
                null,
                object,
                List.of(KafkaCheckpointCoverageV1.from(value.rows().get(0).vector())));
        CanonicalBytes openBytes = KafkaProtocolCheckpointHeadCodecV1.encode(ROOT_PREFIX, value.walRunRootSha(), open);
        CanonicalBytes terminalBytes =
                KafkaProtocolCheckpointHeadCodecV1.encode(ROOT_PREFIX, value.walRunRootSha(), open.terminal());

        StringBuilder output = new StringBuilder("artifactId\tstate\tkey\tlength\tsha256\thex\n");
        append(output, "NWKCP1_OBJECT", "IMMUTABLE", object.key(), object.body());
        String headKey = Nwkcp1ObjectKeyV1.headKey(ROOT_PREFIX);
        append(output, "KAFKA_PROTOCOL_CHECKPOINT_HEAD_OPEN", "OPEN", headKey, openBytes);
        append(output, "KAFKA_PROTOCOL_CHECKPOINT_HEAD_TERMINAL", "TERMINAL", headKey, terminalBytes);
        return new Fixture(
                value, object, openBytes, terminalBytes, output.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static void append(StringBuilder output, String id, String state, String key, CanonicalBytes bytes) {
        output.append(id)
                .append('\t')
                .append(state)
                .append('\t')
                .append(key)
                .append('\t')
                .append(bytes.length())
                .append('\t')
                .append(Sha256Digest.hash(bytes).toHex())
                .append('\t')
                .append(HexFormat.of().formatHex(bytes.toByteArray()))
                .append('\n');
    }

    private record Fixture(
            Nwkcp1ObjectV1 value,
            Nwkcp1EncodedObjectV1 object,
            CanonicalBytes openBytes,
            CanonicalBytes terminalBytes,
            byte[] tsv) {}
}
