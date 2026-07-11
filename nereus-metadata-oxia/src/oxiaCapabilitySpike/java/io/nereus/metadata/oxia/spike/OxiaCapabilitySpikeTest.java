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

package io.nereus.metadata.oxia.spike;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.GetOption;
import io.oxia.client.api.options.GetSequenceUpdatesOption;
import io.oxia.client.api.options.ListOption;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.RangeScanOption;
import io.oxia.testcontainers.OxiaContainer;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
final class OxiaCapabilitySpikeTest {
    private static final String OXIA_CLIENT_COORDINATES = "io.github.oxia-db:oxia-client:0.9.0";
    private static final String OXIA_TESTCONTAINERS_COORDINATES =
            "io.github.oxia-db:oxia-testcontainers:0.7.4";
    private static final String TESTCONTAINERS_COORDINATES = "org.testcontainers:junit-jupiter:1.20.4";
    private static final String OXIA_IMAGE = "oxia/oxia:0.16.3";
    private static final String MULTI_KEY_CAPABILITY = "NOT_SUPPORTED_BY_PUBLIC_JAVA_API";

    @Container
    private static final OxiaContainer OXIA =
            new OxiaContainer(DockerImageName.parse(OXIA_IMAGE)).withShards(10);

    private static final ConcurrentSkipListMap<String, String> EVIDENCE = new ConcurrentSkipListMap<>();

    @Test
    void partitionKeyRoutesGetListAndRangeScan() throws Exception {
        String prefix = keyPrefix("partition");
        String partitionKey = partitionKey(prefix);

        try (SyncOxiaClient client = OxiaClientBuilder.create(OXIA.getServiceAddress()).syncClient()) {
            client.put(prefix + "/a", bytes("a"), Set.of(PutOption.PartitionKey(partitionKey)));
            client.put(prefix + "/b", bytes("b"), Set.of(PutOption.PartitionKey(partitionKey)));
            client.put(prefix + "/c", bytes("c"), Set.of(PutOption.PartitionKey(partitionKey)));
            client.put(prefix + "/d", bytes("d"), Set.of(PutOption.PartitionKey(partitionKey)));

            GetResult result = client.get(prefix + "/a", Set.of(GetOption.PartitionKey(partitionKey)));
            assertThat(result.value()).isEqualTo(bytes("a"));

            String wrongPartitionKey = IntStream.range(0, 100)
                    .mapToObj(index -> "wrong-" + index)
                    .filter(candidate -> client.get(
                            prefix + "/a", Set.of(GetOption.PartitionKey(candidate))) == null)
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("could not find a partition key routed to another shard"));

            List<String> wrongPartitionList =
                    client.list(prefix + "/a", prefix + "/e", Set.of(ListOption.PartitionKey(wrongPartitionKey)));
            assertThat(wrongPartitionList).isEmpty();

            List<String> keys =
                    client.list(prefix + "/a", prefix + "/e", Set.of(ListOption.PartitionKey(partitionKey)));
            assertThat(keys)
                    .containsExactly(prefix + "/a", prefix + "/b", prefix + "/c", prefix + "/d");

            Iterable<GetResult> scan =
                    client.rangeScan(
                            prefix + "/a", prefix + "/d", Set.of(RangeScanOption.PartitionKey(partitionKey)));
            List<String> scannedKeys =
                    StreamSupport.stream(scan.spliterator(), false).map(GetResult::key).toList();
            assertThat(scannedKeys).containsExactly(prefix + "/a", prefix + "/b", prefix + "/c");
        }

        EVIDENCE.put("partitionKeyRouting", "PASS");
    }

    @Test
    void singleKeyCasMapsStaleVersionConflict() throws Exception {
        String prefix = keyPrefix("cas");
        String partitionKey = partitionKey(prefix);
        String key = prefix + "/committed-end-offset";

        try (SyncOxiaClient client = OxiaClientBuilder.create(OXIA.getServiceAddress()).syncClient()) {
            client.put(key, bytes("v1"), Set.of(PutOption.PartitionKey(partitionKey)));
            long versionId =
                    client.get(key, Set.of(GetOption.PartitionKey(partitionKey))).version().versionId();

            client.put(
                    key,
                    bytes("v2"),
                    Set.of(PutOption.PartitionKey(partitionKey), PutOption.IfVersionIdEquals(versionId)));

            assertThatThrownBy(
                            () ->
                                    client.put(
                                            key,
                                            bytes("v3"),
                                            Set.of(
                                                    PutOption.PartitionKey(partitionKey),
                                                    PutOption.IfVersionIdEquals(versionId))))
                    .isInstanceOf(UnexpectedVersionIdException.class);
        }

        EVIDENCE.put("singleKeyConditionalPut", "PASS");
    }

    @Test
    void offsetIndexKeysKeepNumericOrdering() throws Exception {
        String prefix = keyPrefix("offset-index");
        String partitionKey = partitionKey(prefix);
        String indexPrefix = prefix + "/offset-index";

        try (SyncOxiaClient client = OxiaClientBuilder.create(OXIA.getServiceAddress()).syncClient()) {
            client.put(offsetIndexKey(indexPrefix, 100, 0), bytes("100"), Set.of(PutOption.PartitionKey(partitionKey)));
            client.put(offsetIndexKey(indexPrefix, 9, 0), bytes("9"), Set.of(PutOption.PartitionKey(partitionKey)));
            client.put(offsetIndexKey(indexPrefix, 10, 0), bytes("10"), Set.of(PutOption.PartitionKey(partitionKey)));

            List<String> keys =
                    client.list(
                            offsetIndexKey(indexPrefix, 0, 0),
                            offsetIndexKey(indexPrefix, Long.MAX_VALUE, Long.MAX_VALUE),
                            Set.of(ListOption.PartitionKey(partitionKey)));
            assertThat(keys)
                    .containsExactly(
                            offsetIndexKey(indexPrefix, 9, 0),
                            offsetIndexKey(indexPrefix, 10, 0),
                            offsetIndexKey(indexPrefix, 100, 0));
        }

        EVIDENCE.put("fixedWidthOffsetIndexOrdering", "PASS");
    }

    @Test
    void sequenceKeysRequirePartitionKeyAndGenerateStableWidthSuffix() throws Exception {
        String prefix = keyPrefix("sequence");
        String partitionKey = partitionKey(prefix);
        String sequenceRoot = prefix + "/seq";

        try (SyncOxiaClient client = OxiaClientBuilder.create(OXIA.getServiceAddress()).syncClient()) {
            assertThatThrownBy(() -> client.getSequenceUpdates(sequenceRoot, ignored -> {}, Set.of()))
                    .isInstanceOf(IllegalArgumentException.class);

            PutResult first =
                    client.put(
                            sequenceRoot,
                            bytes("first"),
                            Set.of(PutOption.PartitionKey(partitionKey), PutOption.SequenceKeysDeltas(List.of(1L))));
            PutResult second =
                    client.put(
                            sequenceRoot,
                            bytes("second"),
                            Set.of(PutOption.PartitionKey(partitionKey), PutOption.SequenceKeysDeltas(List.of(1L))));

            assertThat(first.key()).isEqualTo(sequenceRoot + "-00000000000000000001");
            assertThat(second.key()).isEqualTo(sequenceRoot + "-00000000000000000002");
            assertThat(client.get(first.key(), Set.of(GetOption.PartitionKey(partitionKey))).value())
                    .isEqualTo(bytes("first"));
            assertThat(client.get(second.key(), Set.of(GetOption.PartitionKey(partitionKey))).value())
                    .isEqualTo(bytes("second"));
        }

        EVIDENCE.put("sequenceKeysWithPartitionKey", "PASS");
    }

    @Test
    void publicJavaApiDoesNotExposeMultiKeyConditionalWrite() {
        List<String> candidates =
                Stream.of(AsyncOxiaClient.class, SyncOxiaClient.class)
                        .flatMap(type -> Arrays.stream(type.getMethods()))
                        .filter(method -> Modifier.isPublic(method.getModifiers()))
                        .filter(method -> !method.isSynthetic())
                        .filter(OxiaCapabilitySpikeTest::looksLikeMultiKeyConditionalWrite)
                        .map(Method::toGenericString)
                        .distinct()
                        .sorted()
                        .toList();

        assertThat(candidates)
                .as("The selected public Oxia Java API must not silently grow a multi-key write primitive without "
                        + "updating the Phase 1 commit design and this spike.")
                .isEmpty();

        EVIDENCE.put("multiKeyConditionalWrite", MULTI_KEY_CAPABILITY);
    }

    @AfterAll
    static void writeReport() throws IOException {
        Path reportDir =
                Path.of(
                        System.getProperty(
                                "nereus.oxiaCapabilitySpike.reportDir",
                                "build/reports/oxia-capability-spike"));
        Files.createDirectories(reportDir);
        Files.writeString(reportDir.resolve("summary.md"), markdownReport(), UTF_8);
        Files.writeString(reportDir.resolve("summary.json"), jsonReport(), UTF_8);
    }

    private static boolean looksLikeMultiKeyConditionalWrite(Method method) {
        String name = method.getName().toLowerCase(Locale.ROOT);
        if (name.equals("write")
                || name.contains("transaction")
                || name.contains("multi")
                || name.contains("batch")
                || name.contains("commit")) {
            return true;
        }
        if (name.contains("range")) {
            return false;
        }
        boolean hasMultiKeyPayload =
                Arrays.stream(method.getParameterTypes())
                        .anyMatch(
                                parameterType ->
                                        Map.class.isAssignableFrom(parameterType)
                                                || (Collection.class.isAssignableFrom(parameterType)
                                                        && !Set.class.isAssignableFrom(parameterType)));
        return hasMultiKeyPayload && (name.contains("put") || name.contains("delete"));
    }

    private static String keyPrefix(String capability) {
        return "nereus/m05/" + capability + "/" + UUID.randomUUID();
    }

    private static String partitionKey(String prefix) {
        return "stream:" + prefix;
    }

    private static String offsetIndexKey(String prefix, long offsetEnd, long generation) {
        return "%s/%019d/%019d".formatted(prefix, offsetEnd, generation);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(UTF_8);
    }

    private static String markdownReport() {
        List<String> lines = new ArrayList<>();
        lines.add("# M0.5 Oxia Capability Spike Report");
        lines.add("");
        lines.add("| Item | Result |");
        lines.add("| --- | --- |");
        lines.add("| Oxia client | `" + OXIA_CLIENT_COORDINATES + "` |");
        lines.add("| Oxia testcontainers | `" + OXIA_TESTCONTAINERS_COORDINATES + "` |");
        lines.add("| Testcontainers | `" + TESTCONTAINERS_COORDINATES + "` |");
        lines.add("| Oxia image | `" + OXIA_IMAGE + "` |");
        EVIDENCE.forEach((key, value) -> lines.add("| " + key + " | `" + value + "` |"));
        lines.add("");
        lines.add("The public Java API result is intentionally a passing spike result. It means M2/M4 must redesign");
        lines.add("`commitStreamSlice` before implementing the original multi-key conditional commit state machine.");
        lines.add("");
        return String.join(System.lineSeparator(), lines);
    }

    private static String jsonReport() {
        StringBuilder json = new StringBuilder();
        json.append("{\n");
        json.append("  \"oxiaClient\": \"").append(jsonEscape(OXIA_CLIENT_COORDINATES)).append("\",\n");
        json.append("  \"oxiaTestcontainers\": \"")
                .append(jsonEscape(OXIA_TESTCONTAINERS_COORDINATES))
                .append("\",\n");
        json.append("  \"testcontainers\": \"").append(jsonEscape(TESTCONTAINERS_COORDINATES)).append("\",\n");
        json.append("  \"oxiaImage\": \"").append(jsonEscape(OXIA_IMAGE)).append("\",\n");
        json.append("  \"evidence\": {\n");
        int index = 0;
        for (Map.Entry<String, String> entry : EVIDENCE.entrySet()) {
            json.append("    \"")
                    .append(jsonEscape(entry.getKey()))
                    .append("\": \"")
                    .append(jsonEscape(entry.getValue()))
                    .append("\"");
            if (++index < EVIDENCE.size()) {
                json.append(',');
            }
            json.append('\n');
        }
        json.append("  }\n");
        json.append("}\n");
        return json.toString();
    }

    private static String jsonEscape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
