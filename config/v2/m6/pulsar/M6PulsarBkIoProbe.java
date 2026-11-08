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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pulsar.broker.storage.nereus.v2.MetadataStoreNereusOwnershipWitnessProvider;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyData;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyType;
import org.apache.pulsar.common.policies.data.NamespaceIsolationData;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;

final class M6PulsarBkIoProbe {
    private static final String NAMESPACE = "public/m6-native";
    private static final String RANGE = "0x00000000_0xffffffff";
    private static final String TOPIC = "persistent://" + NAMESPACE + "/handoff";
    private static final String WRITE_TOPIC = "persistent://" + NAMESPACE + "/io-write";
    private static final String FOREIGN = "public/m6-foreign";
    private static final String FOREIGN_TOPIC = "persistent://" + FOREIGN + "/sentinel";
    private static final byte[] HELD_WRITE = "original native BK append held across unload"
            .getBytes(StandardCharsets.UTF_8);

    public static void main(String[] args) throws Exception {
        String mode = args[0];
        try (var admin = PulsarAdmin.builder().serviceHttpUrl("http://" + args[2]).build();
                var client = PulsarClient.builder().serviceUrl("pulsar://" + args[3])
                        .operationTimeout(20, TimeUnit.SECONDS).build();
                var metadata = MetadataStoreFactory.create(args[1], MetadataStoreConfig.builder().build())) {
            if ("configure".equals(mode)) {
                admin.namespaces().createNamespace(FOREIGN, 1);
                isolate(admin, "foreign-cell", FOREIGN, "localhost");
                isolate(admin, "primary-cell", NAMESPACE, "127[.]0[.]0[.]1");
                // Claim the restored bundle with this sole primary Broker before starting its peer.
                admin.topics().getInternalStats(TOPIC);
                return;
            }
            if ("foreign".equals(mode)) {
                verifyForeign(args[2], args[3], metadata);
                return;
            }
            String target = args[4];
            String cellKey = properties(Path.of(args[5])).getProperty("probeCellOwnerKey");
            var original = M6PulsarBookKeeperSemanticsProbe.readActiveCellOwner(metadata, cellKey);
            var witness = new MetadataStoreNereusOwnershipWitnessProvider(metadata, target);
            var wire = new Wire();
            if ("read".equals(mode)) {
                var state = properties(Path.of(args[6]));
                var last = id(state, Integer.parseInt(state.getProperty("count")) - 1);
                var rawLedgers = admin.topics().getInternalStats(TOPIC).ledgers.stream()
                        .filter(ledger -> !ledger.offloaded && ledger.ledgerId != last.getLedgerId())
                        .map(ledger -> ledger.ledgerId).collect(java.util.stream.Collectors.toSet());
                int index = Integer.parseInt(state.getProperty("count")) - 2;
                while (index >= 0 && !rawLedgers.contains(id(state, index).getLedgerId())) {
                    index--;
                }
                if (index < 0) {
                    throw new AssertionError("No closed raw BookKeeper entry is available for the read cut");
                }
                var position = id(state, index);
                byte[] payload = Base64.getDecoder().decode(state.getProperty("body." + index));
                wire.hold("read", position);
                try (var reader = client.newReader().topic(TOPIC).startMessageId(position)
                        .startMessageIdInclusive().create()) {
                    var observed = reader.readNextAsync();
                    var replies = wire.awaitPeers(1);
                    System.out.println("PULSAR_NATIVE_BK_POSITIVE_READ_REPLY_HELD " + position + " " + replies);
                    if (!observed.cancel(false)) {
                        throw new AssertionError("Raw BK read completed before observer cancellation");
                    }
                    reader.closeAsync();
                    verifyDrain(args, admin, metadata, cellKey, original, witness, wire, observed);
                } finally {
                    wire.call("DELETE", "");
                }
                verifyExact(client, TOPIC, position, payload);
                System.out.println("PULSAR_NATIVE_BK_HELD_READ_CANCEL_AND_UNLOAD_PASS");
            } else if ("write".equals(mode)) {
                MessageIdAdv position;
                try (var producer = client.newProducer().topic(WRITE_TOPIC).enableBatching(false).create()) {
                    var first = (MessageIdAdv) producer.send(new byte[]{1});
                    if (first.getEntryId() != 0) {
                        throw new AssertionError("New raw BK fixture did not leave entry one available");
                    }
                    position = new MessageIdImpl(first.getLedgerId(), 1, first.getPartitionIndex());
                    wire.hold("write", position);
                    var observed = producer.sendAsync(HELD_WRITE);
                    var replies = wire.awaitPeers(2); // Observe the actual write quorum before release.
                    System.out.println("PULSAR_NATIVE_BK_POSITIVE_WRITE_QUORUM_HELD " + position + " " + replies);
                    if (!observed.cancel(false)) {
                        throw new AssertionError("Native BK write completed before observer cancellation");
                    }
                    verifyDrain(args, admin, metadata, cellKey, original, witness, wire, observed);
                } finally {
                    wire.call("DELETE", "");
                }
                verifyExact(client, WRITE_TOPIC, position, HELD_WRITE);
                System.out.println("PULSAR_NATIVE_BK_HELD_WRITE_CANCEL_AND_UNLOAD_PASS");
            } else {
                throw new AssertionError("Unknown raw BookKeeper I/O probe");
            }
        }
    }

    private static void verifyDrain(String[] args, PulsarAdmin admin, MetadataStore metadata, String cellKey,
            M6PulsarBookKeeperSemanticsProbe.CellOwner owner, MetadataStoreNereusOwnershipWitnessProvider target,
            Wire wire, CompletableFuture<?> cancelled) throws Exception {
        var unloading = admin.namespaces().unloadNamespaceBundleAsync(NAMESPACE, RANGE, args[4]);
        try {
            unloading.get(800, TimeUnit.MILLISECONDS);
            throw new AssertionError("Broker released raw BK I/O before its positive callback reached the owner");
        } catch (TimeoutException held) {
            verifyForeign(args[7], args[8], metadata);
            if (unloading.isDone() || !cancelled.isCancelled()
                    || !owner.equals(M6PulsarBookKeeperSemanticsProbe.readActiveCellOwner(metadata, cellKey))
                    || target.read(NAMESPACE + "/" + RANGE).toCompletableFuture().get(3, TimeUnit.SECONDS).isPresent()) {
                throw new AssertionError("Original BookKeeper Cell lease or observer changed before physical I/O drain");
            }
        } finally {
            wire.call("PATCH", "");
        }
        unloading.get(30, TimeUnit.SECONDS);
    }

    static void isolate(PulsarAdmin admin, String name, String namespace, String primary) throws Exception {
        admin.clusters().createNamespaceIsolationPolicy("m6", name, NamespaceIsolationData.builder()
                .namespaces(List.of(namespace)).primary(List.of(primary)).secondary(List.of())
                .unloadScope(org.apache.pulsar.common.policies.data.NamespaceIsolationPolicyUnloadScope.none)
                .autoFailoverPolicy(AutoFailoverPolicyData.builder()
                        .policyType(AutoFailoverPolicyType.min_available)
                        .parameters(Map.of("min_limit", "1", "usage_threshold", "100")).build())
                .build());
    }

    private static void verifyForeign(String web, String service, MetadataStore metadata) throws Exception {
        byte[] bytes = java.util.UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        try (var client = PulsarClient.builder().serviceUrl("pulsar://" + service)
                .operationTimeout(6, TimeUnit.SECONDS).build();
                var producer = client.newProducer().topic(FOREIGN_TOPIC).enableBatching(false).create()) {
            var id = producer.sendAsync(bytes).get(4, TimeUnit.SECONDS);
            if (((MessageIdAdv) id).getLedgerId() >= 1L << 62) {
                throw new AssertionError("Independent Cell did not use its qualified raw BookKeeper route");
            }
            verifyExact(client, FOREIGN_TOPIC, id, bytes);
        }
        var foreign = new MetadataStoreNereusOwnershipWitnessProvider(metadata, web)
                .read(FOREIGN + "/" + RANGE).toCompletableFuture().get(3, TimeUnit.SECONDS).orElseThrow();
        System.out.println("PULSAR_NATIVE_BK_INDEPENDENT_FOREIGN_CELL_PASS " + foreign.acquisitionId());
    }

    private static void verifyExact(PulsarClient client, String topic, MessageId expected, byte[] bytes)
            throws Exception {
        try (var reader = client.newReader().topic(topic).startMessageId(expected).startMessageIdInclusive().create()) {
            var actual = reader.readNext(5, TimeUnit.SECONDS);
            if (actual == null || !expected.equals(actual.getMessageId()) || !Arrays.equals(bytes, actual.getData())) {
                throw new AssertionError("Original physical entry changed across the ownership drain: " + expected);
            }
        }
    }

    private static Properties properties(Path path) throws Exception {
        var result = new Properties();
        try (var stream = Files.newBufferedReader(path)) {
            result.load(stream);
        }
        return result;
    }

    private static MessageIdAdv id(Properties state, int index) throws Exception {
        return (MessageIdAdv) MessageId.fromByteArray(Base64.getDecoder().decode(state.getProperty("position." + index)));
    }

    private static final class Wire {
        private final HttpClient http = HttpClient.newHttpClient();
        private final URI control = URI.create(System.getenv("NEREUS_M6_PULSAR_BK_IO_CONTROL"));
        private final String token = System.getenv("NEREUS_M6_PULSAR_BK_IO_TOKEN");
        private final ObjectMapper json = new ObjectMapper();

        void hold(String operation, MessageIdAdv position) throws Exception {
            call("POST", json.writeValueAsString(Map.of("operation", operation,
                    "ledger", position.getLedgerId(), "entry", position.getEntryId())));
        }

        JsonNode awaitPeers(int minimum) throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            do {
                var observed = call("GET", "");
                if (observed.path("peers").size() >= minimum) {
                    return observed;
                }
                Thread.sleep(20);
            } while (System.nanoTime() < deadline);
            throw new AssertionError("No positive raw BookKeeper reply for the requested native entry");
        }

        JsonNode call(String method, String body) throws Exception {
            var request = HttpRequest.newBuilder(control).timeout(Duration.ofSeconds(3))
                    .header("X-Nereus-Test-Token", token)
                    .method(method, HttpRequest.BodyPublishers.ofString(body))
                    .build();
            var response = http.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new AssertionError("Raw BookKeeper fault relay failed " + method + ": " + response.statusCode());
            }
            return json.readTree(response.body());
        }
    }
}
