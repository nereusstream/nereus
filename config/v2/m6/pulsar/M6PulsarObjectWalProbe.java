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

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.storage.nereus.v2.MetadataStoreNereusOwnershipWitnessProvider;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

final class M6PulsarObjectWalProbe {
    private static final String TOPIC = "persistent://public/m6-native/handoff";
    private static final String BUNDLE = "public/m6-native/0x00000000_0xffffffff";
    private static final long RESERVED_START = 1L << 62;
    private static final long SLICE_END = RESERVED_START + (1L << 40);
    private static final byte[] CRASH_BODY = "acknowledged by original Object WAL before SIGKILL"
            .getBytes(StandardCharsets.UTF_8);
    private static final byte[] UNKNOWN_BODY = "original applied Object WAL append with no response before SIGKILL"
            .getBytes(StandardCharsets.UTF_8);
    private static final byte[] NEW_BODY = "new native owner wrote after Object WAL recovery"
            .getBytes(StandardCharsets.UTF_8);

    public static void main(String[] args) throws Exception {
        Path path = Path.of(args[4]);
        var expected = new Properties();
        try (var input = Files.newBufferedReader(path)) {
            expected.load(input);
        }
        try (var client = PulsarClient.builder().serviceUrl("pulsar://" + args[3])
                    .operationTimeout(75, TimeUnit.SECONDS).build();
                var admin = PulsarAdmin.builder().serviceHttpUrl("http://" + args[2]).build();
                var metadata = MetadataStoreFactory.create(args[1], MetadataStoreConfig.builder().build())) {
            if ("before-crash-with-unknown".equals(args[0])) {
                int count = Integer.parseInt(expected.getProperty("count"));
                var bucket = bucket(args);
                var faults = new M6PulsarBookKeeperSemanticsProbe.Faults();
                try (var producer = client.newProducer().topic(TOPIC).enableBatching(false).create()) {
                    var acknowledged = (MessageIdAdv) producer.send(CRASH_BODY);
                    if (acknowledged.getEntryId() != 0) {
                        throw new AssertionError("Crash fixture must leave one entry in the current virtual ledger");
                    }
                    record(expected, count, acknowledged, CRASH_BODY);
                    faults.set("/" + bucket + "/", "first-put-response-lost");
                    var pending = producer.sendAsync(UNKNOWN_BODY);
                    String version = faults.awaitAppliedVersion();
                    var observed = faults.call("GET", "");
                    String key = observed.path("selectedPath").asText();
                    if (pending.isDone() || observed.path("hits").asInt() != 1
                            || !key.startsWith("/" + bucket + "/") || !key.endsWith(".nwg")) {
                        throw new AssertionError("No original immutable PUT was held before the client response");
                    }
                    key = key.substring(bucket.length() + 2);
                    assertExactVersion(bucket, key, version);
                    var current = new MetadataStoreNereusOwnershipWitnessProvider(metadata, args[2])
                            .read(BUNDLE).toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow();
                    expected.setProperty("crashAcquisition", current.acquisitionId().value());
                    expected.setProperty("crashVersion", Long.toString(current.backendVersion()));
                    expected.setProperty("unknownKey", key);
                    expected.setProperty("unknownVersion", version);
                    record(expected, count + 1, new MessageIdImpl(acknowledged.getLedgerId(), 1,
                            acknowledged.getPartitionIndex()), UNKNOWN_BODY);
                    requirePositions(expected);
                    try (var output = Files.newBufferedWriter(path)) {
                        expected.store(output, null);
                    }
                    if (pending.isDone()) {
                        throw new AssertionError("Client append completed without the original PUT response");
                    }
                    Files.createFile(Path.of(args[6]));
                    // The runner kills this client before the Broker, ruling out a producer resend after takeover.
                    new CountDownLatch(1).await();
                }
                return;
            }
            if (!"after-crash".equals(args[0])) {
                throw new AssertionError("unknown Object WAL process probe");
            }
            requirePositions(expected);
            int count = Integer.parseInt(expected.getProperty("count"));
            var prior = id(expected, count - 1);
            try (var reader = client.newReader().topic(TOPIC).startMessageId(MessageId.earliest).create()) {
                for (int index = 0; index < count; index++) {
                    var message = reader.readNext(15, TimeUnit.SECONDS);
                    var body = Base64.getDecoder().decode(expected.getProperty("body." + index));
                    if (message == null || !id(expected, index).equals(message.getMessageId())
                            || !Arrays.equals(message.getData(), body)) {
                        throw new AssertionError("Object WAL lost or changed entry " + index + " after SIGKILL");
                    }
                }
                var selected = new MetadataStoreNereusOwnershipWitnessProvider(metadata, args[2])
                        .read(BUNDLE).toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow();
                if (selected.acquisitionId().value().equals(expected.getProperty("crashAcquisition"))
                        || selected.backendVersion() <= Long.parseLong(expected.getProperty("crashVersion"))) {
                    throw new AssertionError("Object WAL recovery did not prove a new native owner");
                }
                try (var producer = client.newProducer().topic(TOPIC).enableBatching(false).create()) {
                    var position = producer.send(NEW_BODY);
                    var newer = (MessageIdAdv) position;
                    if (newer.getLedgerId() <= prior.getLedgerId() || newer.getEntryId() != 0) {
                        throw new AssertionError("Recovered full virtual ledger reused an ID: " + prior + " " + newer);
                    }
                    record(expected, count, position, NEW_BODY);
                    requirePositions(expected);
                    var next = reader.readNext(15, TimeUnit.SECONDS);
                    if (next == null || !position.equals(next.getMessageId()) || !Arrays.equals(next.getData(), NEW_BODY)) {
                        throw new AssertionError("Exact successor entry could not be read");
                    }
                }
            }
            var key = "/managed-ledgers/" + TopicName.get(TOPIC).getPersistenceNamingEncoding();
            if (metadata.get(key).get(10, TimeUnit.SECONDS).isPresent()) {
                throw new AssertionError("Object WAL created native BookKeeper ledger metadata");
            }
            if (admin.topics().getInternalStats(TOPIC).ledgers.size() < 4) {
                throw new AssertionError("Native admin did not expose the retained virtual ledger chain");
            }
            if (expected.containsKey("unknownKey")) {
                assertExactVersion(bucket(args), expected.getProperty("unknownKey"),
                        expected.getProperty("unknownVersion"));
                System.out.println("PULSAR_OBJECT_WAL_ORIGINAL_UNKNOWN_RECOVERY_PASS");
            }
            System.out.println("PULSAR_OBJECT_WAL_CRASH_TAKEOVER_PASS");
        }
    }

    private static String bucket(String[] args) throws Exception {
        var props = new Properties();
        try (var input = Files.newBufferedReader(Path.of(args[5]))) {
            props.load(input);
        }
        return props.getProperty("s3Bucket");
    }

    private static void assertExactVersion(String bucket, String key, String version) {
        try (var s3 = S3Client.builder().credentialsProvider(EnvironmentVariableCredentialsProvider.create())
                .endpointOverride(URI.create(System.getenv("NEREUS_M6_PULSAR_DIRECT_S3")))
                .forcePathStyle(true).region(Region.US_EAST_1).build()) {
            var actual = s3.listObjectVersions(request -> request.bucket(bucket).prefix(key));
            if (actual.isTruncated() || !actual.deleteMarkers().isEmpty() || actual.versions().size() != 1
                    || !actual.versions().getFirst().key().equals(key)
                    || !actual.versions().getFirst().versionId().equals(version)) {
                throw new AssertionError("Original Object WAL immutable key/version differs: " + key);
            }
        }
    }

    private static void requirePositions(Properties expected) throws Exception {
        var unique = new HashSet<MessageIdAdv>();
        long previousLedger = -1;
        long previousEntry = -1;
        int size = Integer.parseInt(expected.getProperty("count"));
        for (int index = 0; index < size; index++) {
            var id = id(expected, index);
            if (id.getLedgerId() < RESERVED_START || id.getLedgerId() >= SLICE_END || !unique.add(id)
                    || id.getLedgerId() < previousLedger
                    || id.getEntryId() != (id.getLedgerId() == previousLedger ? previousEntry + 1 : 0)) {
                throw new AssertionError("Position escaped its original Range64 slice or reused an entry: " + id);
            }
            previousLedger = id.getLedgerId();
            previousEntry = id.getEntryId();
        }
    }

    private static void record(Properties state, int index, MessageId position, byte[] bytes) {
        var b64 = Base64.getEncoder();
        state.setProperty("count", Integer.toString(index + 1));
        state.setProperty("position." + index, b64.encodeToString(position.toByteArray()));
        state.setProperty("body." + index, b64.encodeToString(bytes));
    }

    private static MessageIdAdv id(Properties state, int index) throws Exception {
        return (MessageIdAdv) MessageId.fromByteArray(Base64.getDecoder().decode(state.getProperty("position." + index)));
    }
}
