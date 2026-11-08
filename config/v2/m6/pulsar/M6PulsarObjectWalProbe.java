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

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.storage.object.control.WalRunControlCodec;
import com.nereusstream.storage.object.control.WalRunControlKeys;
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
import java.util.concurrent.TimeoutException;
import org.apache.pulsar.broker.storage.nereus.v2.MetadataStoreNereusOwnershipWitnessProvider;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

final class M6PulsarObjectWalProbe {
    private static final String TOPIC = "persistent://public/m6-native/handoff";
    private static final String ROLLOVER_TOPIC = "persistent://public/m6-native/physical-run";
    private static final String BUNDLE = "public/m6-native/0x00000000_0xffffffff";
    private static final String FOREIGN_NAMESPACE = "public/m6-foreign";
    private static final String FOREIGN_TOPIC = "persistent://" + FOREIGN_NAMESPACE + "/sentinel";
    private static final long RESERVED_START = 1L << 62;
    private static final long SLICE_END = RESERVED_START + (1L << 40);
    private static final byte[] CRASH_BODY = "acknowledged by original Object WAL before SIGKILL"
            .getBytes(StandardCharsets.UTF_8);
    private static final byte[] UNKNOWN_BODY = "original applied Object WAL append with no response before SIGKILL"
            .getBytes(StandardCharsets.UTF_8);
    private static final byte[] HELD_WRITE = "original Object WAL append held across native unload"
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
            if ("configure-foreign".equals(args[0])) {
                admin.namespaces().createNamespace(FOREIGN_NAMESPACE, 1);
                isolate(admin, "foreign-cell", FOREIGN_NAMESPACE, "localhost");
                isolate(admin, "primary-cell", "public/m6-native", "127[.]0[.]0[.]1");
                return;
            }
            if ("verify-foreign".equals(args[0])) {
                verifyForeign(args[2], args[3], metadata);
                return;
            }
            if ("live-physical-rollover".equals(args[0])) {
                verifyPhysicalRollover(args, client, metadata);
                return;
            }
            if ("held-write-unload".equals(args[0])) {
                verifyWriteDrain(args, client, admin, metadata, expected);
                return;
            }
            if ("held-read-unload".equals(args[0]) || "held-read-shutdown".equals(args[0])) {
                verifyReadDrain(args, client, admin, metadata, expected);
                return;
            }
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
                    expected.setProperty("unknownPositionIndex", Integer.toString(count + 1));
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
            try (var output = Files.newBufferedWriter(path)) {
                expected.store(output, null);
            }
            if (expected.containsKey("unknownKey")) {
                assertExactVersion(bucket(args), expected.getProperty("unknownKey"),
                        expected.getProperty("unknownVersion"));
                System.out.println("PULSAR_OBJECT_WAL_ORIGINAL_UNKNOWN_RECOVERY_PASS");
            }
            System.out.println("PULSAR_OBJECT_WAL_CRASH_TAKEOVER_PASS");
        }
    }

    private static void verifyPhysicalRollover(String[] args, PulsarClient client, MetadataStore metadata)
            throws Exception {
        var identities = new Properties();
        try (var file = Files.newBufferedReader(Path.of(args[5]).resolveSibling("object-wal-incarnations.properties"))) {
            identities.load(file);
        }
        String prefix = cell(args).getProperty("authorityRoot") + "/native-object-wal/"
                + identities.getProperty(ROLLOVER_TOPIC);
        var witnesses = new MetadataStoreNereusOwnershipWitnessProvider(metadata, args[2]);
        var nativeBefore = witnesses.read(BUNDLE).toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow();
        var positions = new java.util.ArrayList<MessageIdAdv>();
        com.nereusstream.storage.object.control.WalRunReference original;
        final int limit = 1024;
        try (var producer = client.newProducer().topic(ROLLOVER_TOPIC).enableBatching(false).create()) {
            positions.add((MessageIdAdv) producer.send(numbered(0)));
            original = pointer(metadata, prefix).current();
            var root = WalRunControlCodec.decodeRoot(CanonicalBytes.copyOf(raw(metadata, prefix + "/" + original.rootKey())));
            if (root.bounds().maxExtentCount() != limit) {
                throw new AssertionError("Live rollover must cross the original production extent bound");
            }
            for (int index = 1; index < limit + 2; index++) {
                var next = (MessageIdAdv) producer.send(numbered(index));
                var previous = positions.getLast();
                if (next.getLedgerId() < previous.getLedgerId()
                        || next.getEntryId() != (next.getLedgerId() == previous.getLedgerId() ? previous.getEntryId() + 1 : 0)) {
                    throw new AssertionError("Physical rollover repeated or skipped a native position: " + next);
                }
                positions.add(next);
            }
        }
        var next = pointer(metadata, prefix).current();
        var root = WalRunControlCodec.decodeRoot(CanonicalBytes.copyOf(raw(metadata, prefix + "/" + next.rootKey())));
        var previous = root.predecessor().orElseThrow();
        var sealBytes = CanonicalBytes.copyOf(raw(metadata, prefix + "/" + previous.sealKey()));
        var seal = WalRunControlCodec.decodeSeal(sealBytes);
        if (next.shardRunEpoch() != original.shardRunEpoch() + 1 || !original.equals(previous.root())
                || !original.equals(seal.root()) || seal.aggregateExtentCount() != limit
                || !com.nereusstream.domain.bytes.Sha256Digest.hash(sealBytes).equals(previous.sealSha256())) {
            throw new AssertionError("Successor did not select the original complete physical Seal");
        }
        try (var reader = client.newReader().topic(ROLLOVER_TOPIC).startMessageId(positions.get(limit - 1))
                .startMessageIdInclusive().create()) {
            for (int index = limit - 1; index < positions.size(); index++) {
                var actual = reader.readNext(15, TimeUnit.SECONDS);
                if (actual == null || !positions.get(index).equals(actual.getMessageId())
                        || !Arrays.equals(numbered(index), actual.getData())) {
                    throw new AssertionError("Native read changed position or bytes across physical rollover: " + index);
                }
            }
        }
        var nativeAfter = witnesses.read(BUNDLE).toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow();
        if (!nativeBefore.equals(nativeAfter)) {
            throw new AssertionError("Physical rollover unexpectedly changed native ownership");
        }
        System.out.println("PULSAR_OBJECT_WAL_LIVE_PHYSICAL_ROLLOVER_PASS " + limit);
    }

    private static byte[] numbered(int value) {
        return java.nio.ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
    }

    private static com.nereusstream.storage.object.control.CurrentWalRunPointer pointer(
            MetadataStore metadata, String prefix) throws Exception {
        return WalRunControlCodec.decodePointer(CanonicalBytes.copyOf(raw(metadata,
                prefix + "/" + WalRunControlKeys.pointerKey(0))));
    }

    private static void verifyReadDrain(String[] args, PulsarClient client, PulsarAdmin admin, MetadataStore metadata,
            Properties expected) throws Exception {
        String key = expected.getProperty("unknownKey");
        int entry = Integer.parseInt(expected.getProperty("unknownPositionIndex"));
        String prefix = cell(args).getProperty("authorityRoot") + "/native-object-wal/" + binding(key);
        var pointer = pointer(metadata, prefix);
        String gate = prefix + "/dispatch/" + pointer.current().rootSha256().toHex();
        byte[] before = raw(metadata, gate);
        requireGate(before, false);
        var source = new MetadataStoreNereusOwnershipWitnessProvider(metadata, args[2]);
        boolean shutdown = "held-read-shutdown".equals(args[0]);
        var original = source.read(BUNDLE).toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow();
        var faults = new M6PulsarBookKeeperSemanticsProbe.Faults();
        faults.set("/" + bucket(args) + "/" + key, "held-unversioned");
        try (var reader = client.newReader().topic(TOPIC).startMessageId(id(expected, entry))
                .startMessageIdInclusive().create()) {
            var observed = reader.readNextAsync();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (faults.call("GET", "").path("upstreamReads").asInt() == 0) {
                if (observed.isDone() || System.nanoTime() >= deadline) {
                    throw new AssertionError("The exact historical NWG1 range was not held on the S3 wire");
                }
                Thread.sleep(25);
            }
            if (!observed.cancel(false)) {
                throw new AssertionError("The official client observer completed before cancellation");
            }
            reader.closeAsync();
            if (shutdown) {
                var broker = ProcessHandle.of(Long.parseLong(args[6])).orElseThrow();
                if (!broker.destroy()) {
                    throw new AssertionError("Could not initiate shutdown of the original fixture Broker");
                }
                try {
                    Thread.sleep(800);
                    verifyForeign(args[7], args[8], metadata);
                    if (!broker.isAlive() || !observed.isCancelled() || !Arrays.equals(before, raw(metadata, gate))
                            || faults.call("GET", "").path("upstreamReads").asInt() == 0) {
                        throw new AssertionError("Shutdown released the original Object read before terminal I/O");
                    }
                } finally {
                    faults.call("PATCH", "");
                }
                long shutdownDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                byte[] settled = raw(metadata, gate);
                while (settled[5] == 0 && System.nanoTime() < shutdownDeadline) {
                    Thread.sleep(25);
                    settled = raw(metadata, gate);
                }
                requireGate(settled, true);
                System.out.println("PULSAR_OBJECT_WAL_HELD_RANGE_CANCEL_AND_SHUTDOWN_PASS");
                return;
            }
            var target = new MetadataStoreNereusOwnershipWitnessProvider(metadata, args[6]);
            String[] parts = BUNDLE.split("/");
            var unloading = admin.namespaces().unloadNamespaceBundleAsync(parts[0] + "/" + parts[1], parts[2], args[6]);
            try {
                unloading.get(2, TimeUnit.SECONDS);
                throw new AssertionError("Native unload completed before the admitted S3 read");
            } catch (TimeoutException held) {
                verifyForeign(args[7], args[8], metadata);
                if (unloading.isDone() || !Arrays.equals(before, raw(metadata, gate))
                        || target.read(BUNDLE).toCompletableFuture().get(5, TimeUnit.SECONDS).isPresent()
                        || faults.call("GET", "").path("upstreamReads").asInt() == 0
                        || !observed.isCancelled()) {
                    throw new AssertionError("Cancellation released the original Cell, source or read observer");
                }
            } finally {
                faults.call("PATCH", "");
            }
            unloading.get(35, TimeUnit.SECONDS);
        } finally {
            faults.call("DELETE", "");
        }
        requireGate(raw(metadata, gate), true);
        try (var reader = client.newReader().topic(TOPIC).startMessageId(id(expected, entry))
                .startMessageIdInclusive().create()) {
            var same = reader.readNext(15, TimeUnit.SECONDS);
            if (same == null || !id(expected, entry).equals(same.getMessageId())
                    || !Arrays.equals(UNKNOWN_BODY, same.getData())) {
                throw new AssertionError("Late source callback changed the original read on the successor");
            }
        }
        var next = new MetadataStoreNereusOwnershipWitnessProvider(metadata, args[6])
                .read(BUNDLE).toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow();
        if (next.acquisitionId().equals(original.acquisitionId())
                || next.backendVersion() <= original.backendVersion()) {
            throw new AssertionError("Read drain did not hand off to a distinct native acquisition");
        }
        System.out.println("PULSAR_OBJECT_WAL_HELD_RANGE_CANCEL_AND_UNLOAD_PASS");
    }

    private static void verifyWriteDrain(String[] args, PulsarClient client, PulsarAdmin admin, MetadataStore metadata,
            Properties expected) throws Exception {
        String prefix = cell(args).getProperty("authorityRoot") + "/native-object-wal/"
                + binding(expected.getProperty("unknownKey"));
        String gate = prefix + "/dispatch/" + pointer(metadata, prefix).current().rootSha256().toHex();
        requireGate(raw(metadata, gate), false);
        int count = Integer.parseInt(expected.getProperty("count"));
        var last = id(expected, count - 1);
        if (last.getEntryId() != 0) {
            throw new AssertionError("Write drain fixture must leave one entry in the virtual ledger");
        }
        var position = new MessageIdImpl(last.getLedgerId(), 1, last.getPartitionIndex());
        var faults = new M6PulsarBookKeeperSemanticsProbe.Faults();
        String key;
        String version;
        try (var producer = client.newProducer().topic(TOPIC).enableBatching(false).create()) {
            faults.set("/" + bucket(args) + "/", "first-put-response-lost");
            var pending = producer.sendAsync(HELD_WRITE);
            version = faults.awaitAppliedVersion();
            var wire = faults.call("GET", "");
            var path = wire.path("selectedPath").asText();
            if (wire.path("hits").asInt() != 1 || !path.startsWith("/" + bucket(args) + "/")
                    || !pending.cancel(false)) {
                throw new AssertionError("Original native Object WAL dispatch did not stay unresolved");
            }
            key = path.substring(bucket(args).length() + 2);
            assertExactVersion(bucket(args), key, version);
            byte[] original = raw(metadata, gate);
            if (!pendingCandidate(original).equals(key)) {
                throw new AssertionError("Held physical PUT is not the exact durable dispatch candidate");
            }
            String[] parts = BUNDLE.split("/");
            var target = new MetadataStoreNereusOwnershipWitnessProvider(metadata, args[6]);
            var unloading = admin.namespaces().unloadNamespaceBundleAsync(parts[0] + "/" + parts[1], parts[2], args[6]);
            try {
                unloading.get(2, TimeUnit.SECONDS);
                throw new AssertionError("Native unload completed before the original S3 PUT callback");
            } catch (TimeoutException held) {
                verifyForeign(args[7], args[8], metadata);
                if (unloading.isDone() || !Arrays.equals(original, raw(metadata, gate)) || !pending.isCancelled()
                        || target.read(BUNDLE).toCompletableFuture().get(5, TimeUnit.SECONDS).isPresent()) {
                    throw new AssertionError("Held original write no longer owns the same dispatch reservation");
                }
            } finally {
                faults.call("PATCH", "");
            }
            unloading.get(40, TimeUnit.SECONDS);
        } finally {
            faults.call("DELETE", "");
        }
        requireGate(raw(metadata, gate), true);
        try (var reader = client.newReader().topic(TOPIC).startMessageId(position).startMessageIdInclusive().create()) {
            var same = reader.readNext(15, TimeUnit.SECONDS);
            if (same == null || !position.equals(same.getMessageId()) || !Arrays.equals(HELD_WRITE, same.getData())) {
                throw new AssertionError("Original held write changed after the native ownership cut");
            }
        }
        assertExactVersion(bucket(args), key, version);
        record(expected, count, position, HELD_WRITE);
        requirePositions(expected);
        try (var output = Files.newBufferedWriter(Path.of(args[4]))) {
            expected.store(output, null);
        }
        System.out.println("PULSAR_OBJECT_WAL_HELD_WRITE_CANCEL_AND_UNLOAD_PASS");
    }

    private static String pendingCandidate(byte[] gate) {
        var value = java.nio.ByteBuffer.wrap(gate);
        if (value.remaining() < 47 || value.getInt() != 0x50574431
                || value.get() != 1 || value.get() != 0 || value.get() != 1) {
            throw new AssertionError("Original Root has no active write reservation");
        }
        value.position(47);
        int length = Short.toUnsignedInt(value.getShort());
        if (length < 1 || value.remaining() != length + Long.BYTES + 32) {
            throw new AssertionError("Original Root has an invalid candidate identity");
        }
        byte[] path = new byte[length];
        value.get(path);
        return new String(path, StandardCharsets.US_ASCII);
    }

    private static void isolate(PulsarAdmin admin, String policy, String namespace, String primary) throws Exception {
        var configuration = org.apache.pulsar.common.policies.data.NamespaceIsolationData.builder()
                .namespaces(java.util.List.of(namespace)).primary(java.util.List.of(primary))
                .secondary(java.util.List.of())
                .unloadScope(org.apache.pulsar.common.policies.data.NamespaceIsolationPolicyUnloadScope.none)
                .autoFailoverPolicy(org.apache.pulsar.common.policies.data.AutoFailoverPolicyData.builder()
                        .policyType(org.apache.pulsar.common.policies.data.AutoFailoverPolicyType.min_available)
                        .parameters(java.util.Map.of("min_limit", "1", "usage_threshold", "100")).build())
                .build();
        admin.clusters().createNamespaceIsolationPolicy("m6", policy, configuration);
    }

    private static void verifyForeign(String web, String service, MetadataStore metadata) throws Exception {
        byte[] payload = java.util.UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        try (var foreign = PulsarClient.builder().serviceUrl("pulsar://" + service)
                .operationTimeout(8, TimeUnit.SECONDS).build();
                var producer = foreign.newProducer().topic(FOREIGN_TOPIC).enableBatching(false).create()) {
            var accepted = (MessageIdAdv) producer.sendAsync(payload).get(6, TimeUnit.SECONDS);
            if (accepted.getLedgerId() < RESERVED_START || accepted.getLedgerId() >= SLICE_END) {
                throw new AssertionError("Foreign Broker did not write through its own Object WAL Cell");
            }
            try (var reader = foreign.newReader().topic(FOREIGN_TOPIC).startMessageId(accepted)
                    .startMessageIdInclusive().create()) {
                var same = reader.readNext(6, TimeUnit.SECONDS);
                if (same == null || !accepted.equals(same.getMessageId()) || !Arrays.equals(payload, same.getData())) {
                    throw new AssertionError("Foreign Cell could not read its newly accepted entry");
                }
            }
        }
        var owner = new MetadataStoreNereusOwnershipWitnessProvider(metadata, web)
                .read(FOREIGN_NAMESPACE + "/0x00000000_0xffffffff").toCompletableFuture()
                .get(5, TimeUnit.SECONDS).orElseThrow();
        System.out.println("PULSAR_OBJECT_WAL_INDEPENDENT_FOREIGN_CELL_PASS " + owner.acquisitionId());
    }

    private static byte[] raw(MetadataStore metadata, String key) throws Exception {
        return metadata.get(key).get(5, TimeUnit.SECONDS).orElseThrow().getValue();
    }

    private static void requireGate(byte[] gate, boolean closed) {
        var value = java.nio.ByteBuffer.wrap(gate);
        if (gate.length < 7 || value.getInt() != 0x50574431 || value.get() != 1
                || value.get() != (closed ? 1 : 0) || value.get() != 0) {
            throw new AssertionError("Unexpected exact current Root dispatch gate");
        }
    }

    private static String binding(String key) {
        var parts = key.split("/");
        for (int i = 0; i < parts.length - 1; i++) {
            if ("object-wal".equals(parts[i]) && parts[i + 1].matches("[a-f0-9]{64}")) {
                return parts[i + 1];
            }
        }
        throw new AssertionError("Unknown Object WAL identity has no Binding");
    }

    private static String bucket(String[] args) throws Exception {
        return cell(args).getProperty("s3Bucket");
    }

    private static Properties cell(String[] args) throws Exception {
        var props = new Properties();
        try (var input = Files.newBufferedReader(Path.of(args[5]))) {
            props.load(input);
        }
        return props;
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
