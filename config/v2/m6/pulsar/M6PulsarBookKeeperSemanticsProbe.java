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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Properties;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.storage.nereus.v2.MetadataStoreNereusOwnershipWitnessProvider;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;

class M6PulsarBookKeeperSemanticsProbe {
    public static void main(String[] args) throws Exception {
        if (args.length >= 7 && "restart".equals(args[6])) {
            verifyRestart(args);
            return;
        }
        boolean asyncObject = args.length == 8 && "async-object".equals(args[6]);
        var namespace = "public/m6-native";
        var range = "0x00000000_0xffffffff";
        var topic = "persistent://" + namespace + "/handoff";
        var unit = namespace + "/" + range;
        byte[] data = "first native BK message".getBytes(StandardCharsets.UTF_8);
        byte[] successorData = "second native BK message".getBytes(StandardCharsets.UTF_8);
        try (var admin = PulsarAdmin.builder().serviceHttpUrl("http://" + args[1]).build();
             var client = PulsarClient.builder().serviceUrl("pulsar://" + args[3]).operationTimeout(15, TimeUnit.SECONDS)
                     .build();
             var metadata = MetadataStoreFactory.create(args[0], MetadataStoreConfig.builder().build())) {
            admin.namespaces().createNamespace(namespace, 1);
            admin.namespaces().setRetention(namespace,
                    new org.apache.pulsar.common.policies.data.RetentionPolicies(-1, -1));
            admin.topics().createNonPartitionedTopic(topic);
            var missing = topic + "-missing";
            try {
                admin.topics().createNonPartitionedTopic(missing);
                throw new AssertionError("unadmitted topic acquired native storage");
            } catch (org.apache.pulsar.client.admin.PulsarAdminException expected) {
                if (!expected.getMessage().contains("Topic has no admitted Nereus Cell binding")) {
                    throw expected;
                }
            }
            var missingLedger = "/managed-ledgers/"
                    + org.apache.pulsar.common.naming.TopicName.get(missing).getPersistenceNamingEncoding();
            if (metadata.get(missingLedger).get(10, TimeUnit.SECONDS).isPresent()) {
                throw new AssertionError("rejected topic created managed-ledger metadata");
            }
            System.out.println("PULSAR_NATIVE_MISSING_BINDING_REJECTED");
            MessageId first;
            var initialPositions = new java.util.ArrayList<MessageId>();
            var initialBodies = new java.util.ArrayList<byte[]>();
            try (var producer = client.newProducer().topic(topic).enableBatching(false).create()) {
                first = producer.send(data);
                initialPositions.add(first);
                initialBodies.add(data);
                for (int i = 1; i < 6; i++) {
                    byte[] body = ("before rollover " + i).getBytes(StandardCharsets.UTF_8);
                    initialPositions.add(producer.send(body));
                    initialBodies.add(body);
                }
                long ledgerCount = initialPositions.stream().map(id -> ((org.apache.pulsar.client.api.MessageIdAdv) id)
                        .getLedgerId()).distinct().count();
                if (ledgerCount < 2) throw new AssertionError("native rollover not observed: " + initialPositions);
            }
            System.out.println("confirmedPositions=" + initialPositions);
            if (asyncObject) {
                verifyAsyncOffload(admin, client, topic, initialPositions, initialBodies, Path.of(args[7]));
            }
            try (var subscription = client.newConsumer().topic(topic).subscriptionName("m6-durable")
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS).isAckReceiptEnabled(true).subscribe()) {
                var individual = subscription.receive(10, TimeUnit.SECONDS);
                var cumulative = subscription.receive(10, TimeUnit.SECONDS);
                if (individual == null || cumulative == null || !first.equals(individual.getMessageId())
                        || !initialPositions.get(1).equals(cumulative.getMessageId())) {
                    throw new AssertionError("subscription did not read initial entries: "
                            + (individual == null ? null : individual.getMessageId()) + " "
                            + (cumulative == null ? null : cumulative.getMessageId())
                            + ", acknowledged " + initialPositions);
                }
                acknowledgeExactly(() -> subscription.acknowledgeAsync(individual), individual.getMessageId());
                acknowledgeExactly(() -> subscription.acknowledgeCumulativeAsync(cumulative), cumulative.getMessageId());
            }
            if (admin.topics().getStats(topic, true).getSubscriptions().get("m6-durable").getMsgBacklog() != initialPositions.size() - 2) {
                throw new AssertionError("durable individual/cumulative ACK did not move backlog");
            }
            String initialAddress = admin.lookups().lookupTopic(topic);
            boolean onFirst = initialAddress.endsWith(args[3]);
            if (!onFirst && !initialAddress.endsWith(args[4])) throw new AssertionError(initialAddress);
            var oldBroker = onFirst ? args[1] : args[2];
            var newBroker = onFirst ? args[2] : args[1];
            var before = new MetadataStoreNereusOwnershipWitnessProvider(metadata, oldBroker)
                    .read(unit).toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow();
            System.out.println("ownershipBefore=" + before);
            long heldLedger = ((MessageIdAdv) initialPositions.get(2)).getLedgerId();
            var faults = asyncObject ? new Faults() : null;
            java.util.UUID originalAttempt = null;
            String heldVersion = null;
            try (var sourceAdmin = PulsarAdmin.builder().serviceHttpUrl("http://" + oldBroker).build()) {
                if (asyncObject) {
                    faults.set("/m6-native-pulsar-offload/m6-native-pulsar/", "first-pulsar-data-put-response-lost");
                    sourceAdmin.topics().triggerOffload(topic, initialPositions.get(4));
                    heldVersion = faults.awaitAppliedVersion();
                    originalAttempt = readOffloadAttempt(metadata, topic, heldLedger, false);
                    var cellKey = cellProperties(Path.of(args[7])).getProperty("probeCellOwnerKey");
                    var cellBefore = readActiveCellOwner(metadata, cellKey);
                    var unloading = sourceAdmin.namespaces().unloadNamespaceBundleAsync(namespace, range, newBroker);
                    try {
                        unloading.get(1, TimeUnit.SECONDS);
                        throw new AssertionError("Old Broker released the bundle before the original Object SDK callback");
                    } catch (java.util.concurrent.TimeoutException expected) {
                        // Routing is fenced on RELEASING; the old Cell must still retain its original scope.
                        var heldOwner = readActiveCellOwner(metadata, cellKey);
                        var successor = new MetadataStoreNereusOwnershipWitnessProvider(metadata, newBroker)
                                .read(unit).toCompletableFuture().get(5, TimeUnit.SECONDS);
                        if (!cellBefore.equals(heldOwner) || successor.isPresent()) {
                            throw new AssertionError("Native Cell was released or successor armed during original SDK drain");
                        }
                    } finally {
                        faults.call("PATCH", "");
                    }
                    unloading.get(30, TimeUnit.SECONDS);
                } else {
                    sourceAdmin.namespaces().unloadNamespaceBundle(namespace, range, newBroker);
                }
            }
            try (var producer = client.newProducer().topic(topic).enableBatching(false).create()) {
                var second = producer.send(successorData);
                var after = new MetadataStoreNereusOwnershipWitnessProvider(metadata, newBroker)
                        .read(unit).toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow();
                System.out.println("ownershipAfter=" + after);
                if (asyncObject) {
                    try (var target = PulsarAdmin.builder().serviceHttpUrl("http://" + newBroker).build()) {
                        target.topics().triggerOffload(topic, initialPositions.get(4));
                        awaitOffload(target, topic);
                    }
                    if (!originalAttempt.equals(readOffloadAttempt(metadata, topic, heldLedger, true))) {
                        throw new AssertionError("Successor replaced the original incomplete native attempt");
                    }
                    assertImmutablePair(Path.of(args[7]), heldLedger, heldVersion, faults);
                    faults.call("DELETE", "");
                    System.out.println("PULSAR_NATIVE_ORIGINAL_OFFLOAD_DRAIN_AND_RESUME " + originalAttempt);
                }
                if (before.brokerIncarnationId().equals(after.brokerIncarnationId())
                        || before.acquisitionId().equals(after.acquisitionId())
                        || before.backendVersion() >= after.backendVersion()) {
                    throw new AssertionError("native transfer reused ownership authority");
                }
                if (admin.topics().getStats(topic, true).getSubscriptions().get("m6-durable").getMsgBacklog() != initialPositions.size() - 1) {
                    throw new AssertionError("durable ACK did not survive native ownership transfer");
                }
                admin.topics().resetCursor(topic, "m6-durable", initialPositions.get(2), false);
                try (var sub = client.newConsumer().topic(topic).subscriptionName("m6-durable")
                        .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS).isAckReceiptEnabled(true).subscribe()) {
                    var replay = sub.receive(10, TimeUnit.SECONDS);
                    if (replay == null || !initialPositions.get(2).equals(replay.getMessageId())) {
                        throw new AssertionError("reset cursor did not preserve the exact native position");
                    }
                    sub.seek(MessageId.earliest);
                    var sought = sub.receive(10, TimeUnit.SECONDS);
                    if (sought == null || !first.equals(sought.getMessageId())) {
                        throw new AssertionError("official consumer seek did not replay the first position");
                    }
                }
                try (var reader = client.newReader().topic(topic).startMessageId(MessageId.earliest).create()) {
                    for (int i = 0; i < initialPositions.size(); i++) {
                        var old = reader.readNext(10, TimeUnit.SECONDS);
                        if (old == null || !initialPositions.get(i).equals(old.getMessageId())
                                || !Arrays.equals(initialBodies.get(i), old.getData())) {
                            throw new AssertionError("native data or MessageId changed across owner transfer");
                        }
                    }
                    var current = reader.readNext(10, TimeUnit.SECONDS);
                    if (current == null || !second.equals(current.getMessageId())
                            || !Arrays.equals(successorData, current.getData())) {
                        throw new AssertionError("successor write differs");
                    }
                }
                var expected = new Properties();
                var encoder = Base64.getEncoder();
                expected.setProperty("firstBrokerIncarnation", before.brokerIncarnationId().value());
                expected.setProperty("secondBrokerIncarnation", after.brokerIncarnationId().value());
                initialPositions.add(second);
                initialBodies.add(successorData);
                expected.setProperty("count", Integer.toString(initialPositions.size()));
                for (int i = 0; i < initialPositions.size(); i++) {
                    expected.setProperty("position." + i, encoder.encodeToString(initialPositions.get(i).toByteArray()));
                    expected.setProperty("body." + i, encoder.encodeToString(initialBodies.get(i)));
                }
                try (var output = Files.newBufferedWriter(Path.of(args[5]))) {
                    expected.store(output, null);
                }
                System.out.println("PULSAR_NATIVE_BK_PROTOCOL_SEMANTICS_PASS " + first + " " + second);
            }
        }
    }
    private static void verifyAsyncOffload(
            PulsarAdmin admin, PulsarClient client, String topic, java.util.List<MessageId> initialPositions,
            java.util.List<byte[]> initialBodies, Path cell) throws Exception {
        var faults = new Faults();
        faults.set("/m6-native-pulsar-offload/m6-native-pulsar/", "first-pulsar-data-put-response-lost");
        admin.topics().triggerOffload(topic, initialPositions.get(2));
        var heldVersion = faults.awaitAppliedVersion();
        if (admin.topics().offloadStatus(topic).getStatus()
                != org.apache.pulsar.client.admin.LongRunningProcessStatus.Status.RUNNING) {
            throw new AssertionError("Native offload completed before its original PUT result settled");
        }
        appendAcrossRollover(client, topic, "while-original-put-held", initialPositions, initialBodies);
        faults.call("PATCH", "");
        awaitOffload(admin, topic);
        long firstLedger = ((MessageIdAdv) initialPositions.getFirst()).getLedgerId();
        if (admin.topics().getInternalStats(topic).ledgers.stream()
                .noneMatch(row -> row.ledgerId == firstLedger && row.offloaded)) {
            throw new AssertionError("Native ledger did not publish its completed offload");
        }
        assertImmutablePair(cell, firstLedger, heldVersion, faults);
        System.out.println("PULSAR_NATIVE_ENROLLED_NPD1_NPO1_PUBLISHED " + firstLedger);
        faults.set("/m6-native-pulsar-offload/", "bucket-unavailable");
        try {
            appendAcrossRollover(client, topic, "while-object-provider-unavailable", initialPositions, initialBodies);
            if (faults.call("GET", "").path("hits").asInt() != 0) {
                throw new AssertionError("BK rollover contacted the unavailable Object provider");
            }
        } finally {
            faults.call("DELETE", "");
        }
        System.out.println("PULSAR_NATIVE_OBJECT_LOST_REPLY_AND_INDEPENDENT_ROLLOVER " + heldVersion);
    }

    private static Properties cellProperties(Path path) throws Exception {
        var props = new Properties();
        try (var input = Files.newBufferedReader(path)) {
            props.load(input);
        }
        return props;
    }

    private record CellOwner(long generation, java.util.UUID owner) {}

    private static CellOwner readActiveCellOwner(org.apache.pulsar.metadata.api.MetadataStore metadata, String key)
            throws Exception {
        var raw = metadata.get(key).get(5, TimeUnit.SECONDS).orElseThrow().getValue();
        var bytes = java.nio.ByteBuffer.wrap(raw);
        if (raw.length < 132 || bytes.getInt() != 0x4e504348 || bytes.getInt() != 2) {
            throw new AssertionError("Native Cell head did not use its admitted owner record");
        }
        var digest = java.security.MessageDigest.getInstance("SHA-256")
                .digest(Arrays.copyOf(raw, raw.length - 32));
        if (!Arrays.equals(digest, Arrays.copyOfRange(raw, raw.length - 32, raw.length))) {
            throw new AssertionError("Native Cell head failed its exact checksum");
        }
        bytes.position(72);
        var owner = new CellOwner(bytes.getLong(), new java.util.UUID(bytes.getLong(), bytes.getLong()));
        if (owner.generation() <= 0 || bytes.getInt() != 0) {
            throw new AssertionError("Original native Cell head was no longer active");
        }
        return owner;
    }

    private static java.util.UUID readOffloadAttempt(
            org.apache.pulsar.metadata.api.MetadataStore metadata, String topic, long ledgerId, boolean complete)
            throws Exception {
        var path = "/managed-ledgers/"
                + org.apache.pulsar.common.naming.TopicName.get(topic).getPersistenceNamingEncoding();
        var bytes = metadata.get(path).get(5, TimeUnit.SECONDS).orElseThrow().getValue();
        var store = new org.apache.bookkeeper.mledger.impl.MetaStoreImpl(null, null);
        var context = store.parseManagedLedgerInfo(bytes).getLedgerInfosList().stream()
                .filter(source -> source.getLedgerId() == ledgerId).findFirst().orElseThrow().getOffloadContext();
        if (!context.hasUidMsb() || !context.hasUidLsb() || context.isComplete() != complete
                || context.isBookkeeperDeleted()) {
            throw new AssertionError("Unexpected native offload marker for retained ledger " + ledgerId);
        }
        return new java.util.UUID(context.getUidMsb(), context.getUidLsb());
    }

    private static void awaitOffload(PulsarAdmin admin, String topic) throws Exception {
        var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
        while (true) {
            var status = admin.topics().offloadStatus(topic);
            if (status.getStatus() == org.apache.pulsar.client.admin.LongRunningProcessStatus.Status.SUCCESS) {
                return;
            }
            if (status.getStatus() == org.apache.pulsar.client.admin.LongRunningProcessStatus.Status.ERROR
                    || System.nanoTime() > deadline) {
                throw new AssertionError("Native offload failed: " + status.getStatus() + " " + status.getLastError());
            }
            Thread.sleep(150);
        }
    }

    private static void assertImmutablePair(Path cell, long ledgerId, String heldVersion, Faults faults)
            throws Exception {
        var props = cellProperties(cell);
        try (var s3 = software.amazon.awssdk.services.s3.S3Client.builder()
                .credentialsProvider(software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider
                        .create())
                .endpointOverride(java.net.URI.create(System.getenv("NEREUS_M6_PULSAR_DIRECT_S3")))
                .forcePathStyle(true).region(software.amazon.awssdk.regions.Region.US_EAST_1).build()) {
            var versions = s3.listObjectVersions(r -> r.bucket(props.getProperty("s3Bucket"))
                    .prefix("m6-native-pulsar/pulsar-offload/v1/ledger-" + ledgerId + "/")).versions();
            if (versions.size() != 2 || versions.stream().map(value -> value.key()).distinct().count() != 2
                    || versions.stream().noneMatch(value ->
                            value.key().endsWith("/data") && value.versionId().equals(heldVersion))
                    || faults.call("GET", "").path("hits").asInt() != 1) {
                throw new AssertionError("Native offload did not publish the original immutable data/root pair");
            }
        }
    }

    private static void appendAcrossRollover(
            PulsarClient client, String topic, String label, java.util.List<MessageId> positions,
            java.util.List<byte[]> bodies) throws Exception {
        long oldLedger = ((MessageIdAdv) positions.getLast()).getLedgerId();
        try (var producer = client.newProducer().topic(topic).enableBatching(false).create()) {
            for (int i = 0; i < 2; i++) {
                byte[] body = (label + " " + i).getBytes(StandardCharsets.UTF_8);
                positions.add(producer.sendAsync(body).get(8, TimeUnit.SECONDS));
                bodies.add(body);
            }
        }
        if (((MessageIdAdv) positions.getLast()).getLedgerId() == oldLedger) {
            throw new AssertionError("Independent BK writes did not cross a real rollover: " + label);
        }
    }

    static final class Faults {
        private final java.net.http.HttpClient http = java.net.http.HttpClient.newHttpClient();
        private final java.net.URI control = java.net.URI.create(System.getenv("NEREUS_M6_PULSAR_FAULT_CONTROL"));
        private final String token = System.getenv("NEREUS_M6_PULSAR_FAULT_TOKEN");
        private final com.fasterxml.jackson.databind.ObjectMapper json = new com.fasterxml.jackson.databind.ObjectMapper();

        void set(String path, String mode) throws Exception {
            call("POST", json.writeValueAsString(java.util.Map.of("path", path, "mode", mode, "version", "*")));
        }

        String awaitAppliedVersion() throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(12);
            do {
                var response = call("GET", "");
                if (response.path("appliedVersion").isTextual()) {
                    return response.path("appliedVersion").textValue();
                }
                Thread.sleep(25);
            } while (System.nanoTime() < deadline);
            throw new AssertionError("MinIO did not apply the original held native PUT");
        }

        com.fasterxml.jackson.databind.JsonNode call(String method, String body) throws Exception {
            var request = java.net.http.HttpRequest.newBuilder(control)
                    .header("X-Nereus-Test-Token", token)
                    .timeout(java.time.Duration.ofSeconds(5))
                    .method(method, body.isEmpty() ? java.net.http.HttpRequest.BodyPublishers.noBody()
                            : java.net.http.HttpRequest.BodyPublishers.ofString(body))
                    .build();
            var response = http.send(request, java.net.http.HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new AssertionError("Native Object fault control did not accept " + method + ": "
                        + response.statusCode());
            }
            return json.readTree(response.body());
        }
    }

    private static void verifyRestart(String[] args) throws Exception {
        var topic = "persistent://public/m6-native/handoff";
        var unit = "public/m6-native/0x00000000_0xffffffff";
        var expected = new Properties();
        try (var input = Files.newBufferedReader(Path.of(args[5]))) {
            expected.load(input);
        }
        var decoder = Base64.getDecoder();
        try (var client = PulsarClient.builder().serviceUrl("pulsar://" + args[3])
                        .operationTimeout(15, TimeUnit.SECONDS).build();
                var metadata = MetadataStoreFactory.create(args[0], MetadataStoreConfig.builder().build());
                var reader = client.newReader().topic(topic).startMessageId(MessageId.earliest).create()) {
            for (int i = 0; i < Integer.parseInt(expected.getProperty("count")); i++) {
                var actual = reader.readNext(10, TimeUnit.SECONDS);
                var id = MessageId.fromByteArray(decoder.decode(expected.getProperty("position." + i)));
                var body = decoder.decode(expected.getProperty("body." + i));
                if (actual == null || !id.equals(actual.getMessageId()) || !Arrays.equals(body, actual.getData())) {
                    throw new AssertionError("cold restart changed the exact native message at ordinal " + i);
                }
            }
            var ownership = new MetadataStoreNereusOwnershipWitnessProvider(metadata, args[1])
                    .read(unit).toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow();
            var currentIncarnation = ownership.brokerIncarnationId().value();
            if (currentIncarnation.equals(expected.getProperty("firstBrokerIncarnation"))
                    || currentIncarnation.equals(expected.getProperty("secondBrokerIncarnation"))) {
                throw new AssertionError("process restart reused a broker incarnation");
            }
            try (var producer = client.newProducer().topic(topic).enableBatching(false).create()) {
                byte[] body = "post-restart native write".getBytes(StandardCharsets.UTF_8);
                var id = producer.send(body);
                var confirmed = reader.readNext(10, TimeUnit.SECONDS);
                if (confirmed == null || !id.equals(confirmed.getMessageId())
                        || !Arrays.equals(body, confirmed.getData())) {
                    throw new AssertionError("restart did not continue the native ledger chain");
                }
                if (args.length == 8 && "record-restart-position".equals(args[7])) {
                    int next = Integer.parseInt(expected.getProperty("count"));
                    expected.setProperty("count", Integer.toString(next + 1));
                    expected.setProperty("position." + next, Base64.getEncoder().encodeToString(id.toByteArray()));
                    expected.setProperty("body." + next, Base64.getEncoder().encodeToString(body));
                    try (var output = Files.newBufferedWriter(Path.of(args[5]))) {
                        expected.store(output, null);
                    }
                }
            }
            System.out.println("PULSAR_NATIVE_BK_RESTART_PASS");
        }
    }

    private static void acknowledgeExactly(
            java.util.function.Supplier<java.util.concurrent.CompletableFuture<Void>> ack, MessageId position)
            throws Exception {
        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                ack.get().get(10, TimeUnit.SECONDS);
                System.out.println("confirmedAck=" + position + " attempts=" + (attempt + 1));
                return;
            } catch (java.util.concurrent.ExecutionException error) {
                if (!(error.getCause() instanceof PulsarClientException.ServiceNotReadyException)
                        || !error.getCause().getMessage().contains("Native storage ownership changed") || attempt == 2) {
                    throw error;
                }
                System.out.println("acknowledgmentOutcomeUnknown=" + position);
            }
        }
    }
}
