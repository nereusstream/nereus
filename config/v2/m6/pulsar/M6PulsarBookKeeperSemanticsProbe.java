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
        if (args.length == 7 && "restart".equals(args[6])) {
            verifyRestart(args);
            return;
        }
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
            if (admin.topics().getStats(topic, true).getSubscriptions().get("m6-durable").getMsgBacklog() != 4) {
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
            try (var sourceAdmin = PulsarAdmin.builder().serviceHttpUrl("http://" + oldBroker).build()) {
                sourceAdmin.namespaces().unloadNamespaceBundle(namespace, range, newBroker);
            }
            try (var producer = client.newProducer().topic(topic).enableBatching(false).create()) {
                var second = producer.send(successorData);
                var after = new MetadataStoreNereusOwnershipWitnessProvider(metadata, newBroker)
                        .read(unit).toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow();
                System.out.println("ownershipAfter=" + after);
                if (before.brokerIncarnationId().equals(after.brokerIncarnationId())
                        || before.acquisitionId().equals(after.acquisitionId())
                        || before.backendVersion() >= after.backendVersion()) {
                    throw new AssertionError("native transfer reused ownership authority");
                }
                if (admin.topics().getStats(topic, true).getSubscriptions().get("m6-durable").getMsgBacklog() != 5) {
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
            for (int i = 0; i < 7; i++) {
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
